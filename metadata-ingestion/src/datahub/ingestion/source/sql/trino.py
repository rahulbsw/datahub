import logging
from typing import Any, Iterable, Optional,List


# This import verifies that the dependencies are available.
import sqlalchemy_trino # noqa: F401
from sqlalchemy_trino import datatype
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql import text,sqltypes
from sqlalchemy import exc
from sqlalchemy.sql.elements import quoted_name
from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.source.sql.sql_common import (
    RecordTypeClass,
    SQLAlchemyConfig,
    SQLAlchemySource,
    TimeTypeClass,
    ArrayTypeClass,
    StringTypeClass,
    make_sqlalchemy_uri,
    register_custom_type,
    SqlWorkUnit,
    get_schema_metadata,
)
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    EnumTypeClass,
    MySqlDDL,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import ChangeTypeClass, DatasetPropertiesClass


#register_custom_type(custom_types.TIMESTAMP_TZ, TimeTypeClass)
#register_custom_type(custom_types.TIMESTAMP_LTZ, TimeTypeClass)
register_custom_type(datatype.MAP, RecordTypeClass)
register_custom_type(datatype.ROW, RecordTypeClass)
register_custom_type(sqltypes.JSON, RecordTypeClass)
register_custom_type(sqltypes.ARRAY, ArrayTypeClass)
#register_custom_type(sqltypes.UUID, StringTypeClass)


logger: logging.Logger = logging.getLogger(__name__)


class BaseTrinoConfig(ConfigModel):
    # Note: this config model is also used by the snowflake-usage source.

    scheme = "trino"

    username: Optional[str] = None
    password: Optional[str] = None
    catalog: Optional[str] = None
    schema_name: Optional[str] = None
    host_port: str
    verify: Optional[str]

    def get_sql_alchemy_url(self, database=None):
        if database is None:
            database=self.catalog
        if self.schema_name:
            database=f'{database}/{self.schema_name}'

        return make_sqlalchemy_uri(
            self.scheme,
            self.username,
            self.password,
            self.host_port,
            database,
            uri_opts={
                # Drop the options if value is None.
                key: value
                for (key, value) in {
                    "verify": self.verify
                }.items()
                if value
            },
        )


class TrinoConfig(BaseTrinoConfig, SQLAlchemyConfig):
    catalog_pattern: AllowDenyPattern = AllowDenyPattern(
        deny=[
             r"^system$",
             r"^memory$",
            # r"^SNOWFLAKE_SAMPLE_DATA$",
        ]
    )

    def get_sql_alchemy_url(self):
        return super().get_sql_alchemy_url(database=None)


class TrinoSource(SQLAlchemySource):
    config: TrinoConfig
    catalog: str

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "trino")
        self.catalog=config.catalog

    @classmethod
    def create(cls, config_dict, ctx):
        config = TrinoConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_inspectors(self) -> Iterable[Inspector]:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        catalogs:List[str] = [] # List[str]
        # Get all catalogs
        schemas: Dict[Str,TrinoConfig]={}
        for db_row in engine.execute(text("SHOW CATALOGS")):
            with engine.connect() as conn:
                catalog=db_row.Catalog
                if self.config.catalog_pattern.allowed(catalog):
                    catalogs.append(catalog)
                    for schema_row in engine.execute(text(f"SHOW SCHEMAS FROM {catalog}")):
                        schema=schema_row.Schema
                        if self.config.schema_pattern.allowed(schema):
                            local_config=self.config
                            local_config.catalog=catalog
                            local_config.schema_name=schema
                            schemas[schema]=local_config
                        else:
                            self.report.report_dropped(f'{catalog}.{schema}')    
                else:
                    self.report.report_dropped(catalog)

        # inspect each catalog   
        # Todo: current implemenation seems to be hacky becaus of driver limitation 
        for schema in schemas:
           local_config=schemas.get(schema)
           if local_config:
                logger.info(f'Processing Catalog:{local_config.catalog}, Schema:{local_config.schema_name}')
                local_url = local_config.get_sql_alchemy_url()
                logger.debug(f"sql_alchemy_url={local_url}")
                local_engine=create_engine(local_url, **local_config.options)
                self.catalog = local_config.catalog
                with local_engine.connect() as local_conn:
                    inspector = inspect(local_conn)
                    yield inspector
           

    def get_identifier(self, *, schema: str, entity: str, **kwargs: Any) -> str:
        regular = super().get_identifier(schema=schema, entity=entity, **kwargs)
        return f"{self.catalog.lower()}.{regular}"

    def loop_tables(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[SqlWorkUnit]:
        for table in inspector.get_table_names(schema):
            try:
                schema, table = self.standardize_schema_table_names(
                    schema=schema, entity=table
                )
                dataset_name = self.get_identifier(
                    schema=schema, entity=table, inspector=inspector
                )
                self.report.report_entity_scanned(dataset_name, ent_type="table")

                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                columns = inspector.get_columns(table, schema)
                if len(columns) == 0:
                    self.report.report_warning(dataset_name, "missing column information")

                try:
                    # SQLALchemy stubs are incomplete and missing this method.
                    # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
                    table_info: dict = inspector.get_table_comment(table, schema)  # type: ignore
                except NotImplementedError:
                    description: Optional[str] = None
                    properties: Dict[str, str] = {}
                else:
                    description = table_info["text"]

                    # The "properties" field is a non-standard addition to SQLAlchemy's interface.
                    properties = table_info.get("properties", {})

                # TODO: capture inspector.get_pk_constraint
                # TODO: capture inspector.get_sorted_table_and_fkc_names

                dataset_snapshot = DatasetSnapshot(
                    urn=f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{dataset_name},{self.config.env})",
                    aspects=[],
                )
                if description is not None or properties:
                    dataset_properties = DatasetPropertiesClass(
                        description=description,
                        customProperties=properties,
                    )
                    dataset_snapshot.aspects.append(dataset_properties)
                schema_metadata = get_schema_metadata(
                    self.report, dataset_name, self.platform, columns
                )
                dataset_snapshot.aspects.append(schema_metadata)

                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                wu = SqlWorkUnit(id=dataset_name, mce=mce)
                self.report.report_workunit(wu)
                yield wu
            except exc.NoSuchTableError as e:
               logger.warning(e)   

    def loop_views(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[SqlWorkUnit]:
        for view in inspector.get_view_names(schema):
            try:
                schema, view = self.standardize_schema_table_names(
                    schema=schema, entity=view
                )
                dataset_name = self.get_identifier(
                    schema=schema, entity=view, inspector=inspector
                )
                self.report.report_entity_scanned(dataset_name, ent_type="view")

                if not sql_config.view_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                try:
                    columns = inspector.get_columns(view, schema)
                except KeyError:
                    # For certain types of views, we are unable to fetch the list of columns.
                    self.report.report_warning(
                        dataset_name, "unable to get schema for this view"
                    )
                    schema_metadata = None
                else:
                    schema_metadata = get_schema_metadata(
                        self.report, dataset_name, self.platform, columns
                    )

                try:
                    # SQLALchemy stubs are incomplete and missing this method.
                    # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
                    view_info: dict = inspector.get_table_comment(view, schema)  # type: ignore
                except NotImplementedError:
                    description: Optional[str] = None
                    properties: Dict[str, str] = {}
                else:
                    description = view_info["text"]

                    # The "properties" field is a non-standard addition to SQLAlchemy's interface.
                    properties = view_info.get("properties", {})

                try:
                    view_definition = inspector.get_view_definition(view, schema)
                    if view_definition is None:
                        view_definition = ""
                    else:
                        # Some dialects return a TextClause instead of a raw string,
                        # so we need to convert them to a string.
                        view_definition = str(view_definition)
                except NotImplementedError:
                    view_definition = ""
                properties["view_definition"] = view_definition
                properties["is_view"] = "True"

                dataset_snapshot = DatasetSnapshot(
                    urn=f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{dataset_name},{self.config.env})",
                    aspects=[],
                )
                if description is not None or properties:
                    dataset_properties = DatasetPropertiesClass(
                        description=description,
                        customProperties=properties,
                        # uri=dataset_name,
                    )
                    dataset_snapshot.aspects.append(dataset_properties)

                if schema_metadata:
                    dataset_snapshot.aspects.append(schema_metadata)

                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                wu = SqlWorkUnit(id=dataset_name, mce=mce)
                self.report.report_workunit(wu)
                yield wu
            except exc.NoSuchTableError as e: 
               logger.warning(e)   
