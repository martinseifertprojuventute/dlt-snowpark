"""
Simplified Snowpark Destination for dlt v1.20.0

A minimal implementation that:
- Uses Snowpark session (works inside Snowflake stored procedures)
- Implements WithStateSync for incremental loading state persistence
- Uses Snowpark's native Table.merge() for merge operations
- Supports parquet file loading via PUT + COPY INTO

Architecture:
- snowpark: Destination class for dlt.pipeline()
- SnowparkJobClient: Minimal job client with state sync and merge
- Uses dlt's built-in schema management and type mapping
"""

import json
import os
from typing import Optional, Any, Type
import dataclasses

from snowflake.snowpark import Session
from snowflake.snowpark.functions import when_matched, when_not_matched

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.configuration import configspec
from dlt.common.destination.client import (
    StateInfo,
    StorageSchemaInfo,
    WithStateSync,
    DestinationClientDwhConfiguration,
    LoadJob,
    RunnableLoadJob,
)
from dlt.common.schema import Schema, TTableSchema
from dlt.common.utils import uniq_id


# ============================================================================
# Load Job Implementation
# ============================================================================

class SnowparkLoadJob(RunnableLoadJob):
    """Load job that stages and loads parquet files using Snowpark."""

    def __init__(
        self,
        file_path: str,
        snowpark_session: Session,
        database: str,
        schema: str,
        table_name: str,
        table_schema: Optional[TTableSchema] = None,
    ):
        super().__init__(file_path)
        self.snowpark_session = snowpark_session
        self.database = database.upper()
        self.schema_name = schema.upper()
        self.table_name = table_name.upper()
        self.table_schema = table_schema  # dlt schema with column type hints
        self._job_client: "SnowparkJobClient" = None

    def _get_variant_columns(self) -> set:
        """Get column names that should be VARIANT based on dlt schema."""
        variant_cols = set()
        if not self.table_schema:
            return variant_cols

        columns = self.table_schema.get("columns", {})
        for col_name, col_info in columns.items():
            # Check for json data_type hint
            if col_info.get("data_type") == "json":
                variant_cols.add(col_name.upper())
            # Also check for complex type (nested structures)
            if col_info.get("data_type") == "complex":
                variant_cols.add(col_name.upper())
        return variant_cols

    def run(self) -> None:
        """Load parquet file using PUT + COPY INTO target table."""
        if os.path.getsize(self._file_path) == 0:
            return  # Skip empty files

        session = self.snowpark_session
        target_table = f"{self.database}.{self.schema_name}.{self.table_name}"
        ff_parquet = f"{self.database}.{self.schema_name}.FF_PARQUET"

        # Create temporary stage referencing the named file format
        stage_id = uniq_id(8)
        stage_name = f"DLT_STAGE_{stage_id}"
        session.sql(f"CREATE TEMPORARY STAGE {stage_name} FILE_FORMAT = {ff_parquet}").collect()

        # PUT file to stage
        session.file.put(
            self._file_path,
            f"@{stage_name}/",
            auto_compress=False,
            overwrite=True,
        )

        # Get columns that should be VARIANT (json type in dlt schema)
        variant_columns = self._get_variant_columns()
        # Build SQL list for CASE expression, or empty string if no variant columns
        variant_cols_sql = ",".join(f"'{c}'" for c in variant_columns) if variant_columns else "''"

        # Create table from parquet schema if it doesn't exist
        # Transform column names to uppercase to avoid case-sensitive identifiers
        # Force all columns to be NULLABLE to handle schema variations across batches
        # Override type to VARIANT for columns marked as json in dlt schema
        file_name = os.path.basename(self._file_path)
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {target_table}
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                    'COLUMN_NAME', UPPER(COLUMN_NAME),
                    'TYPE', CASE
                        WHEN UPPER(COLUMN_NAME) IN ({variant_cols_sql}) THEN 'VARIANT'
                        ELSE TYPE
                    END,
                    'NULLABLE', TRUE
                ))
                FROM TABLE(INFER_SCHEMA(
                    LOCATION => '@{stage_name}/{file_name}',
                    FILE_FORMAT => '{ff_parquet}'
                ))
            )
        """).collect()

        # COPY INTO target table (uses stage's file format)
        # Snowflake will auto-parse JSON strings into VARIANT columns
        session.sql(f"""
            COPY INTO {target_table}
            FROM @{stage_name}/{file_name}
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = ABORT_STATEMENT
        """).collect()


# ============================================================================
# Job Client with State Sync and Snowpark Merge
# ============================================================================

class SnowparkJobClient(WithStateSync):
    """Simplified Snowpark job client with state sync and native merge."""

    def __init__(
        self,
        schema: Schema,
        config: "SnowparkDestinationClientConfiguration",
        capabilities: DestinationCapabilitiesContext,
    ):
        self.schema = schema
        self.config = config
        self.capabilities = capabilities
        self.snowpark_session = config.snowpark_session
        self.database = config.database.upper()
        self.dataset_name = (config.dataset_name or schema.name).upper()
        self.staging_dataset_name = f"{self.dataset_name}_STAGING"

        # Ensure schemas exist
        self._ensure_schemas_exist()
        self._ensure_dlt_tables_exist()

    # Context manager protocol (required by dlt)
    def __enter__(self) -> "SnowparkJobClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass  # No cleanup needed - Snowpark session is managed externally

    # Required dlt job client methods
    def _get_write_disposition(self, table_name: str) -> str:
        """Get write disposition for a table from the schema.

        For child tables (containing __), inherit from parent table.
        """
        # Try both lowercase and original case
        table_schema = self.schema.tables.get(table_name.lower()) or self.schema.tables.get(table_name)
        if table_schema:
            disposition = table_schema.get("write_disposition")
            if disposition:
                return disposition

        # For child tables, inherit from parent
        if "__" in table_name:
            parent_name = table_name.split("__")[0]
            parent_schema = self.schema.tables.get(parent_name.lower()) or self.schema.tables.get(parent_name)
            if parent_schema:
                return parent_schema.get("write_disposition", "append")

        return "append"

    def should_truncate_table_before_load(self, table_name: str) -> bool:
        """Whether to truncate the table before loading.

        - replace: True (truncate and overwrite)
        - merge: False (use merge logic)
        - append: False (just append)
        """
        disposition = self._get_write_disposition(table_name)
        return disposition == "replace"

    def should_load_data_to_staging_dataset(self, table_name: str) -> bool:
        """Whether to load data to staging dataset first.

        - replace: False (load directly to main table)
        - merge: True (load to staging, then merge)
        - append: False (load directly to main table)
        """
        disposition = self._get_write_disposition(table_name)
        return disposition == "merge"

    def initialize_storage(self, truncate_tables: set = None) -> None:
        """Initialize storage - create schemas and truncate tables if requested.

        Called by dlt before loading data to ensure the destination is ready.
        """
        self._ensure_schemas_exist()
        self._ensure_dlt_tables_exist()

        session = self.snowpark_session

        # Drop all staging tables before loading to ensure clean state
        # Using DROP instead of TRUNCATE so tables get recreated with all nullable columns
        staging_tables = session.sql(f"""
            SELECT table_name
            FROM {self.database}.INFORMATION_SCHEMA.TABLES
            WHERE table_schema = '{self.staging_dataset_name}'
            AND table_name NOT LIKE '_DLT%'
        """).collect()
        for row in staging_tables:
            try:
                session.sql(f"DROP TABLE {self.database}.{self.staging_dataset_name}.{row[0]}").collect()
            except Exception:
                pass

        # Drop main tables if requested (used for replace disposition)
        # Using DROP instead of TRUNCATE to handle schema changes
        if truncate_tables:
            for table_name in truncate_tables:
                qualified_table = f"{self.database}.{self.dataset_name}.{table_name.upper()}"
                try:
                    session.sql(f"DROP TABLE IF EXISTS {qualified_table}").collect()
                except Exception:
                    pass  # Table might not exist yet

    def _ensure_schemas_exist(self) -> None:
        """Create main and staging schemas if they don't exist, plus file format for parquet."""
        session = self.snowpark_session
        session.sql(f"CREATE SCHEMA IF NOT EXISTS {self.database}.{self.dataset_name}").collect()
        session.sql(f"CREATE TRANSIENT SCHEMA IF NOT EXISTS {self.database}.{self.staging_dataset_name}").collect()
        # Named file format required for INFER_SCHEMA
        session.sql(f"CREATE FILE FORMAT IF NOT EXISTS {self.database}.{self.dataset_name}.FF_PARQUET TYPE = PARQUET").collect()
        session.sql(f"CREATE FILE FORMAT IF NOT EXISTS {self.database}.{self.staging_dataset_name}.FF_PARQUET TYPE = PARQUET").collect()

    def _ensure_dlt_tables_exist(self) -> None:
        """Create dlt metadata tables in main schema."""
        session = self.snowpark_session
        schema = f"{self.database}.{self.dataset_name}"

        # _dlt_version: schema version tracking
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {schema}._DLT_VERSION (
                version INTEGER,
                engine_version INTEGER,
                inserted_at TIMESTAMP_TZ,
                schema_name VARCHAR,
                version_hash VARCHAR,
                schema VARIANT
            )
        """).collect()

        # _dlt_pipeline_state: incremental loading state
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {schema}._DLT_PIPELINE_STATE (
                version INTEGER,
                engine_version INTEGER,
                pipeline_name VARCHAR,
                state VARCHAR,
                created_at TIMESTAMP_TZ,
                version_hash VARCHAR,
                _dlt_load_id VARCHAR
            )
        """).collect()

        # _dlt_loads: load tracking
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {schema}._DLT_LOADS (
                load_id VARCHAR,
                status INTEGER,
                schema_name VARCHAR,
                schema_version_hash VARCHAR,
                inserted_at TIMESTAMP_TZ
            )
        """).collect()

    def verify_schema(
        self,
        only_tables: set = None,  # noqa: ARG002 - required by dlt interface
        new_jobs: set = None,  # noqa: ARG002 - required by dlt interface
    ) -> None:
        """Ensure all tables have write_disposition set.

        dlt checks write_disposition before calling prepare_load_table,
        so we need to ensure child tables inherit from their parent.
        Tables are created automatically by COPY INTO with MATCH_BY_COLUMN_NAME.
        """
        # Ensure all child tables have write_disposition inherited from parent
        for table_name, table_schema in self.schema.tables.items():
            if "write_disposition" not in table_schema:
                # Get parent table name (first segment before __)
                parent_name = table_name.split("__")[0]
                parent_schema = self.schema.tables.get(parent_name)
                if parent_schema and "write_disposition" in parent_schema:
                    table_schema["write_disposition"] = parent_schema["write_disposition"]
                else:
                    # Default to append if no parent found
                    table_schema["write_disposition"] = "append"

    def prepare_load_table(self, table_name: str) -> TTableSchema:
        """Prepare and return the table schema for loading.

        Called by dlt before creating a load job.
        Uses dlt's fill_hints_from_parent_and_clone_table to properly inherit write_disposition.
        """
        from dlt.common.schema.utils import fill_hints_from_parent_and_clone_table

        # Try to find table in schema
        table_schema = self.schema.tables.get(table_name) or self.schema.tables.get(table_name.lower())
        if not table_schema:
            # Create minimal schema for unknown table
            table_schema = {"name": table_name}

        # Use dlt's utility to inherit write_disposition from parent
        return fill_hints_from_parent_and_clone_table(self.schema.tables, table_schema)

    def prepare_load_job_execution(self, job: LoadJob) -> None:
        """Prepare load job for execution.

        Called by dlt before running the load job. Sets the job client reference.
        """
        if hasattr(job, '_job_client'):
            job._job_client = self

    def create_load_job(
        self,
        table: TTableSchema,
        file_path: str,
        load_id: str,
        restore: bool = False
    ) -> LoadJob:
        """Create a load job for a parquet file."""
        table_name = table.get("name", "unknown")

        # Determine target schema based on write disposition
        use_staging = self.should_load_data_to_staging_dataset(table_name)
        target_schema = self.staging_dataset_name if use_staging else self.dataset_name

        return SnowparkLoadJob(
            file_path=file_path,
            snowpark_session=self.snowpark_session,
            database=self.database,
            schema=target_schema,
            table_name=table_name,
            table_schema=table,  # Pass dlt schema for VARIANT column detection
        )

    def complete_load(self, load_id: str) -> None:
        """Mark load as complete and merge staging to main."""
        session = self.snowpark_session
        main_schema = f"{self.database}.{self.dataset_name}"

        # Get list of tables that have data in staging
        tables_result = session.sql(f"""
            SELECT table_name
            FROM {self.database}.INFORMATION_SCHEMA.TABLES
            WHERE table_schema = '{self.staging_dataset_name}'
            AND table_name NOT LIKE '_DLT%'
        """).collect()

        for row in tables_result:
            table_name = row[0]
            self._merge_table(table_name)

        # Record load completion
        session.sql(f"""
            INSERT INTO {main_schema}._DLT_LOADS (load_id, status, schema_name, inserted_at)
            VALUES ('{load_id}', 0, '{self.dataset_name}', CURRENT_TIMESTAMP())
        """).collect()

    def _merge_table(self, table_name: str) -> None:
        """Merge a single table from staging to main using Snowpark's native merge."""
        session = self.snowpark_session
        main_table_fqn = f"{self.database}.{self.dataset_name}.{table_name}"
        staging_table_fqn = f"{self.database}.{self.staging_dataset_name}.{table_name}"

        # Check if staging table has data
        count_result = session.sql(f"SELECT COUNT(*) FROM {staging_table_fqn}").collect()
        if not count_result or count_result[0][0] == 0:
            return

        # Ensure main table exists
        try:
            session.sql(f"CREATE TABLE IF NOT EXISTS {main_table_fqn} LIKE {staging_table_fqn}").collect()
        except Exception:
            pass

        # Handle schema evolution: add any new columns from staging to main
        # This ensures ALL BY NAME works even when schema has changed
        staging_cols_result = session.sql(f"""
            SELECT column_name, data_type
            FROM {self.database}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = '{self.staging_dataset_name}' AND table_name = '{table_name}'
        """).collect()
        main_cols_result = session.sql(f"""
            SELECT column_name
            FROM {self.database}.INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = '{self.dataset_name}' AND table_name = '{table_name}'
        """).collect()
        main_cols = {row[0] for row in main_cols_result}

        for row in staging_cols_result:
            col_name, data_type = row[0], row[1]
            if col_name not in main_cols:
                session.sql(f"ALTER TABLE {main_table_fqn} ADD COLUMN {col_name} {data_type}").collect()

        # Get column names from staging table (for Snowpark merge and primary key detection)
        staging = session.table(staging_table_fqn)
        columns = [c.name for c in staging.schema.fields]

        # Determine primary key:
        # 1. Check schema for explicit primary_key
        # 2. Fall back to _DLT_ID if present (dlt-generated for child tables)
        # 3. Fall back to ID if present
        primary_key = None
        table_schema = self.schema.tables.get(table_name.lower()) or self.schema.tables.get(table_name)
        if table_schema:
            pk = table_schema.get("primary_key")
            if pk:
                primary_key = pk[0].upper() if isinstance(pk, list) else pk.upper()

        if not primary_key:
            # Check actual columns for _DLT_ID or ID
            if "_DLT_ID" in columns:
                primary_key = "_DLT_ID"
            elif "ID" in columns:
                primary_key = "ID"
            else:
                # No primary key found - just insert all (no merge possible)
                session.sql(f"INSERT INTO {main_table_fqn} SELECT * FROM {staging_table_fqn}").collect()
                return

        # Use Snowpark's native merge
        main = session.table(main_table_fqn)
        try:
            main.merge(
                staging,
                main[primary_key] == staging[primary_key],
                [
                    when_matched().update({c: staging[c] for c in columns}),
                    when_not_matched().insert({c: staging[c] for c in columns})
                ]
            )
        except Exception:
            # Fallback to SQL MERGE if Snowpark merge fails
            # ALL BY NAME matches columns by name, ignoring column order
            # Missing columns in staging will be set to NULL in main
            session.sql(f"""
                MERGE INTO {main_table_fqn} m
                USING {staging_table_fqn} s
                ON m.{primary_key} = s.{primary_key}
                WHEN MATCHED THEN UPDATE ALL BY NAME
                WHEN NOT MATCHED THEN INSERT ALL BY NAME
            """).collect()

    # ========================================================================
    # WithStateSync Implementation
    # ========================================================================

    def get_stored_state(self, pipeline_name: str) -> Optional[StateInfo]:
        """Retrieve pipeline state from _dlt_pipeline_state table.

        Returns the compressed state string - dlt will decompress it internally.
        """
        try:
            schema = f"{self.database}.{self.dataset_name}"
            result = self.snowpark_session.sql(f"""
                SELECT version, engine_version, state, created_at, _dlt_load_id, version_hash
                FROM {schema}._DLT_PIPELINE_STATE
                WHERE pipeline_name = '{pipeline_name}'
                ORDER BY created_at DESC
                LIMIT 1
            """).collect()

            if not result:
                return None

            row = result[0]
            return StateInfo(
                version=int(row[0]) if row[0] else 1,
                engine_version=int(row[1]) if row[1] else 1,
                pipeline_name=pipeline_name,
                state=str(row[2]) if row[2] else "",
                created_at=row[3],  # Required field
                _dlt_load_id=row[4],
                version_hash=row[5]
            )
        except Exception:
            return None

    def get_stored_schema(self, schema_name: str = None) -> Optional[StorageSchemaInfo]:
        """Retrieve the latest schema version from _DLT_VERSION.

        Note: schema field must be a JSON string, not a dict.
        dlt's pipeline code does json.loads(schema_info.schema).
        """
        try:
            schema_fqn = f"{self.database}.{self.dataset_name}"
            result = self.snowpark_session.sql(f"""
                SELECT version, engine_version, inserted_at, schema_name, version_hash, schema
                FROM {schema_fqn}._DLT_VERSION
                ORDER BY inserted_at DESC
                LIMIT 1
            """).collect()

            if not result:
                return None

            row = result[0]
            # schema must be a JSON string - if Snowflake returns a dict (VARIANT), convert it
            schema_value = row[5]
            if isinstance(schema_value, dict):
                schema_str = json.dumps(schema_value)
            elif schema_value:
                schema_str = str(schema_value)
            else:
                schema_str = "{}"

            return StorageSchemaInfo(
                version_hash=row[4],
                schema_name=row[3] or schema_name or "jira",
                version=int(row[0]) if row[0] else 1,
                engine_version=int(row[1]) if row[1] else 1,
                inserted_at=row[2],
                schema=schema_str,
            )
        except Exception:
            return None

    def get_stored_schema_by_hash(self, version_hash: str) -> Optional[StorageSchemaInfo]:
        """Retrieve a specific schema version by its hash."""
        try:
            schema_fqn = f"{self.database}.{self.dataset_name}"
            result = self.snowpark_session.sql(f"""
                SELECT version, engine_version, inserted_at, schema_name, version_hash, schema
                FROM {schema_fqn}._DLT_VERSION
                WHERE version_hash = '{version_hash}'
                LIMIT 1
            """).collect()

            if not result:
                return None

            row = result[0]
            # schema must be a JSON string - if Snowflake returns a dict (VARIANT), convert it
            schema_value = row[5]
            if isinstance(schema_value, dict):
                schema_str = json.dumps(schema_value)
            elif schema_value:
                schema_str = str(schema_value)
            else:
                schema_str = "{}"

            return StorageSchemaInfo(
                version_hash=row[4],
                schema_name=row[3] or "jira",
                version=int(row[0]) if row[0] else 1,
                engine_version=int(row[1]) if row[1] else 1,
                inserted_at=row[2],
                schema=schema_str,
            )
        except Exception:
            return None

    def update_stored_schema(
        self,
        only_tables: set = None,
        expected_update: set = None,
    ) -> Optional[StorageSchemaInfo]:
        """Store the current schema in _dlt_version table.

        Called by dlt after schema verification to persist schema changes.
        Returns the stored schema info.
        """
        session = self.snowpark_session
        schema_fqn = f"{self.database}.{self.dataset_name}"

        # Serialize the schema to JSON
        schema_dict = self.schema.to_dict()
        schema_json = json.dumps(schema_dict).replace("'", "''")

        # Get current version
        try:
            result = session.sql(f"""
                SELECT COALESCE(MAX(version), 0) + 1 FROM {schema_fqn}._DLT_VERSION
            """).collect()
            new_version = result[0][0] if result else 1
        except Exception:
            new_version = 1

        # Insert new schema version using SELECT instead of VALUES (PARSE_JSON not allowed in VALUES)
        version_hash = self.schema.version_hash
        session.sql(f"""
            INSERT INTO {schema_fqn}._DLT_VERSION
            (version, engine_version, inserted_at, schema_name, version_hash, schema)
            SELECT {new_version}, 1, CURRENT_TIMESTAMP(), '{self.schema.name}', '{version_hash}', PARSE_JSON('{schema_json}')
        """).collect()

        from datetime import datetime, timezone
        return StorageSchemaInfo(
            version=new_version,
            engine_version=1,
            schema_name=self.schema.name,
            schema=schema_dict,
            version_hash=version_hash,
            inserted_at=datetime.now(timezone.utc)
        )


# ============================================================================
# Destination Configuration
# ============================================================================

@configspec
class SnowparkDestinationClientConfiguration(DestinationClientDwhConfiguration):
    """Configuration for Snowpark destination."""

    destination_type: str = dataclasses.field(default="snowpark")
    snowpark_session: Optional[Any] = dataclasses.field(default=None)
    database: str = dataclasses.field(default="RAW")
    dataset_name: Optional[str] = dataclasses.field(default=None)

    def __init__(
        self,
        snowpark_session: Any = None,
        database: str = "RAW",
        dataset_name: str = None,
        destination_name: str = "snowpark",
        environment: Optional[str] = None,
        **kwargs
    ):
        super().__init__(destination_name=destination_name, environment=environment, **kwargs)
        self.destination_type = "snowpark"
        self.snowpark_session = snowpark_session
        self.database = database
        self.dataset_name = dataset_name


# ============================================================================
# Destination Class
# ============================================================================

class snowpark(Destination[SnowparkDestinationClientConfiguration, SnowparkJobClient]):
    """Snowpark destination for dlt pipelines running in Snowflake stored procedures."""

    spec = SnowparkDestinationClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        """Return Snowflake-like capabilities."""
        caps = DestinationCapabilitiesContext.generic_capabilities()

        caps.preferred_loader_file_format = "parquet"
        caps.supported_loader_file_formats = ["parquet", "jsonl"]
        caps.preferred_staging_file_format = "parquet"
        caps.supported_staging_file_formats = ["parquet", "jsonl"]
        # No type_mapper - let Snowflake infer types from parquet

        caps.escape_identifier = lambda x: x.upper()  # No quotes - case insensitive
        caps.escape_literal = lambda x: f"'{str(x).replace(chr(39), chr(39)+chr(39))}'" if isinstance(x, str) else str(x)
        caps.casefold_identifier = str.upper
        caps.has_case_sensitive_identifiers = False

        caps.decimal_precision = (38, 9)  # Snowflake defaults
        caps.wei_precision = (38, 0)
        caps.max_identifier_length = 255
        caps.max_column_identifier_length = 255
        caps.max_query_length = 2 * 1024 * 1024
        caps.is_max_query_length_in_bytes = True
        caps.max_text_data_type_length = 16 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = True

        caps.supports_ddl_transactions = True
        caps.supports_transactions = True
        caps.supports_multiple_statements = True
        caps.timestamp_precision = 6
        caps.supports_truncate_command = True
        caps.supported_merge_strategies = ["upsert", "delete-insert", "scd2"]
        caps.supported_replace_strategies = ["truncate-and-insert", "insert-from-staging"]
        caps.max_timestamp_precision = 9

        return caps

    @property
    def client_class(self) -> Type[SnowparkJobClient]:
        return SnowparkJobClient

    def __init__(
        self,
        snowpark_session: Session,
        database: str = "RAW",
        destination_name: str = None,
        environment: str = None,
        **kwargs
    ) -> None:
        """
        Configure the Snowpark destination.

        Args:
            snowpark_session: Active Snowpark session (from stored procedure)
            database: Target database name
            destination_name: Name of the destination
            environment: Environment name
        """
        super().__init__(
            snowpark_session=snowpark_session,
            database=database,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )
