# dlt-snowpark: Custom dlt Destination for Snowflake Stored Procedures

A custom [dlt](https://dlthub.com/) destination that enables running dlt pipelines **inside Snowflake stored procedures** using the Snowpark Python API.

## Why This Exists

dlt (data load tool) is a powerful Python library for building data pipelines. While dlt claims to run "where Python runs," running it inside Snowflake stored procedures presents unique challenges:

1. **Read-only filesystem**: Snowflake procedures operate in a read-only environment, but dlt needs to write configuration, metadata, and state files
2. **Python connector incompatibility**: dlt's built-in Snowflake destination uses the Python connector, which fails with SQL compilation errors in Snowpark contexts

This custom destination solves these problems by:
- Using the **Snowpark session** directly (no Python connector needed)
- Writing pipeline state to **Snowflake tables** instead of the local filesystem
- Using `/tmp` for ephemeral file operations (the only writable location in stored procedures)

For more background, see: [Can you run dlt inside Snowflake?](https://www.sfrt.io/can-you-run-dlt-inside-snowflake/)

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     Snowflake Stored Procedure                          │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                     Your Pipeline Code                            │  │
│  │  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐    │  │
│  │  │ dlt Source  │───>│ dlt Pipeline│───>│ Snowpark Destination│    │  │
│  │  │ (REST API,  │    │             │    │                     │    │  │
│  │  │  SQL, etc.) │    │  Extract &  │    │  - PUT to stage     │    │  │
│  │  └─────────────┘    │  Normalize  │    │  - COPY INTO table  │    │  │
│  │                     └─────────────┘    │  - MERGE for upsert │    │  │
│  │                                        └─────────────────────┘    │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                    │                                    │
│                                    ▼                                    │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                      Target Schema                                │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌───────────────────────────┐  │  │
│  │  │ Data Tables │  │ _DLT_LOADS  │  │ _DLT_PIPELINE_STATE       │  │  │
│  │  │ (issues,    │  │ (load       │  │ (incremental cursors,     │  │  │
│  │  │  projects)  │  │  tracking)  │  │  schema versions)         │  │  │
│  │  └─────────────┘  └─────────────┘  └───────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Extract**: dlt source fetches data from external API (e.g., Jira REST API)
2. **Normalize**: dlt normalizes JSON into flat tables with proper typing
3. **Load**: Snowpark destination:
   - Writes parquet files to `/tmp` (ephemeral)
   - PUTs files to a temporary Snowflake stage
   - Uses `INFER_SCHEMA` + `CREATE TABLE USING TEMPLATE` for automatic table creation
   - `COPY INTO` loads data to staging tables
   - `MERGE` upserts from staging to main tables
4. **State Sync**: Pipeline state (incremental cursors) stored in `_DLT_PIPELINE_STATE` table

## Comparison: Snowpark Destination vs Built-in Snowflake Destination

| Feature | Built-in Snowflake | Snowpark Destination |
|---------|-------------------|---------------------|
| **Connection** | Python connector (external) | Snowpark session (internal) |
| **Runs in Stored Procedures** | No | Yes |
| **Runs synchronous** | Yes | Yes |
| **Runs asynchronous** | Yes | Yes |
| **Data Transfer** | Data travels twice: to the temporary storage of dlt and then to SNowflake | Data travels once: to the warehouse in Snowflake |
| **Authentication** | Requires credentials in config | Uses procedure's execution context |
| **File Format** | Parquet, JSONL | Parquet (recommended), JSONL |
| **Schema Inference** | dlt type mapping | Snowflake `INFER_SCHEMA` + dlt hints |
| **Merge Strategy** | SQL MERGE | Snowpark `Table.merge()` or SQL MERGE |
| **State Storage** | `_dlt_pipeline_state` table locally or in Snowflake | `_DLT_PIPELINE_STATE` table in Snowflake only |
| **Incremental Loading** | Full support | Full support via `WithStateSync` |

### Performance Considerations

The Snowpark destination can be **faster** for certain workloads because:
- **No network round-trip**: Data extracted from external APIs is loaded directly within Snowflake
- **No data egress**: Intermediate data stays inside Snowflake's compute layer
- **Snowpark optimizations**: Uses Snowflake's native merge operations
- **Starting hot**: A procedure, unlike a containe r, doesn't have to first start up

## Files

```
dlt/
├── snowpark_destination.py             # The custom dlt destination (~700 lines)
├── load_jira_as_snowflake_sproc.sql    # Example: Jira pipeline (~320 lines)jira_to_snowflake/
└── README.md                           # This file
```

### snowpark_destination.py

The core destination implementation with:
- `SnowparkLoadJob`: Handles PUT + COPY INTO for parquet files
- `SnowparkJobClient`: Implements `WithStateSync` for incremental loading
- `snowpark`: The destination class for `dlt.pipeline()`

### Example Stored Procedure

The Jira example demonstrates:
- Using dlt's REST API source with declarative configuration
- Incremental loading with JQL date filters
- Merge (upsert) for issues, replace for projects
- Automatic child table handling (e.g., `issues__changelog__histories`)

## Installation & Usage

### Prerequisites

1. Snowflake account with stored procedure support
2. External access integration for API calls (if loading from external sources)
3. Python UDF stage for the destination file

### Step 1: Upload the Destination

Upload `snowpark_destination.py` to a Snowflake stage:

```sql
-- Create stage for Python files
CREATE STAGE IF NOT EXISTS meta.python.s_python;

-- Upload via SnowSQL or Snowsight
PUT file://snowpark_destination.py @meta.python.s_python/dlt/;
```

### Step 2: Create the Stored Procedure

```sql
CREATE OR REPLACE PROCEDURE your_schema.p_load_data()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'load_data'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'dlt[parquet]', 'pyarrow')
IMPORTS = ('@meta.python.s_python/dlt/snowpark_destination.py')
EXTERNAL_ACCESS_INTEGRATIONS = (your_integration)  -- If calling external APIs
SECRETS = ('api_creds' = your_secret)              -- If needed
EXECUTE AS CALLER
AS
$$
import json
import dlt
from snowpark_destination import snowpark

@dlt.source(name="your_source")
def your_source():
    # Your source definition
    yield dlt.resource(...)

def load_data(snowpark_session):
    pipeline = dlt.pipeline(
        pipeline_name="your_pipeline",
        destination=snowpark(
            snowpark_session=snowpark_session,
            database="YOUR_DATABASE",
        ),
        dataset_name="your_dataset",
        pipelines_dir="/tmp/dlt_pipelines"  # Required: only writable location
    )

    load_info = pipeline.run(your_source(), loader_file_format="parquet")
    return json.dumps({"status": "success", "load_info": str(load_info)})
$$;
```

### Step 3: Run the Pipeline

```sql
CALL your_schema.p_load_data();
```

### Incremental Loading

The destination fully supports dlt's incremental loading. State is automatically persisted to `_DLT_PIPELINE_STATE`:

```sql
-- Check pipeline state
SELECT
    pipeline_name,
    created_at,
    version
FROM your_database.your_dataset._DLT_PIPELINE_STATE
ORDER BY created_at DESC;
```

### Force Full Reload

To reset state and reload all data:

```sql
-- Option 1: Truncate state table
TRUNCATE TABLE your_database.your_dataset._DLT_PIPELINE_STATE;

-- Option 2: Use force_full_load parameter (if your procedure supports it)
CALL your_schema.p_load_data('["endpoint"]', TRUE);
```

## Limitations

1. **dlt Version Dependency**: Tested with dlt v1.20.0. May require updates for newer versions.
2. **File Formats**: Parquet recommended. JSONL supported but less tested.
3. **Complex Types**: Deeply nested JSON may need explicit `data_type: "json"` hints in source config.
4. **Compute Resources**: Long-running pipelines may hit warehouse timeout limits.

## Troubleshooting

### "Read-only file system" Error
Ensure `pipelines_dir="/tmp/dlt_pipelines"` is set in `dlt.pipeline()`.

### State Not Persisting
Check that the destination can create tables in the target schema. The procedure needs appropriate privileges.

### Schema Mismatch Errors
For incremental loads after schema changes, consider a full reload or manually alter the target tables.

## Author

Created by Martin Seifert: [sfrt.io](https://www.sfrt.io/)

## Acknowledgments

- [dlt (data load tool)](https://dlthub.com/) - The excellent Python library this destination extends
- [Snowflake](https://www.snowflake.com/) - For Snowpark and stored procedure support
