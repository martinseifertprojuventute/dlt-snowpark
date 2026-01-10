# Running dlt Inside Snowflake with Custom Snowpark Destination

A production-ready implementation of dlt (data load tool) running inside Snowflake stored procedures using a custom Snowpark destination.

> **Context**: This project addresses the fundamental incompatibility between dlt's built-in Snowflake destination and Snowflake's stored procedure environment. See the detailed analysis in [Can You Run dlt Inside Snowflake?](https://www.sfrt.io/can-you-run-dlt-inside-snowflake/)

## Table of Contents
- [Why a Custom Snowpark Destination?](#why-a-custom-snowpark-destination)
- [Architecture Overview](#architecture-overview)
- [Key Differences from Built-in Destination](#key-differences-from-built-in-destination)
- [Files Overview](#files-overview)
- [Installation & Setup](#installation--setup)
- [Usage](#usage)
- [Performance](#performance)
- [Troubleshooting](#troubleshooting)

---

## Why a Custom Snowpark Destination?

### The Problem

dlt's built-in Snowflake destination **cannot run inside Snowflake stored procedures** due to two fundamental incompatibilities:

1. **Snowflake Python Connector API Unavailable**
   - The built-in destination uses `snowflake.connector` API
   - This API is not available inside stored procedures
   - Snowpark API must be used instead

2. **SQL Parameter Binding Issues**
   - The built-in destination uses parameterized SQL queries
   - Inside stored procedures, this creates SQL compilation errors
   - Direct SQL execution is required

### The Solution

Our **custom Snowpark destination** (`snowpark_destination.py`) replaces the built-in destination while maintaining compatibility with dlt's pipeline framework. It uses:
- ✅ Snowpark Session API (available in stored procedures)
- ✅ Direct SQL execution (no parameter binding)
- ✅ PUT + COPY INTO for bulk loading (Snowflake-optimized)
- ✅ Native MERGE statements for upsert operations

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│ Snowflake Stored Procedure (load_jira_as_snowflake_sproc)  │
│                                                             │
│  ┌──────────────┐      ┌────────────────┐                 │
│  │ Jira Source  │─────▶│ dlt Pipeline   │                 │
│  │ (REST API)   │      │ (extract/norm) │                 │
│  └──────────────┘      └────────┬───────┘                 │
│                                 │                          │
│                                 ▼                          │
│                    ┌─────────────────────────┐            │
│                    │ Snowpark Destination    │            │
│                    │ (snowpark_destination)  │            │
│                    └──────────┬──────────────┘            │
│                               │                           │
│         ┌─────────────────────┼─────────────────────┐    │
│         ▼                     ▼                     ▼    │
│  ┌──────────────┐   ┌─────────────────┐   ┌───────────┐ │
│  │ PUT + COPY   │   │ CREATE TABLE    │   │ MERGE     │ │
│  │ (Parquet)    │   │ (Schema mgmt)   │   │ (Upsert)  │ │
│  └──────┬───────┘   └────────┬────────┘   └─────┬─────┘ │
│         │                    │                   │       │
└─────────┼────────────────────┼───────────────────┼───────┘
          │                    │                   │
          ▼                    ▼                   ▼
    ┌──────────────────────────────────────────────────┐
    │           Snowflake Database                     │
    │  ┌──────────────────┐      ┌─────────────────┐  │
    │  │ JIRA_STAGING     │─────▶│ JIRA (Main)     │  │
    │  │ (Transient)      │      │ (Production)    │  │
    │  └──────────────────┘      └─────────────────┘  │
    └──────────────────────────────────────────────────┘
```

### Data Flow

1. **Extract**: Jira source pulls data from REST API
2. **Normalize**: dlt converts to Parquet files in memory
3. **Stage Load**: `PUT` files to internal stage → `COPY INTO` staging tables
4. **Merge**: Native Snowflake `MERGE` statements upsert to production tables

---

## Key Differences from Built-in Destination

### Comparison Table

| Aspect | Built-in Snowflake Destination | Custom Snowpark Destination |
|--------|-------------------------------|----------------------------|
| **Execution Environment** | External (local/cloud VM) | Inside Snowflake stored procedures |
| **API Used** | `snowflake.connector` | `snowflake.snowpark.Session` |
| **Connection** | Creates new connection | Uses provided session |
| **SQL Execution** | Parameterized queries (`cursor.execute()`) | Direct SQL (`session.sql().collect()`) |
| **Temporary Tables** | `CREATE TEMPORARY TABLE` | `CREATE TABLE` (temp not supported) |
| **User Stages** | `@~` user stage | Named internal stages (`@db.schema.stage_xxx`) |
| **Load Job Classes** | Separate classes for parquet vs SQL | Single `SnowparkLoadJob` handles both |
| **Followup Jobs** | `SnowflakeLoadJob` has `HasFollowupJobs` | Only client has `HasFollowupJobs` |
| **State Storage** | Local filesystem + database | Database only |
| **Performance** | ~2-5 minutes (external overhead) | ~30 seconds (no network latency) |

### Architectural Differences

#### 1. **Job Class Inheritance**

**Built-in Snowflake:**
```python
class SnowflakeLoadJob(RunnableLoadJob, HasFollowupJobs):
    # Handles only data files (parquet, JSONL)
    # Creates followup merge jobs
```

**Custom Snowpark:**
```python
class SnowparkLoadJob(RunnableLoadJob):
    # Handles BOTH data files AND SQL merge files
    # Does NOT create followup jobs (prevents infinite loops)
```

**Why?** In the built-in destination, SQL merge jobs are separate job types. In our custom destination, one unified job class handles everything. To prevent SQL jobs from creating more SQL jobs (infinite loop), we removed `HasFollowupJobs` from the job class and kept it only on the client.

#### 2. **Staging Strategy**

**Built-in Snowflake:**
- Uses user stage (`@~`) for PUT operations
- Can create temporary tables
- Each connection manages its own temp resources

**Custom Snowpark:**
- Uses named internal stages (`@db.schema.dlt_stage_xxx`)
- Cannot use temporary tables in stored procedures
- Must clean up stages explicitly

#### 3. **SQL Execution**

**Built-in Snowflake:**
```python
cursor.execute("INSERT INTO table VALUES (?)", (value,))
```

**Custom Snowpark:**
```python
session.sql("INSERT INTO table VALUES ('value')").collect()
```

**Why?** Snowpark doesn't support parameter binding in the same way. Direct SQL string formatting is required.

#### 4. **Bulk Loading**

Both use the same approach: **PUT + COPY INTO**

```python
# 1. PUT file to stage
session.file.put(local_file, '@stage_name')

# 2. COPY INTO table
session.sql(f"""
    COPY INTO {table}
    FROM @{stage}
    FILE_FORMAT = (TYPE = 'PARQUET')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
""").collect()
```

This is **identical performance** to the built-in destination's approach.

#### 5. **Merge Strategy**

Both use **dlt's built-in `SqlMergeFollowupJob`** which generates native Snowflake MERGE statements:

```python
def _create_merge_followup_jobs(self, table_chain):
    return [SqlMergeFollowupJob.from_table_chain(table_chain, self.sql_client)]
```

**Result:** Identical merge logic and performance.

---

## Files Overview

### 1. `snowpark_destination.py`
The core custom destination implementation.

**Key Classes:**

- **`SnowparkSqlClient`**: SQL client using Snowpark Session API
  - Executes DDL (CREATE TABLE, ALTER TABLE, etc.)
  - Manages schema creation
  - Handles reserved keyword quoting

- **`SnowparkLoadJob`**: Unified job executor
  - Loads Parquet files via PUT + COPY INTO
  - Executes SQL merge files
  - Handles both staging and production writes

- **`SnowparkJobClient`**: Main client coordinator
  - Creates load jobs
  - Manages merge followup job creation
  - Coordinates staging dataset operations

- **`snowpark`**: Destination factory
  - Entry point for dlt pipeline
  - Configures capabilities (parquet, merge strategies, etc.)

**Key Methods:**

```python
# Bulk load parquet files
def run(self):  # SnowparkLoadJob
    if file.endswith('.parquet'):
        # PUT + COPY INTO
        stage = create_internal_stage()
        session.file.put(file, stage)
        session.sql(f"COPY INTO {table} FROM @{stage}").collect()
    elif file.endswith('.sql'):
        # Execute merge statements
        session.sql(sql_content).collect()
```

### 2. `load_jira_as_snowflake_sproc.sql`
Snowflake stored procedure that orchestrates the dlt pipeline.

**Parameters:**

- `target_database` (VARCHAR): Target database (default: 'RAW')
- `endpoints` (VARCHAR): JSON array of endpoints (e.g., `'["issues", "projects"]'`)
- `force_full_load` (BOOLEAN): Load all historical data (default: FALSE)

**Key Features:**

- **Incremental Loading**: Tracks last updated timestamp
- **Secret Management**: Retrieves Jira credentials from Snowflake secrets
- **Error Handling**: Returns structured JSON with errors and stack traces
- **Flexible Endpoints**: Load specific endpoints or all available

**Flow:**

```python
def load_jira_data(snowpark_session, target_database, endpoints, force_full_load):
    # 1. Get Jira credentials from Snowflake secrets
    jira_subdomain = _snowflake.get_generic_secret_string('jira_subdomain')
    jira_creds = _snowflake.get_username_password('jira_creds')

    # 2. Determine incremental starting point
    if force_full_load:
        initial_date = "1970-01-01"
    else:
        initial_date = max(fields__updated) from existing data

    # 3. Create dlt pipeline with Snowpark destination
    pipeline = dlt.pipeline(
        pipeline_name="jira_snowpark",
        destination=snowpark(snowpark_session, database),
        dataset_name="jira"
    )

    # 4. Run pipeline
    load_info = pipeline.run(source, loader_file_format="parquet")

    # 5. Return JSON result
    return {status, load_info, incremental_config, ...}
```

---

## Installation & Setup

### Prerequisites

1. **Snowflake Account** with:
   - Python 3.11 runtime support
   - External access integration for Jira API
   - Artifact repository for custom packages

2. **Jira Credentials**:
   - Jira subdomain (e.g., `yourcompany` for `yourcompany.atlassian.net`)
   - Email address
   - API token

### Step 1: Create Snowflake Secrets

```sql
-- Create secret for Jira subdomain
CREATE SECRET raw.jira.jira_subdomain
TYPE = GENERIC_STRING
SECRET_STRING = 'yourcompany';

-- Create secret for Jira credentials
CREATE SECRET raw.jira.jira_creds
TYPE = PASSWORD
USERNAME = 'your-email@company.com'
PASSWORD = 'your-jira-api-token';
```

### Step 2: Create External Access Integration

```sql
CREATE EXTERNAL ACCESS INTEGRATION i_jira_dlt
ALLOWED_NETWORK_RULES = (
    -- Jira API
    RULE_JIRA_API,
    -- dlt package indexes (if using custom packages)
    RULE_PYPI
)
ALLOWED_AUTHENTICATION_SECRETS = (
    raw.jira.jira_subdomain,
    raw.jira.jira_creds
)
ENABLED = TRUE;
```

### Step 3: Upload Custom Destination

```sql
-- Create stage for custom code
CREATE STAGE meta.python.s_python
DIRECTORY = (ENABLE = TRUE)
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- Upload snowpark_destination.py
PUT file://c:\Users\Martin\git\dataanalytics\dlt\snowpark_destination.py
@meta.python.s_python/dlt/
AUTO_COMPRESS = FALSE
OVERWRITE = TRUE;
```

### Step 4: Create Stored Procedure

```sql
-- Execute the entire load_jira_as_snowflake_sproc.sql file
-- This creates the procedure: raw.jira.p_load_jira
```

---

## Usage

### Basic Usage

```sql
-- Load issues with incremental updates (loads only new/updated issues)
CALL raw.jira.p_load_jira('RAW', '["issues"]', FALSE);

-- Load projects
CALL raw.jira.p_load_jira('RAW', '["projects"]', FALSE);

-- Load all available endpoints
CALL raw.jira.p_load_jira('RAW', NULL, FALSE);
```

### Full Historical Load

```sql
-- Force full historical load (ignores existing data, loads from 1970-01-01)
CALL raw.jira.p_load_jira('RAW', '["issues"]', TRUE);
```

### Result Structure

```json
{
  "status": "success",
  "pipeline_name": "jira_snowpark",
  "dataset_name": "jira",
  "destination": "snowpark",
  "database": "RAW",
  "schema": "jira",
  "endpoints_loaded": ["issues"],
  "load_info": {
    "dataset_name": "jira",
    "started_at": "2026-01-09 21:33:14.596371+00:00",
    "finished_at": "2026-01-09 21:33:43.970105+00:00",
    "total_jobs": 5,
    "packages": 1,
    "jobs_detail": [
      {"file_name": "issues.123.parquet", "status": "completed"},
      {"file_name": "issues_merge.sql", "status": "completed"}
    ]
  },
  "incremental_config": {
    "initial_date_used": "2026-01-09",
    "note": "Loading issues updated since this date"
  }
}
```

### Scheduling

```sql
-- Create task to run daily at 2 AM
CREATE TASK raw.jira.t_load_jira_daily
WAREHOUSE = compute_wh
SCHEDULE = 'USING CRON 0 2 * * * America/New_York'
AS
CALL raw.jira.p_load_jira('RAW', '["issues", "projects"]', FALSE);

-- Enable task
ALTER TASK raw.jira.t_load_jira_daily RESUME;
```

---

## Performance

### Benchmarks

| Metric | Built-in Snowflake (External) | Custom Snowpark (Internal) |
|--------|-------------------------------|---------------------------|
| 50 issues | ~60-90 seconds | ~28 seconds |
| 22,767 issues | ~25-30 minutes | ~21 minutes |
| Network latency | High (data transfer) | None (in-database) |
| Cold start | Slow (VM startup) | Fast (instant) |
| Staging load | PUT + COPY (fast) | PUT + COPY (fast) |
| Merge operations | Native MERGE | Native MERGE (identical) |

### Performance Notes

1. **Bulk Loading**: Identical performance (both use PUT + COPY INTO)
2. **Merge**: Identical performance (both use native MERGE statements)
3. **Overhead**: Snowpark destination eliminates network transfer time
4. **Scalability**: Both scale linearly with data volume

### Optimization Tips

1. **Use Parquet format** (default): More efficient than JSONL
2. **Batch incremental loads**: Load daily rather than real-time
3. **Limit endpoints**: Only load what you need
4. **Use appropriate warehouse**: Scale up for large historical loads

---

## Troubleshooting

### Common Issues

#### 1. "User stage not allowed in stored procedures"

**Error:**
```
RuntimeError: User stage file access is not allowed within an owner's rights SP
```

**Solution:** This is already fixed in the code. We use named internal stages instead of user stages.

#### 2. "CREATE TEMPORARY TABLE not supported"

**Error:**
```
Unsupported statement type 'temporary STAGE'
```

**Solution:** This is already fixed. We use regular `CREATE TABLE` and `CREATE STAGE` instead of temporary variants.

#### 3. Data in staging but not in main table

**Symptoms:**
- `JIRA_STAGING.ISSUES` has data
- `JIRA.ISSUES` is empty or outdated

**Debug Steps:**

```sql
-- Check load status
SELECT * FROM RAW.JIRA._DLT_LOADS
ORDER BY inserted_at DESC LIMIT 5;

-- Check job status
SELECT load_id, file_path, status, error
FROM RAW.JIRA._DLT_LOAD_JOBS
ORDER BY started_at DESC LIMIT 20;

-- Look for SQL merge files
SELECT file_path FROM RAW.JIRA._DLT_LOAD_JOBS
WHERE file_path LIKE '%.sql%'
ORDER BY started_at DESC;
```

**Possible causes:**
- Merge jobs not being created
- Merge jobs failing silently
- Write disposition not set to "merge"

#### 4. Performance degradation

**Symptoms:** Loads taking longer than expected

**Debug:**
```sql
-- Check warehouse size
SHOW WAREHOUSES LIKE 'your_warehouse';

-- Check query history for slow operations
SELECT query_text, execution_time, warehouse_size
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_text LIKE '%JIRA%'
ORDER BY start_time DESC
LIMIT 20;
```

**Solutions:**
- Scale up warehouse for large loads
- Use `force_full_load=FALSE` for incremental updates
- Check for table clustering/partitioning opportunities

---

## Advanced Topics

### Custom Endpoints

To add new Jira endpoints, modify the source configuration:

```python
# In load_jira_as_snowflake_sproc.sql
DEFAULT_ENDPOINTS = {
    "issues": {...},
    "projects": {...},
    # Add your custom endpoint:
    "custom_endpoint": {
        "resource": "custom",
        "table_name": "custom_data"
    }
}
```

### Schema Customization

The destination automatically creates schemas:
- **Main schema**: `{database}.JIRA` (production data)
- **Staging schema**: `{database}.JIRA_STAGING` (temporary staging)

To customize:

```python
# Modify in snowpark_destination.py
dataset_name = "jira"  # Change to your preferred schema name
```

### Error Handling

All errors return structured JSON:

```json
{
  "status": "error",
  "error_type": "ValueError",
  "error_message": "Invalid endpoint specified",
  "traceback": "...",
  "pipeline_name": "jira_snowpark",
  "note": "Error occurred with Snowpark custom destination"
}
```

---

## Best Practices

1. **Incremental Loading**: Always use incremental mode for regular updates
2. **Full Loads**: Reserve `force_full_load=TRUE` for initial setup or recovery
3. **Monitoring**: Set up alerts on `_DLT_LOADS` table for failures
4. **Testing**: Test with small endpoint lists first (`["projects"]`)
5. **Warehouse Sizing**: Use larger warehouses for historical loads, smaller for incrementals
6. **Secret Rotation**: Rotate Jira API tokens regularly
7. **Schema Evolution**: Let dlt handle schema changes automatically

---

## Limitations

1. **Snowpark API Constraints**:
   - No temporary tables in stored procedures
   - No user stage access
   - Limited error handling compared to external execution

2. **dlt Features**:
   - State management limited to database (no local filesystem)
   - Cannot use certain dlt features requiring file system access
   - Pipeline metadata stored only in database

3. **Performance**:
   - Large historical loads (millions of rows) may timeout
   - Stored procedure execution limits apply (e.g., max memory, max execution time)

---

## References

- [dlt Documentation](https://dlthub.com/docs)
- [Snowpark Python API](https://docs.snowflake.com/en/developer-guide/snowpark/python/index)
- [Original Article: Can You Run dlt Inside Snowflake?](https://www.sfrt.io/can-you-run-dlt-inside-snowflake/)
- [dlt Snowflake Destination](https://dlthub.com/docs/dlt-ecosystem/destinations/snowflake)

---

## License

This implementation is provided as-is for educational and production use. Modify as needed for your environment.

---

## Contributing

Found an issue or improvement? This is part of an ongoing exploration of running dlt in constrained environments. See the [original article](https://www.sfrt.io/can-you-run-dlt-inside-snowflake/) for context.

---

**Last Updated**: January 2026
**dlt Version**: 1.4.1
**Snowflake Python Runtime**: 3.11
