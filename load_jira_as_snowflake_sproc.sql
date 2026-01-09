CREATE OR REPLACE PROCEDURE raw.jira.p_load_jira(
    target_database VARCHAR DEFAULT 'RAW',  -- Target database for dlt destination
    endpoints VARCHAR DEFAULT NULL,  -- Optional: JSON array of endpoint names, e.g., '["issues", "projects"]'
    debug_extract_only BOOLEAN DEFAULT FALSE,  -- If TRUE, only extract+normalize without loading (for debugging)
    force_full_load BOOLEAN DEFAULT FALSE  -- If TRUE, load all historical data from 1970-01-01 (ignores existing data)
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'load_jira_data'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python', 'requests', 'dlt[parquet]', 'pyarrow', 'pandas')
IMPORTS = ('@META.PYTHON.S_PYTHON/dlt/snowpark_destination.py')
COMMENT = 'Load Jira data using dlt pipeline - combines issues and projects endpoints'
EXTERNAL_ACCESS_INTEGRATIONS = (i_jira_dlt)
SECRETS = (
    'jira_subdomain' = meta.integration.se_jira_subdomain,
    'jira_creds' = meta.integration.se_jira
)
AS
$$
# ============================================================================
# Jira Data Load using dlt - Stored Procedure Implementation
# ============================================================================

from typing import List, Optional, Iterable, Any
import json
import _snowflake
import pyarrow as pa

import dlt
from dlt.common.typing import DictStrAny, TDataItem
from dlt.sources import DltResource
from dlt.sources.helpers import requests
from snowflake.snowpark import Session

# ============================================================================
# JIRA SOURCE SETTINGS
# ============================================================================

DEFAULT_ENDPOINTS = {
    "issues": {
        "data_path": "issues",
        "api_path": "rest/api/3/search/jql",
        "params": {
            "jql": "project is not EMPTY",
            "fields": "*all",
            "expand": "changelog",
        },
        "columns": {
            # Prevent unnesting of these custom fields - keep as JSON
            "fields__customfield_10003": {"data_type": "json"},
            "fields__customfield_10020": {"data_type": "json"},
            "fields__customfield_10021": {"data_type": "json"},
            "fields__customfield_10108": {"data_type": "json"},
            "fields__customfield_10109": {"data_type": "json"},
            "fields__customfield_10111": {"data_type": "json"},
            "fields__customfield_10112": {"data_type": "json"},
            "fields__customfield_10118": {"data_type": "json"},
            "fields__customfield_10125": {"data_type": "json"},
            "fields__customfield_10169": {"data_type": "json"},
            "fields__customfield_10188": {"data_type": "json"},
            "fields__customfield_10193": {"data_type": "json"},
            "fields__customfield_10196": {"data_type": "json"},
            "fields__customfield_10218": {"data_type": "json"},
            "fields__customfield_10230": {"data_type": "json"},
            "fields__customfield_10240": {"data_type": "json"},
            "fields__customfield_10244": {"data_type": "json"},
            "fields__customfield_10245": {"data_type": "json"},
            "fields__customfield_10262": {"data_type": "json"},
            "fields__customfield_10263": {"data_type": "json"}
        },
        "max_table_nesting": 3  # Unnest comments, changelog, etc.
    },
    "projects": {
        "data_path": "values",
        "api_path": "rest/api/3/project/search",
        "params": {
            "expand": "description,lead,issueTypes,url,projectKeys,permissions,insight"
        },
        "max_table_nesting": 0  # Don't create any child tables for projects
    },
}
DEFAULT_PAGE_SIZE = 50


# ============================================================================
# JIRA SOURCE IMPLEMENTATION
# ============================================================================

def get_paginated_data(
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int,
    api_path: str = "rest/api/2/search",
    data_path: Optional[str] = None,
    params: Optional[DictStrAny] = None,
) -> Iterable[TDataItem]:
    """Fetch paginated data from a Jira API endpoint."""
    url = f"https://{subdomain}.atlassian.net/{api_path}"
    headers = {"Accept": "application/json"}
    auth = (email, api_token)
    params = {} if params is None else params
    params["maxResults"] = page_size

    if "startAt" not in params:
        params["startAt"] = 0

    while True:
        response = requests.get(url, auth=auth, headers=headers, params=params)
        response.raise_for_status()
        result = response.json()

        if isinstance(result, list):
            if len(result) > 0:
                yield result
            break

        if data_path:
            results_page = result.pop(data_path)
        else:
            results_page = result

        if len(results_page) == 0:
            break

        yield results_page

        if "nextPageToken" in result or "isLast" in result:
            if result.get("isLast", False):
                break
            next_page_token = result.get("nextPageToken")
            if not next_page_token:
                break
            params["nextPageToken"] = next_page_token
        else:
            params["startAt"] += len(results_page)


def get_issues_incremental(
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int,
    updated_at: dlt.sources.incremental[str] = dlt.sources.incremental(
        "fields.updated", initial_value="1970-01-01"
    ),
) -> Iterable[TDataItem]:
    """Fetch JIRA issues incrementally based on the updated timestamp."""
    endpoint_config = DEFAULT_ENDPOINTS["issues"]
    api_path = endpoint_config["api_path"]
    base_params = endpoint_config.get("params", {}).copy()

    base_jql = base_params.get("jql", "project is not EMPTY")

    last_value = updated_at.last_value
    if "T" in last_value:
        jql_date = last_value.split("T")[0]
    else:
        jql_date = last_value

    incremental_jql = f"{base_jql} AND updated >= '{jql_date}' ORDER BY updated ASC"
    base_params["jql"] = incremental_jql

    yield from get_paginated_data(
        subdomain=subdomain,
        email=email,
        api_token=api_token,
        page_size=page_size,
        api_path=api_path,
        data_path=endpoint_config.get("data_path"),
        params=base_params,
    )


@dlt.source(max_table_nesting=2)
def jira(
    subdomain: str,
    email: str,
    api_token: str,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> Iterable[DltResource]:
    """Jira source function that generates a list of resource functions based on endpoints."""
    resources = []
    for endpoint_name, endpoint_parameters in DEFAULT_ENDPOINTS.items():
        if endpoint_name == "issues":
            resource_kwargs = {
                "name": endpoint_name,
                "write_disposition": "merge",
                "primary_key": "id"
            }
            if "max_table_nesting" in endpoint_parameters:
                resource_kwargs["max_table_nesting"] = endpoint_parameters["max_table_nesting"]
            if "columns" in endpoint_parameters:
                resource_kwargs["columns"] = endpoint_parameters["columns"]

            res_function = dlt.resource(
                get_issues_incremental,
                **resource_kwargs
            )(
                subdomain=subdomain,
                email=email,
                api_token=api_token,
                page_size=page_size,
            )
        else:
            resource_kwargs = {
                "name": endpoint_name,
                "write_disposition": "replace"
            }
            if "max_table_nesting" in endpoint_parameters:
                resource_kwargs["max_table_nesting"] = endpoint_parameters["max_table_nesting"]

            function_params = {k: v for k, v in endpoint_parameters.items()
                             if k not in ["max_table_nesting", "columns", "table_name_hint"]}

            res_function = dlt.resource(
                get_paginated_data,
                **resource_kwargs
            )(
                **function_params,
                subdomain=subdomain,
                email=email,
                api_token=api_token,
                page_size=page_size,
            )
        resources.append(res_function)

    return resources

# ============================================================================
# STORED PROCEDURE MAIN FUNCTION
# ============================================================================

def load_jira_data(snowpark_session, target_database: str = 'RAW', endpoints: Optional[str] = None, debug_extract_only: bool = False, force_full_load: bool = False) -> str:
    """
    Main stored procedure function to load Jira data using dlt with Snowpark destination.

    Args:
        snowpark_session: Snowpark session object (automatically provided)
        target_database: Target database for dlt destination (default: 'RAW')
        endpoints: Optional JSON string of endpoint names. If None, loads all.
        debug_extract_only: If True, only extract+normalize without loading (for debugging)
        force_full_load: If True, load all historical data from 1970-01-01 (ignores existing data)

    Returns:
        JSON string with load information and status
    """
    try:
        import traceback

        # Retrieve Jira secrets from Snowflake
        jira_subdomain = _snowflake.get_generic_secret_string('jira_subdomain')
        jira_creds = _snowflake.get_username_password('jira_creds')
        jira_username = jira_creds.username
        jira_api_token = jira_api_token = jira_creds.password

        # Parse endpoints if provided
        endpoint_list = None
        if endpoints:
            endpoint_list = json.loads(endpoints)

        if not endpoint_list:
            endpoint_list = list(DEFAULT_ENDPOINTS.keys())

        # =====================================================================
        # RUN PIPELINE WITH SNOWPARK DESTINATION (just like normal dlt!)
        # =====================================================================
        from snowpark_destination import snowpark

        # Check if we have existing data to determine the min date for incremental loading
        initial_date = "1970-01-01"  # Default for first run or force_full_load
        if "issues" in endpoint_list and not force_full_load:
            try:
                max_updated_query = "SELECT MAX(fields__updated) as max_updated FROM RAW.JIRA.ISSUES"
                max_updated_df = snowpark_session.sql(max_updated_query).collect()
                max_updated = max_updated_df[0][0] if (max_updated_df and max_updated_df[0][0]) else None

                if max_updated:
                    # Extract just the date part (YYYY-MM-DD) from the timestamp
                    last_updated = str(max_updated)
                    if 'T' in last_updated:
                        initial_date = last_updated.split('T')[0]
                    elif ' ' in last_updated:
                        initial_date = last_updated.split(' ')[0]
                    else:
                        initial_date = last_updated[:10]  # First 10 chars should be YYYY-MM-DD
            except:
                pass  # Use default

        # Create Jira source with credentials
        source = jira(
            subdomain=jira_subdomain,
            email=jira_username,
            api_token=jira_api_token,
        ).with_resources(*endpoint_list)

        # CRITICAL: Apply hints to override the incremental initial_value with our calculated date
        # This tells the issues resource to start from initial_date instead of 1970-01-01
        if "issues" in endpoint_list and initial_date != "1970-01-01":
            source.issues.apply_hints(incremental=dlt.sources.incremental("fields.updated", initial_value=initial_date))

        # =====================================================================
        # TWO-STAGE APPROACH: Extract+Normalize first, then Load once
        # =====================================================================
        # This is how dlt's built-in destinations work:
        # 1. extract() - Extract data from source
        # 2. normalize() - Normalize to parquet files in /tmp
        # 3. load() - Bulk load all normalized files to destination in ONE operation
        #
        # This avoids the incremental loading problem where each extraction batch
        # triggers a separate merge operation

        import os
        import shutil
        import time

        # Clean up from previous runs BEFORE creating pipeline
        # Remove old pipeline directories to force fresh state
        try:
            if os.path.exists("/tmp/dlt_pipelines"):
                shutil.rmtree("/tmp/dlt_pipelines")
        except:
            pass

        # Drop and recreate staging schema as TRANSIENT
        # TRANSIENT = no fail-safe, reduced time travel (perfect for staging data)
        try:
            snowpark_session.sql(f"DROP SCHEMA IF EXISTS {target_database}.JIRA_STAGING CASCADE").collect()
            time.sleep(1)  # Give Snowflake a moment to complete the drop
            snowpark_session.sql(f"CREATE TRANSIENT SCHEMA {target_database}.JIRA_STAGING").collect()
        except:
            pass

        # CRITICAL: Clear dlt pipeline state to prevent re-loading old pending packages
        # Each failed run leaves pending packages that get re-processed on next run
        # This causes the merge count to grow (4 → 6 → 7 → ...)
        try:
            snowpark_session.sql(f"DELETE FROM {target_database}.JIRA._DLT_PIPELINE_STATE WHERE pipeline_name = 'jira_snowpark'").collect()
        except:
            pass  # Ignore if table doesn't exist yet

        # CRITICAL: Configure dlt to use VERY large buffers to create ONE load package
        # This prevents multiple load packages which would cause dozens of merge statements
        # Set buffer size to essentially unlimited to accumulate ALL data before creating package
        import os as _os
        _os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "1000000"  # 1 million items per buffer
        _os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "1000000"    # 1 million items per file
        _os.environ["NORMALIZE__LOADER_FILE_FORMAT"] = "jsonl"    # Use JSONL instead of parquet for simplicity

        # Now create fresh pipeline
        pipeline = dlt.pipeline(
            pipeline_name="jira_snowpark",
            destination=snowpark(snowpark_session=snowpark_session, database=target_database),
            dataset_name="jira",
            pipelines_dir="/tmp/dlt_pipelines",
            full_refresh=False
        )

        # CRITICAL: Drop any pending packages from previous runs
        # This prevents dlt from re-loading old packages which would cause multiple MERGEs
        try:
            pipeline.drop_pending_packages()
        except:
            pass  # Ignore if no pending packages

        # DEBUG: Clear debug log file before running
        try:
            with open('/tmp/dlt_sql_execution_log.txt', 'w') as log:
                log.write(f"=== SQL Execution Log for {endpoint_list} ===\n")
        except:
            pass

        # CRITICAL: Use pipeline.run() instead of extract/normalize/load separately
        # This ensures dlt creates packages optimally without multiple packages per table
        # The three-stage approach was causing multiple packages even with workers=1

        # Wrap in try/except so we can return debug info even if pipeline fails
        pipeline_error = None
        timeout_occurred = False

        try:
            if debug_extract_only:
                # DEBUG MODE: Only extract and normalize, don't load
                # This lets us see how many SQL files were created without executing them
                pipeline.extract(source)
                normalize_info = pipeline.normalize()

                # Count SQL files generated
                import os as _count_os
                sql_files = []
                load_path = "/tmp/dlt_pipelines/jira_snowpark/load"
                if _count_os.path.exists(load_path):
                    for load_id_dir in _count_os.listdir(load_path):
                        load_id_path = _count_os.path.join(load_path, load_id_dir)
                        if _count_os.path.isdir(load_id_path):
                            for file in _count_os.listdir(load_id_path):
                                if file.endswith('.sql'):
                                    sql_files.append(f"{load_id_dir}/{file}")

                # Write to log
                try:
                    with open('/tmp/dlt_sql_execution_log.txt', 'a') as log:
                        log.write(f"Found {len(sql_files)} SQL files after normalize:\n")
                        for f in sql_files[:20]:  # First 20 files
                            log.write(f"  - {f}\n")
                        if len(sql_files) > 20:
                            log.write(f"  ... and {len(sql_files) - 20} more\n")
                except:
                    pass

                load_info = None
            else:
                # Run the pipeline normally
                # The emergency stop in snowpark_destination.py will raise an exception
                # after processing the first ISSUES SQL file
                load_info = pipeline.run(source, loader_file_format="parquet")

        except Exception as e:
            pipeline_error = str(e)
            load_info = None  # Set to None so we can still return debug info

        # DEBUG: Read SQL execution log
        sql_execution_log = "Log file not found"
        try:
            with open('/tmp/dlt_sql_execution_log.txt', 'r') as log:
                sql_execution_log = log.read()
        except:
            pass

        # If pipeline failed, return error with debug info
        if pipeline_error:
            # Check if it's our debug stop
            is_debug_stop = "DEBUG: Stopped after" in pipeline_error
            return json.dumps({
                "status": "debug_stop" if is_debug_stop else "error",
                "error": pipeline_error[:1000],  # Show more of the error message
                "debug_info": {
                    "sql_execution_log": sql_execution_log,
                    "emergency_stop_triggered": is_debug_stop
                }
            })

        debug_info = {
            "load_packages_count": len(load_info.loads_ids) if load_info and hasattr(load_info, 'loads_ids') else "unknown",
            "load_ids": str(load_info.loads_ids) if load_info and hasattr(load_info, 'loads_ids') else "unknown",
            "sql_execution_log": sql_execution_log,
            "debug_mode": "extract_only" if debug_extract_only else "full_run"
        }

        # Prepare result
        result = {
            "status": "success",
            "pipeline_name": "jira_snowpark",
            "dataset_name": "jira",
            "destination": "snowpark_custom",
            "database": target_database,
            "debug_info": debug_info,
            "schema": "jira",
            "endpoints_loaded": endpoint_list,
            "load_info": {
                "dataset_name": load_info.dataset_name if load_info else "N/A (debug mode)",
                "started_at": str(load_info.started_at) if load_info and load_info.started_at else None,
                "finished_at": str(load_info.finished_at) if load_info and load_info.finished_at else None,
                "jobs": len(load_info.jobs) if load_info and hasattr(load_info, 'jobs') else 0,
            },
            "incremental_config": {
                "initial_date_used": initial_date if "issues" in endpoint_list else "N/A",
                "note": "Loading issues updated since this date" if initial_date != "1970-01-01" else "First run - loading all historical data"
            },
            "note": "Using custom Snowpark destination - no Snowflake connector needed!"
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        error_result = {
            "status": "error",
            "error_type": type(e).__name__,
            "error_message": str(e),
            "traceback": traceback.format_exc(),
            "pipeline_name": "jira_snowpark",
            "note": "Error occurred with Snowpark custom destination"
        }
        return json.dumps(error_result, indent=2)
$$;


-- Test call (uncomment to run):
CALL raw.jira.p_load_jira('RAW', '["projects"]');
CALL raw.jira.p_load_jira('RAW', '["issues"]', FALSE, TRUE);