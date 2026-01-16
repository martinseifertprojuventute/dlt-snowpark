CREATE OR REPLACE PROCEDURE raw.jira.p_load_jira(
    endpoints VARCHAR DEFAULT NULL,  -- Optional: JSON array of endpoint names, e.g., '["issues", "projects"]'
    force_full_load BOOLEAN DEFAULT FALSE  -- If TRUE, load all historical data from 1970-01-01
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'load_jira_data'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python', 'requests', 'dlt[parquet]', 'pyarrow')
IMPORTS = ('@META.PYTHON.S_PYTHON/dlt/snowpark_destination.py')
COMMENT = 'Load Jira data using dlt pipeline with REST API source - simplified v2'
EXTERNAL_ACCESS_INTEGRATIONS = (i_jira_dlt)
SECRETS = (
    'jira_creds' = meta.integration.se_jira
)
EXECUTE AS CALLER
AS
$$
# ============================================================================
# Jira Data Load - Simplified dlt Pipeline (v2)
# ============================================================================
# Uses dlt's REST API source with declarative configuration
# and Snowpark destination with native merge operations.
# ============================================================================

from typing import Any, Optional
import json
import _snowflake
import traceback

import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from snowpark_destination import snowpark


# ============================================================================
# JQL CONVERSION FUNCTION (named function instead of lambda for serialization)
# ============================================================================

def convert_to_jql(val: str) -> str:
    """Convert ISO timestamp to JQL date filter for incremental loading."""
    date_part = val[:10] if val else "1970-01-01"
    return f'project is not EMPTY AND updated >= "{date_part}"'


# ============================================================================
# JIRA SOURCE DEFINITION (declarative REST API configuration)
# ============================================================================

@dlt.source(name="jira")
def jira_source(email: str, api_token: str) -> Any:
    """
    Jira source using dlt's REST API with declarative configuration.

    Loads:
    - issues: Incremental merge based on updated timestamp
    - projects: Full replace on each run
    """
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://projuventute.atlassian.net/",
            "auth": {
                "type": "http_basic",
                "username": email,
                "password": api_token,
            },
        },
        "resource_defaults": {
            "primary_key": "id",
            "endpoint": {
                "paginator": {
                    "type": "offset",
                    "limit": 100,
                    "offset_param": "startAt",
                    "limit_param": "maxResults",
                },
            },
        },
        "resources": [
            {
                "name": "issues",
                "write_disposition": "merge",
                "max_table_nesting": 3,
                "columns": {
                    # Clustering for query performance
                    "fields__project__id": {"cluster": True},
                    # Keep custom fields as JSON (prevent unnesting)
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
                    "fields__customfield_10263": {"data_type": "json"},
                },
                "endpoint": {
                    "path": "rest/api/3/search/jql",
                    "data_selector": "issues",
                    "paginator": {
                        "type": "cursor",
                        "cursor_path": "nextPageToken",
                        "cursor_param": "nextPageToken",
                    },
                    "incremental": {
                        "cursor_path": "fields.updated",
                        "start_param": "jql",
                        "initial_value": "1970-01-01T00:00:00.000+0000",
                        "convert": convert_to_jql,
                    },
                    "params": {
                        "fields": "*all",
                        "expand": "changelog",
                    },
                },
            },
            {
                "name": "projects",
                "write_disposition": "replace",
                "max_table_nesting": 0,
                "endpoint": {
                    "path": "rest/api/3/project/search",
                    "data_selector": "values",
                    "params": {
                        "expand": "description,lead,issueTypes,url,projectKeys,permissions,insight"
                    },
                },
            },
        ],
    }

    yield from rest_api_resources(config)


# ============================================================================
# MAIN STORED PROCEDURE FUNCTION
# ============================================================================

def load_jira_data(snowpark_session, endpoints: Optional[str] = None, force_full_load: bool = False) -> str:
    """
    Main stored procedure function - simplified 2-method pattern.

    Args:
        snowpark_session: Snowpark session (automatically provided)
        endpoints: Optional JSON array of endpoint names to load
        force_full_load: If True, reset state and load all data from 1970-01-01

    Returns:
        JSON string with load status and info
    """
    try:
        # Configuration
        TARGET_DATABASE = "RAW"
        DATASET_NAME = "jira"

        # Get credentials from Snowflake secrets
        jira_creds = _snowflake.get_username_password('jira_creds')

        # Parse endpoints
        endpoint_list = None
        if endpoints:
            endpoint_list = json.loads(endpoints)

        # Create pipeline with Snowpark destination
        pipeline = dlt.pipeline(
            pipeline_name="jira",
            destination=snowpark(
                snowpark_session=snowpark_session,
                database=TARGET_DATABASE,
            ),
            dataset_name=DATASET_NAME,
            pipelines_dir="/tmp/dlt_pipelines"  # Ephemeral filesystem in Snowflake
        )

        # Handle force_full_load by resetting pipeline state
        if force_full_load:
            schema = f"{TARGET_DATABASE}.{DATASET_NAME.upper()}"
            try:
                snowpark_session.sql(f"""
                    TRUNCATE TABLE {schema}._DLT_PIPELINE_STATE
                """).collect()
            except Exception:
                pass  # Ignore if table doesn't exist

        # Create source
        source = jira_source(
            email=jira_creds.username,
            api_token=jira_creds.password,
        )

        # Filter to specific endpoints if requested
        if endpoint_list:
            source = source.with_resources(*endpoint_list)

        # For force_full_load, change merge disposition to replace
        # This causes the destination to drop and recreate tables
        if force_full_load:
            for resource in source.selected_resources.values():
                if resource.write_disposition == "merge":
                    resource.write_disposition = "replace"

        # Use dlt's own decompress_state function
        from dlt.common.versioned_state import decompress_state

        # Query pipeline state BEFORE run to see incremental cursor
        state_before = None
        schema = f"{TARGET_DATABASE}.{DATASET_NAME.upper()}"
        try:
            state_result = snowpark_session.sql(f"""
                SELECT state FROM {schema}._DLT_PIPELINE_STATE
                WHERE pipeline_name = 'jira'
                ORDER BY created_at DESC LIMIT 1
            """).collect()
            if state_result:
                raw_state = state_result[0][0]
                decoded_state = decompress_state(raw_state)
                # Extract just the incremental cursor for issues
                cursor_value = None
                try:
                    sources = decoded_state.get("sources", {})
                    jira_src = sources.get("jira", {})
                    resources = jira_src.get("resources", {})
                    issues_resource = resources.get("issues", {})
                    incremental = issues_resource.get("incremental", {})
                    cursor_value = incremental.get("fields.updated", {}).get("last_value")
                except Exception:
                    pass
                state_before = {
                    "cursor_value": cursor_value,
                    "full_state_keys": list(decoded_state.keys()) if isinstance(decoded_state, dict) else "not_a_dict"
                }
            else:
                state_before = "no_rows_found"
        except Exception as e:
            state_before = f"error: {type(e).__name__}: {str(e)}"

        # Run pipeline - dlt handles everything:
        # - Incremental state tracking
        # - Data extraction with pagination
        # - Schema management
        # - Loading to staging
        # - Merge to main tables
        load_info = pipeline.run(source, loader_file_format="parquet")

        # Query pipeline state AFTER run
        state_after = None
        try:
            state_result = snowpark_session.sql(f"""
                SELECT state FROM {schema}._DLT_PIPELINE_STATE
                WHERE pipeline_name = 'jira'
                ORDER BY created_at DESC LIMIT 1
            """).collect()
            if state_result:
                raw_state = state_result[0][0]
                decoded_state = decompress_state(raw_state)
                # Extract just the incremental cursor for issues
                cursor_value = None
                try:
                    sources = decoded_state.get("sources", {})
                    jira_src = sources.get("jira", {})
                    resources = jira_src.get("resources", {})
                    issues_resource = resources.get("issues", {})
                    incremental = issues_resource.get("incremental", {})
                    cursor_value = incremental.get("fields.updated", {}).get("last_value")
                except Exception:
                    pass
                state_after = {
                    "cursor_value": cursor_value,
                    "full_state_keys": list(decoded_state.keys()) if isinstance(decoded_state, dict) else "not_a_dict"
                }
            else:
                state_after = "no_rows_found"
        except Exception as e:
            state_after = f"error: {type(e).__name__}: {str(e)}"

        # Build result
        result = {
            "status": "success",
            "pipeline_name": "jira",
            "dataset_name": DATASET_NAME,
            "database": TARGET_DATABASE,
            "endpoints_loaded": endpoint_list or ["issues", "projects"],
            "load_info": {
                "dataset_name": load_info.dataset_name,
                "started_at": str(load_info.started_at) if load_info.started_at else None,
                "finished_at": str(load_info.finished_at) if load_info.finished_at else None,
            },
            "load_type": "FULL LOAD" if force_full_load else "INCREMENTAL",
            "state_before": state_before,
            "state_after": state_after,
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        error_result = {
            "status": "error",
            "error_type": type(e).__name__,
            "error_message": str(e),
            "traceback": traceback.format_exc(),
        }
        return json.dumps(error_result, indent=2)
$$;

-- Example calls:
-- Load specific endpoints (incremental):
-- CALL raw.jira.p_load_jira('["issues"]', FALSE);
-- CALL raw.jira.p_load_jira('["projects"]', FALSE);
--
-- Load all endpoints (incremental):
-- CALL raw.jira.p_load_jira(NULL, FALSE);
--
-- Force full historical load:
-- CALL raw.jira.p_load_jira('["issues"]', TRUE);
