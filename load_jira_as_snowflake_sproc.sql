CREATE OR REPLACE PROCEDURE raw.jira.p_load_jira(
    target_database VARCHAR DEFAULT 'RAW',  -- Target database for dlt destination
    endpoints VARCHAR DEFAULT NULL  -- Optional: JSON array of endpoint names, e.g., '["issues", "projects"]'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'load_jira_data'
ARTIFACT_REPOSITORY = snowflake.snowpark.pypi_shared_repository
PACKAGES = ('snowflake-snowpark-python', 'requests', 'dlt[parquet]', 'pyarrow')
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

def load_jira_data(snowpark_session, target_database: str = 'RAW', endpoints: Optional[str] = None) -> str:
    """
    Main stored procedure function to load Jira data using dlt with Snowpark destination.

    Args:
        snowpark_session: Snowpark session object (automatically provided)
        target_database: Target database for dlt destination (default: 'RAW')
        endpoints: Optional JSON string of endpoint names. If None, loads all.

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
        initial_date = "1970-01-01"  # Default for first run
        if "issues" in endpoint_list:
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

        # Create pipeline with Snowpark destination
        # CRITICAL: In Snowpark, local filesystem is ephemeral - state MUST come from destination!
        pipeline = dlt.pipeline(
            pipeline_name="jira_snowpark",
            destination=snowpark(snowpark_session=snowpark_session, database=target_database),
            dataset_name="jira",
            pipelines_dir="/tmp/dlt_pipelines",  # Ephemeral - only used during execution
            full_refresh=False  # Allow incremental loading
        )

        # Run pipeline - incremental loading will start from initial_date
        load_info = pipeline.run(source)

        # Prepare result
        result = {
            "status": "success",
            "pipeline_name": "jira_snowpark",
            "dataset_name": "jira",
            "destination": "snowpark_custom",
            "database": target_database,
            "schema": "jira",
            "endpoints_loaded": endpoint_list,
            "load_info": {
                "dataset_name": load_info.dataset_name,
                "started_at": str(load_info.started_at) if load_info.started_at else None,
                "finished_at": str(load_info.finished_at) if load_info.finished_at else None,
                "jobs": len(load_info.jobs) if hasattr(load_info, 'jobs') else 0,
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
CALL raw.jira.p_load_jira('RAW', '["issues"]');