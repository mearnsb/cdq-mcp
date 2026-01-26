"""
Collibra CDQ MCP Server

Provides tools for interacting with Collibra Data Quality API.
Uses lazy authentication with automatic retry on token expiration.
"""
import os
import json
import requests
import urllib3
import pandas as pd
from datetime import datetime
from typing import Optional
from dataclasses import dataclass
from fastmcp import FastMCP

urllib3.disable_warnings()

# =============================================================================
# DQ Client - Handles authentication and API requests
# =============================================================================

@dataclass
class DQConfig:
    """Configuration for DQ API connection"""
    base_url: str
    username: str
    password: str
    iss: str


class DQClient:
    """
    Client for Collibra DQ API with automatic authentication.
    
    - Lazy auth: Only authenticates on first API call
    - Token caching: Reuses token across calls
    - Auto-retry: Re-authenticates on 401 and retries once
    """
    
    def __init__(self, config: DQConfig):
        self.config = config
        self._token: Optional[str] = None
        self._headers: Optional[dict] = None
    
    def _authenticate(self) -> None:
        """Authenticate and cache the token"""
        url = f"{self.config.base_url}/auth/signin"
        creds = {
            'username': self.config.username,
            'password': self.config.password,
            'iss': self.config.iss
        }
        response = requests.post(
            url, 
            json=creds, 
            headers={'Content-Type': 'application/json'}, 
            verify=False
        )
        response.raise_for_status()
        token_json = response.json()
        self._token = token_json['token']
        self._headers = {
            'Content-Type': 'application/json',
            'accept': 'application/json',
            'Authorization': f'Bearer {self._token}'
        }
    
    @property
    def headers(self) -> dict:
        """Get authenticated headers, authenticating if needed"""
        if self._headers is None:
            self._authenticate()
        return self._headers
    
    def request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """
        Make an authenticated request with auto-retry on 401.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint (e.g., '/v2/getrecentruns')
            **kwargs: Passed to requests (params, json, data, etc.)
        
        Returns:
            requests.Response object
        """
        url = f"{self.config.base_url}{endpoint}"
        kwargs.setdefault('verify', False)
        kwargs['headers'] = self.headers
        
        response = requests.request(method, url, **kwargs)
        
        # If unauthorized, re-authenticate and retry once
        if response.status_code == 401:
            self._authenticate()
            kwargs['headers'] = self.headers
            response = requests.request(method, url, **kwargs)
        
        return response
    
    def get(self, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated GET request"""
        return self.request('GET', endpoint, **kwargs)
    
    def post(self, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated POST request"""
        return self.request('POST', endpoint, **kwargs)
    
    def put(self, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated PUT request"""
        return self.request('PUT', endpoint, **kwargs)


# Singleton client instance
_client: Optional[DQClient] = None


def get_client() -> DQClient:
    """Get or create the DQ client singleton"""
    global _client
    if _client is None:
        from dotenv import load_dotenv
        load_dotenv()
        
        config = DQConfig(
            base_url=os.getenv('DQ_URL'),
            username=os.getenv('DQ_USERNAME'),
            password=os.getenv('DQ_PASSWORD'),
            iss=os.getenv('DQ_ISS')
        )
        _client = DQClient(config)
    return _client


def get_today() -> str:
    """Get today's date in YYYY-MM-DD format"""
    return datetime.now().strftime('%Y-%m-%d')


def call_api(method: str, endpoint: str, **kwargs) -> dict:
    """
    Make an API call with full error handling.
    
    Returns dict with either:
        {"success": True, "data": <response_json>}
        {"success": False, "error": "<error_message>"}
    
    All tools should use this for consistent error handling.
    """
    try:
        client = get_client()
        response = client.request(method, endpoint, **kwargs)
        
        # Check for HTTP errors
        if not response.ok:
            return {
                "success": False,
                "error": f"HTTP {response.status_code}: {response.text[:500]}"
            }
        
        # Parse JSON response
        return {"success": True, "data": response.json()}
        
    except requests.exceptions.ConnectionError as e:
        return {"success": False, "error": f"Connection error: {str(e)}"}
    except requests.exceptions.Timeout as e:
        return {"success": False, "error": f"Request timeout: {str(e)}"}
    except requests.exceptions.RequestException as e:
        return {"success": False, "error": f"Request failed: {str(e)}"}
    except json.JSONDecodeError as e:
        return {"success": False, "error": f"Invalid JSON response: {str(e)}"}
    except Exception as e:
        return {"success": False, "error": f"Unexpected error: {type(e).__name__}: {str(e)}"}


# =============================================================================
# MCP Server and Tools
# =============================================================================

mcp = FastMCP("cdq-mcp")


@mcp.tool()
def get_todays_date() -> str:
    """Get today's date in YYYY-MM-DD format. Useful for run IDs and date parameters."""
    return get_today()


@mcp.tool()
def get_recent_runs() -> str:
    """Get recent DQ job runs. Returns a list of recently executed data quality jobs."""
    return json.dumps(call_api('GET', '/v2/getrecentruns'))


@mcp.tool()
def run_sql(sql: str) -> str:
    """
    Execute a SQL query and return results as markdown table.
    
    Args:
        sql: The SQL query to execute
    
    Returns:
        Query results formatted as a markdown table (max 10 rows)
    """
    cxn = os.getenv('DQ_CXN', 'BIGQUERY')
    params = {'sql': sql, 'cxn': cxn}
    
    result = call_api('POST', '/v2/getsqlresult', params=params)
    
    if not result['success']:
        return json.dumps(result)
    
    try:
        json_data = result['data']
        
        # Extract column names from schema
        column_names = [col['name'] for col in json_data['schema']]
        
        # Extract row data
        data_rows = []
        for row in json_data['rows']:
            row_data = [item['colValue'] for item in row]
            data_rows.append(row_data)
        
        # Create DataFrame and convert to markdown
        df = pd.DataFrame(data_rows, columns=column_names)
        
        markdown = f"Results for: `{sql}`\n\n"
        markdown += df.head(10).to_markdown(index=False, tablefmt='presto')
        
        if len(df) > 10:
            markdown += f"\n\n*Showing 10 of {len(df)} rows*"
        
        return markdown
        
    except (KeyError, TypeError) as e:
        return json.dumps({"success": False, "error": f"Failed to parse response: {str(e)}"})


@mcp.tool()
def get_rules_by_dataset(dataset: str) -> str:
    """
    Get all data quality rules for a dataset.
    
    Args:
        dataset: The dataset name (e.g., 'samples.austin_311')
    
    Returns:
        List of rules with their names and SQL definitions
    """
    result = call_api('GET', f'/v3/rules/{dataset}')
    
    if not result['success']:
        return json.dumps(result)
    
    # Format rules for readability
    rules = result['data']
    if not rules:
        return json.dumps({"success": True, "message": f"No rules found for dataset: {dataset}"})
    
    formatted = []
    for r in rules:
        formatted.append({
            "name": r.get('ruleNm'),
            "sql": r.get('ruleValue'),
            "type": r.get('ruleType'),
            "points": r.get('points')
        })
    
    return json.dumps({"success": True, "data": formatted})


@mcp.tool()
def run_dq_job(dataset: str, run_id: str, sql: str) -> str:
    """
    Register a dataset definition and run a DQ job.
    
    Args:
        dataset: Dataset name (e.g., 'my_project.my_table')
        run_id: Run ID, typically a date (e.g., '2025-01-23')
        sql: Source SQL query for the dataset
    
    Returns:
        Job execution result
    """
    cxn = os.getenv('DQ_CXN', 'BIGQUERY')
    
    # Step 1: Register dataset definition
    data = {
        "dataset": dataset,
        "runId": run_id,
        "pushdown": {"sourceQuery": sql, "connectionName": cxn},
        "agentId": {"id": 0},
        "profile": {"on": False}
    }
    
    reg_result = call_api('PUT', '/v3/datasetDefs', json=data)
    if not reg_result['success']:
        return json.dumps({"success": False, "error": f"Registration failed: {reg_result['error']}"})
    
    # Step 2: Run the job
    params = {"dataset": dataset, "runDate": run_id}
    run_result = call_api('POST', '/v3/jobs/run', params=params)
    
    return json.dumps(run_result)


@mcp.tool()
def search_catalog(search_string: str, limit: int = 50) -> str:
    """
    Search the data catalog for datasets.
    
    Args:
        search_string: Text to search for in dataset names
        limit: Maximum results to return (default 50)
    
    Returns:
        List of matching datasets
    """
    cxn = os.getenv('DQ_CXN', 'BIGQUERY')
    params = {
        'draw': '3',
        'start': '0',
        'length': str(limit),
        'search[value]': search_string,
        'filterPushdownPullup': '1',
        'order[0][column]': '0',
        'order[0][dir]': 'asc',
        'filterSource': cxn
    }
    
    result = call_api('GET', '/v2/getdataassetsarrforserversidewithmultifilters', params=params)
    
    if not result['success']:
        return json.dumps(result)
    
    datasets = result['data'].get('dataAssetList', [])
    return json.dumps({"success": True, "count": len(datasets), "data": datasets})


@mcp.tool()
def get_jobs_in_queue(limit: int = 10, status: str = "") -> str:
    """
    Get DQ jobs currently in the queue.
    
    Args:
        limit: Maximum jobs to return (default 10)
        status: Filter by status (empty for all)
    
    Returns:
        List of queued jobs with dataset, runId, and status
    """
    params = {
        'jobStatus': status,
        'limit': str(limit),
    }
    
    result = call_api('GET', '/v2/getowlcheckq', params=params)
    
    if not result['success']:
        return json.dumps(result)
    
    try:
        jobs = result['data'].get('data', [])
        # Extract key fields
        summary = []
        for job in jobs:
            summary.append({
                "dataset": job.get('dataset'),
                "runId": job.get('runId'),
                "status": job.get('status')
            })
        return json.dumps({"success": True, "count": len(summary), "data": summary})
    except (KeyError, TypeError) as e:
        return json.dumps({"success": False, "error": f"Failed to parse response: {str(e)}"})


@mcp.tool()
def save_rule(dataset: str, rule_name: str, rule_sql: str, points: int = 1, perc: int = 1) -> str:
    """
    Create a new data quality rule for a dataset.
    
    Args:
        dataset: Dataset name to attach the rule to
        rule_name: Name for the rule
        rule_sql: SQL query that defines the rule (records returned = failures)
        points: Point value for the rule (default 1)
        perc: Percentage threshold (default 10)
    
    Returns:
        Created rule details
    """
    json_data = {
        'dataset': dataset,
        'ruleNm': rule_name,
        'ruleType': 'SQLF',
        'ruleValue': rule_sql,
        'points': points,
        'ruleRepo': '',
        'perc': perc,
        'columnName': '',
        'businessCategory': '',
        'businessDesc': '',
        'dimId': None,
        #scoringScheme : 1
    }
    
    return json.dumps(call_api('POST', '/v3/rules', json=json_data))


@mcp.tool()
def get_dataset_definition(dataset: str) -> str:
    """
    Get the configuration/definition for a dataset.
    
    Args:
        dataset: Dataset name
    
    Returns:
        Dataset configuration including connection, query, and settings
    """
    params = {'dataset': dataset}
    return json.dumps(call_api('GET', '/v2/owl-options/get', params=params))


@mcp.tool()
def get_hoot_results(dataset: str, run_id: str) -> str:
    """
    Get DQ job results (hoot) for a specific dataset run.
    
    Args:
        dataset: Dataset name
        run_id: Run ID (typically a date like '2025-01-23')
    
    Returns:
        Detailed quality results including scores and findings
    """
    params = {'dataset': dataset, 'runId': run_id}
    return json.dumps(call_api('GET', '/v2/gethoot', params=params))


@mcp.tool()
def get_alerts(dataset: str) -> str:
    """
    Get all alerts configured for a dataset.
    
    Args:
        dataset: Dataset name
    
    Returns:
        List of alerts with their conditions and settings
    """
    params = {'dataset': dataset}
    return json.dumps(call_api('GET', '/v2/getalerts', params=params))


@mcp.tool()
def save_alert(dataset: str, alert_name: str, condition: str, email: str, message: str = "") -> str:
    """
    Create a new alert for a dataset.
    
    Args:
        dataset: Dataset name
        alert_name: Name for the alert
        condition: Alert condition expression
        email: Email address to send alerts to
        message: Custom alert message (optional)
    
    Returns:
        Created alert details
    """
    json_data = {
        'dataset': dataset,
        'alertNm': alert_name,
        'alertCond': condition,
        'alertFormat': 'EMAIL',
        'alertFormatValue': email,
        'alertMsg': message or f"{condition} for {dataset}",
        'batchName': '',
        'addRuleDetails': True,
        'active': True,
        'alertTypes': ['CONDITION'],
    }
    
    return json.dumps(call_api('POST', '/v3/alerts', json=json_data))


if __name__ == "__main__":
    # Use streamable-http for full HTTP transport (MCP 2024-11 spec)
    # Alternatives: "sse" for legacy SSE, "stdio" for local process
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8765)
