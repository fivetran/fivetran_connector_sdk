import requests
import json
from datetime import datetime
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

def schema(configuration: dict):
    return [
        {
            "table": "repository_views",
            "primary_key": ["repository", "timestamp"],
            "columns": {
                "repository": "STRING",
                "timestamp": "UTC_DATETIME",
                "count": "INT",
                "uniques": "INT",
            },
        },
        {
            "table": "repository_clones",
            "primary_key": ["repository", "timestamp"],
            "columns": {
                "repository": "STRING",
                "timestamp": "UTC_DATETIME",
                "count": "INT",
                "uniques": "INT",
            },
        },
        {
            "table": "repository_referrers",
            "primary_key": ["repository", "referrer"],
            "columns": {
                "repository": "STRING",
                "referrer": "STRING",
                "count": "INT",
                "uniques": "INT",
                "synced_at": "UTC_DATETIME",
            },
        },
        {
            "table": "repository_paths",
            "primary_key": ["repository", "path"],
            "columns": {
                "repository": "STRING",
                "path": "STRING",
                "title": "STRING",
                "count": "INT",
                "uniques": "INT",
                "synced_at": "UTC_DATETIME",
            },
        }
    ]

def update(configuration: dict, state: dict):
    log.info("Starting GitHub repository traffic sync")
    
    # Get configuration values
    token = configuration.get('personal_access_token')
    repositories = configuration.get('repositories')
    
    if not token:
        raise ValueError("Personal access token is required")
    
    if not repositories:
        raise ValueError("Repository must be specified")
    
    # Set up authentication headers
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    now = datetime.utcnow().isoformat() + "Z"

    repo_list = [repo.strip() for repo in repositories.split(',')]
    print(repo_list)

    for repository in repo_list:
        owner, repo_name = repository.split('/')
        base_url = f"https://api.github.com/repos/{owner}/{repo_name}/traffic"

        # Sync repository views
        yield from sync_views(base_url, headers, repository, state)

        # Sync repository clones
        yield from sync_clones(base_url, headers, repository, state)

        # Sync repository referrers
        yield from sync_referrers(base_url, headers, repository, now, state)

        # Sync repository paths
        yield from sync_paths(base_url, headers, repository, now, state)

        # Checkpoint after processing each repository
        yield op.checkpoint(state)

def sync_views(base_url, headers, repository, state):
    """Sync the repository views data"""
    url = f"{base_url}/views"
    response = make_api_request(url, headers)
    
    if not response:
        return
    
    for view in response.get("views", []):
        data = {
            "repository": repository,
            "timestamp": view.get("timestamp"),
            "count": view.get("count"),
            "uniques": view.get("uniques")
        }
        yield op.upsert("repository_views", data)
    
    # Update state with last sync time
    state_key = f"{repository}_views_last_sync"
    state[state_key] = datetime.utcnow().isoformat() + "Z"

def sync_clones(base_url, headers, repository, state):
    """Sync the repository clones data"""
    url = f"{base_url}/clones"
    response = make_api_request(url, headers)
    
    if not response:
        return
    
    for clone in response.get("clones", []):
        data = {
            "repository": repository,
            "timestamp": clone.get("timestamp"),
            "count": clone.get("count"),
            "uniques": clone.get("uniques")
        }
        yield op.upsert("repository_clones", data)
    
    # Update state with last sync time
    state_key = f"{repository}_clones_last_sync"
    state[state_key] = datetime.utcnow().isoformat() + "Z"

def sync_referrers(base_url, headers, repository, sync_time, state):
    """Sync the repository referrers data"""
    url = f"{base_url}/popular/referrers"
    response = make_api_request(url, headers)
    
    if not response:
        return
    
    for referrer in response:
        data = {
            "repository": repository,
            "referrer": referrer.get("referrer"),
            "count": referrer.get("count"),
            "uniques": referrer.get("uniques"),
            "synced_at": sync_time
        }
        yield op.upsert("repository_referrers", data)
    
    # Update state with last sync time
    state_key = f"{repository}_referrers_last_sync"
    state[state_key] = sync_time

def sync_paths(base_url, headers, repository, sync_time, state):
    """Sync the repository paths data"""
    url = f"{base_url}/popular/paths"
    response = make_api_request(url, headers)
    
    if not response:
        return
    
    for path_data in response:
        data = {
            "repository": repository,
            "path": path_data.get("path"),
            "title": path_data.get("title"),
            "count": path_data.get("count"),
            "uniques": path_data.get("uniques"),
            "synced_at": sync_time
        }
        yield op.upsert("repository_paths", data)
    
    # Update state with last sync time
    state_key = f"{repository}_paths_last_sync"
    state[state_key] = sync_time

def make_api_request(url, headers):
    """Make an API request and handle errors"""
    try:
        log.info(f"Making API request to: {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        log.severe(f"API request failed: {str(e)}")
        if hasattr(e.response, 'status_code') and e.response.status_code == 403:
            log.severe("Access forbidden. Ensure your token has the 'repo' scope.")
        return None

# Create connector instance
connector = Connector(update=update, schema=schema)

# For local testing
if __name__ == "__main__":
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)