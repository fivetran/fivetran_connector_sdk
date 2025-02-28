"""
BuildKite connector for Fivetran.

This connector syncs data from the BuildKite REST API to your data warehouse.
It supports syncing organizations, pipelines, builds, jobs, artifacts and agents data.
"""

import requests
import time
from datetime import datetime, timezone
from urllib.parse import urljoin
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


# Define the schema function which configures the tables and columns for this connector
def schema(configuration: dict):
    return [
        {
            "table": "organizations",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "slug": "STRING",
                "url": "STRING",
                "web_url": "STRING",
                "created_at": "UTC_DATETIME",
            },
        },
        {
            "table": "pipelines",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "slug": "STRING",
                "repository": "STRING",
                "description": "STRING",
                "branch_configuration": "STRING",
                "default_branch": "STRING",
                "provider": "STRING",
                "skip_queued_branch_builds": "BOOLEAN",
                "skip_queued_branch_builds_filter": "STRING",
                "cancel_running_branch_builds": "BOOLEAN",
                "cancel_running_branch_builds_filter": "STRING",
                "builds_url": "STRING",
                "badge_url": "STRING",
                "created_at": "UTC_DATETIME",
                "scheduled_builds_count": "INTEGER",
                "running_builds_count": "INTEGER",
                "scheduled_jobs_count": "INTEGER",
                "running_jobs_count": "INTEGER",
                "waiting_jobs_count": "INTEGER",
                "visibility": "STRING",
                "tags": "STRING",
                "org_id": "STRING",
                "org_slug": "STRING",
                "web_url": "STRING",
                "url": "STRING",
                "steps": "STRING",
                "env": "STRING",
                "archived_at": "UTC_DATETIME",
            },
        },
        {
            "table": "builds",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "url": "STRING",
                "web_url": "STRING",
                "number": "INTEGER",
                "state": "STRING",
                "blocked": "BOOLEAN",
                "message": "STRING",
                "commit": "STRING",
                "branch": "STRING",
                "tag": "STRING",
                "source": "STRING",
                "creator": "STRING",
                "creator_email": "STRING",
                "creator_avatar_url": "STRING",
                "pull_request": "STRING",
                "pipeline_id": "STRING",
                "pipeline_slug": "STRING",
                "organization_id": "STRING",
                "organization_slug": "STRING",
                "created_at": "UTC_DATETIME",
                "scheduled_at": "UTC_DATETIME",
                "started_at": "UTC_DATETIME",
                "finished_at": "UTC_DATETIME",
                "meta_data": "STRING",
                "author": "STRING",
                "author_email": "STRING",
            },
        },
        {
            "table": "jobs",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "graphql_id": "STRING",
                "type": "STRING",
                "name": "STRING",
                "step_key": "STRING",
                "command": "STRING",
                "state": "STRING",
                "build_id": "STRING",
                "build_number": "INTEGER",
                "pipeline_id": "STRING",
                "pipeline_slug": "STRING",
                "organization_id": "STRING",
                "organization_slug": "STRING",
                "agent_id": "STRING",
                "agent_name": "STRING",
                "created_at": "UTC_DATETIME",
                "scheduled_at": "UTC_DATETIME",
                "runnable_at": "UTC_DATETIME",
                "started_at": "UTC_DATETIME",
                "finished_at": "UTC_DATETIME",
                "passed": "BOOLEAN",
                "soft_failed": "BOOLEAN",
                "exit_status": "INTEGER",
                "artifact_paths": "STRING",
                "agent_query_rules": "STRING",
                "web_url": "STRING",
                "log_url": "STRING",
                "raw_log_url": "STRING",
                "retried": "BOOLEAN",
                "retried_in_job_id": "STRING",
                "retries_count": "INTEGER",
                "retry_type": "STRING",
                "parallel_group_index": "INTEGER",
                "parallel_group_total": "INTEGER",
                "triggered_from_job_id": "STRING",
            },
        },
        {
            "table": "artifacts",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "job_id": "STRING",
                "build_id": "STRING",
                "pipeline_id": "STRING",
                "organization_id": "STRING",
                "path": "STRING",
                "filename": "STRING",
                "mime_type": "STRING",
                "file_size": "INTEGER",
                "sha1sum": "STRING",
                "url": "STRING",
                "download_url": "STRING",
                "state": "STRING",
                "created_at": "UTC_DATETIME",
            },
        },
        {
            "table": "agents",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "graphql_id": "STRING",
                "url": "STRING",
                "web_url": "STRING",
                "name": "STRING",
                "connection_state": "STRING",
                "hostname": "STRING",
                "ip_address": "STRING",
                "user_agent": "STRING",
                "version": "STRING",
                "creator": "STRING",
                "creator_email": "STRING",
                "created_at": "UTC_DATETIME",
                "last_job_finished_at": "UTC_DATETIME",
                "job_id": "STRING",
                "job_path": "STRING",
                "organization_id": "STRING",
                "ping_at": "UTC_DATETIME",
                "public_ipv4_address": "STRING",
                "priority": "INTEGER",
                "meta_data": "STRING",
                "tags": "STRING",
            },
        },
    ]


# Define the update function, which is called by Fivetran during each sync
def update(configuration: dict, state: dict):
    log.info("Starting BuildKite connector sync")

    # Extract configuration values
    api_token = configuration.get("api_token")
    if not api_token:
        raise ValueError("API token is required in configuration")

    # BuildKite API base URL
    base_url = "https://api.buildkite.com/v2/"

    # Set up the authentication headers
    headers = get_auth_headers(api_token)

    # Initialize or get state for syncing with last updated timestamps
    organizations_cursor = state.get("organizations_updated_at", "1970-01-01T00:00:00Z")
    pipelines_cursor = state.get("pipelines_updated_at", "1970-01-01T00:00:00Z")
    builds_cursor = state.get("builds_updated_at", "1970-01-01T00:00:00Z")
    jobs_cursor = state.get("jobs_updated_at", "1970-01-01T00:00:00Z")
    artifacts_cursor = state.get("artifacts_updated_at", "1970-01-01T00:00:00Z")
    agents_cursor = state.get("agents_updated_at", "1970-01-01T00:00:00Z")

    # Sync organizations
    log.info(f"Syncing organizations since {organizations_cursor}")
    yield from sync_organizations(base_url, headers, state, organizations_cursor)

    # Get list of organizations for pipeline syncing
    organizations = get_organizations(base_url, headers)
    
    # Sync pipelines for each organization
    log.info(f"Syncing pipelines since {pipelines_cursor}")
    for org in organizations:
        org_slug = org["slug"]
        yield from sync_pipelines(base_url, headers, org_slug, state, pipelines_cursor)
    
    # Sync builds, jobs, and artifacts
    log.info(f"Syncing builds since {builds_cursor}")
    for org in organizations:
        org_slug = org["slug"]
        # Get all pipelines for this organization
        pipelines = get_pipelines(base_url, headers, org_slug)
        
        for pipeline in pipelines:
            pipeline_slug = pipeline["slug"]
            yield from sync_builds(base_url, headers, org_slug, pipeline_slug, state, builds_cursor)
            
            # Get builds to sync jobs and artifacts
            builds = get_builds(base_url, headers, org_slug, pipeline_slug, cursor=None, limit=10)
            
            for build in builds:
                build_number = build["number"]
                
                # Sync jobs for this build
                log.info(f"Syncing jobs for {org_slug}/{pipeline_slug} build #{build_number}")
                yield from sync_jobs(base_url, headers, org_slug, pipeline_slug, build_number, state, jobs_cursor)
                
                # Get jobs to sync artifacts
                jobs = get_jobs(base_url, headers, org_slug, pipeline_slug, build_number)
                
                for job in jobs:
                    job_id = job["id"]
                    
                    # Sync artifacts for this job
                    log.info(f"Syncing artifacts for job {job_id}")
                    yield from sync_artifacts(base_url, headers, org_slug, pipeline_slug, build_number, job_id, state, artifacts_cursor)
    
    # Sync agents for each organization
    log.info(f"Syncing agents since {agents_cursor}")
    for org in organizations:
        org_slug = org["slug"]
        yield from sync_agents(base_url, headers, org_slug, state, agents_cursor)

    # Final checkpoint
    yield op.checkpoint(state)


# Helper function to get authentication headers
def get_auth_headers(api_token):
    return {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }


# Generic function to make API requests
def get_api_response(url, headers, params=None):
    log.info(f"Making API call to: {url} with params: {params}")
    
    # Add rate limiting handling
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            if e.response.status_code == 429:  # Rate limited
                retry_after = int(e.response.headers.get("Retry-After", 60))
                log.warning(f"Rate limited. Waiting {retry_after} seconds before retrying.")
                time.sleep(retry_after)
                retry_count += 1
            else:
                raise
        except Exception as e:
            log.error(f"Error making API request: {str(e)}")
            raise
    
    raise Exception(f"Failed after {max_retries} retries")


# Sync organizations data
def sync_organizations(base_url, headers, state, cursor):
    url = urljoin(base_url, "organizations")
    params = {
        "per_page": 100,
    }
    
    response = get_api_response(url, headers, params)
    
    for org in response:
        yield op.upsert("organizations", org)
        
        # Update cursor if this organization is newer
        org_created_at = org.get("created_at", "")
        if org_created_at > cursor:
            cursor = org_created_at
            state["organizations_updated_at"] = cursor
    
    yield op.checkpoint(state)


# Get all organizations (used for subsequent API calls)
def get_organizations(base_url, headers):
    url = urljoin(base_url, "organizations")
    params = {
        "per_page": 100,
    }
    
    return get_api_response(url, headers, params)


# Sync pipelines for an organization
def sync_pipelines(base_url, headers, org_slug, state, cursor):
    url = urljoin(base_url, f"organizations/{org_slug}/pipelines")
    
    # Set up pagination params
    params = {
        "per_page": 100,
    }
    
    page = 1
    more_data = True
    
    while more_data:
        params["page"] = page
        response = get_api_response(url, headers, params)
        
        # Break if no more pipelines
        if not response:
            break
        
        for pipeline in response:
            # Add organization information to pipeline data
            pipeline["org_id"] = pipeline.get("organization", {}).get("id", "")
            pipeline["org_slug"] = pipeline.get("organization", {}).get("slug", "")
            
            # Convert steps and env to strings for storage
            if "steps" in pipeline:
                pipeline["steps"] = str(pipeline["steps"])
            if "env" in pipeline:
                pipeline["env"] = str(pipeline["env"])
            
            yield op.upsert("pipelines", pipeline)
            
            # Update cursor if this pipeline is newer
            pipeline_created_at = pipeline.get("created_at", "")
            if pipeline_created_at > cursor:
                cursor = pipeline_created_at
                state["pipelines_updated_at"] = cursor
        
        # Checkpoint state after each page
        yield op.checkpoint(state)
        
        # Check if we should continue to the next page
        more_data = len(response) == params["per_page"]
        page += 1


# Get all pipelines for an organization (used for subsequent API calls)
def get_pipelines(base_url, headers, org_slug):
    url = urljoin(base_url, f"organizations/{org_slug}/pipelines")
    params = {
        "per_page": 100,
    }
    
    pipelines = []
    page = 1
    more_data = True
    
    while more_data:
        params["page"] = page
        response = get_api_response(url, headers, params)
        
        if not response:
            break
        
        pipelines.extend(response)
        
        # Check if we should continue to the next page
        more_data = len(response) == params["per_page"]
        page += 1
    
    return pipelines


# Sync builds for a pipeline
def sync_builds(base_url, headers, org_slug, pipeline_slug, state, cursor):
    url = urljoin(base_url, f"organizations/{org_slug}/pipelines/{pipeline_slug}/builds")
    
    # Set up pagination and filtering params
    params = {
        "per_page": 100,
        "created_from": cursor,  # Filter by created date from cursor
    }
    
    page = 1
    more_data = True
    
    while more_data:
        params["page"] = page
        response = get_api_response(url, headers, params)
        
        # Break if no more builds
        if not response:
            break
        
        for build in response:
            # Add pipeline and organization information
            build["pipeline_id"] = build.get("pipeline", {}).get("id", "")
            build["pipeline_slug"] = build.get("pipeline", {}).get("slug", "")
            build["organization_id"] = build.get("organization", {}).get("id", "")
            build["organization_slug"] = build.get("organization", {}).get("slug", "")
            
            # Convert structured fields to strings
            if "pull_request" in build:
                build["pull_request"] = str(build["pull_request"])
            if "meta_data" in build:
                build["meta_data"] = str(build["meta_data"])
            
            # Extract creator information if available
            if "creator" in build and isinstance(build["creator"], dict):
                build["creator_email"] = build["creator"].get("email", "")
                build["creator_avatar_url"] = build["creator"].get("avatar_url", "")
                build["creator"] = build["creator"].get("name", "")
            
            yield op.upsert("builds", build)
            
            # Update cursor if this build is newer
            build_created_at = build.get("created_at", "")
            if build_created_at > cursor:
                cursor = build_created_at
                state["builds_updated_at"] = cursor
        
        # Checkpoint state after each page
        yield op.checkpoint(state)
        
        # Check if we should continue to the next page
        more_data = len(response) == params["per_page"]
        page += 1


# Get builds for a pipeline (used for jobs and artifacts syncing)
def get_builds(base_url, headers, org_slug, pipeline_slug, cursor=None, limit=100):
    url = urljoin(base_url, f"organizations/{org_slug}/pipelines/{pipeline_slug}/builds")
    
    params = {
        "per_page": limit,
    }
    
    if cursor:
        params["created_from"] = cursor
    
    return get_api_response(url, headers, params)


# Sync jobs for a build
def sync_jobs(base_url, headers, org_slug, pipeline_slug, build_number, state, cursor):
    url = urljoin(base_url, f"organizations/{org_slug}/pipelines/{pipeline_slug}/builds/{build_number}/jobs")
    
    response = get_api_response(url, headers)
    
    for job in response:
        # Add build, pipeline, and organization information
        job["build_id"] = job.get("build", {}).get("id", "")
        job["build_number"] = build_number
        job["pipeline_id"] = job.get("pipeline", {}).get("id", "")
        job["pipeline_slug"] = pipeline_slug
        job["organization_id"] = job.get("organization", {}).get("id", "")
        job["organization_slug"] = org_slug
        
        # Add agent information if available
        if "agent" in job and isinstance(job["agent"], dict):
            job["agent_id"] = job["agent"].get("id", "")
            job["agent_name"] = job["agent"].get("name", "")
        
        # Convert arrays to strings
        if "agent_query_rules" in job:
            job["agent_query_rules"] = str(job["agent_query_rules"])
        
        yield op.upsert("jobs", job)
        
        # Update cursor if this job is newer
        job_created_at = job.get("created_at", "")
        if job_created_at > cursor:
            cursor = job_created_at
            state["jobs_updated_at"] = cursor
    
    yield op.checkpoint(state)


# Get jobs for a build (used for artifacts syncing)
def get_jobs(base_url, headers, org_slug, pipeline_slug, build_number):
    url = urljoin(base_url, f"organizations/{org_slug}/pipelines/{pipeline_slug}/builds/{build_number}/jobs")
    return get_api_response(url, headers)


# Sync artifacts for a job
def sync_artifacts(base_url, headers, org_slug, pipeline_slug, build_number, job_id, state, cursor):
    url = urljoin(base_url, f"organizations/{org_slug}/pipelines/{pipeline_slug}/builds/{build_number}/jobs/{job_id}/artifacts")
    
    response = get_api_response(url, headers)
    
    for artifact in response:
        # Add job, build, pipeline, and organization information
        artifact["job_id"] = job_id
        artifact["build_id"] = ""  # We don't have the build ID directly here
        artifact["pipeline_id"] = ""  # We don't have the pipeline ID directly here
        artifact["organization_id"] = ""  # We don't have the organization ID directly here
        
        yield op.upsert("artifacts", artifact)
        
        # Update cursor if this artifact is newer
        artifact_created_at = artifact.get("created_at", "")
        if artifact_created_at > cursor:
            cursor = artifact_created_at
            state["artifacts_updated_at"] = cursor
    
    yield op.checkpoint(state)


# Sync agents for an organization
def sync_agents(base_url, headers, org_slug, state, cursor):
    url = urljoin(base_url, f"organizations/{org_slug}/agents")
    
    # Set up pagination params
    params = {
        "per_page": 100,
    }
    
    page = 1
    more_data = True
    
    while more_data:
        params["page"] = page
        response = get_api_response(url, headers, params)
        
        # Break if no more agents
        if not response:
            break
        
        for agent in response:
            # Add organization information
            agent["organization_id"] = org_slug
            
            # Convert complex data types to strings
            if "meta_data" in agent:
                agent["meta_data"] = str(agent["meta_data"])
            if "tags" in agent:
                agent["tags"] = str(agent["tags"])
            
            yield op.upsert("agents", agent)
            
            # Update cursor if this agent is newer
            agent_created_at = agent.get("created_at", "")
            if agent_created_at > cursor:
                cursor = agent_created_at
                state["agents_updated_at"] = cursor
        
        # Checkpoint state after each page
        yield op.checkpoint(state)
        
        # Check if we should continue to the next page
        more_data = len(response) == params["per_page"]
        page += 1


# Create the connector object
connector = Connector(update=update, schema=schema)

# For local testing
if __name__ == "__main__":
    import json
    try:
        with open("configuration.json", "r") as f:
            configuration = json.load(f)
        connector.debug(configuration=configuration)
    except FileNotFoundError:
        print("configuration.json not found. Please create it with your BuildKite API token.")
        print('Example: {"api_token": "YOUR_API_TOKEN"}')