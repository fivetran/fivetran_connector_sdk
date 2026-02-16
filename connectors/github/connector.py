"""GitHub Connector for Fivetran Connector SDK.
This connector demonstrates how to fetch data from GitHub REST API and upsert it into destination.
Fetches repositories, commits, and pull requests with proper pagination and incremental sync support.

See the Technical Reference documentation
(https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation
(https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.
"""

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import required libraries for GitHub API integration
# For making HTTP requests to GitHub API
import requests

# For handling JSON data
import json

# For handling rate limiting delays
import time

# For timestamp handling
from datetime import datetime, timezone

# For generating JWT tokens for GitHub App authentication
import jwt

# Constants for API configuration
__GITHUB_API_BASE_URL = "https://api.github.com"  # Default for GitHub.com
__RATE_LIMIT_DELAY = 1  # Delay in seconds between requests to respect rate limits
__MAX_RETRIES = 3  # Maximum number of retries for failed requests
__ITEMS_PER_PAGE = 100  # GitHub API maximum items per page
__CHECKPOINT_INTERVAL = 1000  # Checkpoint after processing this many records


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters with valid values.
    This function is called at the start of the update method to ensure that the connector has all
    necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or invalid.
    """
    # Validate required configuration parameters exist
    required_configs = ["app-id", "private-key", "organization", "installation-id"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    # Validate app-id
    app_id = configuration.get("app-id")
    if not app_id or (isinstance(app_id, str) and not app_id.strip()):
        raise ValueError("app-id must be a non-empty string or number")

    # Validate private-key format
    private_key = configuration.get("private-key")
    if not private_key or not isinstance(private_key, str):
        raise ValueError("private-key must be a non-empty string")
    if "BEGIN RSA PRIVATE KEY" not in private_key and "BEGIN PRIVATE KEY" not in private_key:
        raise ValueError(
            "private-key must contain valid RSA key markers "
            "(-----BEGIN RSA PRIVATE KEY----- or -----BEGIN PRIVATE KEY-----)"
        )

    # Validate organization
    organization = configuration.get("organization")
    if not organization or not isinstance(organization, str) or not organization.strip():
        raise ValueError("organization must be a non-empty string")

    # Validate installation-id
    installation_id = configuration.get("installation-id")
    if not installation_id or (isinstance(installation_id, str) and not installation_id.strip()):
        raise ValueError("installation-id must be a non-empty string or number")

    # Set default base_url if not provided (for GitHub.com)
    if "base-url" not in configuration:
        configuration["base-url"] = GITHUB_API_BASE_URL
        log.info(f"Using default GitHub.com API: {GITHUB_API_BASE_URL}")
    else:
        base_url = configuration.get("base-url")
        if not base_url or not isinstance(base_url, str) or not base_url.strip():
            raise ValueError("base-url must be a non-empty string")
        if not base_url.startswith("http://") and not base_url.startswith("https://"):
            raise ValueError("base-url must be a valid URL starting with http:// or https://")
        log.info(f"Using custom GitHub Enterprise API: {configuration['base-url']}")


def make_github_request(url: str, headers: dict, params: dict = None):
    """
    Make a request to GitHub API with proper error handling and rate limiting.
    Args:
        url: The API endpoint URL
        headers: Request headers including authentication
        params: Query parameters for the request
    Returns:
        Response object from requests library
    Raises:
        RuntimeError: if the request fails after all retries
    """
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, params=params)

            # Handle rate limiting
            if response.status_code == 403 and "rate limit" in response.text.lower():
                reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
                current_time = int(time.time())
                sleep_time = max(reset_time - current_time, 60)  # Wait at least 60 seconds
                log.warning(f"Rate limit hit. Waiting {sleep_time} seconds before retry.")
                time.sleep(sleep_time)
                continue

            # Handle other HTTP errors
            elif response.status_code == 409:
                log.warning(
                    "HTTP 409 Conflict - skipping repository (likely empty or has git conflict)"
                )
                return None
            elif response.status_code >= 400:
                log.warning(f"HTTP {response.status_code} error: {response.text}")
                if attempt == MAX_RETRIES - 1:
                    response.raise_for_status()
                time.sleep(RATE_LIMIT_DELAY * (attempt + 1))
                continue

            return response

        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise RuntimeError(
                    f"Failed to make request after {MAX_RETRIES} to url {url} attempts: {str(e)}"
                )
            log.warning(f"Request attempt {attempt + 1} failed: {str(e)}")
            time.sleep(RATE_LIMIT_DELAY * (attempt + 1))

    raise RuntimeError(f"Failed to make request after {MAX_RETRIES} attempts")


def parse_pagination_links(link_header: str):
    """
    Parse GitHub API pagination links from Link header.
    Args:
        link_header: The Link header value from GitHub API response
    Returns:
        Dictionary containing pagination URLs
    """
    links = {}
    if not link_header:
        return links

    for link in link_header.split(","):
        parts = link.strip().split(";")
        if len(parts) == 2:
            url = parts[0].strip("<>")
            rel = parts[1].strip().split("=")[1].strip('"')
            links[rel] = url

    return links


def get_repositories(headers: dict, organization: str, base_url: str, since: str = None):
    """
    Fetch all repositories for an organization with pagination support.
    Uses GitHub API endpoint: GET /orgs/{org}/repos
    Args:
        headers: Request headers with authentication
        organization: GitHub organization name
        base_url: Base URL for GitHub API
        since: ISO timestamp to filter repositories updated since this time
    Yields:
        Repository data dictionaries
    """
    url = f"{base_url}/orgs/{organization}/repos"
    params = {"per_page": ITEMS_PER_PAGE, "sort": "updated", "direction": "desc"}

    if since:
        params["since"] = since

    page_count = 0

    while url:
        log.info(f"Fetching repositories page {page_count + 1}")
        response = make_github_request(url, headers, params if page_count == 0 else None)
        repositories = response.json()

        if not repositories:
            break

        for repo in repositories:
            yield {
                "id": repo.get("id"),
                "name": repo.get("name"),
                "full_name": repo.get("full_name"),
                "description": repo.get("description"),
                "private": repo.get("private"),
                "html_url": repo.get("html_url"),
                "clone_url": repo.get("clone_url"),
                "ssh_url": repo.get("ssh_url"),
                "language": repo.get("language"),
                "stargazers_count": repo.get("stargazers_count"),
                "watchers_count": repo.get("watchers_count"),
                "forks_count": repo.get("forks_count"),
                "open_issues_count": repo.get("open_issues_count"),
                "default_branch": repo.get("default_branch"),
                "created_at": repo.get("created_at"),
                "updated_at": repo.get("updated_at"),
                "pushed_at": repo.get("pushed_at"),
                "archived": repo.get("archived"),
                "disabled": repo.get("disabled"),
            }

        # Handle pagination using Link header
        links = parse_pagination_links(response.headers.get("Link", ""))
        url = links.get("next")
        params = None  # Clear params for subsequent pages
        page_count += 1

        # Small delay to respect rate limits
        time.sleep(RATE_LIMIT_DELAY)


def get_commits(headers: dict, repo_full_name: str, base_url: str, since: str = None):
    """
    Fetch commits for a repository with pagination support.
    Uses GitHub API endpoint: GET /repos/{owner}/{repo}/commits
    Args:
        headers: Request headers with authentication
        repo_full_name: Full repository name (owner/repo)
        base_url: Base URL for GitHub API
        since: ISO timestamp to filter commits since this time
    Yields:
        Commit data dictionaries
    """
    url = f"{base_url}/repos/{repo_full_name}/commits"
    params = {"per_page": ITEMS_PER_PAGE}

    if since:
        params["since"] = since

    page_count = 0

    while url:
        log.info(f"Fetching commits for {repo_full_name}, page {page_count + 1}")
        response = make_github_request(url, headers, params if page_count == 0 else None)
        if response:
            commits = response.json()
        else:
            commits = None

        if not commits:
            break

        for commit in commits:
            commit_data = commit.get("commit", {})
            author_data = commit_data.get("author", {})
            committer_data = commit_data.get("committer", {})

            yield {
                "sha": commit.get("sha"),
                "repository": repo_full_name,
                "message": commit_data.get("message"),
                "author_name": author_data.get("name"),
                "author_email": author_data.get("email"),
                "author_date": author_data.get("date"),
                "committer_name": committer_data.get("name"),
                "committer_email": committer_data.get("email"),
                "committer_date": committer_data.get("date"),
                "html_url": commit.get("html_url"),
                "author_login": (
                    commit.get("author", {}).get("login") if commit.get("author") else None
                ),
                "committer_login": (
                    commit.get("committer", {}).get("login") if commit.get("committer") else None
                ),
            }

        # Handle pagination
        links = parse_pagination_links(response.headers.get("Link", ""))
        url = links.get("next")
        params = None
        page_count += 1

        time.sleep(RATE_LIMIT_DELAY)


def get_pull_requests(headers: dict, repo_full_name: str, base_url: str, since: str = None):
    """
    Fetch pull requests for a repository with pagination support.
    Uses GitHub API endpoint: GET /repos/{owner}/{repo}/pulls
    Args:
        headers: Request headers with authentication
        repo_full_name: Full repository name (owner/repo)
        base_url: Base URL for GitHub API
        since: ISO timestamp to filter pull requests updated since this time
    Yields:
        Pull request data dictionaries
    """
    # Fetch both open and closed pull requests
    for state in ["open", "closed"]:
        url = f"{base_url}/repos/{repo_full_name}/pulls"
        params = {
            "state": state,
            "per_page": ITEMS_PER_PAGE,
            "sort": "updated",
            "direction": "desc",
        }

        page_count = 0

        while url:
            log.info(f"Fetching {state} pull requests for {repo_full_name}, page {page_count + 1}")
            response = make_github_request(url, headers, params if page_count == 0 else None)
            pull_requests = response.json()

            if not pull_requests:
                break

            # If we have a since filter, check if we've gone beyond it
            if since and pull_requests:
                last_updated = pull_requests[-1].get("updated_at")
                if last_updated and last_updated < since:
                    # Filter remaining PRs and break
                    pull_requests = [
                        pr for pr in pull_requests if pr.get("updated_at", "") >= since
                    ]
                    for pr in pull_requests:
                        yield format_pull_request(pr, repo_full_name)
                    break

            for pr in pull_requests:
                yield format_pull_request(pr, repo_full_name)

            # Handle pagination
            links = parse_pagination_links(response.headers.get("Link", ""))
            url = links.get("next")
            params = None
            page_count += 1

            time.sleep(RATE_LIMIT_DELAY)


def format_pull_request(pr: dict, repo_full_name: str):
    """
    Format pull request data for consistent output.
    Args:
        pr: Pull request data from GitHub API
        repo_full_name: Full repository name
    Returns:
        Formatted pull request dictionary
    """
    user_data = pr.get("user", {})
    assignee_data = pr.get("assignee", {})
    head_data = pr.get("head", {})
    base_data = pr.get("base", {})

    return {
        "id": pr.get("id"),
        "number": pr.get("number"),
        "repository": repo_full_name,
        "title": pr.get("title"),
        "body": pr.get("body"),
        "state": pr.get("state"),
        "user_login": user_data.get("login"),
        "user_id": user_data.get("id"),
        "created_at": pr.get("created_at"),
        "updated_at": pr.get("updated_at"),
        "closed_at": pr.get("closed_at"),
        "merged_at": pr.get("merged_at"),
        "merge_commit_sha": pr.get("merge_commit_sha"),
        "assignee_login": assignee_data.get("login") if assignee_data else None,
        "head_ref": head_data.get("ref"),
        "head_sha": head_data.get("sha"),
        "base_ref": base_data.get("ref"),
        "base_sha": base_data.get("sha"),
        "html_url": pr.get("html_url"),
        "draft": pr.get("draft", False),
        "merged": pr.get("merged", False),
    }


def generate_jwt(app_id, private_key):
    """
    Generate JWT token for GitHub App authentication.
    Args:
        app_id: GitHub App ID
        private_key: RSA private key for the GitHub App
    Returns:
        Encoded JWT token
    """
    # Handle private key format - some configurations may have spaces instead of newlines
    if "\n" not in private_key:
        prefix_pkey = "-----BEGIN RSA PRIVATE KEY-----\n"
        suffix_pkey = "\n-----END RSA PRIVATE KEY-----"
        new_pkey = (
            private_key.replace("-----BEGIN RSA PRIVATE KEY----- ", "")
            .replace(" -----END RSA PRIVATE KEY-----", "")
            .replace(" ", "\n")
        )
        private_key = f"{prefix_pkey}{new_pkey}{suffix_pkey}"

    payload = {
        "iat": int(time.time()),
        "exp": int(time.time()) + (10 * 60),  # Expires in 10 minutes
        "iss": app_id,
    }
    encoded_jwt = jwt.encode(payload, private_key, algorithm="RS256")
    return encoded_jwt


def get_installation_access_token(jwt_token, installation_id, base_url):
    """
    Exchange JWT token for installation access token.
    Args:
        jwt_token: JWT token generated for the GitHub App
        installation_id: Installation ID for the organization
        base_url: Base URL for GitHub API
    Returns:
        Installation access token
    Raises:
        HTTPError: If the request fails
    """
    headers = {"Authorization": f"Bearer {jwt_token}", "Accept": "application/vnd.github.v3+json"}
    url = f"{base_url}/app/installations/{installation_id}/access_tokens"
    response = requests.post(url, headers=headers)
    response.raise_for_status()
    return response.json()["token"]


def process_repository_data(repo_full_name, headers, base_url, state, current_sync_time):
    """
    Process commits and pull requests for a single repository.
    This function encapsulates the logic for fetching and upserting commits and PRs,
    helping to keep the main sync loop clean and reduce memory footprint.

    Args:
        repo_full_name: Full repository name (owner/repo)
        headers: Request headers with authentication
        base_url: Base URL for GitHub API
        state: State dictionary containing sync timestamps and processed repositories
        current_sync_time: Current sync timestamp

    Returns:
        Tuple of (commit_count, pr_count) - number of commits and PRs processed
    """
    # Extract state variables
    processed_repos = state.get("processed_repos", {})
    last_commit_sync = state.get("last_commit_sync")
    last_pr_sync = state.get("last_pr_sync")

    # Get last sync time for this specific repository
    repo_data = processed_repos.get(repo_full_name, {})
    repo_last_commit_sync = repo_data.get("last_commit_sync", last_commit_sync)
    repo_last_pr_sync = repo_data.get("last_pr_sync", last_pr_sync)

    # Fetch and upsert commits for this repository
    log.info(f"Fetching commits for {repo_full_name}...")
    commit_count = 0
    for commit in get_commits(headers, repo_full_name, base_url, since=repo_last_commit_sync):
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="commits", data=commit)
        commit_count += 1

    log.info(f"Fetched {commit_count} commits for {repo_full_name}")

    # Fetch and upsert pull requests for this repository
    log.info(f"Fetching pull requests for {repo_full_name}...")
    pr_count = 0
    for pr in get_pull_requests(headers, repo_full_name, base_url, since=repo_last_pr_sync):
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table="pull_requests", data=pr)
        pr_count += 1

    log.info(f"Fetched {pr_count} pull requests for {repo_full_name}")

    # Update processed repositories state
    if repo_full_name not in processed_repos:
        processed_repos[repo_full_name] = {}
    processed_repos[repo_full_name]["last_commit_sync"] = current_sync_time
    processed_repos[repo_full_name]["last_pr_sync"] = current_sync_time

    return commit_count, pr_count


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {"table": "repositories", "primary_key": ["id"]},
        {"table": "commits", "primary_key": ["sha"]},
        {"table": "pull_requests", "primary_key": ["id"]},
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.info("Starting GitHub API Connector sync")

    # Validate the configuration to ensure it contains all required values
    validate_configuration(configuration=configuration)

    # Extract configuration parameters
    pkey = configuration.get("private-key")
    organizations = configuration.get("organization")
    app_id = configuration.get("app-id")
    installation_id = configuration.get("installation-id")
    base_url = configuration.get("base-url")

    jwt_token = generate_jwt(app_id, pkey)

    installation_token = get_installation_access_token(jwt_token, installation_id, base_url)

    # Set up authentication headers matching GitHub API documentation
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"token {installation_token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    # Get state variables for incremental sync
    last_repo_sync = state.get("last_repo_sync")
    last_commit_sync = state.get("last_commit_sync")
    last_pr_sync = state.get("last_pr_sync")
    processed_repos = state.get("processed_repos", {})

    try:
        # Track current sync time
        current_sync_time = datetime.now(timezone.utc).isoformat()
        record_count = 0
        repo_count = 0

        for organization in organizations.split(","):
            log.info(f"Starting sync for organization: {organization}")
            log.info("Fetching and processing repositories...")
            for repo in get_repositories(headers, organization, base_url, since=last_repo_sync):
                # The 'upsert' operation is used to insert or update data in the destination table.
                # The first argument is the name of the destination table.
                # The second argument is a dictionary containing the record to be upserted.
                op.upsert(table="repositories", data=repo)
                record_count += 1
                repo_count += 1

                repo_full_name = repo.get("full_name")
                if not repo_full_name:
                    log.warning(f"Skipping repository with missing full_name: {repo}")
                    continue

                log.info(f"Processing repository {repo_count}: {repo_full_name}")

                # Process commits and pull requests for this repository using helper function
                commit_count, pr_count = process_repository_data(
                    repo_full_name, headers, base_url, state, current_sync_time
                )
                record_count += commit_count + pr_count

                # Checkpoint after each repository to enable resumption and avoid memory issues.
                # Save the progress by checkpointing the state. This is important for ensuring
                # that the sync process can resume from the correct position in case of next sync
                # or interruptions. Learn more about how and where to checkpoint by reading our
                # best practices documentation (https://fivetran.com/docs/connectors/connector-sdk/
                # best-practices#largedatasetrecommendation).
                op.checkpoint(
                    state={
                        "last_repo_sync": current_sync_time,
                        "last_commit_sync": current_sync_time,
                        "last_pr_sync": current_sync_time,
                        "processed_repos": processed_repos,
                    }
                )

            log.info(
                f"Completed sync for organization: {organization}. Processed {repo_count} repositories."
            )

            # Final state update with the current sync time for the next run
            final_state = {
                "last_repo_sync": current_sync_time,
                "last_commit_sync": current_sync_time,
                "last_pr_sync": current_sync_time,
                "processed_repos": processed_repos,
            }

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(final_state)

        log.info(f"Sync completed successfully. Total records processed: {record_count}")

    except Exception as e:
        # In case of an exception, raise a runtime error
        raise RuntimeError(f"Failed to sync GitHub data: {str(e)}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the
# command line or IDE 'run' button.
#
# IMPORTANT: The recommended way to test your connector is using the Fivetran debug command:
#   fivetran debug
#
# This local testing block is provided as a convenience for quick debugging during development,
# such as using IDE debug tools (breakpoints, step-through debugging, etc.).
# Note: This method is not called by Fivetran when executing your connector in production.
# Always test using 'fivetran debug' prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
