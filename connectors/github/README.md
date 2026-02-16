# GitHub Connector Example

## Connector overview
This connector integrates with the GitHub REST API to synchronize repository data, commits, and pull requests into your destination. It supports both GitHub.com and GitHub Enterprise (on-premises). The connector uses GitHub App authentication for secure access and supports incremental sync to efficiently fetch only new or updated data. It handles pagination automatically and implements robust error handling with retry logic for production use.

GitHub is a code hosting platform for version control and collaboration. This connector provides comprehensive insights into your organization's repositories, development activity, and code review processes.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- GitHub App authentication: Uses JWT-based authentication for secure, long-term access.
- Incremental sync: Fetches only new or updated data since the last sync using timestamp filtering.
- Multi-organization support: Can sync data from multiple GitHub organizations in a single run.
- Automatic pagination: Handles GitHub's pagination (100 items per page) automatically.
- State management: Tracks sync progress per repository for reliable resumption after interruptions.
- Rate limiting: Implements delays between requests and handles rate limit errors gracefully.
- Retry logic: Automatic retry with exponential backoff for transient errors.
- Checkpointing: Saves progress after each repository to enable safe resumption.

## Configuration file
The configuration file contains the GitHub App credentials required to authenticate with the GitHub API.

### For GitHub.com:
```json
{
  "app-id": "<YOUR_GITHUB_APP_ID>",
  "private-key": "<YOUR_GITHUB_APP_PRIVATE_KEY>",
  "organization": "<YOUR_ORGANIZATION_NAME>",
  "installation-id": "<YOUR_INSTALLATION_ID>",
  "base-url": "https://api.github.com"
}
```

### For GitHub Enterprise (on-premises):
```json
{
  "app-id": "<YOUR_GITHUB_APP_ID>",
  "private-key": "<YOUR_GITHUB_APP_PRIVATE_KEY>",
  "organization": "<YOUR_ORGANIZATION_NAME>",
  "installation-id": "<YOUR_INSTALLATION_ID>",
  "base-url": "https://github.your-company.com/api/v3"
}
```

Configuration parameters:
- `app-id` (required): Your GitHub App ID (found in **GitHub App settings**).
- `private-key` (required): RSA private key for your GitHub App (PEM format with newlines).
- `organization` (required): GitHub organization name to sync (can be comma-separated for multiple orgs).
- `installation-id` (required): Installation ID for the GitHub App on your organization.
- `base-url` (optional): GitHub API base URL. Defaults to `https://api.github.com` for GitHub.com. For GitHub Enterprise, use `https://github.your-company.com/api/v3`.

**Important**: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file
This connector requires the following Python packages:

```
pyjwt==2.11.0
cryptography==36.0.1
```

Explanation: PyJWT is used for GitHub App JWT authentication. The cryptography package is required by PyJWT for RSA algorithm support (RS256), which is used to sign JWTs with the GitHub App's private key.

**Note**: The `fivetran_connector_sdk` and `requests` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication

### GitHub App setup
This connector uses GitHub App authentication, which is the recommended approach for production integrations.

#### Step 1: Create a GitHub App.
1. Go to your organization settings: `https://github.com/organizations/{your-org}/settings/apps`.
2. Click **New GitHub App**.
3. Fill in the required fields:
   - **GitHub App name**: Choose a descriptive name (e.g., "Fivetran Data Sync").
   - **Homepage URL**: Your organization's website or `https://fivetran.com`.
   - **Webhook**: Uncheck "Active" (not needed for this connector).

#### Step 2: Set permissions.
Under **Repository permissions**, set:
- **Contents**: Read-only (to access repository data).
- **Metadata**: Read-only (automatically required).
- **Pull requests**: Read-only (to access PR data).

#### Step 3: Generate private key.
1. Scroll to **Private keys** section.
2. Click **Generate a private key**.
3. Save the downloaded `.pem` file securely.
4. Copy the entire contents (including `-----BEGIN RSA PRIVATE KEY-----` and `-----END RSA PRIVATE KEY-----`).

#### Step 4: Note your App ID.
- At the top of the GitHub App settings page, note the **App ID**.

#### Step 5: Install the app.
1. Go to **Install App** in the left sidebar.
2. Click **Install** next to your organization.
3. Choose whether to give access to all repositories or select specific ones.
4. After installation, note the **Installation ID** from the URL:
   - URL format: `https://github.com/organizations/{org}/settings/installations/{installation-id}`.

#### Step 6: Configure the connector.
Add your credentials to `configuration.json`:
- `app-id`: The **App ID** from Step 4.
- `private-key`: The full private key content from Step 3.
- `organization`: Your GitHub organization name.
- `installation-id`: The **Installation ID** from Step 5.

### Authentication flow
The connector uses a two-step authentication process implemented in `generate_jwt()` and `get_installation_access_token()`:
1. Generate JWT: Creates a JSON Web Token signed with your private key (valid for 10 minutes).
2. Get installation token: Exchanges the JWT for an installation access token (valid for 1 hour).
3. API requests: Uses the installation token for all GitHub API requests.

## Pagination
The connector implements cursor-based pagination using GitHub's Link header.

Pagination details:
- Page size: 100 records per request (GitHub's maximum).
- Link header parsing: Automatically extracts "next" page URLs from response headers using `parse_pagination_links()`.
- Memory efficient: Uses generator functions to process data without loading everything into memory.
- Rate limiting: Adds 1-second delay between page requests to respect GitHub's rate limits.

Example pagination flow:
1. Request first page: `GET /orgs/{org}/repos?per_page=100`.
2. Parse `Link` header: `<https://api.github.com/orgs/{org}/repos?page=2>; rel="next"`.
3. Request next page using extracted URL.
4. Continue until no "next" link is present.

## Data handling

### Incremental sync
The connector supports incremental synchronization to minimize API calls and processing time. Implemented in `get_repositories()`, `get_commits()`, and `get_pull_requests()` functions:

Repositories:
- Uses `since` parameter to fetch only repositories updated after last sync.
- Sorts by `updated` timestamp in descending order.

Commits:
- Uses `since` parameter to fetch commits after last sync time.
- Tracked per repository in state.

Pull requests:
- Fetches both open and closed PRs.
- Filters by `updated_at` timestamp.
- Stops fetching when encountering PRs older than last sync.

### State management
The connector maintains detailed state for resumable syncs, managed in the `update()` function with `process_repository_data()` helper:

```json
{
  "last_repo_sync": "2024-02-13T10:30:00Z",
  "last_commit_sync": "2024-02-13T10:30:00Z",
  "last_pr_sync": "2024-02-13T10:30:00Z",
  "processed_repos": {
    "org/repo1": {
      "last_commit_sync": "2024-02-13T10:35:00Z",
      "last_pr_sync": "2024-02-13T10:35:00Z"
    }
  }
}
```

State tracking:
- Global timestamps: Track overall sync progress for repositories, commits, and PRs.
- Per-repository timestamps: Track individual repository sync progress.
- Checkpointing: Saves state after each repository to enable resumption.

## Error handling
The connector implements comprehensive error handling with retry logic in `make_github_request()`.

### Retry strategy
- Maximum retries: 3 attempts for failed requests.
- Exponential backoff: Waits `2^attempt` seconds between retries (2s, 4s, 8s).
- Rate limit handling: Automatically waits until rate limit resets (checks `X-RateLimit-Reset` header).
- 409 conflict: Skips repositories that return 409 errors and continues processing.

### Error categories
- Configuration errors: Validated at sync start with clear error messages in `validate_configuration()`.
- Authentication errors: JWT generation and token exchange failures.
- HTTP errors: Handled based on status code (4xx vs 5xx).
- Network errors: Retried with exponential backoff.

### Special handling
- 404 errors: Repository not found or no access - skips and continues.
- 403 rate limit: Waits until rate limit resets (minimum 60 seconds).
- 409 conflict: Empty repository or git conflict - skips repository.

## Tables created
The connector creates 3 tables in your destination:

### repositories
Contains repository metadata and statistics.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INT | Primary key - Unique GitHub repository ID |
| `name` | STRING | Repository name |
| `full_name` | STRING | Full repository name (owner/repo) |
| `description` | STRING | Repository description |
| `private` | BOOLEAN | Whether the repository is private |
| `html_url` | STRING | Repository web URL |
| `clone_url` | STRING | HTTPS clone URL |
| `ssh_url` | STRING | SSH clone URL |
| `language` | STRING | Primary programming language |
| `stargazers_count` | INT | Number of stars |
| `watchers_count` | INT | Number of watchers |
| `forks_count` | INT | Number of forks |
| `open_issues_count` | INT | Number of open issues |
| `default_branch` | STRING | Default branch name (e.g., "main") |
| `created_at` | TIMESTAMP | Repository creation timestamp |
| `updated_at` | TIMESTAMP | Last update timestamp |
| `pushed_at` | TIMESTAMP | Last push timestamp |
| `archived` | BOOLEAN | Whether the repository is archived |
| `disabled` | BOOLEAN | Whether the repository is disabled |

### commits
Contains commit history for all repositories.

| Column | Type | Description |
|--------|------|-------------|
| `sha` | STRING | Primary key - Unique commit SHA hash |
| `repository` | STRING | Repository name (owner/repo) |
| `message` | STRING | Commit message |
| `author_name` | STRING | Commit author name |
| `author_email` | STRING | Commit author email |
| `author_date` | TIMESTAMP | When the commit was authored |
| `author_login` | STRING | GitHub username of author |
| `committer_name` | STRING | Committer name |
| `committer_email` | STRING | Committer email |
| `committer_date` | TIMESTAMP | When the commit was committed |
| `committer_login` | STRING | GitHub username of committer |
| `html_url` | STRING | Commit web URL |

### pull_requests
Contains pull request data including both open and closed PRs.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INT | Primary key - Unique GitHub PR ID |
| `number` | INT | PR number within the repository |
| `repository` | STRING | Repository name (owner/repo) |
| `title` | STRING | PR title |
| `body` | STRING | PR description/body |
| `state` | STRING | PR state (open or closed) |
| `user_login` | STRING | PR creator's GitHub username |
| `user_id` | INT | PR creator's GitHub user ID |
| `created_at` | TIMESTAMP | When the PR was created |
| `updated_at` | TIMESTAMP | When the PR was last updated |
| `closed_at` | TIMESTAMP | When the PR was closed (null if open) |
| `merged_at` | TIMESTAMP | When the PR was merged (null if not merged) |
| `merge_commit_sha` | STRING | Merge commit SHA if merged |
| `assignee_login` | STRING | Assignee's GitHub username |
| `head_ref` | STRING | Source branch name |
| `head_sha` | STRING | Source branch commit SHA |
| `base_ref` | STRING | Target branch name |
| `base_sha` | STRING | Target branch commit SHA |
| `html_url` | STRING | PR web URL |
| `draft` | BOOLEAN | Whether the PR is a draft |
| `merged` | BOOLEAN | Whether the PR was merged |

## GitHub API rate limits
- Authenticated requests: 5,000 requests per hour per installation.
- Rate limit headers: Connector monitors `X-RateLimit-Remaining` and `X-RateLimit-Reset`.
- Automatic handling: Waits until rate limit resets when limit is hit.
- Delay between requests: 1 second delay to avoid hitting limits unnecessarily.

## Running the connector
To run the connector locally for testing:

```bash
fivetran debug --configuration configuration.json
```

For production deployment, follow the [Fivetran Connector SDK deployment guide](https://fivetran.com/docs/connectors/connector-sdk/deployment).

## Example use cases
- Development analytics: Track commit frequency, PR velocity, and code review metrics.
- Repository management: Monitor repository growth, stars, forks, and activity.
- Team productivity: Analyze contributor activity, PR turnaround time, and collaboration patterns.
- Compliance and security: Audit code changes, track repository settings, and monitor access.
- Cross-repository insights: Compare activity across multiple repositories in your organization.

## Troubleshooting

### Common issues

404 error on installation token
- Verify your **App ID** matches the GitHub App.
- Ensure the GitHub App is installed on your organization.
- Check that the **Installation ID** is correct from the installation URL.

Empty private key error
- Ensure private key includes header/footer: `-----BEGIN RSA PRIVATE KEY-----` and `-----END RSA PRIVATE KEY-----`.
- Preserve newline characters in the private key (use `\n` in JSON).

No data returned
- Verify the organization name is correct.
- Ensure the GitHub App has access to the repositories.
- Check repository permissions in **GitHub App settings**.

Rate limit errors
- The connector automatically handles rate limits.
- For large organizations, consider increasing the sync frequency.

## Related examples

GitHub connectors:
- [github_traffic](../github_traffic/) - This example shows how to sync GitHub repository traffic metrics (views, clones, referrers, and popular content paths) using the GitHub REST API. It demonstrates how to work with GitHub's traffic analytics endpoints, handle limited historical data (14 days), and use Personal Access Token authentication.

Authentication patterns:
- [Certificate Authentication](../../examples/common_patterns_for_connectors/authentication/certificate/) - This example demonstrates certificate-based authentication with two approaches: using base64 encoded certificates in configuration, or retrieving certificates from AWS S3 at runtime. Useful for understanding advanced credential management patterns that can be applied to GitHub App private keys.

## Resources
- [GitHub Apps Documentation](https://docs.github.com/en/apps)
- [GitHub REST API Documentation](https://docs.github.com/en/rest)
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connectors/connector-sdk)
- [GitHub API Rate Limits](https://docs.github.com/en/rest/overview/rate-limits-for-the-rest-api)

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
