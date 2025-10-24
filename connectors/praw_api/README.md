# PRAW API Connector example
## Connector overview
This custom connector enables data extraction from Reddit using PRAW (Python Reddit API Wrapper). It retrieves posts and comments from defined subreddits and syncs them into Fivetran in structured table format. 

The connector supports incremental replication based on post timestamps to minimize redundant data transfer.



## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.


## Features
* Connects to Reddit via PRAW (Python Reddit API Wrapper) using OAuth2 authentication.
* Retrieves posts and comments from one or more subreddits.
* Supports incremental syncs using post timestamp checkpoints.
* Fetches subreddit data in API rate‑limited batches to prevent throttling and ensure stable syncs.
* Flattens nested Reddit data into structured relational format.

## Configuration file

You can add multiple subreddits in the same string. 

```
{
  "subreddits": "AskReddit,Python",
  "client_id": "YOUR_CLIENT_ID",
  "client_secret": "YOUR_CLIENT_SECRET",
  "user_agent": "YOUR_APP_USER_AGENT"
}
```
* subreddits: Comma-separated list of subreddit names to extract data from.
* client_id: Reddit API client ID for OAuth2 authentication.
* client_secret: Reddit API client secret for OAuth2 authentication.
* user_agent: User agent string required by Reddit API for identification.

Note: Ensure that the configuration.json file is not checked into version control to protect sensitive information.

## Requirements file

```
praw
pandas
```
Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
PRAW requires OAuth2-style authentication via Reddit developer credentials.
To create credentials:
* Log in to https://www.reddit.com/prefs/apps
* Create a new application (choose script type)
* Copy the client ID, client secret, and define a user agent

Insert these credentials into configuration.json.

## Pagination
PRAW manages pagination internally via the limit parameter when retrieving subreddit submissions. The connector requests the newest posts using Reddit’s new() listing endpoint with configurable batch sizes.

## Data handling
The connector performs the following transformations:
* Retrieves posts newer than the last sync epoch.
* Extracts both post data and associated comments.
* Flattens data into relational form for CSV export and table upsert.
* Writes data using Operations.upsert() for incremental schema updates.
```
Schema definition: 
Field           |  Type    |  Description                   
----------------+----------+-------------------------------------------
id              |  INT     |  Sequential row identifier (Surrogate Key)   
post_id         |  STRING  |  Reddit post identifier        
post_text       |  STRING  |  Text or title of the post     
post_author     |  STRING  |  Username of the post author   
comment_id      |  STRING  |  Reddit comment identifier     
comment_author  |  STRING  |  Username of the comment author
comment_text    |  STRING  |  Comment body text             
```
Reddit has its id (post and comment) as alphanumeric instead of a number. 

## Error handling

* Validates configuration parameters before attempting connection.
* Provides detailed error messages if authentication or connection fails.
* Handles timezone-related errors by ensuring consistent timezone awareness.
* Wraps data fetching operations in try/except blocks with informative error messages.
* Logs warnings for recoverable failures such as individual comment retrieval errors,
  allowing sync to continue without aborting the full run
* Implements regular checkpointing to minimize data loss in case of failures.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
