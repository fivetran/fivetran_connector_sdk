"""This connector fetches news articles and analytics from the New York Times API.
It supports multiple endpoints including article archive search and most popular articles.
"""
# For reading configuration from a JSON file
import json

# Additional imports for API functionality
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List

# Fivetran SDK imports
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

class NYTimesAPIError(Exception):
    """Custom exception for NY Times API errors"""
    pass

def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """
    required_configs = ["api_key", "start_date", "period"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    # Validate date format
    try:
        datetime.strptime(configuration["start_date"], "%Y-%m")
    except ValueError:
        raise ValueError("start_date must be in YYYY-MM format")

    # Validate period
    try:
        period = int(configuration["period"])
        if period not in [1, 7, 30]:
            raise ValueError("period must be one of: 1, 7, or 30 days")
    except ValueError:
        raise ValueError("period must be a string that can be converted to an integer (1, 7, or 30)")

def schema(configuration: dict):
    """
    Define the schema for all streams in the NY Times connector.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        List of stream definitions with their schemas.
    """
    return [
        {
            "table": "article",
            "primary_key": ["_id"],
            "columns": {
                "_id": "STRING",
                "web_url": "STRING",
                "snippet": "STRING",
                "print_page": "STRING",
                "print_section": "STRING",
                "source": "STRING",
                "pub_date": "UTC_DATETIME",
                "document_type": "STRING",
                "news_desk": "STRING",
                "section_name": "STRING",
                "type_of_material": "STRING",
                "word_count": "INT",
                "uri": "STRING",
                "archive_date": "UTC_DATETIME",
                "abstract": "STRING"
            }
        },
        {
            "table": "most_popular",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "url": "STRING",
                "adx_keywords": "STRING", 
                "section": "STRING",
                "byline": "STRING",
                "type": "STRING",
                "title": "STRING",
                "abstract": "STRING",
                "published_date": "UTC_DATETIME",
                "source": "STRING",
                "updated": "UTC_DATETIME",
                "uri": "STRING"
            }
        }
    ]

def make_request(url: str, params: Dict, api_key: str, max_retries: int = 3) -> Dict:
    """
    Make an HTTP request to the NY Times API with retries and error handling.
    Args:
        url: The API endpoint URL
        params: Query parameters
        api_key: NY Times API key
        max_retries: Maximum number of retry attempts
    Returns:
        JSON response from the API
    Raises:
        NYTimesAPIError: If the API request fails after all retries
    """
    # Create sanitized URL for logging by replacing API key with ***
    safe_url = url.replace(api_key, "***") if api_key in url else url
    safe_params = params.copy()
    safe_params["api-key"] = "***"
    
    headers = {"Accept": "application/json"}
    params["api-key"] = api_key
    
    retry_count = 0
    while retry_count < max_retries:
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Rate limit exceeded
                retry_count += 1
                if retry_count < max_retries:
                    wait_time = 2 ** retry_count  # Exponential backoff
                    log.warning(f"Rate limit reached for {safe_url}. Waiting {wait_time} seconds before retry {retry_count}")
                    time.sleep(wait_time)
                    continue
                # Max retries reached for rate limit, raise specific error
                raise NYTimesAPIError(f"Rate limit exceeded after {max_retries} retries. API is currently unavailable.")
            # Other HTTP errors
            safe_error = str(e).replace(api_key, "***")
            raise NYTimesAPIError(f"API request failed: {safe_error}")
        except requests.exceptions.RequestException as e:
            safe_error = str(e).replace(api_key, "***")
            raise NYTimesAPIError(f"API request failed: {safe_error}")
    
    raise NYTimesAPIError(f"Max retries ({max_retries}) exceeded for {safe_url}")

def fetch_articles(configuration: dict, start_date: str, end_date: str) -> List[Dict]:
    """
    Fetch articles from the NY Times Archive API.
    Args:
        configuration: Connector configuration
        start_date: Start date in YYYY-MM format
        end_date: End date in YYYY-MM format
    Returns:
        List of article records
    """
    base_url = "https://api.nytimes.com/svc/archive/v1"
    articles = []
    
    year, month = map(int, start_date.split("-"))
    end_year, end_month = map(int, (end_date or start_date).split("-"))

    current_date = datetime(year, month, 1)
    end_date = datetime(end_year, end_month, 1)

    while current_date <= end_date:
        url = f"{base_url}/{current_date.year}/{current_date.month}.json"
        
        try:
            response = make_request(url, {}, configuration["api_key"])
            docs = response.get("response", {}).get("docs", [])
            
            # Add archive_date to each article
            for doc in docs:
                # Format as UTC datetime with midnight time
                doc["archive_date"] = current_date.strftime("%Y-%m-%dT00:00:00+00:00")
            
            articles.extend(docs)
            
            # Checkpoint after each month
            op.checkpoint({"last_sync_date": current_date.strftime("%Y-%m")})
            
        except NYTimesAPIError as e:
            log.warning(f"Failed to fetch articles for {current_date.strftime('%Y-%m')}: {str(e)}")
        
        current_date = current_date.replace(day=1)
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)
    
    return articles

def fetch_most_popular(configuration: dict, endpoint: str) -> List[Dict]:
    """
    Fetch most popular articles from various NY Times endpoints.
    Args:
        configuration: Connector configuration
        endpoint: The specific endpoint (emailed/shared/viewed)
    Returns:
        List of most popular article records
    """
    base_url = "https://api.nytimes.com/svc/mostpopular/v2"
    url = f"{base_url}/{endpoint}/{int(configuration['period'])}.json"
    
    try:
        response = make_request(url, {}, configuration["api_key"])
        return response.get("results", [])
    except NYTimesAPIError as e:
        log.warning(f"Failed to fetch most popular {endpoint} articles: {str(e)}")
        return []

def transform_article(article: Dict) -> Dict:
    """
    Transform article data to match schema requirements.
    Handles nested structures and list fields by converting them to strings.
    """
    pub_date = article.get("pub_date")
    # Convert date to ISO 8601 format with timezone if needed
    def parse_date(date_str):
        if not date_str:
            return None
        try:
            # Try different date formats
            for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
                except ValueError:
                    continue
            return None
        except Exception as e:
            log.warning(f"Failed to parse date {date_str}: {str(e)}")
            return None

    pub_date = parse_date(pub_date)
    
    transformed = {
        "_id": article.get("_id"),
        "web_url": article.get("web_url"),
        "snippet": article.get("snippet"),
        "print_page": article.get("print_page"),
        "print_section": article.get("print_section"),
        "source": article.get("source"),
        "pub_date": pub_date,
        "document_type": article.get("document_type"),
        "news_desk": article.get("news_desk"),
        "section_name": article.get("section_name"),
        "type_of_material": article.get("type_of_material"),
        "word_count": article.get("word_count"),
        "uri": article.get("uri"),
        "archive_date": article.get("archive_date"),
        "abstract": article.get("abstract")
    }
    # Remove None values
    return {k: v for k, v in transformed.items() if v is not None}

def transform_popular_article(article: Dict) -> Dict:
    """
    Transform most popular article data to match schema requirements.
    """
    article_id = str(article.get("id") or article.get("asset_id"))
    published_date = article.get("published_date")
    updated_date = article.get("updated")

    # Convert dates to ISO 8601 format with timezone if needed
    def parse_date(date_str):
        if not date_str:
            return None
        try:
            # Try different date formats
            for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
                except ValueError:
                    continue
            return None
        except Exception as e:
            log.warning(f"Failed to parse date {date_str}: {str(e)}")
            return None

    published_date = parse_date(published_date)
    updated_date = parse_date(updated_date)
    
    transformed = {
        "id": article_id,  # Fallback to asset_id if id is not present
        "url": article.get("url"),
        "adx_keywords": article.get("adx_keywords"),
        "section": article.get("section"),
        "byline": article.get("byline"),
        "type": article.get("type"),
        "title": article.get("title"),
        "abstract": article.get("abstract"),
        "published_date": published_date,
        "source": article.get("source"),
        "updated": updated_date,
        "uri": article.get("uri")
    }
    # Remove None values
    return {k: v for k, v in transformed.items() if v is not None}

def update(configuration: dict, state: dict):
    """
    Main update function to sync data from NY Times API.
    Args:
        configuration: Connector configuration dictionary
        state: State from previous runs
    """
    # Validate configuration
    validate_configuration(configuration=configuration)

    # Get the state variable for the sync
    last_sync_date = state.get("last_sync_date", configuration["start_date"])
    current_date = datetime.now().strftime("%Y-%m")

    try:
        # Fetch and sync archive articles
        articles = fetch_articles(configuration, last_sync_date, current_date)
        for article in articles:
            transformed_article = transform_article(article)
            op.upsert("article", transformed_article)

        # Fetch and sync most popular articles
        for endpoint in ["emailed", "shared", "viewed"]:
            popular_articles = fetch_most_popular(configuration, endpoint)
            for article in popular_articles:
                transformed_article = transform_popular_article(article)
                op.upsert("most_popular", transformed_article)

        # Update state
        new_state = {"last_sync_date": current_date}
        op.checkpoint(new_state)

    except Exception as e:
        log.warning(f"Failed to sync data: {str(e)}")
        raise RuntimeError(f"Failed to sync data: {str(e)}")

# Create the connector object
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Test the connector locally
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    
    connector.debug(configuration=configuration)