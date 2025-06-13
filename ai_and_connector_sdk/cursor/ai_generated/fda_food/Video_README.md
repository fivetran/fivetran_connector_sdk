# Building a Fivetran Connector SDK with AI: FDA Food API Integration

## 🎥 Demo Overview
This repository contains a demonstration of building a Fivetran Connector SDK solution using AI assistance. The demo showcases the end-to-end process of creating, testing, and deploying a connector for the FDA Food Enforcement API.

## 🎯 What You'll Learn
- How to leverage AI for connector development
- Setting up a structured development environment
- Implementing best practices for Fivetran connector development
- Handling API authentication and rate limiting
- Testing and debugging connector functionality
- Deploying to Fivetran

## 🎬 Video Demo
This README accompanies a video demonstration that walks through the entire development process. Watch the video to see:
- Real-time AI-assisted development
- Debugging and problem-solving
- Data validation and testing
- Deployment to Fivetran

_{Insert Video Link}_

## 🛠️ Development Process

### 1. Preparation & Setup
- Created a structured project directory
- Added system instructions for AI assistance
- Gathered API documentation and context
- Set up initial project files:
  - `connector.py`
  - `configuration.json`
  - `requirements.txt`

### 2. AI-Assisted Development
The development process was broken into three key sections:

#### Source Information & Context
- FDA Food API endpoint documentation
- Authentication requirements
- Example queries
- Searchable fields documentation

#### Implementation Requirements
- Dynamic table creation based on API endpoints
- Dictionary flattening for tabular structure
- Primary key definition for schema objects
- Fivetran-compatible data transformation

#### Development Commands
- AI-generated connector code
- Configuration setup
- Requirements management

### 3. Testing & Iteration
- Initial connector testing revealed authentication issues
- AI-assisted debugging and modification
- Successfully tested with and without API key
- Data validation using DuckDB
- Review of table structures and sample data

### 4. Deployment
- Successful deployment to Fivetran
- Verification of data sync
- Destination connection (Snowflake) testing

## 📊 Key Features
- **Flexible Authentication**: Works with or without API key
- **Rate Limiting**: Built-in handling of API rate limits
- **Robust Error Handling**: Retry logic and error reporting
- **Data Transformation**: Automatic flattening of nested JSON
- **State Management**: Efficient sync state tracking
- **Date Handling**: Proper timezone and date format management

## 🚀 Getting Started

### Prerequisites
- Python 3.x
- Fivetran account
- FDA Food API access (optional)

### Configuration
The connector can be configured with or without an API key:
```json
{
    "base_url": "https://api.fda.gov/food/enforcement.json",
    "api_key": "your_api_key_here",  // Optional
    "batch_size": 100,
    "rate_limit_pause": 0.25
}
```

### Running the Connector
1. Install requirements: `pip install -r requirements.txt`
2. Configure your `config.json`
3. Run in debug mode: `python connector.py`

## 📝 Notes
- The connector implements pagination for large datasets
- Rate limiting is automatically adjusted based on API key presence
- For testing purposes, the connector processes a limited number of batches by default

## 🤝 Contributing
We welcome contributions! Feel free to:
- Test the connector with different configurations
- Add support for additional FDA API endpoints
- Improve error handling or data transformation
- Submit requests to our public repository

## 📚 Resources
- [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connectors/connector-sdk)
- [FDA Food API Documentation](https://open.fda.gov/apis/food/)
- [Template Connector Repository](https://github.com/fivetran/connector-template)

---

*Built with ❤️ using Fivetran's Connector SDK and AI assistance* 
