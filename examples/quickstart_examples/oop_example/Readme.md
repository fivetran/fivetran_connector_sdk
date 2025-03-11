# National Parks Example

This example implements a **connector** to fetch, process, and store data from the [National Parks Service API (NPS API)](https://www.nps.gov/subjects/developer/index.htm). The process is built with an **Object-Oriented Programming (OOP)** approach, ensuring modular, maintainable, and reusable code. 

---

## Overview

This example retrieves data from the **NPS API**, processes it, and stores it in a structured format across four tables:  
**Parks**, **Alerts**, **Articles**, and **People**. Each table is encapsulated within its own Python file, ensuring single responsibility and clean organization.

The project follows a modular architecture:
- Each table has its own dedicated class for handling schema definitions and data transformations.
- A centralized **NPS Client** handles API interactions, abstracting away the complexities of HTTP requests.

---

## Features

- **Modular Design:** Each table and the API client are encapsulated in separate files for clarity and reusability.
- **Scalable:** Easily extend it to accommodate additional tables or API endpoints.
- **Customizable:** Modify transformations or table structures without affecting unrelated components.
- **Reliable:** Includes error handling for API interactions and data processing.

---

## Project Structure

```plaintext
├── parks.py         # Handles the Parks table
├── alerts.py        # Handles the Alerts table
├── articles.py      # Handles the Articles table
├── people.py        # Handles the People table
├── nps_client.py    # Handles API initialization and data fetching
├── requirements.txt # Lists dependencies
├── README.md        # Project documentation
└── connector.py     # Main file to run the connector
```


## Run the Connector

```bash
Run fivetran debug 
```

## Output

### Parks

Contains detailed information about national parks

![PARKS](images/Parks.png "Parks Table in DB")

#### Interact with Parks table

```sql

select * from parks

```

### Articles

Stores educational and informational articles about national parks

![Articles](images/Articles.png "Articles Table in DB")

#### Interact with Articles table

```sql

select * from articles

```

### Alerts

Captures active alerts for parks

![Alerts](images/Alerts.png "Alerts Table in DB")



#### Interact with Alerts table

```sql

select * from alerts

```
### People

Lists key figures associated with the parks or their history

![PEOPLE](images/People.png "People Table in DB")

#### Interact with People table

```sql

select * from people

```


