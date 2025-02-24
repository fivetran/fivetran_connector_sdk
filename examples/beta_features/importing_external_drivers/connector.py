# Importing External Drivers using installation.sh script file.

# This module implements a connector that requires external driver installation using installation.sh file
# It is an example of using selenium with chrome driver, to download a csv and iterate over the data to update the connector
# NOTE: For selenium to use chrome driver, we need chromeDriver installed using the installation.py file. It also
#     requires chrome to be installed aswell, which is also done in the installation script.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

# Import datetime for handling date and time conversions.
import time
# import json to infer .
import json
import os
import csv
# Imported selenium and webdriver to download the csv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector, Logging as log, Operations as op


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration : dict):
    return [
        {
            "table": "orders",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "title": "STRING",
                "name": "STRING",
                "store": "FLOAT",
                "price": "FLOAT",
                "disc": "FLOAT",
                "profit": "FLOAT",
                "city": "STRING",
                "type": "STRING",
                "mult": "FLOAT"
            }
        }
    ]


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration : dict, state : dict):
    try:
        download_path = os.getcwd() # Get the current working directory
        download_url = configuration.get("download_url")
        download_xpath = configuration.get("download_xpath")
        log.info("Fetching the CSV from desired location.")
        download_csv_with_selenium(download_url, download_path, download_xpath)

        headers = {"id", "title", "name", "store", "price", "disc", "profit", "city", "type", "mult"}
        log.info("Iterating over the downloaded csv.")
        with open("downloaded_file.csv", 'r') as csvfile:
            csv_reader = csv.reader(csvfile)
            # Iterate over each row in the CSV file
            for row in csv_reader:
                upsert_line = {}
                for col_index, value in enumerate(row):
                    upsert_line[headers[col_index]] = value
                yield op.upsert("orders", upsert_line)

            state["timestamp"] = time.time()
            yield op.checkpoint(state)
    except FileNotFoundError:
        log.severe("Error: File not found.")
    except Exception as e:
        log.severe(f"An error occurred: {e}")



def download_csv_with_selenium(url, download_path, download_button_xpath, file_name="downloaded_file.csv"):
    """
    Downloads a CSV file from a website using Selenium and ChromeDriver.

    Args:
        url (str): The URL of the webpage containing the download link.
        download_path (str): The local directory where the CSV file will be saved.
        download_button_xpath (str): The XPath of the download button.
        file_name (str, optional): The desired name of the downloaded CSV file. Defaults to "downloaded_file.csv".

    Returns:
        bool: True if the download was successful, False otherwise.
    """
    try:
        # Configure Chrome options for headless browsing and download preferences
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless=new")  # Use headless mode
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": download_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })

        # Initialize ChromeDriver
        service = Service('./chromedriver') # Ensure chromedriver is in the same directory, or specify the full path.
        driver = webdriver.Chrome(service=service, options=chrome_options)

        driver.get(url)

        # Wait for the download button to be clickable
        download_button = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.XPATH, download_button_xpath))
        )

        # Click the download button
        download_button.click()

        # Wait for the download to complete (adjust timeout as needed)
        time.sleep(10)  # Simple wait; consider more robust methods

        # Rename the downloaded file if necessary
        downloaded_file_path = os.path.join(download_path, os.listdir(download_path)[-1]) #get the last downloaded file.
        new_file_path = os.path.join(download_path, file_name)
        os.rename(downloaded_file_path, new_file_path)

        driver.quit()
    except Exception as e:
        log.severe(f"An error occurred during downloading: {e}")
        if 'driver' in locals():
            driver.quit() #close the driver, even if there was an error.


connector = Connector(update=update, schema=schema)

# required inputs docs https://fivetran.com/docs/connectors/connector-sdk/technical-reference#technicaldetailsrequiredobjectconnector
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)
