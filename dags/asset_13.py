from airflow.sdk import dag, task, asset
from pendulum import datetime
import os

@asset(
    schedule="@daily",
    uri="/opt/airflow/logs/data/data_extract.txt", # This is optional but good to include for clarity about asset's location
    name="fetch_data" # If no name provided, it'll take the def() name
 )
def fetch_data(self):

    # Ensure the directory exists 
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)
    
    # Simulate data fetching by write to a file
    with open(self.uri, 'w') as f:
        f.write(f"Data fetched successfully.")

    print(f"Data written to {self.uri}")

