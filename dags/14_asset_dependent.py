from airflow.sdk import dag, task, asset
from pendulum import datetime
import os
from asset_13 import fetch_data

@asset(
    schedule=fetch_data,
    uri="/opt/airflow/logs/data/data_processed.txt", # This is optional but good to include for clarity about asset's location
    name="process_data" # If no name provided, it'll take the def() name
 )
def process_data(self):

    # Ensure the directory exists 
    os.makedirs(os.path.dirname(self.uri, exist_ok=True))
    
    # Simulate data fetching by write to a file
    with open(self.uri, 'w') as f:
        f.write(f"Data processed successfully.")

    print(f"Data processed to {self.uri}")

