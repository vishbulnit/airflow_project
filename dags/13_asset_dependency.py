
from airflow.sdk import dag, task, asset  # type: ignore
from pendulum import datetime  # type: ignore
import os
from asset_12 import fetch_data

@asset(
    schedule = fetch_data,
    # where asset is pointing to
    uri = "/opt/airflow/logs/data/data_processed.csv",
    name = "processed_data"
)

def processed_data(self):
    os.makedirs(os.path.dirname(self.uri), exist_ok= True)
    with open(self.uri, "w") as f:
        f.writelines(f"data processed and written successfully.\\n")



