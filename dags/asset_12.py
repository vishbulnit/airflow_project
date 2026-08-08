

from airflow.sdk import dag, task, asset  # type: ignore
from pendulum import datetime  # type: ignore
import os

@asset(
    schedule = "@daily",
    # where asset is pointing to
    uri = "/opt/airflow/logs/data/data_extract.csv",
    name = "fetch_data"
)

def fetch_data(self):
    os.makedirs(os.path.dirname(self.uri), exist_ok= True)
    with open(self.uri, "w") as f:
        f.write(f"data fetched ans written successfully.\\n") 
    


