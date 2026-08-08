
from airflow.sdk import dag, task # type: ignore
from pendulum import datetime, duration # type: ignore
from airflow.timetables.interval import CronDeltaIntervalTimetable  # type: ignore

@dag(
    dag_id="ten_incremental_load_dag",
    schedule=CronDeltaIntervalTimetable("@daily", timezone="America/New_York"),
    start_date=datetime(year=2026, month=8, day=7, tz="America/New_York"),
    end_date=datetime(year=2026, month=8, day=10, tz="America/New_York"),
    catchup=True
)

def incremental_load_dag():
    @task.python
    def incremental_fetch(**kwargs):
        start_date = kwargs['data_interval_start']
        end_date = kwargs['data_interval_end']
        print(f"Fetching data from {start_date} to {end_date}")

    @task.bash
    def incremental_process():
        return f"Processing the fetched data {data_interval_start} to {data_interval_end}"  # type: ignore
        

    incremental_fetch() >> incremental_process() # type: ignore


incremental_load_dag()




