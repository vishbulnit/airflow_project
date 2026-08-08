
from airflow.sdk import dag, task # type: ignore
from pendulum import datetime # type: ignore 
from airflow.timetables.events import EventsTimetable # type: ignore

special_date = EventsTimetable(
    event_date=[
        datetime(year=2026, month=1, day=1),
        datetime(year=2026, month=1, day=26),
        datetime(year=2026, month=1, day=31)
    ]
)

@dag(
    dag_id="eleven_special_date_schedule_dag",
    schedule=special_date,
    start_date=datetime(year=2026, month=1, day=1, tz="America/New_York"),
    end_date=datetime(year=2026, month=1, day=31, tz="America/New_York"),
    catchup=True
)

def incremental_load_dag():
    @task.python
    def incremental_fetch(**kwargs):
        execution_date = kwargs['logical_date']
        print(f"running task for special dates {execution_date}")

    special = incremental_fetch()

incremental_load_dag()




