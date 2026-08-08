

from airflow.sdk import dag, task # type: ignore
from typing import Dict, List
from pendulum import datetime # type: ignore

@dag(
    dag_id = "eight_xcom_schedule_dag",
    start_date = datetime(year=2026, month=8, day=8, tz="America/New_York"),
    schedule = "@daily",
    is_paused_upon_creation = False,
    catchup = True,
)

def xcom_branch_condition_dag():

    @task.python
    def extract_task(**kwargs):
        print("Extracting data from 3 sources")
        ti = kwargs['ti']
        input_data = {"S3" : [1,2,3,4,5],
                      "ADLS" : [6,7,8,9,10],
                      "API" : [11,12,13,14,15],
                      "weekend" : "true"
                      }
        ti.xcom_push(key="return_value", value=input_data)

    @task.python
    def transform_task_s3(**kwargs):
        print("Transforming S3 data")
        ti = kwargs['ti']
        input = ti.xcom_pull(key="return_value", task_ids="extract_task")['S3']  # type: ignore
        transformed_data = {"output": [x*10 for x in input]}
        ti.xcom_push(key="return_value", value={"output": transformed_data}) # type: ignore

    @task.python
    def transform_task_adls(**kwargs):
        print("Transforming ADLS data")
        ti = kwargs['ti']
        input = ti.xcom_pull(key="return_value", task_ids="extract_task")['ADLS']  # type: ignore
        transformed_data = {"output": [x*10 for x in input]}
        ti.xcom_push(key="return_value", value={"output": transformed_data}) # type: ignore
  
    @task.python
    def transform_task_api(**kwargs):
        print("Transforming API data")
        ti = kwargs['ti']
        input = ti.xcom_pull(key="return_value", task_ids="extract_task")['API']  # type: ignore
        transformed_data = {"output": [x*10 for x in input]}
        ti.xcom_push(key="return_value", value={"output": transformed_data}) # type: ignore

    @task.branch
    def branch_task(**kwargs):
        ti = kwargs['ti']
        weekend = ti.xcom_pull(key="return_value", task_ids="extract_task")['weekend']  # type: ignore
        if weekend == "true":
            return "finalize_task"
        else:
            return "finalize_non_task"

    @task.bash
    def finalize_task(**kwargs):
        ti = kwargs['ti']
        s3 =  ti.xcom_pull(key="return_value", task_ids="transform_task_s3")['output']  # type: ignore
        adls =  ti.xcom_pull(key="return_value", task_ids="transform_task_adls")['output']  # type: ignore
        api =  ti.xcom_pull(key="return_value", task_ids="transform_task_api")['output']  # type: ignore
        print("This is the final task. All tasks have been executed.")
        return f"echo 'extracted data: {s3}, {adls}, {api}'"

    @task.bash
    def finalize_non_task(**kwargs):
        ti = kwargs['ti']
        result =  ti.xcom_pull(key="return_value", task_ids="extract_task")['weekend']  # type: ignore
        print("This is non finalize task.")
        return f"echo 'extracted data: {result}'"


    # Define the task dependencies
    extract = extract_task()
    transform_s3 = transform_task_s3() # type: ignore
    transform_adls = transform_task_adls() # type: ignore
    transform_api = transform_task_api() # type: ignore
    finalize = finalize_task()
    finalize_non = finalize_non_task()
    branch = branch_task()

    extract >> [transform_s3, transform_adls, transform_api] >> branch >> [finalize, finalize_non] # type: ignore

# Create an instance of the DAG
xcom_branch_condition_dag()