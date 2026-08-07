
from airflow.sdk import dag, task # type: ignore
from typing import Dict, List

@dag(
    dag_id = "fifth_xcom_manual_dag"
)

def xcom_manual_dag():

    @task.python
    def first_task(**kwargs):
        print("Input data")
        ti = kwargs['ti']
        input_data = {"output" : [1,2,3,4,5,6,7,8,9,10]}
        ti.xcom_push(key="return_result", value=input_data)

    @task.python
    def second_task(**kwargs):
        print("Transforming data")
        ti = kwargs['ti']
        input = ti.xcom_pull(key="return_result", task_ids="first_task")['output']  # type: ignore
        transformed_data = {"output": [x*2 for x in input]}
        ti.xcom_push(key="return_result", value={"output": transformed_data}) # type: ignore
  
    @task.python
    def third_task(**kwargs):
        ti = kwargs['ti']
        load_data =  ti.xcom_pull(key="return_result", task_ids="second_task")['output']  # type: ignore
        return load_data

    # Define the task dependencies
    first = first_task()
    second = second_task() # type: ignore
    third = third_task()

    first >> second >> third  # type: ignore
 
# Create an instance of the DAG
xcom_manual_dag()