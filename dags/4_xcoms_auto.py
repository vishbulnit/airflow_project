

from airflow.sdk import dag, task # type: ignore
from typing import Dict, List

@dag(
    dag_id = "forth_xcom_auto_dag"
)

def xcom_auto_dag():

    @task.python
    def first_task():
        print("Extracting data...this is the first task.")
        extracted_data = [1,2,3,4,5,6,7,8,9,10]
        return extracted_data

    @task.python
    def second_task(data:List[int]):
        print("Transforming data...this is the second task.")
        input = data
        transformed_data = [x*2 for x in input]
        result = {"output": transformed_data}
        return result

    @task.python
    def third_task(data:Dict):
        load_data  = data
        return load_data

    # Define the task dependencies
    first = first_task()
    second = second_task(first) # type: ignore
    third = third_task(second)

# Create an instance of the DAG
xcom_auto_dag()