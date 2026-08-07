
from airflow.sdk import dag, task # type: ignore

@dag(
    dag_id = "second_dag"
)

def second_versioning_dag():

    @task.python
    def first_task():
        print("This is the first task.")

    @task.python
    def second_task():
        print("This is the second task.")

    @task.python
    def third_task():
        print("This is the third task. All tasks have been executed.")

    @task.python
    def versioning_task():
        print("This is the versioning task. This is special task.")        

    # Define the task dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    versioning = versioning_task()
    first >> second >> third >> versioning # type: ignore

# Create an instance of the DAG
second_versioning_dag()
