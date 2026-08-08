
from airflow.sdk import dag, task # type: ignore

@dag(
    dag_id = "second_dag_orchestration"
)

def second_dag_orchestration():

    @task.python
    def first_task():
        with open("/opt/airflow/logs/data/output.txt", "a") as f:
            f.write("This is the second orchestration task 1.\n")

    @task.python
    def second_task():
        with open("/opt/airflow/logs/data/output.txt", "a") as f:
            f.write("This is the second orchestration task 2.\n")

    @task.python
    def third_task():
        with open("/opt/airflow/logs/data/output.txt", "a") as f:
            f.write("This is the second orchestration task 3.\n")

    # Define the task dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

# Create an instance of the DAG
second_dag_orchestration()
