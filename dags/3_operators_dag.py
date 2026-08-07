
from airflow.sdk import dag, task # type: ignore
from airflow.operators.bash import BashOperator # type: ignore

@dag(
    dag_id = "operators_dag"
)

def operators_dag():

    @task.python
    def first_task():
        print("This is the first task.")

    @task.python
    def second_task():
        print("This is the second task.")

    @task.bash
    def bash_task_modern():
        return "echo https://airflow.apache.org"

    bash_task_old = BashOperator(
        task_id='bash_task_old',
        bash_command="echo https://airflow.apache.org"
    )

    
    # Define the task dependencies
    first = first_task()
    second = second_task()
    bash_modern = bash_task_modern()
    bash_old = bash_task_old

    first >> second >> bash_modern >> bash_old

# Create an instance of the DAG
operators_dag()
