
from airflow.sdk import dag, task # type: ignore
from dag_orchestration_1 import first_dag_orchestration
from dag_orchestration_2 import second_dag_orchestration
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # type: ignore

@dag
def parent_dag_orchestration():

    trigger_first_dag = TriggerDagRunOperator(
        task_id="trigger_first_orchestration_dag",
        trigger_dag_id="first_dag_orchestration",
        #wait_for_completion=True # this will wait for the first DAG to complete before triggering the second DAG, a bit slow
    )

    trigger_second_dag = TriggerDagRunOperator(
        task_id="trigger_second_orchestration_dag",
        trigger_dag_id="second_dag_orchestration",
        #wait_for_completion=True  # this will wait for the first DAG to complete before triggering the second DAG, a bit slow
    )
    # Create an instance of the DAG

    trigger_first_dag >> trigger_second_dag

parent_dag_orchestration()