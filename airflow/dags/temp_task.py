from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import json

def my_python_function():
    result1 = 42
    yield json.dumps({'result1': result1})  # Yield a dictionary
    # result2 = "Hello, World!"
    # yield {'result2': result2}  # Yield another dictionary

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 11, 1),
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    schedule_interval=None,  # Set the desired schedule interval
)

task_with_yield = PythonOperator(
    task_id='my_python_task',
    python_callable=my_python_function,
    provide_context=True,  # If you need access to context variables
    dag=dag,
)

def process_result(result, **kwargs):
    # Do something with the yielded result
    print(f"Received result: {result}")

result_handler_task = PythonOperator(
    task_id='result_handler_task',
    python_callable=process_result,
    provide_context=True,
    op_args=[task_with_yield.output],  # Capture the yielded results
    dag=dag,
)

task_with_yield >> result_handler_task  # Set task dependencies

