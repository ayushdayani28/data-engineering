from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Define the default_args for DAG 2
default_args_dag2 = {
    'owner': 'adayani',
    'retries': 0,  # Adjust the number of retries as needed
    'retry_delay': timedelta(minutes=1)
}

# Define DAG 2
with DAG(
    dag_id="consume_and_process_kafka_data",
    default_args=default_args_dag2,
    description="DAG to consume and process data from Kafka with PySpark",
    start_date=datetime(2023, 10, 26),
    schedule_interval=timedelta(minutes=5),  # Adjust the schedule interval
    catchup=False
) as dag:

    start_task_dag2 = DummyOperator(task_id="start_task_dag2")

    # Consume data from Kafka
    consume_kafka_data = ConsumeFromTopicOperator(
        task_id="consume_kafka_data",
        topics=["opensky_stream"],  # Use the same Kafka topic as in DAG 1
        kafka_config_id="kafka_default",
    )
     # kafka_consumer=ConsumeFromTopicOperator(
    #     task_id="opensky_consumer",
    #     kafka_config_id="kafka_default",
    #     topics=[KAFKA_TOPIC],
    #     apply_function=consumer_function,
    #     # apply_function_args=[ti],#["{{ti.xcom_push}}"],
    #     max_messages=1,
    #     do_xcom_push=True,
    #     # provide_context=True, 
    #     # commit_cadence='never'
    # )

    # Define a Python callable to process the Kafka data with PySpark
    def process_kafka_data(data):
        # You can use SparkSubmitOperator or a custom Python code to process the data with PySpark.
        # Here's an example using SparkSubmitOperator:
        print(data)
        submit_task = SparkSubmitOperator(
            task_id="process_kafka_data_with_pyspark",
            application="/path/to/your/pyspark_script.py",  # Specify your PySpark script
            conn_id="spark_default",  # Adjust to your Spark connection ID
            total_executor_cores=2,  # Adjust the Spark configurations as needed
            executor_memory="2g",
            num_executors=1,
            application_args=["{{ ti.xcom_pull(task_ids='consume_kafka_data') }}"]
        )
        return submit_task

    # Execute the PySpark processing task
    process_kafka_data_task = PythonOperator(
        task_id="execute_pyspark_processing",
        python_callable=process_kafka_data,
        op_args =["{{ti.xcom_pull(task_ids='consume_kafka_data') }}"]
    )

    start_task_dag2 >> consume_kafka_data >> process_kafka_data_task
