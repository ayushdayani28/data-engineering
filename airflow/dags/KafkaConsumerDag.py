from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from airflow.models import Variable
import json
# Define the default_args for DAG 2
default_args_dag2 = {
    'owner': 'adayani',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

# Define DAG 2
with DAG(
    dag_id="consume_and_process_kafka_data",
    default_args=default_args_dag2,
    description="DAG to consume and process data from Kafka with PySpark",
    start_date=datetime(2023, 10, 31),
    schedule_interval=None,  # Set the schedule interval as needed
    catchup=False
) as dag:

    start_task_dag2 = DummyOperator(task_id="start_task_dag2")

    def consume_and_forward():
        # Create a KafkaConsumer to listen for new messages
        consumer = KafkaConsumer(
            'opensky_stream',
            bootstrap_servers='localhost:9092',
            group_id='opensky_consumers',
            auto_offset_reset='latest',
            enable_auto_commit=True  # Disable auto commit to control offset manually
        )

        # Collect Kafka messages into a list
        # messages = []

        # Set a limit on the number of messages to consume
        # max_messages = 10

        # Continuously listen for new messages and collect them
        for message in consumer:
            message_value = message.value.decode('utf-8')
            print(f"Received Kafka Message: {message_value}")
            # messages.append(json.dumps(message_value))
            yield(message_value)
            # If the number of messages collected reaches the limit, break the loop
            # if len(messages) >= max_messages:
            #     break

        # Return the list of messages
        # return messages

    # Use a PythonOperator to execute the custom Kafka consumer logic
    kafka_consumer_task = PythonOperator(
        task_id="kafka_consumer_task",
        python_callable=consume_and_forward
    )

    # Define a Python callable to process the Kafka data with PySpark
    def process_kafka_data(messages):
        # Your message processing logic goes here
        for message in messages:
            print("Processing data:", message)

    process_kafka_data_task = PythonOperator(
        task_id="process_kafka_data_task",
        python_callable=process_kafka_data,
        op_args=[kafka_consumer_task.output],  # Pass the output from the previous task
    )

    start_task_dag2 >> kafka_consumer_task >> process_kafka_data_task


# from airflow import DAG
# from airflow.operators.dummy import DummyOperator
# from airflow.operators.python import PythonOperator
# from datetime import datetime, timedelta
# from kafka import KafkaConsumer
# import json

# # Define the default_args for DAG 2
# default_args_dag2 = {
#     'owner': 'adayani',
#     'retries': 0,
#     'retry_delay': timedelta(minutes=1)
# }

# # Define DAG 2
# with DAG(
#     dag_id="consume_and_process_kafka_data",
#     default_args=default_args_dag2,
#     description="DAG to consume and process data from Kafka with PySpark",
#     start_date=datetime(2023, 10, 31),
#     schedule_interval=None,  # Set the schedule interval as needed
#     catchup=False
# ) as dag:

#     start_task_dag2 = DummyOperator(task_id="start_task_dag2")

#     def consume_and_forward():
#         # Create a KafkaConsumer to listen for new messages
#         consumer = KafkaConsumer(
#             'opensky_stream',
#             bootstrap_servers='localhost:9092',
#             group_id='opensky_consumers',
#             auto_offset_reset='earliest',
#             enable_auto_commit=True  # Disable auto commit to control offset manually
#         )

#         # Continuously listen for new messages and forward them
#         for message in consumer:
#             message_value = message.value.decode('utf-8')
#             print(f"Received Kafka Message: {message_value}")

#             # Forward the message to the next task by returning it directly
#             print(type(message_value), message_value)
#             # key = str(random.randint(1, 1000))
#             yield(json.dumps(message_value))

#     # Use a PythonOperator to execute the custom Kafka consumer logic
#     kafka_consumer_task = PythonOperator(
#         task_id="kafka_consumer_task",
#         python_callable=consume_and_forward,
#         provide_context=True  # Enable access to the task instance
#     )

#     # # Define a Python callable to process the Kafka data with PySpark
#     # def process_kafka_data(data):
#     #     # Your message processing logic goes here
#     #     print("Processing data:", data)

#     # process_kafka_data_task = PythonOperator(
#     #     task_id="process_kafka_data_task",
#     #     python_callable=process_kafka_data,
#     #     provide_context=True
#     # )

#     start_task_dag2 >> kafka_consumer_task# >> process_kafka_data_task
