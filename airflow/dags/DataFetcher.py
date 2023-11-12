from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
# from airflow.providers.google.cloud.operators.xcom import XComPushOperator
from airflow.models import Variable
import random
import string
# from aux import to_dataframe#, consumer_function

username = Variable.get("OPENSKY_USERNAME")
password = Variable.get("OPENSKY_PASSWORD")
KAFKA_TOPIC = "opensky_stream"#"mytopic"#"opensky_stream"

# Define a function to fetch data from OpenSky
def fetch_data():
    url = "https://opensky-network.org/api/states/all?extended=1"
    auth = (username, password)

    try:
        response = requests.get(url, auth=auth)
        if response.status_code == 200:
            data = response.json()
            print('got data')
            return data
        else:
            print(f"HTTP Error: {response.status_code}")
            return None
    except Exception as e:
        print("Error:", e)
        return None


def fd():
    def generate_random_string(length):
        # Define the characters from which the random string will be generated
        characters = string.ascii_letters + string.digits  # You can customize this further

        # Generate a random string of the specified length
        random_string = ''.join(random.choice(characters) for _ in range(length))

        return random_string
    return generate_random_string(10)

def save_to_parquet(data):
    if data:
        df = pd.DataFrame(data)  # Assuming the data is in a DataFrame format
        df.to_parquet('/path/to/your/s3/bucket/data.parquet', index=False)  # Adjust the path

def produce_to_kafka(data):
    key = str(random.randint(1, 1000))
    print("produced the data", data)
    yield(key, data)

# Define the default_args and DAG
default_args = {
    'owner': 'adayani',
    'retries': 100,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id="opensky_data_processing",
    default_args=default_args,
    description="DAG to fetch, process, and save data from OpenSky",
    start_date=datetime(2023, 10, 26),
    schedule_interval=timedelta(minutes=1),
    catchup=False
) as dag:
    start_task = DummyOperator(task_id="start_task")

    fetch_task = PythonOperator(
        task_id="fetch_opensky_data",
        python_callable=fd#fetch_data
    )

    kafka_producer = ProduceToTopicOperator(
        task_id="opensky_producer",
        topic=KAFKA_TOPIC,  # Replace with your Kafka topic name
        kafka_config_id="kafka_default",
        producer_function=produce_to_kafka,
        producer_function_args=["{{ti.xcom_pull(task_ids='fetch_opensky_data')}}"],
        poll_timeout=10,
    )
    start_task >> fetch_task >> kafka_producer

# def consumer_function(message, *args, **kwargs):
#     for i in args:
#         print(i, 'args')
#     for k, v in kwargs.items():
#         print(k,v, 'kwargs')
#     # ti = context['ti']
#     key = 'opensky_consumer_data'
#     value = message.value()
#     print(value)
#     # ti.xcom_push(key=key, value=value)
#     yield(key, str(value))


# Define a function to save data to Parquet

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
    # kafka_consumer = PythonOperator(
    #     task_id = "opensky_consumer",
    #     python_callable = consumer_function
    # )

    # consumer_to_pandas = PythonOperator(
    #     task_id="to_pandas_dataframe",
    #     python_callable=to_dataframe,
    # )
    
    # kafka_consumer #>> consumer_to_pandas# >> df_to_parquetS3
    # complete this
    # df_to_parquetS3 = PythonOperator(

    # )
    #complete this
    # 
    # Define the task dependencies
    # kafka_producer >> kafka_consumer
    # kafka_consumer >> save_to_parquet_task



# from airflow.decorators import dag
# from airflow.decorators import task
# from datetime import datetime, timedelta
# from airflow.operators.dummy import DummyOperator  # Use DummyOperator for the start
# from airflow.operators.python import PythonOperator
# from airflow.models import Variable
# from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
# from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
# import os
# import requests
# import sys

# username = Variable.get("OPENSKY_USERNAME")
# password = Variable.get("OPENSKY_PASSWORD")

# def fetch_and_publish_to_kafka():
#     print('abde', username, password)
#     url = "https://opensky-network.org/api/states/all?extended=1"
#     auth = (username, password)

#     try:
#         response = requests.get(url, auth=auth)
#         if response.status_code == 200:
#             data = response.json()
#             print(data)
#             # Calculate the size in bytes
#             size_in_bytes = sys.getsizeof(data)

#             # Convert to kilobytes
#             size_in_kilobytes = size_in_bytes / 1024
#             print(size_in_kilobytes)
#             # You can add the KafkaProducer logic here
#             return data
#         else:
#             print(f"HTTP Error: {response.status_code}")
#     except Exception as e:
#         print("Error:", e)

# @dag(
#     dag_id="opensky_data_to_kafka",
#     default_args={
#         'owner': 'adayani',
#         'retries': 5,
#         'retry_delay': timedelta(minutes=10),
#     },
#     description="Sample DAG to fetch data and publish to Kafka",
#     schedule_interval=None,#timedelta(minutes=10),
#     start_date=datetime(2023, 10, 26,18, 45),
#     catchup=False 
# )
# def dag_definition():
#     start_task = DummyOperator(task_id="start_task")

#     fetch_task = PythonOperator(
#         task_id="fetch_opensky_data",
#         python_callable=fetch_and_publish_to_kafka
#     )

#     save_to_parquet = 
    
#     start_task >> fetch_task >> save_to_parquet

# dag_instance = dag_definition()




#     # task1 = BashOperator(
#     #     task_id = 'first_task',
#     #     bash_command="echo hello world, this is first dag."
#     # )
# # from airflow import DAG
# # from datetime import datetime, timedelta
# # import requests
# # # from kafka import KafkaProducer
# # import json
# # import os

# # # Define your OpenSky API credentials

# # # Define the Kafka topic where data will be published
# # kafka_topic = "opensky-data"

# # # Define the Airflow DAG
# # default_args = {
# #     'owner': 'adayani',
# #     # 'depends_on_past': False,
# #     'start_date': datetime(2023, 10, 26),
# #     'retries': 0,  # Set to 0 to run continuously every 2 minutes
# #     'retry_delay': timedelta(minutes=2),
# # }

# # dag = DAG('opensky_data_to_kafka',
# #           default_args=default_args,
# #           schedule_interval=timedelta(minutes=2))

# # Function to fetch data from OpenSky API and publish to Kafka

# # # Define an Airflow task to run the fetch_and_publish_to_kafka function
# # fetch_and_publish_task = PythonOperator(
# #     task_id='fetch_and_publish_task',
# #     python_callable=fetch_and_publish_to_kafka,
# #     dag=dag,
# # )

# # # Set up task dependencies
# # fetch_and_publish_task

# # Convert the string to bytes (assuming UTF-8 encoding)
#     string_bytes = data.encode('utf-8')

#     # Get the length of the byte representation
#     byte_length = len(string_bytes)

#     # Calculate the size in kilobytes (KB)
#     size_in_kb = byte_length / 1024

#     # Print the size in KB
#     print(f"Size of the string: {size_in_kb:.2f} KB")

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
