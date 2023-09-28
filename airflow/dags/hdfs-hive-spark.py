from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator  # Import DockerOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import subprocess

# Define your DAG
dag = DAG(
    'airflow_test_hadoop_cluster',
    default_args={
        'owner': 'your_name',
        'start_date': datetime(2023, 1, 1),
        # Add other default arguments as needed
    },
    schedule_interval=None,  # Set your desired schedule interval
    catchup=False  # Set to True if you want historical DAG runs to be processed
)



# Dummy task to start the workflow
start_task = DummyOperator(task_id='start', dag=dag)


# SparkSubmitOperator to load breweries.csv from HDFS into Spark
load_into_spark_task = SparkSubmitOperator(
    task_id='load_into_spark',
    application='/airflow/code-for-dag/pysparkscript.py',  # Path to your Spark script
    conn_id='http://localhost:8080/',  # Specify your Spark connection
    conf={
        'spark.driver.memory': '2g',  # Customize Spark configuration as needed
        'spark.executor.memory': '2g',
    },
    dag=dag
)

# Bash Operator to load breweries.csv from HDFS into Hive
load_into_hive_task = BashOperator(
    task_id='load_into_hive',
    bash_command='beeline -u jdbc:hive2://localhost:10000 -n root -f /airflow/code-for-dag/hive-script.sql',
    dag=dag
)

# Dummy task to end the workflow
end_task = DummyOperator(task_id='end', dag=dag)

# Define the task dependencies
start_task >>  load_into_spark_task >> load_into_hive_task >> end_task
