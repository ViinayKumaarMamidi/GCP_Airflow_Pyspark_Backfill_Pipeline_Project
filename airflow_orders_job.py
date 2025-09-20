from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import (
    #DataprocSubmitPySparkJobOperator,  # This operator is not available any more to support 
    DataprocCreateClusterOperator, 
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.param import Param

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 20),
}

dag = DAG(
    'orders_backfilling_dag',
    default_args=default_args,
    description='A DAG to run Spark job with input parameter on Dataproc ',
    schedule_interval=None,
    catchup=False,
    tags=['dev'],
    params={
        'execution_date': Param(default='NA', type='string', description='Execution date in yyyymmdd format'),
    }
)

# Python function to get the execution date

def get_execution_date(ds_nodash, **kwargs):
    execution_date = kwargs['params'].get('execution_date', 'NA')
    if execution_date == 'NA':
        execution_date = ds_nodash
    return execution_date


# PythonOperator to call the get_execution_date function

get_execution_date_task = PythonOperator(
    task_id='get_execution_date',
    python_callable=get_execution_date,
    provide_context=True,
    op_kwargs={'ds_nodash': '{{ ds_nodash }}'},
    dag=dag,
)

# {{ ds_nodash }} = current execution date of dag without dashes

#  2024-07-28

# {{ ds_nodash }} = 20240728

# Dummy BashOperator to print the derived date_variable
# print_date = BashOperator(
#     task_id='print_date',
#     bash_command='echo "Derived execution date: {{ ti.xcom_pull(task_ids=\'get_execution_date\') }}" && echo "hi"',
#     dag=dag,
# )

# Create a Variable in the Airflow with Key as cluster_details and Value- please copy and paste below DICT

# {
#     "CLUSTER_NAME" : "hadoop-cluster-new",
#     "PROJECT_ID" : "psyched-service-442305-q1",
#     "REGION" : "us-central1"
# }

# Define cluster config
CLUSTER_NAME = 'hadoop-cluster-new'
PROJECT_ID = 'constant-grove-472014-c1'
REGION = 'us-east1'
CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1, # Master node
        'machine_type_uri': 'n1-standard-2',  # Machine type
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'worker_config': {
        'num_instances': 2,  # Worker nodes
        'machine_type_uri': 'n1-standard-2',  # Machine type
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'software_config': {
        'image_version': '2.2.26-debian12'  # Image version
    }
}


create_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT_ID,
    region=REGION,
    cluster_config=CLUSTER_CONFIG,
    dag=dag,
)


# Fetch configuration from Airflow variables

config = Variable.get("cluster_details", deserialize_json=True)
CLUSTER_NAME = config['CLUSTER_NAME']
PROJECT_ID = config['PROJECT_ID']
REGION = config['REGION']


#pyspark_job_file_path = 'gs://airflow-projetcs-gds/airflow-project-2/spark_code/orders_data_process.py'


# submit_pyspark_job = DataprocSubmitPySparkJobOperator(
#     task_id='submit_pyspark_job',
#     main=pyspark_job_file_path,
#     arguments=['--date={{ ti.xcom_pull(task_ids=\'get_execution_date\') }}'],  # Passing date as an argument to the PySpark script
#     cluster_name=CLUSTER_NAME,
#     region=REGION,
#     project_id=PROJECT_ID,
#     dag=dag,
# )


pyspark_job_file_path = 'gs://airflow-project-dev2/spark_code/orders_data_process.py'


submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_pyspark_job',
    project_id=PROJECT_ID,
    region=REGION,
    job={
        "placement":{"cluster_name":CLUSTER_NAME},
        "pyspark_job":{
            "main_python_file_uri":pyspark_job_file_path,
            "args":['--date={{ ti.xcom_pull(task_ids=\'get_execution_date\') }}'],
        },
        
    },     
    dag=dag,
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule='all_done',  # ensures cluster deletion even if Spark job fails
    dag=dag,
)



# Set the task dependencies
get_execution_date_task >> create_cluster >> submit_pyspark_job >> delete_cluster


# arguments=['--start_date=2024-01-02', '--end_date=2024-01-03']