from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import ( 
 DataprocCreateClusterOperator,DataprocSubmitPySparkJobOperator,DataprocDeleteClusterOperator
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.dates import days_ago
#defining default args

default_args ={
    'owner':'vivek',
    'depends_on_past':False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

#defining dags

dag= DAG(
    'gcp_dataproc_pyspark_dag',
    default_args = default_args,
    description = 'Airflow Assignment solution one',
    start_date = days_ago(1),
    schedule_interval = timedelta(days=1),
    tags= ['assignments'],
    )
BUCKET_NAME='airflow_assignment_one'
PROJECT_ID='pyspark-learning-407410'
CLUSTER_NAME='aiflow-assignment'
REGION='us-east1'

#creating operator for sensing files

data_sensor_task = GCSObjectExistenceSensor(
    task_id='sensor_task',
    bucket = 'airflow_assignment_one',
    object='input_files/employee.csv',
    poke_interval = 300,
    timeout=43200,
    mode='poke',
    dag=dag,
    )
    
#cluster_configs 
 
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 30},
    },
    "software_config":{
        'image_version':'2.1-debian11'}
}    
 #creating dataproc cluster

cluster_creation =  DataprocCreateClusterOperator(
    task_id='creating_cluster',
    project_id=PROJECT_ID,
    cluster_config=CLUSTER_CONFIG,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    dag=dag,
    )
   
PYSPARK_JOB = {
    'main_python_file_uri': 'gs://airflow_assignment_one/python_file/spark_app.py'
}
   
submitting_job = DataprocSubmitPySparkJobOperator(
    task_id="pyspark_task",
    main=PYSPARK_JOB['main_python_file_uri'],
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

deleting_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    dag=dag,
    )

data_sensor_task >> cluster_creation >> submitting_job >> deleting_cluster