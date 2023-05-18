from airflow import DAG
from datetime import datetime,timedelta 

from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

def _get_message() -> str:
    return 'Load to Hive Success'

default_args = {
    "owner" : "airflow",
    "email_on_failure" : False,
    "email_on_retry" : False,
    "email": "admin@localhost.com",
    "retries" : 1,
    "retry": timedelta(minutes= 5)
}
with DAG(
    "transaction_data_pipeline",
    description = 'A Basic DAG with transfer CSV file to HDFS (Hive) that is Data Warehouse ',
    start_date = datetime(2021,1,1),
    schedule_interval = '@daily', #trigger every day
    default_args = default_args
) as dag:

    #check tesla stock file
    check_csv_file = FileSensor(
        task_id = "csv_check_is_avaiable",
        fs_conn_id = 'data_path',
        filepath = 'transaction_data.csv',
        poke_interval = 5,
        timeout = 10
    )

    #saving csv file to hdfs
    saving_transaction_csv = BashOperator(
        task_id="saving_transaction_csv",
        bash_command="""
            hdfs dfs -mkdir -p /transaction && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/transaction_data.csv /transaction
        """
    )

    #create hive tabel
    create_hive_stock_to_hive = HiveOperator(
        task_id = 'create_hive_table',
        hive_cli_conn_id = 'hive_conn',
        hql= """
            CREATE EXTERNAL TABLE IF NOT EXISTS transaction_table(
                household_key INT,
                BASKET_ID INT,
                DAY INT,
                PRODUCT_ID INT,
                QUANTITY INT,
                SALES_VALUE DOUBLE,
                STORE_ID DOUBLE,
                RETAIL_DISC DOUBLE,
                TRANS_TIME DOUBLE,
                WEEK_NO DOUBLE,
                COUPON_DISC DOUBLE,
                COUPON_MATCH_DISC DOUBLE   
            );       
         """
    )
    #check tesla stock file
    check_pyspark_file = FileSensor(
        task_id = "pyspark_processing_is_avaiable",
        fs_conn_id = 'pyspark_path',
        filepath = 'pyspark_processing.py',
        poke_interval = 5,
        timeout = 10
    )
    #load to hive(datalake) using spark
    load_to_hive = SparkSubmitOperator(
        task_id = 'load_to_hive',
        application = '/opt/airflow/dags/scripts/pyspark_processing.py',
        conn_id = 'spark_conn'
    )
    
    
#data pipeline flow

check_csv_file >> saving_transaction_csv >>  create_hive_stock_to_hive
create_hive_stock_to_hive >> check_pyspark_file >>  load_to_hive