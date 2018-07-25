from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'aervits',
    'start_date': datetime(2018, 7, 24),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('traffic_analysis', default_args=default_args)

# Create external table from hdfs file
t1 = HiveOperator(
    task_id= 'create_external_table',
    hql="""CREATE EXTERNAL TABLE IF NOT EXISTS traffic_csv (
  `DT` STRING,
  `TM` STRING,
  `BOROUGH` STRING,
  `ZIP CODE` STRING,
  `LATITUDE` STRING,
  `LONGITUDE` STRING,
  `LOCATION` STRING,
  `ON STREET NAME` STRING,
  `CROSS STREET NAME` STRING,
  `OFF STREET NAME` STRING,
  `NUMBER OF PERSONS INJURED` STRING,
  `NUMBER OF PERSONS KILLED` STRING,
  `NUMBER OF PEDESTRIANS INJURED` STRING,
  `NUMBER OF PEDESTRIANS KILLED` STRING,
  `NUMBER OF CYCLIST INJURED` STRING,
  `NUMBER OF CYCLIST KILLED` STRING,
  `NUMBER OF MOTORIST INJURED` STRING,
  `NUMBER OF MOTORIST KILLED` STRING,
  `CONTRIBUTING FACTOR VEHICLE 1` STRING,
  `CONTRIBUTING FACTOR VEHICLE 2` STRING,
  `CONTRIBUTING FACTOR VEHICLE 3` STRING,
  `CONTRIBUTING FACTOR VEHICLE 4` STRING,
  `CONTRIBUTING FACTOR VEHICLE 5` STRING,
  `UNIQUE KEY` STRING,
  `VEHICLE TYPE CODE 1` STRING,
  `VEHICLE TYPE CODE 2` STRING,
  `VEHICLE TYPE CODE 3` STRING,
  `VEHICLE TYPE CODE 4` STRING, 
  `VEHICLE TYPE CODE 5` STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
  LOCATION '/tmp/data';""",
    dag=dag)

# Create permanent table
t2 = HiveOperator(
    task_id= 'create_permanent_table',
    hql="""CREATE TABLE IF NOT EXISTS traffic (
    `DT` STRING,
    `TM` STRING,
    `ZIP CODE` STRING,
    `LATITUDE` STRING,
    `LONGITUDE` STRING,
    `LOCATION` STRING,
    `ON STREET NAME` STRING,
    `CROSS STREET NAME` STRING,
    `OFF STREET NAME` STRING,
    `NUMBER OF PERSONS INJURED` STRING,
    `NUMBER OF PERSONS KILLED` STRING,
    `NUMBER OF PEDESTRIANS INJURED` STRING,
    `NUMBER OF PEDESTRIANS KILLED` STRING,
    `NUMBER OF CYCLIST INJURED` STRING,
    `NUMBER OF CYCLIST KILLED` STRING,
    `NUMBER OF MOTORIST INJURED` STRING,
    `NUMBER OF MOTORIST KILLED` STRING,
    `CONTRIBUTING FACTOR VEHICLE 1` STRING,
    `CONTRIBUTING FACTOR VEHICLE 2` STRING,
    `CONTRIBUTING FACTOR VEHICLE 3` STRING,
    `CONTRIBUTING FACTOR VEHICLE 4` STRING,
    `CONTRIBUTING FACTOR VEHICLE 5` STRING,
    `UNIQUE KEY` STRING,
    `VEHICLE TYPE CODE 1` STRING,
    `VEHICLE TYPE CODE 2` STRING,
    `VEHICLE TYPE CODE 3` STRING,
    `VEHICLE TYPE CODE 4` STRING, 
    `VEHICLE TYPE CODE 5` STRING)
    PARTITIONED BY (BOROUGH STRING)
      ROW FORMAT DELIMITED
      FIELDS TERMINATED BY ','
      STORED AS ORC;""",
    dag=dag)

# Insert into permanent table
t3 = HiveOperator(
    task_id= 'insert_to_permanent',
    hql="""SET hive.exec.dynamic.partition.mode=nonstrict;
      INSERT OVERWRITE TABLE TRAFFIC PARTITION (BOROUGH) 
        SELECT `DT`, 
               `TM`, 
               `ZIP CODE`, 
               `LATITUDE`, 
               `LONGITUDE`,
               `LOCATION`,
               `ON STREET NAME`,
               `CROSS STREET NAME`,
               `OFF STREET NAME`,
               `NUMBER OF PERSONS INJURED`,
               `NUMBER OF PERSONS KILLED`,
               `NUMBER OF PEDESTRIANS INJURED`,
               `NUMBER OF PEDESTRIANS KILLED`,
               `NUMBER OF CYCLIST INJURED`,
               `NUMBER OF CYCLIST KILLED`,
               `NUMBER OF MOTORIST INJURED`,
               `NUMBER OF MOTORIST KILLED`,
               `CONTRIBUTING FACTOR VEHICLE 1`,
               `CONTRIBUTING FACTOR VEHICLE 2`,
               `CONTRIBUTING FACTOR VEHICLE 3`,
               `CONTRIBUTING FACTOR VEHICLE 4`,
               `CONTRIBUTING FACTOR VEHICLE 5`,
               `UNIQUE KEY`,
               `VEHICLE TYPE CODE 1`,
               `VEHICLE TYPE CODE 2`,
               `VEHICLE TYPE CODE 3`,
               `VEHICLE TYPE CODE 4`, 
               `VEHICLE TYPE CODE 5`,
               `BOROUGH`
        FROM TRAFFIC_CSV;""",
    dag=dag)


# Drop unnecessary partition
t4 = HiveOperator(
    task_id= 'drop_unnecessary_partition1',
    hql="ALTER TABLE TRAFFIC DROP PARTITION (BOROUGH='__HIVE_DEFAULT_PARTITION__');",
    dag=dag)


# Drop unnecessary partition
t5 = HiveOperator(
    task_id= 'drop_unnecessary_partition2',
    hql="ALTER TABLE TRAFFIC DROP PARTITION (BOROUGH=0);",
    dag=dag)

# Run PySpark Analysis #pyspark directory is by default homedir
""" t6 = SparkSubmitOperator(
    task_id= 'run_spark_analysis',
    application='/tmp/pyspark/sparkHiveAirflow.py',
    name='Spark Analysis',
    executor_cores=1,
    executor_memory='2g',
    driver_memory='4g',
    verbose='true',
    dag=dag) """

pyspark_job = """
    /usr/hdp/current/spark2-client/bin/spark-submit /tmp/pyspark/sparkHiveAirflow.py
    """

# Run PySpark via BashOperator
t7 = BashOperator(
        task_id= 'run_pyspark',
        bash_command=pyspark_job,
        #env='SPARK_MAJOR_VERSION=2',
        dag=dag)

# defining the job dependency
t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t3)
t5.set_upstream(t4)
# t6.set_upstream(t5)
t7.set_upstream(t5)