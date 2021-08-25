import os
import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from operators import (LoadToRedshiftOperator, 
                                DataQualityOperator)
from airflow.utils.task_group import TaskGroup
from etl import demography_analysis
from etl import immigration_analysis
from etl import stage_to_s3
from helpers import Insert_sql
from helpers import Create_sql
from helpers import Copy_sql

import configparser
logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
LOG = logging.getLogger("USA Immigration data pipeline")
LOG.setLevel(os.environ.get("LOG_LEVEL", logging.DEBUG))

config = configparser.ConfigParser()

config.read('dags/config.cfg')

default_args = {
    'owner': 'John_DEV',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}


with DAG (
        'Usa_Immigration_Pipeline',
          default_args=default_args,
          description='Stage, transform and transform immigration data in Redshift with Airflow',
          start_date=datetime.now()
)  as dag:
    start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
    
    build_staging_onpremises_to_s3 = PythonOperator(
        task_id="stage_to_s3",
        python_callable=stage_to_s3
        )

###############################################################################
    with TaskGroup ("stage_immigration_data") as stage_immigration_data:
        create_immigration_tables = PostgresOperator(
            task_id = f"create_staging_immigrations_table",
            dag=dag,
            postgres_conn_id="redshift_conn_id",
            sql=Create_sql.staging_immigrations,
        )
        copy_immigration_data_to_redshift_table = LoadToRedshiftOperator(
            task_id = f"stage_staging_immigrations_table",
            table = "staging_immigrations",
            dag=dag,
            s3_bucket = config['S3']['BUCKET'],
            s3_key= config.get('S3','IMMIGRATION_KEY'),
            ignore_headers = 1,
            data_format='PARQUET'
        )
###############################################################################
    with TaskGroup ("stage_demography_data") as stage_demography_data:
        create_demography_tables = PostgresOperator(
            task_id = f"create_staging_demography_table",
            dag=dag,
            postgres_conn_id="redshift_conn_id",
            sql=Create_sql.staging_cities,
        )
        copy_demography_data_to_redshift_table = LoadToRedshiftOperator(
            task_id = f"stage_staging_cities_table",
            table = "staging_cities",
            dag=dag,
            s3_bucket = config['S3']['BUCKET'],
            s3_key= config['S3']['IMMIGRATION_KEY'],
            ignore_headers = 1,
            data_format='PARQUET'
        )



#################################################################################
    with TaskGroup ("load_immigration_facts_table") as load_immigration_facts_table:
        create_tables = PostgresOperator(
            task_id = f"create_immigrants_facts_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=Create_sql.immigrants_facts
        )

        insert_into_tables = PostgresOperator(
            task_id = f"insert_into_immigrants_facts_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=f"""
                INSERT INTO immigrants_facts (
                    {Insert_sql.immigrations_facts}
                );
            """
        )

#################################################################################
    with TaskGroup ("load_demography_facts_table") as load_demography_facts_table:
        create_tables = PostgresOperator(
            task_id = f"create_us_demography_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=Create_sql.us_geography
        )

        insert_into_tables = PostgresOperator(
            task_id = f"insert_into_us_geography_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=f"""
                INSERT INTO us_geography (
                    {Insert_sql.us_geography}
                );
            """
        )


#################################################################################
    pause_task = DummyOperator (
        task_id = "Break_Session"
    )        
#################################################################################
    with TaskGroup("create_dimensions_table") as create_dimensions_table:
        create_immigrants = PostgresOperator(
            task_id = f"create_immigrants",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=Create_sql.immigrants
        )
        create_us_cities = PostgresOperator(
            task_id = f"create_us_cities",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=Create_sql.us_cities
        )
        create_us_states = PostgresOperator(
            task_id = f"create_us_states",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=Create_sql.us_states
        )
        create_visa_types = PostgresOperator(
            task_id = f"create_visa_types",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=Create_sql.visa_types
        )
        create_transport_modes = PostgresOperator(
            task_id = f"create_transport_modes",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=Create_sql.transport_modes
        )
        create_travel_info = PostgresOperator(
            task_id = f"create_travel_info",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=Create_sql.travel_info
        )
    #################################################################################
    with TaskGroup("insert_into_dimensions_tables") as insert_into_dimensions_tables:
        insert_into_us_cities_table = PostgresOperator(
            task_id = f"insert_into_us_cities_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=f"""
                INSERT INTO us_cities (
                    {Insert_sql.us_cities}
                );
            """
        )
        insert_into_us_states_table = PostgresOperator(
            task_id = f"insert_into_us_states_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=f"""
                INSERT INTO us_states (
                    {Insert_sql.us_states}
                );
            """
        )
        insert_into_visa_types_table = PostgresOperator(
            task_id = f"insert_into_visa_types_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=f"""
                INSERT INTO visa_types (
                    {Insert_sql.visa_types}
                );
            """
        )
        insert_into_transport_modes_table = PostgresOperator(
            task_id = f"insert_into_transport_modes_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=f"""
                INSERT INTO transport_modes (
                    {Insert_sql.transport_modes}
                );
            """
        )
        insert_into_travel_info_table = PostgresOperator(
            task_id = f"insert_into_travel_info_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=f"""
                INSERT INTO travel_info (
                    {Insert_sql.travels_info}
                );
            """
        )
        insert_into_immigrants_table = PostgresOperator(
            task_id = f"insert_into_immigrants_table",
            dag=dag,
            default_args = default_args,
            postgres_conn_id="redshift_conn_id",
            sql=f"""
                INSERT INTO immigrants (
                    {Insert_sql.immigrants}
                );
            """
        )

run_quality_checks = DataQualityOperator(
    task_id="Quality_check_on_the_tables",
    dag=dag,
    redshift_conn_id="redshift_conn_id",
    table=["us_cities", "us_states", "us_geography", 
            "visa_types", "transport_modes", "travel_info",
            "immigrants", "immigrants_facts"]

            )
end_execution = DummyOperator(task_id="End_Execution", dag=dag)



start_operator >> build_staging_onpremises_to_s3
build_staging_onpremises_to_s3 >> stage_immigration_data
build_staging_onpremises_to_s3 >> stage_demography_data
stage_immigration_data >> load_immigration_facts_table
stage_demography_data >> load_demography_facts_table
[load_immigration_facts_table, load_demography_facts_table] >> pause_task
create_dimensions_table << pause_task
create_dimensions_table >> insert_into_dimensions_tables
insert_into_dimensions_tables >> run_quality_checks
run_quality_checks >> end_execution

