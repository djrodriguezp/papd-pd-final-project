import os
import re
from glob import glob
from airflow import DAG
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from structlog import get_logger

import pandas as pd

logger = get_logger()

DF_COLUMNS = {
    "Province/State": "state",
    "Country/Region": "country",
    "Lat" : "lat",
    "Long" : "lon"
}

FILE_CONNECTION_NAME = 'monitor_file'
CONNECTION_DB_NAME = 'mysql_db'

def etl_process(**kwargs):
    mysql_connection = MySqlHook(mysql_conn_id=CONNECTION_DB_NAME).get_sqlalchemy_engine()
    logger.info(kwargs["execution_date"])
    file_path = FSHook(FILE_CONNECTION_NAME).get_path()

    for covid_file in glob(file_path+"covid19/time_series_covid19_*_global.csv"):

        match = re.search('time_series_covid19_(confirmed|deaths|recovered)_global.csv', os.path.basename(covid_file))
        if match:
            logger.info("Processing Covid-19 file " + covid_file)
            covid_status = match.group(1)

            #Read csv file as pandas DataFrame
            df = (pd.read_csv(covid_file)
                  .rename(columns=DF_COLUMNS))
            df.state = df.state.fillna('n/a')
            column_names = df.columns.values.tolist()

            #Get deltas of date columns
            diff_df = df[column_names[4:]].diff(axis=1)
            diff_df[column_names[4]] = df[column_names[4]]
            df_data = [df[column_names[0:4]], diff_df[column_names[4:]].astype(int)]
            transformed_df = pd.concat(df_data, axis=1)


            #Use pandas melt function to transform dates columns to rows
            transformed_df = transformed_df.melt(id_vars=column_names[0:4],
                                     value_vars=column_names[4:],
                                     var_name='date', value_name='cases')

            transformed_df['date'] = pd.to_datetime(transformed_df['date'], format='%m/%d/%y')
            transformed_df['status'] = covid_status
            transformed_df = transformed_df.sort_values(['date'])

            #Insert transformed DataFrame into

            with mysql_connection.begin() as connection:
                connection.execute("DELETE FROM covid19.global_data WHERE status='"+covid_status+"'")
                transformed_df.to_sql('global_data', con=connection, schema='covid19', if_exists='append', index=False)



            logger.info(f"Rows inserted {len(df.index)}")
        else:
            logger.warning("Not valid covid filename status: " + covid_file)

    #Clean all files
    logger.info("Cleaning files...")
    for file_name in  glob(file_path + "covid19/*"):
        logger.info("Deleting filename: "+file_name)
        os.remove(file_name)


dag = DAG('covid19_ingestion_dag', description='Dag to Ingest Covid19 Files',
          default_args={
              'owner': 'Daniel Rodríguez - Luis Florian - Antonio Navas ',
              'depends_on_past': False,
              'max_active_runs': 1,
              'start_date': days_ago(5)
          },
          schedule_interval='0 1 * * *',
          catchup=False)

sensor = FileSensor(task_id="file_sensor_task",
                    dag=dag,
                    filepath="covid19/",
                    fs_conn_id=FILE_CONNECTION_NAME,
                    poke_interval=10,
                    timeout=600)

etl = PythonOperator(task_id="covid19_etl",
                     provide_context=True,
                     python_callable=etl_process,
                     dag=dag
                     )

sensor >> etl