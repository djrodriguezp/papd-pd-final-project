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
    logger.info(str(file_path))
    for covid_file in glob(file_path+"covid19/time_series_covid19_*_global.csv"):

        match = re.search('time_series_covid19_(confirmed|deaths|recovered)_global.csv', os.path.basename(covid_file))
        if match:
            logger.info("Processing Covid-19 file " + covid_file)
            covid_status = match.group(1)

            #Read csv file as pandas DataFrame
            df = (pd.read_csv(covid_file)
                  .rename(columns=DF_COLUMNS))

            df_column_names = df.columns.values.tolist()

            #Use pandas melt function to transform dates columns to rows
            transformed_df = df.melt(id_vars=df_column_names[0:4],
                                     value_vars=df_column_names[4:],
                                     var_name='date', value_name='count')
            transformed_df['date'] = pd.to_datetime(transformed_df['date'], format='%m/%d/%y')
            transformed_df['status'] = covid_status

            #Insert transformed DataFrame into

            with mysql_connection.begin() as connection:
                connection.execute("DELETE FROM covid19.global_data WHERE status='"+covid_status+"'")
                transformed_df.to_sql('global_data', con=connection, schema='covid19', if_exists='append', index=False)



            logger.info(f"Rows inserted {len(df.index)}")
        else:
            logger.warning("Deleting unexpected filename: " + covid_file)

        # Removed processed file
        os.remove(covid_file)

    for wrong_file_name in  glob(file_path + "covid19/*"):
        logger.warning("Deleting unexpected filename: "+wrong_file_name)
        os.remove(wrong_file_name)

    #mysql_connection = MySqlHook(mysql_conn_id=CONNECTION_DB_NAME).get_sqlalchemy_engine()
    #full_path = f'{file_path}/{filename}'
    #df = (pd.read_csv(full_path, encoding = "ISO-8859-1", usecols=COLUMNS.keys(), parse_dates=DATE_COLUMNS)
    #      .rename(columns=COLUMNS)
    #      )

    #with mysql_connection.begin() as connection:
    #    connection.execute("DELETE FROM test.sales WHERE 1=1")
    #    df.to_sql('sales', con=connection, schema='test', if_exists='append', index=False)

    #os.remove(full_path)

    #logger.info(f"Rows inserted {len(df.index)}")





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