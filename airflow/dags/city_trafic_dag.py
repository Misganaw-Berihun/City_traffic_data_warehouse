import os
import sys
import csv
import pendulum
import logging
import pandas as pd
sys.path.append(os.path.abspath(".."))

from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scripts.data_base_connection import DbConnection

default_args = {
    'owner': 'Misganaw',
    'depends_on_past': False,
    'email': ['msganawberihun10@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'start_date': datetime(2023, 9, 20),
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id="city-trafic",
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
)
def CityTraffic():
    create_vehicle_table = PostgresOperator(
        task_id="create_vehicle_table",
        postgres_conn_id="pg_conn",
        sql="sql/vehicle_schema.sql",
    )

    create_trajectory_table = PostgresOperator(
        task_id="create_trajectory_table",
        postgres_conn_id="pg_conn",
        sql="sql/trajectory_schema.sql",
    )

    @task
    def read_data():
        conn = DbConnection()

        with conn.connect() as c:
            file_path = '/opt/airflow/dags/data/20181024_d1_0830_0900.csv'

            with open(file_path, 'r') as file:
                headers = next(file).strip().split(';')

                insert_query_vehicle = """
                    INSERT INTO vehicle ("track_id", "type", "traveled_distance", "avg_speed")
                    VALUES (%s, %s, %s, %s)
                """
                insert_query_trajectory = """
                    INSERT INTO trajectory ("track_id", "lat", "lon", "speed", "lon_acc", "lat_acc", "time")
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """

                vehicle_rows = []
                trajectory_rows = []

                for line in file:
                    row = line.strip().split(';')[:-1]

                    track_id = int(row[0].strip())
                    type = row[1].strip()
                    traveled_distance = float(row[2].strip())
                    avg_speed = float(row[3].strip())

                    vehicle_rows.append((track_id, type, traveled_distance, avg_speed))

                    len_row = len(row)
                    for i in range(4, len_row, 6):
                        lat = float(row[i].strip())
                        lon = float(row[i + 1].strip())
                        speed = float(row[i + 2].strip())
                        lon_acc = float(row[i + 3].strip())
                        lat_acc = float(row[i + 4].strip())
                        time = float(row[i + 5].strip())

                        trajectory_rows.append((track_id, lat, lon, speed, lon_acc, lat_acc, time))

                
                c.execute(insert_query_vehicle, vehicle_rows)
                c.execute(insert_query_trajectory, trajectory_rows)

    create_vehicle_table >> create_trajectory_table >> read_data()

dag = CityTraffic()
