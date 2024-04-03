from datetime import datetime, timedelta
from airflow.decorators import dag, task
import json
from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
import pendulum
import pandas as pd

# 2. Создайте новый dag;
@dag(
    dag_id="dag_HW8",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    )
def dag_HW8():
    @task
    def read_booking():
        booking = pd.read_csv("/home/olesya/airflow/data/booking.csv")
        return booking

    @task
    def read_client():
        client = pd.read_csv("/home/olesya/airflow/data/client.csv")
        return client

    @task
    def read_hotel():
        hotel = pd.read_csv("/home/olesya/airflow/data/hotel.csv")
        return hotel

    @task
    def transform_data(**kwargs):
        ti = kwargs['ti']
        #принимаем датафреймы
        booking = ti.xcom_pull(task_ids = 'read_booking')
        client = ti.xcom_pull(task_ids = 'read_client')
        hotel = ti.xcom_pull(task_ids = 'read_hotel')
        #Объедините все таблицы в одну
        data = pd.merge(booking, client, on='client_id')
        data.rename(columns={'name' : 'client_name', 'type' : 'client_type'}, inplace = True)
        data = pd.merge(data, hotel, on = 'hotel_id')
        data.rename(columns={'name': 'hotel_name'}, inplace = True)
        #Приведите даты к одному виду
        data.booking_date = data.booking_date.apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))
        #Удалите невалидные колонки - не вижу, заполним пропуски:
        #есть пропущенный возраст, заполним средним значением
        data['age'].fillna(data['age'].mean(), inplace=True)
        data['age'] = data['age'].astype(int)
        # есть пустое знчение цены, поставим туда среднюю по категории standard_1_bed
        data.booking_cost.fillna(data[data.room_type == 'standard_1_bed'].booking_cost.mean(), inplace=True)
        # есть пустое знчение валюты, поставим туда валюту категории standard_1_bed
        data.loc[data.room_type == 'standard_1_bed', ['currency']] = 'GBP'
        #Приведите все валюты к одной
        data.loc[data.currency == 'EUR', ['booking_cost']] = data.booking_cost * 0.85
        data.currency.replace("EUR", "GBP", inplace=True)
        data.to_csv("/home/olesya/airflow/data/HW8.csv", index=False)
        print(data)

    @task
    def creat_table():
        MySqlOperator(
            task_id = 'create_table',
            database = 'Airflow',
            mysql_conn_id="mysql_conn",
            sql = '''
                            CREATE TABLE IF NOT EXISTS HW8 (
                                client_id INTEGER NOT NULL,
                                booking_date TEXT NOT NULL,
                                room_type TEXT NOT NULL,
                                hotel_id INTEGER NOT NULL,
                                booking_cost NUMERIC,
                                currency TEXT,
                                age INTEGER,
                                client_name TEXT,
                                client_type TEXT,
                                hotel_name TEXT
                            );
                         '''
        )

    @task
    def load_data():
        MySqlOperator(
            task_id='load_data',
            database='Airflow',
            mysql_conn_id="mysql_conn",
            sql=''' LOAD DATA LOCAL INFILE "/home/olesya/airflow/datas/HW8.csv" INTO TABLE HW8 fields terminated by ',' 
            lines terminated by '\n'
            IGNORE 1 LINES;  '''
        )


    [read_booking(), read_client(), read_hotel()] >> transform_data() >> creat_table() >> load_data()

main_dag = dag_HW8()
