from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime
import random
import json

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 20),
    "retries": 1
}

@dag(
    default_args=default_args,
    schedule_interval='@daily',
    dag_id='dag_HW6',
)
def dag_HW6():
    random_mumber = BashOperator(
        task_id="random_mumber",
        bash_command="echo $((RANDOM % 100))")

    @task
    def square_number():
        number = random.randint(0, 100)
        print(f"Квадрат числа {number} равняется {number ** 2}.")

    get_weather = SimpleHttpOperator(
        task_id="get_weather",
        http_conn_id="http_weather",
        endpoint="/weather/Moscow",
        method='GET',
        headers={}
    )

    @task
    def print_weather(**kwargs):
        weather = kwargs['ti'].xcom_pull(key=None, task_ids="get_weather")
        data = json.loads(weather)
        print(f"Moscow: температура {data['temperature']}, ветер {data['wind']}, {data['description']}.")

    random_mumber >> square_number() >> get_weather >> print_weather()

main_dag = dag_HW6()
