from airflow.decorators import dag
from datetime import datetime
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.models import Variable

API_KEY = Variable.get('openweather_key')
URL = f'/data/2.5/weather?q=Moscow,ru&exclude=current&appid={API_KEY}&units=metric'


def choosing_weather(ti):
    current_temp = ti.xcom_pull(task_ids='get_temperature')
    if current_temp > 15:
        return 'warm_branch'
    return 'cold_branch'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 12),
    'retries': 1
}

@dag(
    dag_id='dag_HW7',
    default_args=default_args,
    schedule_interval=None
)
def dag_HW7():
    get_temperature = HttpOperator(
        task_id='get_temperature',
        method='GET',
        http_conn_id='openweather',
        endpoint=URL,
        response_filter=lambda response: response.json()["main"]["temp"],
        headers={},
    )

    choosing_result = BranchPythonOperator(
        task_id='choosing_result',
        python_callable=choosing_weather
    )

    warm_branch = PythonOperator(
        task_id='warm_branch',
        python_callable=lambda ti: print(f'ТЕПЛО: {ti.xcom_pull(task_ids="get_temperature")}°C'),
    )

    cold_branch = PythonOperator(
        task_id='cold_branch',
        python_callable=lambda ti: print(f'ХОЛОДНО: {ti.xcom_pull(task_ids="get_temperature")}°C'),
    )

    get_temperature >> choosing_result >> [warm_branch, cold_branch]


main_dag = dag_HW7()
