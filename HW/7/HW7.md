— Зарегистрируйтесь в ОрепWeatherApi (https://openweathermap.org/api)
— Создайте ETL, который получает температуру в заданной вами локации, и
дальше делает ветвление:

• В случае, если температура больше 15 градусов цельсия — идёт на ветку, в которой есть оператор, выводящий на
экран «тепло»;
• В случае, если температура ниже 15 градусов, идёт на ветку с оператором, который выводит в консоль «холодно».

Оператор ветвления должен выводить в консоль полученную от АРI температуру.

— Приложите скриншот графа и логов работы оператора ветвленния.

![alt text](image.png)

![alt text](image-1.png)

![alt text](image-2.png)

![alt text](image-3.png)

![alt text](image-4.png)

код дага:

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
