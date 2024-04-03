import datetime
import os

from airflow.decorators import dag, task
from airflow.providers.telegram.operators.telegram import TelegramOperator

os.environ["no_proxy"]="*"

@dag(
    dag_id="wether-tlegram",
    schedule="@once",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    )
def WetherETL():
send_message_telegram_task = TelegramOperator(
task_id='send_message_telegram',
telegram_conn_id='tg_main',
chat_id='-968885419',
text='Wether in Moscow \nYandex: ' + "{{ ti.xcom_pull(task_ids=['yandex_wether'],key='wether')[0]}}" + "
degrees" +
"\nOpen wether: " + "{{ ti.xcom_pull(task_ids=['open_wether'],key='open_wether')[0]}}" + " degrees",
)
@task(task_id='yandex_wether')
def get_yandex_wether(**kwargs):
ti = kwargs['ti']
url = "https://api.weather.yandex.ru/v2/informers/?lat=55.75396&lon=37.620393"
payload={}
headers = {
'X-Yandex-API-Key': '33f45b91-bcd4-46e4-adc2-33cfdbbdd88e'
}
response = requests.request("GET", url, headers=headers, data=payload)
ti.xcom_push(key='wether', value=response.json()['fact']['temp'])
@task(task_id='open_wether')
def get_open_wether(**kwargs):
ti = kwargs['ti']
url =
"https://api.openweathermap.org/data/2.5/weather?lat=55.749013596652574&lon=37.61622153253021&appid=2cd7
8e55c423fc81cebc1487134a6300"
payload={}
headers = {}
response = requests.request("GET", url, headers=headers, data=payload)
ti.xcom_push(key='open_wether', value=round(float(response.json()['main']['temp']) - 273.15, 2))
get_yandex_wether() >> get_open_wether() >> send_message_telegram_task
dag = WetherETL(