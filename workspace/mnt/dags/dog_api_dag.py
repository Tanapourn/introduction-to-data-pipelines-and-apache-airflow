from airflow import DAG 
from airflow.utils import timezone 
from airflow.providers.standard.operators.python import PythonOperator 


def _get_dog_image_url(): #ตั้งชื่อ function ให้มี _ ด้านหน้าจะได้ไม่ซ้ำกับชื่อ task
    print("Hello!")

    import requests

    url = "https://dog.ceo/api/breeds/image/random"
    response = requests.get(url)
    data = response.json()
    print(data)

with DAG( 
    "dog_api_dag", 
    start_date= timezone.datetime(2026, 6, 6), 
    schedule=None 
):

    get_dog_image_url = PythonOperator(
        task_id="get_dog_image_url",
        python_callable=_get_dog_image_url,
    )
    