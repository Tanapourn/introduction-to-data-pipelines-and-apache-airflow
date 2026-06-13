from airflow import DAG 
from airflow.utils import timezone 
from airflow.providers.standard.operators.python import PythonOperator 


def _get_air_quality_data(**context): #ตั้งชื่อ function ให้มี _ ด้านหน้าจะได้ไม่ซ้ำกับชื่อ task, **context เรียก keyword agreement
    print(context)

    ds = context["ds"] #เพราะจะเอา ds มาใช้ใน url

    import requests

    url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude=13.870352796078675&longitude=100.55150541726421&hourly=pm2_5&start_date={ds}&end_date={ds}"
    response = requests.get(url)
    data = response.json()
    print(data)

    import json

    with open(f"/opt/airflow/dags/{ds}-output.json", "w", encoding="utf-8") as f: #แปลงให้เป็น string ใส่ f ลงไป
        json.dump(data, f, ensure_ascii=False, indent=2)

def _find_average_pm2_5(**context):
    ds = context["ds"]
    
    import json

    with open(f"/opt/airflow/dags/{ds}-output.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    pm2_5_values = data["hourly"]["pm2_5"] #จะดึง key pm2.5 จาก folder hourly คือ chain pm2.5 มาต่อ
    average_pm2_5 = sum(pm2_5_values) / len(pm2_5_values) #เป็น list เลย sum ค่าใน list ทั้งหมด แล้วหาด้วย len คือจำนวน pm2.5 ทั้งหมด
    print(f"Average PM2.5 for {ds}: {average_pm2_5}")

    with open(f"/opt/airflow/dags/{ds}-average.json", "w", encoding="utf-8") as f:
        data = {
            "average_pm2_5": average_pm2_5
        }
        json.dump(data, f, ensure_ascii=False, indent=2)

with DAG( 
    "weather_data_dag", 
    start_date= timezone.datetime(2026, 6, 6), 
    schedule="@daily" #เป็น cron exprepression แบบ daily
):

    get_air_quality_data = PythonOperator(
        task_id="get_air_quality_data",
        python_callable=_get_air_quality_data,
    )

    find_average_pm2_5 = PythonOperator(
        task_id="find_average_pm2_5",
        python_callable=_find_average_pm2_5,
    )
    
    get_air_quality_data >> find_average_pm2_5