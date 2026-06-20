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

def _check_pm2_5_key_exists (**context):
    ds = context["ds"]
    
    import json

    with open(f"/opt/airflow/dags/{ds}-output.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    
    assert "hourly" in data #เช็คว่า key hourly เก็บไว้จริงใน data
    assert "pm2_5" in data["hourly"] #เช็คว่า pm 2.5 อยู่ใน data hourly

def _calculate_data_quality_score(**context):
    import json
    import glob
    import os

    pattern = os.path.join("/opt/airflow/dags", "*-output.json")
    files = sorted(glob.glob(pattern))
    results = []
    count_ok = 0
    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        pm = data["hourly"]["pm2_5"]
        pm_count = len(pm)
        if pm_count == 24:
            count_ok += 1
 
    total_files = len(files)
    score = (count_ok / total_files)
    print(f"Data Quality Score: {score}") 

def _transform_to_csv(**context):
    import csv
    import json
    import glob
    import os

    pattern = os.path.join("/opt/airflow/dags", "*-output.json")
    files = sorted(glob.glob(pattern))
    results = []
    count_ok = 0
    with open("/opt/airflow/dags/pm2_5_data.csv", "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["date", "hour", "pm2_5"])
        for f in files:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)        
                for i, pm_value in enumerate(data["hourly"]["pm2_5"]):
                    writer.writerow([data["hourly"]["time"][0].split("T")[0], i, pm_value])

def _find_average_pm2_5_in_duckdb(**context):
    import duckdb
    import os

    conn = duckdb.connect(database="/opt/airflow/dags/myduckdb.db")
    csv_file_path = "/opt/airflow/dags/pm2_5_data.csv"
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS pm2_5_data AS 
        SELECT * FROM read_csv_auto('{csv_file_path}')
    """)

    query = """
        CREATE OR REPLACE VIEW avg_pm2_5 as
        select
            date,
            avg(pm2_5)
        from pm2_5_data
        group by date
    """
    conn.execute(query)

with DAG( 
    "weather_data_dag", 
    start_date= timezone.datetime(2026, 6, 6), 
    schedule="@daily" #เป็น cron exprepression แบบ daily
):

    get_air_quality_data = PythonOperator(
        task_id="get_air_quality_data",
        python_callable=_get_air_quality_data,
    )
#check key เพื่อให้แน่ใจว่า calculate score ได้ 
    check_pm2_5_key_exists = PythonOperator(
        task_id="check_pm2_5_key_exists",
        python_callable=_check_pm2_5_key_exists,
    )

    calculate_data_quality_score = PythonOperator(
        task_id="calculate_data_quality_score",
        python_callable=_calculate_data_quality_score,
    )

    find_average_pm2_5 = PythonOperator(
        task_id="find_average_pm2_5",
        python_callable=_find_average_pm2_5,
    )

    transform_to_csv = PythonOperator(
        task_id="transform_to_csv",
        python_callable=_transform_to_csv,
    )

    find_average_pm2_5_in_duckdb = PythonOperator(
        task_id="find_average_pm2_5_in_duckdb",
        python_callable=_find_average_pm2_5_in_duckdb,
    )

#งานที่ operate จากคำสั่งข้างบน
    get_air_quality_data >> check_pm2_5_key_exists >> find_average_pm2_5
    check_pm2_5_key_exists >> calculate_data_quality_score
    check_pm2_5_key_exists >> transform_to_csv >> find_average_pm2_5_in_duckdb 