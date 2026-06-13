from airflow import DAG #เราจะให้ Dag จาก airflow
from airflow.utils import timezone #import function นึงมาจาก airlow เพื่อจัดการ timezone ดูจาก document ของ airflow ได้
from airflow.providers.standard.operators.empty import EmptyOperator #เลือก empty operator เพื่อเอา dash มาแต่ไม่ต้องทำอะไร แต่ถ้าต้องทำงานจริงอาจต้อง run Python ต้องเลือก Python operator


with DAG( #ปิด scope Dag ที่เราสร้างไว้
    "my_first_dag", #Dag แต่ละตัวจะมี id ของมัน แนะนำให้ใช้ชื่อเดียวกับชื่อไฟล์
    start_date= timezone.datetime(2026, 6, 6), #Dag จะเริ่ม run วันไหน **REQUIRED
    schedule=None #จะให้ run ทุก ๆ เท่าไหร่ ในนี้ใส่ none เพื่อให้เรามากด trigger เอง
):

    t1 = EmptyOperator(task_id="t1") #เรียกใช้ Empty operator
    t2 = EmptyOperator(task_id="t2")
    t3 = EmptyOperator(task_id="t3")
    t4 = EmptyOperator(task_id="t4")
    t5 = EmptyOperator(task_id="t5")

    t1 >> t2 >> t3 #เพื่อให้มันเรียง
    t2 >> t4
    t3 >> t5 
    