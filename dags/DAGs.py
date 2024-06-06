from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append('/opt/airflow/scripts')
from dags.scripts.Tarea1_Extraccion import extraccion
from dags.scripts.Tarea2_Transformacion import transformacion
from dags.scripts.Tarea3_Carga import carga
from dags.scripts.DAG_Callback import success_callback, failure_callback
from dags.scripts.Tarea4_Anomalia import send_email_if_anomaly

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime(2024, 6, 6),
    'retries': 0,
    'on_failure_callback': lambda context: failure_callback(context),
}

dag = DAG(
    'DAG_Colectivos_CABA',
    default_args=default_args,
    description='Cargar API a Redshift',
    schedule_interval='0 0 * * *',
)

tarea_1 = PythonOperator(
    task_id='Extraccion',
    provide_context=True,
    python_callable=extraccion,
    dag=dag,
)

tarea_2 = PythonOperator(
    task_id='Transformacion',
    python_callable=transformacion,
    provide_context=True,
    op_kwargs={'df_dict': "{{ task_instance.xcom_pull(task_ids='Extraccion') }}"},
    dag=dag,
)

tarea_3 = PythonOperator(
    task_id='Carga',
    python_callable=carga,
    provide_context=True,
    op_kwargs={'df_dict': "{{ task_instance.xcom_pull(task_ids='Transformacion')[0] }}"},
    dag=dag,
    on_success_callback = lambda context: success_callback(context)
)

tarea_anomalia = PythonOperator(
    task_id='email_exceso',
    python_callable=send_email_if_anomaly,
    provide_context=True,
    op_kwargs={
        'df_json_filtrado_str': "{{ task_instance.xcom_pull(task_ids='Transformacion')[1] }}",
        'dag': dag,
    },
    dag=dag,
)

tarea_1 >> tarea_2 >> [tarea_anomalia, tarea_3]