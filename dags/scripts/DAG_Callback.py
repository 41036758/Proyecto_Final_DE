from datetime import datetime, timedelta
from airflow.operators.email import EmailOperator
import pytz

def failure_callback(context):
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    exception = context.get('exception')  # Obtener la excepción de la información de contexto
    
    # Agregar el mensaje de la excepción al contenido HTML del correo electrónico
    html_content = f"Ha fallado la tarea {task_id}.<br>"
    if exception:
        html_content += f"Error: {exception}"

    email = EmailOperator(
        task_id='send_email_on_failure',
        to='ombronienzo@gmail.com',
        subject='Error en DAG: DAG_Colectivos_CABA',
        html_content=html_content,  # Utilizar el contenido HTML actualizado
        dag=context['dag']
    )
    email.execute(context)

def success_callback(context):
    # Obtener la hora actual en GMT-3
    timezone = pytz.timezone('America/Argentina/Buenos_Aires')
    now = datetime.now(timezone)
    
    # Obtener la hora de inicio del DAG en GMT-3
    start_time = context['ti'].start_date.astimezone(timezone).strftime('%Y-%m-%d %H:%M:%S')
    
    # Calcular el tiempo de ejecución en segundos
    execution_time_seconds = (now - context['ti'].start_date).total_seconds()

    # Formatear el tiempo de ejecución en HH:MM:SS
    execution_time = str(timedelta(seconds=execution_time_seconds))

    # Obtener el resultado de 'Carga'
    task_instance = context['task_instance']
    result_carga = task_instance.xcom_pull(task_ids='Carga')

    # Crear el contenido del correo electrónico
    email_content = f"El DAG DAG_Colectivos_CABA se ha completado exitosamente.<br>"
    email_content += f"Resultado de la tarea 'Carga': {result_carga}<br>"
    email_content += f"Hora de inicio (GMT-3): {start_time}<br>"
    email_content += f"Hora de finalización (GMT-3): {now.strftime('%Y-%m-%d %H:%M:%S')}<br>"
    email_content += f"Tiempo de ejecución: {execution_time}"

    # Enviar el correo electrónico
    email = EmailOperator(
        task_id='send_email_on_success',
        to='ombronienzo@gmail.com',
        subject='DAG Completado: DAG_Colectivos_CABA',
        html_content=email_content,
        dag=context['dag']
    )
    email.execute(context)


