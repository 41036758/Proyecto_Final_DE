from datetime import datetime, timedelta, timezone
from airflow.operators.email import EmailOperator

def send_email_if_anomaly(df_json_filtrado_str, dag):
    gmt3 = timezone(timedelta(hours=-3))
    fecha_actual = datetime.now(gmt3).strftime('%Y-%m-%d %H:%M:%S')

    registros_separados = df_json_filtrado_str.replace(",", ",<br>").replace("},", "},<br>")

    email_content = f"Fecha y Hora: {fecha_actual}<br><br>" \
                    f"Registros con velocidades mayores o iguales a 27 Km/h (exceso de velocidad):<br><br>{registros_separados}"

    email = EmailOperator(
        task_id='send_email_if_anomaly',
        to='ombronienzo@gmail.com',  # Cambiar al destinatario deseado
        subject='Registros de anomalía en velocidades',
        html_content=email_content,
        dag=dag  # Asegúrate de tener definida la variable 'dag' en tu entorno
    )
    email.execute(df_json_filtrado_str)
