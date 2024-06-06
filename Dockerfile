# Usa la imagen oficial de Airflow como base
FROM apache/airflow:2.5.0

# Establece el usuario a root para permitir la instalación de dependencias
USER root

# Copia el archivo de requisitos al directorio de trabajo en el contenedor
COPY requirements.txt /requirements.txt

# Cambia al usuario de Airflow
USER airflow

# Instala los requisitos del proyecto como usuario 'airflow'
RUN pip install --no-cache-dir -r /requirements.txt --user

# Cambia de nuevo al usuario 'root' para otras operaciones si es necesario
USER root

# Exponer el puerto 8080 para acceder al webserver de Airflow desde localhost
EXPOSE 8080

# Establece la variable de entorno para la zona horaria
ENV AIRFLOW__CORE__DEFAULT_TIMEZONE=America/Argentina/Buenos_Aires

# CMD predeterminado para iniciar el scheduler y el webserver de Airflow
CMD ["airflow", "webserver", "--host", "0.0.0.0"]
