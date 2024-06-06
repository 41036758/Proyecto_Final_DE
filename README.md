# Proyecto de Carga de Datos de Colectivos en Tiempo Real

Resumen

El presente informe detalla el desarrollo de un proyecto para cargar datos de colectivos en tiempo real en una base de datos Redshift utilizando Apache Airflow. El proyecto incluye tareas de extracción, transformación, carga y notificación por correo electrónico en caso de anomalías en las velocidades registradas.

Objetivo

El objetivo principal del proyecto es automatizar el proceso de carga de datos de colectivos en tiempo real en una base de datos Redshift, asegurando la integridad de los datos y notificando sobre anomalías en las velocidades registradas.

Tecnologías Utilizadas

  * Apache Airflow: Plataforma de orquestación de flujos de trabajo.
  * Python: Lenguaje de programación utilizado para desarrollar las tareas y scripts necesarios.
  * Docker: Utilizado para gestionar y desplegar contenedores para Airflow y otros servicios.
  * Redshift: Base de datos utilizada para almacenar los datos de los colectivos en tiempo real.

Estructura del Proyecto
El proyecto se estructura en varias partes clave:

  1. Dockerfile y Docker Compose:
      * Se utiliza un Dockerfile para crear un contenedor ligero para Airflow.
      * Se emplea Docker Compose para orquestar los servicios necesarios, como PostgreSQL, Redis y Airflow.
  
  2. Apache Airflow DAG:
      * Se crea un DAG llamado 'DAG_Colectivos_CABA' que contiene las tareas de extracción, transformación, carga y notificación por correo electrónico.
      * Cada tarea se define como un operador Python en Airflow.
  
  3. Tareas de Extracción, Transformación y Carga:
      * La Tarea 1 (extraccion) extrae los datos de una API y los devuelve en formato JSON.
      * La Tarea 2 (transformacion) transforma los datos JSON, realiza limpieza y filtrado de datos, y los prepara para la carga.
      * La Tarea 3 (conexion, crear_tabla y carga) gestiona la conexión a Redshift, crea la tabla necesaria y carga los datos en Redshift utilizando una operación UPSERT.
      * La Tarea 4 (send_email_if_anomaly) envía un correo electrónico en caso de anomalías en las velocidades registradas.
  
  4. Funciones de Callback para Correos Electrónicos:
      * Se implementan funciones de callback para enviar correos electrónicos en caso de éxito o fallo en el DAG, proporcionando detalles sobre el estado del DAG y las tareas ejecutadas.

Resultados y Beneficios

  Automatización del proceso de carga de datos de colectivos en tiempo real en Redshift.
  Notificación inmediata por correo electrónico en caso de anomalías en las velocidades registradas.
  Mayor eficiencia y control en la gestión de datos en tiempo real.

Conclusiones y Recomendaciones

El proyecto ha logrado satisfactoriamente el objetivo de automatizar la carga de datos y la notificación en caso de anomalías. Se recomienda realizar pruebas exhaustivas y monitoreo continuo para garantizar la integridad y calidad de los datos cargados. Además, se sugiere explorar posibles mejoras en el rendimiento y la escalabilidad del sistema en futuras iteraciones del proyecto.
