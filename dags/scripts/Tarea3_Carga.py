import json
import logging
import psycopg2

# CONEXION
def conexion(credenciales_redshift):
    try:
        # Cargar las credenciales de Redshift desde el archivo JSON
        with open(credenciales_redshift, 'r') as file:
            credentials = json.load(file)
            host = credentials['host']
            port = credentials['port']
            db = credentials['db']
            user = credentials['user']
            password = credentials['password']

        # Crea la cadena de conexión con las opciones adecuadas para Redshift
        conn_str = f"dbname='{db}' user='{user}' host='{host}' port='{port}' password='{password}' sslmode='require'"

        # Intentar conectar a Redshift
        conn = psycopg2.connect(conn_str)
        print("Conexión exitosa a Redshift")
        return conn
    except psycopg2.Error as e:
        print("Error al conectar a Redshift:", e)
        raise  # Propagar la excepción para que Airflow marque la tarea como fallida
    finally:
        # Código que se ejecuta siempre, independientemente de si hubo una excepción o no
        print("Intento de conexión completado.")

# CREAR TABLA
def crear_tabla(conn):
    # Crear un cursor para ejecutar comandos SQL
    cur = conn.cursor()

    table_name = "colectivos_en_tiempo_real_caba"

    # Definir el comando SQL para crear una tabla
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT PRIMARY KEY,
        id_de_ruta VARCHAR(100),
        latitud DOUBLE PRECISION,
        longitud DOUBLE PRECISION,
        velocidad DOUBLE PRECISION,
        direccion INT,
        nombre_de_agencia VARCHAR(100),
        id_de_agencia INT,
        linea_de_colectivo VARCHAR(100),
        id_de_viaje VARCHAR(100),
        destino_de_viaje VARCHAR(100),
        actualizacion TIMESTAMP DEFAULT timezone('GMT+3', CURRENT_TIMESTAMP)
    );
    """  #En realidad es GTM-3 pero Redshift me lo toma al revés

    # Ejecutar el comando para crear la tabla
    cur.execute(create_table_query)

    # Confirmar los cambios
    conn.commit()

    # Cerrar cursor
    cur.close()

    print("Tabla creada exitosamente")

    return table_name


# CARGA
def carga(df_dict):
    """Carga los datos en Redshift."""
    import json
    import logging
    import pandas as pd
    import psycopg2

    file_path = '/opt/airflow/dags/scripts/conexion_redshift.json'

    conn = conexion(file_path)

    table_name = crear_tabla(conn)

    if isinstance(df_dict, str):
        df_dict = json.loads(df_dict)

    df = pd.DataFrame.from_dict(df_dict)

    # Obtener nombres de columnas
    column_names = ', '.join(df.columns)

    # Preparar datos para la operación UPSERT
    data_to_upsert = [tuple(row) for row in df.values]

    try:
        # Obtener todos los ids existentes en la tabla de Redshift
        existing_ids = []

        with conn.cursor() as cursor:
            cursor.execute(f'SELECT id FROM {table_name}')
            existing_ids = [row[0] for row in cursor.fetchall()]

        insert = update = 0
        with conn.cursor() as cursor:
            for data in data_to_upsert:
                if int(data[0]) in existing_ids:
                    # Si existe, actualizar el registro incluyendo la columna actualizacion
                    update_columns = [f'{column} = %s' for column in column_names.split(', ') if column != 'id']
                    update_columns.append("actualizacion = timezone('GMT+3', CURRENT_TIMESTAMP)")   #En realidad es GTM-3 pero redshift me lo toma al revés

                    cursor.execute(f"UPDATE {table_name} SET {', '.join(update_columns)} WHERE id = %s", data[1:] + (data[0],))                    
                    update += 1
                else:
                    # Si no existe, insertar nuevo registro
                    cursor.execute(f'INSERT INTO {table_name} ({column_names}) VALUES ({", ".join(["%s"] * len(data))})', data)
                    insert += 1

                conn.commit()  # Hacer commit después de cada operación
        print("Operación UPSERT completada correctamente en la tabla.")
        conn.close()
        return f"Registros creados: {insert}.\nRegistros actualizados: {update}."
    except psycopg2.Error as e:
        conn.close()
        raise Exception(f'Error al realizar la operación UPSERT: {e}')  # Lanzar excepción después de manejar el error
    
