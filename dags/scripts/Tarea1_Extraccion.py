import json
import requests

def extraccion():

    # Cargar las credenciales desde un archivo JSON
    file_path = '/opt/airflow/dags/scripts/conexion_api.json'
    with open(file_path, 'r') as file:
        credentials = json.load(file)
        url, client_id, client_secret = credentials['url'], credentials['client_id'], credentials['client_secret']

    # Hacer la solicitud a la API y obtener los datos
    params = {'client_id': client_id, 'client_secret': client_secret}
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        # Retornar los datos directamente en formato JSON
        data_json = json.dumps(data)
        return data_json
    else:
        raise Exception(f'Error al hacer la solicitud: {response.status_code}')