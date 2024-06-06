import json
import logging

import pandas as pd

def transformacion(df_dict):
    try:
        # Deserializar el df_dict si es una cadena
        if isinstance(df_dict, str):
            df_dict = json.loads(df_dict)

        df = pd.DataFrame.from_dict(df_dict)

        df = df.drop(columns=['timestamp'])
        df = df.rename(columns={
            'route_id': 'id_de_ruta',
            'latitude': 'latitud',
            'longitude': 'longitud',
            'speed': 'velocidad',
            'direction': 'direccion',
            'agency_name': 'nombre_de_agencia',
            'agency_id': 'id_de_agencia',
            'route_short_name': 'linea_de_colectivo',
            'tip_id': 'id_de_viaje',
            'trip_headsign': 'destino_de_viaje'
        })

        df['velocidad'] = df['velocidad'].round(decimals=1)

        df = df[["id"] + [col for col in df.columns if col != "id"]]
        df_json_serializable = df.to_dict(orient='records')

        # Serializar df a JSON antes de devolverlo
        df_json_serializable_str = json.dumps(df_json_serializable)

        df_exceso = df[df['velocidad'] >= 27]
        df_json_exceso = df_exceso.to_dict(orient='records')
        # Serializar df a JSON antes de devolverlo
        df_json_exceso_str = json.dumps(df_json_exceso)

        return df_json_serializable_str, df_json_exceso_str
    
    except json.JSONDecodeError as e:
        logging.error(f"Error al deserializar JSON: {e}")
        raise Exception(f'Error al deserializar JSON: {e}')  # Lanzar excepción después de manejar el error
    except Exception as e:
        logging.error(f"Error en transformacion: {e}")
        raise Exception(f'Error en transformacion: {e}')  # Lanzar excepción después de manejar el error
