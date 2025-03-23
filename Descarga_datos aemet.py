import yaml
import http.client
import requests
import json
import csv
import os
from urllib.parse import urlparse
from pathlib import Path
import ast
import argparse
import time
import pdb
import numpy as np
from datetime import datetime, timedelta
import time

archivo_config = "./14_TFM/key.yaml"
clave_deseada = "aemet_api_key"
url_base = "https://opendata.aemet.es/opendata"
path_combinado = "./14_TFM/valores_diarios/data.json"
path_combinado_csv = "./14_TFM/valores_diarios/data.csv"

def leer_api_key(archivo_yaml, clave_api):
    """
    Leer una API key desde un archivo YAML.

    Args:
        archivo_yaml (str): La ruta al archivo YAML.
        clave_api (str): El nombre de la clave API que se va a leer.

    Returns:
        str: La API key o None si no se encuentra.
    """
    try:
        with open(archivo_yaml, 'r') as file:
            configuracion = yaml.safe_load(file)
            if configuracion and 'api_keys' in configuracion and clave_api in configuracion['api_keys']:
                return configuracion['api_keys'][clave_api]
            else:
                print(f"La clave '{clave_api}' no se encontró en el archivo YAML.")
                return None
    except FileNotFoundError:
        print(f"Error: El archivo '{archivo_yaml}' no se encontró.")
        return None
    except yaml.YAMLError as e:
        print(f"Error al parsear el archivo YAML: {e}")
        return None

def apirest(api_key, url_base, url_especifica):
    try:
        #url = f"{url_base}{url_posterior}{area[2]}"
        url = f"{url_base}{url_especifica}"

        querystring = {"api_key":f"{api_key}"}

        headers = {
            'cache-control': "no-cache"
            }

        response = requests.request("GET", url, headers=headers, params=querystring)
        print(response.text)
        return response
    except Exception as err:
        print(f'Error:{err}')
        time.sleep(60)
        return -1

def obtener_json_de_url(url):
  """
  Obtiene datos en formato JSON desde una URL.

  Args:
      url (str): La URL de la que se obtendrán los datos JSON.

  Returns:
      dict: Los datos JSON decodificados como un diccionario Python, 
            o None si hay un error.
  """
  try:
      # Realizar la petición HTTP GET a la URL
      respuesta = requests.get(url)
      respuesta.raise_for_status()  # Lanzar excepción para errores HTTP (404, 500, etc.)

      # Decodificar la respuesta JSON en un diccionario Python
      datos_json = respuesta.json()

      return datos_json
  
  except requests.exceptions.RequestException as e:
      print(f"Error en la petición HTTP: {e}")
      return None
  except json.JSONDecodeError as e:
      print(f"Error al decodificar JSON: {e}")
      return None
  except Exception as e:
      print(f"Error inesperado: {e}")
      return None

def genero_fecha_texto(fecha):
    try:
        y = fecha.year
        m = fecha.month
        d = fecha.day
        return f'{y}_{m}_{d}'
    except Exception as err:
        print(err)

def guardar_json_archivo(datos, ruta_archivo):
    """
    Guarda datos en un archivo JSON.

    Args:
        datos (dict o list): Los datos que se guardarán en formato JSON.
        ruta_archivo (str): La ruta del archivo donde se guardarán los datos.
    Returns:
        bool: True si se ha guardado correctamente, False en caso de error
    """
    try:
        with open(ruta_archivo, 'w', encoding='utf-8') as archivo_json:
            json.dump(datos, archivo_json, indent=4, ensure_ascii=False)
        print(f"Datos guardados correctamente en: {ruta_archivo}")
        return True
    except Exception as e:
        print(f"Error al guardar en el archivo JSON: {e}")
        return False

def combinar_json(combinado, path):
    try:
    # Cargar los dos archivos JSON
        with open(combinado, "r") as file1, open(path, "r") as file2:
            data1 = json.load(file1)  # Cargar JSON 1
            data2 = json.load(file2)  # Cargar JSON 2
        
        # Concatenar las listas
        combined_data = data1 + data2

        # Guardar el resultado en un nuevo archivo
        with open(combinado, "w") as output_file:
            json.dump(combined_data, output_file, indent=4)

        print("Archivos concatenados y guardados en 'data.json'")
    except Exception as err:
        print(err)

def convertir_csv():
    try:
        # Cargar el archivo JSON
        with open(path_combinado, "r") as json_file:
            data = json.load(json_file)  # Cargar la lista de diccionarios
            
        # Detectar todas las claves únicas
        fieldnames = set()
        for row in data:
            fieldnames.update(row.keys())
        fieldnames = list(fieldnames)  # Convertir a lista
        print(fieldnames)

        # Convertir a CSV
        with open(path_combinado_csv, "w", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()  # Escribir encabezados
            writer.writerows(data)  # Escribir filas
        print("Archivo CSV generado como 'data.csv'")
    except Exception as err:
        print(err)
    
def descargardatos(url_data, fecha=''):
    contador = 0
    api_key = leer_api_key(archivo_config, clave_deseada)
    response = apirest(api_key, url_base, url_data)
    if response == -1:
        contador+=1
        if contador <= 2:
            print(f'Resolviendo un error. {contador} nuevo intento')
            response = apirest(api_key, url_base, url_data)
        else:
            raise  ValueError("Algo salió mal en la conexión.")
    
    contador = 0
    diccionario = json.loads(response.text)
    api_datos = diccionario['datos']
    api_metadatos = diccionario['metadatos']
    try:
        datos = obtener_json_de_url(api_datos)
        fecha = genero_fecha_texto(fecha)
        valoresdiarios_json = "./14_TFM/valores_diarios/"
        path_json = valoresdiarios_json + fecha + '.json'
        guardar_json_archivo(datos, path_json)
        combinar_json(path_combinado, path_json)
    except Exception as err:
        print(err)
        return -1

def descargo_recurrente():
    try:
        fecha_inicial = datetime(2024, 5, 6)
        numero_iteraciones = 2
        par_fechas = []
        for j in range(14):
            if j == 50:
                time.sleep(60)    
            else: time.sleep(15)
            if j > 0:
                fecha_inicial = par_fechas[len(par_fechas)-1]
                año_fin = par_fechas[len(par_fechas)-1].year
                mes_fin = par_fechas[len(par_fechas)-1].month
                dia_fin = par_fechas[len(par_fechas)-1].day
                año_ini = par_fechas[len(par_fechas)-2].year
                mes_ini = par_fechas[len(par_fechas)-2].month
                dia_ini = par_fechas[len(par_fechas)-2].day
                fechaIniStr = f'{año_ini}-{mes_ini}-{dia_ini}T12:00:00UTC'
                fechaFinStr = f'{año_fin}-{mes_fin}-{dia_fin}T12:00:00UTC'
                url_diarios_historico = f"/api/valores/climatologicos/diarios/datos/fechaini/{fechaIniStr}/fechafin/{fechaFinStr}/todasestaciones"
                fecha_archivar = par_fechas[len(par_fechas)-2]
                descarga = descargardatos(url_diarios_historico, fecha_archivar)
                if descarga == -1:
                    raise ValueError("Error en la descarga")
                convertir_csv()
            # Iterar de 15 en 15 días
            for i in range(numero_iteraciones):
                fecha_actual = fecha_inicial + timedelta(days=15 * i)
                print(fecha_actual.strftime("%Y-%m-%d"))
                par_fechas.append(fecha_actual)
    except Exception as err:
        print(err)

@DeprecationWarning
def descargo_recurrente_provincia():
    # Fecha inicial
    fecha_inicial = datetime(1992, 2, 20)
    numero_iteraciones = 2
    par_fechas = []
    for j in range(2):
        if j == 30:
            time.sleep(5)    
        else: time.sleep(5)
        if j > 0:
            fecha_inicial = par_fechas[len(par_fechas)-1]
            # año_fin = par_fechas[len(par_fechas)-1].year
            # mes_fin = par_fechas[len(par_fechas)-1].month
            # dia_fin = par_fechas[len(par_fechas)-1].day
            # año_ini = par_fechas[len(par_fechas)-2].year
            # mes_ini = par_fechas[len(par_fechas)-2].month
            # dia_ini = par_fechas[len(par_fechas)-2].day
            fechaIniStr = '1992-2-20T12:00:00UTC'#f'{año_ini}-{mes_ini}-{dia_ini}T12:00:00UTC'
            fechaFinStr = '2025-1-1T12:00:00UTC'#f'{año_fin}-{mes_fin}-{dia_fin}T12:00:00UTC'
            # fechaIniStr = f'{año_ini}-{mes_ini}-{dia_ini}T12:00:00UTC'
            # fechaFinStr = f'{año_fin}-{mes_fin}-{dia_fin}T12:00:00UTC'
            tarragona = '0016A'#, '9981A', '0016B', '0002I'
            url_diarios_historico = f"/api/valores/climatologicos/diarios/datos/fechaini/{fechaIniStr}/fechafin/{fechaFinStr}/estacion/{tarragona}"
            # url_diarios_historico = f"/api/valores/climatologicos/diarios/datos/fechaini/{fechaIniStr}/fechafin/{fechaFinStr}/todasestaciones"
            fecha_archivar = par_fechas[len(par_fechas)-2]
            descargardatos(url_diarios_historico, fecha_archivar)
            convertir_csv()
        # Iterar de 15 en 15 días
        for i in range(numero_iteraciones):
            fecha_actual = fecha_inicial + timedelta(days=15 * i)
            print(fecha_actual.strftime("%Y-%m-%d"))
            par_fechas.append(fecha_actual)

def main():
    if argumento == 'historico_diario':
        descargo_recurrente()
        #descargo_recurrente_provincia()

if __name__ == "__main__":
    try:
        #parseo de entrada de linea de comandos
        parser = argparse.ArgumentParser(description='programa para descarga de datos de aemet. --nombre de datos')
        parser.add_argument('--datos', type=str, required=True, help='Ruta del archivo data')
        
        args = parser.parse_args()
        argumento = args.datos
        main()
    except SystemExit as err:
        pass
    except Exception as err:
        pass