import json
import os
import shutil
import subprocess
import pyodbc
import time
import sys
import time
from datetime import datetime, timezone

#-------------------
# --- ARGUMENTOS ---
#-------------------

try:
    index_name = sys.argv[1] # Index del que queremos los datos
except:
    print("1er Argumento incorrecto, debes indicar el nombre del indice")
    exit()

# --update para empezar desde la ultima linea en la que se quedo la bd y --noupdate para empezar desde el principio
try:
    if sys.argv[2] == "--update":
        update = True
    elif sys.argv[2] == "--noupdate":
        update = False
    else:
        raise Exception()
except:
    print("2º Argumento incorrecto, debes poner --update o --noupdate")
    exit()

#---------------------------------------------------------------------------------------------------------------
# ---- FUNCIONES QUE SE DEBEN MODIFICAR EN BASE AL CONTENIDO DE TU BASE DE DATOS Y CAMPOS DEL ELASTICSERACH ----
#---------------------------------------------------------------------------------------------------------------

# Crear tabla si no existe
def crear_tabla(conn, cursor):
    cursor.execute('''IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='traces')
                        BEGIN
                            CREATE TABLE traces (
                                id VARCHAR(255) PRIMARY KEY,
                                Timestamp VARCHAR(255),
                                IdEmpleado INT,
                                Formulario VARCHAR(255),
                                IdLicencia VARCHAR(255),
                                Mensaje VARCHAR(MAX),
                                IdMaquina VARCHAR(255),
                                NetHostname VARCHAR(255),
                                Accion VARCHAR(255),
                                Servicio VARCHAR(255)
                            )
                        END''')

    conn.commit()

# Funcion para filtar los resultados de elastic a partir de la ultima fecha en la db y esta la opcion --update
def filtrar(cursor):
    cursor.execute("SELECT MAX(Timestamp) FROM traces")
    max_timestamp = cursor.fetchone()[0]

    json_data = {
        "bool": {
            "filter": [
                {
                    "range": {
                        "@timestamp": {
                            "format": "strict_date_optional_time",
                            "gte": max_timestamp,
                            "lte": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                        }
                    }
                }
            ]
        }
    }

    with open('filter.json', 'w') as filtro:
        json.dump(json_data, filtro, indent=4)

# Pasado un batch lo inserta en la base de datos
def insertar_lineas(batch, conn, cursor):
    for line in batch:
        data = json.loads(line)
        source_data = data.get('_source', {})
        _id = data.get('_id', '')
        if _id:
            # Verificar si ya existe un registro con el mismo _id
            cursor.execute("SELECT id FROM traces WHERE id = ?", (_id,))
            existing_record = cursor.fetchone()
            if not existing_record:
                cursor.execute('''INSERT INTO traces (
                                    id,
                                    Timestamp,
                                    IdEmpleado,
                                    Formulario,
                                    IdLicencia,
                                    Mensaje,
                                    IdMaquina,
                                    NetHostname,
                                    Accion,
                                    Servicio
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                            (data.get('_id', ''),
                                source_data.get('@timestamp', ''),
                                source_data.get('Attributes.ahora.eid', ''),
                                source_data.get('Attributes.ahora.form', ''),
                                source_data.get('Attributes.ahora.lid', ''),
                                source_data.get('Attributes.ahora.message', ''),
                                source_data.get('Attributes.machine.id', ''),
                                source_data.get('Attributes.net.host.name', ''),
                                source_data.get('Name', ''),
                                source_data.get('Resource.service.name', '')))
            conn.commit()

#---------------------------
# --- FUNCIONES NO TOCAR ---
#---------------------------

# Funcion para leer la configuracion de la conexion
def leer_config(archivo):
    with open(archivo, 'r') as file:

        valores = []

        # Iterar sobre cada línea y extraer los valores
        for linea in file.readlines():

            # Dividir la línea en clave y valor
            clave, valor = linea.strip().split(' = ')
            # Añadir el valor a la lista
            valores.append(str(valor))

        return valores

# Funcion para recibir los datos de un index de ElasticSearch en formato ndjson
# Devuelve string de la ruta del ndjson
def run_elastic_exporter_cli(index, cursor, update):
    
    if os.path.exists(index):
        # Si existe, lo eliminamos
        shutil.rmtree(index)
    
    if (update):
        filtrar(cursor)

        # Ejecutamos el comando que se trae los datos desde ElasticSearch
        command = ["python3", 
                    "ElasticsearchExporter/ElasticExporterCLI.py", 
                    "--index=" + index, 
                    "--backup-folder=" + ".",
                    "--query-file=filter.json" if update else ""]
        
    else:
         # Ejecutamos el comando que se trae los datos desde ElasticSearch SIN FILTRO
        command = ["python3", 
                    "ElasticsearchExporter/ElasticExporterCLI.py", 
                    "--index=" + index, 
                    "--backup-folder=" + "."]

    subprocess.run(command)

    return index + "/Other.ndjson"

# Conexion a la base de datos SQL Server
def conectar_sqlserver():

    conn_config = leer_config("SQLServerConfig.txt") # Obtenemos server, database y username
    #password = getpass.getpass("Introduce la contraseña: ")

    conn_str = f'DRIVER={{SQL Server}};SERVER={conn_config[0]};DATABASE={conn_config[1]};UID={conn_config[2]};PWD={conn_config[3]}'
    conn = pyodbc.connect(conn_str)
    print("CORRECTO: CONEXION CON SQL SERVER")

    return conn

# Mete las lineas del ndjson a una tabla del SQL Server
def ndjson_to_sqlserver(input_file):

    # Lectura del archivo ndjson y almacenamiento en la base de datos
    with open(input_file, 'r', encoding='utf-8') as f:

        lineas = f.readlines() #lineas totales
        
        is_pocas = len(lineas) < 1000
        batch_size = len(lineas) if is_pocas else 100 # Intervalo de insercion para no colapsar pero si son pocas lineas las hace todas seguidas

        print("Insertando " + str(len(lineas)) + " registros en la BD")
        # Si queremos añadir a partir de la ultima fila de la base de datos update debe ser True
        for i in range(0, len(lineas), batch_size):

            batch = lineas[i:i+batch_size]

            if not is_pocas:
                print(str(len(batch)) + " lineas leidas, siguiendo...")
            
            insertar_lineas(batch, conn, cursor)
            time.sleep(0.1)  

    #conn.close()


#-----------------
# --- PROGRAMA ---
#-----------------

# Nos conectamos y creamos un cursor
conn = conectar_sqlserver()
cursor = conn.cursor()

# Creamos la tabla si no existe
crear_tabla(conn, cursor)

while True:

    start_time = time.time()

    print("Empezando a leer ElasticSearch...")
    ndjson = run_elastic_exporter_cli(index_name, cursor, update)
    print("Logs importados, empezando a introducirlos en la BD...")
    print("-----------------------------------------------------------\n\n")

    ndjson_to_sqlserver(ndjson)

    end_time = time.time()

    print("Tiempo de ejecución: ", end_time - start_time, "segundos")

    if (not update): # Cerramos si estamos en modo noupdate
        break

    print("Volviendo a ejecutar en un rato...")
    print("-----------------------------------------------------------\n\n")
    time.sleep(300)