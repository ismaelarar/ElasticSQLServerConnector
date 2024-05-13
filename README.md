# ElasticSQLServerConnector

ElasticSQLServerConnector hace uso del progama ElasticSearchExporter hecho por DisorganizedWizardry para traerse los logs de ElasticSearch y posteriormente ir continuamente insertándolos en una base de datos de SQL Server.

# Download 

> git clone https://github.com/ismaelarar/ElasticSQLServerConnector

# Instalar dependecias python

> cd ElasticSQLServerConnector/
> 
> pip3 install -r requirements.txt

# Configura ElasticSearchExplorer

Dentro de ElasticsearchExporter/ElasticExporterSettings.py modifica la conexion con elastic con los datos de tu conexion. Por ejemplo:

> es = Elasticsearch(
  hosts="https://192.168.22.119:9200",
  verify_certs=False,
  basic_auth=("elastic", "base64password")
  )

Más información en su página de GitHub:

https://github.com/DisorganizedWizardry/ElasticsearchExporter

# Configura ElasticSQLServerConector

Primero deberas introducir los datos de tu Base de Datos en el fichero SQLServerConfig.txt, por ejemplo:

```
server = 192.168.164.208,3317
database = BI
username = qlik
password = mipassword!

```

Luego modificar la función crear_tabla e introdcir el insert con los campos que quieras, por ejemplo:

```
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
```

También introducir en el select del función filtrar el nombre de tu campo que contendrá el timestamp, siguiendo el ejemplo anterior:

```
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
```

Finalmente modificaremos la función insertar_lineas, poniendo en el INSERT los campos de la tabla que creamos en el paso 1 (IMPORTANTE PONER LUEGO EL MISMO NUMERO DE INTERROGANTES) y modificaremos los source_date.get('nombre campo elasticsearch', '') introduciendo cada uno de los campos de ElasticSearch. (IMPORTANTE QUE COINCIDA EL NUMERO Y ORDEN CON LOS CAMPOS DEL INSERT). Por ejemplo:

```
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
```

# OPCIONES

```
--noupdate  Usar cuando tenemos la Base de Datos vacia, se baja todos los logs y los introduce en la base de datos

--update    Modo actualizar, se introducirán unicamente los logs de ElasticSearch que tengan un timestamp posterior al último que haya en la base de datos, por ello ya se debe haber ejecutado el programa con la opción --noupdate una vez. El programa se mantendrá en bucle.


```

# EJEMPLO

La primera vez con la base de vacía:

> cd ElasticSQLServerConnector
>
> python3 .\essc.py index_name --noupdate

Una vez ya tengamos algún log y queramos que se quede en bucle constantemente introduciendo:

> cd ElasticSQLServerConnector
>
> python3 .\essc.py index_name --update
