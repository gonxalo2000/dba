import cx_Oracle
from tabulate import tabulate
import json
#import pandas as pd
#import matplotlib.pyplot as plt
#from matplotlib import style


# Para leer el archivo de configuración
with open('C:\\Users\\Usuario\\OneDrive\\Documentos\\Python\\P1\\db1\\databases.json', 'r') as file:
    databases = json.load(file)
    
def check_schema_in_db(db_info, schema_name):
    results = []
    try:
        dsn = cx_Oracle.makedsn(db_info['host'], db_info['port'], service_name=db_info['sid'])
        conn = cx_Oracle.connect(user='your_username', password='your_password', dsn=dsn)
        cursor = conn.cursor()
        # Ejemplo de consulta para verificar si el esquema existe
        cursor.execute("SELECT username,created FROM dba_users WHERE username like :schema", {'schema': f'%{schema_name.upper()}%'})
        #if cursor.fetchone():
        #    print(f"El esquema {schema_name} existe en la base de datos {db_info['name']}")
        #else:
        #    print(f"El esquema {schema_name} no existe en la base de datos {db_info['name']}")
        for row in cursor:
            results.append({
                'Host': db_info['host'],
                'SID': db_info['sid'],
                'Username': row[0],
                'Creation Date': row[1]
            })
            
        cursor.close()
        conn.close()
    except cx_Oracle.Error as error:
        print(f'Error al conectar o consultar en {db_info["name"]}: {error}')
    return results

all_results = []
# Iterar por todas las bases de datos
for db in databases['databases']:
    check_schema_in_db(db, "NOMBRE_DEL_ESQUEMA")
    all_results.append({
                #'Host': db['host'],
                #'SID': db['sid'],
                'Username': "usuario",
                'Creation Date': "fecha"
            })


if all_results:
    # Mostrando resultados en forma de tabla
    print(tabulate(all_results, headers="keys", tablefmt="grid"))
else:
    print("No se encontraron resultados.")    
        


# Para escribir en el archivo de configuración (agregar o quitar bases de datos)
def update_databases(new_databases):
    with open('databases.json', 'w') as file:
        json.dump({"databases": new_databases}, file, indent=4)

# Ejemplo de cómo agregar una nueva base de datos
new_db = {
    "name": "DB3",
    "host": "host3.example.com",
    "port": 1521,
    "sid": "ORCL3"
}
#databases["databases"].append(new_db)
#update_databases(databases["databases"])
