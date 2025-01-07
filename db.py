import cx_Oracle
from tabulate import tabulate
import json
import argparse
import os
#import pandas as pd
#import matplotlib.pyplot as plt
#from matplotlib import style



def check_schema_in_db(db_info, schema_name):
    results = []
    try:
        dsn = cx_Oracle.makedsn(db_info['host'], db_info['port'], service_name=db_info['service_name'])
        conn = cx_Oracle.connect(user='dbaadmin', password='admin1ora', dsn=dsn)
        cursor = conn.cursor()
        # Ejemplo de consulta para verificar si el esquema existe
        cursor.execute("SELECT username, account_status status, lock_date, created FROM dba_users WHERE username like :schema", {'schema': f'%{schema_name.upper()}%'})
        for row in cursor:
            results.append({
                'HOST': db_info['host'],
                'SERVICE_NAME': db_info['service_name'],
                'USERNAME': row[0],
                'STATUS': row[1],
                'LOCK_DATE': row[2],
                'CREATED': row[3],
            })
            
        cursor.close()
        conn.close()
        
    except cx_Oracle.Error as error:
        print(f'Error al conectar o consultar en {db_info["name"]}: {error}')
    return results


def main():
    # Configuración del parser de argumentos
    parser = argparse.ArgumentParser(description="Buscar usuarios en bases de datos Oracle.")
    parser.add_argument('schema_name', help='Nombre del esquema a buscar')
    #parser.add_argument('-c', '--config', default='dba/databases.json', help='Ruta al archivo de configuración JSON')
    
    args = parser.parse_args()
    
    # Verificar que schema_name no sea nulo o vacío
    if not args.schema_name:
        print("Error: El nombre del esquema no puede estar vacío.")
        parser.print_help()
        sys.exit(1)  # Terminar el programa con código de error 1

    # Cargar la configuración
    script_dir = os.path.dirname(__file__)
    # Construye la ruta al archivo relativo al directorio del script
    rel_path = "databases.json"
    abs_file_path = os.path.join(script_dir, rel_path)
    with open(abs_file_path, 'r') as file:
        databases = json.load(file)

    # Recoger todos los resultados
    all_results = []
    for db in databases['databases']:
        all_results.extend(check_schema_in_db(db, args.schema_name))

    # Ordenar all_results por Host y SERVICE_NAME
    all_results = sorted(all_results, key=lambda x: (x['HOST'], x['SERVICE_NAME']))

    # Procesar resultados para repetir solo host y service_name cuando cambien
    formatted_results = []
    last_host, last_service_name = None, None

    for result in all_results:
        if result['HOST'] != last_host or result['SERVICE_NAME'] != last_service_name:
            # Nuevo Host o SERVICE_NAME, imprimimos ambos
            formatted_results.append(result)
            last_host, last_service_name = result['HOST'], result['SERVICE_NAME']
        else:
            # Mismo Host y service_name, solo actualizamos Username y Creation Date, dejando Host y service_name en blanco
            formatted_results.append({
                'HOST': '',
                'SERVICE_NAME': '',
                'USERNAME': result['USERNAME'],
                'STATUS': result['STATUS'],
                'LOCK_DATE': result['LOCK_DATE'],
                'CREATED': result['CREATED']
            })

    # Mostrar resultados en forma de tabla
    if formatted_results:
        print(tabulate(formatted_results, headers="keys", tablefmt="grid"))
    else:
        print("No se encontraron resultados.")
        
if __name__ == "__main__":
    main()        