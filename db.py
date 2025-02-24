import cx_Oracle
from tabulate import tabulate
import json
import argparse
import os
import sys
import csv


def create_conn (host, port, service_name):
    dsn = cx_Oracle.makedsn(host, port, service_name)
    conn = cx_Oracle.connect(user=USER, password=PASS, dsn=dsn)
    cursor = conn.cursor()
    return cursor

def get_connection(host, port, service_name, username='dbaadmin', password='admin1ora'):
    """
    Abre una conexión a la base de datos Oracle y devuelve un cursor.

    :param host: Dirección del servidor de base de datos.
    :param port: Puerto de escucha del servidor.
    :param service_name: Nombre del servicio de la base de datos.
    :param username: Nombre de usuario para la autenticación.
    :param password: Contraseña para la autenticación.
    :return: Cursor de la conexión.
    """
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    try:
        conn = cx_Oracle.connect(user=username, password=password, dsn=dsn)
        return conn.cursor()
    except cx_Oracle.Error as error:
        print(f'Error al conectar a la base de datos {host}:{service_name}: {error}')
        return None

def close_connection(cursor):
    """
    Cierra el cursor y la conexión asociada.

    :param cursor: Cursor de la conexión a cerrar.
    """
    if cursor:
        try:
            connection = cursor.connection
            cursor.close()
            connection.close()
        except cx_Oracle.Error as error:
            print(f'Error al cerrar la conexión: {error}')    

def execute_custom_query(db_info, query):
    results = []
    cursor = get_connection(db_info['host'], db_info['port'], db_info['service_name'])
    if cursor:
        try:
            # Obtener la versión de la instancia
            cursor.execute("select version from v$instance")
            version = cursor.fetchone()[0]
            
            # Ejecutar la consulta personalizada
            cursor.execute(query)
            if cursor.description:
                # Obtener los nombres de las columnas
                columns = [desc[0] for desc in cursor.description]
                for row in cursor:
                    dynamic_result = dict(zip(columns, row))
                    # Asegurarse de que HOST, SERVICE_NAME y VERSION sean las primeras entradas
                    tupla = {
                        'HOST': db_info['host'],
                        'SERVICE_NAME': db_info['service_name'],
                        'VERSION': version,
                    }
                    # Agregar las columnas dinámicas del cursor
                    tupla.update(dynamic_result)
                    results.append(tupla)
        finally:
            close_connection(cursor)
    return results

        

def format_results(all_results):
    formatted_results = []
    last_host, last_service_name = None, None

    for result in all_results:
        if result['HOST'] != last_host or result['SERVICE_NAME'] != last_service_name:
            # Nuevo Host o SERVICE_NAME, imprimimos ambos
            formatted_results.append(result)
            last_host, last_service_name = result['HOST'], result['SERVICE_NAME']
        else:
            # Mismo Host y service_name, actualizamos solo los datos que no son HOST ni SERVICE_NAME
            new_result = {'HOST': '', 'SERVICE_NAME': ''}
            for key, value in result.items():
                if key not in ('HOST', 'SERVICE_NAME','VERSION'):
                    new_result[key] = value
            formatted_results.append(new_result)
    
    return formatted_results

def save_results_to_csv(results, csv_file):
    """
    Guarda los resultados en un archivo CSV.
    """
    if not results:
        print("No hay resultados para guardar.")
        return

    # Obtener las claves del primer resultado como nombres de columnas
    fieldnames = results[0].keys()

    try:
        with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()  # Escribir la cabecera
            writer.writerows(results)  # Escribir las filas
    except Exception as e:
        print(f"Error al guardar el archivo CSV: {e}")

def main():
    # Configuración del parser de argumentos
    parser = argparse.ArgumentParser(
        description="Ejecuta una consulta SQL en bases de datos Oracle.",
        usage="%(prog)s [-h] bases jsonsql_file [csv_file]"
    )
    parser.add_argument('bases_file', help='Ruta al archivo JSON con las bases de datos donde ejecutar (requerido)')
    parser.add_argument('sql_file', help='Ruta al archivo SQL con la consulta a ejecutar (requerido)')
    parser.add_argument('csv_file', nargs='?', default=None, help='Ruta al archivo CSV donde se guardarán los resultados (opcional)')
    
    args = parser.parse_args()

    # Validar que el archivo JSON existe
    if not os.path.exists(args.bases_file):
        print(f"Error: El archivo de bases de datos '{args.bases_file}' no existe.")
        sys.exit(1)

    # Validar que el archivo SQL existe
    if not os.path.exists(args.sql_file):
        print(f"Error: El archivo SQL '{args.sql_file}' no existe.")
        sys.exit(1)

    # Leer el contenido del archivo SQL
    with open(args.sql_file, 'r') as file:
        query = file.read()

    # Cargar la configuración de las bases de datos
    script_dir = os.path.dirname(__file__)
    rel_path = args.bases_file
    abs_file_path = os.path.join(script_dir, rel_path)
    with open(abs_file_path, 'r') as file:
        databases = json.load(file)

    # Recoger todos los resultados
    all_results = []
    for db in databases['databases']:
        results = execute_custom_query(db, query)
        all_results.extend(results)

    # Ordenar all_results por Host y SERVICE_NAME
    all_results = sorted(all_results, key=lambda x: (x['HOST'], x['SERVICE_NAME']))

    # Elimina hosts y service_name duplicados para imprimir
    formatted_results = format_results(all_results)

    # Mostrar resultados en forma de tabla
    if formatted_results and not args.csv_file:
        print(tabulate(formatted_results, headers="keys", tablefmt="grid"))
    elif args.csv_file:
        print("CSV generado correctamente.")
    else:
        print("No se encontraron resultados.")
    if args.csv_file:
        save_results_to_csv(all_results, args.csv_file)
        
if __name__ == "__main__":
    main()
    