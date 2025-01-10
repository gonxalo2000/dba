import cx_Oracle
from tabulate import tabulate
import json
import argparse
import os
import sys



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
        
def list_schema_obj(db_info, schema_name, obj_name):
    results = []
    cursor = get_connection(db_info['host'], db_info['port'], db_info['service_name'])
    if cursor:
        try:
            # Obtener la versión de la instancia
            cursor.execute("select version from v$instance")
            version = cursor.fetchone()[0]
            
            # Consultar objetos del esquema
            cursor.execute("""
                SELECT owner,object_name, object_type, created, status 
                FROM dba_objects 
                WHERE owner LIKE :schema 
                AND object_name LIKE :obj 
                ORDER BY object_name
                """, {'schema': f'{schema_name.upper()}', 'obj': f'{obj_name.upper()}'})

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


def list_schema_info(db_info, schema_name):
    results = []
    cursor = get_connection(db_info['host'], db_info['port'], db_info['service_name'])
    if cursor:
        try:
            # Obtener la versión de la instancia
            cursor.execute("select version from v$instance")
            version = cursor.fetchone()[0]
            
            # Consultar esquemas
            cursor.execute("""
                           SELECT username, account_status status, lock_date, created 
                           FROM dba_users
                           WHERE username like :schema
                           """, {'schema': f'{schema_name.upper()}'})
            
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


def main():
    
   # Configuración del parser de argumentos
    parser = argparse.ArgumentParser(description="Busca usuarios y sus objetos (si se especifica el parametro) en las bases de datos Oracle.", 
                                    usage="%(prog)s [-h] schema_name [object_name]")
    parser.add_argument('schema_name', help='Nombre del esquema a buscar (requerido)')
    parser.add_argument('object_name', nargs='?', default=None, help='Nombre del objeto a buscar (opcional)')
    parser.add_argument('-o', '--object_name', dest='object_name', help='Nombre del objeto a buscar (opcional)')

    args = parser.parse_args()

    # Verificación de que se ha proporcionado un esquema
    if args.schema_name is None:
        print("Error: Falta especificar un nombre de esquema a consultar.")
        parser.print_help()
        sys.exit(1)
        
    # Cargar la configuración
    script_dir = os.path.dirname(__file__)
    # Construye la ruta al archivo relativo al directorio del script
    rel_path = "databases.json"
    abs_file_path = os.path.join(script_dir, rel_path)
    with open(abs_file_path, 'r') as file:
        databases = json.load(file)

    # Recoger todos los resultados
    all_results = []
    # Si no se pasa nomnre de objeto, se consultan los schemas
    if args.object_name is None:
        for db in databases['databases']:
            all_results.extend(list_schema_info(db, args.schema_name))
    else:
        for db in databases['databases']:
            all_results.extend(list_schema_obj(db, args.schema_name, args.object_name))
        
    # Ordenar all_results por Host y SERVICE_NAME
    all_results = sorted(all_results, key=lambda x: (x['HOST'], x['SERVICE_NAME']))
    # Elimina hosts y service_name duplicados para imprimir
    formatted_results = format_results(all_results)
    
    # Mostrar resultados en forma de tabla
    if formatted_results:
        print(tabulate(formatted_results, headers="keys", tablefmt="grid"))
    else:
        print("No se encontraron resultados.")
        
if __name__ == "__main__":
    main()        