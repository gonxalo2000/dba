import cx_Oracle
import json
import os
from flask import Flask, render_template, request

app = Flask(__name__)


def check_schema_in_db(db_info, schema_name):
    # Aquí va tu lógica existente de check_schema_in_db, con algunas modificaciones para no imprimir
    results = []
    try:
        dsn = cx_Oracle.makedsn(db_info['host'], db_info['port'], service_name=db_info['service_name'])
        conn = cx_Oracle.connect(user='xxx', password='xxx', dsn=dsn)
        cursor = conn.cursor()
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
        results.append({'ERROR': f'Error al conectar o consultar en {db_info["name"]}: {error}'})
    return results

@app.route('/', methods=['GET', 'POST'])
def home():
    results = []
    if request.method == 'POST':
        schema_name = request.form.get('schema_name')
        if schema_name:
            script_dir = os.path.dirname(__file__)
            rel_path = "databases.json"
            abs_file_path = os.path.join(script_dir, rel_path)
            with open(abs_file_path, 'r') as file:
                databases = json.load(file)

            for db in databases['databases']:
                results.extend(check_schema_in_db(db, schema_name))
        
    return render_template('index.html', results=results)

if __name__ == '__main__':
    app.run(debug=True)