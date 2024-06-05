import sqlite3
import urllib.request
import json
from pathlib import Path

url = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search?resource_id=c189442a-18f0-44eb-9c89-3b48147a4d65"

response = urllib.request.urlopen(url)
data = response.read()
json_data = json.loads(data)
records = json_data['result']['records']

ROOT_DIR = Path(__file__).parent
DB_NAME = 'db.sqlite3'
DB_FILE = ROOT_DIR / DB_NAME
TABLE_NAME = 'InfoHidreletrica'

def convert_value(value):
    try:
        return float(value.replace(',', '.'))
    except ValueError:
        return value

if __name__ == "__main__":
    connection = sqlite3.connect(DB_FILE)
    cursor = connection.cursor()

    cursor.execute(f'DROP TABLE IF EXISTS {TABLE_NAME}')
    connection.commit()

    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            DatGeracaoConjuntoDados TEXT,
            CodGeracaoDistribuida TEXT,
            NomRio TEXT,
            MdaPotenciaInstalada REAL,
            DatConexao TEXT,
            MdaPotenciaAparente REAL,
            MdaFatorPotencia REAL,
            MdaTensao REAL,
            MdaNivelOperacionalMontante REAL,
            MdaNivelOperacionalJusante REAL
        )
    ''')
    connection.commit()

    # Insert data
    sql = f'''
        INSERT INTO {TABLE_NAME} (
            DatGeracaoConjuntoDados,
            CodGeracaoDistribuida,
            NomRio,
            MdaPotenciaInstalada,
            DatConexao,
            MdaPotenciaAparente,
            MdaFatorPotencia,
            MdaTensao,
            MdaNivelOperacionalMontante,
            MdaNivelOperacionalJusante
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

    for record in records:
        cursor.execute(sql, (
            record.get('DatGeracaoConjuntoDados'),
            record.get('CodGeracaoDistribuida'),
            record.get('NomRio'),
            convert_value(record.get('MdaPotenciaInstalada')),
            record.get('DatConexao'),
            convert_value(record.get('MdaPotenciaAparente')),
            convert_value(record.get('MdaFatorPotencia')),
            convert_value(record.get('MdaTensao')),
            convert_value(record.get('MdaNivelOperacionalMontante')),
            convert_value(record.get('MdaNivelOperacionalJusante'))
        ))
    connection.commit()

    cursor.close()
    connection.close()
