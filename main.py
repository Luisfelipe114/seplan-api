import sqlite3
import urllib.request
import json
from pathlib import Path

ROOT_DIR = Path(__file__).parent
DB_NAME = "db.sqlite3"
DB_FILE = ROOT_DIR / DB_NAME

URL_DISTRIBUIDORA = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search?resource_id=b1bd71e7-d0ad-4214-9053-cbd58e9564a7"
URL_HIDRELETRICA = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search?resource_id=c189442a-18f0-44eb-9c89-3b48147a4d65"
URL_EOLICA = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search?resource_id=5f903d78-25ae-4a3f-a2bd-9a93351c59fb"
URL_FOTOVOLTAICA = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search?resource_id=49fa9ca0-f609-4ae3-a6f7-b97bd0945a3a"
URL_TERMELETRICA = "https://dadosabertos.aneel.gov.br/api/3/action/datastore_search?resource_id=bd1d3783-b389-49d8-a828-a56e193d0671"


def convertValue(value):
    try:
        return float(value.replace(",", "."))
    except ValueError:
        return value


def fetchData(url):
    response = urllib.request.urlopen(url)
    data = response.read()
    json_data = json.loads(data)
    return json_data["result"]["records"]


def processInfoGeracaoDistribuida():
    table_name = "InfoGeracaoDistribuidora"
    records = fetchData(URL_DISTRIBUIDORA)

    connection = sqlite3.connect(DB_FILE)
    cursor = connection.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    connection.commit()

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
        DatGeracaoConjuntoDados TEXT,
        AnmPeriodoReferencia TEXT,
        NumCNPJDistribuidora TEXT,
        SigAgente TEXT,
        NomAgente TEXT,
        CodClasseConsumo TEXT,
        DscClasseConsumo TEXT,
        CodSubGrupoTarifario TEXT,
        DscSubGrupoTarifario TEXT,
        CodUFibge TEXT,
        SigUF TEXT,
        CodRegiao TEXT,
        NomRegiao TEXT,
        CodMunicipioIbge TEXT,
        NomMunicipio TEXT,
        CodCEP TEXT,
        SigTipoConsumidor TEXT,
        NumCPFCNPJ TEXT,
        NomTitularEmpreendimento TEXT,
        CodEmpreendimento TEXT,
        DthAtualizaCadastralEmpreend TEXT,
        SigModalidadeEmpreendimento TEXT,
        DscModalidadeHabilitado TEXT,
        QtdUCRecebeCredito TEXT,
        SigTipoGeracao TEXT,
        DscFonteGeracao TEXT,
        DscPorte TEXT,
        NumCoordNEmpreendimento TEXT,
        NumCoordEEmpreendimento TEXT,
        MdaPotenciaInstaladaKW REAL,
        NomSubEstacao TEXT,
        NumCoordESub TEXT,
        NumCoordNSub TEXT
        )
    """
    )
    connection.commit()

    sql = f"""
        INSERT INTO {table_name} (
            DatGeracaoConjuntoDados,
            AnmPeriodoReferencia,
            NumCNPJDistribuidora,
            SigAgente,
            NomAgente,
            CodClasseConsumo,
            DscClasseConsumo,
            CodSubGrupoTarifario,
            DscSubGrupoTarifario,
            CodUFibge,
            SigUF,
            CodRegiao,
            NomRegiao,
            CodMunicipioIbge,
            NomMunicipio,
            CodCEP,
            SigTipoConsumidor,
            NumCPFCNPJ,
            NomTitularEmpreendimento,
            CodEmpreendimento,
            DthAtualizaCadastralEmpreend,
            SigModalidadeEmpreendimento,
            DscModalidadeHabilitado,
            QtdUCRecebeCredito,
            SigTipoGeracao,
            DscFonteGeracao,
            DscPorte,
            NumCoordNEmpreendimento,
            NumCoordEEmpreendimento,
            MdaPotenciaInstaladaKW,
            NomSubEstacao,
            NumCoordESub,
            NumCoordNSub
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for record in records:
        cursor.execute(
            sql,
            (
                record.get("DatGeracaoConjuntoDados"),
                record.get("AnmPeriodoReferencia"),
                record.get("NumCNPJDistribuidora"),
                record.get("SigAgente"),
                record.get("NomAgente"),
                record.get("CodClasseConsumo"),
                record.get("DscClasseConsumo"),
                record.get("CodSubGrupoTarifario"),
                record.get("DscSubGrupoTarifario"),
                record.get("CodUFibge"),
                record.get("SigUF"),
                record.get("CodRegiao"),
                record.get("NomRegiao"),
                record.get("CodMunicipioIbge"),
                record.get("NomMunicipio"),
                record.get("CodCEP"),
                record.get("SigTipoConsumidor"),
                record.get("NumCPFCNPJ"),
                record.get("NomTitularEmpreendimento"),
                record.get("CodEmpreendimento"),
                record.get("DthAtualizaCadastralEmpreend"),
                record.get("SigModalidadeEmpreendimento"),
                record.get("DscModalidadeHabilitado"),
                record.get("QtdUCRecebeCredito"),
                record.get("SigTipoGeracao"),
                record.get("DscFonteGeracao"),
                record.get("DscPorte"),
                record.get("NumCoordNEmpreendimento"),
                record.get("NumCoordEEmpreendimento"),
                convertValue(record.get("MdaPotenciaInstaladaKW")),
                record.get("NomSubEstacao"),
                record.get("NumCoordESub"),
                record.get("NumCoordNSub"),
            ),
        )

    connection.commit()
    cursor.close()
    connection.close()


def processInfoHidreletrica():
    table_name = "InfoHidreletrica"
    records = fetchData(URL_HIDRELETRICA)

    connection = sqlite3.connect(DB_FILE)
    cursor = connection.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    connection.commit()

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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
    """
    )
    connection.commit()

    # Insert data
    sql = f"""
        INSERT INTO {table_name} (
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
    """

    for record in records:
        cursor.execute(
            sql,
            (
                record.get("DatGeracaoConjuntoDados"),
                record.get("CodGeracaoDistribuida"),
                record.get("NomRio"),
                convertValue(record.get("MdaPotenciaInstalada")),
                record.get("DatConexao"),
                convertValue(record.get("MdaPotenciaAparente")),
                convertValue(record.get("MdaFatorPotencia")),
                convertValue(record.get("MdaTensao")),
                convertValue(record.get("MdaNivelOperacionalMontante")),
                convertValue(record.get("MdaNivelOperacionalJusante")),
            ),
        )
    connection.commit()

    cursor.close()
    connection.close()

def processInfoEolica():
    records = fetchData(URL_EOLICA)
    table_name = "InfoEolica"

    connection = sqlite3.connect(DB_FILE)
    cursor = connection.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    connection.commit()

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            DatGeracaoConjuntoDados TEXT,
            CodGeracaoDistribuida TEXT,
            NomFabricanteAerogerador TEXT,
            DscModeloAerogerador TEXT,
            MdaPotenciaInstalada REAL,
            MdaAlturaPa REAL,
            DatConexao TEXT,
            IdcEixoRotor TEXT
        )
    """
    )
    connection.commit()

    sql = f"""
        INSERT INTO {table_name} (
            DatGeracaoConjuntoDados,
            CodGeracaoDistribuida,
            NomFabricanteAerogerador,
            DscModeloAerogerador,
            MdaPotenciaInstalada,
            MdaAlturaPa,
            DatConexao,
            IdcEixoRotor
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    for record in records:
        cursor.execute(
            sql,
            (
                record.get("DatGeracaoConjuntoDados"),
                record.get("CodGeracaoDistribuida"),
                record.get("NomFabricanteAerogerador"),
                record.get("DscModeloAerogerador"),
                convertValue(record.get("MdaPotenciaInstalada")),
                convertValue(record.get("MdaAlturaPa")),
                record.get("DatConexao"),
                record.get("IdcEixoRotor"),
            ),
        )

    connection.commit()
    cursor.close()
    connection.close()

def processInfoFotovoltaica():
    records = fetchData(URL_FOTOVOLTAICA)
    table_name = "InfoFotovoltaica"

    connection = sqlite3.connect(DB_FILE)
    cursor = connection.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    connection.commit()

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            DatGeracaoConjuntoDados TEXT,
            CodGeracaoDistribuida TEXT,
            MdaAreaArranjo REAL,
            MdaPotenciaInstalada REAL,
            NomFabricanteModulo TEXT,
            NomFabricanteInversor TEXT,
            DatConexao TEXT,
            MdaPotenciaModulos REAL,
            MdaPotenciaInversores REAL,
            QtdModulos INTEGER,
            NomModeloModulo TEXT,
            NomModeloInversor TEXT
        )
    """
    )
    connection.commit()

    sql = f"""
        INSERT INTO {table_name} (
            DatGeracaoConjuntoDados,
            CodGeracaoDistribuida,
            MdaAreaArranjo,
            MdaPotenciaInstalada,
            NomFabricanteModulo,
            NomFabricanteInversor,
            DatConexao,
            MdaPotenciaModulos,
            MdaPotenciaInversores,
            QtdModulos,
            NomModeloModulo,
            NomModeloInversor
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for record in records:
        cursor.execute(
            sql,
            (
                record.get("DatGeracaoConjuntoDados"),
                record.get("CodGeracaoDistribuida"),
                convertValue(record.get("MdaAreaArranjo")),
                convertValue(record.get("MdaPotenciaInstalada")),
                record.get("NomFabricanteModulo"),
                record.get("NomFabricanteInversor"),
                record.get("DatConexao"),
                convertValue(record.get("MdaPotenciaModulos")),
                convertValue(record.get("MdaPotenciaInversores")),
                record.get("QtdModulos"),
                record.get("NomModeloModulo"),
                record.get("NomModeloInversor"),
            ),
        )

    connection.commit()
    cursor.close()
    connection.close()

def processInfoTermeletrica():
    records = fetchData(URL_TERMELETRICA)
    table_name = "InfoTermeletrica"

    connection = sqlite3.connect(DB_FILE)
    cursor = connection.cursor()

    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    connection.commit()

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            DatGeracaoConjuntoDados TEXT,
            CodGeracaoDistribuida TEXT,
            MdaPotenciaInstalada REAL,
            DatConexao TEXT,
            DscCicloTermodinamico TEXT,
            DscMaquinaMotrizTermica TEXT
        )
    """
    )
    connection.commit()

    sql = f"""
        INSERT INTO {table_name} (
            DatGeracaoConjuntoDados,
            CodGeracaoDistribuida,
            MdaPotenciaInstalada,
            DatConexao,
            DscCicloTermodinamico,
            DscMaquinaMotrizTermica
        ) VALUES (?, ?, ?, ?, ?, ?)
    """

    for record in records:
        cursor.execute(
            sql,
            (
                record.get("DatGeracaoConjuntoDados"),
                record.get("CodGeracaoDistribuida"),
                convertValue(record.get("MdaPotenciaInstalada")),
                record.get("DatConexao"),
                record.get("DscCicloTermodinamico"),
                record.get("DscMaquinaMotrizTermica"),
            ),
        )

    connection.commit()
    cursor.close()
    connection.close()

if __name__ == "__main__":
    processInfoGeracaoDistribuida()
    processInfoEolica()
    processInfoFotovoltaica()
    processInfoHidreletrica()
    processInfoTermeletrica()
