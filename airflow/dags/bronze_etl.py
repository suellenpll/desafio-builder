import config
import database_connect
import pandas as pd

def read_from_mysql_builders():
    host = config.host_mysql_origem
    port = config.port_mysql_origem
    database = config.database_mysql_origem
    user = config.user_mysql_origem
    password = config.password_mysql_origem

    df = database_connect.read_from_mysql(host, port, database, user, password, './queries/bronze_mysql_covid.sql')
    df['city'] = df['city'].str.encode('latin-1').str.decode('utf-8')

    return df

def read_from_mongodb_builders():
    connection_string = config.string_conexao_mongodb
    database = config.nome_banco_mongodb
    collection = config.nome_colecao_mongodb

    df = pd.DataFrame()
    df = database_connect.read_from_mongodb(connection_string, database, collection)

    return df

def write_to_bronze(df, table_name):
    host = config.host_mysql_destino
    port = config.port_mysql_destino
    database = config.database_mysql_destino
    user = config.user_mysql_destino
    password = config.password_mysql_destino

    database_connect.write_to_mysql(host, port, database, user, password, df, table_name)