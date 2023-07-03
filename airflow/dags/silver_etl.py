import config
import database_connect
import pandas as pd

host = config.host_mysql_destino
port = config.port_mysql_destino
database = config.database_mysql_destino
user = config.user_mysql_destino
password = config.password_mysql_destino

def read_from_mysql_bronze(arquivo):
    df = database_connect.read_from_mysql(host, port, database, user, password, arquivo)

    return df

def write_to_silver(df, table_name):

    database_connect.write_to_mysql(host, port, database, user, password, df, table_name)