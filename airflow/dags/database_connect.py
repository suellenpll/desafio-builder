import mysql.connector
from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine
import os
import config

def read_from_mongodb(connection_string, database, collection):

    try:
        # Conectar ao MongoDB
        client = MongoClient(connection_string)

        # Acessar o banco de dados e a coleção
        db = client[database]
        collections = db[collection]

        # Consultar os documentos na coleção
        documents = collections.find()

        # Criar o DataFrame do pandas
        df = pd.DataFrame(documents)
        df['_id'] = df['_id'].apply(str)
        df.fillna('Null', inplace=True)

        print('Conexão bem-sucedida ao banco de dados MongoDB')

        # Exibir o DataFrame
        return df

        # Fechar a conexão
        client.close()

    except Exception as e:
        print("Erro ao conectar-se ao MongoDB:", e)

def read_from_mysql(host, port, database, user, password, arquivo):
    try:
        # Criar uma conexão SQLAlchemy com o MySQL
        engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}')

        print('Conexão bem-sucedida ao banco de dados MySQL')

        # Consultar os dados do banco de dados
        with open(arquivo, 'r') as file:
            query = file.read()
            
        df = pd.read_sql(query, engine)

        # Exibir o DataFrame
        return df

    except mysql.connector.Error as e:
        print('Erro ao conectar-se ao MySQL:', e)

def write_to_mysql(host, port, database, user, password, df, table_name):
    try:
        # Criar uma conexão SQLAlchemy com o MySQL
        engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}')

        print('Conexão bem-sucedida ao banco de dados MySQL')

        # Salvar o DataFrame na tabela do MySQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        print('Dados salvos na tabela', table_name)

    except mysql.connector.Error as e:
        print('Erro ao conectar-se ao MySQL:', e)