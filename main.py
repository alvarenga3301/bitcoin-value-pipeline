import requests  
import psycopg2
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# Pega a URL do banco a partir da variável de ambiente
conn_db = os.environ['DATABASE_URL']

# Link API do IBGE
link ='https://servicodados.ibge.gov.br/api/v1/rmpg/nivel/EMARC?momentoInicial=2025-01-07-10-00&momentoFinal=2025-01-07-13-00&incluirPrevisao=S'


#Extrai dados do nivel do mar
dados = requests.get(link).json()

# Conecta com o Banco de Dados PostgreSQL
with psycopg2.connect(conn_db) as conn:
    cur = conn.cursor() 

    #Executa comando SQL para criar a tabela 
    cur.execute("""
        CREATE TABLE IF NOT EXISTS maregrafo_ibge (
            id SERIAL PRIMARY KEY,
            dt_hr_leitura TIMESTAMP,
            radar FLOAT,
            encoder FLOAT,
            previsao FLOAT
        );
    """)

    #coleta e ordena os dados
    for registro in dados:
        dt_str = registro['dtHrLeitura']
        dt_str = dt_str[:10] + ' ' + dt_str[11:13] + ':' + dt_str[14:]
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")

        radar = registro.get('radar')
        encoder = registro.get('encoder')
        previsao = registro.get('previsao')
        

         #executa comando SQL para inserir dados na tabela
        cur.execute("""
            INSERT INTO maregrafo_ibge (dt_hr_leitura, radar, encoder, previsao)
            VALUES (%s, %s, %s, %s)
        """, (dt, radar, encoder, previsao))

    #confirma todas as operações feitas na conexão e encerra a conexão
    conn.commit()
    cur.close()  

print("Dados inseridos com sucesso!")





