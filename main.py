import requests
import psycopg2
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# Pega a URL do banco a partir da variável de ambiente
conn_db = os.environ['DATABASE_URL']

# Link API do Binance para BTC/BRL - últimas 1000 velas de 1 hora
link = 'https://api.binance.com/api/v3/klines?symbol=BTCBRL&interval=1h&limit=1000'

# Extrai dados do Binance
dados = requests.get(link).json()

# Conecta com o Banco de Dados PostgreSQL
with psycopg2.connect(conn_db) as conn:
    cur = conn.cursor() 

    # Executa comando SQL para criar a tabela 
    cur.execute("""
        CREATE TABLE IF NOT EXISTS binance_btc (
            id SERIAL PRIMARY KEY,
            dt_hr_abertura TIMESTAMP,
            preco_abertura FLOAT,
            preco_maximo FLOAT,
            preco_minimo FLOAT,
            preco_fechamento FLOAT,
            volume_btc FLOAT,
            volume_brl FLOAT,
            num_trades INTEGER
        );
    """)

    # Coleta e ordena os dados
    for registro in dados:
        # Converte timestamp de milissegundos para datetime
        dt = datetime.fromtimestamp(registro[0] / 1000)
        
        preco_abertura = float(registro[1])
        preco_maximo = float(registro[2])
        preco_minimo = float(registro[3])
        preco_fechamento = float(registro[4])
        volume_btc = float(registro[5])
        volume_brl = float(registro[7])
        num_trades = int(registro[8])

        # Executa comando SQL para inserir dados na tabela
        cur.execute("""
            INSERT INTO binance_btc (dt_hr_abertura, preco_abertura, preco_maximo, 
                                   preco_minimo, preco_fechamento, volume_btc, 
                                   volume_brl, num_trades)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (dt, preco_abertura, preco_maximo, preco_minimo, 
              preco_fechamento, volume_btc, volume_brl, num_trades))

    # Confirma todas as operações feitas na conexão e encerra a conexão
    conn.commit()
    cur.close()  

print("Dados do Binance inseridos com sucesso!")