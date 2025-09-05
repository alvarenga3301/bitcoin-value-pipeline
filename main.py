import requests
import psycopg2
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# Pega a URL do banco a partir da variável de ambiente
conn_db = os.environ['DATABASE_URL']

def get_btc_data():
    """Pega dados do Bitcoin da Binance"""
    try:
        link = 'https://api.binance.com/api/v3/klines?symbol=BTCBRL&interval=1h&limit=1000'
        response = requests.get(link, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar API Binance: {e}")
        return []

def get_usd_data():
    """Pega dados do Dólar da AwesomeAPI"""
    try:
        url = 'https://economia.awesomeapi.com.br/json/daily/USD-BRL/7'
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao acessar API USD: {e}")
        return []

def process_usd_data(usd_data):
    """Processa dados do Dólar"""
    processed = []
    for entry in usd_data:
        try:
            # Converte timestamp para datetime
            dt = datetime.fromtimestamp(int(entry['timestamp']))
            processed.append({
                'dt_hr': dt,
                'bid': float(entry['bid']),    # Preço de compra
                'ask': float(entry['ask']),    # Preço de venda
                'high': float(entry['high']),  # Máximo do dia
                'low': float(entry['low']),    # Mínimo do dia
                'currency': 'USD'
            })
        except (KeyError, ValueError) as e:
            print(f"Erro ao processar dado USD: {e}")
    return processed

# Conecta com o Banco de Dados PostgreSQL
try:
    with psycopg2.connect(conn_db) as conn:
        cur = conn.cursor() 

        # cria tabela btc (COM CONTROLE DE DUPLICATAS)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS binance_btc (
                id SERIAL PRIMARY KEY,
                dt_hr_abertura TIMESTAMP UNIQUE,
                preco_abertura FLOAT,
                preco_maximo FLOAT,
                preco_minimo FLOAT,
                preco_fechamento FLOAT,
                volume_btc FLOAT,
                volume_brl FLOAT,
                num_trades INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # cria tabela usd (COM CONTROLE DE DUPLICATAS)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fiat_rates (
                id SERIAL PRIMARY KEY,
                dt_hr TIMESTAMP,
                currency VARCHAR(10),
                bid FLOAT,
                ask FLOAT,
                high FLOAT,
                low FLOAT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(dt_hr, currency) 
            );
        """)

        #coleta dados
        print("Coletando dados BTC...")
        btc_data = get_btc_data()
        
        print("Coletando dados USD...")
        usd_data = get_usd_data()
        usd_processed = process_usd_data(usd_data)

        # insere dados btc (COM VERIFICAÇÃO DE DUPLICATA)
        print("Inserindo dados BTC...")
        btc_count = 0
        for registro in btc_data:
            try:
                dt = datetime.fromtimestamp(registro[0] / 1000)
                
                # verifica se já existe para evitar duplicatas
                cur.execute("SELECT 1 FROM binance_btc WHERE dt_hr_abertura = %s", (dt,))
                if cur.fetchone() is None:  # Só insere se não existir
                    cur.execute("""
                        INSERT INTO binance_btc (dt_hr_abertura, preco_abertura, preco_maximo, 
                                               preco_minimo, preco_fechamento, volume_btc, 
                                               volume_brl, num_trades)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        dt,
                        float(registro[1]),
                        float(registro[2]),
                        float(registro[3]),
                        float(registro[4]),
                        float(registro[5]),
                        float(registro[7]),
                        int(registro[8])
                    ))
                    btc_count += 1
            except Exception as e:
                print(f"Erro ao inserir dado BTC: {e}")

        # insere dados usd (COM CONTROLE DE DUPLICATAS CORRIGIDO)
        print("Inserindo dados USD...")
        usd_count = 0
        for usd_entry in usd_processed:
            try:
                cur.execute("""
                    INSERT INTO fiat_rates (dt_hr, currency, bid, ask, high, low)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (dt_hr, currency) DO NOTHING
                """, (
                    usd_entry['dt_hr'],
                    usd_entry['currency'],
                    usd_entry['bid'],
                    usd_entry['ask'],
                    usd_entry['high'],
                    usd_entry['low']
                ))
                usd_count += 1
            except Exception as e:
                print(f"Erro ao inserir dado USD: {e}")
                conn.rollback()
                break  # Sai do loop para evitar erros

        # Confirma todas as operações
        conn.commit()
        cur.close()
        
        print("✅ Dados atualizados com sucesso!")
        print(f"→ BTC: {btc_count} novos registros inseridos")
        print(f"→ USD: {usd_count} novos registros inseridos")

except psycopg2.Error as e:
    print(f"Erro de banco de dados: {e}")
except Exception as e:
    print(f"Erro inesperado: {e}")