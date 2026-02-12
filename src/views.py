import pandas as pd
import psycopg2

def get_connection():
    """Cria e retorna uma conexão com o PostgreSQL"""
    return psycopg2.connect(
        host='localhost',
        database='ecommerce',
        user='postgres',
        password='laura'
    )

def fetch_view(view_name: str) -> pd.DataFrame:
    """
    Retorna os dados de uma view como DataFrame.
    Usa cursor do psycopg2 para evitar warning do Pandas.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {view_name}")
    colnames = [desc[0] for desc in cursor.description]  # nomes das colunas
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return pd.DataFrame(rows, columns=colnames)

def top10_clientes():
    return fetch_view("vw_top10_clientes")

def ticket_medio_estado():
    return fetch_view("vw_ticket_medio_estado")

def faturamento_mensal():
    return fetch_view("vw_faturamento_mensal")

def produtos_mais_vendidos():
    return fetch_view("vw_produtos_mais_vendidos")

def clientes_recorrentes():
    return fetch_view("vw_clientes_recorrentes")
