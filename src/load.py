import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os

load_dotenv()  # carrega as variáveis do .env

def connect_db():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )


# Função para carregar CSV processado
def load_csv(path="data_processed/dataset_final.csv"):
    return pd.read_csv(path)

# Função para inserir dados em dimensões
def insert_dimensions(cursor, df):
    # Clientes
    execute_batch(cursor, """
        INSERT INTO dim_customers (customer_id, nome_cliente, email, city, state, birthdate)
        VALUES (%s, %s, %s, %s, %s, %s);
    """, df[['customer_id', 'nome_cliente','email','city','state','birthdate']].drop_duplicates().values.tolist())

    # Produtos
    execute_batch(cursor, """
        INSERT INTO dim_products (product_id, name_product, category, price)
        VALUES (%s, %s, %s, %s);
    """, df[['product_id', 'name_product','category','price']].drop_duplicates().values.tolist())

# Função para inserir dados na tabela fato
def insert_fact_sales(cursor, df):
    execute_batch(cursor, """
        INSERT INTO fact_sales (customer_id, product_id, quantity, unit_price, sale_date)
        VALUES (%s, %s, %s, %s, %s);
    """, df[['customer_id','product_id','quantity','unit_price','sale_date']].values.tolist())

# Função principal
def load_to_db():
    # Conecta no banco
    conn = connect_db()
    cursor = conn.cursor()

    # Carrega CSV processado
    all_dfs = load_csv()

    # Insere dados nas dimensões
    insert_dimensions(cursor, all_dfs)

    # Insere dados na tabela fato
    insert_fact_sales(cursor, all_dfs)

    # Commit e fecha conexão
    conn.commit()
    cursor.close()
    conn.close()
    print("Dados carregados com sucesso!")