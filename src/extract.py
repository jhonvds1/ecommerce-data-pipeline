import pandas as pd


    # EXTRAI OS DADOS DE CADA .CSV

def extract_customers():
    return pd.read_csv("data_raw/customers.csv")

def extract_products():
    return pd.read_csv("data_raw/products.csv")

def extract_sales():
    return pd.read_csv("data_raw/sales.csv")