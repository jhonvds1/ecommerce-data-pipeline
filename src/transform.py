import pandas as pd
from extract import extract_products, extract_customers, extract_sales

# Mapeamento de estados
ESTADO_MAP = {
    "acre": "AC", "alagoas": "AL", "amapa": "AP", "amazonas": "AM",
    "bahia": "BA", "ceara": "CE", "distrito federal": "DF", "espirito santo": "ES",
    "goias": "GO", "maranhao": "MA", "mato grosso": "MT", "mato grosso do sul": "MS",
    "minas gerais": "MG", "para": "PA", "paraiba": "PB", "parana": "PR",
    "pernambuco": "PE", "piaui": "PI", "rio de janeiro": "RJ",
    "rio grande do norte": "RN", "rio grande do sul": "RS", "rondonia": "RO",
    "roraima": "RR", "santa catarina": "SC", "sao paulo": "SP", "sergipe": "SE",
    "tocantins": "TO"
}

# Funções de transformação
def clean_customers(customers: pd.DataFrame) -> pd.DataFrame:
    """Limpa e padroniza os dados dos clientes"""
    customers['name'] = customers['name'].str.strip().str.lower()
    customers = customers.drop_duplicates()
    
    # Conversão de tipos
    customers[['name', 'email', 'city', 'state']] = customers[['name', 'email', 'city', 'state']].astype(str)
    customers['birthdate'] = pd.to_datetime(customers['birthdate'], errors='coerce')
    
    # Remover nulos
    customers = customers.dropna()
    
    # Filtrar por idade válida (18-120)
    hoje = pd.Timestamp.now()
    data_min = hoje - pd.DateOffset(years=120)
    data_max = hoje - pd.DateOffset(years=18)
    customers = customers[(customers['birthdate'] >= data_min) & (customers['birthdate'] <= data_max)]
    
    # Padronizar estado
    customers["state"] = (
        customers["state"]
        .str.lower()
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .map(ESTADO_MAP)
        .fillna(customers['state'])
    )
    
    return customers

def clean_products(products: pd.DataFrame) -> pd.DataFrame:
    """Limpa e padroniza os dados de produtos"""
    products = products.drop_duplicates()
    products[['name', 'category']] = products[['name', 'category']].astype(str)
    products['price'] = pd.to_numeric(products['price'], errors='coerce')
    products = products.dropna()
    return products

def clean_sales(sales: pd.DataFrame) -> pd.DataFrame:
    """Limpa e padroniza os dados de vendas"""
    sales = sales.drop_duplicates()
    sales['sale_date'] = pd.to_datetime(sales['sale_date'], errors='coerce')
    sales['unit_price'] = pd.to_numeric(sales['unit_price'], errors='coerce')
    sales = sales.dropna()
    return sales

def merge_data(customers: pd.DataFrame, sales: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    """Realiza o merge das tabelas em um dataset único"""
    customers_sales = pd.merge(customers, sales, on='customer_id')
    all_dfs = customers_sales.merge(products, on='product_id')
    all_dfs = all_dfs.rename(columns={'name_x': 'nome_cliente', 'name_y': 'name_product'})
    return all_dfs

def create_metrics(all_dfs: pd.DataFrame) -> pd.DataFrame:
    """Cria métricas linha a linha no dataset"""
    hoje = pd.Timestamp.now()
    
    # Valor total da venda
    all_dfs['total_value'] = all_dfs['quantity'] * all_dfs['unit_price']
    
    # Idade do cliente
    all_dfs['customer_age'] = (hoje - all_dfs['birthdate']).dt.days // 365
    
    # Cliente recorrente
    counts = all_dfs['customer_id'].value_counts()
    all_dfs['is_repeat_customer'] = all_dfs['customer_id'].map(lambda x: counts[x] > 1)
    
    return all_dfs

def save_dataset(all_dfs: pd.DataFrame, path: str = "data_processed/dataset_final.csv") -> None:
    """Salva o dataset processado em CSV"""
    all_dfs.to_csv(path, index=False)
