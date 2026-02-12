from extract import extract_customers, extract_products, extract_sales
from transform import (
    clean_customers, clean_products, clean_sales,
    merge_data, create_metrics, save_dataset
)
from load import load_to_db
from views import fetch_view


def main():
    #  EXTRAÇÃO
    print("Iniciando extracao de dados...")
    customers = extract_customers()
    products = extract_products()
    sales = extract_sales()
    print("Extracao concluida!")

    # TRANSFORMAÇÃO
    print("Iniciando transformacao dos dados...")
    customers = clean_customers(customers)
    products = clean_products(products)
    sales = clean_sales(sales)

    all_dfs = merge_data(customers, sales, products)
    all_dfs = create_metrics(all_dfs)
    print(f"Transformacao concluida! Dataset final com {len(all_dfs)} linhas.")

    # CARGA NO BANCO
    print("Iniciando carga no banco de dados...")
    load_to_db()
    print("Carga concluida com sucesso!")


    # EXECUTAR VIEWS
def executar_views():
    view_names = [
        "vw_top10_clientes",
        "vw_ticket_medio_estado",
        "vw_faturamento_mensal",
        "vw_produtos_mais_vendidos",
        "vw_clientes_recorrentes"
    ]
    for view in view_names:
        print(f"\n===== {view} =====")
        df = fetch_view(view)
        print(df)


if __name__ == "__main__":
    main()
    executar_views()
