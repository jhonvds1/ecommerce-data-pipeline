# Projeto E-commerce DW – ETL e Análise de Dados

## Sobre o projeto
Este projeto simula um **Data Warehouse (DW)** de um e-commerce, com **pipeline completo de ETL**:

1. **Extract** – extração de dados de fonte bruta (CSV)  
2. **Transform** – limpeza, padronização e criação de métricas (idade, ticket médio, cliente recorrente, valor total de venda)  
3. **Load** – carga das tabelas no **PostgreSQL**, incluindo tabelas de dimensões (`dim_customers`, `dim_products`) e tabela de fatos (`fact_sales`)  

Além disso, são criadas **views analíticas** para insights estratégicos, como top clientes, faturamento mensal, ticket médio por estado, produtos mais vendidos e clientes recorrentes.

---

##  Estrutura do projeto

project/
│
├── data_raw/ # Dados brutos (CSV ou extraídos da API)
├── data_processed/ # Dados limpos e transformados
├── src/
│ ├── extract.py # Funções para extrair os dados
│ ├── transform.py # Limpeza, merge e criação de métricas
│ ├── load.py # Funções para carregar dados no PostgreSQL
│ ├── main.py # Executa pipeline completo: ETL
│ └── views.py # Consulta views analíticas
├── database/
│ ├──schema.sql # Criação de tabelas e views
| └──modelagem.png # star schema do DB
└── README.md # Este arquivo



---

##  Tecnologias utilizadas
- **Python** – Pandas, Faker, psycopg2  
- **PostgreSQL** – Data Warehouse, tabelas dimensões e fatos, views analíticas  
- **CSV** – fontes de dados simulada  

---

##  Funcionalidades do projeto
- Extração de dados de clientes, produtos e vendas  
- Limpeza e padronização:
  - Remoção de duplicados e valores nulos  
  - Padronização de nomes e estados  
  - Filtragem por idade válida (18 a 120 anos)  
- Criação de métricas:
  - `total_value` → valor total de cada venda  
  - `customer_age` → idade do cliente  
  - `is_repeat_customer` → identifica clientes recorrentes  
- Merge das tabelas em um **dataset final**  
- Carga no PostgreSQL nas tabelas:
  - `dim_customers`  
  - `dim_products`  
  - `fact_sales`  
- Criação de views analíticas:
  - `vw_top10_clientes`  
  - `vw_ticket_medio_estado`  
  - `vw_faturamento_mensal`  
  - `vw_produtos_mais_vendidos`  
  - `vw_clientes_recorrentes`  

---

##  Como executar
1. Clonar o projeto:  
```bash
git clone <link-do-repo>
cd project
2. Instalar dependências:
pip install -r requirements.txt

