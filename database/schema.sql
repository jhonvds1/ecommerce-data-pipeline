-- Tabelas do Data Warehouse (Dimensões e Fato)


-- Tabela de clientes (Dimensão)
-- Armazena informações dos clientes
CREATE TABLE IF NOT EXISTS dim_customers(
	customer_id SERIAL PRIMARY KEY,      -- ID único do cliente, gerado automaticamente
	nome_cliente VARCHAR(100),           -- Nome do cliente
	email VARCHAR(100),                   -- E-mail do cliente
	city VARCHAR(50),                     -- Cidade
	state CHAR(2),                        -- Estado (sigla)
	birthdate DATE                        -- Data de nascimento
);

-- Tabela de produtos (Dimensão)
-- Armazena informações dos produtos
CREATE TABLE IF NOT EXISTS dim_products(
	product_id SERIAL PRIMARY KEY,        -- ID único do produto, gerado automaticamente
	name_product VARCHAR(100),            -- Nome do produto
	category VARCHAR(50),                 -- Categoria do produto
	price NUMERIC (10,2)                  -- Preço unitário
);

-- Tabela de vendas (Fato)
-- Armazena cada venda realizada
CREATE TABLE IF NOT EXISTS fact_sales(
	sale_id SERIAL PRIMARY KEY,           -- ID único da venda, gerado automaticamente
	customer_id INT REFERENCES dim_customers(customer_id),  -- Relaciona à tabela de clientes
	product_id INT REFERENCES dim_products(product_id),     -- Relaciona à tabela de produtos
	quantity INT,                         -- Quantidade vendida
	unit_price NUMERIC(10,2),             -- Preço unitário da venda
	sale_date DATE                         -- Data da venda
);

-- Views (Consultas pré-definidas para análise)


-- Top 10 clientes por gasto total
-- Mostra os clientes que mais gastaram, com total de compras e valor gasto
CREATE OR REPLACE VIEW vw_top10_clientes AS
SELECT
	cus.customer_id,
	cus.nome_cliente,
	SUM(sal.quantity * unit_price) AS gasto_total,   -- Soma de todas as compras do cliente
	COUNT(sal.sale_id) AS total_compras              -- Total de compras feitas pelo cliente
FROM fact_sales AS sal
LEFT JOIN dim_customers AS cus
	ON cus.customer_id = sal.customer_id
GROUP BY cus.customer_id, cus.nome_cliente 
ORDER BY gasto_total DESC
LIMIT 10;

-- Faturamento mensal
-- Mostra quanto a empresa faturou em cada mês
CREATE OR REPLACE VIEW vw_faturamento_mensal AS
SELECT 
	EXTRACT(MONTH FROM sale_date) AS mes,              -- Extrai o mês da venda
	SUM(quantity * unit_price) AS faturamento         -- Soma total vendida no mês
FROM fact_sales
GROUP BY mes
ORDER BY faturamento;

-- Ticket médio por estado
-- Calcula o valor médio gasto por venda em cada estado
CREATE OR REPLACE VIEW vw_ticket_medio_estado AS
SELECT 
	cus.state,
	ROUND(AVG(sal.quantity * sal.unit_price), 2) AS ticket_medio
FROM fact_sales AS sal
LEFT JOIN dim_customers AS cus
	on sal.customer_id = cus.customer_id
GROUP BY cus.state;

-- Produtos mais vendidos
-- Lista os produtos e categorias que tiveram mais vendas
CREATE OR REPLACE VIEW vw_produtos_mais_vendidos AS
SELECT 
	pro.name_product,
	pro.category,
	SUM(sal.quantity) AS total_vendido
FROM fact_sales AS sal
LEFT JOIN dim_products AS pro
	ON sal.product_id = pro.product_id
GROUP BY pro.category, pro.name_product
ORDER BY pro.category, total_vendido DESC;

-- Clientes recorrentes
-- Mostra clientes que fizeram mais de uma compra, ordenando pelo gasto total
CREATE OR REPLACE VIEW vw_clientes_recorrentes AS
SELECT 
	cus.customer_id,
	cus.nome_cliente,
	COUNT(sal.sale_id) AS total_compras,              -- Quantidade de compras
	SUM(sal.quantity * sal.unit_price) total_gasto    -- Total gasto
FROM fact_sales AS sal
LEFT JOIN dim_customers AS cus
	ON cus.customer_id = sal.customer_id
GROUP BY cus.customer_id, cus.nome_cliente
HAVING COUNT(sal.sale_id) > 1                        -- Somente clientes com mais de uma compra
ORDER BY total_gasto DESC;