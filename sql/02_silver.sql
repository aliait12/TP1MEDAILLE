-- =============================================
-- SILVER LAYER : données nettoyées
-- =============================================

CREATE DATABASE IF NOT EXISTS ecommerce_silver;
USE ecommerce_silver;

DROP TABLE IF EXISTS customers_clean;
DROP TABLE IF EXISTS products_clean;
DROP TABLE IF EXISTS orders_clean;

CREATE TABLE customers_clean (
    customer_id VARCHAR(20),
    source_system VARCHAR(10),
    nom VARCHAR(100),
    email VARCHAR(150),
    telephone VARCHAR(20),
    ville VARCHAR(100),
    segment VARCHAR(50),
    date_creation DATE
);

CREATE TABLE products_clean (
    product_id INT,
    source_system VARCHAR(10),
    nom_produit VARCHAR(200),
    categorie VARCHAR(100),
    prix_catalogue DECIMAL(10,2),
    prix_achat DECIMAL(10,2),
    prix_vente DECIMAL(10,2),
    stock INT
);

CREATE TABLE orders_clean (
    order_id VARCHAR(20),
    source_system VARCHAR(10),
    client_id VARCHAR(20),
    product_id INT,
    date_commande DATE,
    quantite INT,
    prix_unitaire DECIMAL(10,2),
    statut VARCHAR(50)
);

-- Clients depuis CRM
INSERT INTO customers_clean
SELECT DISTINCT
    crm_client_id,
    'CRM',
    TRIM(nom),
    LOWER(TRIM(email)),
    TRIM(telephone),
    TRIM(ville),
    UPPER(TRIM(segment)),
    STR_TO_DATE(date_inscription, '%Y-%m-%d')
FROM ecommerce_bronze.crm_clients_raw
WHERE crm_client_id IS NOT NULL;

-- Clients depuis ERP
INSERT INTO customers_clean
SELECT DISTINCT
    erp_client_id,
    'ERP',
    TRIM(nom),
    NULL,
    NULL,
    TRIM(ville),
    NULL,
    STR_TO_DATE(date_creation, '%Y-%m-%d')
FROM ecommerce_bronze.erp_clients_raw
WHERE erp_client_id IS NOT NULL;

-- Produits depuis CRM
INSERT INTO products_clean (product_id, source_system, nom_produit, categorie, prix_catalogue)
SELECT DISTINCT
    crm_product_id,
    'CRM',
    TRIM(nom_produit),
    TRIM(categorie),
    prix_catalogue
FROM ecommerce_bronze.crm_produits_raw
WHERE crm_product_id IS NOT NULL;

-- Produits depuis ERP
INSERT INTO products_clean (product_id, source_system, nom_produit, categorie, prix_achat, prix_vente, stock)
SELECT DISTINCT
    erp_product_id,
    'ERP',
    TRIM(designation),
    TRIM(categorie_stock),
    prix_achat,
    prix_vente,
    stock
FROM ecommerce_bronze.erp_produits_raw
WHERE erp_product_id IS NOT NULL;

-- Commandes depuis CRM
INSERT INTO orders_clean
SELECT DISTINCT
    crm_order_id,
    'CRM',
    crm_client_id,
    crm_product_id,
    STR_TO_DATE(date_commande, '%Y-%m-%d'),
    quantite,
    NULL,
    NULL
FROM ecommerce_bronze.crm_details_commandes_raw
WHERE crm_order_id IS NOT NULL;

-- Commandes depuis ERP
INSERT INTO orders_clean
SELECT DISTINCT
    erp_order_id,
    'ERP',
    erp_client_id,
    erp_product_id,
    STR_TO_DATE(date_facture, '%Y-%m-%d'),
    quantite,
    prix_unitaire,
    UPPER(TRIM(statut))
FROM ecommerce_bronze.erp_details_commandes_raw
WHERE erp_order_id IS NOT NULL;