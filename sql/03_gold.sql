-- =============================================
-- GOLD LAYER : Schéma en Étoile
-- =============================================

CREATE DATABASE IF NOT EXISTS ecommerce_gold;
USE ecommerce_gold;

DROP TABLE IF EXISTS fact_ventes;
DROP TABLE IF EXISTS dim_client;
DROP TABLE IF EXISTS dim_produit;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_client (
    client_key INT AUTO_INCREMENT PRIMARY KEY,
    customer_id VARCHAR(20),
    nom VARCHAR(100),
    email VARCHAR(150),
    ville VARCHAR(100),
    segment VARCHAR(50)
);

CREATE TABLE dim_produit (
    produit_key INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT,
    nom_produit VARCHAR(200),
    categorie VARCHAR(100),
    prix_catalogue DECIMAL(10,2),
    prix_achat DECIMAL(10,2)
);

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    date_val DATE,
    annee INT,
    mois INT,
    jour INT,
    trimestre INT
);

CREATE TABLE fact_ventes (
    vente_id INT AUTO_INCREMENT PRIMARY KEY,
    client_key INT,
    produit_key INT,
    date_key INT,
    quantite INT,
    prix_unitaire DECIMAL(10,2),
    revenue DECIMAL(12,2),
    cost DECIMAL(12,2),
    profit DECIMAL(12,2),
    statut VARCHAR(50),
    FOREIGN KEY (client_key) REFERENCES dim_client(client_key),
    FOREIGN KEY (produit_key) REFERENCES dim_produit(produit_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);

-- dim_client : depuis CRM (IDs numériques)
INSERT INTO dim_client (customer_id, nom, email, ville, segment)
SELECT DISTINCT customer_id, nom, email, ville, segment
FROM ecommerce_silver.customers_clean
WHERE source_system = 'CRM';

-- dim_produit : CRM pour le nom/catégorie, ERP pour prix_achat
INSERT INTO dim_produit (product_id, nom_produit, categorie, prix_catalogue, prix_achat)
SELECT DISTINCT
    c.product_id,
    c.nom_produit,
    c.categorie,
    c.prix_catalogue,
    e.prix_achat
FROM ecommerce_silver.products_clean c
LEFT JOIN ecommerce_silver.products_clean e
    ON c.product_id = e.product_id AND e.source_system = 'ERP'
WHERE c.source_system = 'CRM';

-- dim_date
INSERT INTO dim_date
SELECT DISTINCT
    DATE_FORMAT(date_commande, '%Y%m%d'),
    date_commande,
    YEAR(date_commande),
    MONTH(date_commande),
    DAY(date_commande),
    QUARTER(date_commande)
FROM ecommerce_silver.orders_clean
WHERE date_commande IS NOT NULL;

-- fact_ventes : on joint ERP orders avec dim_client via correspondance numérique
-- erp_client_id = 'C001' correspond à crm_client_id = '1'
INSERT INTO fact_ventes (client_key, produit_key, date_key, quantite, prix_unitaire, revenue, cost, profit, statut)
SELECT
    dc.client_key,
    dp.produit_key,
    DATE_FORMAT(o.date_commande, '%Y%m%d'),
    o.quantite,
    o.prix_unitaire,
    o.quantite * o.prix_unitaire,
    o.quantite * dp.prix_achat,
    (o.quantite * o.prix_unitaire) - (o.quantite * dp.prix_achat),
    o.statut
FROM ecommerce_silver.orders_clean o
JOIN dim_produit dp ON dp.product_id = o.product_id
JOIN dim_client dc ON dc.customer_id = CAST(CONVERT(SUBSTRING(o.client_id, 2), UNSIGNED) AS CHAR)
WHERE o.source_system = 'ERP'
  AND o.prix_unitaire IS NOT NULL;