-- =============================================
-- BRONZE LAYER : données brutes
-- =============================================

CREATE DATABASE IF NOT EXISTS ecommerce_bronze;
USE ecommerce_bronze;

-- Vider les tables (Full-Load)
DROP TABLE IF EXISTS crm_clients_raw;
DROP TABLE IF EXISTS crm_produits_raw;
DROP TABLE IF EXISTS crm_details_commandes_raw;
DROP TABLE IF EXISTS erp_clients_raw;
DROP TABLE IF EXISTS erp_produits_raw;
DROP TABLE IF EXISTS erp_details_commandes_raw;

-- CRM
CREATE TABLE crm_clients_raw (
    crm_client_id INT,
    nom VARCHAR(100),
    email VARCHAR(150),
    telephone VARCHAR(20),
    ville VARCHAR(100),
    segment VARCHAR(50),
    date_inscription VARCHAR(20)
);

CREATE TABLE crm_produits_raw (
    crm_product_id INT,
    nom_produit VARCHAR(200),
    categorie VARCHAR(100),
    prix_catalogue DECIMAL(10,2)
);

CREATE TABLE crm_details_commandes_raw (
    crm_order_id INT,
    crm_client_id INT,
    crm_product_id INT,
    date_commande VARCHAR(20),
    quantite INT
);

-- ERP
CREATE TABLE erp_clients_raw (
    erp_client_id VARCHAR(20),
    nom VARCHAR(100),
    ice VARCHAR(50),
    ville VARCHAR(100),
    pays VARCHAR(50),
    date_creation VARCHAR(20)
);

CREATE TABLE erp_produits_raw (
    erp_product_id INT,
    designation VARCHAR(200),
    categorie_stock VARCHAR(100),
    prix_achat DECIMAL(10,2),
    prix_vente DECIMAL(10,2),
    stock INT
);

CREATE TABLE erp_details_commandes_raw (
    erp_order_id INT,
    erp_client_id VARCHAR(20),
    erp_product_id INT,
    date_facture VARCHAR(20),
    quantite INT,
    prix_unitaire DECIMAL(10,2),
    statut VARCHAR(50)
);

-- =============================================
-- CHARGEMENT CSV → Bronze
-- =============================================

LOAD DATA INFILE '/var/lib/mysql-files/crm_source/crm_clients.csv'
INTO TABLE crm_clients_raw
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(crm_client_id, nom, email, telephone, ville, segment, date_inscription);

LOAD DATA INFILE '/var/lib/mysql-files/crm_source/crm_produits.csv'
INTO TABLE crm_produits_raw
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(crm_product_id, nom_produit, categorie, prix_catalogue);

LOAD DATA INFILE '/var/lib/mysql-files/crm_source/crm_details_commandes.csv'
INTO TABLE crm_details_commandes_raw
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(crm_order_id, crm_client_id, crm_product_id, date_commande, quantite);

LOAD DATA INFILE '/var/lib/mysql-files/erp_source/erp_clients.csv'
INTO TABLE erp_clients_raw
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(erp_client_id, nom, ice, ville, pays, date_creation);

LOAD DATA INFILE '/var/lib/mysql-files/erp_source/erp_produits.csv'
INTO TABLE erp_produits_raw
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(erp_product_id, designation, categorie_stock, prix_achat, prix_vente, stock);

LOAD DATA INFILE '/var/lib/mysql-files/erp_source/erp_details_commandes.csv'
INTO TABLE erp_details_commandes_raw
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(erp_order_id, erp_client_id, erp_product_id, date_facture, quantite, prix_unitaire, statut);