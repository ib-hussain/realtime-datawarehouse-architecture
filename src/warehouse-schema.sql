-- =============================================================
-- PROJECT: Near-Real-Time & Periodic DW for Retail Chain
-- PostgreSQL Schema 
-- =============================================================
  CREATE DATABASE realtime_data_stream;
-- =============================================================
-- 0. SCHEMAS
-- =============================================================
CREATE SCHEMA IF NOT EXISTS oltp_online;
CREATE SCHEMA IF NOT EXISTS oltp_physical;
CREATE SCHEMA IF NOT EXISTS waiting_area;
CREATE SCHEMA IF NOT EXISTS staging_area;
CREATE SCHEMA IF NOT EXISTS data_warehouse;
DROP VIEW IF EXISTS data_warehouse.store_quarterly_sales CASCADE;
-- =============================================================
-- 1. ENUMERATED TYPES (PostgreSQL)
-- =============================================================
DROP TYPE IF EXISTS gender_enum              CASCADE;
DROP TYPE IF EXISTS age_group_enum           CASCADE;
DROP TYPE IF EXISTS occupation_enum          CASCADE;
DROP TYPE IF EXISTS city_category_enum       CASCADE;
DROP TYPE IF EXISTS stay_in_city_enum        CASCADE;
DROP TYPE IF EXISTS marital_status_enum      CASCADE;
DROP TYPE IF EXISTS supplier_id_enum         CASCADE;
DROP TYPE IF EXISTS supplier_name_enum       CASCADE;
DROP TYPE IF EXISTS product_category_enum    CASCADE;
DROP TYPE IF EXISTS season_enum              CASCADE;
DROP TYPE IF EXISTS half_year_enum           CASCADE;
DROP TYPE IF EXISTS processing_mode_enum     CASCADE;

CREATE TYPE gender_enum           AS ENUM ('Male', 'Female');
CREATE TYPE age_group_enum        AS ENUM ('0–17', '18–25', '26–35', '36–45',
                                           '46–50', '51–55', '55+');
CREATE TYPE occupation_enum       AS ENUM ('Student', 'Engineer', 'Teacher',
                                           'Executive', 'Clerk', 'Healthcare',
                                           'Retail', 'Homemaker', 'Artist', 'Other');
CREATE TYPE city_category_enum    AS ENUM ('A', 'B', 'C');
CREATE TYPE stay_in_city_enum     AS ENUM ('0', '1', '2', '3', '4+');
CREATE TYPE marital_status_enum   AS ENUM ('Single', 'Married');
CREATE TYPE supplier_id_enum      AS ENUM ('SUP01', 'SUP02', 'SUP03',
                                           'SUP04', 'SUP05');
CREATE TYPE supplier_name_enum    AS ENUM ('Northwind Supplies',
                                           'Atlantic Gifts Ltd',
                                           'Midlands Wholesale',
                                           'Celtic Craft Co',
                                           'EuroPack Partners');
CREATE TYPE product_category_enum AS ENUM ('Home Decor', 'Kitchen', 'Stationery',
                                           'Toys', 'Seasonal Gifts', 'Bags',
                                           'Jewelry', 'Garden', 'Party', 'Other');
CREATE TYPE season_enum           AS ENUM ('Spring', 'Summer', 'Fall', 'Winter');
CREATE TYPE half_year_enum        AS ENUM ('First', 'Second');
CREATE TYPE processing_mode_enum  AS ENUM ('Immediate', 'Shed');
-- =============================================================
-- 2. SCHEMA OLTP_ONLINE
-- =============================================================
CREATE TABLE IF NOT EXISTS oltp_online.online_sales_stream (
    transaction_id BIGINT       NOT NULL,
    event_time     TIMESTAMP    NOT NULL,
    customer_id    BIGINT       NOT NULL CHECK (customer_id > 0),
    product_id     VARCHAR(255) NOT NULL,
    quantity       BIGINT       NOT NULL CHECK (quantity > 0),
    unit_price     DECIMAL(8,3) NOT NULL DEFAULT 0 CHECK (unit_price > 0),
    sales_mode     BOOLEAN      NOT NULL DEFAULT TRUE,
    PRIMARY KEY (transaction_id, product_id)   -- NOTE: see flag #11
);
-- =============================================================
-- 3. SCHEMA OLTP_PHYSICAL
-- =============================================================
CREATE TABLE IF NOT EXISTS oltp_physical.periodic_sales_data (
    transaction_id  BIGSERIAL    PRIMARY KEY,
    event_time      TIMESTAMP    NOT NULL,
    customer_id     BIGINT       NOT NULL CHECK (customer_id > 0),
    product_id      VARCHAR(255) NOT NULL,
    quantity        BIGINT       NOT NULL CHECK (quantity > 0),
    unit_price      DECIMAL(8,3) NOT NULL DEFAULT 0 CHECK (unit_price > 0),
    sales_mode      BOOLEAN      NOT NULL DEFAULT FALSE,
    release_hour    INT          NOT NULL CHECK (release_hour BETWEEN 0 AND 49),
    released        BOOLEAN      NOT NULL DEFAULT FALSE
);
-- =============================================================
-- 4. SCHEMA WAITING_AREA (shed tables)
-- =============================================================
CREATE TABLE IF NOT EXISTS waiting_area.shed_table_online (
    transaction_id BIGINT       NOT NULL,
    event_time     TIMESTAMP    NOT NULL,
    customer_id    BIGINT       NOT NULL CHECK (customer_id > 0),
    product_id     VARCHAR(255) NOT NULL,
    quantity       BIGINT       NOT NULL CHECK (quantity > 0),
    unit_price     DECIMAL(8,3) NOT NULL DEFAULT 0 CHECK (unit_price > 0),
    sales_mode     BOOLEAN      NOT NULL DEFAULT TRUE,
    PRIMARY KEY (transaction_id, product_id)
);
CREATE TABLE IF NOT EXISTS waiting_area.shed_table_physical (
    transaction_id  BIGSERIAL    PRIMARY KEY,
    event_time      TIMESTAMP    NOT NULL,
    customer_id     BIGINT       NOT NULL CHECK (customer_id > 0),
    product_id      VARCHAR(255) NOT NULL,
    quantity        BIGINT       NOT NULL CHECK (quantity > 0),
    unit_price      DECIMAL(8,3) NOT NULL DEFAULT 0 CHECK (unit_price > 0),
    sales_mode      BOOLEAN      NOT NULL DEFAULT FALSE,
    release_hour    INT          NOT NULL CHECK (release_hour BETWEEN 0 AND 49),
    released        BOOLEAN      NOT NULL DEFAULT FALSE   -- see flag #10
);
-- =============================================================
-- 5. SCHEMA STAGING_AREA (master data)
-- =============================================================
CREATE TABLE IF NOT EXISTS staging_area.customer_master (
    customer_id                   BIGINT                NOT NULL PRIMARY KEY,
    gender                        gender_enum           NOT NULL,
    age_group                     age_group_enum        NOT NULL,
    occupation                    occupation_enum       NOT NULL,
    city_category                 city_category_enum    NOT NULL,
    stay_in_current_city_years    stay_in_city_enum     NOT NULL,
    marital_status                marital_status_enum   NOT NULL
);
CREATE TABLE IF NOT EXISTS staging_area.store_list (
    store_id   INT          PRIMARY KEY CHECK (store_id BETWEEN 1 AND 5),
    store_name VARCHAR(100) NOT NULL,
    city       VARCHAR(100) NOT NULL
);
INSERT INTO staging_area.store_list (store_id, store_name, city) VALUES
    (1, 'Online',              'Web'),
    (2, 'London Bridge',       'London'),
    (3, 'Manchester Arndale',  'Manchester'),
    (4, 'Birmingham Bullring', 'Birmingham'),
    (5, 'Edinburgh Princes',   'Edinburgh')
ON CONFLICT (store_id) DO NOTHING;
CREATE TABLE IF NOT EXISTS staging_area.product_master (
    product_id        VARCHAR(255)          NOT NULL PRIMARY KEY,
    product_name      VARCHAR(255)          NOT NULL DEFAULT 'UNKNOWN',
    product_category  product_category_enum NOT NULL,
    supplier_id       supplier_id_enum      NOT NULL,
    supplier_name     supplier_name_enum    NOT NULL
);
-- =============================================================
-- 6. SCHEMA DATA_WAREHOUSE — DIMENSIONS
-- =============================================================
CREATE TABLE IF NOT EXISTS data_warehouse.dim_customer (
    customer_id                 BIGINT                PRIMARY KEY,
    gender                      gender_enum           NOT NULL,
    age_group                   age_group_enum        NOT NULL,
    occupation                  occupation_enum       NOT NULL,
    city_category               city_category_enum    NOT NULL,
    stay_in_current_city_years  stay_in_city_enum     NOT NULL,
    marital_status              marital_status_enum   NOT NULL
);
CREATE TABLE IF NOT EXISTS data_warehouse.dim_product (
    product_id        VARCHAR(255)          PRIMARY KEY,
    product_name      VARCHAR(255)          NOT NULL DEFAULT 'UNKNOWN',
    product_category  product_category_enum NOT NULL
);
CREATE TABLE IF NOT EXISTS data_warehouse.dim_supplier (
    supplier_id    supplier_id_enum    PRIMARY KEY,
    supplier_name  supplier_name_enum  NOT NULL
);
CREATE TABLE IF NOT EXISTS data_warehouse.dim_store (
    store_id    INT          PRIMARY KEY CHECK (store_id BETWEEN 1 AND 5),
    store_name  VARCHAR(100) NOT NULL,
    city        VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS data_warehouse.dim_time (
    time_key      BIGSERIAL   PRIMARY KEY,
    event_time    TIMESTAMP   NOT NULL UNIQUE,
    date          DATE        NOT NULL,
    hour          SMALLINT    NOT NULL CHECK (hour BETWEEN 0 AND 23),
    day_of_month  SMALLINT    NOT NULL CHECK (day_of_month BETWEEN 1 AND 31),
    month         SMALLINT    NOT NULL CHECK (month BETWEEN 1 AND 12),
    quarter       SMALLINT    NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    year          SMALLINT    NOT NULL,
    day_of_week   VARCHAR(10) NOT NULL,
    is_weekend    BOOLEAN     NOT NULL,
    season        season_enum NOT NULL,
    half_year     half_year_enum NOT NULL
);
-- additional non-proper syntax tables below
CREATE TABLE IF NOT EXISTS products_store(
    SERIAL_NUMBER BIGINT PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL, 
    transaction_id VARCHAR(255) NOT NULL DEFAULT 'UNKNOWN', 
    store_id,
    sale,
    event_time
);
CREATE TABLE IF NOT EXISTS transactions(
    transaction_id SERIAL PRIMARY KEY,
    store_id,
    event_time TIMESTAMP NOT NULL,
    customer_id BIGINT NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    quantity BIGINT NOT NULL,
    unit_price DECIMAL(8, 3) NOT NULL DEFAULT 0,
    sales_mode BOOLEAN NOT NULL DEFAULT TRUE, -- true=online, false=physical
    sale DECIMAL(16, 3) NOT NULL DEFAULT 0
    -- link with table 'dim_store'
)
-- =============================================================
-- 7. SCHEMA DATA_WAREHOUSE — FACT (central) TABLE
-- =============================================================
CREATE TABLE IF NOT EXISTS data_warehouse.fact_sales (
    fact_id          BIGSERIAL         PRIMARY KEY,
    transaction_id   BIGINT            NOT NULL,
    customer_id      BIGINT            NOT NULL,   -- FK → dim_customer
    product_id       VARCHAR(255)      NOT NULL,   -- FK → dim_product
    supplier_id      supplier_id_enum  NOT NULL,   -- FK → dim_supplier
    store_id         INT               NOT NULL,   -- FK → dim_store
    time_key         BIGINT            NOT NULL,   -- FK → dim_time
    quantity         BIGINT            NOT NULL CHECK (quantity > 0),
    revenue          DECIMAL(16,3)     NOT NULL CHECK (revenue >= 0),
    sales_mode       BOOLEAN           NOT NULL,          -- TRUE = online, FALSE = periodic
    processing_mode  processing_mode_enum NOT NULL DEFAULT 'Immediate',

    FOREIGN KEY (customer_id) REFERENCES data_warehouse.dim_customer(customer_id),
    FOREIGN KEY (product_id)  REFERENCES data_warehouse.dim_product(product_id),
    FOREIGN KEY (supplier_id) REFERENCES data_warehouse.dim_supplier(supplier_id),
    FOREIGN KEY (store_id)    REFERENCES data_warehouse.dim_store(store_id),
    FOREIGN KEY (time_key)    REFERENCES data_warehouse.dim_time(time_key)
);