-- Active: 1794590548890@@127.0.0.1@5432@realtime_data_stream
-- ============================================================================
-- PROJECT: Near-Real-Time and Periodic Data Warehouse for a Retail Chain
-- DBMS: PostgreSQL
--
-- Run this file after connecting to the target database. Database creation is
-- intentionally kept outside this script because CREATE DATABASE cannot run
-- inside a transaction and does not switch the current connection.
--
-- Design decision:
--   * data_warehouse.transactions is the physical sales fact table.
--   * data_warehouse.products_store is the product-store bridge.
--   * data_warehouse.fact_sales is a compatibility view, not a second fact.
-- ============================================================================
BEGIN;
-- ============================================================================
-- 0. CLEAN REBUILD
-- Dropping data_warehouse also removes STORE_QUARTERLY_SALES if Q20 created it.
-- ============================================================================

DROP SCHEMA IF EXISTS waiting_area  CASCADE;
DROP SCHEMA IF EXISTS staging_area  CASCADE;
DROP SCHEMA IF EXISTS oltp_physical CASCADE;
DROP SCHEMA IF EXISTS oltp_online   CASCADE;
DROP SCHEMA IF EXISTS data_warehouse CASCADE;

CREATE SCHEMA oltp_online;
CREATE SCHEMA oltp_physical;
CREATE SCHEMA waiting_area;
CREATE SCHEMA staging_area;
CREATE SCHEMA data_warehouse;

-- ============================================================================
-- 1. CONTROLLED DOMAIN TYPES
-- Types live inside data_warehouse so a schema rebuild removes them safely.
-- ============================================================================

CREATE TYPE data_warehouse.gender_enum AS ENUM (
    'Male', 'Female'
);

CREATE TYPE data_warehouse.age_group_enum AS ENUM (
    '0-17', '18-25', '26-35', '36-45', '46-50', '51-55', '55+'
);

CREATE TYPE data_warehouse.occupation_enum AS ENUM (
    'Student', 'Engineer', 'Teacher', 'Executive', 'Clerk',
    'Healthcare', 'Retail', 'Homemaker', 'Artist', 'Other'
);

CREATE TYPE data_warehouse.city_category_enum AS ENUM ('A', 'B', 'C');
CREATE TYPE data_warehouse.stay_in_city_enum AS ENUM ('0', '1', '2', '3', '4+');
CREATE TYPE data_warehouse.marital_status_enum AS ENUM ('Single', 'Married');

CREATE TYPE data_warehouse.supplier_id_enum AS ENUM (
    'SUP01', 'SUP02', 'SUP03', 'SUP04', 'SUP05'
);

CREATE TYPE data_warehouse.supplier_name_enum AS ENUM (
    'Northwind Supplies',
    'Atlantic Gifts Ltd',
    'Midlands Wholesale',
    'Celtic Craft Co',
    'EuroPack Partners'
);

CREATE TYPE data_warehouse.product_category_enum AS ENUM (
    'Home Decor', 'Kitchen', 'Stationery', 'Toys', 'Seasonal Gifts',
    'Bags', 'Jewelry', 'Garden', 'Party', 'Other'
);

CREATE TYPE data_warehouse.season_enum AS ENUM (
    'Spring', 'Summer', 'Fall', 'Winter'
);

CREATE TYPE data_warehouse.half_year_enum AS ENUM ('First', 'Second');
CREATE TYPE data_warehouse.processing_mode_enum AS ENUM ('Immediate', 'Shed');

-- ============================================================================
-- 2. ONLINE OPERATIONAL INPUT
-- One row is one product line in a shopping cart. transaction_id is text
-- because UCI InvoiceNo is not a PostgreSQL serial number.
-- ============================================================================

CREATE TABLE oltp_online.online_sales_stream (
    stream_row_id  BIGSERIAL       PRIMARY KEY,
    transaction_id VARCHAR(20)     NOT NULL,
    event_time     TIMESTAMP       NOT NULL,
    customer_id    BIGINT          NOT NULL CHECK (customer_id > 0),
    product_id     VARCHAR(255)    NOT NULL,
    quantity       INTEGER         NOT NULL CHECK (quantity > 0),
    unit_price     NUMERIC(12,3)   NOT NULL CHECK (unit_price > 0),
    sales_mode     BOOLEAN         NOT NULL DEFAULT TRUE,

    CONSTRAINT online_rows_are_online CHECK (sales_mode = TRUE)
);

-- ============================================================================
-- 3. PHYSICAL-STORE PERIODIC STAGING INPUT
-- Data is sorted by event_time and assigned to exactly 48 release groups.
-- ============================================================================

CREATE TABLE oltp_physical.periodic_sales_data (
    periodic_row_id BIGSERIAL      PRIMARY KEY,
    transaction_id  VARCHAR(20)    NOT NULL,
    event_time      TIMESTAMP      NOT NULL,
    customer_id     BIGINT         NOT NULL CHECK (customer_id > 0),
    product_id      VARCHAR(255)   NOT NULL,
    quantity        INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(12,3)  NOT NULL CHECK (unit_price > 0),
    sales_mode      BOOLEAN        NOT NULL DEFAULT FALSE,
    release_hour    SMALLINT       NOT NULL CHECK (release_hour BETWEEN 0 AND 47),
    released        BOOLEAN        NOT NULL DEFAULT FALSE,

    CONSTRAINT periodic_rows_are_physical CHECK (sales_mode = FALSE)
);

-- ============================================================================
-- 4. WAITING AREA FOR LOAD SHEDDING
-- Only periodic transactions may be shed. Online transactions are never
-- written here. The original row id makes the move idempotent.
-- ============================================================================

CREATE TABLE waiting_area.shed_table_physical (
    shed_id          BIGSERIAL      PRIMARY KEY,
    periodic_row_id  BIGINT         NOT NULL UNIQUE,
    transaction_id   VARCHAR(20)    NOT NULL,
    event_time       TIMESTAMP      NOT NULL,
    customer_id      BIGINT         NOT NULL CHECK (customer_id > 0),
    product_id       VARCHAR(255)   NOT NULL,
    quantity         INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price       NUMERIC(12,3)  NOT NULL CHECK (unit_price > 0),
    sales_mode       BOOLEAN        NOT NULL DEFAULT FALSE,
    release_hour     SMALLINT       NOT NULL CHECK (release_hour BETWEEN 0 AND 47),
    shed_at          TIMESTAMPTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed        BOOLEAN        NOT NULL DEFAULT FALSE,
    processed_at     TIMESTAMPTZ,

    CONSTRAINT shed_rows_are_physical CHECK (sales_mode = FALSE),
    CONSTRAINT shed_processing_time_is_valid CHECK (
        (processed = FALSE AND processed_at IS NULL)
        OR
        (processed = TRUE AND processed_at IS NOT NULL)
    ),
    CONSTRAINT fk_shed_periodic_row
        FOREIGN KEY (periodic_row_id)
        REFERENCES oltp_physical.periodic_sales_data(periodic_row_id)
        ON DELETE CASCADE
);

-- A short alias is useful for status commands while preserving the requested
-- physical-table name used by the current project code.
CREATE VIEW waiting_area.shed_table AS
SELECT *
FROM waiting_area.shed_table_physical;

-- ============================================================================
-- 5. MASTER DATA USED BY MESHJOIN
-- These relations remain on disk and are read in pages of 500 tuples.
-- ============================================================================

CREATE TABLE staging_area.customer_master (
    customer_id                BIGINT      PRIMARY KEY CHECK (customer_id > 0),
    gender                     data_warehouse.gender_enum          NOT NULL,
    age_group                  data_warehouse.age_group_enum       NOT NULL,
    occupation                 data_warehouse.occupation_enum      NOT NULL,
    city_category              data_warehouse.city_category_enum   NOT NULL,
    stay_in_current_city_years data_warehouse.stay_in_city_enum    NOT NULL,
    marital_status             data_warehouse.marital_status_enum  NOT NULL
);

CREATE TABLE staging_area.product_master (
    product_id       VARCHAR(255) PRIMARY KEY,
    product_name     VARCHAR(255) NOT NULL DEFAULT 'UNKNOWN',
    product_category data_warehouse.product_category_enum NOT NULL,
    supplier_id      data_warehouse.supplier_id_enum      NOT NULL,
    supplier_name    data_warehouse.supplier_name_enum    NOT NULL,

    CONSTRAINT product_name_is_not_blank CHECK (BTRIM(product_name) <> '')
);

CREATE TABLE staging_area.store_list (
    store_id   SMALLINT     PRIMARY KEY CHECK (store_id BETWEEN 1 AND 5),
    store_name VARCHAR(100) NOT NULL UNIQUE,
    city       VARCHAR(100) NOT NULL
);

INSERT INTO staging_area.store_list (store_id, store_name, city) VALUES
    (1, 'Online',              'Web'),
    (2, 'London Bridge',       'London'),
    (3, 'Manchester Arndale',  'Manchester'),
    (4, 'Birmingham Bullring', 'Birmingham'),
    (5, 'Edinburgh Princes',   'Edinburgh');

-- ============================================================================
-- 6. STAR-SCHEMA DIMENSIONS
-- Original business ids are retained as the dimension keys, as permitted by
-- the brief. time_key is surrogate because event timestamps are not compact.
-- ============================================================================

CREATE TABLE data_warehouse.dim_customer (
    customer_id                BIGINT      PRIMARY KEY CHECK (customer_id > 0),
    gender                     data_warehouse.gender_enum          NOT NULL,
    age_group                  data_warehouse.age_group_enum       NOT NULL,
    occupation                 data_warehouse.occupation_enum      NOT NULL,
    city_category              data_warehouse.city_category_enum   NOT NULL,
    stay_in_current_city_years data_warehouse.stay_in_city_enum    NOT NULL,
    marital_status             data_warehouse.marital_status_enum  NOT NULL
);

CREATE TABLE data_warehouse.dim_product (
    product_id       VARCHAR(255) PRIMARY KEY,
    product_name     VARCHAR(255) NOT NULL DEFAULT 'UNKNOWN',
    product_category data_warehouse.product_category_enum NOT NULL,

    CONSTRAINT dim_product_name_is_not_blank CHECK (BTRIM(product_name) <> '')
);

CREATE TABLE data_warehouse.dim_supplier (
    supplier_id   data_warehouse.supplier_id_enum   PRIMARY KEY,
    supplier_name data_warehouse.supplier_name_enum NOT NULL UNIQUE
);

INSERT INTO data_warehouse.dim_supplier (supplier_id, supplier_name) VALUES
    ('SUP01', 'Northwind Supplies'),
    ('SUP02', 'Atlantic Gifts Ltd'),
    ('SUP03', 'Midlands Wholesale'),
    ('SUP04', 'Celtic Craft Co'),
    ('SUP05', 'EuroPack Partners');

CREATE TABLE data_warehouse.dim_store (
    store_id   SMALLINT     PRIMARY KEY CHECK (store_id BETWEEN 1 AND 5),
    store_name VARCHAR(100) NOT NULL UNIQUE,
    city       VARCHAR(100) NOT NULL
);

INSERT INTO data_warehouse.dim_store (store_id, store_name, city)
SELECT store_id, store_name, city
FROM staging_area.store_list;

CREATE TABLE data_warehouse.dim_time (
    time_key     BIGSERIAL   PRIMARY KEY,
    event_time   TIMESTAMP   NOT NULL UNIQUE,
    date         DATE        NOT NULL,
    hour         SMALLINT    NOT NULL CHECK (hour BETWEEN 0 AND 23),
    day_of_month SMALLINT    NOT NULL CHECK (day_of_month BETWEEN 1 AND 31),
    month        SMALLINT    NOT NULL CHECK (month BETWEEN 1 AND 12),
    quarter      SMALLINT    NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    year         SMALLINT    NOT NULL,
    day_of_week  VARCHAR(9)  NOT NULL CHECK (
        day_of_week IN (
            'Monday', 'Tuesday', 'Wednesday', 'Thursday',
            'Friday', 'Saturday', 'Sunday'
        )
    ),
    is_weekend   BOOLEAN     NOT NULL,
    season       data_warehouse.season_enum    NOT NULL,
    half_year    data_warehouse.half_year_enum NOT NULL,

    CONSTRAINT uq_dim_time_key_event UNIQUE (time_key, event_time),
    CONSTRAINT dim_time_date_parts_match CHECK (
        date = event_time::DATE
        AND hour = EXTRACT(HOUR FROM event_time)::SMALLINT
        AND day_of_month = EXTRACT(DAY FROM event_time)::SMALLINT
        AND month = EXTRACT(MONTH FROM event_time)::SMALLINT
        AND quarter = EXTRACT(QUARTER FROM event_time)::SMALLINT
        AND year = EXTRACT(YEAR FROM event_time)::SMALLINT
    ),
    CONSTRAINT dim_time_weekend_matches_day CHECK (
        is_weekend = (EXTRACT(ISODOW FROM event_time) IN (6, 7))
    ),
    CONSTRAINT dim_time_half_year_matches_month CHECK (
        (month BETWEEN 1 AND 6 AND half_year = 'First')
        OR
        (month BETWEEN 7 AND 12 AND half_year = 'Second')
    ),
    CONSTRAINT dim_time_season_matches_month CHECK (
        (month IN (3, 4, 5) AND season = 'Spring')
        OR
        (month IN (6, 7, 8) AND season = 'Summer')
        OR
        (month IN (9, 10, 11) AND season = 'Fall')
        OR
        (month IN (12, 1, 2) AND season = 'Winter')
    )
);

-- ============================================================================
-- 7. PRODUCT-STORE BRIDGE (REQUIRED PROJECT ARCHITECTURE)
-- One row represents one valid product/store combination. The composite unique
-- constraint lets transactions enforce that its product and store agree with
-- the selected bridge row.
-- ============================================================================

CREATE TABLE data_warehouse.products_store (
    product_store_key BIGSERIAL    PRIMARY KEY,
    product_id        VARCHAR(255) NOT NULL,
    store_id          SMALLINT     NOT NULL,

    CONSTRAINT uq_products_store_pair UNIQUE (product_id, store_id),
    CONSTRAINT uq_products_store_full UNIQUE (
        product_store_key, product_id, store_id
    ),
    CONSTRAINT fk_products_store_product
        FOREIGN KEY (product_id)
        REFERENCES data_warehouse.dim_product(product_id),
    CONSTRAINT fk_products_store_store
        FOREIGN KEY (store_id)
        REFERENCES data_warehouse.dim_store(store_id)
);

-- ============================================================================
-- 8. SALES FACT: data_warehouse.transactions
-- Grain: one source transaction line (one product in one shopping cart).
-- Revenue is stored, not calculated by OLAP queries.
-- ============================================================================

CREATE TABLE data_warehouse.transactions (
    transaction_line_id BIGSERIAL      PRIMARY KEY,
    transaction_id      VARCHAR(20)    NOT NULL,
    customer_id         BIGINT         NOT NULL,
    product_id          VARCHAR(255)   NOT NULL,
    supplier_id         data_warehouse.supplier_id_enum NOT NULL,
    store_id            SMALLINT       NOT NULL,
    time_key            BIGINT         NOT NULL,
    product_store_key   BIGINT         NOT NULL,
    event_time          TIMESTAMP      NOT NULL,
    quantity            INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price          NUMERIC(12,3)  NOT NULL CHECK (unit_price > 0),
    revenue             NUMERIC(18,3)  NOT NULL CHECK (revenue > 0),
    sales_mode          BOOLEAN        NOT NULL,
    processing_mode     data_warehouse.processing_mode_enum
                                       NOT NULL DEFAULT 'Immediate',
    loaded_at           TIMESTAMPTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT transaction_revenue_matches CHECK (
        revenue = quantity::NUMERIC * unit_price
    ),
    CONSTRAINT transaction_store_matches_sales_mode CHECK (
        (sales_mode = TRUE  AND store_id = 1)
        OR
        (sales_mode = FALSE AND store_id BETWEEN 2 AND 5)
    ),
    CONSTRAINT only_periodic_transactions_can_be_shed CHECK (
        processing_mode <> 'Shed' OR sales_mode = FALSE
    ),
    CONSTRAINT fk_transactions_customer
        FOREIGN KEY (customer_id)
        REFERENCES data_warehouse.dim_customer(customer_id),
    CONSTRAINT fk_transactions_product
        FOREIGN KEY (product_id)
        REFERENCES data_warehouse.dim_product(product_id),
    CONSTRAINT fk_transactions_supplier
        FOREIGN KEY (supplier_id)
        REFERENCES data_warehouse.dim_supplier(supplier_id),
    CONSTRAINT fk_transactions_store
        FOREIGN KEY (store_id)
        REFERENCES data_warehouse.dim_store(store_id),
    CONSTRAINT fk_transactions_time
        FOREIGN KEY (time_key, event_time)
        REFERENCES data_warehouse.dim_time(time_key, event_time),
    CONSTRAINT fk_transactions_product_store
        FOREIGN KEY (product_store_key, product_id, store_id)
        REFERENCES data_warehouse.products_store(
            product_store_key, product_id, store_id
        )
);

-- Compatibility name for code or OLAP scripts written against fact_sales.
-- This is a view over transactions, so there is still exactly one fact table.
CREATE VIEW data_warehouse.fact_sales AS
SELECT
    transaction_line_id AS fact_id,
    transaction_id,
    customer_id,
    product_id,
    supplier_id,
    store_id,
    time_key,
    product_store_key,
    event_time,
    quantity,
    unit_price,
    revenue,
    sales_mode,
    processing_mode,
    loaded_at
FROM data_warehouse.transactions;

-- ============================================================================
-- 9. INDEXES FOR ETL, STATUS COMMANDS, AND OLAP ACCESS PATHS
-- Primary keys and UNIQUE constraints already create their own indexes.
-- ============================================================================

CREATE INDEX idx_online_stream_event_time
    ON oltp_online.online_sales_stream (event_time, stream_row_id);

CREATE INDEX idx_periodic_release_queue
    ON oltp_physical.periodic_sales_data (released, release_hour, event_time);

CREATE INDEX idx_shed_unprocessed
    ON waiting_area.shed_table_physical (processed, shed_at)
    WHERE processed = FALSE;

CREATE INDEX idx_customer_master_meshjoin
    ON staging_area.customer_master (customer_id);

CREATE INDEX idx_product_master_meshjoin
    ON staging_area.product_master (product_id);

CREATE INDEX idx_transactions_customer
    ON data_warehouse.transactions (customer_id);

CREATE INDEX idx_transactions_product
    ON data_warehouse.transactions (product_id);

CREATE INDEX idx_transactions_supplier
    ON data_warehouse.transactions (supplier_id);

CREATE INDEX idx_transactions_store_time
    ON data_warehouse.transactions (store_id, time_key);

CREATE INDEX idx_transactions_cart
    ON data_warehouse.transactions (transaction_id, product_id);

CREATE INDEX idx_transactions_time_product
    ON data_warehouse.transactions (time_key, product_id);

CREATE INDEX idx_dim_time_analysis
    ON data_warehouse.dim_time (year, quarter, month, date);

COMMIT;

