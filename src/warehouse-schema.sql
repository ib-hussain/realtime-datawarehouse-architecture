-- IMPLEMENT IN POSTGRESQL
CREATE DATABASE IF NOT EXISTS REALTIME_DATA_STREAM;
-- SCHEMA OLTP_ONLINE     -- (data arrives here from the online stores)
-- static loaded from online_sales_stream.csv
-- the dynamic one just writes to the database
CREATE TABLE IF NOT EXISTS online_sales_stream(
    transaction_id BIGINT PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    customer_id BIGINT NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    quantity BIGINT NOT NULL,
    unit_price DECIMAL(8, 3) NOT NULL DEFAULT 0,
    sales_mode BOOLEAN NOT NULL DEFAULT TRUE -- true=online, false=physical
);
-- SCHEMA OLTP_PHYSICAL   -- (data from physical stores. It's loaded here before anything)
CREATE TABLE IF NOT EXISTS periodic_sales_data(
    transaction_id SERIAL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    customer_id BIGINT NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    quantity BIGINT NOT NULL,
    unit_price DECIMAL(8, 3) NOT NULL DEFAULT 0,
    sales_mode BOOLEAN NOT NULL DEFAULT FALSE -- true=online, false=physical
    release_hour INT NOT NULL,
    released BOOLEAN NOT NULL DEFAULT FALSE
);
-- SCHEMA WAITING_AREA    -- (data is temporarily stored here before processing)
CREATE TABLE IF NOT EXISTS shed_table_online(
    transaction_id BIGINT PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    customer_id BIGINT NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    quantity BIGINT NOT NULL,
    unit_price DECIMAL(8, 3) NOT NULL DEFAULT 0,
    sales_mode BOOLEAN NOT NULL DEFAULT TRUE -- true=online, false=physical
);
CREATE TABLE IF NOT EXISTS shed_table_physical(
    transaction_id SERIAL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    customer_id BIGINT NOT NULL,
    product_id VARCHAR(255) NOT NULL,
    quantity BIGINT NOT NULL,
    unit_price DECIMAL(8, 3) NOT NULL DEFAULT 0,
    sales_mode BOOLEAN NOT NULL DEFAULT FALSE -- true=online, false=physical
    release_hour INT NOT NULL,
    released BOOLEAN NOT NULL DEFAULT TRUE
);
-- SCHEMA STAGING_AREA
enumerated gender('Male', 'Female');
enumerated age_group( 0–17, 18–25, 26–35, 36–45, 46–50, 51–55,55+);
enumerated occupation(Student, Engineer, Teacher, Executive, Clerk, Healthcare, Retail, Homemaker, Artist, Other);
enumerated city_category(A, B, C);
enumerated stay_in_current_city_years( 0, 1, 2, 3, 4+);
enumerated marital_status(Single, Married);
CREATE TABLE IF NOT EXISTS customer_master(
    customer_id BIGINT NOT NULL, 
    gender, 
    age_group, 
    occupation, 
    city_category,
    stay_in_current_city_years, 
    marital_status
);
CREATE TABLE IF NOT EXISTS store_list(
    store_id INT NOT NULL PRIMARY KEY, 
    store_name, 
    city
);
-- Enumeration for stores table(it's all enumerated):
-- store_id,    store_name          city
-- • 1,         Online,             Web
-- • 2,         London Bridge,      London
-- • 3,         Manchester Arndale, Manchester
-- • 4,         Birmingham Bullring,Birmingham
-- • 5,         Edinburgh Princes,  Edinburgh
enumerated supplier_name(Northwind Supplies,Atlantic Gifts Ltd,Midlands Wholesale,Celtic Craft Co,EuroPack Partners);
enumerated supplier_id(SUP01,SUP02,SUP03,SUP04,SUP05);
enumerated product_category(Home Decor, Kitchen, Stationery,Toys, Seasonal Gifts, Bags, Jewelry, Garden, Party, Other);
CREATE TABLE IF NOT EXISTS product_master(
    product_id VARCHAR(255) NOT NULL, 
    product_name VARCHAR(255) NOT NULL DEFAULT 'UNKNOWN', 
    product_category, 
    supplier_id, 
    supplier_name
);
-- SCHEMA DATA_WAREHOUSE

-- fact table:
-- transaction_id
-- customer_id
-- event_time
-- sale            (aggregated maybe?)
-- store_id        (aggregated maybe?)

-- CENTRAL TABLE (this links all of them)
-- CREATE TABLE IF NOT EXISTS central(
--     SERIAL_NUMBER BIGINT PRIMARY KEY,
--     product_id VARCHAR(255) NOT NULL, 
--     customer_id VARCHAR(255) NOT NULL DEFAULT 'UNKNOWN', 
--     store_id,
--     sale,
--     event_time
-- );

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
    -- link with table 'store_list'
)
CREATE TABLE IF NOT EXISTS store_list(
    store_id INT NOT NULL PRIMARY KEY, 
    store_name, 
    city
);
CREATE TABLE IF NOT EXISTS products(
    product_id VARCHAR(255) NOT NULL, 
    product_name VARCHAR(255) NOT NULL DEFAULT 'UNKNOWN', 
    product_category, 
    supplier_id, 
    supplier_name
);
CREATE TABLE IF NOT EXISTS products_store(
    SERIAL_NUMBER BIGINT PRIMARY KEY,
    product_id VARCHAR(255) NOT NULL, 
    transaction_id VARCHAR(255) NOT NULL DEFAULT 'UNKNOWN', 
    store_id,
    sale,
    event_time
);
CREATE TABLE IF NOT EXISTS customers(
    customer_id BIGINT NOT NULL, 
    gender, 
    age_group, 
    occupation, 
    city_category,
    stay_in_current_city_years, 
    marital_status
);
