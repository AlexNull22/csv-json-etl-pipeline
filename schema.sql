CREATE SCHEMA IF NOT EXISTS small_etl;

CREATE TABLE IF NOT EXISTS small_etl.customers (
    email VARCHAR(254) PRIMARY KEY,
    name VARCHAR(200) NOT NULL CHECK (length(trim(name)) > 0)
);

CREATE TABLE IF NOT EXISTS small_etl.orders (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_email VARCHAR(254) NOT NULL REFERENCES small_etl.customers(email),
    order_date DATE NOT NULL,
    amount NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    currency CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    status VARCHAR(32) NOT NULL CHECK (length(trim(status)) > 0)
);

CREATE INDEX IF NOT EXISTS orders_customer_date_idx
    ON small_etl.orders(customer_email, order_date);
