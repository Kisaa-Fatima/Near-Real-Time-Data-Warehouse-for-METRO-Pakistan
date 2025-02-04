-- Drop existing tables if they exist to avoid conflicts
DROP SCHEMA IF EXISTS proj;
DROP TABLE IF EXISTS dw_transaction;
DROP TABLE IF EXISTS transaction_fact;
DROP TABLE IF EXISTS customer_dim;
DROP TABLE IF EXISTS product_data;

-- Create Schema/database
CREATE SCHEMA proj;
USE proj;

-- Create Customer Dimension Table
CREATE TABLE customer_dim (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    gender VARCHAR(10)
);

-- Create the Product Data Table
CREATE TABLE product_data (
    product_id INT PRIMARY KEY,              -- Unique identifier for the product
    product_name VARCHAR(255),               -- Name of the product
    product_price DECIMAL(10, 2),            -- Price of the product
    supplier_id INT,                         -- Supplier identifier
    supplier_name VARCHAR(255),              -- Name of the supplier
    store_id INT,                            -- Store identifier
    store_name VARCHAR(255)                 -- Name of the store
);

-- Create the Transactions Fact Table
CREATE TABLE transactions_fact (
    transaction_id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT NOT NULL,
    order_date DATETIME NOT NULL,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    quantity_ordered INT NOT NULL,
    time_id INT NOT NULL,
    FOREIGN KEY (product_id) REFERENCES product_data(product_id),  -- Foreign key to product_data
    FOREIGN KEY (customer_id) REFERENCES customer_dim(customer_id)  -- Foreign key to customer_dim
);

-- Create DW Transaction Table for data loading after meshjoin
CREATE TABLE dw_transaction (  -- for loading data after meshjoin
    order_id INT PRIMARY KEY,
    order_date DATETIME NOT NULL,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    customer_name VARCHAR(100),
    gender VARCHAR(10),
    quantity_ordered INT NOT NULL,
    product_name VARCHAR(100),
    product_price DECIMAL(10, 2),
    supplier_id INT,
    supplier_name VARCHAR(100),
    store_id INT,
    store_name VARCHAR(100),
    sale DECIMAL(15, 2), -- Regular column for TOTAL_SALE
    FOREIGN KEY (product_id) REFERENCES product_data(product_id),  -- Foreign key to product_data
    FOREIGN KEY (customer_id) REFERENCES customer_dim(customer_id)  -- Foreign key to customer_dim
);

-- -------------------- DATA VALIDATION ---------------------------

SELECT * FROM product_data;
SELECT * FROM customer_dim;
SELECT * FROM transactions_fact;
SELECT * FROM dw_transaction;

SELECT COUNT(*) FROM dw_transaction;
SELECT COUNT(*) FROM product_data;
SELECT COUNT(*) FROM customer_dim;
SELECT COUNT(*) FROM transactions_fact;

-- checking if there are any duplicates
SELECT order_id, COUNT(*) 
FROM dw_transaction 
GROUP BY order_id 
HAVING COUNT(*) > 1;

-- ensuring no null values ----------
SELECT * FROM dw_transaction WHERE customer_name IS NULL OR product_name IS NULL OR sale IS NULL;

-- ensuring similar year values ----------
SELECT DISTINCT YEAR(order_date) FROM dw_transaction;
SELECT DISTINCT YEAR(order_date) FROM transactions_fact;
