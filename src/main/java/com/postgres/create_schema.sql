DROP SCHEMA IF EXISTS e_commerce CASCADE;
DROP SCHEMA IF EXISTS result CASCADE;

CREATE SCHEMA e_commerce;
CREATE SCHEMA result;

CREATE TABLE e_commerce.users (
    User_ID VARCHAR(255) PRIMARY KEY,
    Name VARCHAR(255),
    Email VARCHAR(255),
    Gender VARCHAR(2),
    Birth_day DATE,
    Location VARCHAR(255),
    Phone VARCHAR(255),
    Registration_date Date 
);

CREATE TABLE e_commerce.products (
    Product_ID VARCHAR(355) PRIMARY KEY,
    Product_name VARCHAR(355),
    Category VARCHAR(355),
    Brand VARCHAR(355),
    Price DECIMAL,
    Commission_rate DECIMAL
);

CREATE TABLE result.checkouts (
    product_id VARCHAR(255),
    user_id VARCHAR(255),
    purchase_id VARCHAR(255) PRIMARY KEY,
    total_cost DOUBLE PRECISION,
    payment_method VARCHAR(255),
    quantity DOUBLE PRECISION,
    payment_status VARCHAR(255),
    description VARCHAR(255),
    purchase_time TIMESTAMP,
    name VARCHAR(255),
    email VARCHAR(255),
    gender VARCHAR(255),
    birth_day DATE,
    location VARCHAR(255),
    phone VARCHAR(255),
    registration_date DATE,
    product_name VARCHAR(255),
    category VARCHAR(255),
    brand VARCHAR(255),
    profit DOUBLE PRECISION,
    click_id VARCHAR(255),
    click_time TIMESTAMP,
    click_at VARCHAR(255),
    source VARCHAR(255),
    ip VARCHAR(255)
);


CREATE TABLE result.source_count (
    Source VARCHAR(255),
    Count INT,
    time TIMESTAMP,
    PRIMARY KEY(time, Source)
);

CREATE TABLE result.fail_count (
    Description VARCHAR(255),
    Count INT,
    time TIMESTAMP,
    PRIMARY KEY(time, Description)
);

CREATE TABLE result.category_count (
    Category VARCHAR(255),
    Count INT,
    time TIMESTAMP,
    PRIMARY KEY(time, Category)
);

CREATE TABLE result.gender_count (
    Gender VARCHAR(255),
    Count INT,
    time TIMESTAMP,
    PRIMARY KEY(time, Gender)
);

CREATE TABLE result.revenue (
    Revenue DECIMAL,
    Profit DECIMAL,
    Quantity INT,
    time TIMESTAMP,
    PRIMARY KEY(time)
);

CREATE ROLE grafana WITH LOGIN PASSWORD 'password';

GRANT USAGE ON SCHEMA result TO grafana;

GRANT SELECT ON TABLE result.source_count TO grafana;
GRANT SELECT ON TABLE result.fail_count TO grafana;
GRANT SELECT ON TABLE result.category_count TO grafana;
GRANT SELECT ON TABLE result.revenue TO grafana;
GRANT SELECT ON TABLE result.gender_count TO grafana;

ALTER ROLE grafana SET search_path = 'result';