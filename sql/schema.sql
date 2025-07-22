CREATE DATABASE STAGELOAD;
USE STAGELOAD;

CREATE TABLE orders (
  order_id VARCHAR(64) primary key,
  user_name VARCHAR(64),
  order_status VARCHAR(64),
  order_date VARCHAR(64),
  order_approved_date VARCHAR(64),
  pickup_date VARCHAR(64),
  delivered_date VARCHAR(64),
  estimated_time_delivery VARCHAR(64)
);

CREATE TABLE feedback (
   feedback_id VARCHAR(64) primary key,
   order_id VARCHAR(64),
   feedback_score VARCHAR(64),
   feedback_form_sent_date VARCHAR(64),
   feedback_answer_date VARCHAR(64)
);

CREATE TABLE order_items (
order_id VARCHAR(64),
order_item_id VARCHAR(64),
product_id VARCHAR(64),
seller_id VARCHAR(64),
pickup_limit_date VARCHAR(64),
price VARCHAR(64),
shipping_cost VARCHAR(64),
primary key(order_id, order_item_id)
);

CREATE TABLE payments(
order_id VARCHAR(64),
payment_sequential VARCHAR(64),
payment_type VARCHAR(64),
payment_installments VARCHAR(64),
payment_value VARCHAR(64),
primary key(order_id, payment_sequential )
);

CREATE TABLE products(
product_id VARCHAR(64) primary key,
product_category VARCHAR(64),
product_name_length int,
product_description_length int,
product_photos_qty int,
product_weight_g int,
product_length_cm int,
product_height_cm int,
product_width_cm int
);

CREATE TABLE sellers(
seller_id VARCHAR(64) primary key,
seller_zip_code VARCHAR(20),
seller_city VARCHAR(100),
seller_state VARCHAR(100)
);

CREATE TABLE users (
    user_name VARCHAR(64) PRIMARY KEY,
    customer_zip_code VARCHAR(20),
    customer_city VARCHAR(100),
    customer_state VARCHAR(100)
);
