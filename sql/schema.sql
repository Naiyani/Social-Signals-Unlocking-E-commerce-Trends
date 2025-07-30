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

ALTER table feedback
  add column md5_hash VARCHAR(64),
  add column dv_load_timestamp datetime;

ALTER TABLE orders
  ADD COLUMN md5_hash VARCHAR(64),
  ADD COLUMN dv_load_timestamp datetime;

create table order_duplicate_archive LIKE orders;
create table feedback_duplicate_archive LIKE feedback;

create table final_feedback LIKE feedback;

insert into final_feedback
select * from feedback where md5_hash NOT IN (
    select md5_hash from final_feedback
);

insert into feedback_duplicate_archive
select * from feedback where md5_hash IN (
    select md5_hash from final_feedback
)
AND feedback_id NOT IN ( 
    select feedback_id from final_feedback
);

select * from orders limit 10;
select count(*) from orders;

select order_id, md5_hash, dv_load_timestamp from orders LIMIT 5;

select order_id, count(*) from orders 
GROUP BY order_id 
HAVING count(*) > 1;

select * from feedback LIMIT 10;
select count(*) from feedback;

select feedback_id, md5_hash, dv_load_timestamp from feedback LIMIT 5;

select feedback_id, count(*) from feedback
GROUP BY feedback_id 
HAVING count(*) > 1;

