ALTER table feedback
  add column md5_hash VARCHAR(64),
  add column dv_load_timestamp datetime;

ALTER TABLE orders
  ADD COLUMN md5_hash VARCHAR(64),
  ADD COLUMN dv_load_timestamp datetime;

ALTER TABLE order_items
  ADD COLUMN md5_hash VARCHAR(64),
  ADD COLUMN dv_load_timestamp datetime;
  
ALTER TABLE payments
  ADD COLUMN md5_hash VARCHAR(64),
  ADD COLUMN dv_load_timestamp datetime;

ALTER TABLE products
  ADD COLUMN md5_hash VARCHAR(64),
  ADD COLUMN dv_load_timestamp datetime;
  
ALTER TABLE sellers
  ADD COLUMN md5_hash VARCHAR(64),
  ADD COLUMN dv_load_timestamp datetime;

ALTER TABLE users
  ADD COLUMN md5_hash VARCHAR(64),
  ADD COLUMN dv_load_timestamp datetime;

create table order_duplicate_archive LIKE orders;
create table feedback_duplicate_archive LIKE feedback;
create table order_item_duplicate_archive LIKE order_items;
create table payment_duplicate_archive LIKE payments;
create table products_duplicate_archive LIKE products;
create table seller_duplicate_archive LIKE sellers;
create table user_duplicate_archive LIKE users;

create table final_feedback LIKE feedback;
create table final_orders LIKE orders;
create table final_order_items LIKE order_items;
create table final_payments LIKE payments;
create table final_products LIKE products;
create table final_sellers LIKE sellers;
create table final_users LIKE users;

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

