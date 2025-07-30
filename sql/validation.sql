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

--- Validating the archieved data count
select count(*) from feedback_duplicate_archive;
select count(*) from order_duplicate_archive;
