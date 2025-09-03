-- PAYMENT PREFERANCES (DISTRIBUTION)
SELECT
  payment_type,
  COUNT(*) AS txn_cnt,
  ROUND(100 * COUNT(*) / (SELECT COUNT(*) FROM dwh.dim_payments), 2) AS pct
FROM dwh.dim_payments
GROUP BY payment_type
ORDER BY txn_cnt DESC;

-- ORDER TIMING: Orders by hour of day
SELECT
  HOUR(o.order_date) AS hour_of_day,
  COUNT(*)           AS orders
FROM dwh.dim_orders o
WHERE o.order_date IS NOT NULL
GROUP BY HOUR(o.order_date)
ORDER BY hour_of_day;

-- SEASONAL PATTERN (month across all years)
SELECT
  MONTH(o.order_date)             AS month_num,
  DATE_FORMAT(o.order_date,'%b')  AS month_name,
  COUNT(*)                        AS orders
FROM dwh.dim_orders o
WHERE o.order_date IS NOT NULL
GROUP BY month_num, month_name
ORDER BY month_num;

-- BUSIEST TRAFFIC ROUTES 
SELECT
  CONCAT(s.seller_state, '→', u.customer_state) AS route,
  COUNT(DISTINCT oi.order_id)                   AS orders
FROM dwh.dim_order_items oi
JOIN dwh.dim_sellers s ON s.seller_id = oi.seller_id
JOIN dwh.dim_orders  o  ON o.order_id  = oi.order_id
JOIN dwh.dim_users   u  ON u.user_name = o.user_name
GROUP BY route
ORDER BY orders DESC
LIMIT 15;

-- DELIVERY PERFORMANCE (by state)
SELECT
  u.customer_state,
  ROUND(AVG(DATEDIFF(o.delivered_date, o.order_date)), 2) AS avg_ship_days,
  ROUND(AVG(DATEDIFF(o.delivered_date, o.estimated_time_delivery)), 2) AS avg_actual_minus_est_days,
  ROUND(100 * AVG(o.delivered_date > o.estimated_time_delivery), 2) AS pct_late
FROM dwh.dim_orders o
JOIN dwh.dim_users u ON u.user_name = o.user_name
WHERE o.order_date IS NOT NULL
  AND o.delivered_date IS NOT NULL
  AND o.estimated_time_delivery IS NOT NULL
GROUP BY u.customer_state
ORDER BY avg_actual_minus_est_days DESC;

-- AVERAGE INSTALLMENTS
SELECT AVG(p.payment_installments) AS avg_installments
FROM dwh.dim_payments p
WHERE p.payment_installments IS NOT NULL;

-- CORRELATION BETWEEN DELIVERY DATES AND ESTIMATES DELIVERY TIME
SELECT
  (AVG(t.delay_days * t.score) - AVG(t.delay_days) * AVG(t.score)) /
  (NULLIF(STDDEV_SAMP(t.delay_days),0) * NULLIF(STDDEV_SAMP(t.score),0)) AS pearson_corr
FROM (
  SELECT
    TIMESTAMPDIFF(DAY, o.estimated_time_delivery, o.delivered_date) AS delay_days,
    CAST(f.feedback_score AS DECIMAL(10,2))                          AS score
  FROM dwh.dim_orders   o
  JOIN dwh.dim_feedback f ON f.order_id = o.order_id
  WHERE o.delivered_date IS NOT NULL
    AND o.estimated_time_delivery IS NOT NULL
    AND f.feedback_score IS NOT NULL
) t;
