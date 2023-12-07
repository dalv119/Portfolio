
DELETE FROM mart.f_customer_retention
WHERE period_id= DATE_PART ('week', '{{ds}}')
;

INSERT INTO mart.f_customer_retention (
	new_customers_count,
	returning_customers_count,
	refunded_customer_count,
	period_name,
	period_id,
	item_id,
	new_customers_revenue,
	returning_customers_revenue,
	customers_refunded)
WITH gr_cust AS (
	SELECT 
		DATE_PART ('week', date_time) as period_id,
		customer_id,
		COUNT (customer_id) AS cust_count,
		item_id,
		sum(payment_amount) as payment_amount,
		status
	FROM staging.user_order_log AS uol
	GROUP BY customer_id, city_id, item_id, DATE_PART ('week', date_time), status
)
SELECT 
	SUM(CASE WHEN cust_count = 1 AND status <> 'refunded' THEN 1 ELSE 0 END) AS new_customers_count,
	SUM(CASE WHEN cust_count > 1 AND status <> 'refunded' THEN 1 ELSE 0 END) AS returning_customers_count ,
	SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END)AS refunded_customer_count, 
	'weekly' AS period_name,
	period_id,
	item_id,
	SUM(CASE WHEN cust_count = 1 AND status <> 'refunded' THEN payment_amount END) AS new_customers_revenue ,
	SUM(CASE WHEN cust_count > 1 AND status <> 'refunded' THEN payment_amount END) AS returning_customers_revenue ,
	SUM ( CASE WHEN status = 'refunded' THEN 1 END ) AS customers_refunded 
FROM gr_cust AS gc
GROUP BY period_id,	item_id
HAVING period_id = DATE_PART ('week', '{{ds}}')
;