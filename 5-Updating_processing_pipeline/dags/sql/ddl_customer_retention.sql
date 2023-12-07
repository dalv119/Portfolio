CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
	new_customers_count integer,
	returning_customers_count integer,
	refunded_customer_count integer,
	period_name varchar(10),
	period_id integer,
	item_id integer,
	new_customers_revenue numeric(10, 2),
	returning_customers_revenue numeric(10, 2),
	customers_refunded integer,
	PRIMARY KEY (period_id, item_id)
);