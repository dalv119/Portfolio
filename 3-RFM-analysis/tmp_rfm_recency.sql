--Recency (пер. «давность») — сколько времени прошло с момента последнего заказа.
--Фактор Recency измеряется по последнему заказу. Распределите клиентов по шкале от одного до пяти, 
--где значение 1 получат те, кто либо вообще не делал заказов, либо делал их очень давно, 
--а 5 — те, кто заказывал относительно недавно.

DELETE FROM analysis.tmp_rfm_recency ;

INSERT INTO analysis.tmp_rfm_recency (
	user_id, 
	recency)
WITH max_dt as ( -- максимальная дата заказа по клиенту
		SELECT 
			user_id,
			max(order_ts) as max_ord_dt
		FROM analysis.orders o
		LEFT JOIN analysis.orderstatuses AS os
			ON o.status = os.id 
		WHERE os.key = 'Closed'
			AND EXTRACT(YEAR FROM order_ts) >= 2022 
		group by user_id
)	
SELECT --сегментация клиентов по времени последнего заказа
	u.id AS user_id,
	NTILE(5) OVER (ORDER BY coalesce(max_ord_dt, to_date('1900-01-01', 'YYYY-MM-DD'))) as recency
FROM analysis.users u 
LEFT JOIN max_dt
ON u.id = max_dt.user_id
;
