--Frequency (пер. «частота») — количество заказов.
--Фактор Frequency оценивается по количеству заказов. Распределите клиентов по шкале от одного до пяти, 
--где значение 1 получат клиенты с наименьшим количеством заказов, а 5 — с наибольшим.

DELETE FROM analysis.tmp_rfm_frequency;

INSERT INTO analysis.tmp_rfm_frequency (
	user_id, 
	frequency)
WITH rn AS ( 
	SELECT 
		user_id,
		COUNT(order_id) AS order_count,
		ROW_NUMBER() OVER(ORDER BY COUNT(order_id) DESC) AS rn_frequency
	FROM analysis.orders o
	LEFT JOIN analysis.orderstatuses as os
		ON o.status = os.id 
	WHERE os.key = 'Closed'
	GROUP BY user_id
)
SELECT 
	u.id AS user_id,
	NTILE(5) OVER (ORDER BY rn_frequency DESC) as frequency
FROM analysis.users u 
LEFT JOIN rn
ON u.id = rn.user_id
;

