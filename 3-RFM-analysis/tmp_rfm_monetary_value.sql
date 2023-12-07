--Monetary Value (пер. «денежная ценность») — сумма затрат клиента.
--Фактор Monetary Value оценивается по потраченной сумме. Распределите клиентов по шкале от одного до пяти, 
--где значение 1 получат клиенты с наименьшей суммой, а 5 — с наибольшей.

DELETE FROM analysis.tmp_rfm_monetary_value;

INSERT INTO analysis.tmp_rfm_monetary_value (
	user_id, 
	monetary_value)
WITH rn AS ( 
	SELECT 
		user_id ,
		SUM(payment) AS order_sum,
		ROW_NUMBER() OVER(ORDER BY SUM(payment) DESC) AS rn_monetary_value
	FROM analysis.orders as o
	LEFT JOIN analysis.orderstatuses as os
		ON o.status = os.id 
	WHERE os.key = 'Closed'
	GROUP BY user_id
)
SELECT 
	u.id AS user_id,
	NTILE(5) OVER (ORDER BY rn_monetary_value DESC) as monetary_value
FROM analysis.users u 
LEFT JOIN rn
	ON u.id = rn.user_id
;

