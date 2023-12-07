--Витрина RFM сегментации клиентов
--RFM (от англ. Recency, Frequency, Monetary Value) — способ сегментации клиентов, 
--при котором анализируют их лояльность: как часто, на какие суммы и когда в последний раз 
--тот или иной клиент покупал что-то.

DELETE FROM analysis.dm_rfm_segments;

INSERT INTO analysis.dm_rfm_segments (
	user_id, 
	recency, 
	frequency, 
	monetary_value)
SELECT 
	r.user_id, 
	recency, 
	frequency, 
	monetary_value
FROM analysis.tmp_rfm_recency r
INNER JOIN analysis.tmp_rfm_frequency f 
on r.user_id = f.user_id 
INNER JOIN analysis.tmp_rfm_monetary_value mv 
on r.user_id = mv.user_id 
;

