-- Создадим Staging в Vertica ------------------------------------
-- Создайте нужные таблицы для staging-слоя на основе скачанных файлов


TRUNCATE TABLE STV2023070314__STAGING.transactions;
DROP TABLE IF EXISTS STV2023070314__STAGING.transactions;

CREATE TABLE IF NOT EXISTS STV2023070314__STAGING.transactions (
	operation_id UUID PRIMARY KEY, -- id транзакции
	account_number_from INTEGER, -- внутренний бухгалтерский номер счёта транзакции ОТ КОГО
	account_number_to INTEGER, -- внутренний бухгалтерский номер счёта транзакции К КОМУ
	currency_code INTEGER, -- трёхзначный код валюты страны, из которой идёт транзакция
	country VARCHAR(30), -- страна-источник транзакции
	status VARCHAR(30), -- статус проведения транзакции: queued («транзакция в очереди на обработку сервисом»), in_progress («транзакция в обработке»), blocked («транзакция заблокирована сервисом»), done («транзакция выполнена успешно»), chargeback («пользователь осуществил возврат по транзакции»)
	transaction_type VARCHAR(30), -- тип транзакции во внутреннем учёте: authorisation («авторизационная транзакция, подтверждающая наличие счёта пользователя»), sbp_incoming («входящий перевод по системе быстрых платежей»), sbp_outgoing («исходящий перевод по системе быстрых платежей»), transfer_incoming («входящий перевод по счёту»), transfer_outgoing («исходящий перевод по счёту»), c2b_partner_incoming («перевод от юридического лица»), c2b_partner_outgoing («перевод юридическому лицу»)
	amount DECIMAL(19,5) NOT NULL, -- целочисленная сумма транзакции в минимальной единице валюты страны (копейка, цент, куруш)
	transaction_dt TIMESTAMP -- дата и время исполнения транзакции до миллисекунд
   )
ORDER BY operation_id
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2)
;


TRUNCATE TABLE STV2023070314__STAGING.currencies;
DROP TABLE IF EXISTS STV2023070314__STAGING.currencies;

CREATE TABLE IF NOT EXISTS STV2023070314__STAGING.currencies (
	date_update TIMESTAMP, -- дата обновления курса валют
	currency_code INTEGER NOT NULL, -- трёхзначный код валюты транзакции
	currency_code_with INTEGER NOT NULL, -- отношение другой валюты к валюте трёхзначного кода
	currency_with_div DECIMAL(19, 5) NOT NULL -- значение отношения единицы одной валюты к единице валюты транзакции
	)

	
-- Создание таблицы витрины	-------------------------------
	
-- Создайте витрину global_metrics с агрегацией по дням. 
--     Витрина должна помогать отвечать на такие вопросы:
--		какая ежедневная динамика сумм переводов в разных валютах;
--		какое среднее количество транзакций на пользователя;
--		какое количество уникальных пользователей совершают транзакции в валютах;
--		какой общий оборот компании в единой валюте
	
TRUNCATE TABLE STV2023070314__STAGING.global_metrics;	
CREATE TABLE IF NOT EXISTS STV2023070314__DWH.global_metrics (
	date_update TIMESTAMP, -- дата расчёта
	currency_from INTEGER, -- код валюты транзакции
	amount_total DECIMAL(19, 5), -- общая сумма транзакций по валюте в долларах (код валюты доллар США - USD (420))
	cnt_transactions DECIMAL(19, 5), -- общий объём транзакций по валюте
	avg_transactions_per_account DECIMAL(19, 5), -- средний объём транзакций с аккаунта
	cnt_accounts_make_transactions INTEGER -- количество уникальных аккаунтов с совершёнными транзакциями по валюте
	)

	