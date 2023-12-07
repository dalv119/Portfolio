-- ПРОЕКТ СПРИНТ-2
-----------------------
-->>> 1. Создайте справочник стоимости доставки в страны shipping_country_rates из данных, 
--	  указанных в shipping_country и shipping_country_base_rate, 
--	  сделайте первичный ключ таблицы — серийный id, 
--    то есть серийный идентификатор каждой строчки. Важно дать серийному ключу имя «id». 
--    Справочник должен состоять из уникальных пар полей из таблицы shipping.

--DROP TABLE IF EXISTS public.shipping_country_rates CASCADE;
--DROP TABLE IF EXISTS public.shipping_agreement;
--DROP TABLE IF EXISTS public.shipping_transfer;
--DROP TABLE IF EXISTS public.shipping_info;
--DROP TABLE IF EXISTS public.shipping_status ;

-- Создание таблицы справочника shipping_country_rates

CREATE TABLE public.shipping_country_rates (
	id serial,
	shipping_country varchar(20) NULL,
	shipping_country_base_rate numeric(14, 3) NULL,
	PRIMARY KEY (id)
);

-- Вставка данных в справочник shipping_country_rates

INSERT INTO shipping_country_rates (
	shipping_country,
	shipping_country_base_rate)
SELECT DISTINCT
	shipping_country, 
	shipping_country_base_rate
FROM public.shipping
;

-------------------------------
-->>> 2. Создайте справочник тарифов доставки вендора по договору shipping_agreement 
--    из данных строки vendor_agreement_description через разделитель «:» (двоеточие без кавычек)

-- Создание таблицы справочника shipping_agreement

CREATE TABLE public.shipping_agreement (
	agreement_id bigint,
	agreement_number varchar(20),
	agreement_rate numeric (14, 2),
	agreement_commission numeric (14, 2), 
	PRIMARY KEY (agreement_id)
);

-- Вставка данных в справочник shipping_agreement

INSERT INTO public.shipping_agreement (
	agreement_id,
	agreement_number,
	agreement_rate,
	agreement_commission )
SELECT DISTINCT
	( regexp_split_to_array (vendor_agreement_description, ':+') ) [1]::bigint as agreement_id,
	( regexp_split_to_array (vendor_agreement_description, ':+') ) [2] as agreement_number,
	( regexp_split_to_array (vendor_agreement_description, ':+') ) [3]::numeric (14, 2)as agreement_rate,
	( regexp_split_to_array (vendor_agreement_description, ':+') ) [4]::numeric (14, 2) as agreement_commission
FROM public.shipping
;

-----------------------------------------
--> 3. Создайте справочник о типах доставки shipping_transfer из строки shipping_transfer_description 
--	через разделитель «:» (двоеточие без кавычек).

-- Создание таблицы справочника shipping_transfer

CREATE TABLE public.shipping_transfer (
	id serial,
	transfer_type varchar(10),
	transfer_model varchar(20),
	shipping_transfer_rate numeric (14, 3),
	PRIMARY KEY (id)
);

-- Вставка данных в справочник shipping_transfer

INSERT INTO public.shipping_transfer (
	transfer_type ,
	transfer_model ,
	shipping_transfer_rate )
SELECT DISTINCT
	( regexp_split_to_array ( shipping_transfer_description, ':+' )) [1] as transfer_type ,
	( regexp_split_to_array ( shipping_transfer_description, ':+' )) [2] as transfer_model ,
	shipping_transfer_rate
FROM public.shipping s 
;

-----------------------------------------
--> 4. Создайте таблицу shipping_info, справочник комиссий по странам, 
--	с уникальными доставками shipping_id и свяжите ее с созданными справочниками 
--	shipping_country_rates, shipping_agreement, shipping_transfer 
--	и константной информации о доставке shipping_plan_datetime, payment_amount, vendor_id.

-- Создание таблицы справочника shipping_info

CREATE TABLE public.shipping_info (
	shipping_id bigint,
	vendor_id int8 NULL,
	payment_amount numeric(14, 2) NULL,
	shipping_plan_datetime timestamp NULL,
	shipping_transfer_id bigint,
	shipping_agreement_id bigint,
	shipping_country_rates_id bigint,
	PRIMARY KEY (shipping_id),
	FOREIGN KEY (shipping_transfer_id) REFERENCES shipping_transfer (id) ON UPDATE CASCADE,
	FOREIGN KEY (shipping_agreement_id) REFERENCES shipping_agreement (agreement_id) ON UPDATE CASCADE,
	FOREIGN KEY (shipping_country_rates_id) REFERENCES shipping_country_rates (id) ON UPDATE CASCADE
);

-- Вставка данных в справочник shipping_info

INSERT INTO public.shipping_info (
	shipping_id ,
	vendor_id ,
	payment_amount ,
	shipping_plan_datetime ,
	shipping_transfer_id ,
	shipping_agreement_id ,
	shipping_country_rates_id)
SELECT DISTINCT
	shippingid as shipping_id ,
	vendorid as vendor_id ,
	payment_amount ,
	shipping_plan_datetime ,
	st.id as shipping_transfer_id ,
	( regexp_split_to_array (vendor_agreement_description, ':+') ) [1]::bigint as shipping_agreement_id,
	scr.id as shipping_country_rates_id
FROM public.shipping as s
LEFT JOIN shipping_country_rates as scr
	ON s.shipping_country = scr.shipping_country
LEFT JOIN shipping_transfer as st
	ON ( regexp_split_to_array ( s.shipping_transfer_description, ':+' )) [1] = st.transfer_type
	AND ( regexp_split_to_array ( s.shipping_transfer_description, ':+' )) [2] = st.transfer_model
;

---------------------------------------
--> 5. Создайте таблицу статусов о доставке shipping_status и включите туда информацию 
--	из лога shipping (status , state). Добавьте туда вычислимую информацию 
--	по фактическому времени доставки shipping_start_fact_datetime, shipping_end_fact_datetime . 
--	Отразите для каждого уникального shipping_id его итоговое состояние доставки.

-- Создание таблицы shipping_status

CREATE TABLE shipping_status (
	shipping_id bigint,
	status varchar(20) NULL,
	state varchar(20) NULL,
	shipping_start_fact_datetime timestamp,
	shipping_end_fact_datetime timestamp
);

-- Вставка данных d shipping_status

INSERT INTO shipping_status (
	shipping_id ,
	status ,
	state ,
	shipping_start_fact_datetime ,
	shipping_end_fact_datetime 
	)
WITH ship_fact as (
	SELECT 
		shippingid , --as shipping_id ,
		MAX (state_datetime) as max_state_dt,
		MAX (CASE WHEN state = 'booked' THEN state_datetime 
		 		END) as shipping_start_fact_datetime ,
		MAX (CASE WHEN state = 'recieved' THEN state_datetime 
		 		END) as shipping_end_fact_datetime
	FROM public.shipping as s
	GROUP BY shippingid
)
SELECT 
	s.shippingid as shipping_id ,
	s.status ,
	s.state ,
	sf.shipping_start_fact_datetime ,
	sf.shipping_end_fact_datetime
FROM ship_fact as sf
INNER JOIN public.shipping as s
	ON  sf.shippingid = s.shippingid AND  sf.max_state_dt = s.state_datetime
;

--------------------------------------------
--> 6. Создайте представление shipping_datamart на основании готовых таблиц для аналитики 
	
CREATE OR REPLACE VIEW shipping_datamart AS
SELECT
	si.shipping_id ,
	si.vendor_id ,
	transfer_type ,
		DATE_PART ('day', shipping_end_fact_datetime - shipping_start_fact_datetime) 
	as full_day_at_shipping ,
		(CASE WHEN shipping_end_fact_datetime > shipping_plan_datetime THEN 1 
			  WHEN shipping_end_fact_datetime IS NULL THEN NULL 
		      ELSE 0 END) 
	as is_delay,
		(CASE WHEN status = 'finished' THEN 1 ELSE 0 END) 
	as is_shipping_finish,
		(CASE WHEN shipping_end_fact_datetime > shipping_plan_datetime 
			  THEN DATE_PART ('day', shipping_end_fact_datetime - shipping_plan_datetime) ELSE 0 END) 
	as delay_day_at_shipping ,
	payment_amount ,
		payment_amount * (shipping_country_base_rate + agreement_rate + shipping_transfer_rate) 
	as vat,
		payment_amount * agreement_commission 
	as profit
FROM public.shipping_info as si
LEFT JOIN shipping_transfer AS st 
	ON si.shipping_transfer_id = st.id
LEFT JOIN shipping_status
	USING (shipping_id) 
LEFT JOIN shipping_agreement as sa 
	ON si.shipping_agreement_id = sa.agreement_id 
LEFT JOIN shipping_country_rates as scr 
	ON si.shipping_country_rates_id = scr.id
;

	

 