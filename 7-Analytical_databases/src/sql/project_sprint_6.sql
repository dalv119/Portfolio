-- ПРОЕКТ СПРИТНТ-6 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>


-- 1. Загрузить данные из S3 ----------------------------------------
-- Напишите DAG в Airflow, чтобы подключиться к бакету sprint6 в S3 и 
-- выгрузить файл group_log.csv в папку data
-- см.C:\Users\11961\Documents\DE_Yandex\s6-lessons\dags\project_import_from_S3_dag.py

-- 2. Создать таблицу group_log в Vertica ----------------------------

TRUNCATE TABLE STV2023070314__STAGING.group_log;
DROP TABLE IF EXISTS STV2023070314__STAGING.group_log;

CREATE TABLE IF NOT EXISTS STV2023070314__STAGING.group_log (
	group_id INTEGER,
	user_id INTEGER,
	user_id_from INTEGER,
	event VARCHAR(20),
	datetime TIMESTAMP
   )
ORDER BY group_id, user_id
PARTITION BY "datetime"::date
GROUP BY calendar_hierarchy_day("datetime"::date, 3, 2)
;

-- 3. Загрузить данные в Vertica -----------------------------------------
-- Реализуйте DAG, который будет считывать из папки data файл group_log.csv и 
-- загружать его в созданную таблицу group_log в схеме *__STAGING

-- df_group_log['user_id_from'] = pd.array(df_group_log['user_id_from'], dtype="Int64")
-- в даге сделано через vertica-python оператор.

-- COPY STV2023070314__STAGING.group_log (
-- 	group_id ,
-- 	user_id ,
-- 	user_id_from ,
-- 	event ,
-- 	datetime )
-- FROM LOCAL '/data/group_log.csv'
-- DELIMITER ','
-- ENCLOSED by '"'
-- null ''
-- skip 1
-- REJECTED DATA AS TABLE STV2023070314__STAGING.dialogs_rej
-- ;

-- 4. Создать таблицу связи -----------------------------------------------

drop table if exists STV2023070314__DWH.l_user_group_activity;

create table STV2023070314__DWH.l_user_group_activity(
	hk_l_user_group_activity 	bigint primary key,
	hk_user_id		bigint not null 
		CONSTRAINT fk_l_user_group_activity_users 
		REFERENCES STV2023070314__DWH.h_users (hk_user_id),
	hk_group_id 	bigint not null 
		CONSTRAINT fk_l_user_group_activity_groups 
		REFERENCES STV2023070314__DWH.h_groups (hk_group_id),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2)
;

-- 5. Создать скрипты миграции в таблицу связи ------------------------------

TRUNCATE TABLE STV2023070314__DWH.l_user_group_activity ;

INSERT INTO STV2023070314__DWH.l_user_group_activity(
	hk_l_user_group_activity, 
	hk_user_id,
	hk_group_id,
	load_dt,
	load_src)
select distinct
	hash( hg.hk_group_id, hu.hk_user_id ),
	hu.hk_user_id,
	hg.hk_group_id,
	now() as load_dt,
	's3' as load_src
from STV2023070314__STAGING.group_log as gl
left join STV2023070314__DWH.h_users AS hu
	on gl.user_id = hu.user_id
left join STV2023070314__DWH.h_groups AS hg
	on gl.group_id = hg.group_id
where hash( hg.hk_group_id, hu.hk_user_id ) not in ( select hk_l_user_group_activity 
												 from STV2023070314__DWH.l_user_group_activity )
;

-- 6. Создать и наполнить сателлит -------------------------------------------

DROP TABLE IF EXISTS STV2023070314__DWH.s_auth_history;

CREATE TABLE STV2023070314__DWH.s_auth_history (
	hk_l_user_group_activity INTEGER 
		CONSTRAINT fk_s_auth_history_l_user_group_activity 
		REFERENCES STV2023070314__DWH.l_user_group_activity (hk_l_user_group_activity), 
	user_id_from INTEGER,
	event VARCHAR(20),
	event_dt TIMESTAMP,
	load_dt DATETIME,
	load_src VARCHAR(20))
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity ALL NODES
PARTITION BY load_dt::DATE
GROUP BY calendar_hierarchy_day(load_dt::DATE, 3, 2)

INSERT INTO STV2023070314__DWH.s_auth_history (
	hk_l_user_group_activity, 
	user_id_from,
	event,
	event_dt,
	load_dt,
	load_src)
SELECT 
	luga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	gl.datetime,
	now() as load_dt,
	's3' as load_src
FROM STV2023070314__STAGING.group_log AS gl
LEFT JOIN STV2023070314__DWH.h_groups AS hg 
	ON gl.group_id = hg.group_id
LEFT JOIN STV2023070314__DWH.h_users AS hu 
	ON gl.user_id = hu.user_id
LEFT JOIN STV2023070314__DWH.l_user_group_activity AS luga 
ON hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id
;


-- 7. Подготовить CTE для ответов бизнесу --------------------------------------

-- 7.1. Подготовить CTE user_group_messages 

WITH user_group_messages AS (
    SELECT 
    	hg.hk_group_id AS hk_group_id,
    	count (DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
	FROM STV2023070314__DWH.h_groups AS hg
    LEFT JOIN STV2023070314__DWH.l_groups_dialogs AS lgd
    	ON hg.hk_group_id = lgd.hk_group_id
    LEFT JOIN STV2023070314__DWH.l_user_message AS lum
    	ON lgd.hk_message_id = lum.hk_message_id
    WHERE hg.group_id IN ( SELECT group_id 
    					   FROM STV2023070314__DWH.h_groups 
    					   ORDER BY registration_dt 
    					   LIMIT 10)
    GROUP BY hg.hk_group_id					   
)
SELECT hk_group_id,
       cnt_users_in_group_with_messages
FROM user_group_messages
ORDER BY cnt_users_in_group_with_messages
LIMIT 10
;


-- 7.2. Подготовить CTE user_group_log 

WITH user_group_log AS (
    SELECT 
    	hg.hk_group_id AS hk_group_id,
    	count (DISTINCT luga.hk_user_id) AS cnt_added_users
	FROM STV2023070314__DWH.h_groups AS hg
	LEFT JOIN STV2023070314__DWH.l_user_group_activity AS luga
		ON hg.hk_group_id = luga.hk_group_id
	LEFT JOIN STV2023070314__DWH.s_auth_history AS sah
		ON luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
    WHERE hg.group_id IN ( SELECT group_id 
    					   FROM STV2023070314__DWH.h_groups 
    					   ORDER BY registration_dt 
    					   LIMIT 10)
    	AND sah.event = 'add'
    GROUP BY hg.hk_group_id
)
SELECT hk_group_id
       ,cnt_added_users
FROM user_group_log
ORDER BY cnt_added_users
LIMIT 10
;


-- 7.3. Написать запрос и ответить на вопрос бизнеса 

WITH user_group_log AS (
    SELECT 
    	hg.hk_group_id AS hk_group_id,
    	count (DISTINCT luga.hk_user_id) AS cnt_added_users
	FROM STV2023070314__DWH.h_groups AS hg
	LEFT JOIN STV2023070314__DWH.l_user_group_activity AS luga
		ON hg.hk_group_id = luga.hk_group_id
	LEFT JOIN STV2023070314__DWH.s_auth_history AS sah
		ON luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
    WHERE hg.group_id IN ( SELECT group_id 
    					   FROM STV2023070314__DWH.h_groups 
    					   ORDER BY registration_dt 
    					   LIMIT 10)
    	AND sah.event = 'add'
    GROUP BY hg.hk_group_id
    ORDER BY cnt_added_users
	LIMIT 10
),
user_group_messages AS (
    SELECT 
    	hg.hk_group_id AS hk_group_id,
    	count (DISTINCT lum.hk_user_id) AS cnt_users_in_group_with_messages
	FROM STV2023070314__DWH.h_groups AS hg
    LEFT JOIN STV2023070314__DWH.l_groups_dialogs AS lgd
    	ON hg.hk_group_id = lgd.hk_group_id
    LEFT JOIN STV2023070314__DWH.l_user_message AS lum
    	ON lgd.hk_message_id = lum.hk_message_id
    WHERE hg.group_id IN ( SELECT group_id 
    					   FROM STV2023070314__DWH.h_groups 
    					   ORDER BY registration_dt 
    					   LIMIT 10)
    GROUP BY hg.hk_group_id
    ORDER BY cnt_users_in_group_with_messages
	LIMIT 10
)
SELECT  
	ugl.cnt_added_users,
	ugm.cnt_users_in_group_with_messages,
	ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users AS group_conversion
FROM user_group_log AS ugl
LEFT JOIN user_group_messages AS ugm 
	ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users DESC
;
