-- 1.4. Подготовьте витрину данных

-- 1.4.1 Сделайте представление для таблиц из базы production в analysis 

CREATE OR REPLACE VIEW analysis.orderitems AS 
SELECT * FROM production.orderitems;

CREATE OR REPLACE VIEW analysis.orders AS 
SELECT * FROM production.orders;

CREATE OR REPLACE VIEW analysis.orderstatuses AS 
SELECT * FROM production.orderstatuses;

CREATE OR REPLACE VIEW analysis.products AS 
SELECT * FROM production.products;

CREATE OR REPLACE VIEW analysis.users AS 
SELECT * FROM production.users;

