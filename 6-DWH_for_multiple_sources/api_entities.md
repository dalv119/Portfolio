Название витрины: cdm.dm_courier_ledger.

Список полей, которые необходимы для витрины

* `id` — идентификатор записи.
* `courier_id` — ID курьера, которому перечисляем.
* `courier_name` — Ф. И. О. курьера.
* `settlement_year` — год отчёта.
* `settlement_month` — месяц отчёта, где `1` — январь и `12` — декабрь.
* `orders_count` — количество заказов за период (месяц).
* `orders_total_sum` — общая стоимость заказов.
* `rate_avg` — средний рейтинг курьера по оценкам пользователей.
* `order_processing_fee` — сумма, удержанная компанией за обработку заказов, которая высчитывается как `orders_total_sum * 0.25`.
* `courier_order_sum` — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
* `courier_tips_sum` — сумма, которую пользователи оставили курьеру в качестве чаевых.
* `courier_reward_sum` — сумма, которую необходимо перечислить курьеру. Вычисляется как `courier_order_sum + courier_tips_sum * 0.95` (5% — комиссия за обработку платежа).

Список таблиц в слое DDS, из которых вы возьмёте поля для витрины. Отметьте, какие таблицы уже есть в хранилище, а каких пока нет. Недостающие таблицы вы создадите позднее. Укажите, как они будут называться.

Для создания ветрины из слоя DDS используются таблицы

вновь созданные загруженные через API:

1. dds.dm_couriers

* `courier_id`
* `courier_name`

2. dds.fct_deliveries

* `order_id` — ID заказа;
* `order_ts` — дата и время создания заказа;
* `delivery_id` — ID доставки;
* `courier_id` — ID курьера;
* `address` — адрес доставки;
* `delivery_ts` — дата и время совершения доставки;
* `rate` — рейтинг доставки, который выставляет покупатель: целочисленное значение от 1 до 5;
* `sum` — сумма заказа (в руб.);
* `tip_sum` — сумма чаевых, которые оставил покупатель курьеру (в руб.).
