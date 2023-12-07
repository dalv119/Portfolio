# ------------------------------------------------------------------------------------------------
# Шаг 4. Построить витрину для рекомендации друзей

# Напомним, как будет работать рекомендация друзей: если пользователи подписаны на один канал, ранее никогда не переписывались 
# и расстояние между ними не превышает 1 км, то им обоим будет предложено добавить другого в друзья. Образовывается парный атрибут, 
# который обязан быть уникальным: порядок упоминания не должен создавать дубли пар.
    # user_left — первый пользователь;
    # user_right — второй пользователь;
    # processed_dttm — дата расчёта витрины;
    # zone_id — идентификатор зоны (города);
    # local_time — локальное время.

# ===============================================================================
# В И Т Р И Н А - 3 джоба

# Шаг 4. Построить витрину для рекомендации друзей

import sys
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'


from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import datetime
from pyspark.sql.types import TimestampType

import findspark
findspark.init()
findspark.find() 

def input_event_paths(events_base_path, date, depth):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [f"{events_base_path}date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(depth)]

def main():
    date = sys.argv[1]                  # date = "2022-05-30"
    depth = sys.argv[2]                 # depth = 30
    events_base_path = sys.argv[3]      # /user/master/data/geo/events/
    city_geo_path = sys.argv[4]         # /user/vladimirdo/geo.csv    
    output_geo_path = sys.argv[5]       # /user/vladimirdo/data/analitics/

    conf = SparkConf().setAppName(f"ConnectionUsersJob-{date}-d{depth}")
    sc = SparkContext(conf=conf)
    spark = SQLContext(sc)

    # читаем в датафрейм таблицу с событиями, переводим координаты событий из градусов в радианы 
    # и для ускорения тестирования ограничиваем выборку до 1%
    events = spark.read\
        .parquet(events_base_path)\
        .sample(0.01)\
        .withColumn('lat_1', F.radians(F.col('lat'))) \
        .withColumn('lon_1', F.radians(F.col('lon')))\

    # читаем в датафрейм таблицу с координатами городов и переводим их из градусов в радианы 
    city_geo = spark.read \
        .options(header= True, delimiter = ';')\
        .csv(city_geo_path)\
        .withColumn('lat_2', F.radians(F.regexp_replace(F.col('lat'), ',', '.').cast('float')))\
        .withColumn('lon_2', F.radians(F.regexp_replace(F.col('lng'), ',', '.').cast('float')))\
        .select('id', 'city', 'lat_2', 'lon_2', 'timezone_list')
                    
    # формула расчета расстояния между двумя координатами
    d = F.lit(2) * F.lit(6371) * F.asin(F.sqrt(\
        F.pow(F.sin((F.col('lat_1') - F.col('lat_2'))/F.lit(2)), 2) + \
        F.cos(F.col('lat_1')) * F.cos(F.col('lat_2')) * \
        F.pow(F.sin((F.col('lon_1') - F.col('lon_2'))/F.lit(2)), 2) \
        ))

    # определяем ближайший город пользователя и отбираем координаты его последнего отправленного сообщения 
    user_geo = events\
        .select('lat_1', 'lon_1', "event.user", 'event.datetime', 'date')\
        .crossJoin(city_geo)\
        .withColumn("distance", d)\
        .withColumn("rank", F.row_number().over(Window.partitionBy("date","user").orderBy(F.asc("distance"))))\
        .where("rank = 1")\
        .withColumn('rn', F.row_number().over(Window.partitionBy('user').orderBy(F.desc('date'))))\
        .where('rn = 1')\
        .select('lat_1', 'lon_1', F.col('user').alias("user_id"), F.col('id').alias('zone_id'), 
                'city', 'datetime', 'timezone_list')


    # Условие: если пользователи подписаны на один канал, ранее никогда не переписывались и расстояние между ними не превышает 1 км

    # создаем датафрейм уникальных подписчиков каналов
    friends = events\
        .select(F.col('event.subscription_channel').alias('r_sub_channel'), F.col('event.user').alias('r_user'))\
        .where('event_type = "subscription"')\
        .distinct()\
        .select('r_sub_channel', 'r_user')

    # делаем join таблицы на саму себя и находим комбинации подписчиков канала
    # например все комбинации user 
    # +---+----+
    # | gr|user|
    # +---+----+
    # |  1|  10|
    # |  1|  20|
    # |  1|  50|
    # |  2|  30|
    # |  2|  40|
    # +---+----+
    # в результате получаем
    # +---+----+------+
    # | gr|user|r_user|
    # +---+----+------+
    # |  1|  10|    20|
    # |  1|  10|    50|
    # |  1|  20|    10|
    # |  1|  20|    50|
    # |  1|  50|    10|
    # |  1|  50|    20|
    # |  2|  30|    40|
    # |  2|  40|    30|
    # +---+----+------+
    friend_comb = events\
        .select('event_type', F.col('event.subscription_channel').alias('sub_channel'), 'event.user')\
        .where('event_type = "subscription"')\
        .join(friends, (F.col('sub_channel') == friends.r_sub_channel) & (F.col('user') != friends.r_user))\
        .select('sub_channel', 'user', 'r_user')
    #     .cache()    
        
    # определяем пользователей которые ранее уже переписывались, т.е. хоть раз отправляли друг другу сообщения
    old_friend = events\
        .select('event_type', 'event.message_from', 'event.message_to')\
        .where("event_type = 'message'")\
        .distinct()\
        .select('message_from', 'message_to')

    # кандидаты в друзья - определяем пользователей которые были подписаны на один канал, но ранее никогда не переписывались
    # Пояснение:
    # чтобы исключить пользователей которые были ранее в переписке связываем пару подписчиков на канал [user_left, user_right] с
    # парой которая была в переписке [message_from, message_to] через  JOIN left_anti. В результате этого в отчет попадают
    # только те пары которые не связались, т.е. не были в переписке.
    candid_friend = friend_comb\
        .join(old_friend, (friend_comb.user == old_friend.message_from) & (friend_comb.r_user == old_friend.message_to), "left_anti")\
        .select(F.col('user').alias('user_left'), F.col('r_user').alias('user_right'))
    #     .cache()

    # определяем координаты кандидата в друзья user_left
    geo_frend_1 = candid_friend\
        .join(user_geo, candid_friend.user_left == user_geo.user_id, 'left')\
        .select('user_left', 'user_right',  F.col('lat_1').alias('lat_2'), 
                F.col('lon_1').alias('lon_2'))

    # определяем координаты кандидата в друзья user_right,
    # вычисляем расстояние между кандидатами в друзья и отбираем тех расстояние между которыми не превышает 1 км
    geo_frend_2 = geo_frend_1\
        .join(user_geo, candid_friend.user_right == user_geo.user_id, 'left')\
        .select('user_left', 'user_right', 'zone_id', 'lat_1', 'lon_1', 'lat_2', 'lon_2')\
        .withColumn("distance", d)\
        .where("distance <= 1")\
        .select('user_left', 'user_right', 'zone_id')

    
    # определяем Местное время события — время последнего события пользователя, о котором у нас есть данные 
    # с учётом таймзоны геопозициии этого события    
    get_local_time = user_geo\
        .withColumn("TIME_UTC",
                    F.col("datetime").cast("Timestamp"))\
        .withColumn("local_time",
                    F.from_utc_timestamp(F.col("TIME_UTC"), F.col('timezone_list')))\
        .where('local_time is not null')\
        .distinct()\
        .select('user_id', 'city', "TIME_UTC", "local_time")

    # итоговый расчет - добавляем в витрину местное время и дату расчёта витрины
    report_3 = geo_frend_2\
            .join(get_local_time, geo_frend_2.user_left == get_local_time.user_id)\
            .withColumn('processed_dttm', 
                        F.current_timestamp())\
            .select('user_left', 'user_right', 'zone_id', 'processed_dttm', 'local_time')
    # .show(10)
 
    # Сохранение в файл резудьтатов витрины
    report_3.write.mode("overwrite").parquet(f"{output_geo_path}/date={date}")

if __name__ == "__main__":
    main()


# ==================================================================================================================================

