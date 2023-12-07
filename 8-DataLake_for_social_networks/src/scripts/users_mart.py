# Шаг 2. Создать витрину в разрезе пользователей

# Найдите расстояние от координаты отправленного сообщения до центра города. 
# Событие относится к тому городу, расстояние до которого наименьшее.
# Когда вы вычислите геопозицию каждого отправленного сообщения, создайте витрину с тремя полями:
    # user_id — идентификатор пользователя.
    # act_city — актуальный адрес. Это город, из которого было отправлено последнее сообщение.
    # home_city — домашний адрес. Это последний город, в котором пользователь был дольше 27 дней.
# Выясните, сколько пользователь путешествует. Добавьте в витрину два поля:
    # travel_count — количество посещённых городов. Если пользователь побывал в каком-то городе повторно, то это считается за отдельное посещение.
    # travel_array — список городов в порядке посещения.
    # local_time — местное время события — время последнего события пользователя, о котором у нас есть данные с учётом таймзоны геопозициии

# =============================================================================>>>
# В И Т Р И Н А - 1 джоба

# Шаг 2. Создать витрину в разрезе пользователей


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
    date = sys.argv[1]                  # date = "2022-05-31"
    depth = sys.argv[2]                 # depth = 30
    events_base_path = sys.argv[3]      # /user/master/data/geo/events/
    city_geo_path = sys.argv[4]         # /user/vladimirdo/geo.csv    
    output_geo_path = sys.argv[5]       # /user/vladimirdo/data/analitics/

    conf = SparkConf().setAppName(f"ConnectionUsersJob-{date}-d{depth}")
    sc = SparkContext(conf=conf)
    spark = SQLContext(sc)


    # читаем в датафрейм таблицу с событиями, переводим координаты событий из градусов в радианы 
    # и для ускорения тестирования ограничиваем выборку типом данных - message
    messages = spark.read\
        .option("basePath", events_base_path)\
        .parquet(*input_event_paths(events_base_path, date, depth))\
        .withColumn('lat_1', F.radians(F.col('lat'))) \
        .withColumn('lon_1', F.radians(F.col('lon'))) \
        .where("event_type='message'")

    # читаем в датафрейм таблицу с координатами городов, заменяем "," на "." и переводим их из градусов в радианы 
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

    # определяем город из которого было отправлено сообщение с помощью оконной функции 
    # как наменьшее расстояние до ближайшего города
    get_city = messages\
        .crossJoin(city_geo)\
        .withColumn("distance", d)\
        .withColumn("rank", F.row_number().over(Window.partitionBy("date","event.message_from").orderBy(F.asc("distance"))))\
        .where("rank = 1")\
        .selectExpr("event.message_from as user_id", 'id', 'city', "date", 'event.datetime', 'timezone_list')
    #     .cache()

    # определяем актуальный адрес. Это город, из которого было отправлено последнее сообщение.   
    get_act_city = get_city\
        .withColumn("rank_last_message_city", F.row_number().over(Window.partitionBy("user_id",).orderBy(F.desc("date"))))\
        .where("rank_last_message_city = 1")\
        .withColumnRenamed("city", "act_city")\
        .select("user_id", "act_city")

    # определяем домашний адрес. Это последний город, в котором пользователь был дольше 27 дней.
    # для расчета принял условие, что 27 дней это разница дат между первым и последним сообщением в последовательности сообщений 
    # отправленных из одного города
    # 1 - сначала создаём столбец, чтобы определить, изменился ли city для данного user_id
    # 2 - затем заменяем null на 0 и просуммируем столбцы city_change для user_id
    # 3 - группируем неизменяемые поля и выводим справочно два столбца с датами начала и конца периода поледовательности
    # 4 - вычисляем длину последовательности более 27 дней
    # 5 - выбираем самую последнюю такую последовательность
    get_home_city = get_city\
        .withColumn("city_change", 
                    (F.col("city") != F.lag("city").over(Window.partitionBy("user_id",).orderBy("date"))).cast("int"))\
        .fillna(0).withColumn("city_group", 
                            F.sum("city_change").over(Window.partitionBy("user_id",).orderBy("date")))\
        .withColumn('count_city', 
                    F.count('city').over(Window.partitionBy('user_id', 'city_group').orderBy('user_id')))\
        .groupBy("user_id", "city", 'count_city', "city_group")\
        .agg(F.min("date").alias("start"), 
            F.max("date").alias("end"))\
        .withColumn('date_diff', 
                    F.datediff('end', 'start'))\
        .where('date_diff > 27')\
        .withColumn('rn_last_city', 
                    F.row_number().over(Window.partitionBy('user_id').orderBy(F.desc('start'))))\
        .where('rn_last_city = 1')\
        .orderBy( F.desc("count_city"), "user_id")\
        .selectExpr("user_id", "start", "end", "city as home_city", "count_city", 'date_diff')
    #     .show(10)

    # сколько пользователь путешествует. 
    # travel_count — количество посещённых городов
    # travel_array — список городов в порядке посещения.
    travels_array = get_city\
        .select("user_id", 'city')\
        .groupBy("user_id") \
        .agg(F.array_distinct(F.collect_list('city')).alias('travel_array')) \
        .select('user_id', 'travel_array', F.size('travel_array').alias('travel_count'))
    # .show(10)

    # Местное время события — время последнего события пользователя, о котором у нас есть данные 
    # с учётом таймзоны геопозициии этого события
    get_local_time = get_city\
        .withColumn("TIME_UTC",
                    F.col("datetime").cast("Timestamp"))\
        .withColumn("timezone",F.concat(F.lit("Australia/"),F.col('city'))) \
        .withColumn("local_time",F.from_utc_timestamp(F.col("TIME_UTC"), F.col('timezone_list')))\
        .where('local_time is not null')\
        .distinct()\
        .select('user_id', 'city', "TIME_UTC", "local_time")
    #     .show(50)

    # итоговый расчет - добавляем в витрину местное время и дату расчёта витрины
    report_1 = get_act_city\
    .join(get_home_city, 'user_id')\
    .join(travels_array, 'user_id')\
    .join(get_local_time, 'user_id')\
    .select('user_id', "act_city", 'home_city', 'travel_count','travel_array', 'local_time')\
    .show(10)

    # Сохранение резудьтатов витрины в файл 
    report_1.write.mode("overwrite").parquet(f"{output_geo_path}/date={date}")

if __name__ == "__main__":
    main()





# ===============================================================================================================================

