# Шаг 3. Создать витрину в разрезе зон

# Создать витрину в разрезе зон
# Вы создадите геослой — найдёте распределение атрибутов, связанных с событиями, по географическим зонам (городам). 
# Если проанализировать этот слой, то можно понять поведение пользователей по различным регионам.
# В этой витрине вы учитываете не только отправленные сообщения, но и другие действия — подписки, реакции, регистрации 
# (рассчитываются по первым сообщениям). 
# Пока присвойте таким событиям координаты последнего отправленного сообщения конкретного пользователя.
    # month — месяц расчёта;
    # week — неделя расчёта;
    # zone_id — идентификатор зоны (города);
    # week_message — количество сообщений за неделю;
    # week_reaction — количество реакций за неделю;
    # week_subscription — количество подписок за неделю;
    # week_user — количество регистраций за неделю;
    # month_message — количество сообщений за месяц;
    # month_reaction — количество реакций за месяц;
    # month_subscription — количество подписок за месяц;
    # month_user — количество регистраций за месяц.

# ===============================================================================
# В И Т Р И Н А - 2 джоба

# Шаг 3. Создать витрину в разрезе зон

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

    
    events = spark.read\
        .parquet(events_base_path)\
        .sample(0.05)\
        .withColumn('lat_1', 
                    F.radians(F.col('lat'))) \
        .withColumn('lon_1', 
                    F.radians(F.col('lon')))      

    city_geo = spark.read\
        .csv(city_geo_path, sep = ";", header = True )\
        .withColumn('lat_2', 
                    F.radians(F.regexp_replace(F.col('lat'), ',', '.').cast('float')))\
        .withColumn('lon_2', 
                    F.radians(F.regexp_replace(F.col('lng'), ',', '.').cast('float')))

    # расчет расстояния между двумя координатами
    d = F.lit(2) * F.lit(6371) * F.asin(F.sqrt(\
        F.pow(F.sin((F.col('lat_1') - F.col('lat_2'))/F.lit(2)), 2) + \
        F.cos(F.col('lat_1')) * F.cos(F.col('lat_2')) * \
        F.pow(F.sin((F.col('lon_1') - F.col('lon_2'))/F.lit(2)), 2) \
        ))


    # определяем город из которого было отправлено сообщение
    get_city = events\
        .crossJoin(city_geo)\
        .withColumn("distance", d)\
        .withColumn("rank", F.row_number().over(Window.partitionBy("date","event.message_from").orderBy(F.asc("distance"))))\
        .where("rank = 1")\
        .selectExpr("event.message_from as user_id", 'event_type', 'id as zone_id', 'city', "date", 'rank')        


    w_week = Window().partitionBy('week', 'zone_id')
    w_month = Window().partitionBy('month', 'zone_id')

    report_2 = get_city \
        .withColumn("month",
                    F.trunc(F.col("date"), "month"))\
        .withColumn("week",
                    F.trunc(F.col("date"), "week"))\
        .withColumn("rank",
                    F.row_number().over(Window().partitionBy('user_id').orderBy(F.col('date'))))\
        .withColumn("week_message",
                    F.count(F.when(F.col('event_type') == "message", 1).otherwise(0)).over(w_week))\
        .withColumn("week_reaction",
                    F.sum(F.when(F.col('event_type') == "reaction",1).otherwise(0)).over(w_week))\
        .withColumn("week_subscription",
                    F.sum(F.when(F.col('event_type') == "subscription",1).otherwise(0)).over(w_week))\
        .withColumn("week_user",
                    F.sum(F.when(F.col('rank') == 1, 1).otherwise(0)).over(w_week))\
        .withColumn("month_message",
                    F.sum(F.when(F.col('event_type') == "message", 1).otherwise(0)).over(w_month)) \
        .withColumn("month_reaction",
                    F.sum(F.when(F.col('event_type') == "reaction", 1).otherwise(0)).over(w_month)) \
        .withColumn("month_subscription",
                    F.sum(F.when(F.col('event_type') == "subscription", 1).otherwise(0)).over(w_month))\
        .withColumn("month_user",
                    F.sum(F.when(F.col('rank') == 1, 1).otherwise(0)).over(w_month))\
        .select('month', 'week', 'zone_id', 'week_message', 'week_reaction', 'week_subscription', 'week_user', 
                'month_message', 'month_reaction', 'month_subscription', 'month_user')\
        .distinct()

    # Сохранение резудьтатов витрины в файл 
    report_2.write.mode("overwrite").parquet(f"{output_geo_path}/date={date}")

if __name__ == "__main__":
    main()




# ==========================================================================================================================

