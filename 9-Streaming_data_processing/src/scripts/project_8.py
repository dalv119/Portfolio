import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType



def create_spark_session():
    # необходимые библиотеки для интеграции с Kafka и PostgreSQL
    spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

    # создаём Spark сессию 
    spark = SparkSession.builder \
        .appName("RestaurantSubscribeStreamingService") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()
    
    return spark

# настройки безопасности
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";'
}

def read_kafka_stream():
    # код для чтения Kafka-стрима

    restaurant_read_stream_df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .options(**kafka_security_options) \
        .option('kafka.ssl.truststore.location', '/usr/lib/jvm/java-1.17.0-openjdk-amd64/lib/security/cacerts') \
        .option('kafka.ssl.truststore.password', 'changeit') \
        .option('subscribe', 'vladimirdo_in') \
        .load()
    
    return restaurant_read_stream_df

def get_current_timestamp_utc():
    # определяем текущее время в UTC в миллисекундах
    current_timestamp_utc = int(round(datetime.utcnow().timestamp()))
    
    return current_timestamp_utc

def filter_stream_data(filtered_read_stream_df, current_timestamp_utc):
    # код для фильтрации данных из Kafka-стрима

    # определяем схему входного сообщения для json
    incomming_message_schema = StructType([
        StructField('restaurant_id' , StringType(), False),
        StructField('adv_campaign_id' , StringType(), False),
        StructField('adv_campaign_content' , StringType(), False),
        StructField('adv_campaign_owner' , StringType(), False),
        StructField('adv_campaign_owner_contact' , StringType(), False),
        StructField('adv_campaign_datetime_start' , LongType(), False),
        StructField('adv_campaign_datetime_end' , LongType(), False),
        StructField('datetime_created' , LongType(), False),
                ])
    
   
    # десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
    filtered_read_stream_df = (restaurant_read_stream_df 
                            .withColumn('value', col('value').cast(StringType()))
                            .withColumn('event', from_json(col('value'), incomming_message_schema))
                            .selectExpr('event.*')
                            .where((col("adv_campaign_datetime_start") < current_timestamp_utc) & (col("adv_campaign_datetime_end") > current_timestamp_utc))
                                ) 
    
    return filtered_read_stream_df

def read_subscribers_data():
    # код для чтения данных о подписчиках

    # вычитываем всех пользователей с подпиской на рестораны
    subscribers_restaurant_df = spark.read \
                        .format('jdbc') \
                        .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                        .option('driver', 'org.postgresql.Driver') \
                        .option('dbtable', 'subscribers_restaurants') \
                        .option('user', 'student') \
                        .option('password', 'de-student') \
                        .load()
    
    return subscribers_restaurant_df

def join_and_transform_data(filtered_read_stream_df, subscribers_restaurant_df):
    # код для объединения и преобразования данных

    # джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
    result_df = (filtered_read_stream_df
                .join(subscribers_restaurant_df, filtered_read_stream_df.restaurant_id == subscribers_restaurant_df.restaurant_id, 'inner')
                .withColumn("feedback", lit(None).cast("string"))
                .withColumn("client_id", f.substring('client_id', 0, 6))
                .withColumn("trigger_datetime_created", lit(datetime.now()))
                .dropDuplicates(['client_id', 'adv_campaign_id'])
                .withWatermark('timestamp', '1 minutes')
                .select('restaurant_id',
                        'adv_campaign_id',
                        'adv_campaign_content',
                        'adv_campaign_owner',
                        'adv_campaign_owner_contact',
                        'adv_campaign_datetime_start',
                        'adv_campaign_datetime_end',
                        'datetime_created',
                        'client_id',
                        'trigger_datetime_created',
                        'feedback',
                        )
                )
    return result_df

def save_to_postgresql_and_kafka(df):
    # код для сохранения данных в PostgreSQL и Kafka

    # записываем df в PostgreSQL с полем feedback
    try:
        df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:15432/de") \
        .option('driver', 'org.postgresql.Driver') \
        .option("dbtable", "words") \
        .option("user", "jovyan") \
        .option("password", "jovyan") \
        .save()

    # обработка ошибок в коде    
    except Exception as e:
        print(f"Error writing to PostgreSQL: {str(e)}")

    # создаём df для отправки в Kafka. Сериализация в json
    try:
        df_kafka = df.select(to_json(struct('restaurant_id',
                                 'adv_campaign_id',
                                 'adv_campaign_content',
                                 'adv_campaign_owner',
                                 'adv_campaign_owner_contact',
                                 'adv_campaign_datetime_start',
                                 'adv_campaign_datetime_end',
                                 'datetime_created',
                                 'client_id',
                                 'trigger_datetime_created'))
                                 .alias('value')
                    )
        # отправляем сообщения в результирующий топик Kafka без поля feedback
        df_kafka \
            .writeStream \
            .outputMode("append") \
            .format("kafka") \
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
            .options(**kafka_security_options) \
            .option("topic", 'vladimirdo_out') \
            .option("checkpointLocation", "test_query") \
            .trigger(processingTime="15 seconds") \
            .start()

    # обработка ошибок в коде      
    except Exception as e:
        print(f"Error writing to Kafka: {str(e)}")

if __name__ == "__main__":
    spark = create_spark_session()
    restaurant_read_stream_df = read_kafka_stream()
    current_timestamp_utc = get_current_timestamp_utc()
    filtered_data = filter_stream_data(restaurant_read_stream_df, current_timestamp_utc)
    subscribers_data = read_subscribers_data()
    result_df = join_and_transform_data(filtered_data, subscribers_data)
    save_to_postgresql_and_kafka(result_df)
    spark.stop()




# ===========================================================================================

