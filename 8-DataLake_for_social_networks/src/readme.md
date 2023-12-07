Обновление структуры Data Lake
Структура хранилища

Слой сырых данных — Raw
Таблица событий содержащая два дополнительных поля (широта и долгота исходящих сообщений) 			находится в HDFS по пути -
/user/master/data/geo/events

Координаты городов Австралии [geo.csv](https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv) в HDFS по пути -
/user/vladimirdo/

Директория в которой будут тестироваться данные
/user/vladimirdo/data/tmp/

Директория предназначенная для аналитиков
/user/vladimirdo/data/analitics/

Частота обновления данных - 1 раз в неделю

Формат выходных данных - parquet
