import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig

from dds_loader.dds_message_processor_job import DdsMessageProcessor
from dds_loader.repository import DdsRepository
import pandas as pd

app = Flask(__name__)


# Заводим endpoint для проверки, поднялся ли сервис.
# Обратиться к нему можно будет GET-запросом по адресу localhost:5000/health.
# Если в ответе будет healthy - сервис поднялся и работает.
@app.get('/health')
def hello_world():
    return 'healthy'


if __name__ == '__main__':
    # Устанавливаем уровень логгирования в Debug, чтобы иметь возможность просматривать отладочные логи.
    app.logger.setLevel(logging.DEBUG)

    # Инициализируем конфиг. Для удобства, вынесли логику получения значений переменных окружения в отдельный класс.
    config = AppConfig()

    # Инициализируем процессор сообщений.
    proc = DdsMessageProcessor(
        consumer=config.kafka_consumer(),
        producer=config.kafka_producer(),
        dds_repository=DdsRepository(config.pg_warehouse_db()),
        batch_size=100,
        logger=app.logger
    )

    # Запускаем процессор в бэкграунде.
    # BackgroundScheduler будет по расписанию вызывать функцию run нашего обработчика(StgMessageProcessor).
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=25)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', use_reloader=False)
