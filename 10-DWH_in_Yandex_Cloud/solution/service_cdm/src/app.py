import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig

from cdm_loader.cdm_message_processor_job import CdmMessageProcessor
from cdm_loader.repository import CdmRepository
import pandas as pd

app = Flask(__name__)

# Инициализируем конфиг. Для удобства, вынесли логику получения значений переменных окружения в отдельный класс.
config = AppConfig()

# Заводим endpoint для проверки, поднялся ли сервис.
# Обратиться к нему можно будет GET-запросом по адресу localhost:5000/health.
# Если в ответе будет healthy - сервис поднялся и работает.
@app.get('/health')
def hello_world():
    return 'healthy'


if __name__ == '__main__':
    # Устанавливаем уровень логгирования в Debug, чтобы иметь возможность просматривать отладочные логи.
    app.logger.setLevel(logging.DEBUG)

    # Инициализируем процессор сообщений.
    proc = CdmMessageProcessor(
        consumer=config.kafka_consumer(),
        producer=config.kafka_producer(),
        cdm_repository=CdmRepository(config.pg_warehouse_db()),
        batch_size=100,
        logger=app.logger
    )

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=25)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', use_reloader=False)
