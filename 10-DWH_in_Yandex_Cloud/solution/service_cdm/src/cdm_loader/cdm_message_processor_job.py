from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from cdm_loader.repository import CdmRepository, OrderCdmBuilder


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger,
                 ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._cdm_repository = cdm_repository
        self._batch_size = 100
        self._logger = logger

        

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                self._logger.info(f"{datetime.utcnow()}: NO messages. Quitting.")
                break

            self._logger.info(f"{datetime.utcnow()}: {msg}")

            order_dict = msg['payload']

            if order_dict['status'] == 'CANCELLED':
                self._logger.info(f"{datetime.utcnow()}: CANCELLED. Skipping.")
                continue

            builder = OrderCdmBuilder(order_dict)

            for c in builder.user_category_counters():
                self._cdm_repository.cdm_user_category_counters(c)

            for p in builder.user_product_counters():
                self._cdm_repository.cdm_user_product_counters(p)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
