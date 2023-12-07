from datetime import datetime
from logging import Logger
from typing import Any, Dict, List
import uuid


from lib.kafka_connect import KafkaConsumer, KafkaProducer

from dds_loader.repository import DdsRepository, OrderDdsBuilder


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._repository = dds_repository

        self._batch_size = 30

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
            builder = OrderDdsBuilder(order_dict)

            self._load_hubs(builder)

            self._logger.info(f"{datetime.utcnow()}: {msg}")
            
            self._load_links(builder)
            self._load_sats(builder)

            dst_msg = {
                "object_id": str(builder.h_order().h_order_pk),
                "sent_dttm": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "object_type": "order_report",
                "payload": {
                    "id": str(builder.h_order().h_order_pk),
                    "order_dt": builder.h_order().order_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "status": builder.s_order_status().status,
                    "restaurant": {
                        "id": str(builder.h_restaurant().h_restaurant_pk),
                        "name": builder.s_restaurant_names().name
                    },
                    "user": {
                        "id": str(builder.h_user().h_user_pk),
                        "username": builder.s_user_names().username
                    },
                    "products": self._format_products(builder)
                }
            }

            self._logger.info(f"{datetime.utcnow()}: {dst_msg}")
            self._producer.produce(dst_msg)

        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _load_hubs(self, builder: OrderDdsBuilder) -> None:

        self._repository.dds_h_user(builder.h_user())

        for p in builder.h_product():
            self._repository.dds_h_product(p)

        for c in builder.h_category():
            self._repository.dds_h_category(c)

        self._repository.dds_h_restaurant(builder.h_restaurant())

        self._repository.dds_h_order(builder.h_order())

    def _load_links(self, builder: OrderDdsBuilder) -> None:
        self._repository.dds_l_order_user(builder.l_order_user())

        for op_link in builder.l_order_product():
            self._repository.dds_l_order_product(op_link)

        for pr_link in builder.l_product_restaurant():
            self._repository.dds_l_product_restaurant(pr_link)

        for pc_link in builder.l_product_category():
            self._repository.dds_l_product_category(pc_link)

    def _load_sats(self, builder: OrderDdsBuilder) -> None:
        self._repository.dds_s_order_cost(builder.s_order_cost())
        self._repository.dds_s_order_status(builder.s_order_status())
        self._repository.dds_s_restaurant_names(builder.s_restaurant_names())
        self._repository.dds_s_user_names(builder.s_user_names())

        for pn in builder.s_product_names():
            self._repository.dds_s_product_names(pn)

    def _format_products(self, builder: OrderDdsBuilder) -> List[Dict]:
        products = []

        p_names = {x.h_product_pk: x.name for x in builder.s_product_names()}

        cat_names = {x.h_category_pk: {"id": str(x.h_category_pk), "name": x.category_name} for x in builder.h_category()}
        prod_cats = {x.h_product_pk: cat_names[x.h_category_pk] for x in builder.l_product_category()}

        for p in builder.h_product():
            msg_prod = {
                "id": str(p.h_product_pk),
                "name": p_names[p.h_product_pk],
                "category": prod_cats[p.h_product_pk]
            }

            products.append(msg_prod)

        return products
