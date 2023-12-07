import uuid
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


# определение классов, которые используются для хранения данных с помощью аннотаций

class User_Category_Counters(BaseModel):
    user_id: uuid.UUID
    category_id: uuid.UUID
    category_name: str
    order_cnt: int

class User_Product_Counters(BaseModel):
    user_id: uuid.UUID
    product_id: uuid.UUID
    product_name: str
    order_cnt: int


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    # Загрузка данных для счётчика заказов по категориям товаров
    def cdm_user_category_counters(self,
                       user_id: uuid,
                       category_id: uuid,
                       category_name: str,
                       order_cnt: int
                       ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_category_counters (
                            user_id, 
                            category_id, 
                            category_name, 
                            order_cnt )
                        VALUES (
                            %(user_id)s,
                            %(category_id)s,
                            %(category_name)s,
                            1
                            )
                            ON CONFLICT (user_id, category_id) 
                            DO UPDATE SET 
                                order_cnt = user_product_counters.order_cnt + 1,
                                category_name = EXCLUDED.category_name

                    """,
                    {
                        'user_id': user_id,
                        'category_id': category_id,
                        'category_name': category_name,
                    }
                )

    # Загрузка данных для счётчика заказов по продуктам (блюдам)
    def cdm_user_product_counters(self,
                       user_id: uuid,
                       product_id: uuid,
                       product_name: str,
                       order_cnt: int
                       ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_product_counters (
                            user_id, 
                            product_id, 
                            product_name, 
                            order_cnt )
                        VALUES (
                            %(user_id)s,
                            %(product_id)s,
                            %(product_name)s,
                            1
                            )
                            ON CONFLICT (user_id, product_id) 
                            DO UPDATE SET 
                                order_cnt = user_product_counters.order_cnt + 1,
                                product_name = EXCLUDED.product_name

                    """,
                    {
                        'user_id': user_id,
                        'product_id': product_id,
                        'product_name': product_name,
                    }
                )

##############################################################################################


class OrderCdmBuilder:
    def __init__(self, dict: Dict) -> None:
        self._dict = dict
        self.source_system = "orders-system-kafka"
        self.order_ns_uuid = uuid.UUID('5a4d3c50-1590-468e-90bd-411ef6ac192f')

    # ф-ция создание UUID, уникальный идентификатор, сгенерированный на основе id объекта
    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj)) 
     
# Подготовка данных для загрузки в cdm слой витрин ----------------------------------------------

    def user_category_counters(self) -> List[User_Category_Counters]:
        categories = []
        user_id = self._dict['user']['id']

        for prod_dict in self._dict['products']:
            cat_name = prod_dict['category']
            categories.append( 
                User_Category_Counters(
                    user_id=self._uuid(user_id),
                    category_id=self._uuid(cat_name),
                    category_name=cat_name
            )
        )
        return categories


    def user_product_counters(self) -> List[User_Product_Counters]:
        products = []
        user_id = self._dict['user']['id']

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            prod_name = prod_dict['name']
            products.append(
                User_Product_Counters(
                    user_id=self._uuid(user_id),
                    product_id=self._uuid(prod_id),
                    product_name=prod_name,
                )
            )
        return products









