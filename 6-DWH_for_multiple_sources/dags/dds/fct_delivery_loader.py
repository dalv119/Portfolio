

from lib import PgConnect


class DeliveryStgLoad:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def init_delivery(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(    
                    """
                        INSERT INTO dds.fct_deliveries (
                            order_id, 
                            order_ts, 
                            delivery_id, 
                            courier_id, 
                            address, 
                            delivery_ts, 
                            rate, 
                            sum, 
                            tip_sum)
                        SELECT 
                            sdd.order_id, 
                            sdd.order_ts, 
                            sdd.delivery_id, 
                            sdd.courier_id, 
                            sdd.address, 
                            sdd.delivery_ts, 
                            sdd.rate, 
                            sdd.sum, 
                            sdd.tip_sum
                        FROM stg.deliverysystem_deliveries AS sdd
                        LEFT JOIN dds.fct_deliveries AS dfd
                        ON sdd.order_id = dfd.order_id
                        WHERE  sdd.order_ts > ( SELECT MAX(order_ts) FROM dds.fct_deliveries )
	                        AND dfd.order_id IS NULL
                        ;
                    """
                )



