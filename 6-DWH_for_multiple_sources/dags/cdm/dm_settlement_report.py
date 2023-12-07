

from lib import PgConnect


class SettlementReportLoad:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def init_settlement_report(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(    
                    """
                        DELETE FROM cdm.dm_settlement_report;
                        INSERT INTO cdm.dm_settlement_report (
                            restaurant_id, 
                            restaurant_name, 
                            settlement_date, 
                            orders_count, 
                            orders_total_sum, 
                            orders_bonus_payment_sum, 
                            orders_bonus_granted_sum, 
                            order_processing_fee, 
                            restaurant_reward_sum)
                        SELECT 
                            dp.restaurant_id,
                            dr.restaurant_name,
                            dt.date,
                            COUNT ( DISTINCT fps.order_id ) as orders_count, 
                            SUM (fps.total_sum) as orders_total_sum, 
                            SUM (fps.bonus_payment) as orders_bonus_payment_sum, 
                            SUM (fps.bonus_grant) as orders_bonus_granted_sum, 
                            SUM (fps.total_sum) * 0.25 as order_processing_fee, 
                            (SUM (fps.total_sum) - SUM (fps.total_sum) * 0.25 - SUM (fps.bonus_payment)) as restaurant_reward_sum
                        FROM dds.fct_product_sales as fps
                        LEFT JOIN dds.dm_products AS dp 
                        ON fps.product_id = dp.id
                        LEFT JOIN dds.dm_restaurants AS dr 
                        ON dp.restaurant_id = dr.id
                        LEFT JOIN dds.dm_orders AS dmo
                        ON fps.order_id = dmo.id
                        LEFT JOIN dds.dm_timestamps AS dt
                        ON dmo.timestamp_id = dt.id
                        WHERE dmo.order_status = 'CLOSED'
                        GROUP BY dp.restaurant_id, dr.restaurant_name, dt.date
                        ;
                    """
                )



