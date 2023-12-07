

from lib import PgConnect


class CourierledgerLoad:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def init_courier_ledger(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(    
                    """
                        DELETE FROM cdm.dm_courier_ledger
                        WHERE  concat(settlement_month, settlement_year ) <> to_char(now(), 'MM')::numeric||to_char(now(), 'YYYY') ;
                        
                        INSERT INTO cdm.dm_courier_ledger (
                            courier_id,
                            courier_name,
                            settlement_year,
                            settlement_month,
                            orders_count,
                            orders_total_sum,
                            rate_avg,
                            order_processing_fee,
                            courier_order_sum,
                            courier_tips_sum,
                            courier_reward_sum 
                            )
                        WITH courier_gr AS (
                            SELECT
                                courier_id,
                                order_id,
                                date_part('year', order_ts) AS settlement_year,
                                date_part('month', order_ts) AS settlement_month,
                                "sum",
                                AVG(rate) OVER (order by (courier_id,TO_CHAR(order_ts, 'MMYYYY'))) as rate_avg,
                                CASE 
                                    WHEN (AVG(rate) OVER (order by (courier_id,TO_CHAR(order_ts, 'MMYYYY'))))< 4 THEN (CASE WHEN "sum" * 0.05 < 100 THEN 100 ELSE "sum" * 0.05 END)
                                    WHEN (AVG(rate) OVER (order by (courier_id,TO_CHAR(order_ts, 'MMYYYY'))))>= 4 AND (AVG(rate) OVER (order by (courier_id,TO_CHAR(order_ts, 'MMYYYY'))))< 4.5 THEN (CASE WHEN "sum" * 0.07 < 150 THEN 150 ELSE "sum" * 0.07 END)
                                    WHEN (AVG(rate) OVER (order by (courier_id,TO_CHAR(order_ts, 'MMYYYY'))))>= 4.5 AND (AVG(rate) OVER (order by (courier_id,TO_CHAR(order_ts, 'MMYYYY'))))< 4.9 THEN (CASE WHEN "sum" * 0.08 < 175 THEN 175 ELSE "sum" * 0.08 END)
                                    WHEN (AVG(rate) OVER (order by (courier_id,TO_CHAR(order_ts, 'MMYYYY'))))>= 4.9 THEN (CASE WHEN "sum" * 0.1 < 200 THEN 200 ELSE "sum" * 0.1 END)
                                END  AS courier_order,
                                tip_sum
                            FROM dds.fct_deliveries
                        )
                        SELECT 
                            courier_id,
                            dc.name AS courier_name,
                            settlement_year,
                            settlement_month,
                            COUNT ( DISTINCT order_id ) AS orders_count,
                            SUM("sum") as orders_total_sum,
                            rate_avg,
                            SUM("sum") * 0.25 AS order_processing_fee,
                            SUM(courier_order) AS courier_order_sum,
                            SUM(tip_sum) AS courier_tips_sum,
                            SUM(courier_order) + SUM(tip_sum) * 0.95 AS courier_reward_sum
                        FROM courier_gr AS cgr
                        LEFT JOIN dds.dm_couriers AS dc
                        ON cgr.courier_id = dc._id
                        GROUP BY courier_id, dc.name, settlement_year, settlement_month, rate_avg
                        ;
                    """
                )



