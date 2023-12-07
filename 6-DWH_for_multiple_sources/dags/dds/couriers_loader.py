

from lib import PgConnect


class CourierStgLoad:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def init_couriers(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(    
                    """
                        INSERT INTO dds.dm_couriers (
                            _id,
                            name)
                        SELECT 
                            sdc._id,
                            sdc.name
                        FROM stg.deliverysystem_couriers AS sdc
                        LEFT JOIN dds.dm_couriers AS ddc
                                ON sdc._id = ddc._id 
                        WHERE ddc._id IS NULL
                        ;
                    """
                )



