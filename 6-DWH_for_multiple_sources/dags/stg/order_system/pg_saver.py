from datetime import datetime, timedelta

from lib import PgConnect
from lib.dict_util import json2str


class PgSaver:
    def __init__(self, connect: PgConnect) -> None:
        self._db = connect

    def init_collection(self, collection_name: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        DELETE FROM stg.ordersystem_{collection_name}
                        WHERE update_ts < %(threshold)s;
                    """.format(collection_name=collection_name),
                    {
                        "threshold": datetime.utcnow() - timedelta(days=30)
                    }
                )

    def save_object(self, collection_name: str, id: str, update_ts: datetime, val) -> None:
        str_val = json2str(val)
        self._upsert_value(collection_name, id, update_ts, str_val)

    def _upsert_value(self, collection_name: str, id: str, update_ts: datetime, val: str):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO stg.ordersystem_{collection_name}(object_id, object_value, update_ts)
                        VALUES (%(id)s, %(val)s, %(update_ts)s)
                        ON CONFLICT (object_id) DO UPDATE
                        SET
                            object_value = EXCLUDED.object_value,
                            update_ts = EXCLUDED.update_ts;
                    """.format(collection_name=collection_name),
                    {
                        "id": id,
                        "val": val,
                        "update_ts": update_ts
                    }
                )
