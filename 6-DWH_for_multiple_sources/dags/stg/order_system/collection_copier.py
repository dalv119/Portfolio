from logging import Logger

from stg.order_system.collection_loader import CollectionLoader
from stg.order_system.pg_saver import PgSaver


class CollectionCopier:
    _LOG_THRESHOLD = 100
    _SESSION_LIMIT = 10000

    def __init__(self, collection_loader: CollectionLoader, pg_saver: PgSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.log = logger

    def run_copy(self, collection: str) -> int:
        data = self.collection_loader.get_documents(collection, self._SESSION_LIMIT)
        self.log.info(f"found {len(data)} documents to sync from {collection}.")

        self.pg_saver.init_collection(collection)

        i = 0

        for d in data:
            self.pg_saver.save_object(collection, str(d["_id"]), d["update_ts"], d)

            i += 1
            if i % self._LOG_THRESHOLD == 0:
                self.log.info(f"processed {i} documents of {len(data)} while syncing {collection}.")

        return len(data)
