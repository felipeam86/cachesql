import logging
import time
from dataclasses import InitVar, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union

import pandas as pd
from sqlalchemy import create_engine

from . import __version__
from .store import ParquetStore

logger = logging.getLogger(__name__)


@dataclass
class DB:
    """Database connector with caching functionality

    Generic class to connect to SQL databases. When querying DBs the results will
    be cached on a disk to speed up the next time the same query is done.

    Parameters
    ----------
    name
        Name of the database.
    uri
        URI string passed to SQLalchemy to connect to the database
    cache_store
        Path where cache should be stored
    """

    name: str
    uri: InitVar[str]
    cache_store: InitVar[Union[str, Path]] = Path(__file__).parent / ".cache"

    def __post_init__(self, uri, cache_store) -> None:
        self.store = ParquetStore(cache_store=Path(cache_store) / self.name)
        self.engine = create_engine(uri, convert_unicode=True)

    def querydb(
        self, query_string, force: bool = False, cache: bool = True
    ) -> pd.DataFrame:

        if self.store.exists(query_string) and not (force or not cache):
            logger.info(f"Loading results of {self.name}.querydb() call from cache.")
            results, metadata = self.store.load(query_string)
            logger.info(
                f"Finished loading backup. "
                f"The previous execution was done on the {metadata['executed_at']} "
                f"and lasted {timedelta(seconds=metadata['duration'])}s"
            )
        else:
            executed_at = datetime.now().isoformat()
            logger.info(f"Executing {self.name}.querydb()")
            start_time = time.time()
            results = self._querydb(query_string)
            duration = time.time() - start_time
            logger.info(f"Finished execution in {timedelta(seconds=duration)}s")

            if cache:
                metadata = {
                    "db_name": self.name,
                    "sqlcache": __version__,
                    "username": self.engine.url.username or "unknown",
                    "executed_at": executed_at,
                    "duration": duration,
                }
                self.store.dump(query_string, results, metadata)
                logger.info(f"Finished backup of results")

        return results

    def exists_in_cache(self, query_string) -> bool:
        """Return True if a given query_string has cached results"""
        return self.store.exists(query_string)

    def _querydb(self, query_string: str) -> pd.DataFrame:
        return pd.read_sql(sql=query_string, con=self.engine)
