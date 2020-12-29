import logging
import time
from dataclasses import InitVar, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Union
from zipfile import ZipFile

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
    normalize
        If True, normalize the queries to make the cache independent from formatting changes
    """

    name: str
    uri: InitVar[str]
    cache_store: InitVar[Union[str, Path]] = Path(__file__).parent / ".cache"
    normalize: InitVar[bool] = True

    def __post_init__(self, uri, cache_store, normalize) -> None:
        self.store = ParquetStore(
            cache_store=Path(cache_store) / self.name,
            normalize=normalize,
        )
        self.engine = create_engine(uri, convert_unicode=True)
        self.session = set()

    def querydb(
        self, query_string: str, force: bool = False, cache: bool = True
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

        self.session.add(query_string)
        return results

    def exists_in_cache(self, query_string: str) -> bool:
        """Return True if a given query_string has cached results"""
        return self.store.exists(query_string)

    def _querydb(self, query_string: str) -> pd.DataFrame:
        return pd.read_sql(sql=query_string, con=self.engine)

    def export_session(self, filename: Union[str, Path]) -> None:
        """Export contents of cache obtained during this session to a zip file

        Used in conjunction with the :py:meth:`Store.import_cache <Store.import_cache>` method,
        you can share the cache of one specific coding session with your colleagues in order to
        guarantee reproducibility of your code and speed up collaboration. Or you can simply use
        it to migrate your cache from one environment to the other.

        Parameters
        ----------
        filename
            Path to a zip file where cache will be exported
        """

        filename = Path(filename)
        filename = filename.with_suffix(".zip") if filename.suffix == "" else filename
        with ZipFile(filename, "w") as myzip:
            for query in self.session:
                cache_file = self.store.get_cache_filepath(query)
                metadata_file = self.store.get_metadata_filepath(query)
                myzip.write(str(cache_file), arcname=Path(cache_file).name)
                myzip.write(str(metadata_file), arcname=Path(metadata_file).name)
