import hashlib
import json
from pathlib import Path
from typing import Tuple, Union
from zipfile import ZipFile

import pandas as pd


def hash_query(query_string: str) -> str:
    return hashlib.sha1(query_string.encode()).hexdigest()


class ParquetStore:
    """Disk store that stores to parquet files

    Parameters
    ----------
    cache_store
        Location where the returned values are cached.
    """

    def __init__(self, cache_store: Path) -> None:
        self.cache_store = Path(cache_store).expanduser()
        self.cache_store.mkdir(parents=True, exist_ok=True)

    def exists(self, query_string: str) -> bool:
        """Returns True if the results of the given query exist in cache"""
        metadata_file = self.get_metadata_filepath(query_string)
        cache_file = self.get_cache_filepath(query_string)
        return metadata_file.exists() and cache_file.exists()

    def get_metadata_filepath(self, query_string: str) -> Path:
        """Return the metadata filepath corresponding to that query_string"""
        arg_hash = hash_query(query_string)
        return self.cache_store / (arg_hash + ".json")

    def get_cache_filepath(self, query_string: str) -> Path:
        """Return the cached results filepath corresponding to that query_string"""
        arg_hash = hash_query(query_string)
        return self.cache_store / (arg_hash + ".parquet")

    def load_metadata(self, query_string: str) -> dict:
        """Load metadata of cached results for query_strings if it exists in cache"""
        metadata_file = self.get_metadata_filepath(query_string)
        if metadata_file.exists():
            return json.loads(metadata_file.read_text())
        else:
            raise ValueError("Metadata for the given query_string does not exist.")

    def load_results(self, query_string: str) -> pd.DataFrame:
        """Load cached results for query_strings if it exists in cache"""
        cache_file = self.get_cache_filepath(query_string)
        if cache_file.exists():
            return pd.read_parquet(cache_file)
        else:
            raise ValueError("Cached results for the given query_string do not exist.")

    def load(self, query_string: str) -> Tuple[pd.DataFrame, dict]:
        return self.load_results(query_string), self.load_metadata(query_string)

    def dump_metadata(self, query_string: str, metadata: dict) -> None:
        metadata["cache_file"] = self.get_cache_filepath(query_string).name
        metadata["query_string"] = query_string
        metadata_file = self.get_metadata_filepath(query_string)
        metadata_file.write_text(json.dumps(metadata, indent=True))

    def dump_results(self, query_string: str, results: pd.DataFrame) -> None:
        cache_file = self.get_cache_filepath(query_string)
        results.to_parquet(cache_file)

    def dump(self, query_string: str, results: pd.DataFrame, metadata: dict) -> None:
        self.dump_results(query_string, results)
        self.dump_metadata(query_string, metadata)

    def list(self) -> pd.DataFrame:
        """List cached function calls with some useful metadata"""
        # List everything first
        cache_list = [
            json.loads(f.read_text()) for f in self.cache_store.glob("*.json")
        ]

        if len(cache_list) == 0:
            default_metadata = [
                "query_string",
                "cache_file",
                "executed_at",
                "duration",
            ]
            return pd.DataFrame(columns=default_metadata)

        return pd.DataFrame(cache_list)

    def export(self, filename: Union[str, Path]) -> None:
        """Export contents of cache to a zip file

        Used in conjunction with the :py:meth:`Cache.import_cache <Cache.import_cache>` method,
        you can share your cache with your colleagues in order to guarantee reproducibility of
        your code. Or you can simply use it to migrate your cache from one environment to the
        other.

        Parameters
        ----------
        filename : Union[str, Path]
            Path to a zip file where cache will be exported
        """

        filename = Path(filename)
        filename = filename.with_suffix(".zip") if filename.suffix == "" else filename
        cache = self.list()
        normalized_cache_files = cache.cache_file.map(
            lambda p: self.cache_store / Path(p).name
        )
        with ZipFile(filename, "w") as myzip:
            for cache_file in normalized_cache_files:
                cache_file = self.cache_store / cache_file.name
                metadata_file = cache_file.with_suffix(".json")
                myzip.write(str(cache_file), arcname=Path(cache_file).name)
                myzip.write(str(metadata_file), arcname=Path(metadata_file).name)

    def import_cache(self, filename: Union[str, Path]) -> None:
        """Import contents to cache

        Used in conjunction with the :py:meth:`Cache.export <Cache.export>` method,
        you can share your cache with your colleagues in order to guarantee reproducibility of
        your code. Or you can simply use it to migrate your cache from one environment to the
        other.

        Parameters
        ----------
        filename : Union[str, Path]
            Path to a zip file containing a previously exported cache
        """
        with ZipFile(filename, "r") as myzip:
            myzip.extractall(path=self.cache_store)
