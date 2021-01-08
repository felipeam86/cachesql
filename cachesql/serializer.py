from pathlib import Path
from typing import Union

import joblib
import pandas as pd
import pyarrow as pa


class BaseSerializer:
    fmt = ""
    extension = ""


class ParquetSerializer(BaseSerializer):
    """A serializer of pd.DataFrame to parquet format.

    Parameters
    ----------
    compression : {'snappy', 'gzip', 'brotli', None}, default 'snappy'
        Name of the compression to use. See pd.DataFrame.to_parquet docs
    """

    fmt = "parquet"
    extension = ".parquet"

    def __init__(self, compression=None):
        self.compression = compression or "snappy"

    @classmethod
    def load(cls, filepath: Union[str, Path]) -> pd.DataFrame:
        """Load dataframe from parquet file."""
        return pd.read_parquet(filepath)

    def dump(self, results: pd.DataFrame, filepath: Union[str, Path]) -> None:
        """Dump dataframe to parquet file."""
        try:
            results.to_parquet(filepath, compression=self.compression)
        except pa.ArrowInvalid:
            raise ValueError(
                "It seems that your query is returning a column with a type not "
                "yet supported by Arrow. Consider using 'joblib' instead: "
                "Database(uri='...', store_backend='joblib')"
            )


class JoblibSerializer(BaseSerializer):
    """A serializer of pd.DataFrame based on joblib.

    Parameters
    ----------
    compression
        Optional compression level for the data. Passed to parameter
        'compress' of joblib.dump. See joblib docs
    """

    fmt = "joblib"
    extension = ".joblib"

    def __init__(self, compression=None):
        self.compression = compression or 0

    @classmethod
    def load(cls, filepath: Union[str, Path]) -> pd.DataFrame:
        """Load dataframe from file dumped with joblib."""
        return joblib.load(filepath)

    def dump(self, results: pd.DataFrame, filepath: Union[str, Path]) -> None:
        """Dump dataframe with joblib."""
        joblib.dump(results, filepath, compress=self.compression, protocol=4)
