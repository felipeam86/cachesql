from pathlib import Path
from typing import Union

import joblib
import pandas as pd
import pyarrow as pa


class BaseSerializer:
    fmt = ""
    extension = ""


class ParquetSerializer(BaseSerializer):

    fmt = "parquet"
    extension = ".parquet"

    @classmethod
    def load(cls, filepath: Union[str, Path]) -> pd.DataFrame:
        """Load dataframe from parquet file."""
        return pd.read_parquet(filepath)

    @classmethod
    def dump(cls, results: pd.DataFrame, filepath: Union[str, Path]) -> None:
        """Dump dataframe to parquet file."""
        try:
            results.to_parquet(filepath)
        except pa.ArrowInvalid as e:
            raise ValueError(
                "It seems that your query is returning a column with a type not "
                "yet supported by Arrow. Consider using 'joblib' instead: "
                "Database(uri='...', store_backend='joblib')"
            )


class JoblibSerializer(BaseSerializer):

    fmt = "joblib"
    extension = ".joblib"

    @classmethod
    def load(cls, filepath: Union[str, Path]) -> pd.DataFrame:
        """Load dataframe from file dumped with joblib."""
        return joblib.load(filepath)

    @classmethod
    def dump(cls, results: pd.DataFrame, filepath: Union[str, Path]) -> None:
        """Dump dataframe with joblib."""
        joblib.dump(results, filepath)
