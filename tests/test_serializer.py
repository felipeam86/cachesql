from uuid import uuid1

import pandas as pd
import pytest

from cachesql import serializer


class TestParquetSerializer:
    def test_dump_load_results(self, tmp_path, results):
        s = serializer.ParquetSerializer()
        s.dump(results, tmp_path / "file.parquet")
        assert (tmp_path / "file.parquet").exists()
        results_loaded = s.load(tmp_path / "file.parquet")
        assert results.equals(results_loaded)

    def test_invalid_arrow_type(self, tmp_path):
        s = serializer.ParquetSerializer()
        results = pd.Series([uuid1() for i in range(3)], name="uuid_col").to_frame()
        with pytest.raises(ValueError) as excinfo:
            s.dump(results, tmp_path / "file.parquet")
        assert "Database(uri='...', store_backend='joblib')" in str(excinfo.value)


class TestJoblibSerializer:
    def test_dump_load_results(self, tmp_path, results):
        s = serializer.JoblibSerializer()
        s.dump(results, tmp_path / "file.parquet")
        assert (tmp_path / "file.parquet").exists()
        results_loaded = s.load(tmp_path / "file.parquet")
        assert results.equals(results_loaded)
