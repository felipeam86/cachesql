import pytest

from sqlcache import store, utils


class TestParquetStore:
    def test_init(self, tmp_path):
        s = store.ParquetStore(cache_store=tmp_path)
        assert s.cache_store.exists()

    def test_get_filepaths(self, tmp_path, query_string):
        """Test the metadata and results cache file"""
        s = store.ParquetStore(cache_store=tmp_path)
        metadata_file = s.get_metadata_filepath(query_string)
        cache_file = s.get_cache_filepath(query_string)
        assert metadata_file.stem == store.hash_query(query_string)
        assert cache_file.stem == store.hash_query(query_string)
        assert metadata_file == tmp_path / (store.hash_query(query_string) + ".json")
        assert cache_file == tmp_path / (store.hash_query(query_string) + ".parquet")

    def test_dump_load_metadata(self, parquet_store, query_string, metadata):
        parquet_store.dump_metadata(query_string, metadata)
        assert parquet_store.get_metadata_filepath(query_string).exists()
        metadata_loaded = parquet_store.load_metadata(query_string)
        assert metadata == metadata_loaded

        assert metadata_loaded["query_string"] != query_string
        assert metadata_loaded["query_string"] == utils.normalize_query(query_string)

        with pytest.raises(ValueError) as excinfo:
            parquet_store.load_metadata("select * from dummy")
        assert f"Metadata for the given query_string does not exist." in str(
            excinfo.value
        )

    def test_dump_load_results(self, parquet_store, query_string, results):
        parquet_store.dump_results(query_string, results)
        assert parquet_store.get_cache_filepath(query_string).exists()
        results_loaded = parquet_store.load_results(query_string)
        assert results.equals(results_loaded)

        with pytest.raises(ValueError) as excinfo:
            parquet_store.load_results("select * from dummy")
        assert f"Cached results for the given query_string do not exist." in str(
            excinfo.value
        )

    def test_dump_load(self, parquet_store, query_string, results, metadata):
        parquet_store.dump(query_string, results, metadata)
        assert parquet_store.get_cache_filepath(query_string).exists()
        assert parquet_store.get_metadata_filepath(query_string).exists()
        results_loaded, metadata_loaded = parquet_store.load(query_string)
        assert results.equals(results_loaded)
        assert metadata == metadata_loaded

        with pytest.raises(ValueError) as excinfo:
            parquet_store.load("select * from dummy")
        assert f"Cached results for the given query_string do not exist." in str(
            excinfo.value
        )

    def test_exists_in_cache(self, parquet_store, query_string, metadata, results):
        """Test the function that asserts if there is cache for a given string"""
        assert not parquet_store.get_metadata_filepath(query_string).exists()
        assert not parquet_store.get_cache_filepath(query_string).exists()
        assert not parquet_store.exists(query_string)

        parquet_store.dump(query_string, results, metadata)

        assert parquet_store.get_metadata_filepath(query_string).exists()
        assert parquet_store.get_cache_filepath(query_string).exists()
        assert parquet_store.exists(query_string)

    def test_list_empty_store(self, parquet_store):
        store_content = parquet_store.list()
        assert store_content.shape == (0, 4)
        assert list(store_content.columns) == [
            "query_string",
            "cache_file",
            "executed_at",
            "duration",
        ]

    def test_list_store_with_one_element(
        self, parquet_store, query_string, metadata, results
    ):
        parquet_store.dump(query_string, results, metadata)
        store_content = parquet_store.list()
        assert store_content.shape == (1, 4)
        assert list(store_content.columns) == [
            "query_string",
            "cache_file",
            "executed_at",
            "duration",
        ]
        assert (
            store_content.loc[0, "cache_file"]
            == parquet_store.get_cache_filepath(query_string).name
        )
        assert store_content.loc[0, "query_string"] == utils.normalize_query(
            query_string
        )

    def test_export_import_cache(self, tmp_path, query_string, metadata, results):
        cache_store1 = tmp_path / "cache1"
        cache_store2 = tmp_path / "cache2"
        cache_export_file = tmp_path / "cache.zip"

        store1 = store.ParquetStore(cache_store=cache_store1)
        store2 = store.ParquetStore(cache_store=cache_store2)

        store1.dump(query_string, results, metadata)

        store1.export(cache_export_file)
        store2.import_cache(cache_export_file)

        assert store1.list().equals(store2.list())

    def test_export_import_cache_with_queries_list(self, tmp_path, query_string, metadata, results):
        queries = [
            "select top 10 * from Receipts",
            "select top 20 * from Receipts",
            "select top 5 * from Receipts",
            "select top 25 * from Receipts",
        ]

        cache_store1 = tmp_path / "cache1"
        cache_store2 = tmp_path / "cache2"
        cache_export_file = tmp_path / "cache.zip"

        store1 = store.ParquetStore(cache_store=cache_store1)
        store2 = store.ParquetStore(cache_store=cache_store2)
        for query in queries:
            store1.dump(query, results, metadata)

        store1.export(cache_export_file, queries=queries[:2])
        store2.import_cache(cache_export_file)

        store1_cache = store1.list()
        store2_cache = store2.list()
        assert store1_cache.shape[0] == 4
        assert store2_cache.shape[0] == 2
        assert set(store2_cache.query_string) == set(queries[:2])

    def test_cache_independent_from_format(self, tmp_path, metadata, results):
        query1 = "select top 3 * from receipts"
        query2 = "SELECT top 3 * FROM receipts"

        assert utils.normalize_query(query1) == utils.normalize_query(query2)

        parquet_store = store.ParquetStore(cache_store=tmp_path, normalize=True)

        for query in (query1, query2):
            assert not parquet_store.get_metadata_filepath(query).exists()
            assert not parquet_store.get_cache_filepath(query).exists()
            assert not parquet_store.exists(query)

        parquet_store.dump(query1, results, metadata)

        for query in (query1, query2):
            assert parquet_store.get_metadata_filepath(query).exists()
            assert parquet_store.get_cache_filepath(query).exists()
            assert parquet_store.exists(query)


class TestHashQuery:
    def test_hash_query(self, query_string):
        assert (
            store.hash_query(query_string) == "26689adeaee8e1b156ad49334ee522dd89bd9142"
        )

        assert store.hash_query(query_string) != store.hash_query(
            query_string, normalize=True
        )
