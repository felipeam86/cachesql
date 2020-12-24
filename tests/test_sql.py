from sqlcache import __version__


class TestDBConnector:
    def test_load_results_from_cache(self, db, query_string):
        """Test the cache fetches results from cache on a second call"""

        assert not db.exists_in_cache(query_string)
        assert (
            db._querydb.call_count == 0
        ), "'db._querydb' should not have been called up to this point"

        # First call should call the function and dump results to cache
        df1 = db.querydb(query_string=query_string)
        assert db.exists_in_cache(query_string)
        assert (
            db._querydb.call_count == 1
        ), "'db._querydb' should have been invoked by the cache mechanism"

        # Second call should load results from cache without calling the function
        df2 = db.querydb(query_string=query_string)
        assert db._querydb.call_count == 1, (
            "'db._querydb' should have not been been called any more. "
            "Results should have been loaded from cache"
        )
        assert df1.equals(df2)

    def test_force_results_to_cache(self, db, query_string):
        """Test the cache is refreshed with the force flag and a second function call"""

        assert not db.exists_in_cache(query_string)
        assert (
            db._querydb.call_count == 0
        ), "'db._querydb' should not have been called up to this point"

        # First call should call the function and dump results to cache
        _ = db.querydb(query_string=query_string)
        assert db.exists_in_cache(query_string)
        assert (
            db._querydb.call_count == 1
        ), "'db._querydb' should have been invoked by the cache mechanism"

        # Second call should call the function again and refresh results on cache
        _ = db.querydb(query_string=query_string, force=True)
        assert (
            db._querydb.call_count == 2
        ), "'db._querydb' should have been invoked again because of the use of force flag"

    def test_bypass_cache(self, db, query_string):
        """Test that bypassing the cache ignores previous calls and does not dump results"""

        assert not db.exists_in_cache(query_string)
        assert (
            db._querydb.call_count == 0
        ), "'db._querydb' should not have been called up to this point"

        # First call that should completely bypass the cache
        _ = db.querydb(query_string=query_string, cache=False)
        assert not db.exists_in_cache(
            query_string
        ), "Nothing should have been dumped to cache because of the bypass flag"
        assert (
            db._querydb.call_count == 1
        ), "'querydb' should have been invoked by the cache mechanism"

        # Second call that should dump the results to cache
        _ = db.querydb(query_string=query_string, cache=True)
        assert db.exists_in_cache(
            query_string
        ), "This time, results should have been dumped to cache"
        assert (
            db._querydb.call_count == 2
        ), "'querydb' should have been invoked again because nothing was saved on cache on previous call"

        # Third call that should ignore the already existing dumped results
        _ = db.querydb(query_string=query_string, cache=False)
        assert db._querydb.call_count == 3, (
            "'querydb' should have been invoked again because of "
            "the bypass flag that ignores existing results on cache"
        )

    def test_metadata(self, db, query_string):
        # First call should call the function and dump results to cache
        _ = db.querydb(query_string=query_string)
        assert db.exists_in_cache(query_string)
        assert (
            db._querydb.call_count == 1
        ), "'db._querydb' should have been invoked by the cache mechanism"

        metadata = db.store.load_metadata(query_string)
        assert metadata["db_name"] == db.name
        assert metadata["sqlcache"] == __version__
        assert metadata["username"] == db.engine.url.username or "unknown"
        assert metadata["query_string"] == query_string
        assert "executed_at" in metadata
        assert "duration" in metadata
