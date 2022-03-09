from unittest.mock import patch

from cachesql import Database, __version__


def test_version():
    assert __version__ == "0.2.1"


def test_load_results_from_cache(query, tmp_path):
    """Test Database can be imported from cachesql"""

    with patch("cachesql.Database._query") as mock_query:
        db = Database(
            name="db",
            uri="sqlite:///file:path/to/database1b?mode=ro&uri=true",
            cache_store=tmp_path,
        )
        assert (
            mock_query.call_count == 0
        ), "'Database._query' should not have been called up to this point"
        df = db.query(query=query)
        assert mock_query.call_count == 1, "'Database._query' should have been invoked"
