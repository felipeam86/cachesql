#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
from unittest.mock import Mock

import pandas as pd
import pytest

from sqlcache import sql, store


def fake_results(query_string):
    query_hash = store.hash_query(query_string)
    return pd.DataFrame(
        data=[[query_string, query_hash]], columns=["query_string", "query_hash"]
    )


@pytest.fixture
def querydb():
    func = Mock(side_effect=fake_results)
    return func


@pytest.fixture
def db(querydb, tmp_path):
    dbconnector = sql.DB(
        name="dbtest",
        uri="sqlite:///file:path/to/database?mode=ro&uri=true",
        cache_store=tmp_path,
    )
    dbconnector._querydb = querydb
    return dbconnector


@pytest.fixture
def query_string():
    return "select top 3 * from Receipts"


@pytest.fixture
def metadata():
    return {
        "query_string": "select top 3 * from Receipts",
        "cache_file": "cache_file.parquet",
        "executed_at": datetime.now().isoformat(),
        "duration": 600,
    }


@pytest.fixture
def results():
    return pd.DataFrame(data=[[0, 0, 0], [1, 1, 1]], columns=["a", "b", "c"])


@pytest.fixture
def parquet_store(tmp_path):
    return store.ParquetStore(cache_store=tmp_path)
