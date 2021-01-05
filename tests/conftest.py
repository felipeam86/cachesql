#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime

import pandas as pd
import pytest

from cachesql import sql, store


@pytest.fixture
def db(tmp_path):
    dbconnector = sql.Database(
        name="dbtest",
        uri="sqlite:///file:path/to/database?mode=ro&uri=true",
        cache_store=tmp_path,
    )
    return dbconnector


@pytest.fixture
def query():
    return "select top 3 * from Receipts"


@pytest.fixture
def metadata():
    return {
        "query": "SELECT top 3 *\nFROM Receipts",
        "cache_file": "cache_file.parquet",
        "executed_at": datetime.now().isoformat(),
        "duration": 600,
    }


@pytest.fixture
def results():
    return pd.DataFrame(data=[[0, 0, 0], [1, 1, 1]], columns=["a", "b", "c"])


@pytest.fixture
def file_store(tmp_path):
    return store.FileStore(cache_store=tmp_path, normalize=True)
