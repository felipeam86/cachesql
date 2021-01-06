# CacheSQL: Fast, resilient and reproducible data analysis with cached SQL queries

[![Package version](https://img.shields.io/pypi/v/cachesql.svg)](https://pypi.org/project/cachesql) [![Coverage Status](https://coveralls.io/repos/github/felipeam86/cachesql/badge.svg)](https://coveralls.io/github/felipeam86/cachesql) [![Build Status](https://travis-ci.com/felipeam86/cachesql.svg?branch=develop)](https://travis-ci.com/felipeam86/cachesql)

CacheSQL is a simple library for making SQL queries with cache functionality. The main target of this library are
data scientists and data analysts that rely on [SQLalchemy](https://pypi.org/project/SQLAlchemy/) to query data from SQL
and [pandas](https://pypi.org/project/pandas/) to do the heavy lifting in Python.

The key features are:

- **Speed up development cycles**: Run your queries the first time against the database, get it from cache the next times.
  You no longer need to wait for your queries anymore.
- **Resilience**: If you lose access to the databases, you can rely on your cached results to run your code.
- **Reproducibility**: By freezing the state of your queries results in cache, you guarantee reproducibility of your code.
- **Simplify collaboration**: By sharing the state of your cache with colleagues, you can guarantee they will get the
  same results as you.
- **Your development code is your production code**: When ready to launch your code in production, simply turn off the
  cache functionality!
- **Reduce the load on production DBs**: By using the cache, you reduce the number of times you query the DBs.
- **Fast, efficient and reliable cache**: This comes from the usage of the parquet format through
  [pyarrow](https://pypi.org/project/pyarrow/).
- **Simpler code**: No more added complexity on your code to load backups from disk or run the query if no backup exists.

## The basics
Install with pip

```bash
pip install cachesql
```


Run your queries once, get them from cache the next time!

```python
>>> from cachesql import Database
>>> db = Database(uri="postgresql://user:password@localhost:5432/mydatabase")
>>> expensive_query = "SELECT * FROM table WHERE {conditions}" #  <--- Imagine this is a very long and expensive query.
>>> df = db.query(expensive_query)
INFO:cachesql.sql:Querying 'mydatabase'
INFO:cachesql.sql:Finished in 0:23:04.005710s
INFO:cachesql.sql:Results have been stored in cache
```

Ok, that took 23 minutes, but I need to run my code again from scratch!

```python
>>> df = db.query(expensive_query)
INFO:cachesql.sql:Querying 'mydatabase'
INFO:cachesql.sql:Loading from cache. #  <--- When you run it again, it will get the data from cache
INFO:cachesql.sql:The cached query was executed on the 2021-01-03T20:06:21.401556 and lasted 0:23:04.005710s
```

Phew... that was fast! Although, I know now that there's new data on the DB so I want fresh data! -->
Use the `force=True` flag:

```python
>>> df = db.query(expensive_query, force=True) #  <--- force=True will tell cachesql to refresh the cache.
INFO:cachesql.sql:Querying 'mydatabase'
INFO:cachesql.sql:Finished in 0:23:10.023650s
INFO:cachesql.sql:Results have been stored in cache
```

Perfect, now that my report is ready to go in production, I wan't to run this once a day without
unnecessarily wasting disk space with cache -->  Use the `cache=False` flag:

```python
>>> df = db.query(expensive_query, cache=False) #  <--- For production ready code, you can turn off the cache
INFO:cachesql.sql:Querying 'mydatabase'
INFO:cachesql.sql:Finished in 0:22:43.031210s
```
You got your data and nothing is saved to cache!

**NOTE**: By default `cachesql` has logging disabled. This is to allow the user to choose within
their own environment how and when to log messages. If you want to see the log messages as in the
previous examples, add this line on top of your code:

```python
import logging
logging.basicConfig(level=logging.INFO)
```



## Rationale
Exploratory data analysis requires doing numerous iterations to test different ideas and hypothesis.
During some of these iterations, it is important to start from scratch and run your code from the
beginning to guarantee its integrity or simply refresh your environment (yep, that messy and unordered
jupyter notebook). The problem with this is that often we need to do expensive queries to get the
initial data. This poses several problems:


1. We don't want to have to wait for the data each time. Development cycles should be fast if we
   want to be efficient at our tasks. We also want to have the freedom to run from scratch our code
   as much as possible.
2. We don't want to overload our databases with the same query several times. We don't always have
   the luxury to have dedicated DBs for data analysis and sometimes have to query production DBs.
   I know, it sucks... but that's life and we want to keep a helthy relationship with whomever is
   in charge of the DBs.
3. Let's face it, we don't all work in perfect environments, infrastructure sometimes fail and we
   loose access to the databases.


To remediate this, we all end up putting this type of logic in our codes in some way or another:
```python
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://user:password@localhost:5432/mydatabase")
data_backup = Path("data_backup.parquet")
if data_backup.exists():
    df = pd.read_parquet(backup_file)
else:
    df = pd.read_sql(sql="SELECT * FROM table WHERE {conditions}", con=engine)
    df.to_parquet(data_backup)
```

Perfect! You would think... It does the trick, but it comes with its downsides:

1. If you want fresh data, you need to manually erase the backup or modify the code by hand. Not a
   big deal, but very much prone to error!
2. That is an overhead of 6 extra lines of code to add backup logic
3. Some of our reports are one shot, but some others are meant to be run in production on a regular
   basis. For those cases, you would need to erase the boilerplate backup code or to add more
   boilerplate to avoid the backup in production.
4. Some analyses require more than one query from different databases --> Multiply the previous
   boilerplate by the number of queries.


Enter `cachesql`: 

```python
from cachesql import Database
db = Database(uri="postgresql://user:password@localhost:5432/mydatabase")
df = db.query("SELECT * FROM table WHERE something in (...)")
```

The previous 10 lines are perfectly replaced by these 3 lines of code. Although those 3 lines do
not exploit `cachesql` at it's fullest. Here is a more complete example of the basic usage:


```python
from cachesql import Database

db1 = Database(uri="postgresql://user:password@localhost:5432/mydatabase")
db2 = Database(uri="sqlite:///db2.db")

def get_first_dataset(force=False, cache=True):
    df = db1.query("SELECT * FROM table WHERE {conditions}", force=force, cache=cache)
    # Do some operations on df
    ...
    return df


def get_second_dataset(force=False, cache=True):
    df = db2.query("SELECT * FROM table WHERE {conditions}", force=force, cache=cache)
    # Do some operations on df
    ...
    return df


def get_data(force=False, cache=True):
    df1 = get_first_dataset(force=force, cache=cache)
    df2 = get_second_dataset(force=force, cache=cache)
    df = df1.merge(df2, on="common_column")
    # Do some operations on df
    ...
    return df


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action='store_true')
    parser.add_argument("--no-cache", action='store_true')
    args = parser.parse_args()

    df = get_data(force=args.force, cache=not args.no_cache)

```

Suppose the previous code is stored on `report.py`. You can either import `get_data` in other modules
and control cache with `force` and `cache`parameters, or you can run the report on top of your
jupyter notebook with the magic command
[%run](https://ipython.readthedocs.io/en/stable/interactive/magics.html#magic-run) 
to populate your environment with the required data:


```jupyter
%run report.py # For running  the report and rely on the cache
```


```jupyter
%run report.py --force # Refresh the cache to get new data
```

Once you have finalized your development cycles and `report.py` has the complete logic for generating
your report, you can run it in production without cache as follows:


```bash
$ python report.py --no-cache
```


## Acknowledgements
This package was proudly developed at the [Rwanda Revenue Authority](https://www.rra.gov.rw/) which
kindly agreed to let me open source it. The internal version has been serving a growing team of data
scientists since 2018. It has been a pillar of our infrastructure to guarantee fast development
cycles, resilient workflow to infrastructure issues, reproducibility of our analysis and simplified
collaboration by sharing cache state among colleagues.
