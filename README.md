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

**NOTE**: By default `cachesql` has logging disabled. This is to allow the user to choose within
their own environment how and when to log messages. If you want to see the log messages as in the
following examples, add these lines on top of your code:


```python
import logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
```



Run your queries once, get them from cache the next time!

```pycon
>>> from cachesql import Database
>>> db = Database(uri="postgresql://user:password@localhost:5432/mydatabase")
>>> expensive_query = "SELECT * FROM table WHERE {conditions}" #  <--- Imagine this is a very long and expensive query.
>>> df = db.query(expensive_query)
Querying 'mydatabase'
Finished in 0:23:04.005710s
Results have been stored in cache
```

Ok, that took 23 minutes, but I need to run my code again from scratch!

```pycon
>>> df = db.query(expensive_query)
Querying 'mydatabase'
Loading from cache. #  <--- When you run it again, it will get the data from cache
The cached query was executed on the 2021-01-03T20:06:21.401556 and lasted 0:23:04.005710s
```

Phew... that was fast! Although, I know now that there's new data on the DB, so I want fresh data! -->
Use the `force=True` flag:

```pycon
>>> df = db.query(expensive_query, force=True) #  <--- force=True will tell cachesql to refresh the cache.
Querying 'mydatabase'
Finished in 0:23:10.023650s
Results have been stored in cache
```

Perfect, now that my report is ready to go in production, I want to run this once a day without
unnecessarily wasting disk space with cache -->  Use the `cache=False` flag:

```pycon
>>> df = db.query(expensive_query, cache=False) #  <--- For production ready code, you can turn off the cache
Querying 'mydatabase'
Finished in 0:22:43.031210s
```
You got your data and nothing is saved to cache!



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
   I know, it sucks... but that's life, and we want to keep a healthy relationship with whomever is
   in charge of the DBs.
3. Let's face it, we don't all work in perfect environments, infrastructure sometimes fail, and we
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

## Tell me more!

### Where is my data stored?

The folder where the cache is stored is controlled by three parameters of the `Database` object:

- `name`: This is a name allocated to the database used as namespace for the cache and to identify the database on the log messages. If not provided, it will try to infer a name from the `uri`, otherwise it will be set to `unnameddb`.


- `cache_store`: The root folder of the cache. The default value is `.cache` on the current working directory.

- `store_backend`: The serializer used to dump DataFrames to cache. It is `parquet` by default and can also take the value `joblib`. See below for an explanation on how to choose the backend.

Your cache will be therefore located at `/{cache_store}/{name}/{store_backend}`.  You can access the location of your cache with the attribute `Database.cache.cache_store`. Here are some examples for different parameters

```pycon
>>> db = Database(uri="sqlite:///db2.db", name="mydb")
>>> db.cache.cache_store
PosixPath('/home/······/····/.cache/mydb/parquet')
```

```pycon
>>> db = Database(uri="sqlite:///db2.db", name="mydb", cache_store="/tmp", store_backend="joblib")
>>> db.cache.cache_store
PosixPath('/tmp/mydb/joblib')
```
### How can I share my cache state?


The current state of your cache can be exported with the method `Database.cache.export` which takes two arguments: 

- `filename`: The file path where you want to export the cache as a zipfile

- `queries`: If you only want to export a subset of the cache, you can use this parameter to restrict the list of queries that you want to include.

For example:

```pycon
>>> db = Database(uri="sqlite:///db2.db")
>>> db.cache.export("mycache.zip")
```
Once you have exported the cache, it can be imported in any other environment of by one of your colleagues using the `Database.cache.import_cache`, e.g.:

```pycon
>>> db = Database(uri="sqlite:///db2.db")
>>> db.cache.import_cache("mycache.zip")
```

Another useful way of exporting the cache is to restrict it to one runtime session. For this, there is the `Database.export_session` method!
Internally, the `Database` instance keeps track of what queries were run during a runtime session on the `Database.session` attribute.
The `.export_session` method will restrict the cache export to only those queries. This is particularly useful when you want to share only the cache that is relevant for a piece of code. This will guarantee lighter export files restricted to the relevant scope.

```pycon
>>> db = Database(uri="sqlite:///db2.db")
>>> df = db.query("SELECT * FROM table WHERE {conditions}")
>>> db.export_session("mycache.zip")
```

The previous code will only export the cache contents related to the query `SELECT * FROM table WHERE {conditions}` ignoring everything else that is present on cache.

This last method can be used to guarantee reproducibility and simplify collaboration. By sharing the cache related to the scope of a given project, you guarantee that whoever runs the project again will get the same results.

### Choosing your data serializer backend

`cachesql` relies on data serializers to dump DataFrames to the cache:

- Parquet: This is used by default as it is a fast, efficient and reliable serializer. It works through the [pyarrow](https://pypi.org/project/pyarrow/) library. It is important to know that `pyarrow` has some limitations on the type of objects it can serialize, e.g., it doesn not know how to serialize UUID data type.

- Joblib: This is the most robust alternative as it can serialize any arbitrary python object, therefore, it can be used as an alternative to parquet when you are dealing with data types unknown to the `pyarrow` ecosystem. This relies on the [joblib](https://joblib.readthedocs.io/en/latest/) library. You can choose this backend with the `store_backend` parameter of the `Database` object as follows:

```pycon
>>> db = Database(uri="sqlite:///db2.db", store_backend="joblib")
```

The `Database` object takes an optional parameter `compression` that is passed on to the serializers. Please refer to the documentation of `pandas.DataFrame.to_parquet` or `joblib.dump` for details on how this can be tweaked.

### CacheSQL is resilient to differences on query formats!

The cache mechanism is based on a notion of unicity of a query that is independent of the format.
It achieves this by using [sqlparse](https://github.com/andialbrecht/sqlparse) on the background to normalize the format of the queries.
As an example, the two following queries will both point to the same cached result:

```sql
select * from table
```

```sql
SELECT *
FROM TABLE
```

This provides extra liberty on your development cycles for you to change the formatting of your queries without invalidating the cache.
The normalization can be turned off with the `normalize` argument as follows:

```pycon
>>> db = Database(uri="sqlite:///db2.db", normalize=False)
```

## Extending the library
Section to be filled in

- How to create new serializers.
- How to create different cache stores.


## Acknowledgements
This package was proudly developed at the [Rwanda Revenue Authority](https://www.rra.gov.rw/) which
kindly agreed to let me open source it. The internal version has been serving a growing team of data
scientists since 2018. It has been a pillar of our infrastructure to guarantee fast development
cycles, resilient workflow to infrastructure issues, reproducibility of our analysis and simplified
collaboration by sharing cache state among colleagues.
