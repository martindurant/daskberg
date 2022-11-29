# Python client for iceberg

## Installation

For now, you either need to clone the repo, or install directly from
github
```commandline
pip install git+https://github.com/martindurant/daskberg
```

This software is not released and very poorly tested. With luck, you might find
it useful. 

## Quickstart

Let's say you have access to an Iceberg dataset at a known path. The "root"
path of an Iceberg dataset is the one above the "metadata/" and "data/"
directories. In this quickstart, I will demo with the test data included in this
repo, so assume you are in the root directory of the repo.

```python
In [14]: ORIG_DIR = "/Users/mdurant/temp/warehouse/db/my_table"
In [15]: ice = daskberg.ice.IcebergDataset("./test-data/my_table/", ORIG_DIR)

In [16]: ice.version  # latest version file found
Out[16]: 5

In [17]: ice.schema
Out[17]:
[{'id': 1, 'name': 'name', 'required': False, 'type': 'string'},
 {'id': 2, 'name': 'age', 'required': False, 'type': 'int'},
 {'id': 3, 'name': 'email', 'required': False, 'type': 'string'}]

In [18]: len(ice.snapshots)
Out[18]: 3

In [19]: ice.read()
Out[19]:
Dask DataFrame Structure:
                 name    age   email
npartitions=5
               object  Int32  object
                  ...    ...     ...
...               ...    ...     ...
                  ...    ...     ...
                  ...    ...     ...
Dask Name: read-parquet, 1 graph layer

In [20]: ice.read().compute()
Out[20]:
    name  age              email
0    Bob   20               None
0   John   56  email@email.email
0  Fiona   25               None
0  Roger   25               None
0   Alex   36               None

In [21]: ice.open_snapshot(-1)

In [22]: ice.read().compute()
Out[22]:
    name  age
0    Bob   20
0  Fiona   25
0  Roger   25
0   Alex   36
```

Some notes:
- the data were created in a different location to where they are now found. Iceberg
 doesn't normally allow you to do this, but we can correct for it with ORIGIN_DIR.
- We can introspect the schema and any partitioning without touching any data files
- we create dask dataframes by default, and you can use these on a distributed clister
 if all the workers can access the data files
- You can move to different snapshots. Here we went one step back in time. See how the
 schema changed.
- Reading from any location supported by ``fsspec`` is allowed.

(the data were created with pyspark and following a Dremio community
[tutorial](https://www.dremio.com/subsurface/introduction-to-apache-iceberg-using-spark/))

### What works

- most data types
- filtering (meaning you don't load data files or even manifest files)
- derived partitions
- some basic operations with the REST iceberg service, particularly to find the
 current metadata file's location for some table

Testing was mostly done with fastparquet, which newly supports schema evolution.

### Missing

- any writing at all
- we do not make use of much of the available metadata, as dask's API was not built
 thinking you might already have such information.

#### Instructions for running the local REST server

- clone https://github.com/tabular-io/iceberg-rest-image
- build with ``gradle``
- run ``docker build -t ice .`` in that directory

I also posted images at dockerhub: `mdurant/ice`.
