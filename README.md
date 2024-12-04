# ODMX

ODMX Data Model

See [ODMX.org](https://odmx.org)

## Getting Started

Install the package by running pip:

`pip3 install .`

This will install the package and all dependencies.

## Installing PostgreSQL

ODMX uses exclusively PostgresSQL with Postgis extension. This library is compatible with PostgreSQL 13. It may work with other versions, but has not been fully tested.

### Mac

`brew install postgresql`

### Ubuntu

`sudo apt-get install postgresql postgresql-contrib postgis postgresql-13-postgis-3`

## Starting PostgreSQL

### Mac

`brew services start postgresql`

### Linux

`sudo service postgresql start`

Initial configuration will depend on your operating system. There are tutorials available for most setups (for example, [Ubuntu server](https://ubuntu.com/server/docs/databases-postgresql) or [Arch](https://wiki.archlinux.org/title/PostgreSQL))

## Building an example project

### Create a new work directory from example

`cp ./example_work_dir ~/odmx_workspace`

### Edit the config file for running the example

The config file is under `config.yml` in the work directory

Edit the config file, set `db_user` and `db_pass` your postgres user and password. You must
have permissions to create a database, as the workflow will automatically handle database creation

## Building the example database

`python -m odmx.pipeline /path/to/odmx_workspace --project example --from-scratch True`

The `--from-scratch True` option will drop and recreate the database, which is necessary the first time you run the pipeline.

The first run will take a while, as it will download data from public sources such as nwis and snotel

If you want more verbose output, add the `--verbose True` option.

For a list of all options, run `python -m odmx.pipeline --help`

One useful option is to skip the harvest stage which pulls data from remote:

`--data-processes ingest,process`

This will only run the ingest and process stages

Once everything runs, you can start querying the database.

Subsequent reruns without the `--from-scratch True` parameter will attempt to pull and update the database with new data

## Run and use the REST API

The REST API is a part of the odmx python package. An API instance runs against a single project:

`python -m odmx.rest_api --config /path/to/odmx_projects/config.yaml --project example`

The rest API will be running on localhost port 8000. This can be deployed to a
production server using gunicorn or similar, and can be run behind a reverse proxy
with authentication by specifying the --project-name-header and --user-id-header fields
to the rest_api module.

In addition to CLI arguments, the rest API can be configured with environment variables:
- `PROJECT`
- `PROJECT_NAME_HEADER`
- `USER_ID_HEADER`
- `ENABLE_WRITES`

You cannot specify both PROJECT and PROJECT_NAME_HEADER, as they are mutually exclusive.

### Conventions

The API follows REST conventions. There are endpoints for each object in the data model.
Parameters for each endpoint correspond to the fields in the data model, therefore understanding the data model is key to using the API.

GET requests accept query parameters for filtering, sorting, and pagination. The API returns JSON by default, but can return CSV or JSON table format.
Special parameters for controlling the API output start with an underscore `_` to distinguish them from data model fields.

When the API has writes enabled, POST, PUT, and DELETE requests can be used to create, update, and delete objects in the database.

### Run some queries using CURL

These are some examples using CURL. They are easily translated to python requests,
or any other HTTP client such as javascript's fetch. We can use `python -m json.tool`
to format the output for readability.


There are a few special endpoints that are not part of the data model, such as
`/api/odmx/v3/datastream_data`, which returns data for a datastream. Additional
special endpoints are WIP for simplifying some kinds of queries such as those
involving sample data

Full documentation of the API is forthcoming.

#### Data Model Query Examples

##### Get all CV Units:
```sh
curl  localhost:8000/api/odmx/v3/cv_units | python -m json.tool
```

##### Get a CV Unit by ID:
```sh
curl  localhost:8000/api/odmx/v3/cv_units/1 | python -m json.tool
```

##### Get all sampling features:
```sh
curl  localhost:8000/api/odmx/v3/sampling_features | python -m json.tool
```

##### Get a sampling feature by ID:
```sh
curl  localhost:8000/api/odmx/v3/sampling_features/1 | python -m json.tool
```

##### Get a sampling feature list by many IDs:
```sh
curl  localhost:8000/api/odmx/v3/sampling_features/1,2,3 | python -m json.tool
```

##### Query sampling features by code:
```sh
curl  localhost:8000/api/odmx/v3/sampling_features\?sampling_feature_code=ER
```

##### Do a fuzzy search on sampling feature code:
```sh
curl  localhost:8000/api/odmx/v3/sampling_features\?sampling_feature_code=ER\&_fuzzy
```

##### Only return certain fields:
```sh
curl  localhost:8000/api/odmx/v3/sampling_features\?sampling_feature_code=ER\&_fuzzy\&_cols=sampling_feature_code,sampling_feature_name
```

##### Specifying single fields returns flat list by default:

Rather than having many entries with a single key, if you specify a single
column there will be a flat list returned.

```sh
curl  localhost:8000/api/odmx/v3/sampling_features\?sampling_feature_code=ER\&_fuzzy\&_cols=sampling_feature_code
```

##### Force JSON object list format, which is the default for multiple columns, for single column

If you always want to have the same return format regardless, simply explicitly specify it

```sh
curl  localhost:8000/api/odmx/v3/sampling_features\?sampling_feature_code=ER\&_fuzzy\&_cols=sampling_feature_codes\&_format=json_obj_list
```

##### Return as CSV

CSVs are more easily readable with spreadsheet software, but are also better for
streaming data while processing it.

```sh
curl  localhost:8000/api/odmx/v3/sampling_features\?sampling_feature_code=ER\&_fuzzy\&_cols=sampling_feature_code,sampling_feature_name\&_format=csv
```

##### Return as JSON table (with headers and rows keys)

The JSON table format is a more space efficient method of returning data.

It consists of an object with three keys: `headers`, `rows`, and `row_count`

Headers contains a list of N columns for the return, and rows contains a list of
lists each of size N

It's a matter of associating the column with the index of the header to know
what the data is.

```sh
curl  localhost:8000/api/odmx/v3/sampling_features\?sampling_feature_code=ER\&_fuzzy\&_cols=sampling_feature_code,sampling_feature_name\&_format=json_table
```


##### Using limit and offset for pagination

Some requests may return a large number of results. You can use the `_limit` and `_offset` parameters to paginate the results.

The resulting response will be a window of the results, starting at `_offset` and ending at `_offset + _limit`. The `row_count` key in the response will tell you how many total rows there are which can be used to calculate the number of pages.

```sh
curl  localhost:8000/api/odmx/v3/sampling_features\?_limit=10\&_offset=0\&_format=json_table
```

You may also use `_limit = 0` with the `json_table` format to determine the number of rows that would be returned without actually returning any data.

```sh
curl  localhost:8000/api/odmx/v3/sampling_features\?_limit=0\&_format=json_table
```

#### Querying Timeseries Datastreams

Timeseries data in ODMX is handled by a concept called datastreams.

Here are some examples of querying data

##### Find all variables ids that have datastreams:

Here we are searching for all variables in the system that have data in a
datastream

```sh
curl http://localhost:8000/api/odmx/v3/sampling_feature_timeseries_datastreams\?_cols=variable_id\&_distinct
```
```json
[1, 2, 35, 3, 37, 39, 563, 53, 54, 55, 22, 125]
```

##### Find all sampling feature IDs that have datastreams:

Here are all sampling features which have associated datastreams

```sh
curl http://localhost:8000/api/odmx/v3/sampling_feature_timeseries_datastreams\?_cols=sampling_feature_id\&_distinct
```

```json
[1, 3, 293, 5, 7, 9, 213, 215]
```

##### Find variables associated with a sampling feature:

Here we can use the datastreams table to find variables that are associated
with a specific sampling feature

```sh
curl http://localhost:8000/api/odmx/v3/sampling_feature_timeseries_datastreams\?sampling_feature_id=213\&_cols=variable_id\&_distinct
```
```json
[1, 563, 54, 55, 125]
```

#### Datastream Queries Examples

Here we will do a step by step for querying air temperature data

Find the variable id associated with air temperature. We know that the controlled
vocabulary for this is airTemperature, so query that:

```sh
curl http://localhost:8000/api/odmx/v3/variables\?variable_term=airTemperature\&_cols=variable_id
```
```json
[1]
```

We know the variable_id we need is 1

##### Find all datastreams associated with the variable

Let's figure out what datastreams are available:

```sh
curl http://localhost:8000/api/odmx/v3/sampling_feature_timeseries_datastreams\?variable_id=1\&_cols=sampling_feature_id
```
```json
[12,18,25,30,35]
```


##### Inspect the first datastream

```sh
curl http://localhost:8000/api/odmx/v3/datastreams/35 | python -m json.tool
```

```json
{
    "sampling_feature_id": 7,
    "datastream_type": "physicalsensor",
    "variable_id": 1,
    "units_id": 265,
    "datastream_tablename": "snotel_680_co_sntl_temperature[degf]_meas",
    "datastream_id": 35,
    "datastream_uuid": "98136efb-abba-43db-9776-d755b7fb6f40",
    "equipment_id": 8,
    "datastream_database": "datastream",
    "first_measurement_date": "2003-07-15T20:00:00",
    "last_measurement_date": "2023-05-14T13:00:00",
    "total_measurement_numbers": 147010,
    "datastream_attribute": "{\"sensor_elevation\": 0.0, \"sensor_elevation_units\": \"meter\", \"instrument\": \"unknown sensor\"}",
    "datastream_access_level": null,
    "datastream_source_category": null,
    "datastream_classifier": null
}
```

##### Inspect associated sampling feature

```sh
curl http://localhost:8000/api/odmx/v3/sampling_features/7 | python -m json.tool
```

```json
{
    "sampling_feature_uuid": "dc6b5fc5-38c8-4eca-9b12-3c7e537fee62",
    "sampling_feature_type_cv": "site",
    "sampling_feature_code": "NRCS-680",
    "sampling_feature_id": 7,
    "sampling_feature_name": "snotel_park_cone",
    "sampling_feature_description": "NRCS SNOTEL station. See: https://wcc.sc.egov.usda.gov/nwcc/site?sitenum=680",
    "sampling_feature_geotype_cv": "point",
    "feature_geometry": "0101000000295C8FC2F5684340E09C11A5BDA55AC0",
    "feature_geometry_wkt": "POINT(38.82 -106.5897)",
    "elevation_m": 2923.57,
    "elevation_datum_cv": "NAVD88",
    "latitude": 38.82,
    "longitude": -106.5897,
    "epsg": null
}
```

So we can see that this is public snotel data from a site in colorado.

Let's look at its air temperature data

##### Query all the data from the datastream (this can be a lot of data)

If you're feeling up to it, pull all the data

```sh
curl http://localhost:8000/api/odmx/v3/datastream_data/35 | python -m json.tool
```

```json
    ],
    [
        "2023-05-15T01:00:00",
        8.6,
        "z"
    ],
    [
        "2023-05-15T02:00:00",
        6.5,
        "z"
    ],
    [
        "2023-05-15T03:00:00",
        6.7,
        "z"
    ],
    [
        "2023-05-15T04:00:00",
        5.4,
        "z"
    ]
]
```

The standard data return is the following
- Timestamp in UTC (see below for timezone information
- The data value
- The qa/qc flag

##### qa/qc flags

qa/qc flags set on data after going through a qa/qc process.

You can query the flags under `cv_quality_code`

```sh
curl http://localhost:8000/api/odmx/v3/cv_quality_code | python -m json.tool
```

```json
[
    {
        "term": "NaN",
        "name": "Not a Number",
        "definition": "Data reported as not a number.",
        "qa_flag": "c",
        "category": null,
        "provenance": null,
        "provenance_uri": null,
        "note": null
    },
    ...
    {
        "term": "marginal",
        "name": "Marginal",
        "definition": "Data has been manually marked as marginal.",
        "qa_flag": "w",
        "category": null,
        "provenance": null,
        "provenance_uri": null,
        "note": null
    },
    {
        "term": "good",
        "name": "Good",
        "definition": "Data passed all quality assessment tests.",
        "qa_flag": "z",
        "category": null,
        "provenance": null,
        "provenance_uri": null,
        "note": null
    }
]
```

By default, queries only return data marked with 'z'. There are two parameters
that control this behavior:

- `qa_flag` : The flag we want to query for, default is Z
- `qa_flag_mode`: One of `greater_or_eq`, `less_or_eq` or `equal`, default is `greater_or_eq`

Thus, if you want to query specifically for data that isn't good:

```sh
curl http://localhost:8000/api/odmx/v3/datastream_data/35\?qa_flag=x\&qa_flag_mode=less_or_eq | python -m json.tool
```

```json
    [
        "2023-03-28T12:00:00",
        null,
        "c"
    ],
    [
        "2023-03-28T13:00:00",
        null,
        "c"
    ],
    [
        "2023-03-28T14:00:00",
        null,
        "c"
    ],
    [
        "2023-03-28T15:00:00",
        null,
        "c"
    ],
    [
        "2023-04-06T13:00:00",
        null,
        "c"
    ]
]
```

As you can see from the `cv_quality_codes`, these are values that were marked
as NaN. They are returned here in the form of `null` because JSON has no native
support for NaN

For a CSV return, null values are empty cells

##### Query datastream data restricted by date range

```sh
curl http://localhost:8000/api/odmx/v3/datastream_data/35\?start_date=2010-01-01\&end_date=2012-01-02
```

##### If you need finer time ranges, you can use the `start_datetime` and `end_datetime` parameters

```sh
curl http://localhost:8000/api/odmx/v3/datastream_data/35\?start_datetime=2010-01-01T00:00:00\&end_datetime=2010-01-01T12:00:00
```

##### Query datastream downsampled data

It's often important or necessary to downsample data for usage with visualization
and similar tools. For this, there are two controlling parameters:

`downsample_interval` can be 'hour', 'day', 'week', 'month', 'year', or a number of seconds

`downsample_method` can be 'mean', 'sum', 'count', 'stddev', 'variance', 'min', 'max', 'min_max'

```sh
curl http://localhost:8000/api/odmx/v3/datastream_data/35\?start_date=2010-01-01\&end_date=2010-01-02\&downsample_interval=day\&downsample_method=mean
```

The min, max, `min_max` methods have extra columns for the specific datetime, for example, the
min_max method will return the min and max values and the datetimes they occurred at for each interval.
See the examples below

#### Query with specific timezone

All data stored in the database is in UTC.  The default is to return the data in UTC.  You can specify a different
timezone by using the `tz` parameter.  The timezone parameter can be any timezone supported by postgresql, for example,
`America/Denver` or `PST` or an offset from UTC, for example, `UTC+7`

This affects the downsampled data for intervals that are not UTC aligned, for example, if you downsample to a day, the
day will be in the specified timezone.

```sh
curl http://localhost:8000/api/odmx/v3/datastream_data/35\?start_date=2010-01-01\&end_date=2010-01-02\&downsample_interval=day&downsample_method=mean&tz=America/Denver
```

##### Let's find out the min/max temperature for each year and what day it occurred on

```sh
curl http://localhost:8000/api/odmx/v3/datastream_data/35\?downsample_interval=year\&downsample_method=min_max | python -m json.tool
```

Notice that with min_max we have 5 entries:

- Interval Timestamp
- Min Timestamp
- Min Value
- Max Timestamp
- Max Value

```json
[
    ...
    [
        "2019-01-01T00:00:00",
        "2019-12-31T15:00:00",
        -33.4,
        "2019-07-11T21:00:00",
        24.7
    ],
    [
        "2020-01-01T00:00:00",
        "2020-02-05T16:00:00",
        -32.2,
        "2020-08-19T21:00:00",
        26.5
    ],
    [
        "2021-01-01T00:00:00",
        "2021-01-11T14:00:00",
        -28.9,
        "2021-06-15T21:00:00",
        28.1
    ],
    [
        "2022-01-01T00:00:00",
        "2022-02-03T15:00:00",
        -33.8,
        "2022-07-09T20:00:00",
        25.6
    ],
    [
        "2023-01-01T00:00:00",
        "2023-02-10T14:00:00",
        -29.4,
        "2023-05-01T19:00:00",
        16.8
    ]
]
```


##### Let's find the min/max for each day in the America/Denver timezone

Here we do the min/max for a few days.

Let see if it passes a vibe check for where we expect it to see daily temperature swings in october in Colorado

```sh
curl http://localhost:8000/api/odmx/v3/datastream_data/35\?&start_date=2022-10-01\&end_date=2022-10-10\&downsample_interval=day\&downsample_method=min_max\&tz=America/Denver | python -m json.tool
```


```json
[
    [
        "2022-10-01T00:00:00",
        "2022-10-01T07:00:00",
        3.2,
        "2022-10-01T13:00:00",
        9.8
    ],
    [
        "2022-10-02T00:00:00",
        "2022-10-02T00:00:00",
        3.7,
        "2022-10-02T16:00:00",
        8.1
    ],
    [
        "2022-10-03T00:00:00",
        "2022-10-03T23:00:00",
        2.8,
        "2022-10-03T14:00:00",
        11.7
    ]
]
```

Seems plausible

##### Getting full output precision

By default, data returns favor convience of short values over accuracy under the
assumption that the precision of the raw data is not high enough to warrant
full decimal representation to the nearest floating point value

If you need full representation of the floating point output, you can
pass the `full_precision` flag. Following the previous example:

```sh
curl http://localhost:8000/api/odmx/v3/datastream_data/35\?start_date=2022-10-01\&end_date=2022-10-03\&downsample_interval=day\&downsample_method=min_max\&tz=America/Denver\&full_precision | python -m json.too
```

```json
[
    [
        "2022-10-01T00:00:00",
        "2022-10-01T07:00:00",
        3.1999999999999957,
        "2022-10-01T13:00:00",
        9.799999999999992
    ],
    [
        "2022-10-02T00:00:00",
        "2022-10-02T00:00:00",
        3.6999999999999944,
        "2022-10-02T16:00:00",
        8.09999999999999
    ],
    [
        "2022-10-03T00:00:00",
        "2022-10-03T23:00:00",
        2.7999999999999967,
        "2022-10-03T14:00:00",
        11.69999999999999
    ]
]
```

These unwieldy decimal values are closer to the raw binary representation stored
in the database, but remember that the raw values ingested may not have
been this precise to begin with, and some downsampling operations
perform calculations that produce meaningless precision.

##### Getting CSV returns

As with the standard model endpoints, you can retrieve the data as a CSV with
the `format` parameter. The `format` parameter can be set to `csv` or `json`
for datastreams:

```sh
curl http://localhost:8000/api/odmx/v3/datastream_data/35\?downsample_interval=year\&downsample_method=min_max\&format=csv
```

```csv
truncated_datetime_local,min_datetime_local,min_data_value,max_datetime_local,max_data_value
2003-01-01 00:00:00,2003-12-29 14:00:00,-30.0,2003-07-18 20:00:00,31.1
2004-01-01 00:00:00,2004-01-05 11:00:00,-34.2,2004-07-12 20:00:00,28.1
2005-01-01 00:00:00,2005-12-15 15:00:00,-33.7,2005-07-13 20:00:00,28.7
2006-01-01 00:00:00,2006-01-21 15:00:00,-32.4,2006-07-16 21:00:00,28.5
2007-01-01 00:00:00,2007-12-28 15:00:00,-33.8,2007-07-03 21:00:00,27.6
2008-01-01 00:00:00,2008-01-01 15:00:00,-35.3,2008-06-17 08:00:00,11.6
2009-01-01 00:00:00,2009-12-11 14:00:00,-31.4,2009-09-27 22:00:00,21.0
2010-01-01 00:00:00,2010-01-08 13:00:00,-30.5,2010-07-16 22:00:00,26.5
2011-01-01 00:00:00,2011-01-01 15:00:00,-35.3,2011-07-17 21:00:00,25.8
2012-01-01 00:00:00,2012-12-20 14:00:00,-32.5,2012-06-24 19:00:00,27.0
2013-01-01 00:00:00,2013-01-15 11:00:00,-34.8,2013-06-27 22:00:00,27.4
2014-01-01 00:00:00,2014-02-02 15:00:00,-29.2,2014-07-22 22:00:00,26.9
2015-01-01 00:00:00,2015-12-27 14:00:00,-27.9,2015-06-30 19:00:00,25.7
2016-01-01 00:00:00,2016-01-01 15:00:00,-33.1,2016-06-20 22:00:00,28.0
2017-01-01 00:00:00,2017-01-27 15:00:00,-34.0,2017-06-20 21:00:00,25.2
2018-01-01 00:00:00,2018-02-21 15:00:00,-29.1,2018-07-22 20:00:00,26.3
2019-01-01 00:00:00,2019-12-31 15:00:00,-33.4,2019-07-11 21:00:00,24.7
2020-01-01 00:00:00,2020-02-05 16:00:00,-32.2,2020-08-19 21:00:00,26.5
2021-01-01 00:00:00,2021-01-11 14:00:00,-28.9,2021-06-15 21:00:00,28.1
2022-01-01 00:00:00,2022-02-03 15:00:00,-33.8,2022-07-09 20:00:00,25.6
2023-01-01 00:00:00,2023-02-10 14:00:00,-29.4,2023-05-01 19:00:00,16.8
```

####  Data Model Create/Update/Delete examples

The default REST API allows for updates to the database. This may not be
desirable, so you can disable this with `--enable-writes false`
CLI argument to the rest\_api module above.

##### Create a new sampling feature:

Following REST conventions, you can create a new ODMX entity with POST requests

Most entities will have required values. They must be specified or else you
will get 400 bad request.

The return value is the ID of the newly created entity.

```sh
curl -X POST -H "Content-Type: application/json" -d '{"sampling_feature_code":"TEST","sampling_feature_name":"Test Sampling Feature","sampling_feature_uuid":"foo","sampling_feature_type_cv": "site"}' localhost:8000/api/odmx/v3/sampling_features
```

```json
1234
```

##### Update a sampling feature:

Also following REST conventions, PUT will update an existing entity.

```
curl -X PUT -H "Content-Type: application/json" -d '{"sampling_feature_name":"Test Sampling Feature Updated"}' localhost:8000/api/odmx/v3/sampling_features/1234
```

##### Delete a sampling feature:

Deleting an entity removes it from the system.

```
curl -X DELETE localhost:8000/api/odmx/v3/sampling_features/1
```

## Using the ODMX Python API

The ODMX Python API is a simple database wrapper based around psycopg3. It has
type checking and some basic validation. It is not a full ORM, but it is very
easy to use.

### Connecting to the database

```python
from odmx.support.db import db
import odmx.data_model as odmx
con = db.connect(
  db_name='odmx_example',
  db_user='odmx',
  db_pass='odmx'
)

db.set_current_schema(con, 'odmx')

# Get a sampling feature by ID
sf = odmx.read_sampling_features_by_id(con, 1)

# Get a sampling feature by code

sf = odmx.read_sampling_features(con,
  sampling_feature_code='ER')

# Iterate over all sampling features
sfs = odmx.read_sampling_features(con)
# Note that this is a generator, so it will not be evaluated until you iterate over it.
# This means that you can iterate over a large number of sampling features without
# loading them all into memory at once.
for sf in sfs:
  print(sf.sampling_feature_code)

# If your IDE supports autocomplete, you can see all the fields available on the sampling feature
# by typing sf. and seeing the list of fields that pops up.

# You can also see what fields are available for queries and writes:

odmx.write_sampling_features(con, sampling_feature_code='TEST', sampling_feature_name='Test Sampling Feature', sampling_feature_type_id=1)

# You can also update a sampling feature by specifying ID:

odmx.write_sampling_features(con, sampling_feature_id=1, sampling_feature_code='TEST', sampling_feature_name='Test Sampling Feature Updated', sampling_feature_type_id=1)

# There are also corresponding classes for all the other tables in the database, such as cv_units, cv_variable_types, etc:

cv_unit = odmx.CvUnit(
  cv_unit_name='Meters',
  cv_unit_abbreviation='m'
)

odmx.write_cv_unit_obj(con, cv_unit)

# The DB modesl are type checked using beartype for simple validation

# There are many helper functions in the odmx.support.db module such as
# dumping to CSV and copying tables efficiently

# con is a psycopg3 connection, so it supports all of the psycopg3 concepts such
# as explicit transactions to do complicated DB operations atomically:

with con.transaction():
   odmx.do_something()


```

## Snapshot the database into new project directory

It is often desirable to snapshot an ODMX project that is live, this allows retaining entities through `--from-scratch True` and
provides a degree of reproducability and provinance. The project's base tables and CV entries could, for example, be tracked in a git repository.

If you wish to overwrite an existing project directory and have backups or version control, you can pass the `--overwrite True` parameter
and set the output dir to that of the project directory

```
python -m odmx.snapshot --config /path/to/odmx_projects/config.yaml --output-dir /path/to/odmx_projects/example_snapshot --project example --include-feeder True
```

If you pass `--include-feeder True` then a snapshot of the feeder tables (which are data ingested into the database from various data sources) are also
dumped under the `feeder` directory as CSV files.


# Acknowledgements

This material is based upon work supported by the U.S. Department of Energy, Office of Science, Office of Biological and Environmental Research SBIR program under Awards Number DE-SC-0009732 and DE-SC-0018447.

Additional development was supported under DOE SBIR Award DE-SC0024850 to Subsurface Insights and support from DOE is gratefully acknowledged.
