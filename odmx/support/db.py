#!/usr/bin/env python3

"""
Convenience functions for handling DB operations as well as generating DB
classes based on existing DB schemas

Authors: Erek Alper, Doug Johnson
"""

import os
from beartype.typing import Union, List, Dict, Optional, Any, TextIO, Sequence, Tuple
from beartype import beartype
import warnings
import difflib
import tempfile
import pandas as pd
from pandas import DataFrame
import datetime
import psycopg
from psycopg import Cursor
import psycopg.sql
from psycopg.sql import SQL, Identifier, Literal
import psycopg.rows
from odmx.support.config import Config
import argparse
import json
from contextlib import contextmanager
import re

class ListDict:
    """
    This is an object which mimmicks both a list and a dict by allowing the
    user to access values by index or by key. This is useful for db returns
    """
    def __init__(self, values: Sequence[Any], fields_pos: Dict[str, int]):
        self.values = values
        self.fields_pos = fields_pos

    def __getitem__(self, key: Union[str, int]) -> Any:
        if isinstance(key, str):
            return self.values[self.fields_pos[key]]
        return self.values[key]

    def as_dict(self) -> Dict[str, Any]:
        return {k: self.values[v] for k, v in self.fields_pos.items()}

    def to_dict(self) -> Dict[str, Any]:
        """
        Alias of as_dict for consistency
        """
        return self.as_dict()

    def __iter__(self):
        return iter(self.values)

    def __iteritems__(self):
        return iter(self.as_dict().items())

    def to_list(self) -> List[str]:
        return list(self.values)

    def to_tuple(self) -> Tuple[str]:
        return tuple(self.values)

    def to_json(self, as_list=False) -> str:
        # Deal with datetime.datetime and
        # datetime.date objects
        def json_serial(obj):
            """JSON serializer for objects not serializable by default json code"""
            if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
                return obj.isoformat()
            return json.encoder.JSONEncoder.default(obj)
        if as_list:
            return json.dumps(self.to_list(), default=json_serial)
        else:
            return json.dumps(self.as_dict(), default=json_serial)

    def to_json_list(self) -> str:
        return self.to_json(as_list=True)



class ListDictFactory:
    def __init__(self, cursor: Cursor[Any]):
        if cursor.description is None:
            self.fields = []
            self.field_pos = {}
        else:
            self.fields = [c.name for c in cursor.description]
            self.field_pos = {c.name: i for i, c in enumerate(cursor.description)}

    def __call__(self, values: Sequence[Any]) -> ListDict:
        return ListDict(values, self.field_pos)


class NoResultsFound(Exception):
    pass

class MultipleResultsFound(Exception):
    pass

info = lambda *_: None



@beartype
def set_verbose(is_verbose: bool) -> None:
    """
    Sets whether the sql module should print information about what it's
    doing. This value is set to false by default, but can be set by the
    environment using SSI_SQL_VERBOSE variable
    @is_verbose Boolean true or false to enable verbosity.
    """
    # Set info function based on verbose setting.
    # TODO Switch to logging module
    # pylint: disable=global-statement
    global info
    if is_verbose:
        info = print
    else:
        info = lambda *_: None

verbose = os.getenv("SSI_SQL_VERBOSE")
if verbose is not None:
    set_verbose(verbose.strip().lower() in ("true", "1"))
    # THis wwill only print if set verbose was true
    info("Verbose logging enabled")

Connection = psycopg.Connection
Transaction = psycopg.Transaction

@beartype
def add_db_parameters_to_config(config: Config,
                                prefix: Optional[str] = None,
                                add_db_schema: bool = False,
                                add_db_name: bool = False):
    """
    Add DB Parameter definitions to a config object for the user to specify or
    override. These are all optional in which case we don't pass these into the
    database connection

    Sometimes we want to specify in the configuration the database name and the
    database schema (for postgres) as well as the database type, however, most
    of the time we do not as these are dynamically generated or hard coded by
    the application (for example, ODMX does not support mysql so anything
    dealing with that shouldn't allow a configuration for database type)

    @param config The odmx.support.config.Config object to which standard database
    configuration keys will be added.

    @param prefix A configuration prefix, for example 'ert' which is added to
    the beginning of the database parameters for 'ert_db_user' etc.

    @param add_db_schema Whether to add the db schema key
    @param add_db_name Whether to add the db name key

    """
    if not prefix:
        prefix = ""
        db_help_name = ""
    else:
        if not prefix.endswith("_"):
            prefix += "_"
        db_help_name = prefix.rstrip('_') + " "
    config.add_config_param(
        prefix + 'db_user',
        help=f"The {db_help_name}database user to authenticate with",
    )
    config.add_config_param(
        prefix + 'db_pass',
        help=f"The {db_help_name}database password to authenticate with",
    )
    config.add_config_param(
        prefix + 'db_port',
        optional=True,
        help=f"The port of the {db_help_name}database")
    config.add_config_param(
        prefix + 'db_host',
        help=f"The host of the {db_help_name}database")
    if add_db_schema:
        config.add_config_param(
            prefix + 'db_schema',
            optional=True,
            help=f"The {db_help_name}database schema"
        )
    if add_db_name:
        config.add_config_param(
            prefix + 'db_name',
            optional=True,
            help=f"The {db_help_name}database name"
        )

@beartype
def connect(
        config: Optional[Config] = None,
        config_prefix: Optional[str] = None,
        db_user: Optional[str] = None,
        db_pass: Optional[str] = None,
        db_name: Optional[str] = None,
        db_host: Optional[str] = None,
        db_port: Optional[int] = None,
        db_schema: Optional[str] = None,
        additional_args: Optional[dict] = None) -> Connection:
    """
    Creates a connection to the database using the psycopg library. The
    connection is set to autocommit mode by default to mimmick the behavior of
    SQL Alchemy. This can be overridden by passing in additional arguments.
    The other thing that you can do is use explicit transactions with
    with con.transaction():

    @param config An odmx.support.config Config object containing DB config information
    @param config_prefix The prefix to the configuration options. This is useful
        for sharing multiple db configurations in the same configuration space.
    @param db_user An override to the user found in the config.
    @param db_pass An override to the password found in the config.
    @param db_name An override to the database name found in the config.
    @param db_host An override to the host name found in the config.
    @param db_port An override to the port number found in the config.
    @param db_schema The main schema of the DB
    @param additional_args Additional arguments to the psycopg2 connect function
    @return A connnection to the database
    """

    if config:
        if not config_prefix:
            config_prefix = ""
        elif not config_prefix.endswith('_'):
            config_prefix = f"{config_prefix}_"
        db_user = db_user or config.get(config_prefix + 'db_user')
        db_pass = db_pass or config.get(config_prefix + 'db_pass')
        db_name = db_name or config.get(config_prefix + 'db_name')
        db_host = db_host or config.get(config_prefix + 'db_host')
        db_port = db_port or config.get(config_prefix + 'db_port')
        db_schema = db_schema or config.get(config_prefix + 'db_schema')
    db_string = ""
    if db_user:
        db_string += f"user={db_user} "
    if db_pass:
        db_string += f"password={db_pass} "
    if db_name:
        db_string += f"dbname={db_name} "
    if db_host:
        db_string += f"host={db_host} "
    if db_port:
        db_string += f"port={db_port} "
    info(f"Connecting to database using db_string: '{db_string}'")
    if not additional_args:
        additional_args = {}
    additional_args['autocommit'] = additional_args.get('autocommit', True)
    return psycopg.connect(
            db_string,
            options=f"-c search_path={db_schema}" if db_schema else None,
            **(additional_args or {}),
            row_factory=ListDictFactory)
@beartype
@contextmanager
def schema_scope(con: Connection, schema: str):
    """
    Context manager to set the search path to a schema for a connection
    and reset it when the context is exited.
    """
    current_schema = get_current_schema(con)
    if current_schema != schema:
        set_current_schema(con, schema)
    try:
        yield con
    finally:
        if current_schema != schema:
            set_current_schema(con, current_schema)

def quote_id(con: Connection,
             string_to_quote: str):
    """
    Returns a properly quoted string for use in a raw SQL query as an
    identifier. E.g., if in MySQL, will properly apply `` to a string. If in
    PostgreSQL, will apply "".

    @param con The connection or engine.
    @param string_to_quote The string to quote.
    @return The properly quoted string.
    """
    return psycopg.sql.Identifier(string_to_quote).as_string(con)

def quote_val(con: Connection,
              string_to_quote: str):
    """
    Returns a properly quoted string for use in a raw SQL query as a value.
    """
    return psycopg.sql.Literal(string_to_quote).as_string(con)

def does_db_exist(con: Connection, db: str) -> bool:
    """
    Returns whether a database exists or not.

    @param con The connection for the database in question.
    @param db The database name in question.
    @return A True/False value saying whether or not a database exists.
    """
    result = con.execute('''
        SELECT 1 FROM pg_database WHERE datname = %s
    ''', [db])
    db_exists = bool(result.fetchall())
    return db_exists

def drop_database(con: Connection, db: str) -> None:
    """
    Drop or create a database.

    @param con The connection to use to connect to SQL.
    @param db The name of the database to check/drop.
    """
    db_exists = does_db_exist(con, db)
    if not db_exists:
        info(f"Database \"{db}\" does not exist to drop. Continuing.")
    else:
        info(f"Dropping database \"{db}\".")
        # Remove any potential users from the database if we're in postgres.
        con.execute('''
            SELECT pg_terminate_backend(pid) FROM pg_stat_activity
            WHERE datname = %s
        ''', [db])
        # TODO Set isolation level to autocommit?

        # Drop the database.
        con.execute(SQL('''
            DROP DATABASE {}
        ''').format(Identifier(db)))

def drop_schema(con: Connection, schema: str) -> None:
    con.execute(SQL('''
        DROP SCHEMA IF EXISTS {} CASCADE
    ''').format(Identifier(schema)))

def create_database_as_copy(
        con: Connection, src: str, dst: str) -> None:
    """
    Creates a database as a copy of an existing one

    @param con The connection/engine to use
    @param src The name of the database to copy from
    @param dst The name of the database to create
    """
    # TODO String interpolation to a dead end function is a waste (See logging)
    info(f'Creating database "{dst}" as a copy of "{src}"')
    con.execute(SQL('''
        CREATE DATABASE {} WITH TEMPLATE {}
    ''').format(Identifier(dst), Identifier(src)))


def create_database(con: Connection, db: str) -> bool:
    """
    Create a database if it doesn't exist yet.

    @param con The connection to use to connect to SQL.
    @param db The name of the database to check/create.
    @return A True/False value saying whether or not a database was created.
    """

    db_exists = does_db_exist(con, db)
    if db_exists:
        info(f"Database \"{db}\" already exists; can't create.")
        return False
    con.execute(SQL('''
        CREATE DATABASE {}
    ''').format(Identifier(db)))
    return True


def does_schema_exist(con: Connection, schema: str) -> bool:
    """
    Returns whether a schema exists or not.

    @param con The connection for the database in question.
    @param schema The schema name for the database in question.
    @return A True/False value saying whether or not a schema exists.
    """

    query = '''
        SELECT schema_name FROM information_schema.schemata
        WHERE schema_name = %s
    '''
    result = con.execute(query, [schema])
    schema_exists = bool(result.fetchall())
    return schema_exists


def create_schema(con: Connection, schema: str) -> bool:
    """
    Create a schema if it doesn't exist yet.

    @param con The connection to use to connect to SQL.
    @param db The name of the database the schema lives in.
    @param schema The name of the schema to be created.
    @return A True/False value saying whether or not a schema was created.
    """
    # Check if the feeder schema exists.
    schema_exists = does_schema_exist(con, schema)
    if schema_exists:
        info(f"The schema \"{schema}\" already exists. Continuing.")
        return False
    else:
        info(f"The schema \"{schema}\" does not exist. Creating it.")
        query = SQL('''
            CREATE SCHEMA {}
        ''')
        con.execute(query.format(Identifier(schema)))
    return True


def get_schema_tables(con: Connection, schema: str):
    """
    Returns A list of all tables present in a given schema.

    @param con The connection for the database in question.
    @param schema The schema name for the database in question.
    @return A list of tuples of all tables in the schema.
    """

    query = '''
        SELECT * FROM information_schema.tables
        WHERE table_schema = %s
    '''
    result = con.execute(query, [schema])
    return result.fetchall()


def does_table_exist(con: Connection, table: str)->bool:
    """
    Returns whether a table exists or not.

    @param con The connection for the database in question.
    @param schema The schema name for the database in question.
    @param table The table name in question.
    @return A True/False value saying whether or not a table exists.
    """
    schema = get_current_schema(con)
    result = con.execute('''
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
    ''', [schema, table])
    table_exists = bool(result.fetchall())
    return table_exists

@beartype
def get_table_count(con: Connection, table: str) -> int:
    """
    Returns the number of rows in a table.

    @param con The connection for the database in question.
    @param schema The schema name for the database in question.
    @param table The table name in question.
    @return The number of rows in the table.
    """
    result =con.execute(SQL('''
        SELECT COUNT(*) FROM {}
    ''').format(Identifier(table))).fetchone()
    assert result is not None
    return int(result[0])

@beartype
def is_table_empty(con: Connection, table: str) -> bool:
    """
    Returns whether a table is empty or not.

    @param con The connection for the database in question.
    @param schema The schema name for the database in question.
    @param table The table name in question.
    @return A True/False value saying whether or not the table is empty.
    """
    return get_table_count(con, table) == 0

@beartype
def get_tables(con: Connection, schema: Optional[str] = None) -> list[str]:
    """
    Returns a list of all tables in a database.
    """
    if not schema:
        schema = get_current_schema(con)
    query = '''
        SELECT * FROM information_schema.tables
        WHERE table_schema = %s
    '''
    result = con.execute(query, [schema])
    tables = []
    for row in result:
        tables.append(row['table_name'])
    return tables

@beartype
def get_columns(con: Connection, table: str,
                schema: Optional[str] = None, cache: bool=True) -> List:
    return [i['column_name'] for i in get_column_info(
        con, table, schema, cache)]

@beartype
def get_column_info(con: Connection, table: str,
                    schema: Optional[str] = None, cache: bool=True) -> List:
    """
    Returns a list of a table's columns and their types in a tuples
    (column_name, column_type, is_nullable).
    """
    if not schema:
        schema = get_current_schema(con)
    if cache:
        if hasattr(con, '__ssi_column_info_cache'):
            data = con.__ssi_column_info_cache.get((table, schema))
            if data:
                return data
        else:
            con.__ssi_column_info_cache = {}
    query = '''
        SELECT * FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    '''
    result = con.execute(query, [schema, table])
    columns = []
    for row in result:
        columns.append(row.as_dict())
    if cache:
        con.__ssi_column_info_cache[(table, schema)] = columns
    return columns


@beartype
def add_column_to_table(con: Connection,
                        table: str,
                        column: str,
                        dtype: str):
    """
    Add a column to an existing table
    @param con The connection or engine to operate with
    @param table The name of the table to add a column to
    @param schema The name of the schema the table exists in
    @param column The name of the new column
    @param dtype The sql data type of the new column
    """
    # TODO(doug) Se whould quote new col here but not sure if it should be
    # identifier quote or not
    query = SQL('''
        ALTER TABLE {}
        ADD COLUMN {} {}
    ''').format(Identifier(table), Identifier(column), Literal(dtype))
    info(f"Adding column {column} to "
         f"{table} as {dtype}")
    con.execute(query)

@beartype
def get_current_schema(con: Connection) -> Optional[str]:
    result = con.execute('''
        SELECT current_schema()
    ''')
    result = result.fetchone()
    assert result is not None
    return result[0]

@beartype
def set_current_schema(con: Connection, schema: str):
    con.execute(SQL('''
        SET search_path TO {}
    ''').format(schema))
    if get_current_schema(con) != schema:
        raise Exception(f"Failed to set schema to {schema}, probably doesn't exist")

@beartype
def get_table_constraints(
        con: Connection,
        table: str,
        cache: bool = True) -> List:
    """
    Get the constraints for a table
    @param con The connection or engine to operate with
    @param table The name of the table to get constraints for
    @param cache Whether to cache the results
    @return A list of constraint dicts
    """
    query = '''
        SELECT c.*, tc.*
        FROM information_schema.table_constraints tc
        JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
        JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
            AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
        WHERE tc.table_schema = %s
        AND tc.table_name = %s
    '''
    result = con.execute(query, [get_current_schema(con), table])
    constraints = []
    for row in result:
        constraints.append(row.as_dict())
    return constraints


@beartype
def get_table_id_column(
        con: Connection,
        table: str,
        schema: Optional[str] = None,
        cache: bool = True) -> Optional[str]:
    """
    Get the name of the id column for a table
    @param table The name of the table to get the id column for
    @param schema The name of the schema the table exists in
    @return The name of the id column or None if there is no id column
    """
    # Sometimes the schema is a blank string, we should treat this the same
    # as None, but ideally we shuold trace where this is coming from
    if not schema:
        schema = get_current_schema(con)
    if hasattr(con, '__ssi_id_col_cache'):
        _id_col_cache = con.__ssi_id_col_cache
    else:
        con.__ssi_id_col_cache = _id_col_cache = {}
    if cache and (schema, table) in _id_col_cache:
        id_col = _id_col_cache[(schema, table)]
        #print(f"Using cache for {schema}.{table} id column: {id_col}")
        return id_col
    # See https://stackoverflow.com/questions/1214576/how-do-i-get-the-primary-keys-of-a-table-from-postgres-via-plpgsql
    query = '''
        SELECT c.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)
        JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
            AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
        WHERE constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = %s
        AND tc.table_name = %s
    '''
    result = con.execute(query, [schema, table])
    if result.rowcount == 0:
        id_col = None
    else:
        r = result.fetchone()
        assert r
        id_col = str(r[0])
    # assert isinstance(id_col, str)
    if cache:
        #print("Caching")
        _id_col_cache[(schema, table)] = str(id_col)
    return id_col

@beartype
def table_get_foreign_keys(con: Connection, table: str) -> list[dict]:
    schema = get_current_schema(con)
    query = '''
        SELECT
            tc.constraint_name,
            tc.table_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM
            information_schema.table_constraints AS tc
            JOIN
                information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN
                information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
        WHERE
            constraint_type = 'FOREIGN KEY'
            AND tc.table_name=%s AND tc.table_schema=%s
    '''
    r = con.execute(query, (table, schema))
    return [i.as_dict() for i in r.fetchall()]

@beartype
def insert(
        con: Connection,
        data: dict,
        table: str,
        check: bool=False):
    """
    Insert a row into a table from a dictionary of data
    @param table The name of the table to insert into
    @param schema The name of the schema the table exists in
    @param data A dictionary of data to insert
    @param check Whether to perform expensive validation
    @return ID of the inserted row
    """
    if check:
        check_msg = ""
        # Check that the data dictionary has the same keys as the table
        # columns.
        cols = set(get_columns(con, table))
        data_keys = set(data.keys())
        for i in data_keys - cols:
            check_msg +=(f"Key {i} in data is not in table {table}\n")
            # Find nearest match
            match = difflib.get_close_matches(i, cols)
            if match:
                check_msg += (f"Did you mean {match[0]}?")
        if check_msg:
            raise ValueError(check_msg)
    columns = ', '.join(['{}' for _ in data.keys()])
    values = ', '.join(['%s' for _ in data.keys()])
    id_col = get_table_id_column(con, table)
    if id_col is None:
        id_col = '*'
    query = SQL(f'''
        INSERT INTO {{table}} ({columns})
        VALUES ({values})
        RETURNING {{id_col}}
    ''').format(
            *[Identifier(i) for i in data.keys()],
            table=Identifier(table),
            id_col=Identifier(id_col))
    r = con.execute(query, tuple(data.values())).fetchone()
    assert r is not None
    return r[0]

@beartype
def adjust_autoincrement_cols(con: Connection, table: str):
    # Adjust the auto increment sequence if it exists. We have to
    # search for it in the table definition because the
    # association used by pg_get_serial_sequence is not created
    # by tools like DBWrench
    column_info = get_column_info(con, table)
    for column in column_info:
        col = column['column_name']
        column_default = column['column_default']
        if column_default and 'nextval' in column_default:
            # UGH
            sequence_name = column_default.split("'")[1]
            con.execute(SQL('''
                SELECT setval({sequence_name}, coalesce(max({col}),0)+1, true)
                FROM {table}
            ''').format(
                sequence_name=sequence_name,
                table=Identifier(table),
                col=Identifier(col)))


_last_tmp_table = 0
@beartype
def insert_file(
        con: Connection,
        fp: TextIO,
        table: str,
        skip_header=False,
        sep='\t',
        columns=None,
        upsert=True)->int:
    """
    Insert data from a file into a table
    @param con The connection or engine to operate with
    @param fp The file pointer to read from
    @param table The name of the table to insert into
    @param schema The name of the schema the table exists in
    @param skip_header Whether to skip the first line of the file
    @param sep The separator to use for the file, tab separated by default
    @param columns The columns to insert into, if None, the columns will be
        assumed to be the same as the table
    """
    if columns is None:
        columns = get_columns(con, table)
    if skip_header:
        fp.readline()
    id_col = get_table_id_column(con, table)
    # Upserts are not valid when there is no id column passed in or there is
    # no ID column defined in the table
    if not id_col or id_col not in columns:
        upsert = False
    # Create a temporary table for this transaction
    if upsert:
        global _last_tmp_table
        tmp_table=table+'_tmp_'+str(_last_tmp_table)
        _last_tmp_table += 1
        with con.transaction():
            assert id_col
            cur = con.cursor()
            cur.execute(SQL("""
                CREATE TEMP TABLE {tmp_table} (LIKE {table} INCLUDING ALL)
                ON COMMIT DROP
            """).format(tmp_table=Identifier(tmp_table), table=Identifier(table)))
            # Copy the data into the temporary table
            col_id_list = [Identifier(i) for i in columns]
            cols = SQL(', '.join(['{}' for _ in columns])).format(
                *col_id_list)
            col_id_list_doubled = [i for i in col_id_list for _ in range(2)]
            cols_set = SQL(', '.join(['{}=EXCLUDED.{}' for _ in columns])).format(
                *col_id_list_doubled)
            with cur.copy(SQL('''
                COPY {}
                    ({})
                FROM STDIN WITH (FORMAT CSV, DELIMITER E%s, NULL '\\N')
            ''').format(Identifier(tmp_table), cols),[sep]) as copy:
                for line in fp:
                    copy.write(line)
            # Insert the data from the temporary table into the real table with an
            # upsert
            query = SQL('''
                INSERT INTO {table}
                    SELECT *
                    FROM {tmp_table}
                ON CONFLICT ({id_col}) DO UPDATE
                SET {cols_set}
            ''').format(
                table=Identifier(table),
                tmp_table=Identifier(tmp_table),
                cols=cols,
                cols_set=cols_set,
                id_col=Identifier(id_col))
            cur.execute(query)
            count = cur.rowcount
            fp.close()
    else:
        cur = con.cursor()
        col_id_list = [Identifier(i) for i in columns]
        cols = SQL(', '.join(['{}' for _ in columns])).format(
            *col_id_list)
        with cur.copy(SQL('''
            COPY {}
                ({})
            FROM STDIN WITH (FORMAT CSV, DELIMITER E%s, NULL '\\N')
        ''').format(Identifier(table), cols),[sep]) as copy:
            for line in fp:
                copy.write(line)
        count = cur.rowcount
        fp.close()
    adjust_autoincrement_cols(con, table)
    return count

def insert_many(
        con: Connection,
        table: str,
        data: List[Union[Dict, List]],
        columns: Optional[List[str]] = None,
        upsert=True) -> int:
    """
    Insert multiple rows into a table from a list of dictionaries of data.
    If the dictionaries do not have the same keys, the keys will be unioned
    and missing keys will be filled with None. Columns which are entirely None
    are omitted from the insertion. This allows using None as a sentinel value
    for "Use default" such as auto increment columns
    @param table The name of the table to insert into
    @param schema The name of the schema the table exists in
    @param data A list of dictionaries or list of lists of data to insert
    @return Number of rows inserted
    """
    if columns is None:
        _columns = set()
        if isinstance(data[0], dict):
            for row in data:
                assert isinstance(row, dict)
                keys = set()
                for key, val in row.items():
                    if val is None:
                        continue
                    keys.add(key)
                _columns = _columns.union(keys)
            columns = list(_columns)
    else:
        columns = get_columns(con, table)
        if isinstance(data[0], list):
            if (len(columns) != len(data[0])):
                raise ValueError(
                        "Number of columns in data does not match table")
    assert columns is not None
    # Write from csv to a temp table
    count = 0
    with tempfile.NamedTemporaryFile(mode='w', newline='') as f:
        def sanitize_entry(entry):
            if entry is None:
                return '\\N'
            return str(entry).replace('\t', '\\t').replace('\n', '\\n').replace('"', '""')
        if isinstance(data[0], dict):
            for row in data:
                assert isinstance(row, dict)
                count+=1
                f.write('\t'.join([sanitize_entry(row.get(i)) for i in columns]) + '\n')
        else:
            for row in data:
                count+=1
                f.write('\t'.join([sanitize_entry(i) for i in row]) + '\n')
        f.flush()
        f.seek(0)
        fp = open(f.name, 'r')
        count = insert_file(con, fp, table, columns=columns, upsert=upsert)
        fp.close()
    return count

def insert_many_df(
        con: Connection,
        table: str,
        df: DataFrame,
        upsert=True):
    """
    Improved/Simplified DF to sql
    """
    columns = df.columns
    count = df.shape[0]
    # Write from temp csv
    with tempfile.NamedTemporaryFile(mode='w', newline='') as f:
        df.to_csv(f, sep='\t', index=False, header=False, na_rep='\\N', float_format='%.18f')
        f.flush()
        f.seek(0)
        fp = open(f.name, 'r')
        insert_file(con, fp, table, columns=columns, upsert=upsert)
        fp.close()
    return count



def upsert(con: Connection, table: str, data: dict, schema: Optional[str] = None):
    """
    Upsert a row into a table from a dictionary of data
    @param table The name of the table to insert into
    @param schema The name of the schema the table exists in
    @param data A dictionary of data to upsert
    @return The last inserted ID/primary key
    """
    id_col = get_table_id_column(con, table, schema)
    if id_col is None or (id_col in data and data[id_col] is None):
        del data[id_col]
        # Demote to insert
        return insert(con, data, table, check=False)
    columns = ', '.join([quote_id(con, i) for i in data.keys()])
    values = ', '.join(['%s' for _ in data.keys()])
    updates = ', '.join([f'{quote_id(con, i)} = %s' for i in data.keys()])
    if id_col is not None:
       id_col = quote_id(con, id_col)
    else:
        id_col = 'ctid'
    table = quote_id(con, table)
    query = f'''
        INSERT INTO {table}
            ({columns})
        VALUES
            ({values})
        ON CONFLICT ({id_col}) DO UPDATE SET
            {updates}
        RETURNING {id_col}
    '''
    # Duplicatet the values for the Vlaues and Set clauses
    values = list(data.values()) + list(data.values())
    r = con.execute(query, values) # pyright: ignore [reportGeneralTypeIssues]
    r = r.fetchone()
    assert r is not None
    return r[0]

@beartype
def query(con: Connection, table: str, params: Optional[dict] = None, filter_none=True):
    """
    Query a database and return the results as a DB cursor
    @param con A database connection
    @param params The parameters to filter by (the WHERE clause with AND)
    """
    where = ''
    # Filter out None values
    if params and filter_none:
        params = {k: v for k, v in params.items() if v is not None}
    if params:
        where = 'WHERE ' + ' AND '.join(
            ['{} = %s' for _ in params.keys()])
    else:
        params = {}
    query = SQL('''
        SELECT *
        FROM {table}
    ''' + where).format(
        *[Identifier(k) for k in params.keys()],
        table=Identifier(table))
    return con.execute(query, list(params.values()))

@beartype
def query_fuzzy(con: Connection, table: str, params: Optional[dict] = None):
    """
    Query a database and return the results as a DB cursor
    @param con A database connection
    @param params Params to filter by (the WHERE clause with LIKE)
    """
    where = ''
    if params:
        params = {k: f'%{v}%' for k, v in params.items() if v is not None}
        where = ' WHERE '+ ' AND '.join(['{} LIKE %s' for _ in params.keys()])
    else:
        params = {}
    query = SQL(f'''
        SELECT *
        FROM {{table}}
        {where}
    ''').format(
        *[Identifier(i) for i in params.keys()],
        table=Identifier(table))
    return con.execute(query, list(params.values()))

@beartype
def query_any(con: Connection, table: str, params: Optional[dict] = None):
    """
    Query a database and return the results matching a list of parameters
    """
    where = ''
    if params:
        params = {k: v for k, v in params.items() if v is not None}
        l = []
        for param in params:
            if not isinstance(params[param], list):
                params[param] = [params[param]]
            size = len(params[param])
            l.append(f'{{}} = ANY(ARRAY[{",".join(["%s" for _ in range(size)])}])')
        where = ' WHERE '+ ' OR '.join(l)
    else:
        params = {}
    query = SQL(f'''
        SELECT *
        FROM {{table}}
        {where}
    ''').format(
        *[Identifier(i) for i in params.keys()],
        table=Identifier(table))
    complete_list = []
    for param in params:
        complete_list.extend(params[param])
    return con.execute(query, complete_list)

@beartype
def query_one(
        con: Connection,
        table: str, params: Optional[dict] = None) -> dict[str, Any]:
    """
    Query a database and return the only result.
    If there is more than one result or if there are no results, the
    exceptions MultipleResultsFound and NoResultsFound are
    raised respectively
    @param con A database connection
    @param table The name of the table to query
    @param params A dictionary of parameters to query
    @return A dictionary of the result
    @note An exception is raised if there is more than one result
    """
    if params:
        # filter null values
        params = {k: v for k, v in params.items() if v is not None}
    r = query(con, table, params)
    if r.rowcount > 1:
        raise MultipleResultsFound(
                f'Expected 1 result in table {table} '
                f'where {params}, got {r.rowcount}')
    row = r.fetchone()
    if row is None:
        raise NoResultsFound(
                f'Expected 1 result in table {table} '
                f'where {params}, got 0')
    return row.as_dict()

@beartype
def query_one_or_none(
        con: Connection,
        table: str, params: Optional[dict] = None) -> Optional[dict[str, Any]]:
    """
    Query a database and return the only result or None if there is no result.
    If there is more than one result, the exception MultipleResultsFound is
    raised
    @param con A database connection
    @param table The name of the table to query
    @param params A dictionary of parameters to query
    @return A dictionary of the result, None if there were no results
    """
    try:
        return query_one(con, table, params)
    except NoResultsFound:
        return None


@beartype
def update(
        con: Connection,
        table: str,
        data: dict,
        filter_none=True):
    """
    Update a row in a table from a dictionary of data
    @param table The name of the table to insert into
    @param schema The name of the schema the table exists in
    @param data A dictionary of data to insert
    @return The ID of the updated row
    """
    # Filter out None values
    id_col = get_table_id_column(con, table)
    id = None
    if id_col is None:
        raise ValueError(f'No ID column found in table {table}')
    if id_col not in data:
        raise ValueError(f'Column {id_col} not found in data')
    id = data[id_col]
    del data[id_col]
    if filter_none:
        data = {k: v for k, v in data.items() if v is not None}
    updates = SQL(', '.join(['{} = %s' for _ in data.keys()])).format(
        *[Identifier(i) for i in data.keys()])
    query = SQL('''
        UPDATE {table}
        SET
            {updates}
        WHERE
            {id_col} = %s
    ''').format(
        *[Identifier(i) for i in data.keys()],
        updates=updates,
        table = Identifier(table),
        id_col = Identifier(id_col))
    values = list(data.values())
    print(values)
    con.execute(query, [*values, id])
    return id

class NoCommonColumnsException(Exception):
    pass


@beartype
def cross_con_table_copy(
        con_src: Connection, table_src: str, con_dst: Connection, table_dst: str) -> int:
    """
    Copy a table from one con to another, If the table already exists, the
    data will be appended, otherwise the table will be created. If there are
    columns in the source table that are not in the destination table, they will
    be ignored. If there are columns in the destination table that are not in
    the source table, they will be set to null. If there are no columns in
    common, an exception will be raised. If the table does not exist in the
    source database, an exception will be raised.

    Note that this routine has to be used for cross database copies as well
    because without foreign server views or something similar it's not possible
    to copy postgres database tables.

    @param con1 The connection to the source server
    @param con2 The connection to the destination server
    @param table The name of the table to copy
    @return The number of rows copied
    """
    columns = get_column_info(con_src, table_src)
    table_dst_q = quote_id(con_dst, table_dst)
    table_src_q = quote_id(con_src, table_src)
    columns_by_name = {c["column_name"]: c for c in columns}
    where_clause = ''
    if does_table_exist(con_dst, table_dst):
        columns_dst = get_column_info(con_dst, table_dst)
        columns_dst_by_name = {c["column_name"]: c for c in columns_dst}
        common_columns = set(columns_by_name.keys()) & set(columns_dst_by_name.keys())
        if len(common_columns) == 0:
            raise NoCommonColumnsException(
                    f'No common columns between {con_src} {table_src} and {con_dst} {table_dst}')
        id_col = get_table_id_column(con_src, table_src)
        if id_col is None:
            raise ValueError(f'No ID column found in table {table_src}')
        id_col_dst = get_table_id_column(con_dst, table_dst)
        if id_col_dst is None:
            raise ValueError(f'No ID column found in table {table_dst}')
        if id_col != id_col_dst:
            raise ValueError(
                    f'ID column mismatch between {table_src} and {table_dst}')
        id_col_type = None
        for info in get_column_info(con_dst, table_dst):
            name = info['column_name']
            data_type = info['data_type']
            if name == id_col:
                id_col_type = data_type
        id_col_type = postgres_type_to_python_type(id_col_type)
        if id_col_type == 'int' or 'datetime' in id_col_type:
            query = SQL('''
                SELECT MAX({id_col}) FROM {table_dst}
            ''').format(
                id_col=Identifier(id_col),
                table_dst=Identifier(table_dst))
            r = con_dst.execute(query)
            max_id = r.fetchone()[0]
            if max_id is not None:
                max_id = quote_val(con_src, max_id)
                id_col = quote_id(con_src, id_col)
            where_clause = f'WHERE {id_col} > {max_id}'
    else:
        # Create the table
        with con_dst.transaction():
            names_and_types = [(quote_id(con_dst, c['column_name']), c['data_type']) for c in columns]
            create_sql = f'''
                CREATE TABLE {table_dst_q} (
                    {', '.join([f'{name} {type}' for (name, type) in names_and_types])}
                )
            '''
            con_dst.execute(create_sql)
            constraints = get_table_constraints(con_src, table_src)
            for constraint in constraints:
                definition = f'{constraint["constraint_type"]} ({constraint["column_name"]})'
                query = SQL(f'''
                    ALTER TABLE {{table_dst}}
                    ADD CONSTRAINT {{constraint}}
                    {definition}
                ''').format(
                    table_dst=Identifier(table_dst),
                    constraint=Identifier(constraint['constraint_name']))
                con_dst.execute(query)
        common_columns = columns_by_name.keys()
        # Create the same constraints
    columns = [columns_by_name[c] for c in common_columns]
    src_cur = con_src.cursor()
    dst_cur = con_dst.cursor()
    count = 0
    quoted_columns = [quote_id(con_src, c) for c in common_columns]
    column_list = ', '.join(quoted_columns)
    total = get_table_count(con_src, table_src)
    with src_cur.copy(f'''
        COPY (SELECT {column_list} FROM {table_src_q} {where_clause}) TO STDOUT (
            FORMAT TEXT
        )  ''') as src:
        with dst_cur.copy(f'''
            COPY {table_dst_q} ({column_list}) FROM STDIN (
                FORMAT TEXT
            )''') as dst:
            while data := src.read():
                dst.write(data)
                count += 1
        dst_cur.close()
    src_cur.close()
    return count

def dump_table_as_json(
        con: Connection,
        table: str,
        outfp: TextIO):
    """
    Dump a table as a json file
    @param con The connection to the database
    @param table The name of the table to dump
    @return The number of rows dumped
    """
    with con.cursor() as cur:
        cur.execute(f"SELECT * FROM {table}")
        count = 0
        outfp.write('[\n')
        for row in cur:
            row_dict = row.as_dict()
            # TODO convert timestamps and such
            outfp.write(json.dumps(row_dict, indent=4))
            outfp.write('\n')
            count += 1
        outfp.write(']\n')

def dump_table_as_csv(
        con: Connection,
        table: str,
        outfp: TextIO):
    """
    Dump a table as a csv file
    @param con The connection to the database
    @param table The name of the table to dump
    @return The number of rows dumped
    """
    rows = 0
    with con.cursor() as cur:
        with cur.copy(
                SQL('COPY {} TO STDOUT WITH CSV HEADER').format(
                    Identifier(table))) as copy:
            while data := copy.read():
                outfp.write(data)
                rows += 1
    return rows

def postgres_type_to_python_type(type: str) -> str:
    # Map postgres types to python types
    python_type = None
    if type == "integer":
        python_type = "int"
    elif type == "character varying":
        python_type = "str"
    elif type == "timestamp without time zone":
        python_type = "datetime.datetime"
    elif type == "date":
        python_type = "datetime.date"
    elif type == "boolean":
        python_type = "bool"
    elif type == "numeric":
        python_type = "float"
    elif type == "text":
        python_type = "str"
    elif type == "double precision":
        python_type = "float"
    elif type == "bigint":
        python_type = "int"
    else:
        warnings.warn(f"Unknown type {type}")
        python_type = "str"
    return python_type

def delete(con: Connection, table: str, params: Optional[dict] = None):
    """
    Delete rows from a table
    @param con A database connection
    @param table The name of the table to delete from
    @param params A dictionary of parameters to delete
    @return A delete statement
    """
    where = ''
    if params:
        where = SQL(' WHERE ')+ SQL(' AND '.join(['{} = %s' for _ in params.keys()])).format(
            *[Identifier(i) for i in params.keys()])
    else:
        params = {}
    query = SQL('''
        DELETE FROM {table}
        {where}
    ''').format(
        table=Identifier(table),
        where=where)
    return con.execute(query, list(params.values()))

def get_column_comments(con: Connection, table: str) -> dict[str, str]:
    """
    Return the comments associated with each column,
    useful for generating docstrings and similar
    """
    schema = get_current_schema(con)
    query = '''
        SELECT
            c.column_name,
            pgd.description
        FROM
            pg_catalog.pg_statio_all_tables AS st
            INNER JOIN pg_catalog.pg_description pgd
                ON (pgd.objoid=st.relid)
            INNER JOIN information_schema.columns c
                ON (pgd.objsubid=c.ordinal_position
                    AND c.table_schema=st.schemaname
                    AND c.table_name=st.relname)
        WHERE
            c.table_schema = %s
            AND c.table_name = %s
    '''
    r = con.execute(query, [table, schema])
    return {i[0]: i[1] for i in r.fetchall()}

def get_table_comment(
        con: Connection,
        table: str) -> Optional[str]:
    """
    Return the comments associated with a table
    """
    schema = get_current_schema(con)
    query = '''
        SELECT pg_catalog.obj_description(c.oid, 'pg_class'), t.table_name
        FROM information_schema.tables AS t
        INNER JOIN pg_catalog.pg_class AS c ON t.table_name::text = c.relname
        WHERE t.table_name = %s
        AND t.table_schema = %s
        AND t.table_type = 'BASE TABLE'
    '''
    r = con.execute(query, [table, schema])
    if r.rowcount == 0:
        return None
    r = r.fetchone()
    assert r is not None
    return r[0]

def generate_camel_case_name(name: str) -> str:
    """
    Convert a name to camel case
    """
    return ''.join([i.capitalize() for i in name.split('_')])

def sanitize_snake_case_name(name: str) -> str:
    """
    Ensure that a name doesn't have [ ] and etc
    """
    # TODO not very robust
    return name.replace('[', '').replace(']', '').replace(' ', '_')


def generate_python_class_for_db_table(
        con: Connection, table: str, fp: TextIO) -> None:
    print("Processing table", table)
    snake_case_table = sanitize_snake_case_name(table)
    camel_case_table = generate_camel_case_name(table)
    table_comment = get_table_comment(con, table)
    required_columns = []
    column_comments = get_column_comments(con, table)
    optional_columns = []
    columns_by_name = {}
    column_position = 0
    for column_info in get_column_info(con, table):
        column_position += 1
        column_info['column'] = sanitize_snake_case_name(column_info['column_name'])
        column_info['comment'] = column_comments.get(column_info['column_name'], "")
        column_info['position'] = column_position
        if column_info['is_nullable'] == 'YES':
            column_info['optional'] = True
            optional_columns.append(column_info)
        elif column_info['is_identity'] == 'YES':
            column_info['optional'] = True
            optional_columns.append(column_info)
        elif column_info['column_default']:
            optional_columns.append(column_info)
            # column default is an expression which messes things up
            column_info['optional'] = True
        else:
            column_info['optional'] = False
            required_columns.append(column_info)
        columns_by_name[column_info['column_name']] = column_info
    fp.write("@beartype_wrap_init\n")
    fp.write("@dataclasses.dataclass\n")
    fp.write(f"class {camel_case_table}:\n")
    fp.write( '    """\n')
    if table_comment:
        fp.write(f"    {table_comment}\n\n")
    parameter_comments = ""
    for column_info in columns_by_name.values():
        parameter_comments += (f"    @param {column_info['column_name']} {column_info['comment']}\n")
    fp.write(parameter_comments)
    fp.write(f"\n")
    fp.write(f"    This is an automatically generated class\n")
    fp.write( '    """\n')
    for column_info in (required_columns + optional_columns):
        column = column_info['column']
        type = column_info['data_type']
        optional = column_info['optional']
        column_position = column_info['position']
        column_comment = column_info['comment']
        column_comment = column_comment.replace('\n', ' ')
        python_type = postgres_type_to_python_type(type)
        column_info['python_type'] = python_type
        if optional:
            python_declaration = f"{column}: Optional[{python_type}] = None"
        else:
            python_declaration = f"{column}: {python_type}"
        column_info['python_declaration'] = python_declaration
        fp.write(f"    {python_declaration}")
        fp.write(f" # {column_info['column_name']} {type} (default: {column_info.get('default', '')})\n")
    id_col = get_table_id_column(con, table)
    if id_col is None:
        fp.write(f"    PRIMARY_KEY: ClassVar[Optional] = None\n")
        id_col_type = "None"
    else:
        id_col_type = postgres_type_to_python_type(columns_by_name[id_col]['data_type'])
        fp.write(f"    PRIMARY_KEY: ClassVar[str] = '{id_col}'\n\n")

    fp.write(f"    def to_json_dict(self) -> Dict[str, Any]:\n")
    fp.write(f"        obj = dataclasses.asdict(self)\n")
    for column_info in (required_columns + optional_columns):
        column = column_info['column']
        type = column_info['data_type']
        python_type = postgres_type_to_python_type(type)
        if python_type == "datetime.datetime" or python_type == "datetime.date" or python_type == "datetime.time":
            if column_info['optional']:
                fp.write(f"        if self.{column} is not None:\n")
                fp.write(f"            obj['{column}'] = self.{column}.isoformat()\n")
            else:
                fp.write(f"        obj['{column}'] = self.{column}.isoformat()\n")
    fp.write(f"        return obj\n")

    foreign_keys = table_get_foreign_keys(con, table)
    for foreign_key in foreign_keys:
        foreign_table = foreign_key['foreign_table_name']
        foreign_column = sanitize_snake_case_name(foreign_key['foreign_column_name'])
        foreign_table = sanitize_snake_case_name(foreign_table)
        foreign_table_class = generate_camel_case_name(foreign_table)
        column = sanitize_snake_case_name(foreign_key['column_name'])
        # This trick aids with the mess of using plural table names
        # as well as helping with specialized foreign key names by
        # using the column name
        entity_name = column
        if column.endswith("_id"):
            entity_name = column[:-3]
        fp.write("\n")
        fp.write(f"    @beartype.beartype\n")
        fp.write(f"    def get_{entity_name}(self, con: db.Connection) -> Optional['{foreign_table_class}']:\n")
        fp.write(f"        return read_{foreign_table}_one_or_none(con, {foreign_column}=self.{column})\n")
    write_param_list = []
    update_param_list = []
    for column_info in (required_columns + optional_columns):
        write_param_list.append(column_info['python_declaration'])
    read_param_list = []
    read_param_lists_list = []
    for column_info in (required_columns + optional_columns):
        column = column_info['column']
        python_type = column_info['python_type']
        python_list_type = f"Optional[List[{python_type}]] = None"
        python_type = f"Optional[{python_type}] = None"
        read_param_list.append(f"{column}: {python_type}")
        read_param_lists_list.append(f"{column}: {python_list_type}")
    update_param_list = [f"{id_col}: {id_col_type}"]
    for column_info in (required_columns + optional_columns):
        column = column_info['column']
        if column != id_col:
            python_type = column_info['python_type']
            python_type = f"Optional[{python_type}] = None"
            update_param_list.append(f"{column}: {python_type}")

    fp.write(f"\n")


    pack_params_to_dict = "    data = {\n"
    for column_name in columns_by_name.keys():
        pack_params_to_dict += f"        '{column_name}': {column_name},\n"
    pack_params_to_dict += "    }\n"
    fp.write(f"@beartype.beartype\n")
    fp.write(f"def create_{snake_case_table}_from_json_dict(json_obj: dict):\n")
    fp.write(f"        \"\"\"\n")
    fp.write(f"        Create a {camel_case_table} from a json object dict\n")
    fp.write(f"        doing type conversions (IE, datetime str) as necessary\n")
    fp.write(f"        \"\"\"\n")
    for column_info in (required_columns + optional_columns):
        type = column_info['data_type']
        python_type = postgres_type_to_python_type(type)
        column_info['python_type'] = python_type
        column = column_info['column_name']
        if python_type == 'datetime.datetime' or python_type == 'datetime.date' or python_type == 'datetime.time':
            if column_info['optional']:
                fp.write(f"        if '{column}' in json_obj and json_obj['{column}'] is not None:\n")
                fp.write(f"            json_obj['{column}'] = {python_type}.fromisoformat(json_obj['{column}'])\n")
            else:
                fp.write(f"        json_obj['{column}'] = {python_type}.fromisoformat(json_obj['{column}'])\n")
    fp.write(f"        return {camel_case_table}(**json_obj)\n\n")
    fp.write("\n")
    fp.write(f"@beartype.beartype\n")
    fp.write(f"def write_{snake_case_table}_obj(con: db.Connection, obj: {camel_case_table}) -> {id_col_type}:\n")
    fp.write(f'    """\n')
    fp.write(f"    Write a {camel_case_table} object to the database\n")
    fp.write(f"    @param con: database connection\n")
    fp.write(f"    @param obj: {camel_case_table} object\n")
    fp.write(f"    @return id of the inserted/updated row\n")
    fp.write(f'    """\n')
    fp.write(f"    return db.upsert(con, '{table}', dataclasses.asdict(obj))\n\n")
    fp.write(f"@beartype.beartype\n")
    write_param_list = ',\n            '.join(write_param_list)
    fp.write(f"def write_{snake_case_table}(\n")
    fp.write(f"            con: db.Connection,\n")
    fp.write(f"            {write_param_list}) -> {id_col_type}:\n")
    fp.write(f'    """\n')
    fp.write(f"    Write to the {snake_case_table} table in the database\n")
    fp.write(f"    @param con: database connection\n")
    fp.write(parameter_comments)
    fp.write(f"    @return id of the inserted/updated row\n")
    fp.write(f'    """\n')
    fp.write(pack_params_to_dict)
    fp.write(f"    return db.upsert(con, '{table}', data)\n\n")
    fp.write(f"@beartype.beartype\n")
    fp.write(f"def write_{snake_case_table}_many(con: db.Connection, objs: List[{camel_case_table}], upsert: bool = False) -> int:\n")
    fp.write(f'    """\n')
    fp.write(f"    Write a list of {camel_case_table} objects to the database\n")
    fp.write(f"    @param con: database connection\n")
    fp.write(f"    @param objs: list of {camel_case_table} objects\n")
    fp.write(f"    @param upsert: if True, update existing rows based on ID\n")
    fp.write(f"    @return The number of rows inserted\n")
    fp.write(f'    """\n')
    fp.write(f"    return db.insert_many(con, '{table}', [dataclasses.asdict(obj) for obj in objs], upsert=upsert)\n\n")
    update_param_list = ',\n            '.join(update_param_list)
    fp.write(f"@beartype.beartype\n")
    fp.write(f"def update_{snake_case_table}(con: db.Connection, {update_param_list}) -> int:\n")
    fp.write(f'    """\n')
    fp.write(f"    Update a row in the {snake_case_table} table in the database\n")
    fp.write(f"    @param con: database connection\n")
    fp.write(parameter_comments)
    fp.write(f"    @return The number of rows updated\n")
    fp.write(f'    """\n')
    fp.write(pack_params_to_dict)
    fp.write(f"    return db.update(con, '{table}', data)\n\n")

    fp.write(f"@beartype.beartype\n")
    read_param_list = ',\n             '.join(read_param_list)
    fp.write(f"def read_{snake_case_table}(\n")
    fp.write(f"            con: db.Connection,\n")
    fp.write(f"            {read_param_list}) -> Generator[{camel_case_table}, None, None]:\n")
    fp.write(f'    """\n')
    fp.write(f"    Read from the {snake_case_table} table in the database, optionally filtered by a parameter\n")
    fp.write(f"    Returns a generator so that not all rows are fetched in memory at once\n")
    fp.write(f"    @param con: database connection\n")
    fp.write(parameter_comments)
    fp.write(f"    @return generator of {camel_case_table} objects\n")
    fp.write(f'    """\n')
    fp.write(pack_params_to_dict)
    fp.write(f"    result = db.query(con, '{table}', data)\n")
    fp.write(f"    for row in result:\n")
    fp.write(f"        yield {camel_case_table}(**row.as_dict())\n\n")
    fp.write(f"@beartype.beartype\n")
    fp.write(f"def read_{snake_case_table}_fuzzy(con: db.Connection, {read_param_list}) -> Generator[{camel_case_table}, None, None]:\n")
    fp.write(f'    """\n')
    fp.write(f"    Read from the {snake_case_table} table in the database, optionally filtered by fuzzy parameter matching\n")
    fp.write(f"    Returns a generator so that not all rows are fetched in memory at once\n")
    fp.write(f"    @param con: database connection\n")
    fp.write(parameter_comments)
    fp.write(f"    @return generator of {camel_case_table} objects\n")
    fp.write(f'    """\n')
    fp.write(pack_params_to_dict)
    fp.write(f"    result = db.query_fuzzy(con, '{table}', data)\n")
    fp.write(f"    for row in result:\n")
    fp.write(f"        yield {camel_case_table}(**row.as_dict())\n\n")
    fp.write(f"@beartype.beartype\n")
    read_param_lists_list = ',\n             '.join(read_param_lists_list)
    fp.write(f"def read_{snake_case_table}_any(con: db.Connection, {read_param_lists_list}) -> Generator[{camel_case_table}, None, None]:\n")
    fp.write(f'    """\n')
    fp.write(f"    Read from the {snake_case_table} table in the database, optionally filtered by fuzzy parameter matching\n")
    fp.write(f"    Returns a generator so that not all rows are fetched in memory at once\n")
    fp.write(f"    @param con: database connection\n")
    fp.write(parameter_comments)
    fp.write(f"    @return generator of {camel_case_table} objects\n")
    fp.write(f'    """\n')
    fp.write(pack_params_to_dict)
    fp.write(f"    result = db.query_any(con, '{table}', data)\n")
    fp.write(f"    for row in result:\n")
    fp.write(f"        yield {camel_case_table}(**row.as_dict())\n\n")
    fp.write(f"@beartype.beartype\n")
    fp.write(f"def read_{snake_case_table}_one_or_none(con: db.Connection, {read_param_list}) -> Optional[{camel_case_table}]:\n")
    fp.write(f'    """\n')
    fp.write(f"    Read from the {snake_case_table} table in the database, filtered by a required parameter.\n")
    fp.write(f"    Returns None if no row is found.\n")
    fp.write(f"    Raises MultipleResultsFound if more than one ro/w matches\n")
    fp.write(f"    @param con: database connection\n")
    fp.write(f'    """\n')
    fp.write(pack_params_to_dict)
    fp.write(f"    result = db.query_one_or_none(con, '{table}', data)\n")
    fp.write(f"    if result is None:\n")
    fp.write(f"        return None\n")
    fp.write(f"    return {camel_case_table}(**result)\n\n")
    fp.write(f"@beartype.beartype\n")
    fp.write(f"def read_{snake_case_table}_one(con: db.Connection, {read_param_list}) -> {camel_case_table}:\n")
    fp.write(f'    """\n')
    fp.write(f"    Read from the {snake_case_table} table in the database, filtered by a required parameter.\n")
    fp.write(f"    Raises MultipleResultsFound if more than one row matches\n")
    fp.write(f"    Raises NoResultsFound if no row matches\n")
    fp.write(f"    @param con: database connection\n")
    fp.write(f'    """\n')
    fp.write(pack_params_to_dict)
    fp.write(f"    result = db.query_one(con, '{table}', data)\n")
    fp.write(f"    return {camel_case_table}(**result)\n\n")
    fp.write(f"@beartype.beartype\n")
    fp.write(f"def read_{snake_case_table}_all(con: db.Connection, {read_param_list}) -> List[{camel_case_table}]:\n")
    fp.write(f'    """\n')
    fp.write(f"    Read from the {snake_case_table} table in the database, \n")
    fp.write(f"    optionally filtered by parameters\n")
    fp.write(f"    @param con: database connection\n")
    fp.write(f'    """\n')
    fp.write(pack_params_to_dict)
    fp.write(f"    result = db.query(con, '{table}', data)\n")
    fp.write(f"    return [{camel_case_table}(**row.as_dict()) for row in result]\n\n")
    fp.write(f"@beartype.beartype\n")
    if id_col is not None:
        fp.write(f"def read_{snake_case_table}_by_id(con: db.Connection, {id_col}: {id_col_type}) -> Optional[{camel_case_table}]:\n")
        fp.write(f"    result = db.query_one(con, '{table}', {{'{id_col}': {id_col}}})\n")
        fp.write(f"    if result is None:\n")
        fp.write(f"        return None\n")
        fp.write(f"    return {camel_case_table}(**result)\n\n")
        fp.write(f"@beartype.beartype\n")
        fp.write(f"def delete_{snake_case_table}_by_id(con: db.Connection, {id_col}: {id_col_type}):\n")
        fp.write(f"    db.delete(con, '{table}', {{'{id_col}': {id_col}}})\n")
    fp.write("# Associate the functions with the class\n")
    fp.write(f"{camel_case_table}.create_from_json_dict = create_{snake_case_table}_from_json_dict\n")
    fp.write(f"{camel_case_table}.write = write_{snake_case_table}\n")
    fp.write(f"{camel_case_table}.update = update_{snake_case_table}\n")
    fp.write(f"{camel_case_table}.write_many = write_{snake_case_table}_many\n")
    fp.write(f"{camel_case_table}.read = read_{snake_case_table}\n")
    fp.write(f"{camel_case_table}.read_fuzzy = read_{snake_case_table}_fuzzy\n")
    fp.write(f"{camel_case_table}.read_any = read_{snake_case_table}_any\n")
    fp.write(f"{camel_case_table}.read_one = read_{snake_case_table}_one\n")
    fp.write(f"{camel_case_table}.read_one_or_none = read_{snake_case_table}_one_or_none\n")
    fp.write(f"{camel_case_table}.read_all = read_{snake_case_table}_all\n")
    fp.write(f"{camel_case_table}.delete = delete_{snake_case_table}_by_id\n")
    if id_col is not None:
        fp.write(f"{camel_case_table}.read_by_id = read_{snake_case_table}_by_id\n")
        fp.write(f"{camel_case_table}.delete_by_id = delete_{snake_case_table}_by_id\n")
    fp.write("\n\n\n")

#def generate_python_fast_api_functions_for_db_table(con: Connection, table: str,  fp: TextIO):
#    """
#    Generate corresponding FastAPI functions for DB table operations
#    """

def generate_python_class_file_for_db_table(con: Connection, output_file: str):
    """
    Generate a python class file for each table in the database.
    This is useful for autocompletion and similar IDE integration.

    We do this instead of using the previous solution of SQLAlchemy's ORM
    for a few reasons:
    - The ORM is more complicated than our needs, using Session management and
      automatic object instantiation from foreign keys. More trouble than it's
      worth, we prefer using our own queries and just have objects/methods for
      basic uses. In practice, we have to hand code queries for many cases
      anyway
    - The ORM is slow, at least how we used it.
    - The ORM doesn't easily allow static generation of ORM classes, requiring
      a complex automapper which doesn't allow for IDE integration since it
      relies on reflection. Generating our own classes allows us to have
      IDE integration and typecheaking with beartype
    """
    with open(output_file, "w") as fp:
        fp.write('"""\n')
        fp.write('Autogenerated python classes for database tables\n')
        fp.write(f"Generated on {datetime.datetime.now()} by db module\n")
        fp.write('"""\n')
        fp.write("import dataclasses\n")
        fp.write("import datetime\n")
        fp.write("from beartype.typing import Optional, Generator, List, ClassVar, Type, Dict, Any\n")
        fp.write("import beartype\n")
        fp.write("import odmx.support.db as db\n\n")
        fp.write("def beartype_wrap_init(cls):\n")
        fp.write("    assert dataclasses.is_dataclass(cls)\n")
        fp.write("    cls.__init__ = beartype.beartype(cls.__init__)\n")
        fp.write("    return cls\n\n")
        for table in get_tables(con):
            generate_python_class_for_db_table(con, table, fp)
        fp.write(f"_table_classes_by_name = {{\n")
        for table in get_tables(con):
            snake_case_table = sanitize_snake_case_name(table)
            camel_case_table = generate_camel_case_name(snake_case_table)
            fp.write(f"    '{table}': {camel_case_table},\n")
        fp.write("}\n\n")
        fp.write(f"def get_table_class(table_name: str) -> Optional[Type]:\n")
        fp.write("    return _table_classes_by_name.get(table_name)\n\n")



def test_python_class_file_for_db_table(con: Connection, file_path: str):
    print(f"Importing {file_path}...")
    import importlib
    module_name = file_path.replace("./", "").replace('/', '.').replace('.py', '')
    print(f"module_name: {module_name}")
    module = importlib.import_module(module_name)
    print(f"Successfully imported {module_name}!")
    for table in get_tables(con):
        print(f"Testing {table}...")
        snake_case_table = sanitize_snake_case_name(table)
        camel_case_table = generate_camel_case_name(snake_case_table)
        try:
            TableClass = getattr(module, camel_case_table)
            read_table = getattr(module, f"read_{snake_case_table}")
            read_table_all = getattr(module, f"read_{snake_case_table}_all")
            read_table_one = getattr(module, f"read_{snake_case_table}_one")
            read_table_by_id = getattr(module, f"read_{snake_case_table}_by_id")
            write_table = getattr(module, f"write_{snake_case_table}")
            write_table_obj = getattr(module, f"write_{snake_case_table}_obj")
            objects = read_table(con)
            for obj in objects:
                assert isinstance(obj, TableClass)
            all_objects = read_table_all(con)
            for obj in all_objects:
                assert isinstance(obj, TableClass)

        except AttributeError:
            print(f"Class {camel_case_table} not found in {file_path}!")
            raise
        print(f"Successfully tested {table}!")




class NewColumnsException(Exception):
    """
    If there are columns which are in the data frame but not in the database,
    this exception is raised with a .columns attribute containing the set of
    missing columns
    """
    def __init__(self, message, columns):
        super().__init__(message)
        self.columns = columns

def query_df(con: Connection, table: str, where: Optional[dict] = None) -> DataFrame:
    """
    Returns a table as a DataFrame, optionnally filtered by where paraemters

    @param con The connection to the database
    @param table THe table to query
    @param where A dictionary of parameters to filter the return by. Note that
        values set to None are ignored, this is not filtering by NULL.
    @return The requested table as a DataFrame
    """
    table = quote_id(con, table)
    where_clause = ""
    if where:
        where = {k: v for k, v in where.items() if v is not None}
        where_clause = "WHERE " + " AND ".join(
            [f"{quote_id(con, k)} = {quote_val(con, v)}" for k, v in where])
    query = f"SELECT * FROM {table} {where_clause}"
    return pd.read_sql(query, con)

def generate_json_schema_from_db_table(con: Connection, table: str) -> dict:
    """
    Generates a JSON schema from a database table
    """
    columns = get_column_info(con, table)
    properties = {}
    for column in columns:
        property_type = None
        property_format = None
        python_type = postgres_type_to_python_type(column['data_type'])
        if python_type == 'datetime.datetime':
            property_type= 'string'
            property_format = 'date-time'
        elif python_type == 'datetime.date':
            property_type= 'string'
            property_format = 'date'
        elif python_type == 'datetime.time':
            property_type= 'string'
            property_format = 'time'
        elif python_type == 'int':
            property_type= 'integer'
        elif python_type == 'float':
            property_type= 'number'
        elif python_type == 'bool':
            property_type= 'boolean'
        else:
            property_type= 'string'
            property_format = None
        if column['is_nullable'] == 'YES':
            property_type = [property_type, 'null']
        if column['column_default'] is not None:
            property_type = [property_type, 'null']
        prop = {'type': property_type}
        if property_format is not None:
            prop['format'] = property_format
        if (column['column_default'] is not None and
            column['column_default'].startswith('nextval')):
               prop['minimum'] = 1
        properties[column['column_name']] = prop
    return {
        'type': 'array',
        'items': {
            'type': 'object',
            'properties': properties
        }
    }

def generate_json_schemas_from_db(con: Connection, json_schema_dir: str):
    """
    Generate json schema files from the ODMX database
    """
    if not os.path.exists(json_schema_dir):
        os.makedirs(json_schema_dir)
    elif not os.path.isdir(json_schema_dir):
        raise ValueError(f"{json_schema_dir} is not a directory")
    tables = get_tables(con)
    for table in tables:
        json_schema_file = os.path.join(json_schema_dir, table + '_schema.json')
        print("Generating schema for", table)
        schema = generate_json_schema_from_db_table(con, table)
        if os.path.exists(json_schema_file):
            with open(json_schema_file, 'r') as f:
                try:
                    old_schema = json.load(f)
                except json.decoder.JSONDecodeError:
                    old_schema = {}
            if old_schema == schema:
                print("Schema unchanged from existing file")
                return
            else:
                print("Schema changed from existing file, adding to .new")
                json_schema_file += '.new'
        with open(json_schema_file, 'w') as f:
            json.dump(schema, f, indent=4)



def reset_db(con: Connection, name, sql_template=None):
    """
    Drop and recreate a database, restoring from a sql template if.
    necessary
    """
    drop_database(con, name)
    create_database(con, name)
    print(name)
    con = con.connect(
        f"dbname={name} user={con.info.user} password={con.info.password} "
        f"host={con.info.host} port={con.info.port}")
    if sql_template is not None:
        print(f"Populating {name} from {sql_template}.")
        with open(sql_template) as f:
            sql = f.read()
        # Remove comments
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        # Split into statements
        sql = sql.split(';')
        # Remove empty statements
        sql = [s for s in sql if s.strip()]
        for s in sql:
            con.execute(s)
        con.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Perform operations on/from database')
    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True
    parser_gen_db_class_file = subparsers.add_parser(
        'gen_db_class_file',
        help='Generate a Python class file for a database table')
    parser_gen_db_class_file.add_argument(
        'output_file',
        help='The file to output the Python class to')
    parser_gen_json_schema = subparsers.add_parser(
        'gen_json_schema',
        help='Generate JSON schema files from the database')
    parser_gen_json_schema.add_argument(
        'json_schema_dir',
        help='The directory to output the JSON schema files to')
    config = Config()
    add_db_parameters_to_config(config, add_db_name=True, add_db_schema=True)
    config.add_args_to_argparser(parser)
    args = parser.parse_args()
    config.validate_config(args)
    con = connect(
        config=config)
    if args.command == 'gen_db_class_file':
        generate_python_class_file_for_db_table(con, args.output_file)
        test_python_class_file_for_db_table(con, args.output_file)
    if args.command == 'gen_json_schema':
        generate_json_schemas_from_db(con, args.json_schema_dir)

