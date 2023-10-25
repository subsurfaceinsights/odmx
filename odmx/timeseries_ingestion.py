#!/usr/bin/env python3

"""
A script to house common timeseries ingestion functions.
"""

import numpy as np
import odmx.support.math as ssimath
from odmx.support import db
from odmx.support.db import quote_id

def get_latest_timestamp(feeder_db_con, feeder_table):
    """ Get latest timestamp"""
    if not db.does_table_exist(feeder_db_con, feeder_table):
        return None
    if db.is_table_empty(feeder_db_con, feeder_table):
        return None
    query = f'''
        SELECT timestamp FROM
            {quote_id(feeder_db_con,
                       feeder_table)}
            ORDER BY timestamp DESC LIMIT 1
    '''
    result = feeder_db_con.execute(query).fetchone()[0]
    return result

def general_timeseries_ingestion(feeder_db_con, feeder_table, df):
    """
    Perform the general ingestion routine for timeseries data.

    @param obj The class object for the data type in question.
    @param df A DataFrame containing the data we want to ingest, in a form
              that's almost ready for ingestion.
    """


    # Make sure that the columns are unique
    if len(df.columns) != len(set(df.columns)):
        raise ValueError("Ingestion DataFrame must have unique columns")

    # As a final check on the data, drop any missed duplicates.
    df.drop_duplicates('timestamp', inplace=True, ignore_index=True)
    # Ensure that the timestamp column is the first column in order.
    cols = df.columns.tolist()
    cols.insert(0, cols.pop(cols.index('timestamp')))
    df = df.loc[:, cols]

    # At read-in, our DataFrame might have numbers, NaNs, and text. That text
    # should be None (NULL in PostgreSQL), but having a DataFrame with both NaN
    # and None in the same column is... tricky. We have a fix, though. First,
    # figure out where (aside from the timestamps) we don't have floats. Since
    # Python treats NaN as a float, anything that isn't a float should
    # eventually go to Null in PostgreSQL.
    floats = df[df.columns[1:]].applymap(ssimath.is_not_number)
    # Before we do anything about that, we convert all NaN values to strings
    # that read 'NaN'. When we send the DataFrame to PostgreSQL, the database
    # will read that string as an actual NaN, and so preserve it.
    df[df.columns[1:]] = df[df.columns[1:]].replace([np.nan], 'NaN')
    # Next, where the initial not-float mapping read True, we set those values
    # to None, which in this case pandas will force into a real NaN. This is
    # okay, though, because on read-in to PostgreSQL, pandas will actually send
    # true NaNs as None values. It's super dumb.
    df[df.columns[1:]] = np.where(floats, None, df[df.columns[1:]])

    # With that nonsense out of the way, check to see if the table exists in
    # the database.
    print(f"Checking to see if \"{feeder_table}\" exists"
          " in the database.")
    table_exist = db.does_table_exist(feeder_db_con,
                                          feeder_table)
    # If it doesn't exist, create it.
    if not table_exist:
        print(f"\"{feeder_table}\" doesn't exist yet."
              " Creating it.")
        # Since pandas.to_sql doesn't support primary key creation, we need
        # to handle that. First we make the table in PostgreSQL.
        query = f'''
            CREATE TABLE {quote_id(feeder_db_con,
                           feeder_table)}
                (index SERIAL PRIMARY KEY)
        '''
        feeder_db_con.execute(query)
        add_columns(feeder_db_con, feeder_table,
                df.columns.tolist())
    # If it does exist, we only want to add new data to it.
    else:
        # First check if we added any columns
        print(f"\"{feeder_table}\" already exists.")
        existing_columns = set(db.get_columns(feeder_db_con,
                                                  feeder_table))
        data_columns = set(df.columns.tolist())
        new_columns = data_columns - existing_columns
        if len(new_columns) > 0:
            print(f"Adding new columns '{new_columns}'")
            add_columns(feeder_db_con, feeder_table,
                    new_columns)
        last_time = get_latest_timestamp(feeder_db_con,
                                         feeder_table)
        print (f"Last timestamp in {feeder_table} is {last_time}")
        if last_time is not None:
            df = df[df['timestamp'] > last_time]
        else:
            print(f"\"{feeder_table}\" is empty. Adding"
                  " to it.")
        # If the DataFrame is empty, there's no new data to ingest.
        if df.empty:
            print("No new data to ingest.\n")
            return

    print(f"Populating {feeder_table}.")
    # TODO the records generation is the slowest step here. We should
    # probably try to optimize it.
    db.insert_many_df(feeder_db_con, feeder_table, df, upsert=False)

    # Add the values in.
    #check_cols = ['timestamp']
    #write_cols = df.columns.tolist()
    #update_cols = df.columns.tolist()
    #db.df_to_sql(df, feeder_con, feeder_schema,
                     #feeder_table, check_cols,
                     #write_cols, update_cols)


def add_columns(feeder_con, feeder_table, columns):
    """
    Adds columns to a given table
    """
    # Now we need to add columns and define their types. Construct a string
    # for that.
    col_str = ''
    for i, col in enumerate(columns):
        col_type = 'double precision'
        if col == 'timestamp':
            col_type = 'timestamp without time zone NOT NULL'
        col_str = f'{col_str}ADD COLUMN \"{col}\" {col_type}'
        if i < len(columns) - 1:
            col_str = f'{col_str}, '
    query = f'''
        ALTER TABLE {quote_id(feeder_con,
                           feeder_table)}
            {col_str}
    '''
    return feeder_con.execute(query)
