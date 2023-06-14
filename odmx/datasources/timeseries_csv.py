#!/usr/bin/env python3

"""
Module for LBNLSFA gradient data harvesting, ingestion, and processing.

TODO - add equipment json generation
"""

import os
import pandas as pd
import odmx.support.general as ssigen
from odmx.abstract_data_source import DataSource
from odmx.timeseries_ingestion import general_timeseries_ingestion
from odmx.timeseries_processing import general_timeseries_processing


def clean_name(col):
    """
    Generate ingested name from csv column name
    """
    replace_chars = {' ': '_', '-': '_', '/': '_', '²': '2', '³': '3',
                     '°': 'deg', '__': '_', '%': 'percent'}
    new_col = col.lower()
    for key, value in replace_chars.items():
        new_col = new_col.replace(key, value)
    # Make sure our replacement worked
    try:
        new_col.encode('ascii')
    except UnicodeEncodeError as exc:
        raise RuntimeError(f"Column '{new_col}' derived from decagon "
                           "data still has special characters. "
                           "Check the find/replace list") from exc
    return new_col

class TimeseriesCsvDataSource(DataSource):
    """
    Class for generic timeseries csv data source objects.
    """

    def __init__(self, project_name, project_path, data_path,
                 data_source_timezone, data_source_name, data_source_path,
                 equipment_path):
        self.project_name = project_name
        self.project_path = project_path
        self.data_path = data_path
        self.data_source_timezone = data_source_timezone
        self.data_source_name = data_source_name
        self.feeder_table = f'feeder_{data_source_name}'
        self.data_source_path = data_source_path
        self.equipment_path = (f'{equipment_path}/device_{data_source_name}')

    def harvest(self):
        """
        There is no harvesting to be done for this data type. It was all
        provided manually, and so this is a placeholder function.

        For this data source type, data_source_path defines the subdirectory
        within the main data directory to search for csv files. Within that
        subdirectory, there should be a folder for each distinct data source,
        named after the source (device, location, etc.) containing all csvs
        for the source. This module is written to combine all csvs from that
        folder.

        Each file should have a column named 'datetime' and then distinct names
        for each additional column.
        """

    def ingest(self, feeder_db_con):
        """
        Manipulate harvested gradient data in a file on the server into a
        feeder database.
        """

        # Define the file path.
        file_path = os.path.join(self.data_path,
                                self.data_source_path,
                                self.data_source_name)

        # Find all csvs in the path for this data source
        csv_paths_list = ssigen.get_files(file_path, '.csv')[1]

        dfs = []
        for site_path in csv_paths_list:
            args = {
                'parse_dates': True,
                'infer_datetime_format': True,
                'float_precision': 'high',
            }
            df = ssigen.open_csv(site_path, args=args, lock=True)

            # Make sure we have a datetime column, report error if not
            if 'datetime' not in df.columns.tolist():
                print(f"reading data from {site_path}")
                print(f"No datetime column. Available columns are: {df.columns}")
                return

            # Convert datetime to timestamp
            df['timestamp'] = pd.to_datetime(df['datetime'],
                                            format='%Y-%m-%d %H:%M:%S')

            # Add to list of dataframes
            dfs.append(df)
        df = pd.concat(dfs, ignore_index=True).drop_duplicates()

        # Sort the DataFrame by timestamp and drop unused datetime column
        df.sort_values(by='timestamp', inplace=True)
        df.drop(columns='datetime', inplace=True)

        # Ensure column names are compatible with database
        new_cols = []
        for col in df.columns.tolist():
            new_col = clean_name(col)
            new_cols.append(new_col)

        df.columns = new_cols

        # The rest of the ingestion is generic.
        general_timeseries_ingestion(feeder_db_con,
                                     feeder_table=self.feeder_table, df=df)

    def process(self, feeder_db_con, odmx_db_con, sampling_feature_code):
        """
        Process ingested gradient data into timeseries datastreams.
        """

        general_timeseries_processing(self, feeder_db_con, odmx_db_con,
                                      sampling_feature_code=\
                                          sampling_feature_code,
                                          equipment_directory=\
                                              self.equipment_path)
