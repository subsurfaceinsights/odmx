#!/usr/bin/env python3

"""
Module for LBNLSFA gradient data harvesting, ingestion, and processing.

TODO - add equipment json generation
"""

import os
import pandas as pd
from odmx.support.file_utils import get_files, open_csv
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

class GenericTimeseriesDataSource(DataSource):
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
        self.equipment_path = equipment_path

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

        Each file should have a column named 'datetime' with datetimes in the
        timezone specified for the datasource, formatted as YYYY-MM-DD HH:MM:SS
        and then distinct names for each additional column.

        Equipment jsons must be prepared manually.
        """

    def ingest(self, feeder_db_con):
        """
        Manipulate harvested data in a file on the server into a feeder table.
        """
        # Read the data source config information
        config_file = os.path.dirname(self.data_source_path) + \
            '/data_source_config.json'
        data_source_config = ssigen.open_json(config_file)
        ext = data_source_config["data_file_extension"]
        tabs = data_source_config["data_file_tabs"]
        columns = pd.DataFrame(data_source_config["data_columns"])

        # Only take the columns for the current sampling feature
        columns = columns[columns["sampling_feature"]==self.data_source_name]

        # Find all csvs in the path for this data source
        csv_paths_list = get_files(file_path, '.csv')[1]

        dfs = []
        for site_path in csv_paths_list:
            # Set up args for reading files
            args = {'parse_dates': True}

            if ext in ['.xlsx', '.xls']:
                tab_dfs = []
                for tab in tabs:
                    args.update({'sheet_name': tab['name'],
                                 'usecols': list(columns['name'])})
                    tab_dfs.append(ssigen.open_spreadsheet(site_path,
                                                           args=args,
                                                           lock=True))
                df = pd.concat(tab_dfs, ignore_index=True)
            elif ext == '.csv':
                args.update({'infer_datetime_format': True,
                             'float_precision': 'high',
                             'usecols': list(columns['name'])})
                df = ssigen.open_csv(site_path, args=args, lock=True)

            mapper = {}
            for col in df.columns.tolist():
                mapper.update({col: columns[columns['name']==col]['clean_name']})

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
        Process ingested data into timeseries datastreams.
        """

        general_timeseries_processing(self, feeder_db_con, odmx_db_con,
                                      sampling_feature_code=\
                                          sampling_feature_code,
                                          equipment_directory=\
                                              self.equipment_path)
