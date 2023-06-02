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


class TimeseriesCsvDataSource(DataSource):
    """
    Class for generic timeseries csv data source objects.
    """

    def __init__(self, project_name, project_path, data_path,
                 data_source_timezone, data_source_name, data_source_path):
        self.project_name = project_name
        self.project_path = project_path
        self.data_path = data_path
        self.data_source_timezone = data_source_timezone
        self.data_source_name = data_source_name
        self.feeder_table = f'feeder_{data_source_name}'
        self.data_source_path = data_source_path

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
        cav_paths_list = ssigen.get_files(file_path, '.csv')[1]

        dfs = []
        for site_path in cav_paths_list:
            args = {
                'parse_dates': True,
                'infer_datetime_format': True,
                'float_precision': 'high',
            }
            df = ssigen.open_csv(site_path, args=args, lock=True)

            # Convert datetime to timestamp
            df['datetime'] = pd.to_datetime(df['datetime'],
                                            format='%Y-%m-%d %H:%M:%S')
            df['timestamp'] = df['datetime'].apply(lambda x: int(x.value/10**9))
            if "UTC" in self.data_source_timezone:
                tz_offset = int(self.data_source_timezone[-3:])
                df['timestamp'] = df['timestamp'] + tz_offset

            # Add to list of dataframes
            dfs.append(df)
        df = pd.concat(dfs, ignore_index=True).drop_duplicates()

        # Sort the DataFrame by datetime.
        df.sort_values(by='timestamp', inplace=True)
        df.reset_index(drop=True, inplace=True)

        # The rest of the ingestion is generic.
        general_timeseries_ingestion(feeder_db_con,
                                     feeder_table=self.feeder_table, df=df)

    def process(self, feeder_db_con, odmx_db_con, sampling_feature_code):
        """
        Process ingested gradient data into timeseries datastreams.
        """

        general_timeseries_processing(self, feeder_db_con, odmx_db_con,
                                      sampling_feature_code=\
                                          sampling_feature_code)
