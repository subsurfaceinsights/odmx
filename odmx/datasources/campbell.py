#!/usr/bin/env python3

"""
Module for Campbell data arvesting, ingestion, and processing.
"""

import os
import pandas as pd
import odmx.support.general as ssigen
import odmx.support.db as db
from odmx.abstract_data_source import DataSource
from odmx.harvesting import simple_rsync
from odmx.timeseries_ingestion import general_timeseries_ingestion
from odmx.timeseries_processing import general_timeseries_processing

class CampbellDataSource(DataSource):
    """
    Class for Campbell data source objects.feeder_schema
    """

    def __init__(self,
                 project_name,
                 project_path,
                 data_path,
                 data_source_path,
                 data_source_timezone,
                 feeder_table):
        self.project_name = project_name
        self.project_path = project_path
        self.data_path = data_path
        self.data_source_path = data_source_path
        self.data_source_timezone = data_source_timezone
        self.feeder_table = feeder_table

    def harvest(self, remote_user, remote_server, remote_base_path, pull_list):
        """
        Harvests data from a remote server.
        """

        # Define variables.
        local_base_path = self.data_source_path

        # Pull the data.
        simple_rsync(remote_user, remote_server, remote_base_path,
                     local_base_path, pull_list)

    def ingest(self, feeder_db_con: db.Connection):
        """
        Manipulate harvested Campbell data in a file on the server into a
        feeder database.
        """

        # Define the path for files.
        file_path = os.path.join(self.data_path, self.data_source_path)
        # Log the .dat files (they're actually .csv files).
        dat_paths_list = ssigen.get_files(file_path, '.dat')[1]
        # Create a DataFrame for all of the files.
        dfs = []
        for dat_path in dat_paths_list:
            args = {
                'skiprows': [0, 2, 3],
                'na_values': ['NAN', 'Null'],
                'float_precision': 'high',
            }
            # TODO this takes a lot of memory, this should definitely be
            # checked against the existing table and only new data should be
            # processed
            df = ssigen.open_csv(dat_path, args=args, lock=True)
            dfs.append(df)
        df = pd.concat(dfs, ignore_index=True).drop_duplicates()

        # Make sure the column headers are lower case.
        df.columns = df.columns.str.lower()
        # Turn the datetime column into an actual datetime, and sort by it.
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.sort_values(by='timestamp', inplace=True)
        df.reset_index(drop=True, inplace=True)

        # The rest of the ingestion is generic.
        general_timeseries_ingestion(feeder_db_con, self.feeder_table, df)

    def process(self, feeder_db_con: db.Connection, odmx_db_con: db.Connection, sampling_feature_code: str, equipment_directory: str):
        """
        Process ingested Campbell data into timeseries datastreams.
        """

        general_timeseries_processing(self,
                                      feeder_db_con,
                                      odmx_db_con,
                                      sampling_feature_code=sampling_feature_code,
                                      equipment_directory=equipment_directory)
