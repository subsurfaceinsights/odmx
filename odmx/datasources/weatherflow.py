#!/usr/bin/env python3

"""
Module for WeatherFlow data harvesting, ingestion, and processing.
"""

import os
import pandas as pd
from odmx.support.file_utils import get_files, open_csv
from odmx.abstract_data_source import DataSource
from odmx.harvesting import simple_rsync
from odmx.timeseries_ingestion import general_timeseries_ingestion
from odmx.timeseries_processing import general_timeseries_processing


class WeatherflowDataSource(DataSource):
    """
    Class for WeatherFlow data source objects.
    """

    def __init__(self, ds_dict):
        self.shared_info = ds_dict['shared_info']
        self.harvesting_info = ds_dict['harvesting_info']
        self.ingestion_info = ds_dict['ingestion_info']
        self.processing_info = ds_dict['processing_info']

    def harvest(self):
        """
        Harvests data from a WeatherFlow station.
        """

        # Define variables.
        remote_user = self.harvesting_info['remote_user']
        remote_server = self.harvesting_info['remote_server']
        remote_base_path = self.harvesting_info['remote_base_path']
        local_base_path = self.shared_info['data_source_path']
        pull_list = self.harvesting_info['pull_list']

        # Pull the data.
        simple_rsync(remote_user, remote_server, remote_base_path,
                     local_base_path, pull_list)

    def ingest(self):
        """
        Manipulate harvested WeatherFlow data in a file on the server into a
        feeder database.
        """

        # Define the file path.
        file_path = os.path.join(self.shared_info['data_source_path'])
        # Log the .csv files.
        csv_paths = get_files(file_path, '.csv')[1]
        # Create a DataFrame for all of the files.
        dfs = []
        for csv_path in csv_paths:
            args = {'float_precision': 'high',}
            df = open_csv(csv_path, args=args, lock=True)
            # One of the WeatherFlow data headers has commas in it, and as such
            # is read in improperly above. We fix that here.
            for i, col in enumerate(df.columns.tolist()):
                if col.startswith('precipitation_type'):
                    correct_col = (f'{col},{df.columns.tolist()[i + 1]},'
                                   f'{df.columns.tolist()[i + 2]},'
                                   f'{df.columns.tolist()[i + 3]}')
                    last_cols = df.columns.tolist()[i + 4:]
                    new_cols = (df.columns.tolist()[:i] + [correct_col]
                                + last_cols)
                    df.drop(columns=df.columns.tolist()[i + 5:],
                            inplace=True)
                    df.columns = new_cols
                    break
            # Check to make sure all of the timestamps are appropriate.
            try:
                df['time_epoch[s]'] = pd.to_datetime(df['time_epoch[s]'],
                                                     unit='s')
            except OverflowError as e:
                raise OverflowError(
                    f"One of the timestamps in file {csv_path} cannot be"
                    " converted properly. Please check to see what's wrong."
                ) from e
            dfs.append(df)
        df = pd.concat(dfs, ignore_index=True).drop_duplicates()

        # Make sure the column headers are lower case.
        df.columns = df.columns.str.lower()
        # Sort by the timestamp column.
        df.rename(columns={'time_epoch[s]': 'timestamp'}, inplace=True)
        df.sort_values(by='timestamp', inplace=True)
        df.reset_index(drop=True, inplace=True)

        # The rest of the ingestion is generic.
        general_timeseries_ingestion(self, df)

    def process(self, odmx_db_con):
        """
        Process ingested WeatherFlow data into timeseries datastreams.
        """
        general_timeseries_processing(self, odmx_db_con)
