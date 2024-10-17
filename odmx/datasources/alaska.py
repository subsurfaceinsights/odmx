#!/usr/bin/env python3
# pylint: disable=too-many-instance-attributes, no-member
"""
Module for Alaska data harvesting, ingestion, and processing.
"""

import os
from importlib.util import find_spec
from string import Template
import datetime
from functools import cache
import requests
from tqdm import tqdm
import yaml
import pytz
import pandas as pd
from odmx.support.file_utils import open_csv, open_json, clean_name
from odmx.abstract_data_source import DataSource
from odmx.timeseries_ingestion import general_timeseries_ingestion
from odmx.timeseries_processing import general_timeseries_processing
from odmx.log import vprint
from odmx.write_equipment_jsons import gen_equipment_entry, \
    gen_data_to_equipment_entry, check_diff_and_write_new, \
    read_or_start_data_to_equipment_json
# TODO make manual qa list configurable here

mapper_path = find_spec("odmx.mappers").submodule_search_locations[0]
json_schema_files = find_spec("odmx.json_schema").submodule_search_locations[0]


class AlaskaDataSource(DataSource):
    """
    Class for Alaska data source objects.
    """

    def __init__(self, project_name, project_path, equipment_path, data_path,
                 data_source_timezone, data_source_path):
        self.project_name = project_name
        self.project_path = project_path
        self.data_path = data_path
        self.data_source_timezone = data_source_timezone
        self.data_source_path = data_source_path
        self.feeder_table = "data_file"
        self.equipment_directory = equipment_path
        self.param_df = pd.DataFrame(
            open_json(f'{mapper_path}/{project_name}.json'))
        # self.param_df.set_index(
        #    "clean_name", inplace=True, verify_integrity=True)

    def harvest(self):
        """
        Pull data from the Campbell data logger over HTTP.
        """

        # Define where the data should be harvested to.
        local_base_path = os.path.join(self.data_path, self.data_source_path)
        # First check to make sure the proper directory exists.
        os.makedirs(local_base_path, exist_ok=True)
        # Set the name of the file we will harvest
        file_name = 'data_file.csv'
        file_path = os.path.join(local_base_path, file_name)

        tz = pytz.timezone(self.data_source_timezone)

        # Add a simple check to see if the last time-stamp in the file is the same as now
        # If we already have this data ingested we want to check the latest timestamp
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            # Read the last row of the CSV to get the most recent timestamp
            try:
                # Create a DataFrame of the file.
                args = {'float_precision': 'high', 'skiprows': [0, 2, 3]}
                df = pd.read_csv(file_path, **args)
                last_timestamp = df['TIMESTAMP'].max()

                if isinstance(last_timestamp, str):
                    last_timestamp = datetime.datetime.strptime(
                        last_timestamp, '%Y-%m-%d %H:%M:%S')

                # Get the current time in the specified timezone
                current_time = datetime.datetime.now(tz)

                # If last_timestamp is naive (no timezone), make it aware
                if last_timestamp.tzinfo is None:
                    last_timestamp = tz.localize(last_timestamp)

                # Now we can directly compare the two timezone-aware datetimes
                time_difference = current_time - last_timestamp
                if time_difference.total_seconds() <= 3600:
                    print(
                        f"Data already up-to-date as of {last_timestamp}. Skipping download.")
                    return
            except Exception as e:
                print(
                    f"Error reading the file: {e}. Proceeding with download.")

        # TODO: Build the query so that we can control the time range we pull over
        if self.project_name == 'oliktok':
            # Set the base URL for the Oliktok data logger
            base_url = "http://remote_ip/"
            # Build the query that we will append to the base URL
            query = "tables.html?command=DataQuery&mode=since-record&format=toa5&uri=dl:Oliktok_ERT_Data&p1=0"
        elif self.project_name == 'utqiagvik':
            base_url = "http://remote_ip/"
            query = "tables.html?command=DataQuery&mode=since-record&format=toa5&uri=dl:Utqiavik_Data&p1=0"
        else:
            ValueError(
                f'{self.project_name} does not match data source harvesting')
        url = base_url + query

        try:
            # Send a request to get the data in stream mode (chunked)
            with requests.get(url, auth=("user", "pass"), stream=True, timeout=30) as response:
                # Check if the request was successful
                if response.status_code == 200:
                    # Get the total file size from the headers (if available)
                    total_size = int(response.headers.get(
                        'content-length', 0)) or None

                    # Open a file to write the downloaded content
                    with open(file_path, 'wb') as f:
                        # Use tqdm to show progress, chunk size set to 1024 bytes
                        for data in tqdm(response.iter_content(chunk_size=1024),
                                         total=total_size // 1024 if total_size else None,
                                         unit='KB', desc="Downloading data"):
                            if not data:  # Break if no more data is received
                                break
                            f.write(data)
                    print("Data downloaded successfully and saved as CSV.")
                    return

                else:
                    print(
                        f"Failed to download data. HTTP Status Code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

    def ingest(self, feeder_db_con, update_equipment_jsons):
        """
        Manipulate harvested Hydrovu data in a file on the server into a feeder
        database.
        """

        # Bring in the data that we harvested
        local_base_path = os.path.join(self.data_path, self.data_source_path)
        file_name = 'data_file.csv'
        file_path = os.path.join(local_base_path, file_name)
        args = {'float_precision': 'high', 'skiprows': [0, 2, 3]}
        df = open_csv(file_path, args=args, lock=True)

        # At Oliktok there is some lost data that is not on the logger
        if self.project_name == 'oliktok':
            lost_data_file = 'Oliktok_Point_Remote_Oliktok_ERT_Data.dat'
            lost_file_path = os.path.join(local_base_path, lost_data_file)
            df_lost = open_csv(lost_file_path, args=args, lock=True)

            # Combine the dataframes, keeping only unique rows based on the 'timestamp' column
            df = pd.concat([df_lost, df]).drop_duplicates(
                subset='TIMESTAMP')
            # Sort by 'timestamp' to ensure the dates are in chronological order
            df = df.sort_values(by='TIMESTAMP').reset_index(drop=True)
            # Reset the 'record' column as an index starting from 0
            df['record'] = df.index

        # Rename datetime column to timestamp for compatibiltiy with general
        # ingestion
        df['timestamp'] = pd.to_datetime(
            df['TIMESTAMP'], format="%Y-%m-%d %H:%M:%S")
        # df['timestamp'] = df['timestamp'].dt.tz_convert('America/Anchorage')
        # df['timestamp'] = df['timestamp'].dt.tz_localize(None)
        df.drop(columns='TIMESTAMP', inplace=True)
        df.sort_values(by='timestamp', inplace=True)
        df.reset_index(drop=True, inplace=True)

        # Drop columns where the header contains 'anonymous' -- this corresponds to equipment
        df = df.drop(
            columns=[col for col in df.columns if 'anonymous' in col.lower()])

        new_cols = df.columns.tolist()

        # Write equipment jsons if update_equipment_jsons is true
        if update_equipment_jsons:
            equip_path = (f"{self.project_path}/odmx/equipment/"
                          f"{self.equipment_directory}")

            equip_file = f"{equip_path}/equipment.json"
            data_to_equipment_map_file = (f"{equip_path}/"
                                          "data_to_equipment_map.json")

            # Get start timestamp from dataframe
            start = int(df.iloc[0]['timestamp'].timestamp())

            # Read equipment.json if it exists, otherwise start new
            if os.path.isfile(equip_file):
                equip_schema = os.path.join(json_schema_files,
                                            'equipment_schema.json')
                equipment = open_json(equip_file,
                                      validation_path=equip_schema)[0]
            else:
                os.makedirs(self.equipment_directory, exist_ok=True)
                equipment = gen_equipment_entry(
                    acquiring_instrument_uuid=None,
                    name="unknown sensor",
                    code="unknown sensor",
                    serial_number=None,
                    relationship_start_date_time_utc=start,
                    position_start_date_utc=start,
                    vendor="Campbell")

            # Retrieve device uuid from equipment dict
            dev_uuid = equipment['equipment_uuid']

            # Same for data to equipment map
            data_to_equip, col_list =\
                read_or_start_data_to_equipment_json(data_to_equipment_map_file,
                                                     equipment)

            # Setup mappers with column names for lookup
            lookup_df = self.param_df.set_index("clean_name")

            for column_name in new_cols:
                if column_name in col_list:
                    continue
                variable_domain_cv = "instrumentMeasurement"
                try:
                    variable_term = lookup_df['cv_term'][column_name]
                    unit = lookup_df['cv_unit'][column_name]
                    expose_as_datastream = True
                except KeyError:
                    variable_term = None
                if variable_term is None:
                    continue
                data_to_equip.append(
                    gen_data_to_equipment_entry(
                        column_name=column_name,
                        var_domain_cv=variable_domain_cv,
                        acquiring_instrument_uuid=dev_uuid,
                        variable_term=variable_term,
                        expose_as_ds=expose_as_datastream,
                        units_term=unit))
            # Write the new files
            print("Writing equipment jsons.")
            check_diff_and_write_new(data_to_equip, data_to_equipment_map_file)
            check_diff_and_write_new([equipment], equip_file)

        # The rest of the ingestion is generic.
        general_timeseries_ingestion(feeder_db_con,
                                     feeder_table=self.feeder_table, df=df)

    def process(self, feeder_db_con, odmx_db_con, sampling_feature_code):
        """
        Process ingested Hydrovu data into timeseries datastreams.
        """
        general_timeseries_processing(self, feeder_db_con, odmx_db_con,
                                      sampling_feature_code=sampling_feature_code)
