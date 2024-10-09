#!/usr/bin/env python3
# pylint: disable=too-many-instance-attributes, no-member
"""
Module for Oliktok data harvesting, ingestion, and processing.
"""

import os
from importlib.util import find_spec
from string import Template
import datetime
from functools import cache
import requests
import yaml
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


class OliktokDataSource(DataSource):
    """
    Class for Oliktok data source objects.
    """

    def __init__(self, project_name, project_path, equipment_path, data_path,
                 data_source_timezone, data_source_path):
        self.project_name = project_name
        self.project_path = project_path
        self.data_path = data_path
        self.data_source_timezone = data_source_timezone
        self.data_source_path = data_source_path
        self.feeder_table = "oliktok_ert_data"
        self.equipment_directory = equipment_path
        self.param_df = pd.DataFrame(open_json(f'{mapper_path}/oliktok.json'))
        # self.param_df.set_index(
        #    "clean_name", inplace=True, verify_integrity=True)

    def harvest(self):
        """
        Nothing to harvest at the moment.
        """

    def ingest(self, feeder_db_con, update_equipment_jsons):
        """
        Manipulate harvested Hydrovu data in a file on the server into a feeder
        database.
        """

        # Define the file name and path.
        local_base_path = os.path.join(self.data_path, self.data_source_path)
        file_name = 'Oliktok_ERT_Data.csv'
        file_path = os.path.join(local_base_path, file_name)
        print(f'file_path: {file_path}')
        # Create a DataFrame of the file.
        args = {'float_precision': 'high',
                'skiprows': [0, 2, 3]}
        df = open_csv(file_path, args=args, lock=True)

        # Rename datetime column to timestamp for compatibiltiy with general
        # ingestion
        df['timestamp'] = pd.to_datetime(
            df['TIMESTAMP'], format="%m/%d/%y %H:%M")
        # df['timestamp'] = df['timestamp'].dt.tz_convert('America/Anchorage')
        # df['timestamp'] = df['timestamp'].dt.tz_localize(None)
        df.drop(columns='TIMESTAMP', inplace=True)
        df.sort_values(by='timestamp', inplace=True)
        df.reset_index(drop=True, inplace=True)

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
