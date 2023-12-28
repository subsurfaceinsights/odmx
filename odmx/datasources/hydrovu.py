#!/usr/bin/env python3
# pylint: disable=too-many-instance-attributes, no-member
"""
Module for Hydrovu data harvesting, ingestion, and processing.
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
from odmx.write_equipment_jsons import gen_equipment_entry,\
    gen_data_to_equipment_entry, check_diff_and_write_new,\
        read_or_start_data_to_equipment_json
# TODO make manual qa list configurable here

mapper_path = find_spec("odmx.mappers").submodule_search_locations[0]
json_schema_files = find_spec("odmx.json_schema").submodule_search_locations[0]

class HydrovuDataSource(DataSource):
    """
    Class for Hydrovu data source objects.
    """

    def __init__(self, project_name,project_path, data_path, device_name,
                 device_type, device_id, data_source_timezone):
        self.data_source_path = (f'{data_path}/hydrovu/{device_name}_'
                                 f'{device_type}.csv')
        self.project_name = project_name
        self.project_path = project_path
        self.device_name = device_name
        self.device_type = device_type
        self.device_id = device_id
        device_type = device_type.lower()
        if device_type == 'vulink':
            self.device_code = "VuLink"
        elif device_type == 'at500':
            self.device_code = "Aqua TROLL 500 Data Logger"
        elif device_type == 'at200':
            self.device_code = "Aqua TROLL 200 Data Logger"
        else:
            raise ValueError(f"Unknown Device type {device_type}")
        self.equipment_directory = (f'{project_path}/hydrovu/{device_name}_'
                                    f'{device_type}')
        self.feeder_table = f'{device_name.lower()}_{device_type}'
        self.data_source_timezone = data_source_timezone
        # There are two ids with clean_name "density" but both map to the same
        # cv term, so don't check for duplicates (doesn't matter)
        # We don't use level_elevation so that cv_term is intentionally null
        self.param_df = pd.DataFrame(
            open_json(f'{mapper_path}/hydrovu_parameters.json'))
        self.param_df.set_index('id', inplace=True, verify_integrity=True)

        self.unit_df = pd.DataFrame(
            open_json(f'{mapper_path}/hydrovu_units.json'))
        self.unit_df.set_index('id', inplace=True, verify_integrity=True)

    @cache
    def get_bearer_token(self, auth_yml):
        """Get bearer token"""

        # Define parameters for the API call.
        auth = auth_yml
        auth_file = f"{os.environ.get('SSI_BASE', '/opt/ssi')}/{auth}"
        with open(auth_file, encoding="utf-8") as file:
            auth_params = yaml.load(file, Loader=yaml.FullLoader)

        auth_url = auth_params['auth_url']
        client_id = auth_params['client_id']
        client_secret = auth_params['client_secret']
        token_params = {"client_id": client_id,
                        "client_secret": client_secret,
                        "grant_type": "client_credentials",
                        "scope": "read:locations"}
        r = requests.post(auth_url, token_params)
        r.raise_for_status()
        token = r.json()['access_token']
        return token

    def harvest(self, auth_yml):
        """
        Harvest Hydrovu data using their public API and save it as a
        .csv to our servers.
        """
        base_url = 'https://www.hydrovu.com/public-api/v1/'
        token = self.get_bearer_token(auth_yml)
        data_request = Template(f"{base_url}locations/"
                                "${alias}/data?startTime=${start_time}")
        headers = {'accept': 'application/json',
                   'authorization': f"Bearer {token}"}

        location_id = self.device_id
        file_name = self.data_source_path
        vprint(' harvesting to file ', file_name)
        if os.path.exists(file_name):
            # Read the file in as a pandas dataframe.
            existing_df = pd.read_csv(file_name)
            # Set the timestamp column as the index.
            existing_df.set_index('timestamp', inplace=True)
            # Sort the dataframe by the index.
            existing_df.sort_index(inplace=True)
            # get last timestamp as unix utc
            # vprint('existing_df: ', existing_df)
            last_timestamp = existing_df.index[-1] + 1  # Add one for API
            # reasons
        else:
            existing_df = pd.DataFrame()
            last_timestamp = 0
        count = 0
        while True:
            vprint("last_timstamp : ",
                   datetime.datetime.fromtimestamp(last_timestamp))
            vprint('location_id : ', location_id)
            vprint("name : ", self.device_name)
            feature_url = data_request.substitute(alias=location_id,
                                                  start_time=last_timestamp)
            data = requests.get(feature_url, headers=headers).json()
            # vprint('data : ', data)
            # Check if anything was returned.
            data_df = pd.DataFrame()
            # vprint('data_df : ', data_df)
            # vprint('data[parameters] : ', data['parameters'])
            for param in data['parameters']:
                # vprint('param : ', param)
                nice_name = self.param_df['nice_name'][param['parameterId']]
                nice_unit = self.unit_df['nice_name'][param['unitId']]
                col_name = f"{nice_name}[{nice_unit}]"
                readings = param['readings']
                param_data = pd.DataFrame(readings,
                                          columns=['timestamp', 'value'])
                param_data = param_data.set_index('timestamp')
                param_data.rename(columns={'value': col_name}, inplace=True)
                data_df = pd.concat((data_df, param_data), axis=1)
            print(data_df)
            vprint((f"hydrovu: Data collected for {self.device_name} "
                    f"page {count}"))
            if len(data_df) == 0:
                break
            count += 1
            # vprint('existing_df : ', existing_df)
            # vprint('data_df : ', data_df)
            existing_df = pd.concat((existing_df, data_df))
            last_timestamp = data_df.index[-1] + 1
        if count > 0:
            existing_df.to_csv(file_name)
            vprint(f"hydrovu: Data saved for {self.device_name}")
        else:
            vprint(f"hydrovu: No new data for {self.device_name}")

    def ingest(self, feeder_db_con, update_equipment_jsons):
        """
        Manipulate harvested Hydrovu data in a file on the server into a feeder
        database.
        """
        csv_path = self.data_source_path
        vprint(f"Reading {csv_path}")
        # Get the actual data.
        args = {'float_precision': 'high'}
        df: pd.DataFrame = pd.DataFrame(open_csv(csv_path, args=args,
                                                 lock=True))

        # Rename all of the column headers.
        new_cols = []
        for col in df.columns.tolist():
            new_col = clean_name(col)
            new_cols.append(new_col)

        # Write equipment jsons if update_equipment_jsons is true
        if update_equipment_jsons:
            equip_path = (f"{self.project_path}/odmx/equipment/"
                          f"{self.equipment_directory}")

            equip_file = f"{equip_path}/equipment.json"
            data_to_equipment_map_file = (f"{equip_path}/"
                                          "data_to_equipment_map.json")
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
                    name=self.device_type,
                    code=self.device_code,
                    serial_number=self.device_id)

            # Retrieve device uuid from equipment dict
            dev_uuid = equipment['equipment_uuid']

            # Same for data to equipment map
            data_to_equip, col_list =\
            read_or_start_data_to_equipment_json(data_to_equipment_map_file,
                                                 equipment)

            # Setup mappers with column names for lookup
            param_lookup = self.param_df.set_index('clean_name')
            unit_lookup = self.unit_df.set_index('clean_name')

            for column_name in new_cols:
                if column_name in col_list:
                    continue
                name, unit_name = column_name.split("[")
                variable_domain_cv = "instrumentMeasurement"
                variable_term = param_lookup['cv_term'][name]
                unit = unit_lookup['cv_term'][unit_name[:-1]]
                expose_as_datastream = True
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

        df.columns = new_cols
        df.set_index('timestamp', inplace=True)
        # Convert unix timestamp to utc timestamp (without timzone)
        df['timestamp'] = pd.to_datetime(df.index, unit='s')
        general_timeseries_ingestion(feeder_db_con, self.feeder_table, df)

    def process(self, feeder_db_con, odmx_db_con, sampling_feature_code):
        """
        Process ingested Hydrovu data into timeseries datastreams.
        """
        general_timeseries_processing(self, feeder_db_con, odmx_db_con,
                                      sampling_feature_code=\
                                          sampling_feature_code)
