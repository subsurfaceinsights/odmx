#!/usr/bin/env python3
# pylint: disable=too-many-instance-attributes, no-member
"""
Module for Hydrovu data harvesting, ingestion, and processing.
"""

import os
from string import Template
import json
import uuid
import datetime
import shutil
from deepdiff import DeepDiff
import requests
import yaml
import pandas as pd
import odmx.support.general as ssigen
from odmx.abstract_data_source import DataSource
from odmx.timeseries_ingestion import general_timeseries_ingestion
from odmx.timeseries_processing import general_timeseries_processing
from odmx.log import vprint
from odmx.write_equipment_jsons import get_mapping,\
    gen_equipment_entry, gen_data_to_equipment_entry
from functools import cache
# TODO make manual qa list configurable here


class HydrovuDataSource(DataSource):
    """
    Class for Hydrovu data source objects.
    """

    def __init__(self, project_name, project_path, data_path, device_name, device_type, device_id, data_source_timezone):
        self.data_source_path = f'{data_path}/hydrovu/{device_name}_{device_type}.csv'
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
            raise Exception(f"Unknown Device type {device_type}")
        self.equipment_directory = f'{project_path}/hydrovu/{device_name}_{device_type}'
        self.feeder_table = f'{device_name.lower()}_{device_type.lower()}'
        self.data_source_timezone = data_source_timezone
        self.param_df = pd.DataFrame([{"id": "10",
                                       "nice_name": "Specific Conductivity",
                                       "clean_name": "specific_conductivity",
                                       "cv_term": "waterSpecificConductance"},
                                      {"id": "11",
                                       "nice_name": "Resistivity",
                                       "clean_name": "resistivity",
                                       "cv_term": "waterResistivity"},
                                      {"id": "12",
                                       "nice_name": "Salinity",
                                       "clean_name": "salinity",
                                       "cv_term": "waterSalinity"},
                                      {"id": "13",
                                       "nice_name": "Total Dissolved Solids",
                                       "clean_name": "total_dissolved_solids",
                                       "cv_term": "waterTotalDissolvedSolids"},
                                      {"id": "14",
                                       "nice_name": "Density",
                                       "clean_name": "density",
                                       "cv_term": "waterDensity"},
                                      {"id": "16",
                                       "nice_name": "Baro",
                                       "clean_name": "baro",
                                       "cv_term": ("airAtmosphericPressure"
                                                   "Absolute")},
                                      {"id": "17",
                                       "nice_name": "pH",
                                       "clean_name": "ph",
                                       "cv_term": "waterpH"},
                                      {"id": "18",
                                       "nice_name": "pH MV",
                                       "clean_name": "ph_mv",
                                       "cv_term": "waterpHmv"},
                                      {"id": "19",
                                       "nice_name": "ORP",
                                       "clean_name": "orp",
                                       "cv_term": "waterORP"},
                                      {"id": "1",
                                       "nice_name": "Temperature",
                                       "clean_name": "temperature",
                                       "cv_term": "waterTemperature"},
                                      {"id": "45",
                                       "nice_name": "Pulse",
                                       "clean_name": "pulse",
                                       "cv_term": "pulse"},
                                      {"id": "2",
                                       # TODO: confirm pressure type
                                       "nice_name": "Pressure",
                                       "clean_name": "pressure",
                                       "cv_term": "waterPressureAbsolute"},
                                      {"id": "3",
                                       "nice_name": "Depth",
                                       "clean_name": "depth",
                                       # We don't expose this parameter
                                       "cv_term": None},
                                      {"id": "4",
                                       "nice_name": "Level: Depth to Water",
                                       "clean_name": "level_depth_to_water",
                                       # TODO: check NAD83 datum is appropriate
                                       "cv_term": "waterDepthBelow"
                                       "TopOfCasing"},
                                      {"id": "9",
                                       "nice_name": "Actual Conductivity",
                                       "clean_name": "actual_conductivity",
                                       "cv_term": ("waterElectrical"
                                                   "Conductivity")},
                                      {"id": "20",
                                       "nice_name": "DO",
                                       "clean_name": "do",
                                       "cv_term": ("waterOpticalDissolved"
                                                   "Oxygen")},
                                      {"id": "21",
                                       "nice_name": "% Saturation O₂",
                                       "clean_name": "percent_saturation_o2",
                                       "cv_term": ("waterOpticalDissolved"
                                                   "OxygenPercentAir"
                                                   "Saturation")},
                                      {"id": "25",
                                       "nice_name": "Turbidity",
                                       "clean_name": "turbidity",
                                       "cv_term": "waterTurbidityNTU"},
                                      {"id": "30",
                                       "nice_name": "Partial Pressure O₂",
                                       "clean_name": "partial_pressure_o2",
                                       "cv_term": "partialPressureOxygen"},
                                      {"id": "32",
                                       "nice_name": "External Voltage",
                                       "clean_name": "external_voltage",
                                       "cv_term": "externalVoltage"},
                                      {"id": "33",
                                       "nice_name": "Battery Level",
                                       "clean_name": "battery_level",
                                       "cv_term": "batteryCharge"},
                                      {"id": "density",
                                       "nice_name": "Density",
                                       "clean_name": "density",
                                       "cv_term": "waterDensity"},
                                      # No longer exposing this,
                                      # setting cv_term to None - Doug
                                      # 2021-04-07
                                      {"id": "5",
                                       "nice_name": "Level: Elevation",
                                       "clean_name": "level_elevation",
                                       "cv_term": None}])

        self.unit_df = pd.DataFrame([{"id": "194",
                                      "nice_name": "NTU",
                                      "clean_name": "ntu",
                                      "cv_term": "nephelometricTurbidityUnit"},
                                     {"id": "117",
                                      "nice_name": "mg/L",
                                      "clean_name": "mg_l",
                                      "cv_term": "milligramPerLiter"},
                                     {"id": "97",
                                      "nice_name": "psu",
                                      "clean_name": "psu",
                                      "cv_term": "practicalSalinityUnit"},
                                     {"id": "17",
                                      "nice_name": "psi",
                                      "clean_name": "psi",
                                      "cv_term": "psi"},
                                     {"id": "163",
                                      "nice_name": "V",
                                      "clean_name": "v",
                                      "cv_term": "volt"},
                                     {"id": "241",
                                      "nice_name": "%",
                                      "clean_name": "percent",
                                      "cv_term": "percent"},
                                     {"id": "1",
                                      "nice_name": "C",
                                      "clean_name": "c",
                                      "cv_term": "degreeCelsius"},
                                     {"id": "322",
                                      "nice_name": "Hz",
                                      "clean_name": "hz",
                                      "cv_term": "hertz"},
                                     {"id": "129",
                                      "nice_name": "g/cm³",
                                      "clean_name": "g_cm3",
                                      "cv_term": "gramPerCubicCentimeter"},
                                     {"id": "65",
                                      "nice_name": "µS/cm",
                                      "clean_name": "us_cm",
                                      "cv_term": "microsiemenPerCentimeter"},
                                     {"id": "177",
                                      "nice_name": "% sat",
                                      "clean_name": "percent_sat",
                                      "cv_term": "percentSaturation"},
                                     {"id": "35",
                                      "nice_name": "m",
                                      "clean_name": "m",
                                      "cv_term": "meter"},
                                     {"id": "145",
                                      "nice_name": "pH",
                                      "clean_name": "ph",
                                      "cv_term": "ph"},
                                     {"id": "81",
                                      "nice_name": "Ω-cm",
                                      "clean_name": "ohm_cm",
                                      "cv_term": "ohmCentimeter"}])

    # TODO this shuold be moved to a more general location
    def generate_equipment_jsons(self,
                                 var_names,
                                 overwrite=False):
        """
        Generate equipment.json and data_to_equipment.json
        """

        dev_uuid = str(uuid.uuid4())
        file_path = self.equipment_directory
        os.makedirs(file_path, exist_ok=True)
        data_to_equipment_map = []
        for column_name in var_names:
            if 'timestamp' in column_name:
                variable_domain_cv = "instrumentTimestamp"
                variable_term = "nonedefined"
                unit = "datalogger_time_stamp"
                expose_as_datastream = False
            else:
                name, unit_name = column_name.split("[")
                variable_domain_cv = "instrumentMeasurement"
                variable_term = get_mapping(mapper=self.param_df,
                                            lookup_target='cv_term',
                                            lookup_key='clean_name',
                                            lookup_obj=name)
                unit = get_mapping(mapper=self.unit_df,
                                   lookup_target='cv_term',
                                   lookup_key='clean_name',
                                   lookup_obj=unit_name[:-1])
                expose_as_datastream = True
            if variable_term is None:
                continue
            data_to_equipment_map.append(
                gen_data_to_equipment_entry(column_name=column_name,
                                            var_domain_cv=variable_domain_cv,
                                            acquiring_instrument_uuid=dev_uuid,
                                            variable_term=variable_term,
                                            expose_as_ds=expose_as_datastream,
                                            units_term=unit))
        data_to_equipment_map_file = f"{file_path}/data_to_equipment_map.json"
        if os.path.exists(data_to_equipment_map_file):
            if not overwrite:
                vprint(f"Skipping, {data_to_equipment_map_file} exists")
                return
            with open(data_to_equipment_map_file, 'r') as f:
                existing_map = json.load(f)
                deepdiff = DeepDiff(existing_map, data_to_equipment_map)
                if deepdiff:
                    print(f"Existing map differs from new map: {deepdiff}")
                    print("Backing up existing map")
                    date_str = datetime.datetime.now().strftime("%Y%m%d")
                    shutil.copyfile(data_to_equipment_map_file,
                                f"{data_to_equipment_map_file}.{date_str}.bak")
                else:
                    vprint("Skipping data_to_equipment_map, no changes")
                    return

        vprint("Writing data_to_equipment_map to "
               f"{data_to_equipment_map_file}")
        with open(data_to_equipment_map_file, 'w', encoding='utf-8') as f:
            json.dump(data_to_equipment_map, f, ensure_ascii=False, indent=4)

        dev_id = self.device_id
        # FIXME: Why is the start hardcoded?
        start = 1420070400
        equipment_entry = gen_equipment_entry(
            acquiring_instrument_uuid=dev_uuid,
            name=self.device_type,
            code=self.device_code,
            serial_number=dev_id, relationship_start_date_time_utc=start,
            position_start_date_utc=start)
        equip_file = f"{file_path}/equipment.json"
        with open(equip_file, 'w+', encoding='utf-8') as f:
            json.dump([equipment_entry], f, ensure_ascii=False, indent=4)

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
                nice_name = get_mapping(mapper=self.param_df,
                                        lookup_target='nice_name',
                                        lookup_key='id',
                                        lookup_obj=param['parameterId'])
                nice_unit = get_mapping(mapper=self.unit_df,
                                        lookup_target='nice_name',
                                        lookup_key='id',
                                        lookup_obj=param['unitId'])
                col_name = f"{nice_name}[{nice_unit}]"
                readings = param['readings']
                param_data = pd.DataFrame(readings,
                                          columns=['timestamp', 'value'])
                param_data = param_data.set_index('timestamp')
                param_data.rename(columns={'value': col_name}, inplace=True)
                data_df = pd.concat((data_df, param_data), axis=1)
            print(data_df)
            vprint(f"hydrovu: Data collected for {self.name} page {count}")
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

    def ingest(self, feeder_db_con):
        """
        Manipulate harvested Hydrovu data in a file on the server into a feeder
        database.
        """
        csv_path = self.data_source_path
        vprint(f"Reading {csv_path}")
        # Get the actual data.
        args = {'float_precision': 'high'}
        df: pd.DataFrame = pd.DataFrame(ssigen.open_csv(csv_path, args=args,
                                                        lock=True))

        # Rename all of the column headers.
        new_cols = []
        replace_chars = {' ': '_', '-': '_', '/': '_', '²': '2', '³': '3',
                         '°': 'deg', '__': '_', '%': 'percent', 'µ': 'u',
                         'Ω': 'ohm', 'ω': 'ohm', '₂': '2', ':': ''}
        for col in df.columns.tolist():
            new_col = col.lower()
            for key, value in replace_chars.items():
                new_col = new_col.replace(key, value)
            # Make sure our replacement worked
            try:
                new_col.encode('ascii')
            except UnicodeEncodeError as exc:
                raise RuntimeError(f"Column '{new_col}' derived from hydrovu "
                                   "data still has special characters. "
                                   "Check the find/replace list") from exc
            new_cols.append(new_col)
        self.generate_equipment_jsons(new_cols, overwrite=False)
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
                                      sampling_feature_code=sampling_feature_code)
