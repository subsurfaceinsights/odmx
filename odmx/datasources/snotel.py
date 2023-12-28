#!/usr/bin/env python3
# pylint: disable=consider-using-dict-comprehension, too-many-instance-attributes, no-else-return

"""
Module for SNOTEL data harvesting, ingestion, and processing.
"""

import os
import datetime
from importlib.util import find_spec
from functools import reduce
import io
import pandas as pd
import numpy as np
import isodate
import suds.client
from odmx.support.file_utils import open_csv, open_json, clean_name
from odmx.abstract_data_source import DataSource
from odmx.timeseries_ingestion import general_timeseries_ingestion
from odmx.timeseries_processing import general_timeseries_processing
from odmx.harvesting import commit_csv
from odmx.parse_waterml import parse_site_values, parse_sites
from odmx.write_equipment_jsons import gen_equipment_entry,\
    gen_data_to_equipment_entry, check_diff_and_write_new,\
        read_or_start_data_to_equipment_json

mapper_path = find_spec("odmx.mappers").submodule_search_locations[0]
json_schema_files = find_spec("odmx.json_schema").submodule_search_locations[0]

def get_waterml_version(suds_client):
    """Get waterML version"""
    tns_str = str(suds_client.wsdl.tns[1])
    if tns_str == 'http://www.cuahsi.org/his/1.0/ws/':
        return '1.0'
    elif tns_str == 'http://www.cuahsi.org/his/1.1/ws/':
        return '1.1'
    else:
        raise NotImplementedError(
            "only WaterOneFlow 1.0 and 1.1 are currently supported")


def to_bytes(string):
    """convert str to bytes for py 2/3 compat
    """

    if isinstance(string, bytes):
        return string

    return string.encode('utf-8', 'ignore')


def get_snotel_site_info(suds_client,
                         waterml_namespace,
                         station_id):
    """Retrieve snotel site info using API"""

    response = suds_client.service.GetSiteInfo(station_id)
    response_buffer = io.BytesIO(to_bytes(response))
    sites = parse_sites(response_buffer, waterml_namespace)

    if len(sites) == 0:
        return {}
    site_info = list(sites.values())[0]
    series_dict = dict([
        (series['variable']['vocabulary'] + ':' + series['variable']['code'],
            series)
        for series in site_info['series']
    ])
    site_info['series'] = series_dict
    return site_info


def get_snotel_data(site_code,
                    variable_code,
                    start, end,
                    suds_client,
                    waterml_namespace):
    """Retrieve snotel data using API"""
    start_dt_isostr = None
    end_dt_isostr = None
    if start is not None:
        start_datetime = pd.Timestamp(start).to_pydatetime()
        start_dt_isostr = isodate.datetime_isoformat(start_datetime)
    if end is not None:
        end_datetime = pd.Timestamp(end).to_pydatetime()
        end_dt_isostr = isodate.datetime_isoformat(end_datetime)

    response = suds_client.service.GetValues(
        site_code, variable_code, startDate=start_dt_isostr,
        endDate=end_dt_isostr)

    response_buffer = io.BytesIO(to_bytes(response))
    values = parse_site_values(response_buffer, waterml_namespace)

    if variable_code is not None:
        return list(values.values())[0]
    else:
        return values


class SnotelDataSource(DataSource):
    """
    Class for SNOTEL data source objects.
    """

    def __init__(self, project_name, project_path, data_path, station_id,
                 data_source_timezone):
        self.project_name = project_name
        self.project_path = project_path
        self.station_id = station_id
        self.data_source_timezone = data_source_timezone
        self.data_source_path = f'{data_path}/snotel'
        self.feeder_table = station_id.replace(':', '_').lower()
        self.equipment_directory = f'snotel/{self.feeder_table}'
        self.param_df = pd.DataFrame(
            open_json(f'{mapper_path}/snotel_variables.json'))
        self.param_df.set_index('clean_name', inplace=True,
                                verify_integrity=True)
        # Using abbrevations rather than names for units because otherwise
        # we get units like "international inch"
        self.unit_df = pd.DataFrame(
            open_json(f'{mapper_path}/snotel_units.json'))
        self.unit_df.set_index('clean_name', inplace=True,
                               verify_integrity=True)

    def harvest(self):
        """
        Harvest SNOTEL data from the API and save it to a .csv on our servers.
        """

        # Grab the current time to get the most recent data.
        current_time = datetime.datetime.utcnow().replace(microsecond=0)

        # Define variables from the harvesting info file.
        wsdl_url = 'https://hydroportal.cuahsi.org/Snotel/cuahsi_1_1.asmx?WSDL'
        suds_client = suds.client.Client(wsdl_url)
        waterml_version = get_waterml_version(suds_client)
        waterml_namespace = ("{http://www.cuahsi.org/waterML/"
                             f"{waterml_version}/}}")

        local_base_path = self.data_source_path
        station_id = self.station_id

        # First check to make sure the proper directory exists.
        os.makedirs(local_base_path, exist_ok=True)

        # Then check to see if the data already exists on our server.
        file_name = f'{self.feeder_table}.csv'
        file_path = os.path.join(local_base_path, file_name)
        # If it does, we want to find only new data.
        server_df = None
        site_info = get_snotel_site_info(suds_client,
                                         waterml_namespace,
                                         station_id)
        if os.path.isfile(file_path):
            # Grab the data from the server.
            server_df = pd.read_csv(file_path, parse_dates=[0])
            # Find the latest timestamp.
            last_server_time = server_df['timestamp'].max()
            params = {
                'start': last_server_time + datetime.timedelta(minutes=1),
                'end': current_time,
            }
        # If it doesn't, we want all available data from the date that
        # snowfall data first appears. We don't really care about data
        # prior to that. (It is often just NaNs anyway. Years of NaNs.)
        else:
            print("Finding the earliest date that snowfall was recorded at"
                  f" {station_id}.")
            # 01/01/1753 is, for some reason, the first date that's allowed.
            snow_datastream = site_info['series']['SNOTEL:SNWD_H']
            time_info_key = ("{http://www.cuahsi.org/water_ml/"
                             f"{waterml_version}/}}variable_time_interval")
            first_snow = snow_datastream[time_info_key]['begin_date_time_utc']
            first_snow = datetime.datetime.strptime(first_snow,
                                                    '%Y-%m-%dT%H:%M:%S')
            params = {
                'start': first_snow,
                'end': current_time,
            }

        # Download the data using ulmo.
        print(f"Harvesting SNOTEL site {station_id}.")
        # Get a list of all variables present at the given site.
        variables = site_info['series']
        # If nothing was returned, we're done.
        if not variables:
            print(f"No data returned for SNOTEL site {station_id}.\n")
            return
        # Cull the list down to hourly variables that we're interested in.
        vars_we_want = list(self.param_df['id'])
        vars_dict = {}
        for key, value in variables.items():
            if key.split(':')[-1] in vars_we_want:
                variable = value['variable']
                var_info = {'variable_name': variable['name'],
                            'unit': variable['units']['abbreviation']}
                vars_dict[key] = var_info
        # Creating a space for a no-values return, since that can happen.
        df_temp_list = []
        no_vals_list = []
        # Loop through the variables and get the DataFrames.
        for variable_code, variable_info in vars_dict.items():
            variable_name = variable_info['variable_name']
            variable_name = variable_name.lower().replace(' ', '_')
            new_name = f"{variable_name}[{variable_info['unit']}]"
            try:
                values = get_snotel_data(station_id,
                                         variable_code,
                                         **params,
                                         suds_client=suds_client,
                                         waterml_namespace=waterml_namespace)
                datetimes_list = [value['datetime']
                                  for value in values['values']]
                values_list = [value['value'] for value in values['values']]
                df = pd.DataFrame(list(zip(datetimes_list, values_list)),
                                  columns=['timestamp', variable_name])
                df.rename(columns={variable_name: new_name}, inplace=True)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df_temp_list.append(df)
            except suds.WebFault:
                print(f"No new {variable_name} data available.")
                no_vals_list.append(new_name)

        # Combine the DataFrames if any were created.
        if df_temp_list:
            df = reduce(lambda left, right: pd.merge(left, right,
                                                     on=['timestamp'],
                                                     how='outer'),
                        df_temp_list)
            # Check to see if any extra columns need to be added (columns that
            # the station does have data for, but not necessarily in the
            # timeframe specified).
            if no_vals_list:
                for col in no_vals_list:
                    df[col] = np.nan
            # Sort everything by timestamp.
            df.sort_values(by='timestamp', inplace=True)
            # Drop any potential duplicates (just in case).
            df.drop_duplicates(inplace=True)
            # Reset the index.
            df.reset_index(drop=True, inplace=True)

            # If the file already exists, and we used its final datetime as the
            # start date, for some reason, sometimes the data return gives data
            # just before the start date. So, we need to filter that out if it
            # exists. NOTE: This is true for NWIS, though might not be true for
            # SNOTEL.
            if os.path.isfile(file_path):
                df = df[df['timestamp'] > last_server_time]
                df.reset_index(drop=True, inplace=True)
            commit_csv(file_path, df, server_df)
        # If there were no dataframes, however, we just tie up some loose ends.
        else:
            print(f"No new data available for {file_path}.\n")

    def ingest(self, feeder_db_con, update_equipment_jsons):
        """
        Manipulate harvested SNOTEL data in a file on the server into a feeder
        database.
        """

        # Define the file name and path.
        local_base_path = self.data_source_path
        file_name = f'{self.feeder_table}.csv'
        file_path = os.path.join(local_base_path, file_name)
        # Create a DataFrame of the file.
        args = {'float_precision': 'high', }
        df = open_csv(file_path, args=args, lock=True)

        # Turn the datetime column into an actual datetime, and sort by it.
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')
        df.sort_values(by='timestamp', inplace=True)
        df.reset_index(drop=True, inplace=True)

        new_cols = []
        for col in df.columns.tolist():
            new_col = clean_name(col)
            new_cols.append(new_col)

        # Write equipment jsons if specified
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
                    name="unknown sensor",
                    code="unknown sensor",
                    serial_number=None,
                    vendor="NRCS")

            # Retrieve device uuid from equipment dict
            dev_uuid = equipment['equipment_uuid']

            # Same for data to equipment map
            data_to_equip, col_list =\
            read_or_start_data_to_equipment_json(data_to_equipment_map_file,
                                                 equipment)

            for column_name in new_cols:
                if column_name in col_list:
                    continue
                name, unit_name = column_name.split("[")
                variable_domain_cv = "instrumentMeasurement"
                variable_term = self.param_df['cv_term'][name]
                unit = self.unit_df['cv_term'][unit_name[:-1]]
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

        # The rest of the ingestion is generic.
        general_timeseries_ingestion(feeder_db_con, self.feeder_table, df)

    def process(self, feeder_db_con, odmx_db_con, sampling_feature_code):
        """
        Process ingested SNOTEL data into timeseries datastreams.
        """

        general_timeseries_processing(self,
                                      feeder_db_con=feeder_db_con,
                                      odmx_db_con=odmx_db_con,
                                      sampling_feature_code=\
                                          sampling_feature_code)
