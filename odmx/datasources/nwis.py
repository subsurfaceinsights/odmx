#!/usr/bin/env python3

"""
Module for NWIS data harvesting, ingestion, and processing.
"""

import os
from importlib.util import find_spec
import datetime
import pandas as pd
from dataretrieval import nwis
from odmx.support.file_utils import open_csv, open_json
from odmx.abstract_data_source import DataSource
from odmx.timeseries_ingestion import general_timeseries_ingestion
from odmx.timeseries_processing import general_timeseries_processing
from odmx.harvesting import commit_csv
from odmx.write_equipment_jsons import gen_equipment_entry,\
    gen_data_to_equipment_entry, check_diff_and_write_new,\
        read_or_start_data_to_equipment_json
from odmx.log import vprint

mapper_path = find_spec("odmx.mappers").submodule_search_locations[0]
json_schema_files = find_spec("odmx.json_schema").submodule_search_locations[0]

class NwisDataSource(DataSource):
    """
    Class for NWIS data source objects.
    """

    def __init__(self, project_name, project_path, data_path,
                 data_source_timezone, site_code):
        self.project_name = project_name
        self.project_path = project_path
        self.data_path = data_path
        self.data_source_timezone = data_source_timezone
        self.data_source_path = 'nwis'
        self.site_code = site_code
        self.feeder_table = f'nwis_{site_code}'
        self.equipment_directory = f'nwis/nwis_{site_code}'
        self.param_df = pd.DataFrame(open_json(f'{mapper_path}/nwis.json'))
        self.param_df.set_index("id", inplace=True, verify_integrity=True)

    def harvest(self):
        """
        Harvest NWIS data from the API and save it to a .csv on our servers.
        """

        # Define variables from the harvesting info file.
        local_base_path = os.path.join(self.data_path, self.data_source_path)
        site_code = self.site_code
        tz = self.data_source_timezone

        # First check to make sure the proper directory exists.
        os.makedirs(local_base_path, exist_ok=True)

        # Check to see if the data already exists on our server.
        file_name = f'nwis_{site_code}.csv'
        file_path = os.path.join(local_base_path, file_name)
        # If it does, we want to find only new data.
        server_df = None
        if os.path.isfile(file_path):
            # Grab the data from the server.
            args = {'parse_dates': [0], }
            server_df = open_csv(file_path, args=args, lock=True)
            # Find the latest timestamp.
            last_server_time = server_df['datetime'].max()
            if isinstance(last_server_time, str):
                last_server_time = datetime.datetime.strptime(
                    last_server_time, '%Y-%m-%d %H:%M:%S'
                )
            # Add a minute to it so that we don't get it in the return (the
            # data is never so granular anyway).
            start = last_server_time + datetime.timedelta(minutes=1)
            end = datetime.datetime.utcnow().replace(microsecond=0)
        # If it doesn't, we want all available data.
        else:
            # The earliest start date NWIS allows one to use is 1900-01-01.
            # However, their system still claims that's too early, so we
            # add a day.
            start = datetime.datetime(1900, 1, 2)
            end = datetime.datetime.utcnow().replace(microsecond=0)

        # Download the data using ulmo.
        print(f"Harvesting NWIS site {site_code}.")
        data = nwis.get_record(sites=site_code,
                               service='iv',
                               start=start.strftime("%Y-%m-%d"),
                               end=end.strftime("%Y-%m-%d"))

        # If nothing was returned, we're done.
        if data.empty:
            print(f"No new data available for NWIS site {site_code}.\n")
            return
        # Otherwise, we examine the data.
        # If data exists, first drop qa/qc columns and 'site_no' column
        droplist = ['site_no']
        skipped_parameters = []
        for column in data.columns:
            if column not in list(self.param_df.index):
                droplist.append(column)
                skipped_parameters.append(column)
            if '_cd' in column:
                droplist.append(column)
            if 'discontinued' in column:
                droplist.append(column)
                skipped_parameters.append(column)
        vprint(f"skipped parameters: {skipped_parameters}. To keep these, "
               "add them to the NWIS mapper")
        data.drop(columns=droplist, inplace=True, errors='ignore')
        mapper = {'datetime': 'datetime'}
        for column in data.columns:
            mapper.update({column: self.param_df["clean_name"][column]})

        data.rename(columns=mapper, inplace=True)

        # Drop any potential duplicates (just in case).
        # Only drop duplicate INDEX, not duplicate values
        data = data[~data.index.duplicated(keep='first')]
        # Move datetime to its own column and then reset index
        data['datetime'] = data.index
        data.reset_index(drop=True, inplace=True)
        # localize datetime column
        data['datetime'] = data['datetime'].dt.tz_convert(tz)
        data['datetime'] = data['datetime'].dt.tz_localize(None)
        # If the file already exists, and we used its final datetime as the
        # start date, for some reason, sometimes the data return gives data
        # just before the start date. So, we need to filter that out if it
        # exists.
        if os.path.isfile(file_path):
            data = data[data['datetime'] > last_server_time]
            data.reset_index(drop=True, inplace=True)

        # We update the .csv files on the server if we actually got new data.
        if not data.empty:
            commit_csv(file_path, data, server_df,
                       to_csv_args={'date_format': '%Y-%m-%d %H:%M:%S'})
        else:
            print(f"No new data available for {file_path}.\n")

    def ingest(self, feeder_db_con, update_equipment_jsons):
        """
        Manipulate harvested NWIS data in a file on the server into a feeder
        database.
        """
        # Define the file name and path.
        local_base_path = os.path.join(self.data_path, self.data_source_path)
        site_code = self.site_code
        file_name = f'nwis_{site_code}.csv'
        file_path = os.path.join(local_base_path, file_name)
        # Create a DataFrame of the file.
        args = {'float_precision': 'high', }
        df = open_csv(file_path, args=args, lock=True)

        # Rename datetime column to timestamp for compatibiltiy with general
        # ingestion
        df['timestamp'] = pd.to_datetime(df['datetime'])
        df.drop(columns='datetime', inplace=True)
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
                    vendor="USGS")

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
                variable_term = lookup_df['cv_term'][column_name]
                unit = lookup_df['cv_unit'][column_name]
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

        # The rest of the ingestion is generic.
        general_timeseries_ingestion(feeder_db_con,
                                     feeder_table=self.feeder_table, df=df)

    def process(self, feeder_db_con, odmx_db_con, sampling_feature_code):
        """
        Process ingested NWIS data into timeseries datastreams.
        """

        general_timeseries_processing(self, feeder_db_con, odmx_db_con,
                                      sampling_feature_code=\
                                          sampling_feature_code)
