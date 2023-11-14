#!/usr/bin/env python3

"""
Module for NWIS data harvesting, ingestion, and processing.
"""

import os
import uuid
import json
import datetime
import pandas as pd
from dataretrieval import nwis
import odmx.support.general as ssigen
from odmx.abstract_data_source import DataSource
from odmx.timeseries_ingestion import general_timeseries_ingestion
from odmx.timeseries_processing import general_timeseries_processing
from odmx.harvesting import commit_csv
from odmx.write_equipment_jsons import get_mapping,\
    gen_equipment_entry, gen_data_to_equipment_entry
from odmx.log import vprint


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
        self.param_df = pd.DataFrame(ssigen.open_json((project_path +
                                            '/mappers/nwis.json')))
        self.param_df.set_index("id", inplace=True, verify_integrity=True)

    def generate_equipment_jsons(self, var_names, overwrite=False):
        """
        Generate equipment.json and data_to_equipment.json
        """

        dev_uuid = str(uuid.uuid4())
        equip_dir = self.equipment_directory
        file_path = (f"{self.project_path}/odmx/"
                     f"equipment/{equip_dir}")
        if os.path.exists(file_path) and not overwrite:
            return
        os.makedirs(file_path, exist_ok=True)
        data_to_equipment_map = []
        for name in var_names:
            if 'timestamp' in name:
                var_domain_cv = "instrumentTimestamp"
                variable_term = "nonedefined"
                unit = "datalogger_time_stamp"
                expose_as_ds = False
            else:
                var_domain_cv = "instrumentMeasurement"
                variable_term = get_mapping(mapper=self.param_df,
                                            lookup_target='cv_term',
                                            lookup_key='clean_name',
                                            lookup_obj=name)
                unit = get_mapping(mapper=self.param_df,
                                   lookup_target='cv_unit',
                                   lookup_key='clean_name',
                                   lookup_obj=name)
                expose_as_ds = True
            if variable_term is None:
                continue
            data_to_equipment_map.append(
                gen_data_to_equipment_entry(column_name=name,
                                            var_domain_cv=var_domain_cv,
                                            acquiring_instrument_uuid=dev_uuid,
                                            variable_term=variable_term,
                                            expose_as_ds=expose_as_ds,
                                            units_term=unit))
        data_to_equipment_map_file = (f"{file_path}/"
                                      "data_to_equipment_map.json")
        vprint("Writing data_to_equipment_map to "
               f"{data_to_equipment_map_file}")
        with open(data_to_equipment_map_file, 'w', encoding='utf-8') as f:
            json.dump(data_to_equipment_map, f,
                      ensure_ascii=False, indent=4)

        # FIXME: Why is the start hardcoded?
        start = 1420070400
        equipment_entry = gen_equipment_entry(
            acquiring_instrument_uuid=dev_uuid,
            name="unknown sensor",
            code="unknown sensor",
            serial_number=None,
            relationship_start_date_time_utc=start,
            position_start_date_utc=start,
            vendor="USGS")
        equip_file = f"{file_path}/equipment.json"
        with open(equip_file, 'w+', encoding='utf-8') as f:
            json.dump([equipment_entry], f, ensure_ascii=False, indent=4)

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
            server_df = ssigen.open_csv(file_path, args=args, lock=True)
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
        for column in data.columns:
            if '_cd' in column:
                droplist.append(column)
        data.drop(columns=droplist, inplace=True, errors='ignore')
        mapper = {'datetime': 'datetime'}
        for column in data.columns:
            try:
                var_name = self.param_df["clean_name"][column]
            except KeyError as e:
                raise KeyError(
                    f"The returned data {column} is not present in the"
                    " NWIS module. Need to add it first."
                ) from e
            mapper.update({column: var_name})

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

    def ingest(self, feeder_db_con):
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
        df = ssigen.open_csv(file_path, args=args, lock=True)

        # Make sure the column headers are lower case.
        df.columns = df.columns.str.lower()
        # Rename datetime column to timestamp for compatibiltiy with general
        # ingestion
        df['timestamp'] = pd.to_datetime(df['datetime'])
        df.drop(columns='datetime', inplace=True)
        df.sort_values(by='timestamp', inplace=True)
        df.reset_index(drop=True, inplace=True)

        new_cols = df.columns.tolist()
        self.generate_equipment_jsons(new_cols, overwrite=True)

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
