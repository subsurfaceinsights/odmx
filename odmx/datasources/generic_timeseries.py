#!/usr/bin/env python3

"""
Module for generic timeserties data in csv files or excel spreadsheets

"""

import os
from importlib.util import find_spec
import pandas as pd
from odmx.support.file_utils import get_files, open_csv, clean_name,\
    open_spreadsheet, open_json
from odmx.abstract_data_source import DataSource
from odmx.timeseries_ingestion import general_timeseries_ingestion
from odmx.timeseries_processing import general_timeseries_processing
from odmx.write_equipment_jsons import generate_equipment_jsons

json_schema_files = find_spec("odmx.json_schema").submodule_search_locations[0]

class GenericTimeseriesDataSource(DataSource):
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
        self.data_source_path = data_source_path
        # Define the file path.
        self.data_file_path = os.path.join(self.data_path,
                                            self.data_source_path)

        # Read the data source config information
        self.config_file = os.path.join(self.data_file_path,
                                        'data_source_config.json')
        # TODO can we query the default units defined in cv_quantity_kind?
        # or, do we even need to define this, since default units are defined in
        # the variables cv via quantity kind
        self.keep_units = ['kiloPascal', "percent", "cubicMeterPerCubicMeter",
                           "wattPerSquareMeter", "degreeCelsius",
                           "meterPerSecond"]
        self.split_list = []

    def harvest(self):
        """
        There is no harvesting to be done for this data type.

        For this data source type, data_source_path defines the subdirectory
        within the main data directory to search for data files. All files with
        the specified extension within this directory will be read and ingested.
        """

    def ingest(self, feeder_db_con, update_equipment_jsons):
        """
        Manipulate data into a feeder table. If update_equipment_jsons is true,
        new equipment.json and data_to_equipment_map.json files will be written
        from metadata provided in data_source_config.json (which should be saved
        alongside the data files). NOTE: equipment jsons will be written from
        scratch and existing versions saved as backups, this datasource is not
        designed to append to or modify existing equipment
        """

        data_source_config = open_json(self.config_file)
        ext = data_source_config["data_file_extension"]

        for block in data_source_config['data_split']:
            sampling_feature = block['sampling_feature']
            equipment_dir = block['equipment_path']
            equipment_path = os.path.join(self.project_path, 'odmx',
                                          'equipment', equipment_dir)

            # Get time column from data_source_config and add to column mapper
            time_col = block['base_equipment']['time_column']
            time_map = {"name": time_col,
                        "variable_cv": "nonedefined",
                        "unit_cv": "datalogger_time_stamp",
                        "expose": False}
            # Add the time column to columns listed for this sampling feature
            block['columns'].append(time_map)
            mapper = pd.DataFrame(block['columns'])

            # Extract column list and then use original names as index
            columns = mapper['name'].tolist()
            mapper.set_index('name', inplace=True)


            # Make a separate feeder table for each data block
            feeder_table = \
                f'feeder_{clean_name(self.data_source_name)}_{clean_name(sampling_feature)}'  #pylint:disable=line-too-long

            # Store tuple of sampling feature, equipment path, and feeder table
            self.split_list.append((sampling_feature,
                               equipment_dir,
                               feeder_table))

            # Find all data files in the path for this data source
            file_list = get_files(self.data_file_path, ext)[1]

            dfs = []
            for file in file_list:
                # Set up args for reading files
                args = {'parse_dates': True}

                if ext in ['xlsx', 'xls']:
                    tabs = data_source_config["data_file_tabs"]
                    tab_dfs = []
                    for tab in tabs:
                        args.update({'sheet_name': tab,
                                     'usecols': columns})
                        tab_dfs.append(open_spreadsheet(file,
                                                        args=args,
                                                        lock=True))
                    part_df = pd.concat(tab_dfs, ignore_index=True)
                elif ext == 'csv':
                    args.update({'float_precision': 'high',
                                 'usecols': columns})
                    part_df = open_csv(file, args=args, lock=True)


                # Convert time column to timestamp in specific format
                part_df['timestamp'] = pd.to_datetime(part_df[time_col],
                                                format='%Y-%m-%d %H:%M:%S')

                # Add to list of dataframes
                dfs.append(part_df)

            # Combine all dfs, drop duplicates, and sort by timestamp
            df = pd.concat(dfs, ignore_index=True).drop_duplicates()
            df.sort_values(by='timestamp', inplace=True)

            # Sanitize names
            # Create column to store sanitized names
            mapper["clean_name"] = pd.Series()
            # rename timestamp explicitly
            mapper.loc[time_col, "clean_name"] = "timestamp"

            # Iterate through the rest
            new_cols = []
            for col in df.columns.tolist():
                new_col = clean_name(col)
                mapper.loc[col, "clean_name"] = new_col
                new_cols.append(new_col)

            # Now we can drop the old time column
            df.drop(columns=time_col, inplace=True)

            # as well as the excess "timestamp" column from mapper
            mapper.drop(labels="timestamp", inplace=True)

            # Rename data columns, then use clean_name as index on mapper
            df.rename(columns=mapper['clean_name'].to_dict(), inplace=True)
            mapper.set_index('clean_name', inplace=True)

            # Prepare the equipment jsons for these columns
            if update_equipment_jsons is True:
                generate_equipment_jsons(equipment_path,
                                         block,
                                         time_col,
                                         new_cols,
                                         mapper,
                                         self.keep_units)

            # The rest of the ingestion is even more generic.
            general_timeseries_ingestion(feeder_db_con,
                                         feeder_table=feeder_table, df=df)

    def process(self, feeder_db_con, odmx_db_con):
        """
        Process ingested data into timeseries datastreams.
        """

        for split in self.split_list:
            sampling_feature_code, equipment_path, feeder_table = split
            general_timeseries_processing(self, feeder_db_con, odmx_db_con,
                                          sampling_feature_code=\
                                              sampling_feature_code,
                                              equipment_directory=\
                                                  equipment_path,
                                              feeder_table=feeder_table)
