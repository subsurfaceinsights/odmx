#!/usr/bin/env python3

"""
Module for generic timeserties data in csv files or excel spreadsheets

"""

import os
from importlib.util import find_spec
import pandas as pd
from odmx.support.file_utils import get_files, open_csv, clean_name,\
    open_spreadsheet, open_json, expand_column_names
from odmx.abstract_data_source import DataSource
from odmx.timeseries_ingestion import general_timeseries_ingestion
from odmx.timeseries_processing import general_timeseries_processing
from odmx.write_equipment_jsons import gen_equipment_entry,\
    gen_data_to_equipment_entry, check_diff_and_write_new

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
            block['columns'].append(time_map)
            mapper = pd.DataFrame(block['columns'])

            # Add the time column to whatever columns belong with this
            # sampling feature
            columns = mapper['name'].tolist()

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

            # Combine all dfs and drop duplicates
            df = pd.concat(dfs, ignore_index=True).drop_duplicates()

            # Sort the DataFrame by timestamp and drop unused datetime column
            df.sort_values(by='timestamp', inplace=True)
            df.drop(columns=time_col, inplace=True)

            # Move time column back to the front for adding new names to mapper
            col = df.pop("timestamp")
            df.insert(0, col.name, col)

            # Ensure column names are compatible with database
            new_cols = []
            for col in df.columns.tolist():
                new_col = clean_name(col)
                new_cols.append(new_col)

            # Add new column names to mapper and use as index for easy lookup
            df.columns = new_cols
            mapper['clean_name'] = new_cols
            mapper.set_index('clean_name', inplace=True)

            # Prepare the equipment jsons for these columns
            if update_equipment_jsons is True:
                equip_file = f"{equipment_path}/equipment.json"
                data_to_equipment_map_file = (f"{equipment_path}/"
                                              "data_to_equipment_map.json")

                # Read equipment.json if it exists, otherwise start new
                # Make the directories
                os.makedirs(equipment_path, exist_ok=True)

                # Read base equipment from data_source_config
                logger = block['base_equipment']
                start = logger['start_timestamp']
                end = logger['end_timestamp']

                # Start equipment.json entries
                equipment = gen_equipment_entry(
                    acquiring_instrument_uuid=None,
                    code=logger['equipment_name'],
                    name=logger['equipment_name'],
                    serial_number=logger['equipment_serial_number'],
                    vendor=None,
                    description=None,
                    position_start_date_utc=start,
                    position_end_date_utc=end,
                    relationship_start_date_time_utc=start,
                    relationship_end_date_time_utc=end,
                    z_offset_m=logger['equipment_z_offset'])

                # Store uuid for use in data_to_equipment_map
                logger_uuid = equipment['equipment_uuid']

                # Initialize data_to_equipment_map with timestamp
                data_to_equip = \
                    [gen_data_to_equipment_entry(
                        column_name='timestamp',
                        var_domain_cv='instrumentTimestamp',
                        acquiring_instrument_uuid=logger_uuid,
                        variable_term='nonedefined',
                        units_term='datalogger_time_stamp',
                        units_conversion=True,
                        expose_as_ds=False)]

                # Remove timestamp from logger column list
                logger['columns'].remove(time_col)

                # If anything is left, add it as instrumentMetadata,
                # not exposed as a datastream
                for column in logger['columns']:
                    data_to_equip.append(
                        gen_data_to_equipment_entry(
                            column_name=column,
                            var_domain_cv='instrumentMetadata',
                            acquiring_instrument_uuid=logger_uuid,
                            variable_term=mapper['variable_cv'][column],
                            units_term=mapper['unit_cv'][column],
                            units_conversion=True,
                            expose_as_ds=False))

                # If there are attached sensors, iterate through
                child_equipment = []
                if block['attached_sensors'] is not None:
                    for sensor in block['attached_sensors']:
                        # These should inherit from logger if not defined
                        start = sensor['start_timestamp']
                        end = sensor['end_timestamp']

                        # Generate equipment entry
                        child = gen_equipment_entry(
                            acquiring_instrument_uuid=None,
                            code=sensor['sensor_name'],
                            name=sensor['sensor_name'],
                            serial_number=sensor['serial_number'],
                            vendor=None,
                            description=None,
                            position_start_date_utc=start,
                            position_end_date_utc=end,
                            relationship_start_date_time_utc=start,
                            relationship_end_date_time_utc=end,
                            z_offset_m=sensor['sensor_z_offset'])

                        # Append to child equipment list
                        child_equipment.append(child)

                        # Expand column names if they were specified with *
                        sensor['columns'] = expand_column_names(
                            sensor['columns'], new_cols)

                        # Now iterate over the column names
                        for column in sensor['columns']:
                            col_name = clean_name(column)
                            units_term=mapper['unit_cv'][col_name]

                            # check if units term is one of our defined keepers
                            if units_term in self.keep_units:
                                units_conversion = False
                            else:
                                units_conversion = True

                            # Create the data to equipment map entry
                            # Need to explicitly typecast expose_as_ds, else
                            # it is type numpy.bool_ which is incompatible
                            # with json.dump
                            data_to_equip.append(
                                gen_data_to_equipment_entry(
                                    column_name=col_name,
                                    var_domain_cv='instrumentMeasurement',
                                    acquiring_instrument_uuid=\
                                        child['equipment_uuid'],
                                    variable_term=\
                                        mapper['variable_cv'][col_name],
                                    units_term=units_term,
                                    units_conversion=units_conversion,
                                    expose_as_ds=\
                                        bool(mapper['expose'][col_name])))

                # Add child equipment to base
                equipment.update({'equipment': child_equipment})

                # Check diff, backup, and write new jsons
                check_diff_and_write_new(data_to_equip,
                                         data_to_equipment_map_file)
                check_diff_and_write_new([equipment], equip_file)

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
