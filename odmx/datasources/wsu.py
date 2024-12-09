#!/usr/bin/env python3

"""
Module for WSU data harvesting, ingestion, and processing.
"""

import os
from importlib.util import find_spec
import datetime
import pandas as pd
import odmx.data_model as odmx
import odmx.support.db as db
from odmx.timeseries_ingestion import add_columns
from odmx.timeseries_processing import create_view, materialize, create_datastream_entry, \
    ingest_equipment
from odmx.support.file_utils import get_files, open_csv, clean_name, \
    open_spreadsheet, open_json
from odmx.abstract_data_source import DataSource
from odmx.log import vprint

mapper_path = find_spec("odmx.mappers").submodule_search_locations[0]


class WsuDataSource(DataSource):
    """
    Class for WSU data source objects.
    """

    def __init__(self, project_name, project_path, data_path,
                 data_source_timezone,  data_source_name, data_source_path):
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
        self.param_df = pd.DataFrame(
            open_json(f'{mapper_path}/wsu.json'))
        self.split_list = []

    def harvest(self):
        """
        Can link to Google Drive Sheets.
        """

    def ingest(self, feeder_db_con, update_equipment_jsons):
        """
        Manipulate data into a feeder table. If update_equipment_jsons is true,
        new equipment.json and data_to_equipment_map.json files will be written
        from metadata provided in data_source_config.json (which should be saved
        alongside the data files). NOTE: equipment jsons will be written from
        scratch and existing versions saved as backups, this datasource is not
        designed to append to or modify existing equipment.
        """

        data_source_config = open_json(self.config_file)
        ext = data_source_config["data_file_extension"]

        # Find all data files in the path for this data source
        file_list = get_files(self.data_file_path, ext)[1]

        dfs = []
        for file in file_list:
            print(file)
            # Set up args for reading files
            args = {'parse_dates': True,
                    'comment': '#'}

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
                args.update({'float_precision': 'high'}),
                # 'usecols': columns})

                part_df = open_csv(file, args=args, lock=True)

            # Drop empty columns
            part_df = part_df.dropna(axis=1, how='all')

            # Handle blank values in sample time column
            part_df['SampleTime'] = part_df['SampleTime'].fillna('00:00')
            # Combine SampleDate and SampleTime into a single datetime column
            part_df['timestamp'] = pd.to_datetime(
                part_df['SampleDate'] + ' ' + part_df['SampleTime'], format='%B %d %Y %H:%M')

            # Ensure the timestamp is formatted as desired
            part_df['timestamp'] = part_df['timestamp'].dt.strftime(
                '%Y-%m-%d %H:%M:%S')

            # Convert time column to timestamp in specific format
            part_df['timestamp'] = pd.to_datetime(part_df['timestamp'],
                                                  format='%Y-%m-%d %H:%M:%S')

            dfs.append(part_df)

        # Create dataframe
        df = pd.concat(dfs, ignore_index=True)  # .drop_duplicates()
        df.sort_values(by='timestamp', inplace=True)
        df.reset_index(drop=True, inplace=True)

        # Create feeder table
        # This is very similar to the code for general_timeseries_ingestion
        # But adapted to deal with the headers here
        if len(df.columns) != len(set(df.columns)):
            raise ValueError(
                "Ingestion DataFrame must have unique columns")

        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(subset='timestamp', inplace=True)
        # Drop unneccesary date/time cols
        df.drop(columns=['SampleDate', 'SampleTime'], inplace=True)

        # Drop unnecessary columns
        drop_cols = ['Data enterer', 'Data checker',
                     'SpecialNotes', 'Lab', 'Probe', 'FlowCondition']
        df.drop(columns=drop_cols, inplace=True)

        # Ensure 'Result' column is numeric, replacing errors with NaN
        df['Result'] = pd.to_numeric(df['Result'], errors='coerce')

        df = df.pivot_table(index=['StationID', 'SampleDepth', 'timestamp'],
                            columns='Analyte',
                            values='Result',
                            aggfunc='mean').reset_index()

        df = df.rename_axis(None, axis=1)

        # TODO: map StationIDs to sampling features
        # Cast all stationids as lower
        df['StationID'] = df['StationID'].astype(str).str.lower()

        # reformat depth strings to match sample features
        def transform_stationid(stationid):
            # Check if "depth" is present
            if "depth" in stationid:
                # Extract the main part (e.g., 'pr3d') and the depth value (e.g., '4m')
                main_part, depth = stationid.split(
                    ' ')[0], stationid.split(' ')[1]
                # Format as main_part_depth (e.g., 'pr3d_4.0')
                # Convert depth to float, remove 'm'
                return f"{main_part}_{float(depth[:-1]):.1f}"
            return stationid

        df['StationID'] = df['StationID'].apply(transform_stationid)

        # drop duplicate/ blank
        df = df[~df['StationID'].str.contains(
            r'dupe|e|b', case=False, na=False)]

        df['StationID'] = df['StationID'].str.strip()

        # Drop superfluous columns
        columns_to_drop = ['SampleDepth',
                           'Depth', 'Depth, Secchi disk depth']
        df = df.drop(columns=columns_to_drop)

        # Ensure that the timestamp column is the first column in order.
        cols = df.columns.tolist()
        cols.insert(0, cols.pop(cols.index('timestamp')))
        df = df.loc[:, cols]

        # remap column names
        for station in df.StationID.unique():
            sampling_feature = station
            feeder_table = \
                f'feeder_{clean_name(self.data_source_name)}_{clean_name(sampling_feature)}'
            self.split_list.append((sampling_feature, feeder_table))

            df = df[df['StationID'] == station]

            print(f"Checking to see if \"{feeder_table}\" exists"
                  " in the database.")
            table_exist = db.does_table_exist(feeder_db_con, feeder_table)
            # If it doesn't exist, create it.
            if not table_exist:
                print(f"\"{feeder_table}\" doesn't exist yet."
                      " Creating it.")
                # Since pandas.to_sql doesn't support primary key creation, we need
                # to handle that. First we make the table in PostgreSQL.
                query = f'''
                    CREATE TABLE {db.quote_id(feeder_db_con,
                                feeder_table)}
                        (index SERIAL PRIMARY KEY)
                '''
                feeder_db_con.execute(query)
                add_columns(feeder_db_con, feeder_table,
                            df.columns.tolist(), col_type='text')  # , override_all=True)
            # If it does exist, we only want to add new data to it.
            else:
                # First check if we added any columns
                print(f"\"{feeder_table}\" already exists.")
                existing_columns = set(db.get_columns(feeder_db_con,
                                                      feeder_table))
                data_columns = set(df.columns.tolist())
                new_columns = data_columns - existing_columns
                if len(new_columns) > 0:
                    print(f"Adding new columns '{new_columns}'")
                    add_columns(feeder_db_con, feeder_table,
                                new_columns, col_type='text', override_all=True)
                query = f'''
                    SELECT timestamp FROM
                        {db.quote_id(feeder_db_con,
                                feeder_table)}
                        ORDER BY timestamp DESC LIMIT 1
                '''
                result = feeder_db_con.execute(query).fetchone()
                # However, the table could exist but be empty (odd edge case that
                # should only happen if the table is created, but then there's an
                # ingestion error, and we try to ingest again).
                if result is not None:
                    last_time = result[0]
                    df = df[df['timestamp'] > last_time]
                    # If the DataFrame is empty, there's no new data to ingest.
                    if df.empty:
                        print("No new data to ingest.\n")
                        return
                else:
                    print(f"\"{feeder_table}\" is empty. Adding"
                          " to it.")

            print(f"Populating {feeder_table}.")
            db.insert_many_df(
                feeder_db_con, feeder_table, df, upsert=False)

    def process(self, feeder_db_con, odmx_db_con):
        """
        Process ingested data into timeseries datastreams.
        """
        # Define file path from datasource info
        local_base_path = os.path.join(self.data_path, self.data_source_path)

        data_source_config = open_json(self.config_file)

        self.param_df.set_index("clean_name", inplace=True,
                                verify_integrity=True)

        # Before we start the data processing, grab the equipment models to use
        # throughout the process.
        equipment_models_list = odmx.read_equipment_models_all(odmx_db_con)
        equipment_models_df = pd.DataFrame(equipment_models_list)

        for split in self.split_list:
            sampling_feature_code, feeder_table = split

            print(f"Beginning processing of {feeder_table}.")

            sf = odmx.read_sampling_features_one(odmx_db_con,
                                                 sampling_feature_code=sampling_feature_code)
            sf_id = sf.sampling_feature_id if sf else None

            # Ingest equipment
            eqp_dir_path = os.path.join(self.project_path, 'odmx', 'equipment')
            equipment_path = os.path.join(eqp_dir_path, 'equipment.json')
            vprint(f"Opening the equipment file: {equipment_path}")
            equipment_json = open_json(equipment_path)
            # Ingest the equipment.
            ingest_equipment(odmx_db_con, equipment_json,
                             equipment_models_df, sf_id)

            # Read data from the feeder table
            # feeder_table_columns = db.get_columns(feeder_db_con, feeder_table)
            df = db.query_df(feeder_db_con, feeder_table)

            df.drop(columns=['StationID'], inplace=True)

            for col in df.columns[2:]:
                vprint(f"Working on column: {col}")
                variable_term = self.param_df["cv_term"][col]
                if variable_term is None:
                    variable_term = 'unknown'
                odmx_variable = odmx.read_variables_one(
                    odmx_db_con, variable_term=variable_term)
                if not odmx_variable:
                    raise ValueError(f"Could not find variable term {variable_term} "
                                     f"as defined in {self.param_df}")
                variable_id = odmx_variable.variable_id
                assert variable_id is not None

                units_term = self.param_df["cv_unit"][col]
                if units_term is None:
                    units_term = 'unknown'
                odmx_unit = odmx.read_cv_units_one_or_none(
                    odmx_db_con, term=units_term)
                if not odmx_unit:
                    raise ValueError(f"Could not find units term {units_term} "
                                     f"as define in {self.param_df}")
                units_id = odmx_unit.units_id
                assert units_id is not None

                # Define the view
                vprint(
                    f"{variable_term} should be exposed as a view. Continuing.")
                mat_view_name = f"{feeder_table}_{variable_term}"
                # At this point we need to find out how many bytes the materialized
                # view's name is. PostgreSQL has a 63 byte limit for table names, and
                # we want to add 10 bytes to the name shortly, so here it can be no
                # more than 53 bytes.
                byte_len = len(mat_view_name.encode('utf-8'))
                while (byte_len > 53 or mat_view_name.endswith(' ')
                        or mat_view_name.endswith('_')):
                    mat_view_name = mat_view_name[:-1]
                    byte_len = len(mat_view_name.encode('utf-8'))
                view_name = f"{mat_view_name}_view"
                vprint(f"Working with view: {view_name}")

                # Before we define the view, find the units it should use.
                unit = odmx.read_cv_units_by_id(odmx_db_con, units_id)
                assert unit, f"Could not find units with ID {units_id}"
                # TODO: Add in section handling units conversoin
                unit_conversion = None
                # TODO: Add in a section handling manual QA
                manual_qa_list = None
                # If the feeder db is foreign, we need to pull the data down
                # from the foreign db.
                if (odmx_db_con.info.host != feeder_db_con.info.host or
                    odmx_db_con.info.dbname != feeder_db_con.info.dbname or
                        odmx_db_con.info.port != feeder_db_con.info.port):
                    vprint(
                        "Feeder db is foreign. Pulling data from foreign db.")
                    # Create the table if necessary.
                    with (db.schema_scope(odmx_db_con, 'feeder'),
                            db.schema_scope(feeder_db_con, 'feeder')):
                        count = db.cross_con_table_copy(feeder_db_con,
                                                        feeder_table,
                                                        odmx_db_con,
                                                        feeder_table)
                        if count:
                            vprint(
                                f"Inserted {count} rows into {feeder_table}")
                        else:
                            vprint(
                                f"Table {feeder_table} already updated.")

                # Create the view if necessary.
                create_view(
                    odmx_db_con,
                    feeder_table, view_name, unit,
                    col, unit_conversion
                )
                # Create/update the materialized view.
                view_df = materialize(odmx_db_con, mat_view_name, view_name,
                                      self.data_source_timezone,
                                      variable_id, units_id,
                                      manual_qa_list)
                acquiring_instrument_uuid = "02fba1a8-5b90-4207-80d9-460b7d767a33"
                if not view_df.empty:
                    create_datastream_entry(
                        odmx_db_con, mat_view_name, view_df, sf_id, acquiring_instrument_uuid,
                        variable_id, units_id)
                # print(df_sub.head())
            # create datastreams

            print("Materialized view creation complete.\n")
