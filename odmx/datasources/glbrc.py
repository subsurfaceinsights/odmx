#!/usr/bin/env python3

"""
Module for generic timeserties data in csv files or excel spreadsheets
"""

import os
import datetime
from importlib.util import find_spec
import numpy as np
import pandas as pd
from odmx.support.file_utils import get_files, open_csv, clean_name, \
    open_spreadsheet, open_json
import odmx.support.db as db
from odmx.support.db import quote_id
from odmx.abstract_data_source import DataSource
from odmx.timeseries_ingestion import add_columns, get_latest_timestamp
# from odmx.timeseries_processing import general_timeseries_processing
from odmx.write_equipment_jsons import generate_equipment_jsons
from odmx.geochem_ingestion_core import fieldspecimen_child_sf_creation, \
    sampleaction_routine, write_sample_results, feature_action
from odmx.log import vprint
import odmx.data_model as odmx

mapper_path = find_spec("odmx.mappers").submodule_search_locations[0]


class GlbrcDataSource(DataSource):
    """
    Class for GLBRC data source objects.
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
            open_json(f'{mapper_path}/glbrc.json'))
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
            # TODO: change "unit_cv" from "datalogger_..." to "aquisition_date"
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
                f'feeder_{clean_name(self.data_source_name)}_{clean_name(sampling_feature)}'  # pylint:disable=line-too-long

            # Store tuple of sampling feature, equipment path, and feeder table
            self.split_list.append((sampling_feature,
                                    equipment_dir,
                                    feeder_table))

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
                    args.update({'float_precision': 'high',
                                 'usecols': columns})
                    part_df = open_csv(file, args=args, lock=True)

                # Correct the formatting of time column - we need %H:%M:%S'
                try:
                    part_df['timestamp'] = pd.to_datetime(
                        part_df[time_col], format='%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        # Attempt to parse the timestamp with the format '%Y-%m-%d'
                        part_df['timestamp'] = pd.to_datetime(
                            part_df[time_col], format='%Y-%m-%d')
                        # Add default time '00:00:00' to the parsed date
                        part_df['timestamp'] += pd.to_timedelta(0)
                        print(part_df['timestamp'][0])
                        # Reformat the timestamp column to '%Y-%m-%d %H:%M:%S'
                        # part_df['timestamp'] = part_df['timestamp'].dt.strftime(
                        #   '%Y-%m-%d %H:%M:%S')
                        # Parse again with the new format
                        # part_df['timestamp'] = pd.to_datetime(
                        #    part_df['timestamp'], format='%Y-%m-%d %H:%M:%S')
                    except ValueError as e:
                        raise ValueError(
                            f"Failed to parse datetime column '{time_col}' "
                            f"in file '{file}'.") from e

                assert part_df['timestamp'].dtype == 'datetime64[ns]'

                part_df = part_df.dropna(subset=['timestamp'])

                # Add to list of dataframes
                dfs.append(part_df)

            # Combine all dfs, drop duplicates, and sort by timestamp
            # TODO: Add logic here to prevent duplicated measurements
            # same plot on the same data from being ingested
            df = pd.concat(dfs, ignore_index=True)  # .drop_duplicates()
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

            # Make sure to cast year astype string, this is actually growing season
            if 'year' in df.columns:
                df['year'] = df['year'].astype(str)

            # Remove leading/trailing spaces
            # df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            # TODO: Identify all other instances where we need to map to nan
            if 'wi_plot_id' in df.columns:
                df['wi_plot_id'] = df['wi_plot_id'].replace(
                    'Not applicable', np.nan)

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

            # Normally we would call general_timeseries_ingestion here but as the
            # GLBRC data is somewhat unique we need to write our own code

            # Make sure that the columns are unique
            if len(df.columns) != len(set(df.columns)):
                raise ValueError(
                    "Ingestion DataFrame must have unique columns")

            # Ensure that the timestamp column is the first column in order.
            cols = df.columns.tolist()
            cols.insert(0, cols.pop(cols.index('timestamp')))
            df = df.loc[:, cols]

            # We need ensure that boolean columns are properly formatted for PostgreSQL.
            # df[df.columns[1:]] = df[df.columns[1:]].map(
            #    lambda x: 'TRUE' if x is True else 'FALSE' if x is False else x).astype(str)

            # We need to convert all NaN values to strings so that they are
            # preserved when we send the dataframe to PostgreSQL
            df[df.columns[1:]] = df[df.columns[1:]].replace([np.nan], 'NaN')
            # There were originally more checks here but they write our text data
            # to NoneType so we are leaving it at this for now

            # With that nonsense out of the way, check to see if the table exists in
            # the database.
            print(f"Checking to see if \"{feeder_table}\" exists"
                  " in the database.")
            table_exist = db.does_table_exist(feeder_db_con,
                                              feeder_table)
            # If it doesn't exist, create it.
            if not table_exist:
                print(f"\"{feeder_table}\" doesn't exist yet."
                      " Creating it.")
                # Since pandas.to_sql doesn't support primary key creation, we need
                # to handle that. First we make the table in PostgreSQL.
                query = f'''
                    CREATE TABLE {quote_id(feeder_db_con,
                                feeder_table)}
                        (index SERIAL PRIMARY KEY)
                '''

                feeder_db_con.execute(query)
                add_columns(feeder_db_con, feeder_table,
                            df.columns.tolist(), col_type='text', override_all=True)
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
                last_time = get_latest_timestamp(feeder_db_con,
                                                 feeder_table)
                print(f"Last timestamp in {feeder_table} is {last_time}")
                if last_time is not None:
                    df = df[df['timestamp'] > last_time]
                else:
                    print(f"\"{feeder_table}\" is empty. Adding"
                          " to it.")
                # If the DataFrame is empty, there's no new data to ingest.
                if df.empty:
                    print("No new data to ingest.\n")
                    return

            print(f"Populating {feeder_table}.")
            db.insert_many_df(feeder_db_con, feeder_table, df, upsert=False)

    def process(self, feeder_db_con, odmx_db_con, sampling_feature_code):  # , sampling_feature_code
        """"
        Process ingested GLBRC data into timeseries datastreams.
        """

        # Define file path from datasource info
        local_base_path = os.path.join(self.data_path, self.data_source_path)

        # Load data source config, we need this to define data_file_name later
        data_source_config = open_json(self.config_file)

        # Create list of extension property names
        extension_properties_df = db.query_df(
            odmx_db_con, 'extension_properties')
        extension_properties = extension_properties_df['property_name'].to_list(
        )

        # Now we're using the column headers applied during harvest so use
        # those for the param_df index instead (clean_name)
        self.param_df.set_index("clean_name", inplace=True,
                                verify_integrity=True)
        # Check what the feeder tables have
        feeder_tables = db.get_tables(feeder_db_con)
        vprint(f"Using feeder tables {feeder_tables}")
        # we convert the name of the feeder table into the associated sampling
        # feature table
        print('iterating through feeder tables')
        for feeder_table in feeder_tables:
            print(f'feeder_table: {feeder_table}')

            # see if we have this sf in our sampling features
            # sampling_feature = \
            #    odmx.read_sampling_features_one_or_none(
            #        odmx_db_con, sampling_feature_code=sampling_feature_code)
            # print(
            #    f'feeder_table: {feeder_table} | sampling_feature: {sampling_feature}')
            # if not sampling_feature:
            #    print(f'did not find an associated sampling feature with the '
            #          f'feeder table: {feeder_table}')

            # We create a specimen collection for this feeder_table and will associate
            # all sampling features with it.
            print("Creating a new specimen collection.")
            # TODO: decide if we want to name the specimen collection differently
            specimen_collection_cv = 'fieldCampaign'
            specimen_collection_note = ''
            # speciment_collection_parent_id = 1
            data_file_name = data_source_config["data_file_name"]
            data_file_path = os.path.join(local_base_path, data_file_name)
            specimen_collection_id = odmx.write_specimen_collection(
                odmx_db_con,
                specimen_collection_cv=specimen_collection_cv,
                specimen_collection_file=data_file_path,
                specimen_collection_name=data_file_name,
                specimen_collection_note=specimen_collection_note,
            )
            # Write to the `actions`, `action_by`, and `related_actions`
            # tables. Start by defining metadata info.
            # TODO: decide if we want to apply the correct analysis date
            analyst_name = 'GLBRC Analyst'
            analysis_date = datetime.datetime.utcnow()
            analysis_timezone = 'UTC'
            affiliation_id = 1  # This is hardcoded for weighing vegetation
            action_id = sampleaction_routine(odmx_db_con,
                                             analyst_name, affiliation_id, analysis_date,
                                             analysis_timezone, data_file_path)

            # Now that the high-level tables are out of the way we can move on to the data
            feeder_table_columns = db.get_columns(
                feeder_db_con, feeder_table)
            data_df = db.query_df(feeder_db_con, feeder_table)

            # Drop all data from Arlington sites
            data_df = data_df[~data_df['site'].str.contains('Arlington')]

            # We can drop variables in order to facilitate testing
            # List of variables to keep
            # variables = ["yield_mg_ha", "moisture_at_harvest", "standard_moisture",
            #             "yield_std_moisture_bu_ac", "yield_std_moisture_mg_ha",
            #             "yield_dry_matter_mg_ha", "yield_dry_matter_tons_ac",
            #             "dry_matter_yield_Mg_ha", "dry_matter_yield_ton_acre",
            #             "biomass_g", "dry_matter_yield_Kg_ha", "year"]
            variables = []

            if len(variables) > 0:

                feeder_table_columns = feeder_table_columns[:2] + [
                    col for col in feeder_table_columns[2:] if col in variables]

                # Drop columns that are not in the columns_to_keep list
                data_df = data_df[feeder_table_columns]

            assert feeder_table_columns == data_df.columns.tolist()

            data_df_rows = data_df.shape[0]
            data_df_columns = data_df.shape[1]

            # parameter_units = data_df.iloc[0]

            data_df['timestamp'] = pd.to_datetime(
                data_df['timestamp'], format='%Y-%m-%d')
            # data_df['timestamp'] += pd.to_timedelta(0)
            data_df['timestamp'] = data_df['timestamp'].dt.strftime(
                '%Y-%m-%d %H:%M:%S')

            # we iterate over tables from 1 (first one with data) to the last
            # one we use the timestamp to create a sample name, which will be
            # parentsamplingfeaturename_geochem_date

            for index in range(data_df_rows):
                timestamp = datetime.datetime.strptime(data_df.iloc[index, 1],
                                                       '%Y-%m-%d %H:%M:%S')
                sample_date = data_df.iloc[index, 1].replace(' ',
                                                             '-').replace(':',
                                                                          '-')

                # Define a sampling feature code w/ columns shared by all data sources
                site = data_df.iloc[index]['site']
                treatment = data_df.iloc[index]['treatment']
                replicate = data_df.iloc[index]['replicate']

                # Logic to map site strings to sampling feature code
                if 'KBS' in str(site):
                    site = 'KBS'
                elif 'Arlington' in str(site):
                    site = 'Arlington'
                else:
                    ValueError(f'{site} is not sampling features')

                sampling_feature_code = f'{site}-{treatment}-{replicate}'

                # Capture any other info that can be used to distinguish specimens
                try:
                    plot_section = data_df.iloc[index]['main_or_microplot']
                except:
                    try:
                        # Sometime we don't have main or microplot but we have windrows
                        plot_section = data_df.iloc[index]['windrow']
                    except:
                        # Here field section denotes main or microplot
                        plot_section = data_df.iloc[index]['field_section']
                        field_section_as_main_micro = True

                try:
                    subplot = data_df.iloc[index]['subplot']
                except:
                    try:
                        if field_section_as_main_micro:
                            pass
                        subplot = data_df.iloc[index]['field_section']
                    except:
                        try:
                            subplot = data_df.iloc[index]['location_no']
                        except:
                            subplot = None

                # Create collected sampling feature code for child specimens
                if subplot is None:
                    collected_sf_code = (f'{sampling_feature_code}_'
                                         f'harvest_{sample_date}_'
                                         f'{plot_section}')
                else:
                    collected_sf_code = (f'{sampling_feature_code}_'
                                         f'harvest_{sample_date}_'
                                         f'{plot_section}-{subplot}')

                relation = 'wasCollectedAt'
                # we now have the name of the sample and the relation
                # we will now create the sample in our ODMX database
                # within this call we also create the entry in specimens
                specimen_type_cv = 'grab'
                specimen_medium_cv = 'plant'
                specimen_sf_id, new_specimen = fieldspecimen_child_sf_creation(
                    odmx_db_con, timestamp, sampling_feature_code,
                    collected_sf_code, relation, specimen_type_cv,
                    specimen_medium_cv, specimen_collection_id
                )

                # If new_specimen is False then we have already processed these results
                if new_specimen is False:
                    # print('Breaking loop at index:', index)
                    break
                else:
                    # we create a feature_action_id which will make the bridge
                    # between the specimen and the results
                    feature_action_id = feature_action(odmx_db_con, action_id,
                                                       specimen_sf_id)
                    print(
                        f'Writing feature_action_id for {specimen_sf_id}')

                    # Now we run through the entries in the row.
                    # the first value is the index and the second is the timestamp
                    # (which we already read), which is why we start at 2
                    need_to_add = []

                    for rowind in range(2, data_df_columns):
                        rowvalue = data_df.iloc[index][rowind]
                        if not rowvalue == 'nan':
                            # if rowind - 2 < len(feeder_table_columns):
                            #    clean_name = feeder_table_columns[rowind - 2]
                            clean_name = feeder_table_columns[rowind]
                            odmx_cv_term = self.param_df["cv_term"][clean_name]
                            cv_unit = self.param_df.loc[self.param_df['cv_term']
                                                        == odmx_cv_term, 'cv_unit'].values[0]

                            # we have two cases.
                            # If the clean name is not in our extension properties list
                            # then we write the row to results
                            # If it is in the list then we write to extension properties

                            if clean_name not in extension_properties:
                                # we have all the data we need to parse
                                # we first look up the parameter id and the units
                                # id and then we create a result associated with
                                # the sample
                                # print(f'cv_unit: {cv_unit}')
                                odmx_unit = odmx.read_cv_units_one_or_none(
                                    odmx_db_con, term=cv_unit)
                                assert odmx_unit is not None
                                units_id = odmx_unit.units_id
                                odmx_variable = \
                                    odmx.read_variables_one_or_none(
                                        odmx_db_con, variable_term=odmx_cv_term)
                                if odmx_variable is None:
                                    need_to_add.append(
                                        (odmx_cv_term, clean_name))
                                    # variable_id = None
                                else:
                                    variable_id = odmx_variable.variable_id
                                    # now we know the variable and units and the
                                    # value. We are ready to create a result and
                                    # associated values in the odmx database
                                    # TODO: should we be writing to related features somewhere?
                                    # related_features_relation_id = \
                                    #    odmx.read_related_features_one_or_none(
                                    #        odmx_db_con,
                                    #        sampling_feature_id=specimen_sf_id).relation_id

                                    # now we write the results
                                    # Set placeholder variables for the write sample
                                    # results routine
                                    timezone = 'est'
                                    depth_m = None
                                    passed_result_id = None
                                    data_type = 'na'
                                    stddev = False
                                    # result_id  tells us the id in the results table
                                    # so we can add other values
                                    censor_code = ''
                                    quality_code = ''
                                    result_id = write_sample_results(odmx_db_con,
                                                                     units_id, variable_id, rowvalue,
                                                                     timestamp, timezone, depth_m,
                                                                     data_type, passed_result_id,
                                                                     feature_action_id, stddev,
                                                                     censor_code, quality_code)
                                    print(
                                        f'Writing {rowvalue} to sample results as result_id {result_id}')
                            else:
                                with odmx_db_con.transaction():
                                    con = odmx_db_con
                                    glbrc_property_id = odmx.read_extension_properties_one(
                                        con,
                                        property_name=clean_name).property_id
                                    assert glbrc_property_id is not None

                                odmx.write_sampling_feature_extension_property_values(
                                    con,
                                    sampling_feature_id=specimen_sf_id,
                                    property_id=glbrc_property_id,
                                    property_value=rowvalue
                                )
                                print(f'Writing sampling feature extension property '
                                      f'for {glbrc_property_id}: {rowvalue}')
                    print(f'New Variables to add: {need_to_add}')
            print(f'Processed {feeder_table}.')
