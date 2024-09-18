#!/usr/bin/env python3

"""
Module for generic timeserties data in csv files or excel spreadsheets
"""

import os
import re
import datetime
from importlib.util import find_spec
import numpy as np
import pandas as pd
from meteostat import Point, Daily, Hourly
from odmx.support.file_utils import get_files, open_csv, clean_name, \
    open_spreadsheet, open_json, get_column_value
import odmx.support.db as db
from odmx.support.db import quote_id
from odmx.abstract_data_source import DataSource
from odmx.timeseries_ingestion import add_columns, get_latest_timestamp
# from odmx.timeseries_processing import general_timeseries_processing
from odmx.write_equipment_jsons import generate_equipment_jsons
from odmx.geochem_ingestion_core import fieldspecimen_child_sf_creation, \
    sampleaction_routine, write_sample_results, feature_action
from odmx.harvesting import commit_csv
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
        Harvest Meteostat data from the API and save it as a csv on your local drive.
        """

        self.param_df.set_index(
            "clean_name", inplace=True, verify_integrity=True)

        local_base_path = "./data/glbrc/Biofuel Cropping System Experiment"
        # tz = self.data_source_timezone

        # First check to make sure the proper directory exists.
        os.makedirs(local_base_path, exist_ok=True)

        # Check to see if the data already exists on our server.
        file_name = f'weather/meteostat/meteostat.csv'
        file_path = os.path.join(local_base_path, file_name)
        # If it does, we want to find only new data.
        server_df = None
        if os.path.isfile(file_path):
            # Grab the data from the server.
            args = {'parse_dates': [0]}
            server_df = open_csv(file_path, args=args, lock=True)
            # Find the latest timestamp
            last_server_time = server_df['datetime'].max()
            if isinstance(last_server_time, str):
                last_server_time = datetime.datetime.strptime(
                    last_server_time, '%Y-%m-%d %H:%M:%S'
                )

            # Add a minute to it so that we don't get it in the return (the
            # data is never so granular anyway).
            start = last_server_time + datetime.timedelta(minutes=1)
            end = datetime.datetime.utcnow().replace(microsecond=0)

        # The biofuels cropping system experiment began 1 January 2008
        else:
            start = datetime.datetime(2008, 1, 1)
            end = datetime.datetime.utcnow().replace(microsecond=0)

        # TODO: Set the locations for KBS & Arlington using the datasource json
        kbs = Point(42.39518, -85.37339)
        arl = Point(43.297152, -89.381846)

        # Download the data using meteostat
        def read_meteo(start, end, location):
            data = Daily(location, start, end)
            df = data.fetch()
            return df

        def clean_meteo(df):
            df.dropna(how='all', axis=1, inplace=True)
            df.index.rename(['datetime'], inplace=True)
            df.reset_index(inplace=True)
            df.rename(columns={'prcp': 'precipitation',
                               'tavg': 'air_temp_mean',
                               'tmin': 'air_temp_min',
                               'tmax': 'air_temp_max'}, inplace=True)
            return df

        # Clean kbs data, we don't want precip or temp data because the GLBRC data is better
        df_kbs = read_meteo(start, end, kbs)
        df_kbs = clean_meteo(df_kbs)
        df_kbs['site'] = 'KBS'
        df_kbs.drop(columns=['precipitation', 'air_temp_min',
                    'air_temp_max', 'air_temp_mean'], inplace=True)

        # For Arlington we'll take everything bc we don't have any met data for that site
        df_arl = read_meteo(start, end, arl)
        df_arl = clean_meteo(df_arl)
        df_arl['site'] = 'Arlington'

        df = pd.concat([df_arl, df_kbs], ignore_index=True)

        # df['datetime'] = df['datetime'].dt.tz_convert(tz)
        # df['datetime'] = df['datetime'].dt.tz_convert(None)

        if len(df) == 0:
            print("No new data weather data available.")
            return

        # droplist = []
        # skipped_parameters = []
        # for column in df.columns:
        #    if column not in list(self.param_df.index):
        #        droplist.append(column)
        #        skipped_parameters.append(column)
        # vprint(f"skipped parameters: {skipped_parameters}. To keep these, "
        #       "add them to the glbrc mapper")
        # df.drop(columns=droplist, inplace=True, errors='ignore')

        if len(df) >= 1:
            commit_csv(file_path, df, server_df, to_csv_args={
                       'date_format': '%Y-%m-%d %H:%M:%S'})

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

                # Drop empty columns
                part_df = part_df.dropna(axis=1, how='all')

                # Limit the dataframe to the first twenty lines when testing the ingestion
                # part_df = part_df.iloc[:20]

                # Correct the formatting of time column - we need %H:%M:%S'
                try:
                    # Attempt to parse the timestamp with automatic format detection
                    part_df['timestamp'] = pd.to_datetime(
                        part_df[time_col], errors='raise')
                    part_df['timestamp'] = part_df['timestamp'].dt.tz_localize(
                        None)
                except ValueError:
                    try:
                        # Attempt to parse the timestamp with the format '%Y-%m-%d %H:%M:%S'
                        part_df['timestamp'] = pd.to_datetime(
                            part_df[time_col], format='%Y-%m-%d %H:%M:%S', errors='raise')
                    except ValueError:
                        try:
                            # Attempt to parse the timestamp with the format '%Y-%m-%d'
                            part_df['timestamp'] = pd.to_datetime(
                                part_df[time_col], format='%Y-%m-%d', errors='raise')
                            # Add default time '00:00:00' to the parsed date
                            part_df['timestamp'] = part_df['timestamp'] + \
                                pd.to_timedelta('00:00:00')
                        except ValueError as e:
                            raise ValueError(
                                f"Failed to parse datetime column '{time_col}' in file '{file}'.") from e

                print(part_df['timestamp'][0])
                print(part_df['timestamp'].dtype)

                # Ensure the 'timestamp' column is in datetime format
                assert part_df['timestamp'].dtype == 'datetime64[ns]'
                '''try:
                        # Attempt to parse the timestamp with automatic format detection
                    part_df['timestamp'] = pd.to_datetime(part_df[time_col])
                except ValueError:
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

                    assert part_df['timestamp'].dtype == 'datetime64[ns]' '''

                part_df = part_df.dropna(subset=['timestamp'])

                # Add to list of dataframes
                dfs.append(part_df)

            # Combine all dfs, drop duplicates, and sort by timestamp
            # We do not acutally drop duplicates because we can have multiple
            # measurements from the same place, at the same time
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
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

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
        file_type = data_source_config["data_file_type"]

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

        # Ensure that we only process each feeder_table once
        processed_feeder_tables = set()
        # NOTE: Skipping the trace gas data until we get an update on it's location from Sven
        processed_feeder_tables.add(
            'feeder_n2o,_co2,_and_ch4_by_icos_and_tga_(2021_to_present))_')

        print('iterating through feeder tables')
        for feeder_table in feeder_tables:
            if feeder_table in processed_feeder_tables:
                print(
                    f'Skipping already processed feeder_table: {feeder_table}')
                continue
            print(f'Processing feeder_table: {feeder_table}')
            # see if we have this sf in our sampling features
            sampling_feature = \
                odmx.read_sampling_features_one_or_none(
                    odmx_db_con, sampling_feature_code=sampling_feature_code)
            print(
                f'feeder_table: {feeder_table} | sampling_feature: {sampling_feature}')
            if not sampling_feature:
                print(f'did not find an associated sampling feature with the '
                      f'feeder table: {feeder_table}')

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
            # data_df = data_df[~data_df['site'].str.contains('Arlington')]

            # We can drop variables in order to facilitate testing
            # List of variables to keep
            # variables = ["yield_mg_ha", "moisture_at_harvest", "standard_moisture",
            #             "yield_std_moisture_bu_ac", "yield_std_moisture_mg_ha",
            #             "yield_dry_matter_mg_ha", "yield_dry_matter_tons_ac",
            #             "dry_matter_yield_Mg_ha", "dry_matter_yield_ton_acre",
            #             "biomass_g", "dry_matter_yield_Kg_ha", "year"]
            # Drop calculated variables from the dataframe
            variables = ["c_g_m2", "n_g_m2",
                         "yield_dry_matter_tons_ac", "dry_matter_yield_ton_acre"]

            if variables:
                # Filter out columns that are in the variables list
                feeder_table_columns = [
                    col for col in feeder_table_columns if col not in variables]

                # Drop columns from data_df that are in the variables list
                data_df = data_df[feeder_table_columns]

            assert feeder_table_columns == data_df.columns.tolist()

            # There are some rows in the GLBRC data that are not pertaining to our experiment
            # Let's be sure not to process those rows
            treatments_to_drop = ['aux', 'SW', 'FT',
                                  'GT', 'BT', 'CF', 'DF', 'micro']

            # Check if 'treatment' column exists and filter rows if it does
            if 'treatment' in data_df.columns:
                # Create a regex pattern with the treatments to drop
                pattern = '|'.join(treatments_to_drop)
                data_df = data_df[~data_df['treatment'].str.contains(
                    pattern, case=False, na=False)]

            if 'replicate' in data_df.columns:
                # Create a regex pattern with the treatments to drop
                pattern = '|'.join(treatments_to_drop)
                data_df = data_df[~data_df['replicate'].str.contains(
                    pattern, case=False, na=False)]

            # Skip processing for weather data that predates the experiment
            data_df = data_df[data_df['timestamp'] >= '2008-01-01']

            data_df_rows = data_df.shape[0]
            data_df_columns = data_df.shape[1]

            # parameter_units = data_df.iloc[0]

            try:
                data_df['timestamp'] = pd.to_datetime(
                    data_df['timestamp'], format='%Y-%m-%d %H:%M:%S')
                data_df['timestamp'] = data_df['timestamp'].dt.strftime(
                    '%Y-%m-%d %H:%M:%S')
            except ValueError:
                data_df['timestamp'] = pd.to_datetime(
                    data_df['timestamp'], format='%Y-%m-%d')
                # data_df['timestamp'] += pd.to_timedelta(0)
                data_df['timestamp'] = data_df['timestamp'].dt.strftime(
                    '%Y-%m-%d %H:%M:%S')

            if 'treatment' in data_df.columns:
                # In some instances the feeder_table has an extra 0 that needs to be removed
                data_df['treatment'] = data_df['treatment'].apply(
                    lambda x: re.sub(r'(G)0(\d)', r'\1\2', x))

            # we iterate over tables from 1 (first one with data) to the last
            # one we use the timestamp to create a sample name, which will be
            # parentsamplingfeaturename_geochem_date

            for index in range(data_df_rows):
                timestamp = datetime.datetime.strptime(data_df.iloc[index, 1],
                                                       '%Y-%m-%d %H:%M:%S')
                sample_date = data_df.iloc[index, 1].replace(' ',
                                                             '-').replace(':',
                                                                          '-')

                # We need to create a mapping to generate child sampling features
                # This is not straight forward because there is not a consistent column
                # structure accross all feeder tables. Essentially, we need to ensure
                # that each row, which is associated with a location and time,
                # is assigned a child sampling feature. This is critical because some rows
                # can be duplicated accross feeder tables, and we use child sf to
                # prevent ingesting duplicate measurements into ODMX

                # TODO: Drop all rows with treatment keys that contain
                # The questoin is whether we do this in the ingest step -- yes bc this data
                # is unrelated to our experiment
                # aux, SW, FT, GT, BT, CF, and DF

                # Define a sampling feature code w/ columns shared by all data sources
                if feeder_table in ['feeder_leaf_area_index_(lai)_(2009_to_2017)_',
                                    'feeder_species_transect_plant_heights_(2018_to_present)_',
                                    'feeder_phenology_2010_2012_(2010_to_2012)_',
                                    'feeder_phenology_2013_2017_(2013_to_2017)_',
                                    'feeder_total_carbon_and_nitrogen_content_at_peak_biomass_(2008_',
                                    'feeder_poplar_damage_assessment_(2019)_',
                                    'feeder_species_transect_plant_heights_(2018_to_present)_',
                                    'feeder_n2o,_co2,_and_ch4_by_icos_and_tga_(2021_to_present))_',
                                    'feeder_soil_temperature_at_gas_sampling_(2008_to_2016)_',
                                    'feeder_7_lter+weather+station+daily+precip+and+air+temp+1724961']:
                    site = 'KBS'
                else:
                    try:
                        site = data_df.iloc[index]['site']
                    except ValueError:
                        site = data_df.iloc[index]['Site']

                if feeder_table == 'feeder_poplar_damage_assessment_(2019)_':
                    treatment = str(data_df.iloc[index]['plot'])[:2]
                    replicate = str(data_df.iloc[index]['plot'])[-2:]
                elif feeder_table == 'feeder_poplar_stand_counts_(2012_to_2014)_':
                    treatment = 'G8'
                    replicate = data_df.iloc[index]['replicate']
                elif feeder_table == 'feeder_n2o,_co2,_and_ch4_by_icos_and_tga_(2021_to_present))_':
                    treatment = str(data_df.iloc[index]['chamber_name'])[
                        :2].capitalize()
                    replicate = replicate = f'R{str(data_df.iloc[index]["chamber_name"])[-1:]}'
                else:
                    try:
                        treatment = data_df.iloc[index]['treatment']
                        replicate = data_df.iloc[index]['replicate']
                    except:
                        KeyError(
                            'The data does not have a treatment or replicate column')
                        treatment = None
                        replicate = None

                # Logic to map site strings to sampling feature code
                if 'KBS' in str(site):
                    site = 'KBS'
                elif 'Arlington' in str(site):
                    site = 'Arlington'
                else:
                    ValueError(f'{site} is not sampling features')

                site_params = [site]
                if treatment is not None:
                    site_params.append(treatment)
                if replicate is not None:
                    site_params.append(replicate)

                site_code = '-'.join(site_params).replace(' ', '')

                # Also note that in root biomass the biomass is averaged accross stations at KBS
                # and measured per station at Arlington
                # Add in additional details that may or may not be contained wihtin the feeder_table
                plot_section, source_column = get_column_value(
                    data_df, index, 'main_or_microplot', 'windrow', 'field_section', 'station', 'transect', 'row', 'direction', 'nitrogen_fertilization', 'transect_position', 'sample_id')
                # Determine subplot based on source_column
                if source_column == 'field_section':
                    subplot, _ = get_column_value(
                        data_df, index, 'subplot', 'location_no')
                elif source_column == 'station':
                    subplot = None
                else:
                    subplot, _ = get_column_value(
                        data_df, index, 'subplot', 'field_section', 'location_no', 'station', 'line')

                campaign, _ = get_column_value(
                    data_df, index, 'campaign')
                depth_columns = [
                    'depth', 'depth_cm', 'top_depth', 'horizon_top_depth', 'soil_depth_cm', 'top_depth_cm']
                depth, depth_col = get_column_value(
                    data_df, index, *depth_columns)
                fraction, _ = get_column_value(
                    data_df, index, 'fraction')
                species, _ = get_column_value(
                    data_df, index, 'species', 'spcies')
                tree_id, _ = get_column_value(
                    data_df, index, 'tree_id')
                plant_id, _ = get_column_value(
                    data_df, index, 'plant_id')
                location, _ = get_column_value(
                    data_df, index, 'location')
                material, _ = get_column_value(
                    data_df, index, 'material')
                method, _ = get_column_value(
                    data_df, index, 'method')
                microplot, _ = get_column_value(
                    data_df, index, 'microplot')

                # Build collected_sf_code based on the presence of values
                parts = [sampling_feature_code,
                         sample_date, site_code]  # feeder_table,

                if plot_section is not None and pd.notna(plot_section):
                    parts.append(plot_section)
                if subplot is not None and pd.notna(subplot):
                    parts.append(subplot)
                if microplot is not None and pd.notna(microplot):
                    parts.append(microplot)
                if campaign is not None and pd.notna(campaign):
                    parts.append(campaign)
                if species is not None and pd.notna(species):
                    parts.append(species)
                if fraction is not None and pd.notna(fraction):
                    parts.append(fraction)
                if tree_id is not None and pd.notna(tree_id):
                    parts.append(tree_id)
                if plant_id is not None and pd.notna(plant_id):
                    parts.append(plant_id)
                if location is not None and pd.notna(location):
                    parts.append(location)
                if material is not None and pd.notna(material):
                    parts.append(material)
                if method is not None and pd.notna(method):
                    parts.append(method)
                if depth is not None and pd.notna(depth):
                    # print(f'depth_col: {depth_col}: {depth}')
                    parts.append(depth)

                collected_sf_code = '_'.join(parts).replace(' ', '')
                # We have one unqiue case for the soil water chemistry data where multiple
                # measurements exist per location without a way to differentiate them
                # We need to assign these measurements an additional identifier so that we
                # can ingest them into ODMX. Here we use their row index.
                if feeder_table in ['feeder_soil_water_chemistry_(2009_to_2021)_',
                                    'feeder_species_transect_plant_heights_(2018_to_present)_',
                                    'feeder_poplar_stem_diameter_and_biomass_measurements_(2008_to_p']:
                    collected_sf_code = f'{collected_sf_code}_{index}'

                # print(f'collected_sf_code: {collected_sf_code}')

                # fix the loop without causing the code to repeat itself
                relation = 'wasCollectedAt'
                # we now have the name of the sample and the relation
                # we will now create the sample in our ODMX database
                # within this call we also create the entry in specimens
                # Map speciment type and medium to the proper definition
                if 'soil' in feeder_table:
                    specimen_medium_cv = 'soil'
                    specimen_type_cv = 'grab'  # biota
                elif 'root' in feeder_table:
                    specimen_medium_cv = 'root'
                    specimen_type_cv = 'biota'
                elif 'tissue' in feeder_table:
                    specimen_medium_cv = 'tissue'
                    specimen_type_cv = 'biota'
                elif 'icos' in feeder_table:
                    specimen_medium_cv = 'gas'
                    specimen_type_cv = 'automated'
                elif 'gas' in feeder_table:
                    specimen_medium_cv = 'gas'
                    specimen_type_cv = 'automated'
                elif 'weather' in feeder_table:
                    specimen_medium_cv = 'gas'
                    specimen_type_cv = 'automated'
                else:
                    specimen_medium_cv = 'plant'
                    specimen_type_cv = 'biota'

                specimen_sf_id, new_specimen = fieldspecimen_child_sf_creation(
                    odmx_db_con, timestamp, sampling_feature_code,
                    collected_sf_code, relation, specimen_type_cv,
                    specimen_medium_cv, specimen_collection_id
                )

                # If new_specimen is False then we have already processed these results
                if new_specimen:
                    feature_action_id = feature_action(odmx_db_con, action_id,
                                                       specimen_sf_id)
                    print(
                        f'Writing feature_action_id for {specimen_sf_id}')

                    # Now we run through the entries in the row.
                    # the first value is the index and the second is the timestamp
                    # (which we already read), which is why we start at 2
                    # TODO: Make the last print statement a need to add with reference
                    # to each feeder table
                    need_to_add = []

                    for rowind in range(2, data_df_columns):
                        rowvalue = data_df.iloc[index][rowind]
                        if not rowvalue == 'NaN':
                            # if rowind - 2 < len(feeder_table_columns):
                            #    clean_name = feeder_table_columns[rowind - 2]
                            clean_name = feeder_table_columns[rowind]
                            print(f'clean_name: {clean_name}')
                            odmx_cv_term = self.param_df["cv_term"][clean_name]
                            print(f'odmx_cv_term: {odmx_cv_term}')
                            cv_unit = self.param_df.loc[self.param_df['cv_term']
                                                        == odmx_cv_term, 'cv_unit'].values[0]
                            print(f'cv_unit: {cv_unit}')
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
                                # Check if we are dealing with yieldDryMatter
                                if odmx_cv_term == 'yieldDryMatter':
                                    rowvalue = float(rowvalue)
                                    # Check if that variable has the correct units
                                    if str(odmx_unit.term) == 'megagramPerHectare':
                                        pass
                                    elif str(odmx_unit.term) == 'kilogramPerHectare':
                                        # Convert kilograms per hectare to megagrams per hectare
                                        rowvalue = (rowvalue * 0.001)
                                        units_id = 1214

                                    elif str(odmx_unit.term) == 'tonPerAcre':
                                        # Convert tons per acre to megagrams per hectare
                                        rowvalue = (
                                            (rowvalue * 0.907185) * 2.471)
                                        units_id = 1214

                                    elif str(odmx_unit.term) == 'bushelsPerAcre':
                                        # Determine the conversion factor based on the crop type
                                        crop = str(
                                            data_df.iloc[index]['crop'])
                                        if crop == 'corn':
                                            conv_factor = 25.4012
                                        elif crop == 'canola':
                                            conv_factor = 22.25
                                        elif crop == 'soybean':
                                            conv_factor = 27.2155
                                        else:
                                            raise ValueError(
                                                f"Unknown crop type: {crop}")

                                        # Convert bushels per acre to megagrams per hectare
                                        rowvalue = (
                                            ((rowvalue * conv_factor) * 0.001) * 2.471)
                                        units_id = 1214

                                    else:
                                        raise ValueError(
                                            f"Unknown unit: {odmx_unit}")

                                    assert units_id == 1214

                                # For some reason our conversion is not applying to yields reported in kg/ha
                                # So we are going to specify the conversion specifically for the appropriate vars
                                # dry_yields = [
                                #    'dry_matter_yield_Kg_ha', 'dry_matter_yield_kg_ha']
                                # if clean_name in dry_yields:
                                #    rowvalue = float(rowvalue)
                                    # if rowvalue > 100:
                                #    rowvalue = (rowvalue * 0.001)
                                #    units_id = 1214

                                if feeder_table == 'feeder_soil_moisture_at_gas_sampling_(2008_to_present)':
                                    if odmx_cv_term == 'soilGravimetricWaterContent':
                                        rowvalue = float(rowvalue)
                                        rowvalue = (rowvalue * 100)
                                        units_id = 1213

                                elif odmx_cv_term == 'co2Flux':
                                    rowvalue = float(rowvalue)
                                    rowvalue = (rowvalue * 1000)
                                    units_id = 1223

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
                                    timezone = 'UTC'
                                    depth_m = None
                                    passed_result_id = None
                                    data_type = 'na'
                                    # Add in standard error and standard deviation if need be
                                    if '_sd' in clean_name:
                                        stddev = True
                                        passed_result_id = result_id
                                    else:
                                        stddev = False

                                    if clean_name == 'sel' or clean_name == 'sem':
                                        stderr = True
                                        passed_result_id = result_id
                                    else:
                                        stderr = False
                                    # result_id  tells us the id in the results table
                                    # so we can add other values
                                    censor_code = ''
                                    quality_code = ''
                                    result_id = write_sample_results(odmx_db_con,
                                                                     units_id, variable_id, rowvalue,
                                                                     timestamp, timezone, depth_m,
                                                                     data_type, passed_result_id,
                                                                     feature_action_id, stddev,
                                                                     censor_code, quality_code, stderr)
                                    print(
                                        f'Writing {rowvalue} to sample results as result_id {result_id}')
                            elif clean_name in extension_properties:
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
                            else:
                                ValueError(
                                    f'{clean_name} is neither a variable nor an extension property!')
                    print(f'New Variables to add: {need_to_add}')
            print(f'Processed feeder_table: {feeder_table}.')
            processed_feeder_tables.add(feeder_table)
