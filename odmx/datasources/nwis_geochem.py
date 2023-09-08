#!/usr/bin/env python3

"""
Module for NWIS data harvesting, ingestion, and processing.
"""

import os
import datetime
import pandas as pd
from dataretrieval import nwis
import odmx.support.general as ssigen
import odmx.support.db as db
from odmx.abstract_data_source import DataSource
from odmx.timeseries_ingestion import add_columns
from odmx.ssi.geochem_ingestion import fieldspecimen_child_sf_creation,\
    sampleaction_routine, write_sample_results, feature_action
from odmx.harvesting import commit_csv
from odmx.log import vprint
import odmx.data_model as odmx


class NwisGeochemDataSource(DataSource):
    """
    Class for NWIS data source objects.
    """

    def __init__(self, project_name, project_path, data_path,
                 data_source_timezone, site_code):
        self.project_name = project_name
        self.project_path = project_path
        self.data_path = data_path
        self.data_source_timezone = data_source_timezone
        self.data_source_path = 'nwis_geochem'
        self.site_code = site_code
        self.feeder_table = f'nwis_geochem_{site_code}'
        self.param_df = pd.DataFrame(ssigen.open_json((project_path +
                                            '/mappers/nwis_geochem.json')))
        self.remarks_df = pd.DataFrame(
            ssigen.open_json((f"{project_path}/mappers/"
                             "usgs_remarks_to_censor_codes.json")))
        self.remarks_df.set_index("remark_cd", inplace=True,
                                  verify_integrity=True)
        self.vqc_df = pd.DataFrame(
            ssigen.open_json((f"{project_path}/mappers/usgs_val_quals.json")))
        self.vqc_df.set_index("val_qual_cd", inplace=True,
                                  verify_integrity=True)

#Need to try turning the string into a timestamp also
    def apply_annotations(self, element):
        """
        Function to parse USGS annotations (remarks and qualifier codes)

        This is intended for use in the ingestion step, with data read in from
        a csv as strings, and has been reworked with that in mind. It will not
        work in the harvest step as written.
        """
        remark = None
        qualifiers = None
        value = None
        if isinstance(element, (float, int)):
            annotated = str(element)
        elif element.strip() == "nan":
            annotated = element.strip()
        else:
            element.strip()
            try:
                float(element)
                annotated = str(element)
            except ValueError:

                try:
                    datetime.datetime.strptime(element, '%Y-%m-%d %H:%M:%S')
                    annotated = element
                except ValueError:
                    parts = element.split()
                    if len(parts) == 2:
                        try:
                            float(parts[0])
                            value, qualifiers = parts
                        except:  # pylint: disable=bare-except
                            remark, value = parts
                    elif len(parts) == 3:
                        remark, value, qualifiers = parts
                    elif len(parts) == 1:
                        # For present but not quantified, there's just a remark
                        # I think this is the only case that would apply to
                        remark = parts[0]
                    else:
                        raise ValueError(f"Too many parts to result {element}."
                                         " Maximum is three: "
                                         "remark, value, qualifiers")
                    if value:
                        annotated = value
                    else:
                        annotated = ''
                    if remark:
                        prefix = self.remarks_df["censor_cv"][remark]
                        annotated = f'{prefix}_{annotated}'
                    if qualifiers:
                        postfix = ''
                        for char in qualifiers:
                            postfix += f'_qualifier_{self.vqc_df["val_qual_nm"][char]}'  # pylint: disable=line-too-long
                        postfix.replace('__', '_')
                        annotated += f'_vqc{postfix}'
        return annotated

    def harvest(self):
        """
        Harvest NWIS data from the API and save it to a .csv on our servers.
        """
        # Set USGS code to param_df index for easier lookup
        self.param_df.set_index("id", inplace=True, verify_integrity=True)


        # Define variables from the harvesting info file.
        local_base_path = os.path.join(self.data_path, self.data_source_path)
        site_code = self.site_code
        tz = self.data_source_timezone

        # First check to make sure the proper directory exists.
        os.makedirs(local_base_path, exist_ok=True)

        # Check to see if the data already exists on our server.
        file_name = f'nwis_geochem_{site_code}.csv'
        file_path = os.path.join(local_base_path, file_name)
        # If it does, we want to find only new data.
        server_df = None
        if os.path.isfile(file_path):
            # Grab the data from the server.
            args = {'parse_dates': [0], 'header': [0,1]}
            server_df = ssigen.open_csv(file_path, args=args, lock=True)
            # Find the latest timestamp.
            last_server_time = server_df['datetime'].max()['timestamp']
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
                               service='qwdata',
                               start=start.strftime("%Y-%m-%d"),
                               end=end.strftime("%Y-%m-%d"))

        # If nothing was returned, we're done.
        if data.empty:
            print(f"No new data available for NWIS site {site_code}.\n")
            return
        # Otherwise, we examine the data.
        # If data exists, first drop qa/qc columns and 'site_no' column
        droplist = ['site_no', 'sample_dt', 'sample_tm', 'sample_end_dt',
                    'sample_end_tm', 'tu_id', 'body_part_id',
                    'sample_lab_cm_txt']
        skipped_parameters = []
        for column in data.columns:
            if column not in list(self.param_df.index):
                droplist.append(column)
                skipped_parameters.append(column)
        vprint(f"skipped parameters: {skipped_parameters}. To keep these, "
               "add them to the NWIS geochem mapper")
        data.drop(columns=droplist, inplace=True, errors='ignore')
        mapper = {'datetime': 'datetime'}
        units = []
        for column in data.columns:
            mapper.update({column: self.param_df["clean_name"][column]})
            units.append(self.param_df["cv_unit"][column])

        # Move datetime to its own column and then reset index
        data['datetime'] = data.index
        data.reset_index(drop=True, inplace=True)

        # localize datetime column
        data['datetime'] = data['datetime'].dt.tz_convert(tz)
        data['datetime'] = data['datetime'].dt.tz_localize(None)

        # Rename columns and add units row to header
        # Has to happen after datetime conversion because renaming doesn't work
        # on MultiIndex, and empty unit for datetime breaks datetime operations
        units.append('timestamp')
        data.rename(columns=mapper, inplace=True)
        new_header = [list(data.columns), units]
        data.columns = new_header


        # We update the .csv files on the server if we actually got new data.
        if not data.empty:
            # print(f"New headers are: {list(data.columns)}")
            # print(f"Old headers were: {list(server_df.columns)}")
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
        file_name = f'nwis_geochem_{site_code}.csv'
        file_path = os.path.join(local_base_path, file_name)
        # Create a DataFrame of the file.
        args = {'float_precision': 'high', 'header': [0,1]}
        df = ssigen.open_csv(file_path, args=args, lock=True)

        # clean up remarks and qualifier codes in results
        df = df.applymap(self.apply_annotations)

        # Rename datetime column to timestamp to match convention
        df['timestamp'] = pd.to_datetime(df[('datetime', 'timestamp')])
        df.drop(columns='datetime', inplace=True)
        df.sort_values(by='timestamp', inplace=True)
        df.reset_index(drop=True, inplace=True)

        # Split header back into two rows
        headers = [col[0] for col in list(df.columns)]
        units = [col[1] for col in list(df.columns)]

        df.columns = headers
        df.loc[-1] = units
        df = df.sort_index().reset_index(drop=True)

        # Create feeder table
        # This is very similar to the code for general_timeseries_ingestion
        # But adapted to deal with the headers here
        if len(df.columns) != len(set(df.columns)):
            raise ValueError("Ingestion DataFrame must have unique columns")

        # As a final check on the data, drop any missed duplicates.
        df.drop_duplicates('timestamp', inplace=True, ignore_index=True)
        # Ensure that the timestamp column is the first column in order.
        cols = df.columns.tolist()
        cols.insert(0, cols.pop(cols.index('timestamp')))
        df = df.loc[:, cols]

        # With that nonsense out of the way, check to see if the table exists
        #  in the database.
        print(f"Checking to see if \"{self.feeder_table}\" exists"
              " in the database.")
        table_exist = db.does_table_exist(feeder_db_con, self.feeder_table)
        # If it doesn't exist, create it.
        if not table_exist:
            print(f"\"{self.feeder_table}\" doesn't exist yet."
                  " Creating it.")
            # Since pandas.to_sql doesn't support primary key creation, we need
            # to handle that. First we make the table in PostgreSQL.
            query = f'''
                CREATE TABLE {db.quote_id(feeder_db_con,
                               self.feeder_table)}
                    (index SERIAL PRIMARY KEY)
            '''
            feeder_db_con.execute(query)
            add_columns(feeder_db_con, self.feeder_table,
                    df.columns.tolist(), col_type='text', override_all=True)
        # If it does exist, we only want to add new data to it.
        else:
            # First check if we added any columns
            print(f"\"{self.feeder_table}\" already exists.")
            existing_columns = set(db.get_columns(feeder_db_con,
                                                      self.feeder_table))
            data_columns = set(df.columns.tolist())
            new_columns = data_columns - existing_columns
            if len(new_columns) > 0:
                print(f"Adding new columns '{new_columns}'")
                add_columns(feeder_db_con, self.feeder_table,
                            new_columns, col_type='text', override_all=True)
            query = f'''
                SELECT timestamp FROM
                    {db.quote_id(feeder_db_con,
                               self.feeder_table)}
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
                print(f"\"{self.feeder_table}\" is empty. Adding"
                      " to it.")

        print(f"Populating {self.feeder_table}.")
        db.insert_many_df(feeder_db_con, self.feeder_table, df, upsert=False)


    def process(self, feeder_db_con, odmx_db_con, sampling_feature_code):
        """
        Process ingested NWIS data into timeseries datastreams.

        Build the geochemical datastreams: NOTE THIS WILL BE EDITED LATER - NOT
        CORRECT NOW
        a) We look for all locations which have specimens.
        b) For each of these locations, we get the parameters we have for these
           locations.
        c) We check whether we already have an entry for these in the
           sampling_feature_timeseries_datastreams table. We do this by
           matching the sampling_feature_id, variable_id, datastream_tablename.
        d) If we already have a table we update it.
        e) If not we create a new entry.

         The NWIS geochem feeder tables have the name nwis_geochem_$USGSid, so
         e.g. nwis_geochem_09111250. In each feeder table we have a row of
         entries that correspond to what we need to parse
         the NWIS sampling features on the other hand have the name
         NWIS-$USGSid
        """
        # Define file path from datasource info
        local_base_path = os.path.join(self.data_path, self.data_source_path)
        site_code = self.site_code

        # Now we're using the column headers applied during harvest so use
        # those for the param_dff index instead (celan_name)
        self.param_df.set_index("clean_name", inplace=True,
                                verify_integrity=True)

        # we see what feeder tables we have
        feeder_tables=db.get_tables(feeder_db_con)
        print(feeder_tables)
        # we convert the name of the feeder table into the associated sampling
        # feature table
        print('iterating through feeder tables')
        for feeder_table in feeder_tables:
            print(feeder_table)
            # sf_code=feeder_table.replace('nwis_geochem_','NWIS-')
            # see if we have this sf in our sampling features
            sampling_feature = \
                odmx.read_sampling_features_one_or_none(
                    odmx_db_con, sampling_feature_code=sampling_feature_code)
            print(feeder_table, sampling_feature)
            if not sampling_feature:
                print('did not find an associated sampling feature with the '
                      'feeder table')
            """
            We create a specimen collection for this entry and will associate
            all sampling features with it.
            """
            print("Creating a new specimen collection.")
            specimen_collection_cv = 'analysisReport'
            specimen_collection_note = \
                ('Analysis report from USGS for samples '
                 f'from{sampling_feature.sampling_feature_code}')
            # TODO figure out what this is supposesd to be and do a lookup
            #specimen_collection_parent_id = 1
            data_file_name = f'nwis_geochem_{site_code}.csv'
            data_file_path = os.path.join(local_base_path, data_file_name)
            specimen_collection_id = odmx.write_specimen_collection(
                odmx_db_con,
                specimen_collection_cv = specimen_collection_cv,
                specimen_collection_file = data_file_path,
                specimen_collection_name = data_file_name,
                specimen_collection_note = specimen_collection_note,
            )
            # Write to the `actions`, `action_by`, and `related_actions`
            # tables. Start by defining metadata info.
            analyst_name = 'USGS Analyst'
            analysis_date = datetime.datetime.utcnow()
            analysis_timezone = 'UTC'
            # right now the affiliation is hardcoded.. We need to look this
            # up by organization (TODO)
            affiliation_id = 1
            action_id = sampleaction_routine(odmx_db_con,
                    analyst_name,affiliation_id, analysis_date,
                                        analysis_timezone, data_file_path)

            """
            ok, now we know the sampling feature that these samples are
            associated with
            we now read one row at a time. We do the following
            a) we create a new sample, and associate that with the parent
            sampling feature
            b) we create results for each sample
            """
            # we read the data
            feeder_table_columns = db.get_columns(feeder_db_con,feeder_table)
            data_df = db.query_df(feeder_db_con,feeder_table)
            data_df_rows = data_df.shape[0]
            data_df_columns = data_df.shape[1]

            print('rows,columns ', data_df_rows, data_df_columns)

            parameter_units = data_df.iloc[0]
            # if we want to see what units are associated we can print this out
            # for index in feeder_table_columns:
            #      print(index,' : ',parameter_units[index])
            # we iterate over tables from 1 (first one with data) to the last
            # one we use the timestamp to create a sample name, which will be
            # parentsamplingfeaturename_geochem_date
            for index in range(1,data_df_rows):
                timestamp = datetime.datetime.strptime(data_df.iloc[index][1],
                                                       '%Y-%m-%d %H:%M:%S')
                sample_date = data_df.iloc[index][1].replace(' ',
                                                           '-').replace(':',
                                                                        '-')
                collected_sf_code = (f'{sampling_feature_code}'
                                     f'_geochem{sample_date}')
                relation = 'wasCollectedAt'
                # print(index,data_df.iloc[index][1],collected_sf_code)
                # we now have the name of the sample and the relation
                # we will now create the sample in our ODMX database
                # within this call we also create the entry in specimens
                specimen_type_cv = 'grab'
                specimen_medium_cv = 'liquidAqueous'
                specimen_sf_id, did_it_exist = fieldspecimen_child_sf_creation(
                         odmx_db_con, timestamp, sampling_feature_code,
                         collected_sf_code, relation, specimen_type_cv,
                         specimen_medium_cv,specimen_collection_id
                     )

                # note that the specimen_sf_id is the sampling
                # print('specimen_sf_id :', specimen_id)
                #
                # note: there is a lot of metadata we can store (e.g. about
                # who collected the data, where and when and how it was
                # analyzed, and so on. We created some of this, but it has some
                # fake information
                #
                # we create a feature_action_id which will make the bridge
                # between the specimen and the results

                feature_action_id = feature_action(odmx_db_con, action_id,
                                               specimen_sf_id)
                # Now we run through the entries in the row. Note that the USGS
                # can have a lot of results, but for most of the time they are
                # nan so we see if they are not nan and then we process them
                # Note that the nan is a string, so we need to do double quotes
                # the first value is the index and the second is the timestamp
                # (which we already read), which is why we start at 2
                need_to_add = []
                for rowind in range(2, data_df_columns):
                    rowvalue = data_df.iloc[index][rowind]
                    if not rowvalue == 'nan':
                        # CV has already been expanded, do still need to add
                        # attributes for fltered/unfiltered and lab/field though
                        clean_name = feeder_table_columns[rowind]
                        odmx_cv_term = self.param_df["cv_term"][clean_name]
                        # we already mapped the units to our own unit cvs
                        usgs_unit = parameter_units[rowind]
                        # we have two cases.
                        # First, the value us numeric, in which
                        # case the parsing is easy.
                        # Second, the value has text in it, in which case we
                        # need to do some more complex parsing
                        # note that we also want to catch scientific notations
                        # so we use float to test the value
                        try:
                            float(rowvalue)
                            print(f'Parsing {odmx_cv_term} {usgs_unit} '
                                  f'{rowvalue}')
                            # we have all the data we need to parse
                            # we first look up the parameter id and the units
                            # id and then we create a result associated with
                            # the sample
                            odmx_unit = odmx.read_cv_units_one_or_none(
                                odmx_db_con, term=usgs_unit)
                            print('odmx_unit : ', odmx_unit)
                            units_id = odmx_unit.units_id
                            odmx_variable = \
                                odmx.read_variables_one_or_none(
                                    odmx_db_con, variable_term=odmx_cv_term)
                            if odmx_variable is None:
                                need_to_add.append((odmx_cv_term, clean_name))
                            print('odmx_variable :', odmx_variable)
                            variable_id = odmx_variable.variable_id
                            #now we know the variable and units and the
                            #value. We are ready to create a result and
                            #associated values in the odmx database
                            related_features_relation_id = \
                                odmx.read_related_features_one_or_none(
                                    odmx_db_con,
                                    sampling_feature_id = \
                                        specimen_sf_id).relation_id
                            print('related features relation id :',
                                  related_features_relation_id)

                            # now we write the results
                            # Set placeholder variables for the write sample
                            # results routine
                            timezone = 'est'
                            depth_m = None
                            passed_result_id = None
                            data_type = 'na'
                            stddev = False
                            print('writing result')
                            print('units_id : ', units_id)
                            print('variable_id : ', variable_id)
                            print('rowvalue : ', rowvalue)
                            print('feature_action_id : ', feature_action_id)
                            # What is result_id supposed to be used for?
                            result_id = write_sample_results(odmx_db_con,
                                          units_id,variable_id, rowvalue,
                                          timestamp, timezone, depth_m,
                                          data_type, passed_result_id,
                                          feature_action_id,stddev)
                        except:  # pylint: disable=bare-except
                            print('this needs tlc', rowvalue)
        print(f'New Variables to add: {need_to_add}')
