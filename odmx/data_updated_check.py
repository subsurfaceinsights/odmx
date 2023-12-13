#!/usr/bin/env python3

"""
Checks to see if data is up-to-date based on update intervals in the
datasource_description table.
"""

import os
from pkg_resources import resource_filename
import argparse
import urllib.parse
import datetime
import yaml
import pandas as pd
from odmx.support import db
from odmx.support.api_client import ApiClient
from odmx.support.file_utils import open_json
from odmx import odmx

def main(project_name):
    """
    The main function.
    """

    # First off, set paths for use in the script.
    ssi_path = os.path.join('/opt', 'ssi')

    # Open and validate the project metadata .json file.
    project_meta_path = os.path.join(ssi_path, 'projects', project_name,
                                     f'{project_name}.json')
    project_meta_schema_path = resource_filename('odmx', 'json_schema')
    project_meta = open_json(project_meta_path,
                             validation_path=project_meta_schema_path)
    db_info = project_meta['databases']

    # Define initial constants.
    project_db = db_info['odmx_project']['name']
    odmx_schema = db_info['odmx_project']['schema']['odmx_schema']
    config_path = os.path.join(ssi_path, 'config', 'odmx')
    postgres_file = os.path.join(config_path, 'pgsql_config.yml')

    # Establish a connection to the local ODMX database.
    engine = db.engine(yml_file=postgres_file, db_name=project_db,
                                schema=odmx_schema)

    # Create the API connection.
    print("Establishing the remote API connection.")
    pull_config_file = os.path.join(config_path, 'pull_config.yml')
    with open(pull_config_file, 'r', encoding='utf-8') as f:
        config = yaml.load(f, Loader=yaml.Loader)
    api_endpoint = urllib.parse.urlparse(
        config['UPSTREAM_API_ENDPOINT']
    ).geturl()
    api = ApiClient(url=api_endpoint)

    # Get all sampling feature timeseries datastreams from the local database.
    local_datastreams = odmx.read_sampling_feature_timeseries_datastreams_all(
        engine)
    # Convert the datastreams into a DataFrame.
    local_datastreams_df = pd.DataFrame(local_datastreams)
    # Cull the DataFrame to what we need.
    local_datastreams_df = local_datastreams_df[
        ['datastream_id', 'datastream_tablename', 'first_measurement_date',
         'last_measurement_date', 'total_measurement_numbers']
    ]
    # Do some cleanup from the call.
    local_datastreams_df.replace({pd.np.nan: None}, inplace=True)

    # Get all sampling feature timeseries datastreams from the remote database.
    remote_datastreams = api.call('odmx/get_datastream_info',
                                  {'return_db_metadata': True})
    # Convert the datastreams into a DataFrame.
    remote_datastreams_df = pd.DataFrame(remote_datastreams)
    # Do some cleanup on the DataFrame (remnants from the API call that we're
    # not using).
    remote_datastreams_df['first_measurement_date'] = pd.to_datetime(
        remote_datastreams_df['first_measurement_date'], unit='s'
    )
    remote_datastreams_df['last_measurement_date'] = pd.to_datetime(
        remote_datastreams_df['last_measurement_date'], unit='s'
    )
    # Cull the DataFrame to what we need.
    remote_datastreams_df = remote_datastreams_df[
        ['datastream_id', 'datastream_tablename', 'first_measurement_date',
         'last_measurement_date', 'total_measurement_numbers']
    ]
        # Loop through the local datastreams at hand.
    for row in local_datastreams_df.itertuples():
        datastream_tablename = row.datastream_tablename
        print(f"Examining {datastream_tablename}.")

        # Try to find the datastream in the remote table.
        remote_row = remote_datastreams_df.query(
            f'datastream_tablename == "{datastream_tablename}"'
        )
        if remote_row.empty:
            print("The remote row does not exist.")
            print("This case should never occur.")
        else:
            # Get the total measurement numbers.
            local_meas_nums = row.total_measurement_numbers
            remote_meas_nums = remote_row.total_measurement_numbers.iloc[0]

            # Get the last measurement date.
            local_last_meas_date = row.last_measurement_date
            remote_last_meas_date = (
                remote_row.last_measurement_date.iloc[0]
            )

            # If the local total measurements are less than the remote, it
            # might be an issue.
            if local_meas_nums < remote_meas_nums:
                # Is the local last measurement date within 24 hours of the
                # remote?
                time_diff = remote_last_meas_date - local_last_meas_date
                if time_diff > datetime.timedelta(days=1):
                    print("The local row is more than 24 hours behind the"
                          " remote row.")
                else:
                    print("The local row is behind the remote row, but"
                          " within 24 hours.")
            # If they're equal, it's probably not an issue.
            elif local_meas_nums == remote_meas_nums:
                time_diff = datetime.datetime.now() - local_last_meas_date
                if time_diff > datetime.timedelta(days=1):
                    print("The rows are up to date, but the data itself is"
                          " more than 24 hours old.")
                else:
                    print("The rows are up to date.")
            # If the local is somehow greater, that's a big issue.
            else:
                print("The remote row has fewer measurements than the"
                      " local row.")
                print("This case should never occur.")
        print("")


# Run the main function in the script.
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process data for a specified"
                                     " project and data source.")
    parser.add_argument('project_name', type=str, help="The name of the"
                        " project to work with. E.g., \"lbnlsfa\".")
    args = parser.parse_args()

    main(project_name=args.project_name)
