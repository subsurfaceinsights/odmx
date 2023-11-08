"""
This module is used to sync the local database with the remote database using
the REST API.
"""

from odmx.support.config import Config
import odmx.support.db as db
import odmx.rest_client as rest_client
import odmx.data_model as odmx
from beartype.typing import Generator
import io as IO
import pandas as pd
import argparse

from multiprocessing.pool import ThreadPool


def main(config):
    api = rest_client.ApiClient(config.remote_url)
    if config.project_name_header is not None:
        print("Setting project name header to ", config.project_name)
        api.add_request_header(config.project_name_header, config.project_name)
    correlation_mode = config.correlation_mode
    if correlation_mode == 'uuid':
        correlation_field = 'datastream_uuid'
    elif correlation_mode == 'tablename':
        correlation_field = 'datastream_tablename'
    else:
        raise Exception("Unknown correlation mode: " + correlation_mode)
    db_name = 'odmx_' + config.project_name
    con = db.connect(config, db_schema='odmx', db_name=db_name)
    datastream_con = db.connect(config, db_schema='datastreams', db_name=db_name)
    print("Getting local datastreams")
    local_datastreams = odmx.read_sampling_feature_timeseries_datastreams_all(con)
    print("Getting remote datastreams")
    remote_datastreams: Generator[odmx.SamplingFeatureTimeseriesDatastreams, None, None] = \
            api.get(odmx.SamplingFeatureTimeseriesDatastreams) # pyright: ignore[reportGeneralTypeIssues]
    local_datastreams_by_field = {getattr(datastream, correlation_field): datastream for datastream in local_datastreams}
    updated = 0
    for remote_datastream in remote_datastreams:
        if getattr(remote_datastream, correlation_field) in local_datastreams_by_field:
            datastream = local_datastreams_by_field[getattr(remote_datastream, correlation_field)]
            if remote_datastream.total_measurement_numbers != datastream.total_measurement_numbers:
                print("Updating datastream", datastream.datastream_tablename)
                local_count = datastream.total_measurement_numbers
                remote_count = remote_datastream.total_measurement_numbers
                if remote_count is None:
                    remote_count = 0
                if local_count is None:
                    local_count = 0
                if local_count > remote_count:
                    print("WARNING: local datastream has more measurements than remote datastream!")
                    print(f"local_count = {local_count}, remote_count = {remote_count}")
                    continue
                start = datastream.last_measurement_date
                end = remote_datastream.last_measurement_date
                if start is None:
                    start = remote_datastream.first_measurement_date
                if end is None:
                    print("WARNING: remote datastream has no measurements!")
                    continue
                assert remote_datastream.datastream_id is not None
                new_data = api.datastream(remote_datastream.datastream_id,
                                          start_datetime=start,
                                          end_datetime=end,
                                          full_precision=True,
                                          qa_flag = 'a',
                                          qa_flag_mode = 'greater_or_eq',
                                          downsample_interval = None,
                                          downsample_method = None,
                                          format='csv',
                                          tz='UTC',
                                          open_interval='start')
                df = pd.read_csv(IO.StringIO(new_data))
                df = df.rename(columns={'datetime_local': 'utc_time'})
                # Convert utc_time to unix time
                df['utc_time'] = pd.to_datetime(df['utc_time']).astype(int) // 10**9
                db.insert_many_df(datastream_con, datastream.datastream_tablename, df)
                # Update the datastream
                odmx.update_sampling_feature_timeseries_datastreams(
                        con,
                        datastream.datastream_id,
                        last_measurement_date=end,
                        total_measurement_numbers=remote_count)
                con.commit()
                updated += 1
        else:
            print("Warning: remote datastream not found locally:", remote_datastream)
    print("Updated ", updated, " datastreams")

if __name__ == '__main__':
    config = Config()
    db.add_db_parameters_to_config(config)
    argparser = argparse.ArgumentParser()
    config.add_config_param('remote_url',
                            help='The URL of the remote server to connect to',
                            default='http://localhost:8000/api/odmx/v3')
    config.add_config_param('project_name',
                            help='The name of the project to sync')
    config.add_config_param('project_name_header',
                            help='The name of the header to use for the project name'
                                 ' when making requests to the remote server, if '
                                 'the REST API is configured for it',
                            optional=True)
    config.add_config_param('correlation_mode',
                            help='The correlation mode to use, "uuid" or "tablename"',
                            default='tablename')
    config.add_args_to_argparser(argparser)
    args = argparser.parse_args()
    config.validate_config(args)
    main(config)


