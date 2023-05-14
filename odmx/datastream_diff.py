#!/usr/bin/env python3

"""
Checks the differences between datastreams.

Currently prints out to terminal, but can be piped into a file from the command
line to better review the output (which can be lengthy and can also take up
space. A recent run-through produced a 1.5 GB text file.
"""

import argparse
from pprint import pprint
import pandas as pd
from deepdiff import DeepDiff
import odmx.support.api_client as ssicli


def main(url_old, url_new, token, deep_diff):
    """
    The main function.
    """

    # Define the API clients for the old and new databases.
    api_old = ssicli.ApiClient(url=url_old, token=token)
    api_new = ssicli.ApiClient(url=url_new, token=token)

    # Get the `sampling_feature_timeseries_datastreams` tables.
    print("Finding old datastreams. This may take a moment.")
    ds_old = api_old.call('odmx/v2/get_datastream_info',
                          {'return_db_metadata': True})
    ds_df_old = pd.DataFrame(ds_old)
    # We need the `return_db_metadata` option to get the datastream's name
    # returned, but we don't need the extra info that comes with it.
    ds_df_old.drop(columns=['first_useful_measurement_date',
                            'first_useful_measurement_value', 'attribute'],
                   inplace=True)
    print("Finding new datastreams. This may take a moment.")
    ds_new = api_new.call('odmx/v2/get_datastream_info',
                          {'return_db_metadata': True})
    ds_df_new = pd.DataFrame(ds_new)
    ds_df_new.drop(columns=['first_useful_measurement_date',
                            'first_useful_measurement_value', 'attribute'],
                   inplace=True)

    # Create DataFrames of the units and variables tables for later lookup.
    print("Finding old units.")
    units_old = api_old.call('odmx/v2/get_units_info')
    units_df_old = pd.DataFrame(units_old)
    print("Finding new units.")
    units_new = api_new.call('odmx/v2/get_units_info')
    units_df_new = pd.DataFrame(units_new)
    print("Finding old variables.")
    variables_old = api_old.call('odmx/v2/get_variable_info')
    variables_df_old = pd.DataFrame(variables_old)
    print("Finding new variables.")
    variables_new = api_new.call('odmx/v2/get_variable_info')
    variables_df_new = pd.DataFrame(variables_new)

    # Remove any Landsat 8 datastreams from the "old" grouping, as we don't
    # want those anymore.
    ds_df_old = ds_df_old[
        ~ds_df_old['datastream_tablename'].str.startswith('ls8')
    ]

    # At this time, the datastream's name is its unique identifier. However,
    # some of the datastreams had their names changed in the revamp (due to us
    # now allowing whatever column names are present in the base data files
    # instead of semi-manually changing them). For the ones whose names
    # changed, it was always a small fragment of a change, generally having to
    # do with parens now being allowed. Here we define a mapping dictionary for
    # those changes, so that the correct datastreams are always compared.
    name_map_dict = {
        'wvc_1': 'wvc(1)',
        'wvc_2': 'wvc(2)',
        'wvc_3': 'wvc(3)',
        'wvc_4': 'wvc(4)',
        'avg_1': 'avg(1)',
        'avg_2': 'avg(2)',
        'avg_3': 'avg(3)',
        'avg_4': 'avg(4)',
        'avg_5': 'avg(5)',
        'avg_6': 'avg(6)',
        'avg_7': 'avg(7)',
        'avg_8': 'avg(8)',
        'avg_9': 'avg(9)',
        'pluv2_4': 'pluv2(4)',
        'pumphouse_streamflow_corrrected_level_m':\
            'pumphouse_streamflow_corrected level [m]',
        'pumphouse_streamflow_q_cms': 'pumphouse_streamflow_q [cms]'
    }

    print("Checking datastreams.\n")
    count = 0
    missing_list = []
    metadata_list = []
    deepdiff_list = []
    # Loop through the old datastreams.
    for row_old in ds_df_old.itertuples():
        count += 1
        ds_name_old = row_old.datastream_tablename
        if ds_name_old != 'geochemdata_ph_isco_deltao18_no_depth':
            continue

        # Check to see if any of the datastream names are among those that
        # changed. If so, define the new name for later use.
        if any(i in ds_name_old for i in list(name_map_dict.keys())):
            for key, value in name_map_dict.items():
                if key in ds_name_old:
                    ds_name_new = ds_name_old.replace(key, value)
                    break
        # If not, set the new name variable to the old name for simple
        # comparisons.
        else:
            ds_name_new = ds_name_old
        # Find the corresponding row in the new DataFrame of datastreams.
        row_new = ds_df_new.query(
            f'datastream_tablename == "{ds_name_new}"'
        ).copy()

        # If the new row is empty, it means that the old datastream has no
        # counterpart in the new database. This shouldn't happen!
        if row_new.empty:
            print(f"{ds_name_old} not present in new database.\n")
            missing_list.append(ds_name_old)
            continue

        # Now that we know both datastreams exist, we want to compare their
        # metadata. Create two DataFrames based on the current rows, for easy
        # comparison.
        row_df_old = pd.Series(dict(row_old._asdict())).to_frame().T
        ds_id_old = row_df_old['datastream_id'].item()
        row_df_old.drop(columns=['Index', 'datastream_id',
                                 'datastream_attribute'],
                        inplace=True)
        row_df_old.reset_index(drop=True, inplace=True)
        units_id_old = row_df_old['units_id'].item()
        units_old = units_df_old.query(f'units_id == {units_id_old}')
        units_term_old = units_old['term'].item()
        variable_id_old = row_df_old['variable_id'].item()
        variables_old = variables_df_old.query(
            f'variable_id == {variable_id_old}'
        )
        variable_term_old = variables_old['variable_term'].item()
        row_df_old.drop(columns=['units_id', 'variable_id'], inplace=True)
        row_df_old['units_term'] = units_term_old
        row_df_old['variable_term'] = variable_term_old

        row_df_new = pd.DataFrame(row_new)
        ds_id_new = row_df_new['datastream_id'].item()
        row_df_new.drop(columns=['datastream_id', 'datastream_uuid',
                                 'equipment_id', 'datastream_attribute'],
                        inplace=True)
        # In the new row's DataFrame, replace the new table name with the old
        # one if it's changed, to make the comparison simpler.
        if ds_name_new != ds_name_old:
            row_df_new['datastream_tablename'] = ds_name_old
        row_df_new.reset_index(drop=True, inplace=True)
        units_id_new = row_df_new['units_id'].item()
        units_new = units_df_new.query(f'units_id == {units_id_new}')
        units_term_new = units_new['term'].item()
        variable_id_new = row_df_new['variable_id'].item()
        variables_new = variables_df_new.query(
            f'variable_id == {variable_id_new}'
        )
        variable_term_new = variables_new['variable_term'].item()
        row_df_new.drop(columns=['units_id', 'variable_id'], inplace=True)
        row_df_new['units_term'] = units_term_new
        row_df_new['variable_term'] = variable_term_new

        # Find the difference between the two.
        meta_diff = DeepDiff(row_df_old.to_dict(), row_df_new.to_dict())

        # Now check the actual data in the two datastreams.
        results_old = api_old.call('odmx/v2/get_timeseries_data',
                                   {'datastream_id': ds_id_old,
                                    'begin_date': 1,
                                    'end_date': 4102444800,  # year 2100
                                    'qa_flag': 'a',
                                    'return_qa_flag': True})
        results_df_old = pd.DataFrame(results_old[0]['data_values'])
        results_new = api_new.call('odmx/v2/get_timeseries_data',
                                   {'datastream_id': ds_id_new,
                                    'begin_date': 1,
                                    'end_date': 4102444800,  # year 2100
                                    'qa_flag': 'a',
                                    'return_qa_flag': True})
        results_df_new = pd.DataFrame(results_new[0]['data_values'])
        data_diff_simple = results_df_old.equals(results_df_new)

        # Print the results.
        if meta_diff or not data_diff_simple:
            print(f"Issues detected with {ds_name_old}.")
            if meta_diff:
                metadata_list.append(ds_name_old)
                print(f"Difference found in metadata for {ds_name_old}.")
                pprint(meta_diff)
            if not data_diff_simple:
                deepdiff_list.append(ds_name_old)
                if not deep_diff:
                    print("DeepDiff needed.")
                else:
                    print("Performing DeepDiff.")
                    # Drop rows with NaNs, since DeepDiff doesn't treat them
                    # correctly.
                    deep_df_old = results_df_old.copy().dropna()
                    deep_df_old.reset_index(drop=True, inplace=True)
                    deep_df_new = results_df_new.copy().dropna()
                    deep_df_new.reset_index(drop=True, inplace=True)
                    data_diff_deep = DeepDiff(deep_df_old.dropna().to_dict(),
                                              deep_df_new.dropna().to_dict())
                    if data_diff_deep:
                        pprint(data_diff_deep)
            print("")
    print("Datastream checks complete.")
    print(f"Datastreams examined: {count}")
    print(f"Datastreams missing from new database: {len(missing_list)}")
    if missing_list:
        pprint(missing_list)
    print(f"Datastreams with different metadata: {len(metadata_list)}")
    if metadata_list:
        pprint(metadata_list)
    print(f"Datastreams needing a DeepDiff: {len(deepdiff_list)}")
    if deepdiff_list:
        pprint(deepdiff_list)


# Run the main function in the script.
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Checks the differences"
                                     " between datastreams in two ODMX"
                                     " databases.", epilog="Example usage: "
                                     " python3 odmxstreams_diff.py"
                                     " url_old url_new token --deep_diff")
    parser.add_argument('url_old', type=str, help="The URL of the server with"
                        "the old database to check.")
    parser.add_argument('url_new', type=str, help="The URL of the server with"
                        "the new database to check.")
    parser.add_argument('token', type=str, help="The token for both URLs.")
    parser.add_argument('--deep-diff', action='store_true', help="Set this"
                        " argument to perform DeepDiffs of datastreams. If"
                        " omitted, DeepDiffs will not be performed when two"
                        " datastreams are not exactly equal. This was done to"
                        " save time, as the DeepDiff can take a little while.")
    args = parser.parse_args()

    main(url_old=args.url_old, url_new=args.url_new, token=args.token,
         deep_diff=args.deep_diff)
