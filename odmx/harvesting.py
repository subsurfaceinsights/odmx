#!/usr/bin/env python3
"""
A script to house common harvesting functions used by data modules.
"""

import os
import odmx.support.rsync as ssisyn
import pandas as pd
from odmx.log import vprint

def simple_rsync(remote_user, remote_server, remote_base_path, local_base_path,
                 pull_list):
    """
    A simple wrapper function for repeated rsync code that appears in the
    harvesting functions of various data modules/classes.
    """

    # Create the sync context object.
    sync_context = ssisyn.RsyncContext(
        remote_user=remote_user,
        remote_server=remote_server,
        remote_base_path=remote_base_path,
        local_base_path=local_base_path
    )
    # Pull the data.
    sync_context.pull_list(pull_list)

class CsvHeaderError(Exception):
    """
    This exception is thrown when processing CSV files if headers are
    mismatched or unexpected
    """

def commit_csv(csv_path, new_data_df, old_data_df, ignore_missing=True,
        update_added=True, alert_on_added = True, to_csv_args=None):
    """
    Updates a CSV file according to the data in the given pandas df. If the
    columns differ, we have two options
    """
    if to_csv_args is None:
        to_csv_args = {}
    if old_data_df is not None:
        # Need the ordering of the original headers to make sure that they are
        # preserved in the new df
        headers_old_list = old_data_df.columns.to_list()
        headers_old = set(headers_old_list)
        headers_new = set(new_data_df.columns.to_list())
        missing = headers_old - headers_new
        added = headers_new - headers_old
        retained = headers_old & headers_new
        if len(retained) == 0:
            new_data_df.to_csv(f"{csv_path}.new", index=False, header=True)
            raise CsvHeaderError(f"CSV headers for '{csv_path}' have nothing "
                "in common with new data. This is probably an error or "
                "something has radically changed about the return. Requires "
                f"human investigation. New data is at '{csv_path}.new'")
        if len(missing) == 0 and len(added) == 0:
            # Make sure the headers are in the same order as the existing data
            new_data_df = new_data_df[headers_old_list]
            # We do a simple append
            vprint(f"Appending data to {csv_path}")
            new_data_df.to_csv(csv_path, index=False, mode='a', header=False,
                    **to_csv_args)
        elif len(missing) and not ignore_missing:
            new_data_df.to_csv(f"{csv_path}.new", index=False, header=True,
                    **to_csv_args)
            raise CsvHeaderError(f"New data for {csv_path} is missing these "
                f"expected headers: {missing}. It has these extra headers: "
                f"{added}. New data is at '{csv_path}.new'")
        elif len(added) and not update_added:
            new_data_df.to_csv(f"{csv_path}.new", index=False, header=True)
            raise CsvHeaderError(f"New data for {csv_path} has additional "
                f"headers that are unexpected: {added}. New data is at "
                f"'{csv_path}.new'")
        else:
            vprint("New data doesn't quite match old CSV, doing pd.concat to "
                    f"reshape the existing csv at '{csv_path}'\n"
                    f"\tRetained: {retained}\n"
                    f"\tMissing: {missing}\n"
                    f"\tAdded: {added}\n")
            if alert_on_added and len(added) > 0:
                print("TODO: Alert that additional headers have been added to "
                    f"'{csv_path}': '{added}'")
            data_df = pd.concat([old_data_df, new_data_df])
            data_df.to_csv(csv_path, index=False, header=True, **to_csv_args)
    else:
        assert not os.path.exists(csv_path)
        # in this case we just write the new data to file
        vprint(f"Creating new {csv_path}")
        new_data_df.to_csv(csv_path, index=False, header=True, **to_csv_args)
