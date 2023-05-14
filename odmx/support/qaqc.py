#!/usr/bin/env python3

"""
Subpackage to handle QA/QC checks.
"""

import datetime
import numpy as np


def set_flags_to_a(df):
    """
    Create a QA/QC column if it's not there and set the flags to `a`.

    @param df The DataFrame on which to perform the checks. It is expected that
              this DataFrame has a column named `data_value`.
    @return The modified DataFrame.
    """

    # Set everything to `a`.
    print("Setting all flags to \"a\".")
    df['qa_flag'] = 'a'
    print(f"Number of flags changed: {len(df)}")

    return df


def check_min_max(df, minimum, maximum):
    """
    Perform min/max checks, setting out-of-bounds values to `f`.

    @param df The DataFrame on which to perform the checks. It is expected that
              this DataFrame has a column named `data_value`.
    @param minimum The minimum value for the data.
    @param maximum The maximum value for the data.
    @return The modified DataFrame.
    """

    # Check the mix/max values.
    print(f"Setting flags for min values below {minimum} to \"f\".")
    mins = df['data_value'] < minimum
    df['qa_flag'] = np.where(mins, 'f', df['qa_flag'])
    print(f"Number of flags changed: {len(df[mins])}")
    print(f"Setting flags for max values above {maximum} to \"f\".")
    maxs = df['data_value'] > maximum
    df['qa_flag'] = np.where(maxs, 'f', df['qa_flag'])
    print(f"Number of flags changed: {len(df[maxs])}")

    return df


def check_infinity(df):
    """
    Perform a check for infinity values. We don't have a proper flag for them
    yet, so for now we set them to `b` (for generic bad).

    @param df The DataFrame on which to perform the checks. It is expected that
              this DataFrame has a column named `data_value`.
    @return The modified DataFrame.
    """

    # Check for infinity values.
    print("Setting flags for infinity values to \"b\".")
    infs = np.isinf(df['data_value'].astype(float))
    df['qa_flag'] = np.where(infs, 'b', df['qa_flag'])
    print(f"Number of flags changed: {len(df[infs])}")
    # We want to warn people when infinities pop up. Will add this in when
    # reporting is live, but for now we'll just print it out.
    infs_df = df.query('qa_flag == "b"')
    for row in infs_df.itertuples():
        human_time = datetime.datetime.utcfromtimestamp(row.utc_time)
        print(f"\n{row.data_value} FOUND AT {row.utc_time} ({human_time})\n")

    return df


def check_nans(df):
    """
    Perform a check for NaN values, and set them all to `c`.

    @param df The DataFrame on which to perform the checks. It is expected that
              this DataFrame has a column named `data_value`.
    @return The modified DataFrame.
    """

    # Find all NaNs and set to `c`.
    print("Setting flags for NaN values to \"c\".")
    nans = df['data_value'].apply(lambda x: isinstance(x, float)) & df['data_value'].isna()
    df['qa_flag'] = np.where(nans, 'c', df['qa_flag'])
    print(f"Number of flags changed: {len(df[nans])}")

    return df


def check_nulls(df):
    """
    Perform a check for Null values, and set them all to `d`.

    @param df The DataFrame on which to perform the checks. It is expected that
              this DataFrame has a column named `data_value`.
    @return The modified DataFrame.
    """

    # Find all None and set to `d`.
    print("Setting flags for NULL values to \"d\".")
    nones = df['data_value'].apply(lambda x: not isinstance(x, float)) & df['data_value'].isna()
    df['qa_flag'] = np.where(nones, 'd', df['qa_flag'])
    print(f"Number of flags changed: {len(df[nones])}")

    return df


def set_flags_to_z(df):
    """
    Set all `a` QA flags to `z`.

    @param df The DataFrame on which to perform the checks. It is expected that
              this DataFrame has a column named `data_value`.
    @return The modified DataFrame.
    """

    # Change all `a` flags to `z`.
    print("Setting \"a\" flags to \"z\".")
    zs = df['qa_flag'] == 'a'
    df['qa_flag'] = np.where(zs, 'z', df['qa_flag'])
    print(f"Number of flags changed: {len(df[zs])}")

    return df
