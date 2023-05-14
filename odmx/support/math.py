#!/usr/bin/env python3

"""
Subpackage to perform various math functions.
"""

import statistics
import datetime
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta


def phase_diff(signal1, signal2):
    """
    Takes two signals and returns the phase difference.

    @param signal1 The first signal.
    @param signal2 The second signal.
    @return The phase difference in radians, the phase difference in radians
            divided by pi (in pi notation), and the phase difference in degrees.
    """

    # Find the complex correlation coefficient.
    inner1_2 = np.inner(signal1, np.conj(signal2))
    inner1_1 = np.inner(signal1, np.conj(signal1))
    inner2_2 = np.inner(signal2, np.conj(signal2))
    ccc = inner1_2 / np.sqrt(inner1_1 * inner2_2)

    # Find the phase difference in radians.
    phase_rad = np.angle(ccc)
    phase_rad_pi = phase_rad / np.pi

    # Find the phase difference in radians.
    phase_deg = phase_rad * (180.0 / np.pi)

    return phase_rad, phase_rad_pi, phase_deg

# TODO How does this work when lon is unused?
# pylint: disable=unused-argument
def lat_lon_len(lat, lon):
    """
    Finds the length in kilometers of a degree of lat/lon.

    @param lat The latitude at a point.
    @param lon The longitude at a point.
    @return The length of a degree of latitude in km, and the length of a
            degree of longitude in km.
    """

    # Define the conversion from degrees to radians, as required by numpy
    # trig operations.
    deg_rad = (np.pi / 180.0)

    # Find the length of a degree of latitude in km.
    deg_lat_m = (111132.954
                 - 559.822 * np.cos(2.0 * lat * deg_rad)
                 + 1.175 * np.cos(4.0 * lat * deg_rad))
    deg_lat_km = deg_lat_m / 1000.0

    # Define some information about the Earth.
    eq_rad_km = 6378.137
    pol_rad_km = 6356.752
    # e = np.sqrt((eq_rad_km**2.0 - pol_rad_km**2.0) / eq_rad_km**2.0)
    # Find the length of a degree of longitude in km.
    # deg_lon_km = ((deg_lat_km * np.cos(lat * deg_rad))
    #               / np.sqrt(1.0 - (e**2.0 * np.sin(lat * deg_rad)**2.0)))

    # Find the length of a degree of longitude in km.
    phi = np.arctan((pol_rad_km / eq_rad_km) * np.tan(lat * deg_rad))
    deg_lon_km = deg_rad * eq_rad_km * np.cos(phi)

    return deg_lat_km, deg_lon_km


def lat_lon_distance(lat1, lon1, lat2, lon2):
    """
    Finds the distance in kilometers between two lat/lon points.
    Based on https://gist.github.com/rochacbruno/2883505

    @param lat1 The latitude of the first point.
    @param lon1 The longitude of the first point.
    @param lat2 The latitude of the second point.
    @param lon2 The longitude of the second point.
    @return The distance in km between the two points.
    """

    # Volumetric mean radius of the Earth
    # (https://nssdc.gsfc.nasa.gov/planetary/factsheet/earthfact.html)
    rad_earth_km = 6371

    # Convert to radians.
    lat1 = np.deg2rad(lat1)
    lon1 = np.deg2rad(lon1)
    lat2 = np.deg2rad(lat2)
    lon2 = np.deg2rad(lon2)
    # Find the differences.
    lat_diff = lat2 - lat1
    lon_diff = lon2 - lon1
    # Calculate the distance.
    dist = (np.sin(lat_diff / 2)**2
            + np.cos(lat1) * np.cos(lat2) * np.sin(lon_diff / 2)**2)
    dist = 2 * rad_earth_km * np.arcsin(np.sqrt(dist))

    return dist


def lat_lon_centroid(lat_lon_list):
    """
    Find the center of any number of lat/lon points.

    @param lat_lon_list A list of lat/lon points, with the expected format:
                        `[[12, -42], [12, 33], [65, -53]]`
    """

    # Convert the lat/lon list to radians.
    lat_lon_rads = [[i[0] * np.pi / 180.0, i[1] * np.pi / 180.0]
                    for i in lat_lon_list]

    # Define the Cartesian x, y, z coordinates of the lat/lon points.
    xs = [np.cos(i[0]) * np.cos(i[1]) for i in lat_lon_rads]
    ys = [np.cos(i[0]) * np.sin(i[1]) for i in lat_lon_rads]
    zs = [np.sin(i[0]) for i in lat_lon_rads]

    # Find the average x, y, and z coordinates.
    x = np.average(xs)
    y = np.average(ys)
    z = np.average(zs)

    # Convert the coordinates back to latitude and longitude.
    lon = np.arctan2(y, x)
    hyp = np.sqrt(x**2 + y**2)
    lat = np.arctan2(z, hyp)

    # Convert latitude and longitude back to degrees.
    lat = lat * 180.0 / np.pi
    lon = lon * 180.0 / np.pi

    return lat, lon


def is_not_number(x):
    """
    Test if something is not a number.

    @param x A number, maybe.
    @return True/False, depending on if the input is not a number.
    """

    try:
        float(x)
        return False
    except ValueError:
        return True


def mode(items):
    """
    Return the mode of whatever list is passed, or NaN is there is no mode.

    @param items A list, set, series of items where we want to find the mode.
    @return The mode of the items, or NaN if no mode is present.
    """

    try:
        return statistics.mode(items)
    # TODO What is this supposed to catch? We don't want to suppress all
    # possible errors with a nan
    #pylint: disable=bare-except
    except:
        return np.nan


def df_stats(df, key_column, data_column, frequency_count, frequency_unit,
             operation, kind=None, data_column_only=True):
    """
    Return the result of the statistical operation on a DataFrame.

    @param df The DataFrame on which we want to perform the operation.
    @param key_column The timestamp column on which we want to resample.
    @param data_column The column on which we want resampled data.
    @param frequency_count A number that determines how often the resampling
                           should be done.
    @param frequency_unit A string that determines what the resampling should
                          be, e.g., 'H' for hour.
    @param operation The operation to perform on the resampled data, e.g.,
                     `mean`.
    @param kind How the resampling is done. Default is `None`. In that case,
                the resampling period is automatically assumed to be the start
                of the day in which the first timestamp falls. If set to
                `period`, however, the resampling period is set to start with
                the first timestamp itself. (It also changes the format of the
                return slightly, too, though we negate that here.)
    @param data_column_only `True` if we only want statistics on the desired
                            data column. `False` if we want statistics on any
                            columns present.
    @return The new DataFrame.
    """

    # First define the resampling frequency from the frequency_count and
    # frequency_unit.
    frequency = f'{frequency_count}{frequency_unit}'

    # Define a mapping dictionary for whichever operation is passed, so that we
    # can apply the right function to it.
    operations_map = {
        'mean': pd.DataFrame.mean,
        'average': pd.DataFrame.mean,
        'median': pd.DataFrame.median,
        'mode': statistics.mode,
        'stdev': pd.DataFrame.std,
        'minimum': pd.DataFrame.min,
        'min': pd.DataFrame.min,
        'maximum': pd.DataFrame.max,
        'max': pd.DataFrame.max,
        'first': lambda x: x.iloc[0],
        'last': lambda x: x.iloc[-1],
    }
    # Also define a list of operations for which original timestamps should be
    # preserved. These are generally operations for which no data manipulation
    # would occur (i.e., the minimum and not the mean).
    non_manip_operations = [
        'minimum',
        'min',
        'maximum',
        'max',
        'first',
        'last',
    ]

    # When we resample a DataFrame, the resulting object is a series of smaller
    # DataFrames containing data for each resampled time chunk. If a time chunk
    # didn't exist in the original data, the resampling will create NaN data to
    # fill in the gaps; that's the point of resampling. However, for this use
    # case, that's false data that we don't want to return to the user. But we
    # can't simply drop NaNs after resampling and applying the statistical
    # operation, as the original data might also contain legitimate NaNs, and
    # we want the return to reflect that fact. The solution is to apply a
    # custom function to the resampled object, where it checks if each "mini"
    # internal DataFrame is empty (a sign that it was created by the
    # resampler). If it's not empty, we run the operation on it, which will
    # result in a real value or a legitimate NaN. If it's empty, we return an
    # Inf, which can later be safely dropped, thus preserving the integrity of
    # the original data.
    def apply_operation(subsample_df):
        """
        Takes an "internal" DataFrame from a resampled object and applies the
        proper operation from the `operations_map`, or returns an Inf if the
        DataFrame was created by the resampler.

        @param subsample_df The "internal" DataFrame from the resampler.
        @return The result of the desired operation, NaN, or Inf.
        """

        if not subsample_df.empty:
            try:
                return operations_map[operation](subsample_df)
            # TODO We really don't want to suppress every possible error with a
            # NaN.
            #pylint: disable=bare-except
            except:
                return np.nan
        else:
            return np.inf

    # Sort the DataFrame by the timestamp column to be safe.
    new_df = df.sort_values(by=key_column)

    # Depending on whether or not the applied operation manipulates the data
    # (e.g., means manipulate data, but mins do not), we need to treat the
    # DataFrame differently.
    if operation in non_manip_operations:
        # If the operation does not manipulate data, then we want to set the
        # DataFrame's index as the timestamp column, which will allow the
        # resampling and operations to be applied in an easier fashion.
        # However, we don't want to also drop the timestamp column. The
        # resampling process itself creates a new timestamp index, which makes
        # sense for manipulated data like the mean. But for non-manipulated
        # data like the minimum, we want to return the original timestamps
        # rather than generated ones. As such, we'll preserve and rename the
        # timestamp column for later use.
        new_df.set_index(key_column, drop=False, inplace=True)
        # Find a new name for the original timestamp column.
        iterator = 1
        while f'{key_column}_{iterator}' in new_df.columns:
            iterator += 1
        new_df.rename(columns={key_column: f'{key_column}_{iterator}'},
                      inplace=True)

        # Next, we need to figure out what columns should be acted on by the
        # resampling and operation. If we only want to return the data column,
        # then we keep that and the renamed timestamp column (it will be
        # deleted later, but we need it for now.)
        if data_column_only is True:
            cols_to_keep = [data_column, f'{key_column}_{iterator}']
        # If we want to return all columns, it's easy.
        else:
            cols_to_keep = new_df.columns.tolist()
    # If the applied operation doesn't manipulate data, then things are much
    # simpler.
    else:
        # We can just drop the timestamp column after setting it as the index.
        new_df.set_index(key_column, drop=True, inplace=True)
        # This is initially counterintuitive, but if the operation is
        # manipulative then for now we only keep the data column. This is
        # because a manipulative operation will turn all non-data into NaNs,
        # and will take significant processing time to do so. As such, we
        # simply won't process that data, and will fake columns of NaNs later.
        cols_to_keep = [data_column]

    # Figure out which columns should be dropped, if any.
    cols_to_drop = list(set(cols_to_keep) ^ set(new_df.columns.tolist()))
    new_df.drop(columns=cols_to_drop, inplace=True)

    # Resample the DataFrame.
    resampled = new_df.resample(frequency, kind=kind)
    # Apply the appropriate operation to it.
    new_df = resampled.apply(apply_operation)

    # To wrap things up, we again need a conditional about the type of
    # operation.
    if operation in non_manip_operations:
        # If we're dealing with a non-manipulating operation, we now want to
        # reset the index while dropping it, and use the original timestamp
        # column as the new timestamp column.
        new_df.reset_index(drop=True, inplace=True)
        new_df.rename(columns={f'{key_column}_{iterator}': key_column},
                      inplace=True)
        col = new_df.pop(key_column)
        new_df.insert(0, key_column, col)
    else:
        # With manipulative data we still want to reset the index, but we want
        # to preserve it as a new timestamp column.
        new_df.reset_index(drop=False, inplace=True)

        # We also need to account for our "cheat" in how we dealt with columns
        # other than the data column to speed up the processing.
        if data_column_only is False:
            for col in cols_to_drop:
                new_df[col] = np.nan

    # Create a new DataFrame by merging together the results where the numbers
    # are real, and the results where the numbers are only NaNs. This excludes
    # any Infs that we artificially introduced, which were there as
    # placeholders for any fake data that the resampling inserted.
    new_df = new_df[np.isfinite(new_df[data_column])].merge(new_df[new_df[data_column].isna()],
                                                            how='outer')
    # Sort the new DataFrame by the timestamp to reorder everything.
    new_df.sort_values(by=key_column, inplace=True)
    # Reset the index one more time, just in case.
    new_df.reset_index(drop=True, inplace=True)

    # At this point, if the operation is meant to manipulate the data, we want
    # to find new timestamps for each bin that effectively fall in the middle
    # of each bin.
    if operation not in non_manip_operations:
        # Define a list in which to hold the new timestamps.
        new_timestamps = []
        # Iterate through the resampled bins.
        for i in resampled:
            # The timestamp is what pandas assigns as the bin's new overall
            # timestamp.
            timestamp = i[0]
            # As long as the resampled bin has data, we proceed.
            if not i[1].empty:
                # If the resampling was hours, we add half of the resampling
                # frequency's value (in hours) to the timestamp.
                if frequency_unit == 'H':
                    timestamp = timestamp + datetime.timedelta(hours=frequency_count / 2)
                # If in days, we add half of the resampling frequency's value
                # (in days).
                elif frequency_unit == 'D':
                    timestamp = timestamp + datetime.timedelta(days=frequency_count / 2)
                # If in weeks, we subtract half of the resampling frequency's
                # value (in weeks).
                elif frequency_unit == 'W':
                    timestamp = timestamp - datetime.timedelta(weeks=frequency_count / 2)
                    # We also add a day, since this will yield Wednesdays at
                    # noon or Sundays at midnight, and we want Thursdays at
                    # noon and Mondays at midnight.
                    timestamp = timestamp + datetime.timedelta(days=1)
                # If in months we need to switch to relative time deltas, as
                # standard ones can only go up to weeks. But, relative deltas,
                # while being able to handle months and years, cannot handle
                # fractions of those units. So, we need to figure out the
                # number of days present in each bin, find that mid-point, and
                # add it to the bin's starting timestamp.
                elif frequency_unit == 'MS':
                    start = timestamp
                    end = timestamp + relativedelta(months=+frequency_count)
                    delta_days = (end - start).days
                    timestamp = timestamp + datetime.timedelta(days=delta_days / 2)
                # Same idea for years as above for months.
                elif frequency_unit in ['AS', 'AS-OCT']:
                    start = timestamp
                    end = timestamp + relativedelta(years=+frequency_count)
                    delta_days = (end - start).days
                    timestamp = timestamp + datetime.timedelta(days=delta_days / 2)
                # Now that we have the new timestamp, we add it to the list.
                new_timestamps.append(timestamp)
        # With the complete list in hand, we replace the timestamp column in
        # the new DataFrame.
        new_df[key_column] = new_timestamps

    return new_df
