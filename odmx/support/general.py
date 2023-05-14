#!/usr/bin/env python3

"""
Subpackage to handle general code functions.
"""

import sys
import os
import platform
import socket
import json
import jsonschema
import filelock
import requests
import pandas as pd
import numpy as np


def in_ipython():
    """
    Determine if the code is running in an IPython environment.

    @return A boolean for whether we're in IPython or not.
    """

    try:
        get_ipython()
        return True
    except NameError:
        return False


def module_info(module_list):
    """
    Print out the version information for imported modules.

    @param module_list A list of imported modules.
    """

    print("System information:")
    print(f" - Operating System: {platform.system()}")
    print(f" - Network Name: {platform.node()}")
    print(f" - Release Number: {platform.release()}")
    print(f" - Release Version: {platform.version()}")
    print(f" - Processor Type: {platform.machine()}")
    print(f" - Processor: {platform.processor()}")
    print("Module information:")
    # Cycles through the module list and finds the version info if it's there.
    for i in module_list:
        if hasattr(i, '__version__'):
            print(f" - {i.__name__} version: {i.__version__}")
        elif hasattr(i, 'version'):
            print(f" - {i.__name__} version: {i.version}")
        elif hasattr(i, 'TkVersion'):
            print(f" - {i.__name__} version: {i.TkVersion}")
        else:
            print(f" - {i.__name__} has no version attribute.")


def get_files(path_to_check, file_ext_list=None, prefix=None):
    """
    Get and print a list of files in a directory.

    @param path_to_check The path to check for files.
    @param file_ext_list An optional list of file extensions for file types to
                         search for.
    @param prefix An optional string to look for as a file prefix.
    @return A list of file names and a list of file paths.
    """

    # Make sure that file_ext_list is a list object. Convert it if not.
    if file_ext_list is None:
        file_ext_list = []
    if not isinstance(file_ext_list, list):
        file_ext_list = [file_ext_list]

    # Make sure that the file extensions start with a ".".
    file_ext_list = [file_ext if file_ext.startswith('.')
                     else f'.{file_ext}' for file_ext in file_ext_list]

    # Make sure that the list accounts for upper and lower case extensions.
    uppers = [ext.upper() for ext in file_ext_list]
    lowers = [ext.lower() for ext in file_ext_list]
    file_ext_list = file_ext_list + uppers + lowers
    file_ext_list = list(set(file_ext_list))

    # Look through the path of interest and log the files of interest.
    if file_ext_list:
        files_list = sorted([file for file in os.listdir(path_to_check)
                             if os.path.isfile(os.path.join(path_to_check,
                                                            file))
                             and os.path.splitext(file)[1] in file_ext_list])
    else:
        files_list = sorted([file for file in os.listdir(path_to_check)
                             if os.path.isfile(os.path.join(path_to_check,
                                                            file))])

    # Check to see if a prefix was provided.
    if prefix is not None:
        files_list = [file for file in files_list if file.startswith(prefix)]

    # Set the paths as well.
    paths_list = [os.path.join(path_to_check, file) for file in files_list]

    # Return the files list and the paths list.
    return files_list, paths_list


def get_dirs(path_to_check):
    """
    # Function to get and print a list of directories.

    @param path_to_check The path to check for directories.
    @return A list of directory names and a list of directory paths.
    """

    # Look through the path of interest and log the directories.
    dirs_list = sorted([item for item in os.listdir(path_to_check)
                        if os.path.isdir(os.path.join(path_to_check, item))])
    paths_list = [os.path.join(path_to_check, item) for item in dirs_list]

    # Return the files list and the paths list.
    return dirs_list, paths_list


def list_items(items):
    """
    Print a list/dictionary of items for a user to select.

    @param items Some combination of items being considered (list, dictionary).
    @TODO Add in DataFrame functionality.
    """

    # Print out what was found.
    if len(items) == 0:
        print(f"Found {len(items)} items. Quitting.")
        sys.exit(1)
    elif len(items) == 1:
        print(f"Found {len(items)} item:")
    else:
        print(f"Found {len(items)} items:")

    # If the bunch of items is a list.
    if isinstance(items, list):
        for i, item in enumerate(items, 1):
            print(f"{i}. {item}.")
    # If the bunch of items is a dictionary.
    elif isinstance(items, dict):
        # Saves the key, value pair to use.
        for i, key in enumerate(sorted(items.keys()), 1):
            print(f"{i}. {key}")
    elif isinstance(items, pd.DataFrame):
        pass


def choose_item(items):
    """
    Ask the user what [x] they'd like to choose from a list.

    @param items Some combination of items being considered (list, dictionary).
    @return The chosen value and item.
    """

    # Asks the user to choose an item and checks against bad inputs.
    while True:
        try:
            user_input = int(input("\nWhich option do you want to use?"
                                   " (Enter the number.)\n"))
        except ValueError:
            print("Please enter a valid number.")
            continue
        if user_input not in range(1, len(items) + 1):
            print("You must choose a valid option from those listed above.")
            continue
        # User input is valid; exit the loop.
        break

    # Saves the item to use.
    # If the bunch of items is a list.
    if isinstance(items, list):
        for i, item in enumerate(items, 1):
            if user_input == i:
                item_to_use = item
                break
        print(f"\nUsing {os.path.basename(item_to_use)}.")
        return user_input, item_to_use
    # If the bunch of items is a dictionary.
    if isinstance(items, dict):
        # Saves the key, value pair to use.
        for i, keyvalue in enumerate(sorted(items.items()), 1):
            if user_input == i:
                key = keyvalue[0]
                value = keyvalue[1]
                break
        print(f"\nUsing {key}.")
        return user_input, key, value
    if isinstance(items, pd.DataFrame):
        # Saves two column entires to use.
        row = items.iloc[user_input]
        val1 = row[0]
        val2 = row[1]
        print(f"\nUsing {val1}, {val2}.")
        return user_input, val1, val2
    raise Exception("Not sure what kind of object the list is.")


# def group_df_date_or_time(df, group_col, date_or_time):
#     """
#     Group a DataFrame's columns by count based on a timestamp.

#     @param df A DataFrame.
#     @param group_col The column in the DataFrame to group by (meant to be a
#                      datetime column).
#     @param date_or_time A variable that represents specific date or time
#                         grouping.
#     @return The grouped DataFrame.
#     """

#     if date_or_time == 'date':
#         df = df.groupby(df[group_col].dt.date).count()
#     elif date_or_time == 'time':
#         df = df.groupby(df[group_col].dt.time).count()
#     # Delete extra columns.
#     for i in range(1, len(df.columns)):
#         del df[df.columns[1]]
#     # Reset the index of the DataFrame from 1 rather than 0.
#     df.reset_index(inplace=True)
#     df.index += 1
#     # Rename the DataFrame's columns.
#     df = df.rename(index=str, columns={df.columns[0]: 'time',
#                                        df.columns[1]: 'count'})

#     return df


def rectangle(top, bottom, left, right, width, height):
    """
    Get a list of (x, y) points inside of a rectangle.

    @param top The top of the rectangle.
    @param bottom The bottom of the rectangle.
    @param left The left of the rectangle.
    @param right The right of the rectangle.
    @param width The width of the picture.
    @param height The height of the picture.
    @return A list of all (x, y) points in the rectangle.
    """

    if 0 <= left <= right <= width and 0 <= top <= bottom <= height:
        return [(x, y) for x in range(left, right + 1)
                for y in range(top, bottom + 1)]

    errors = ""
    if top > bottom:
        errors += ("- The top dimension must be less than the bottom"
                   " dimension.\n")
    if top < 0:
        errors += "- The top dimension must be greater than zero.\n"
    if bottom < 0:
        errors += "- The bottom dimension must be greater than zero.\n"
    if top > height:
        errors += "- The top dimension must be less than the image's height.\n"
    if bottom > height:
        errors += ("- The bottom dimension must be less than the image's"
                   " height.\n")
    if left > right:
        errors += ("- The left dimension must be less than the right"
                   " dimension.\n")
    if left < 0:
        errors += "- The left dimension must be greater than zero.\n"
    if right < 0:
        errors += "- The right dimension must be greater than zero.\n"
    if left > width:
        errors += "- The left dimension must be less than the image's width.\n"
    if right > width:
        errors += ("- The right dimension must be less than the image's"
                   " width.\n")
    errors += "Please try again."
    print(errors)
    return None


#TODO Unclear why bottomright is unused
#pylint: disable=unused-argument
def rectangle2(topleft, topright, bottomleft, bottomright):
    """
    Get a list of (x, y) points inside of a rectangle.

    @param topleft The top left coordinate of the rectangle.
    @param topright The bottom left coordinate of the rectangle.
    @param bottomleft The top right coordinate of the rectangle.
    @param bottomright The bottom right coordinate of the rectangle.
    @return A list of all (x, y) points in the rectangle.
    """

    return [(x, y) for x in range(topleft[0], topright[0] + 1)
            for y in range(topleft[1], bottomleft[1] + 1)]

#TODO Unclear why topright and bottomleft is unused
#pylint: disable=unused-argument
def rectangle3(topleft, topright, bottomleft, bottomright):
    """
    Yet another function to get a list of (x, y) points inside of a rectangle.

    @param topleft The top left coordinate of the rectangle.
    @param topright The bottom left coordinate of the rectangle.
    @param bottomleft The top right coordinate of the rectangle.
    @param bottomright The bottom right coordinate of the rectangle.
    @return The top-most column to start on, the bottom-most column to end on,
            the left-most row to start on, and the right-most row to end on.
    """

    top = topleft[1]
    bottom = bottomright[1]
    left = topleft[0]
    right = bottomright[0]

    return top, bottom, left, right


def circle(center, radius, width, height):
    """
    Get a list of (x, y) points inside of a circle.

    @param center The center point of the circle.
    @param radius The radius of the circle.
    @param width The width of the picture.
    @param height The height of the picture.
    @return A list of all (x, y) points in the circle.
    """

    radius = abs(radius)
    if (center[0] - radius >= 0
        and center[0] + radius <= width
        and center[1] - radius >= 0
        and center[1] + radius <= height):
        xy_list = []
        for x in range(center[0] - radius, center[0] + radius):
            yspan = radius * np.sin(np.arccos((center[0] - x) / radius))
            for y in range(center[1] - int(yspan), center[1] + int(yspan)):
                xy_list.append((x, y))
        # Plotting it, but this is more of a test.
        # ax.plot((np.array(points).T)[0], (np.array(points).T)[1], marker='.',
        #         ms=2, ls='None')

        return xy_list

    errors = ""
    if center[0] - radius < 0:
        errors += ("- The circle's radius cannot exceed the left-hand side of"
                   " the image.\n")
    if center[0] + radius > width:
        errors += ("- The circle's radius cannot exceed the right-hand side of"
                   " the image.\n")
    if center[1] - radius < 0:
        errors += "- The circle's radius cannot exceed the top of the image.\n"
    if center[1] + radius > height:
        errors += ("- The circle's radius cannot exceed the bottom of the"
                   " image.\n")
    errors += "Please try again."
    print(errors)
    return None


def colnum_string(n):
    """
    Converts a number into an Excel-appropriate column header.
    Credit to: https://stackoverflow.com/a/23862195/623534

    @param n The number to be converted to an Excel-style column header.
    @return The Excel-style column header.
    """

    # Define the return string.
    col_head = ''
    # Loop through and figure out the remainder until n == 0.
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        # Use the remainder to get the column header, and append it.
        col_head = f'{chr(65 + remainder)}{col_head}'

    return col_head


def get_response(url, params=None, headers=None):
    """
    Get an HTTP response with the requests package.

    @param url The base URL to query for a response.
    @param params A dictionary of parameters to pass as URL arguments.
    @param headers A dictionary of headers to pass to the server.
    @return The response from the server.
    """

    try:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code != 200:
            print(response)
    except (socket.gaierror, requests.exceptions.ConnectionError) as e:
        print(f"Error: {e}")
        sys.exit(1)

    return response


def open_json(file_path, args=None, lock=False, timeout=300,
              validation_path=None):
    """
    Wrapper routine to open a .json file with commonly used options.

    @param file_path The full path of the .json file to open.
    @param args A dictionary of arguments for the json.load function.
    @param lock If the file should be locked while opening it.
    @param timeout The number of seconds after which filelock should timeout.
    @param validation_path The full path of the json schema validation file.
    @return The Python object from the .json file.
    """

    # Define the args parameter explicitly if nothing was passed.
    if args is None:
        args = {}

    # Define the actual opening routine.
    def open_the_file(file_path=file_path):
        try:
            with open(file_path) as json_file:
                return json.load(json_file, **args)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"No file found at: {file_path}.") from e
        except json.decoder.JSONDecodeError as e:
            raise Exception(f"The .json file {file_path} could not be"
                            " decoded properly.") from e

    # Define the args parameter explicitly if nothing was passed.
    if args is None:
        args = {}

    # Open the file based on whether it should be locked or not.
    if lock:
        try:
            with filelock.FileLock(f'{file_path}.lock', timeout=timeout):
                json_data = open_the_file()
        except filelock.Timeout as e:
            raise filelock.Timeout(
                f"Another script holds the lock on {file_path}."
            ) from e
    else:
        json_data = open_the_file()

    # Now open the validation schema if needed.
    if validation_path is not None:
        validation_schema = open_the_file(validation_path)
        jsonschema.validate(json_data, validation_schema)

    return json_data

def write_json(
        file_path,
        data,
        merge=False,
        args=None,
        lock=False,
        timeout=300):
    """
    @param file_path The full path of the .json file to write
    @param data The data to write to the JSON file
    @param merge Whether to merge with the existing JSON file or overwrite
    @param args A dictionary of arguments for the json.write function
    @param lock If the file exists, whether it should be locked before writing
    @param timeout The number of seconds to timeout for filelock
    """
    def write_the_file(data):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w+") as json_file:
            if merge:
                old_data = json.load(json_file)
                old_data.update(data)
                data = old_data
            json.dump(data, json_file)
    if lock:
        try:
            with filelock.FileLock(f'{file_path}.lock', timeout=timeout):
                write_the_file(data)
        except filelock.Timeout as e:
            raise filelock.Timeout(
                f"Another script holds the lock on {file_path}."
            ) from e
    else:
        write_the_file(data)

def open_csv(file_path, args=None, lock=False, timeout=300):
    """
    Wrapper routine to open a .csv file with commonly used options.

    @param file_path The full path of the .csv file to open.
    @param args A dictionary of arguments for the Pandas read_csv function.
    @param lock If the file should be locked while opening it.
    @param timeout The number of seconds after which filelock should timeout.
    @return The pandas object from the .csv file.
    """

    # Define the actual opening routine.
    def open_the_file(passed_path, args_to_apply):
        """
        @param passed_path The file path to open.
        @param args_to_apply A dictionary of arguments to use for read_csv.
                             Could be empty.
        """

        try:
            return pd.read_csv(passed_path, **args_to_apply)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"No file found at: {passed_path}.") from e

    # Define the args parameter explicitly if nothing was passed.
    if args is None:
        args = {}

    # Open the file based on whether it should be locked or not.
    if lock:
        try:
            with filelock.FileLock(f'{file_path}.lock', timeout=timeout):
                csv_data = open_the_file(file_path, args)
        except filelock.Timeout as e:
            raise filelock.Timeout(
                f"Another script holds the lock on {file_path}."
            ) from e
        except pd.errors.EmptyDataError as e:
            raise pd.errors.EmptyDataError(
                f"{file_path} exists but contains no data or headers."
            ) from e
    else:
        csv_data = open_the_file(file_path, args)

    return csv_data


def open_spreadsheet(file_path, args=None, lock=False, timeout=300):
    """
    Wrapper routine to open a spreadsheet file with commonly used options.

    @param file_path The full path of the .csv file to open.
    @param args A dictionary of arguments for the Pandas read_csv function.
    @param lock If the file should be locked while opening it.
    @param timeout The number of seconds after which filelock should timeout.
    @return The Python object from the .csv file.
    """

    # Define the actual opening routine.
    def open_the_file(passed_path, args_to_apply):
        """
        @param passed_path The file path to open.
        @param args_to_apply A dictionary of arguments to use for read_csv.
                             Could be empty.
        """

        try:
            return pd.read_excel(passed_path, **args_to_apply)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"No file found at: {passed_path}.") from e

    # Define the args parameter explicitly if nothing was passed.
    if args is None:
        args = {}

    # Open the file based on whether it should be locked or not.
    if lock:
        try:
            with filelock.FileLock(f'{file_path}.lock', timeout=timeout):
                sheet_data = open_the_file(file_path, args)
        except filelock.Timeout as e:
            raise filelock.Timeout(
                f"Another script holds the lock on {file_path}."
            ) from e
    else:
        sheet_data = open_the_file(file_path, args)

    return sheet_data


def get_usgs_elevation(lat, lon, units='Meters'):
    """
    Get elevation at a lat/lon point from the USGS Elevation Point Query
    Service.

    @param lat The latitude.
    @param lon The longitude.
    @param units The units for the return. Must be either `Meters` or `Feet`.
                 Defaults to `Meters`.
    @return The elevation.
    """

    url = 'https://nationalmap.gov/epqs/pqs.php'
    params = {
        'x': lon,
        'y': lat,
        'units': units,
        'output': 'json'
    }
    response = get_response(url, params=params).json()
    query = response['USGS_Elevation_Point_Query_Service']['Elevation_Query']
    elevation = float(query['Elevation'])

    return elevation
