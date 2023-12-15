#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File utilities for the ODMX package
"""
import os
import shutil
import datetime
import json
import filelock
import jsonschema
import pandas as pd
from deepdiff import DeepDiff
from odmx.log import vprint

def clean_name(col):
    """
    Generate ingested name from csv column name
    """
    replace_chars = {' ': '_', '-': '_', '/': '_', '²': '2', '³': '3',
                     '°': 'deg', '__': '_', '%': 'percent'}
    new_col = col.lower()
    for key, value in replace_chars.items():
        new_col = new_col.replace(key, value)
    # Make sure our replacement worked
    try:
        new_col.encode('ascii')
    except UnicodeEncodeError as exc:
        raise RuntimeError(f"Column '{new_col}' derived from decagon "
                           "data still has special characters. "
                           "Check the find/replace list") from exc
    return new_col


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
            with open(file_path, encoding="utf-8") as json_file:
                return json.load(json_file, **args)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"No file found at: {file_path}.") from e
        except json.decoder.JSONDecodeError as e:
            raise ValueError(f"The .json file {file_path} could not be"
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


def check_diff_and_write_new(new_data, existing_file):
    """
    Check if data to equipment map file has changed
    """
    if os.path.exists(existing_file):
        with open(existing_file, 'r', encoding='utf-8') as f:
            existing_map = json.load(f)
            deepdiff = DeepDiff(existing_map, new_data)
            if deepdiff:
                # print(f"Existing map differs from new map: {deepdiff}")
                print("Backing up existing json")
                date_str = datetime.datetime.now().strftime("%Y%m%d")
                shutil.copyfile(existing_file,
                            f"{existing_file}.{date_str}.bak")
                with open(existing_file, 'w', encoding='utf-8') as f:
                    json.dump(new_data, f, ensure_ascii=False, indent=4)
            else:
                vprint(f"Skipping update of {existing_file}, no changes")


def get_last_timestamp_csv(file_path, timestamp_index=0, max_line_size=8192):
    # Read the last line of the file to get the timestamp. We do this
    # by seeking to the file 4096 bytes, then reading the last line.
    # This is a hack, but it works for campbell data.
    # We need to open the file in binary mode to use seek.
    with open(file_path, 'rb') as f:
        # Get size
        f.seek(0, os.SEEK_END)
        size = f.tell()
        print(size)
        if size == 0:
            print(f"Notice: file '{file_path}' is empty.")
            return None
        # Get the last line.
        f.seek(-max_line_size, os.SEEK_END)
        last_line = f.read().splitlines()[-1]
        # Convert the bytes to a string.
        last_line = last_line.decode('utf-8')
        # Get the timestamp from the last line, which is the first
        # element in the list.
        file_last_timestamp = last_line.split(',')[timestamp_index]
        # Remove quotes
        file_last_timestamp = file_last_timestamp.replace('"', '')
        # Parse the timestamp.
        file_last_timestamp = pd.to_datetime(file_last_timestamp)
        return file_last_timestamp