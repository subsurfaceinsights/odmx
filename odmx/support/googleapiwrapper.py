#!/usr/bin/env python3

"""
Subpackage to handle various Google Drive API functions.
"""

import os
import pickle
import h5py
import imageio
import pandas as pd
import numpy as np
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


def api_connect(service_type, scope):
    """
    Set up the REST API service for Drive or Sheets.

    @param service_type The name of the service to connect to. "drive" or "sheets".
    @param scope The scope of what the connection should allow. "read" or "readwrite".
    @return The connection service object.
    """

    # Make sure that the passed service_type is valid.
    service_type = service_type.lower()
    service_map = {
        'drive': 'drive',
        'sheets': 'spreadsheets',
    }
    if service_type not in service_map:
        raise ValueError(f"The service type {service_type} is not supported.")

    # Make a mapping dictionary for the current versions of the APIs.
    version_map = {
        'drive': 'v3',
        'sheets': 'v4',
    }

    # Make sure that the passed scope is valid.
    scope = scope.lower()
    scope_map = ['read', 'readwrite']
    if scope not in scope_map:
        raise ValueError(f"The scope {scope} is not supported.")

    # Define the base system path for the API credentials.
    api_path = os.path.join('/opt', 'ssi', 'config', 'googleapi', service_type,
                            scope)
    token_path = os.path.join(api_path, 'token.pickle')
    cred_path = os.path.join(api_path, 'credentials.json')

    # Define the scopes to use. In our use-case, Erek originally set up each
    # set of credentials to be scope-specific, and while it's now possible to
    # have multiple scopes in one set of credentials, Erek still needs to work
    # through how we'd implement that properly.
    scopes = f'https://www.googleapis.com/auth/{service_map[service_type]}'
    if scope == 'read':
        scopes = f'{scopes}.readonly'

    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time. Here we define the authentication token based on that file.
    token = None
    if os.path.exists(token_path):
        print(f"Authentication token file found for {service_type}.{scope}.")
        with open(token_path, 'rb') as token_file:
            token = pickle.load(token_file)
    else:
        print(f"Authentication token file absent for {service_type}.{scope}.")

    # If the authentication token is missing or invalid, refresh or create one.
    if token is None or not token.valid:
        # If the token exists, is expired, and there is a refresh token field,
        # refresh it.
        if token is not None and token.expired and token.refresh_token is not None:
            print("Refreshing the authentication token.")
            token.refresh(Request())
            print("The authentication token has been refreshed.")
        # Otherwise, we create a new authentication token.
        else:
            print("Generating new authentication token. You will now be"
                  " prompted to log into Google. Please use the Subsurface"
                  " Insights AWS account to proceed.")
            flow = InstalledAppFlow.from_client_secrets_file(cred_path, scopes)
            token = flow.run_local_server(port=0)
            print("New authentication token generated.")
        # Save the credentials for the next run
        print("Saving the new authentication token.")
        with open(token_path, 'wb') as token_file:
            pickle.dump(token, token_file)
        print("Authentication token saved.")

    # Create the service object.
    service = build(service_type, version_map[service_type], credentials=token,
                    cache_discovery=False)
    print(f"API connection successful for {service_type}.{scope}.\n")

    return service


def drive_make_copy(service, file_drive_id, file_name):
    """
    Make a copy of a select file in Drive.

    @param service The Drive service connection object.
    @param file_drive_id The Drive ID of the file to copy.
    @param file_name The new name for the copied file.
    """

    copied_file = {'name': file_name}
    service.files().copy(fileId=file_drive_id, body=copied_file).execute()


def drive_list_folders(service, folder_drive_id):
    """
    Get a list of folders from a top-level Drive folder.

    @param service The Drive service connection object.
    @param file_drive_id The Drive ID of the directory to examine.
    @return A dictionary containing folder names and IDs.
    """

    # Define a "placeholder" token for the Drive API call.
    # This is done because the API call has to cycle through "pages"
    # capped at 1000 files each, and we have to start somewhere.
    token = None
    folder_dict = {}
    # After each "page", the API returns a new page token.
    # Once there are no more "pages", it becomes None and the loop ends.
    while True:
        folder_return = service.files().list(q='mimeType='
                                             '"application/vnd.google-apps.folder"'
                                             ' and trashed=false and'
                                             f' "{folder_drive_id}" in parents',
                                             pageSize=1000, pageToken=token,
                                             fields='nextPageToken,'
                                             ' files(id, name)').execute()
        folder_list = folder_return.get('files', [])
        for d in folder_list:
            temp_dict = {d['name']: d['id']}
            folder_dict.update(temp_dict)
        token = folder_return.get('nextPageToken', None)
        if token is None:
            break

    return folder_dict


def drive_list_sheets(service, folder_drive_id):
    """
    Get a list of Sheets files from a top-level Drive folder.

    @param service The Drive service connection object.
    @param file_drive_id The Drive ID of the directory to examine.
    @return A dictionary containing Sheets names and IDs.
    """

    # Define a "placeholder" token for the Drive API call.
    # This is done because the API call has to cycle through "pages"
    # capped at 1000 files each, and we have to start somewhere.
    token = None
    sheets_dict = {}
    # After each "page", the API returns a new page token.
    # Once there are no more "pages", it becomes None and the loop ends.
    while True:
        sheets_return = service.files().list(q='mimeType='
                                             '"application/vnd.google-apps.spreadsheet"'
                                             ' and trashed=false and'
                                             f' "{folder_drive_id}" in parents',
                                             pageSize=1000, pageToken=token,
                                             fields='nextPageToken,'
                                             ' files(id, name)').execute()
        sheets_list = sheets_return.get('files', [])
        for d in sheets_list:
            temp_dict = {d['name']: d['id']}
            sheets_dict.update(temp_dict)
        token = sheets_return.get('nextPageToken', None)
        if token is None:
            break

    return sheets_dict


def drive_list_imgs(service, site_folders_dict):
    """
    Get a list of images from a series of drive folders.

    @param service The Drive service connection object.
    @param site_folders_dict A dictionary containing the names and IDs of Drive
                             directories to look in.
    @return A list of image info, and a DataFrame of the same.
    """

    print("Starting data acquisition. This may take some time.")
    img_list = []
    for key, value in site_folders_dict.items():
        print(f"Looking in: {key}.")
        # Define a "placeholder" token for the Drive API call.
        # This is done because the API call has to cycle through "pages"
        # capped at 1000 files each, and we have to start somewhere.
        token = None
        i = 1
        # After each "page", the API returns a new page token.
        # Once there are no more "pages", it becomes None and the loop ends.
        while True:
            img_dict = service.files().list(q='mimeType contains "image/jpeg"'
                                            f' and trashed=false and "{value}"'
                                            ' in parents', pageSize=1000,
                                            pageToken=token,
                                            fields='nextPageToken,'
                                            ' files(fullFileExtension, name,'
                                            ' id, imageMediaMetadata)').execute()
            img_list.append(img_dict.get("files", []))
            print(f"Step {i}: Found {len(img_dict.get('files', []))} new"
                  " files.")
            i += 1
            token = img_dict.get('nextPageToken', None)
            if token is None:
                break
    print("Data acquisition complete.")

    print("Cleaning the data.")
    # The file data is in a list of lists of dictionaries,
    # so this line flattens that into a single list of dictionaries.
    img_list = [item for sublist in img_list for item in sublist]
    # Remove any "bad" entries (if the file starts with ".").
    img_list = [item for item in img_list if not item['name'].startswith('.')]
    # "Flatten" the list further: each dictionary has a sub-dictionary,
    # so we bring that to the surface.
    final_img_list = []
    for img_dict in img_list:
        temp_dict = {}
        for key, value in img_dict.items():
            if isinstance(value, dict):
                for subkey, subvalue in value.items():
                    temp_dict[subkey] = subvalue
            else:
                temp_dict[key] = value
        final_img_list.append(temp_dict)
    del temp_dict
    # Remove whitespace.
    final_img_list = [{k:v.strip() if isinstance(v, str)
                       else v for k, v in d.items()} for d in final_img_list]
    print("Data cleaned.")

    # Create a DataFrame of the images.
    # The metadata info is what's present in most of the phenocam images.
    # name and id come from Google Drive, but the rest are from the images.
    metadata_info = [
        'name',
        'time',
        'id',
        'width',
        'height',
        'rotation',
        'cameraMake',
        'cameraModel',
        'exposureTime',
        'aperture',
        'flashUsed',
        'focalLength',
        'isoSpeed',
        'meteringMode',
        'sensor',
        'exposureMode',
        'colorSpace',
        'whiteBalance',
        'exposureBias',
        'maxApertureValue',
    ]
    df = pd.DataFrame(final_img_list, columns=metadata_info)
    # Change the time column from strings to datetime objects.
    df['time'] = pd.to_datetime(df['time'], format="%Y:%m:%d %H:%M:%S")
    # Remove any rows where the time is Not a Time, or the width or height are
    # zero.
    df = df[~df['time'].isnull()]
    df = df.query('width != 0')
    df = df.query('height != 0')
    # Sort the DataFrame by time and reset the index.
    df.sort_values(by='time', inplace=True)
    df.reset_index(drop=True, inplace=True)

    return final_img_list, df


def get_data_from_sheet(service, sheet_id, data_range):
    """
    Get information from a Sheet as a DataFrame.

    @param service The Sheets service connection object.
    @param sheet_id The Drive ID of the Sheet to examine.
    @param data_range The data range to grab from the Sheet.
    @return A DataFrame containing everything in the data range.
    """

    # Repeatedly try to connect to Google to get the Sheet.
    while True:
        try:
            data_dict = service.spreadsheets().values().get(spreadsheetId=sheet_id,
                                                            range=data_range,
                                                            valueRenderOption='UNFORMATTED_VALUE').execute()
            break
        #TODO Only catch the specific API exception
        #pylint: disable=broad-except
        except Exception as e:
            print(e)
            print("Trying again.\n")

    # Isolate the actual Sheet values and headers into discrete lists.
    values = data_dict['values']
    headers = values.pop(0)

    # Here we have two odd possible edge cases:
    # 1) If there are any headers "at the end" that exist without any actual
    #    data in their columns (e.g., a "notes" column with no actual entries),
    #    Google doesn't return any values for them (not even ''), and so
    #    there's a length mismatch between the number of columns and the values
    #    in the columns. In this case, we need to add ''.
    # 2) If there is data in columns "at the end" that exist without an actual
    #    header (e.g., someone added a note without specifically adding in
    #    "notes" as a column header), Google doesn't provide headers for these
    #    columns, and so there's a length mismatch again. In this case, we
    #    actually want to delete those columns, since we don't want to
    #    interpret what the user was trying to do.
    for i, value in enumerate(values):
        # Address (1).
        if len(value) < len(headers):
            diff = len(headers) - len(value)
            values[i] += ['' for j in range(diff)]
        # Address (2).
        elif len(value) > len(headers):
            values[i] = value[:len(headers)]

    # Turn the data into a DataFrame.
    data_df = pd.DataFrame(values, columns=headers)
    # Strip whitespace.
    data_df = data_df.mask(data_df.apply(pd.to_numeric, errors='coerce').isnull(), data_df.astype(str).apply(lambda x: x.str.strip()))
    # Replace blank entires with None.
    data_df = data_df.applymap(lambda x: None if isinstance(x, str) and (not x or x.isspace()) else x)

    return data_df


def drive_make_hdf5(service, file_path, df, num_of_img, dim_width, dim_height):
    """
    Create an HDF5 file based on the chosen time of day.

    @param service The Drive service connection object.
    @param file_path The full path of the new HDF5 file.
    @param df A DataFrame.
    @param num_of_img The number of images.
    @param dim_width The width of the images in question.
    @param dim_height The height of the images in question.
    """

    # Define some constants.
    IMG_DATA = 'img_data'
    IMG_NAME = 'img_name'
    IMG_TIME = 'img_time'
    IMG_ID = 'img_id'

    with h5py.File(file_path, 'w') as f:
        # Define the dataset for the images.
        data_dset = f.create_dataset(IMG_DATA,
                                     (num_of_img, dim_height, dim_width, 3),
                                     dtype=np.uint8)
        name_dset = f.create_dataset(IMG_NAME, (num_of_img,), dtype='S40')
        time_dset = f.create_dataset(IMG_TIME, (num_of_img,), dtype='S40')
        id_dset = f.create_dataset(IMG_ID, (num_of_img,), dtype='S40')

        # Iterate through the DataFrame and add the data to the HDF5 file.
        for row in df.itertuples():
            img_name = row.phenocam_image_name
            img_time = row.observation_time_utc
            img_id = row.google_drive_id
            print(f"Processing image {row.Index}: {img_name}.")
            # This is the older way of getting a file's data.
            # img_data = np.array(PIL.Image.open(io.BytesIO(service.files().get_media(fileId=img_id).execute())))
            img_data = imageio.imread(service.files().get_media(fileId=img_id)
                                      .execute(), format='JPG')

            # Save data string converted as a numpy array.
            # f = h5py.File(file_path, 'r+')
            # f[IMG_DATA][i] = img_data
            data_dset[row.Index] = img_data
            name_dset[row.Index] = img_name.encode('ascii', 'ignore')
            time_dset[row.Index] = str(img_time).encode('ascii', 'ignore')
            id_dset[row.Index] = img_id.encode('ascii', 'ignore')
            # f.close()
