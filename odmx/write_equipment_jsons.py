#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TODO Make these classes with a to_json method
"""
import os
import uuid
import shutil
import json
import datetime
from importlib.util import find_spec
from deepdiff import DeepDiff
from odmx.log import vprint
from odmx.support.file_utils import open_json, open_csv, clean_name,\
    expand_column_names

json_schema_files = find_spec("odmx.json_schema").submodule_search_locations[0]

def gen_data_to_equipment_entry(column_name, var_domain_cv,
                                acquiring_instrument_uuid, variable_term,
                                units_term, units_conversion=True,
                                expose_as_ds=True,
                                datetime_start=None,
                                datetime_end=None,
                                manual_qa_flag=None,
                                manual_qa_note=None,
                                sensor_type="measured",
                                notes=None):
    """
    Generate data_to_equipment.json file for specified device

    @param column_name
    @param var_domain_cv
    @param acquiring_instrument_uuid
    @param variable_term
    @param units_term
    @param units_conversion optional, default True
    @param expose_as_ds optional, default True
    @param sensor_type, optional, default measured
    @param notes, optional, default None
    @returns dict to be written to data_to_equipment.json
    """
    if manual_qa_flag is not None:
        qa_list = [{
            "datetime_start": datetime_start,
            "datetime_end": datetime_end,
            "qa_flag": manual_qa_flag,
            "qa_note": manual_qa_note
        }]
    else:
        qa_list = None

    return {
        "column_name": column_name,
        "variable_domain_cv": var_domain_cv,
        "acquiring_instrument_uuid": acquiring_instrument_uuid,
        "variable_term": variable_term,
        "units_term": units_term,
        "unit_conversion": units_conversion,
        "expose_as_datastream": expose_as_ds,
        "datastream_manual_qa_list": qa_list,
        "sensor_type": sensor_type,
        "notes": notes
    }


def gen_equipment_entry(code, name, serial_number,
                        acquiring_instrument_uuid=None,
                        owner_first_name="Roelof",
                        owner_last_name="Versteeg", owner_role="owner",
                        vendor=None, purchase_date=None,
                        purchase_order_number=None,
                        description=None, documentation_link=None,
                        position_start_date_utc=None,
                        position_end_date_utc=None,
                        relationship_start_date_time_utc=None,
                        relationship_end_date_time_utc=None,
                        z_offset_m=0, ns_offset_m=0,
                        ew_offset_m=0, height_note=None):
    """
    Generate equipment.json content

    @param code
    @param name
    @param serial_number
    @param acquiring_instrument_uuid optional, default generate new
    @param owner_first_name optional, default Roelof
    @param owner_last_name optional, default Versteeg
    @param owner_role optional, default owner
    @param vendor optional, default In Situ
    @param purchase_date optional, default None
    @param purchase_order_number optional, default None
    @param description optional, default None
    @param documentation_link optional, default None
    @param position_start_date_utc optional, default None
    @param position_end_date_utc optional, default None
    @param relationship_start_date_time_utc optional, default None
    @param relationship_end_date_time_utc optional, default None
    @param z_offset_m optional, default 0
    @param ns_offset_m optional, default 0
    @param ew_offset_m optional, default 0
    @param height_note optional, default None
    @returns dict to write to equipment.json

    """
    if not acquiring_instrument_uuid:
        acquiring_instrument_uuid = str(uuid.uuid4())
    return {
        "equipment_uuid": acquiring_instrument_uuid,
        "equipment_code": code,
        "equipment_name": name,
        "equipment_serial_number": serial_number,
        "equipment_owner_first_name": owner_first_name,
        "equipment_owner_last_name": owner_last_name,
        "equipment_owner_role": owner_role,
        "equipment_vendor": vendor,
        "equipment_purchase_date": purchase_date,
        "equipment_purchase_order_number": purchase_order_number,
        "equipment_description": description,
        "equipment_documentation_link": documentation_link,
        "position_start_date_utc": position_start_date_utc,
        "position_end_date_utc": position_end_date_utc,
        "relationship_start_date_time_utc": relationship_start_date_time_utc,
        "relationship_end_date_time_utc": relationship_end_date_time_utc,
        "equipment_z_offset_m": z_offset_m,
        "equipment_ns_offset_m": ns_offset_m,
        "equipment_ew_offset_m": ew_offset_m,
        "equipment_height_note": height_note,
        "equipment": None,
    }


def read_or_start_data_to_equipment_json(data_to_equipment_map_file,
                                         equipment):
    """
    Load existing data_to_equipment map or initialize a new one with timestamp
    as only column
    @param data_to_equipment_map_file Path of data to equipment map
    @param equipment dict of equipment
    @returns data_to_equip list of dicts with data mappings
    @returns col_list list of data columns
    """
    base_uuid = equipment['equipment_uuid']

    if os.path.isfile(data_to_equipment_map_file):
        data_to_equip_schema = os.path.join(
            json_schema_files,
            'data_to_equipment_map_schema.json')
        data_to_equip = open_json(data_to_equipment_map_file,
                                  validation_path=data_to_equip_schema)
    else:
        data_to_equip = [
            gen_data_to_equipment_entry(column_name='timestamp',
                                    var_domain_cv='instrumentTimestamp',
                                    acquiring_instrument_uuid=base_uuid,
                                    variable_term='nonedefined',
                                    units_term='datalogger_time_stamp',
                                    expose_as_ds=False)]

    col_list = [d2e['column_name'] for d2e in data_to_equip]

    return data_to_equip, col_list


def check_diff_and_write_new(new_data, existing_file):
    """
    Check if json file has changed, if it has back up the original before
    writing the new data to specified path
    @param new_data new data to be written
    @param existing_file Path of (possible) existing file
    """
    if os.path.exists(existing_file):
        with open(existing_file, 'r', encoding='utf-8') as f:
            existing_map = json.load(f)
        deepdiff = DeepDiff(existing_map, new_data)
        if deepdiff:
            vprint(f"Existing map differs from new map: {deepdiff}")
            vprint("Backing up existing json")
            date_str = datetime.datetime.now().strftime("%Y%m%d")
            shutil.copyfile(existing_file,
                        f"{existing_file}.{date_str}.bak")
            with open(existing_file, 'w', encoding='utf-8') as f:
                json.dump(new_data, f, ensure_ascii=False, indent=4)
        else:
            vprint(f"Skipping update of {existing_file}, no changes")
    else:
        vprint(f"{existing_file} does not exist, writing it.")
        if not os.path.exists(os.path.dirname(existing_file)):
            os.makedirs(os.path.dirname(existing_file))
        with open(existing_file, 'w', encoding='utf-8') as f:
            json.dump(new_data, f, ensure_ascii=False, indent=4)


def generate_equipment_jsons(equipment_path,
                             data_block,
                             time_col,
                             columns,
                             mapper,
                             keep_units,
                             start_override=None,
                             end_override=None):
    """
    Generate equipment jsons from data_source_config.json

    @param equipment_path Path to save equipment jsons
    #param data_block single block from data_split in data_source_config
    @param time_col name of timestamp column in original data
    @param columns list of column names (sanitized) to use
    @param mapper dataframe of column names and cv terms and units
    @param keep_units list of units to keep (not convert)
    """
    equip_file = f"{equipment_path}/equipment.json"
    data_to_equipment_map_file = (f"{equipment_path}/"
                                  "data_to_equipment_map.json")

    # Read equipment.json if it exists, otherwise start new
    # Make the directories
    os.makedirs(equipment_path, exist_ok=True)

    # Read base equipment from data_source_config
    logger = data_block['base_equipment']
    if start_override is None:
        start = logger['start_timestamp']
    else:
        start = start_override
    if end_override is None:
        end = logger['end_timestamp']
    else:
        end = end_override

    # Start equipment.json entries
    equipment = gen_equipment_entry(
        acquiring_instrument_uuid=None,
        code=logger['equipment_name'],
        name=logger['equipment_name'],
        serial_number=logger['equipment_serial_number'],
        vendor=None,
        description=None,
        position_start_date_utc=start,
        position_end_date_utc=end,
        relationship_start_date_time_utc=start,
        relationship_end_date_time_utc=end,
        z_offset_m=logger['equipment_z_offset'])

    # Store uuid for use in data_to_equipment_map
    logger_uuid = equipment['equipment_uuid']

    # Initialize data_to_equipment_map with timestamp
    data_to_equip = \
        [gen_data_to_equipment_entry(
            column_name='timestamp',
            var_domain_cv='instrumentTimestamp',
            acquiring_instrument_uuid=logger_uuid,
            variable_term='nonedefined',
            units_term='datalogger_time_stamp',
            units_conversion=True,
            expose_as_ds=False)]

    # Remove timestamp from logger column list (if it's there)
    if time_col in logger['columns']:
        logger['columns'].remove(time_col)

    # If anything is left, add it as instrumentMetadata,
    # not exposed as a datastream
    for column in logger['columns']:
        data_to_equip.append(
            gen_data_to_equipment_entry(
                column_name=column,
                var_domain_cv='instrumentMetadata',
                acquiring_instrument_uuid=logger_uuid,
                variable_term=mapper['variable_cv'][column],
                units_term=mapper['unit_cv'][column],
                units_conversion=True,
                expose_as_ds=False))

    # If there are attached sensors, iterate through
    child_equipment = []
    if data_block['attached_sensors'] is not None:
        for sensor in data_block['attached_sensors']:
            # These should inherit from logger if not defined
            if sensor['start_timestamp'] is not None:
                start = sensor['start_timestamp']
            if sensor['end_timestamp'] is not None:
                end = sensor['end_timestamp']

            # If equipment code isn't specified, use 'sensor_name'
            if 'sensor_code' in sensor:
                sensor_code = sensor['sensor_code']
            else:
                sensor_code = sensor['sensor_name']

            # Generate equipment entry
            child = gen_equipment_entry(
                acquiring_instrument_uuid=None,
                code=sensor_code,
                name=sensor['sensor_name'],
                serial_number=sensor['serial_number'],
                vendor=None,
                description=None,
                position_start_date_utc=start,
                position_end_date_utc=end,
                relationship_start_date_time_utc=start,
                relationship_end_date_time_utc=end,
                z_offset_m=sensor['sensor_z_offset'])

            # Append to child equipment list
            child_equipment.append(child)

            # Sanitize specified column names
            sensor_columns = \
                [clean_name(x) for x in sensor['columns']]
            # Expand column names if they were specified with *
            sensor['columns'] = expand_column_names(
                sensor_columns, columns)

            # Now iterate over the column names
            for column in sensor['columns']:
                units_term=mapper['unit_cv'][column]

                # check if units term is one of our defined keepers
                if units_term in keep_units:
                    units_conversion = False
                else:
                    units_conversion = True

                # Create the data to equipment map entry
                # Need to explicitly typecast expose_as_ds, else
                # it is type numpy.bool_ which is incompatible
                # with json.dump
                data_to_equip.append(
                    gen_data_to_equipment_entry(
                        column_name=column,
                        var_domain_cv='instrumentMeasurement',
                        acquiring_instrument_uuid=\
                            child['equipment_uuid'],
                        variable_term=\
                            mapper['variable_cv'][column],
                        units_term=units_term,
                        units_conversion=units_conversion,
                        expose_as_ds=\
                            bool(mapper['expose'][column])))

    # Add child equipment to base
    equipment.update({'equipment': child_equipment})

    # Check diff, backup, and write new jsons
    check_diff_and_write_new(data_to_equip,
                             data_to_equipment_map_file)
    check_diff_and_write_new([equipment], equip_file)

def setup_csv_data_source_config_json(csv_path):
    """
    Initialize data_source_config.json with data column names from csv
    """
    directory = os.path.dirname(csv_path)
    df = open_csv(csv_path)

    cols = []
    for col in df.columns.tolist():
        cols.append({"name": col,
                     "variable_cv": "",
                     "unit_cv": "",
                     "expose": True})



    data_split = [{"sampling_feature": "",
                   "equipment_metadata": None,
                   "columns": cols}]

    config = {"data_file_extension": "csv",
              "data_file_tabs": None,
              "data_split": data_split}

    with open(f'{directory}/data_source_config.json', 'w',
              encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=4)
