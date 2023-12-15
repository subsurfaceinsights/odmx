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
import pandas as pd
from deepdiff import DeepDiff
from odmx.log import vprint
from odmx.support.file_utils import open_json

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


def gen_equipment_entry(acquiring_instrument_uuid, code, name, serial_number,
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

    @param acquiring_instrument_uuid
    @param code
    @param name
    @param serial_number
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


def check_existing_equipment(equip_file):
    if os.path.isfile(equip_file):
        equip_schema = os.path.join(json_schema_files,
                                    os.path.basename(equip_file).replace(
                                        '.json', '_schema.json'))
        equipment = open_json(equip_file,
                              validation_path=equip_schema)[0]
        base_uuid = equipment['equipment_uuid']
        rel_start = equipment['position_start_date_utc']
        child_equipment = equipment['equipment']
        child_df = pd.DataFrame(equipment['equipment'])


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
