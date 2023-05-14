#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TODO Make these classes with a to_json method
"""
from odmx.log import vprint


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


def get_mapping(mapper,
                lookup_target,
                lookup_key,
                lookup_obj,
                verbose=False):
    """
    Retrieve mapping to CV or friendly name for a unit or variable.

    @param lookup_target Identifier to return
            options: 'cv_term', 'nice_name', 'id'
    @param lookup_key Type of object used for lookup
                        ('nice_name', 'cv_term', or 'id')
    @param lookup_obj Handle to use for lookup (name, ID, or CV)
    """
    # print out come parameters
    if verbose:
        vprint(' lookup target : ', lookup_target)
        vprint(' lookup key   : ', lookup_key)
        vprint(' lookup obj    : ', lookup_obj)

    mapper_keys = set(mapper.columns)
    if lookup_target not in mapper_keys:
        raise ValueError(f"Invalid lookup target: {lookup_target}")
    if lookup_key not in mapper_keys:
        raise ValueError(f"Invalid lookup key: {lookup_key}")

    obj = mapper[mapper[lookup_key] == lookup_obj]

    if obj.empty:
        raise ValueError(f"Invalid lookup object: {lookup_key} "
                         f"{lookup_obj} not found in mapper")
    alt_name = obj[lookup_target].array[0]
    return alt_name
