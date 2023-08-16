#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Write data_sources.json for a project. Currently only tested with hydrovu.
"""
import os
import json
import argparse


def write_data_sources(project: str,
                       datasource: str,
                       auth: str,
                       device_list: str):
    """
    Write data_sources.json programmatically based on location list

    @param project Project name (str)
    @param datasource Datassource name (str)
    @param auth Path to yaml file with auth details (str)
    @param device_list Path to list of locations and instruments
    @returns data_sources.json written to file in project directory
    """
    # Define core paths
    base_path = os.environ.get('SSI_BASE', '/opt/ssi')
    project_path = f"{base_path}/projects/{project}"
    if not os.path.exists(project_path):
        raise OSError(f"Project path {project_path} does not exist")

    # Load file inputs
    with open(device_list, 'r', encoding='utf-8') as d:
        devices = json.load(d)

    # Check if we already have a datasources, and if so what's in it
    file_path = f"{project_path}/data_sources.json"

    if os.path.exists(file_path):
        with open(file_path, encoding='utf-8') as f:
            data_sources = json.load(f)
    else:
        raise OSError(f"{file_path} not found. Check SSI_BASE variable")
    existing_devices = []
    for source in data_sources:
        if datasource in source['shared_info']['data_source_name']:
            existing_devices.append(source['harvesting_info']['device_name'])

    # For now this will just prevent this from being used for other data
    # sources. Going forward the plan would be to add alternate structures
    # this way, probably using Templates, but for now that's needlessly complex
    if 'hydrovu' in datasource:
        pass
    else:
        raise ValueError(f"Helper script not compatible with {datasource}")

    for device in devices:
        device_id = device['id']
        dev_location = device['location']
        dev_model = device['model']
        # timezone = device['timezone']

        device_name = f"{dev_location}_{dev_model}"
        if device_name in existing_devices:
            continue
        ds_path = f"{project}/{datasource}/{device_name}"

        feeder_table = device_name.lower()

        shared_info = {"data_source_name": datasource,
                       "data_source_scope": "project_specific",
                       # TODO Unclear whether the timezone is correct for
                       # hydrovu
                       "data_source_timezone": 'UTC',
                       # "data_source_timezone": timezone,
                       "data_source_path": ds_path}

        harvest_info = {"auth_yml": auth,
                        "device_id": device_id,
                        "device_name": device_name}

        ingest_info = {"feeder_table": feeder_table}

        process_info = {"sampling_feature_code": dev_location,
                        "equipment_directory": f"{datasource}/{device_name}"}

        ds_dict = {"shared_info": shared_info,
                   "harvesting_info": harvest_info,
                   "ingestion_info": ingest_info,
                   "processing_info": process_info}

        data_sources.append(ds_dict)

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data_sources, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("project", help="Project name")
    # TODO make datasource an enum based on modules
    parser.add_argument("datasource", help="Datasource name")
    parser.add_argument("auth", help="Path to yaml file with auth details")
    parser.add_argument("device_list", help="Path to list of locations and "
                        "instruments JSON file (may be in project)")
    args = parser.parse_args()

    write_data_sources(args.project,
                       args.datasource,
                       args.auth,
                       args.device_list)
