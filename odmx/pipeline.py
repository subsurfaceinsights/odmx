#!/usr/bin/env python3

"""
Automate the creation of a project's databases, including harvesting,
ingesting, and processing data sources.
"""

import os
import sys
import argparse
import importlib
from odmx.support.config import Config
import json
from odmx.log import set_verbose
from odmx.log import vprint
from odmx.timeseries_processing import check_datastream_entries
from odmx.populate_base_tables import populate_base_tables
from odmx.populate_cvs import populate_cvs
from odmx.json_validation import open_json
from odmx.abstract_data_source import DataSource
import odmx.support.db as db
from odmx.support.db import Connection
import dataclasses
from odmx.support.db import reset_db
from beartype import beartype
from beartype.typing import Optional
from odmx.shared_conf import setup_base_config, validate_config

@dataclasses.dataclass
class data_source_info:
    """
    Shared information for a data source.
    """
    # The type of the data source. This is used to lookup the module that
    # contains the data source class.
    type: str
    # The scope of the data source. This is used to determine which database
    # the data source will be ingested into, one of 'project' or 'global'.
    scope: str
    # The object instantiated with the shared info
    data_source_obj: DataSource
    # Parameters for the harvesting stage which is a method of the data source
    # class.
    harvesting_info: dict
    # Parameters for the ingesting stage which is a method of the data source
    # class.
    ingestion_info: dict
    # Parameters for the processing stage which is a method of the data source
    # class.
    processing_info: dict

class MissingDataSourceError(Exception):
    """
    Exception for missing data sources.
    """

_data_source_class_cache = {}
def find_data_source_class(type: str):
    if type in _data_source_class_cache:
        return _data_source_class_cache[type]
    if '.' in type:
        imports_to_try = [type]
    else:
        imports_to_try = [
            type, f'odmx.datasources.{type}'
        ]
    camel_type = type.split('.')[-1].title().replace('_', '')
    classes_to_try = [
        camel_type, f'{camel_type}DataSource'
    ]
    tried = []
    for i in imports_to_try:
        try:
            module = importlib.import_module(i)
            for j in classes_to_try:
                try:
                    found = getattr(module, j)
                    vprint('Found data source class '
                           f'{module.__name__}.{found.__name__} for {type}')
                    _data_source_class_cache[type] = found
                    return found
                except AttributeError:
                    tried.append(f'{i}.{j}')
        except ImportError as e:
            vprint(f'Could not import {i}: {e}')
            tried.append(i)
    raise MissingDataSourceError("\n\t"
        f"Could not find a data source class for {type}. Tried: {tried}.\n\t"
        "This could be because the data source module is not found "
        "or the data source class is not named correctly.\n\tIt's also "
        "possible  that there is an import error in the data source module."
        "\n\tSet --verbose True (on CLI or config or env) to see more "
        "information."
        )

def get_method_params(method):
    return method.__code__.co_varnames[:method.__code__.co_argcount]

def run_pipeline(conf: Config, work_dir: str):
    """
    The main function that runs the pipeline.

    @param conf The main config object containing information from a PostgreSQL
                connection yaml file and CL arguments.
    """

    # Define paths.
    project_name = conf.project_name
    project_path = os.path.join(work_dir, 'projects', project_name)
    global_path = os.path.join(work_dir, 'projects', 'global')
    data_path = os.path.join(work_dir, 'data', project_name)
    global_data_path = os.path.join(work_dir, 'data', 'global')
    data_sources_path = os.path.join(project_path,
                                     'data_sources.json')
    if not os.path.exists(data_sources_path):
        raise Exception(
                f"Could not find data sources file '{data_sources_path}'.")
    if not os.path.exists(project_path):
        raise Exception(f"Could not find project path '{project_path}'.")
    if not os.path.exists(global_path):
        raise Exception(f"Could not find global path '{global_path}'.")
    if not os.path.exists(data_path):
        print(f"Creating data path '{data_path}'.")
        os.makedirs(data_path)
    if not os.path.exists(global_data_path):
        print(f"Creating global data path '{global_data_path}'.")
        os.makedirs(global_data_path)
    # Open and validate the data sources .json file.
    data_sources_json = open_json(data_sources_path)
    data_sources = []
    for num, d in enumerate(data_sources_json):
        original_entry = d.copy()
        i = d['shared_info']
        type = i['data_source_type']
        del i['data_source_type']
        scope = i['data_source_scope']
        del i['data_source_scope']
        # Whatever remains are paraemters that belong to the data source class.
        # and aren't passed to the data source class's methods.
        shared_info = i
        harvesting_info = d.get('harvesting_info', {}) or {}
        ingestion_info = d.get('ingestion_info', {}) or {}
        processing_info = d.get('processing_info', {}) or {}
        data_source_class = find_data_source_class(type)
        # Now we check the data source class for the required methods and
        # make sure required parameters are passed.
        def check_dict_against_method_params(d, method):
            internal_params = [
                    'self',
                    'odmx_db_con',
                    'feeder_db_con',
                    'project_path',
                    'project_name',
                    'data_path']
            dict_params = set(d.keys())
            # We semantically consider parameters with null values to be
            # missing.
            for p in dict_params:
                if d[p] is None:
                    del d[p]
            dict_params = set(d.keys())
            params = get_method_params(method)
            for p in params:
                if p not in d:
                    if p in internal_params:
                        continue
                    # check if there is a default value
                    if method.__defaults__ is not None:
                        if p not in method.__kwdefaults__:
                            continue
                    raise Exception(
                        f"Data Source JSON missing expected parameter {p} "
                        f"for {data_source_class.__name__}.{method.__name__}() "
                        f"while processing data_sources.json entry no {num+1} "
                        f"of type {type}. This happens when a parameter in the"
                        " JSON is missing or has a null value but is required "
                        "Full entry: \n"
                        f"{json.dumps(original_entry, indent=4)}\n"
                        f"Method signature: {params}"
                    )
                else:
                    dict_params.remove(p)
            if len(dict_params) > 0:
                raise Exception(
                    "Data Source JSON contains unknown parameters "
                    f"{dict_params} for "
                    f"{data_source_class.__name__}.{method.__name__} while "
                    f"processing {type} data source entry no {num+1}. "
                    "This is caused when a parameter in the JSON does not "
                    "match a parameter in the method signature."
                    f"Full entry: \n:"
                    f"{json.dumps(original_entry, indent=4)}\n"
                    f"Method signature: {params}"
                )
        check_dict_against_method_params(shared_info,
                                         data_source_class.__init__)
        check_dict_against_method_params(harvesting_info,
                                         data_source_class.harvest)
        check_dict_against_method_params(ingestion_info,
                                         data_source_class.ingest)
        check_dict_against_method_params(processing_info,
                                         data_source_class.process)
        # Now we can instantiate the data source class.
        data_source_obj = data_source_class(
            project_name=project_name,
            project_path=project_path if scope == 'project_specific' else global_path,
            data_path=data_path if scope == 'project_specific' else global_data_path,
            **shared_info)

        data_sources.append(data_source_info(
            type=type,
            scope=scope,
            data_source_obj=data_source_obj,
            harvesting_info=harvesting_info,
            ingestion_info=ingestion_info,
            processing_info=processing_info
        ))
    # Check that the passed data source names are valid for this project.
    project_sources = set({i.type
                            for i in data_sources})

    if conf.data_source_types is None:
        data_source_types = project_sources
    else:
        data_source_types = set(conf.data_source_types)
        if conf.skip_data_source_types is not None:
            raise Exception(
                "Cannot pass both `data_source_types` and"
                " `skip_data_source_types`.")
        missing = set(conf.data_source_types) - project_sources
        if missing:
            raise Exception(
                f"Data source types {missing} are not valid for this project. "
                f"Valid data source types are: {project_sources}."
            )
    if conf.skip_data_source_types is not None:
        skip_data_source_types = set(conf.skip_data_source_types)
        if conf.data_source_types is not None:
            raise Exception(
                "Cannot pass both `data_source_types` and"
                " `skip_data_source_types`.")
        missing = skip_data_source_types - project_sources
        if missing:
            raise Exception(
                f"Data source types {missing} are not valid for this project. "
                f"Valid data source types are: {project_sources}."
            )
        data_source_types = project_sources - conf.skip_data_source_types



    data_sources = [i for i in data_sources
                    if i.type
                    in data_source_types]

    from_scratch = conf.from_scratch
    wipe_odmx = conf.wipe_odmx
    wipe_feeder = conf.wipe_feeder
    wipe_global = conf.wipe_global
    if from_scratch:
        wipe_odmx = True
        wipe_feeder = True
        wipe_global = True

    if wipe_odmx and 'populate' not in conf.data_processes:
        raise Exception(
            "Cannot wipe ODMX database without populating it. "
            "Please add 'populate' to the data_processes list."
        )

    con = db.connect(
        config=conf,
    )
    project_db = f'odmx_{conf.project_name}'
    sql_dir = os.path.realpath(
            f'{os.path.dirname(__file__)}/../db/odmsqlscript')
    odmx_sql_template = f'{sql_dir}/ODMX_Schema_Latest.sql'
    if wipe_global:
        print("Wiping global feeder database.")
        reset_db(con, 'odmx_feeder_global')
        with db.connect(
            config=conf,
            db_name='odmx_feeder_global'
        ) as db_con:
            db.create_schema(db_con, 'feeder')
    if wipe_feeder:
        try:
            with db.connect(
                config=conf,
                db_name=project_db
            ) as db_con:
                # TODO cleanup pathing
                print("Wiping feeder schema in odmx project database")
                db.drop_schema(db_con, 'feeder')
                db.create_schema(db_con, 'feeder')
        except Exception as e:
            print("Error wiping feeder schema in odmx project database")
            print(e)
    if wipe_odmx:
        print("Wiping ODMX database.")
        reset_db(con, project_db, sql_template=odmx_sql_template)
        # Connect to the ODMX database and create the feeder schema.
        with db.connect(
            config=conf,
            db_name=project_db
        ) as odmx_db_con:
            db.create_schema(odmx_db_con, 'feeder')
            db.create_schema(odmx_db_con, 'datastreams')

    valid_data_processes = set([
        'populate', 'harvest', 'ingest', 'process', 'check'])
    # Check the passed data processes.
    if conf.data_processes is None:
        data_processes = valid_data_processes
    else:
        data_processes = set(conf.data_processes)
        missing = set(conf.data_processes) - valid_data_processes
        if missing:
            raise Exception(
                f"Data processes {missing} are not valid. "
                f"Valid data processes are: {valid_data_processes}."
            )

    odmx_db_con = db.connect(
        config=conf,
        db_name=project_db)
    db.set_current_schema(odmx_db_con, 'odmx')
    feeder_db_con = db.connect(
        config=conf,
        db_name=project_db)
    db.set_current_schema(feeder_db_con, 'feeder')
    global_db_con = db.connect(
        config=conf,
        db_name='odmx_feeder_global')
    db.set_current_schema(global_db_con, 'feeder')


    if 'populate' in data_processes:
        print("Starting populate process")
        populate_cvs(odmx_db_con, global_path)
        populate_cvs(odmx_db_con, project_path)
        populate_base_tables(odmx_db_con, global_path, project_path)
    if 'harvest' in conf.data_processes:
        print("Starting harvest process.")
        for data_source in data_sources:
            obj = data_source.data_source_obj
            obj.harvest(
                    **data_source.harvesting_info)
    if 'ingest' in conf.data_processes:
        print("Starting ingest process.")
        for data_source in data_sources:
            obj = data_source.data_source_obj
            feeder_db_con = feeder_db_con if data_source.scope == 'project_specific' else global_db_con
            kwargs = data_source.ingestion_info
            params = set(get_method_params(obj.ingest))
            if 'feeder_db_con' in params:
                kwargs['feeder_db_con'] = feeder_db_con
            if 'odmx_db_con' in params:
                kwargs['odmx_db_con'] = odmx_db_con
            obj.ingest(**kwargs)
    if 'process' in conf.data_processes:
        print("Starting processing into ODMX process.")
        for data_source in data_sources:
            obj = data_source.data_source_obj
            feeder_db_con = feeder_db_con if data_source.scope == 'project_specific' else global_db_con
            kwargs = data_source.processing_info
            params = set(get_method_params(obj.process))
            if 'feeder_db_con' in params:
                kwargs['feeder_db_con'] = feeder_db_con
            if 'odmx_db_con' in params:
                kwargs['odmx_db_con'] = odmx_db_con
            obj.process(**kwargs)
    if 'check' in conf.data_processes:
        print("Starting check process")
        # Initialize the ODMX API.
        passed = check_datastream_entries(odmx_db_con, conf.fix)
        sys.exit(0 if passed else 1)



# Run the main part of the script.
if __name__ == '__main__':
    # Define the config object.
    config = Config()
    # Set up the argparser, and add parameters to the config object.
    parser = argparse.ArgumentParser(description="Create databases and process"
                                     " data for a specified project.")
    setup_base_config(config, parser)
    config.add_config_param('from_scratch', default=False, validator='bool',
                            optional=True, help="If this flag is set, recreate"
                            " all project databases.")
    config.add_config_param('wipe_odmx', default=False, validator='bool',
                            optional=True, help="If this flag is set, recreate"
                            " the ODMX database.")
    config.add_config_param('wipe_feeder', default=False, validator='bool',
                            optional=True, help="If this flag is set, recreate"
                            " the feeder database.")
    config.add_config_param('wipe_global', default=False, validator='bool',
                            optional=True, help="If this flag is set, recreate"
                            " the global feeder database.")
    config.add_config_param('data_source_types', default=None, optional=True,
                            help="A list of data source types to evaluate. "
                            "Provide this as a comma separated list with no "
                            "spaces.")
    config.add_config_param('skip_data_source_types', default=None,
                            optional=True, help="A list of data source types "
                            "to skip. Provide this as a comma separated list "
                            "with no spaces.")
    config.add_config_param('data_processes',
                            default=[
                                'populate', 'harvest',
                                'ingest', 'process', 'check'],
                            optional=True, help="A list of processes to run."
                            " Provide this as a comma separated list"
                            " with no spaces.")
    config.add_config_param('fix', optional=True, help="If this flag is set,"
                            " fix any issues that are found in the check "
                            "stage.", validator='bool', default=False)
    work_dir = validate_config(config, parser)
    # If the --from-scratch flag was set, we supersede other wiping flags.
    if config.from_scratch:
        config.wipe_odmx = True
        config.wipe_feeder = True
        config.wipe_global = True
        config.wipe_ert = True
    # Parse the --data-source-types and --data-processes flags, since
    # argparse is odd about lists. Easiest to do it this way.
    if config.data_source_types is not None:
        config.data_source_types = [
            i.strip() for i in config.data_source_types.split(',')
        ]
    if config.skip_data_source_types is not None:
        config.skip_data_source_types = [
            i.strip() for i in config.skip_data_source_types.split(',')
        ]
    if isinstance(config.data_processes, str):
        config.data_processes = [
            i.strip() for i in config.data_processes.split(',')
        ]

    run_pipeline(config, work_dir)
