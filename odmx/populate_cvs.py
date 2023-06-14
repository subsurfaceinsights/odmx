#!/usr/bin/env python3

"""
Read controlled vocabulary (CV) data from .json files into an ODMX database.
"""

import os
import argparse
import odmx.support.general as ssigen
import odmx.support.db as db
import odmx.support.config as ssiconf
import odmx.data_model as odmx
from odmx.log import vprint
from odmx.json_validation import open_json


UPDATE_ONLY = os.environ.get('UPDATE_ONLY', False)

def populate_cvs(odmx_db_con: db.Connection, project_path: str):
    """
    Populate CVs into an ODMX database.

    @param conf The main config object containing information from a PostgreSQL
                connection yaml file and CL arguments.
    """

    # First off, set paths for use in the script.
    cvs_path = os.path.join(project_path, 'odmx', 'cvs')
    if os.path.exists(cvs_path):
        vprint(f'Found CVs directory at {cvs_path}')
    else:
        vprint(f'There are no custom CVs for this project')
        return
    # Find all of the CV .json files.
    cv_files, cv_paths = ssigen.get_files(cvs_path, 'json')

    # Run through each CV .json file.
    for i, cv in enumerate(cv_files):
        con = odmx_db_con
        # Define the CV name and the path.
        cv_name = os.path.splitext(cv)[0]
        cv_path = cv_paths[i]
        # Open the .json file.
        cv_json = open_json(cv_path)
        # Variables is a special CV case due to different handling of min/max
        # values and other wierd things
        if cv_name == 'variables':
            # Everything but the min and max values go in this table.
            variables_list = [{k: v for k, v in d.items()
                               if k not in ['min_valid_range',
                                            'max_valid_range']}
                              for d in cv_json]
            # Create a list of variables objects.
            objects = [odmx.Variables(**dict_obj) for dict_obj
                       in variables_list]
            # If the variable_id is None, insert seperately.
            objects_no_id = []
            for obj in objects:
                if obj.variable_id is None:
                    objects_no_id.append(obj)
            # Remove the objects with no ID from the list.
            objects = [obj for obj in objects if obj.variable_id is not None]
            # Pass the list to the proper create service function.
            # Insert the objects without ID
            if objects_no_id:
                # We have some annoying special handling. The variable_term
                # must be unique, so we need to check if the variable_term
                # already exists in the table. TODO proper upsert on non
                # primary key fields
                new_objects = []
                for obj in objects_no_id:
                    variable = odmx.read_variables_one_or_none(
                            con,
                            variable_term=obj.variable_term)
                    if variable is not None:
                        obj.variable_id = variable.variable_id
                        objects.append(obj)
                    else:
                        new_objects.append(obj)
                # Insert any new objects
                if new_objects:
                    if UPDATE_ONLY:
                        raise ValueError('UPDATE_ONLY is set, but there are '
                                         f'new objects to insert into {cv_name}')
                    num_inserted = odmx.write_variables_many(con, new_objects)
                    vprint(f'Inserted {num_inserted} new rows into {cv_name}.')
            # Insert the objects with ID
            if objects:
                num_inserted = odmx.write_variables_many(con, objects, upsert=True)
                vprint(f'Inserted/Updated {num_inserted} rows with ID into {cv_name}.')


            # Then split out min/max into its own set of objects.
            # This table has the term, ID, min, and max.
            min_max_list = [{k: v for k, v in d.items()
                             if k in ['variable_term', 'min_valid_range',
                                      'max_valid_range']}
                            for d in cv_json]
            for min_max in min_max_list:
                variable = odmx.read_variables_one(
                        con,
                        variable_term=min_max['variable_term'])
                variable_id = variable.variable_id
                min_max['variable_id'] = variable_id
            # Create a list of min/max objects.
            # Make sure min/max are floats to avoid beartype wrath
            for obj in min_max_list:
                obj['min_valid_range'] = float(obj['min_valid_range'])
                obj['max_valid_range'] = float(obj['max_valid_range'])
            objects = [odmx.VariableQaMinMax(**dict_obj)
                       for dict_obj in min_max_list]
            # for an upsert find the existing objects and add the primary key
            # to the new objects
            objects_no_id = []
            objects_with_id = []
            for obj in objects:
                if obj.variable_qa_min_max_id is None:
                    # See if we have an entry already
                    existing = odmx.read_variable_qa_min_max_one_or_none(
                            con, variable_id=obj.variable_id)
                    if existing is not None:
                        obj.variable_qa_min_max_id = existing.variable_qa_min_max_id
                        objects_with_id.append(obj)
                    else:
                        objects_no_id.append(obj)
                else:
                    objects_with_id.append(obj)
            if objects_no_id:
                num_inserted = odmx.write_variable_qa_min_max_many(con, objects_no_id)
                vprint(f'Inserted {num_inserted} new rows into variable_qa_min_max.')
            if objects_with_id:
                num_inserted = odmx.write_variable_qa_min_max_many(con, objects_with_id, upsert=True)
                vprint(f'Inserted/Updated {num_inserted} rows with ID into variable_qa_min_max.')

        else:
            TableClass = odmx.get_table_class(cv_name)
            assert TableClass is not None, f'No table class found for {cv_name}'
            # Create a list of objects from the .json file.
            objects = [TableClass(**dict_obj)
                       for dict_obj in cv_json]
            objects_no_id = []
            for obj in objects:
                if getattr(obj, TableClass.PRIMARY_KEY) is None:
                    objects_no_id.append(obj)
            # Remove the objects with no ID from the list.
            objects = [obj for obj in objects if getattr(obj, TableClass.PRIMARY_KEY) is not None]
            if objects:
                num_inserted = TableClass.write_many(con, objects, upsert=True)
                vprint(f'Inserted/Updated {num_inserted} rows with IDs into {cv_name}.')
            if objects_no_id:
                if UPDATE_ONLY:
                    raise ValueError('UPDATE_ONLY is set, but there are '
                                     f'new objects to insert into {cv_name}')
                num_inserted = TableClass.write_many(con, objects_no_id)
                vprint(f'Inserted {num_inserted} rows without IDs into {cv_name}.')


