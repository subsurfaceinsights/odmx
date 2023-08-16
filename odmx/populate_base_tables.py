#!/usr/bin/env python3

"""
Read base project data from .json files into an ODMX database.
"""

import os
import argparse
import deepdiff
import odmx.support.config as ssiconf
from odmx.support import db
import odmx.data_model as odmx
import odmx.support.general as ssigen
from odmx.log import vprint
from odmx.json_validation import open_json

def populate_base_tables(odmx_db_con: db.Connection,
                         global_path: str,
                         project_path: str):
    """
    Populate base tables into an ODMX database.

    @param conf The main config object containing information from a PostgreSQL
                connection yaml file and CL arguments.
    """

    project_odmx_path = os.path.join(project_path, 'odmx')
    global_odmx_path = os.path.join(global_path, 'odmx')
    project_it_path = os.path.join(project_odmx_path, 'ingestion_tables')
    global_it_path = os.path.join(global_odmx_path, 'ingestion_tables')
    with db.schema_scope(odmx_db_con, 'odmx'):
        # TODO For now, some of this needs to be hard-coded, because we've not
        # yet addressed how we deal with persons, affiliations, organizations,
        # and equipment_models. Those are meant to be global tables, and
        # although they currently live in the "global" project, they're meant
        # to be higher-level than that. But they're "localized" for now, and
        # need to be ingested in a certain order (also need to decide how
        #  they're meant to link together).
        # "global" ingestion tables order.
        global_it_files, global_it_paths = ssigen.get_files(global_it_path,
                                                            'json')
        global_it_order = ['organizations.json', 'persons.json',
                           'affiliations.json', 'equipment_models.json']
        # Same deal for the project specific files. Some need to be ingested
        # before others.
        project_it_files, project_it_paths = ssigen.get_files(project_it_path,
                                                              'json')
        # Begin with a dedicated starting order, regardless of whether that
        # file exists.
        starting_order = ['features_of_interest.json',
                          'extension_properties.json',
                          'sampling_features.json']
        # Pare down the starting order based on if those files exist.
        starting_order = [i for i in starting_order if i in project_it_files]
        # Now add in the difference of the project files.
        project_it_order = starting_order + list(set(project_it_files)
                                                 - set(starting_order))
        # Combine global and project-specific.
        it_order = global_it_order + project_it_order
        it_files = global_it_files + project_it_files
        it_paths = global_it_paths + project_it_paths

        # Create a mapping dictionary for specialized ingestion tables and
        # which function ingests them.
        ingestion_mapping_dict = {
            'extension_properties': ingest_extension_properties,
            'equipment_models': ingest_equipment_models,
            'variable_mapping': ingest_variable_mapping,
            'sampling_features': ingest_sampling_features
        }

        # Run through each ingestion table .json file.
        for in_table in it_order:
            # Define the ingestion table name and the path.
            it_name = os.path.splitext(in_table)[0]
            it_path = it_paths[it_files.index(in_table)]
            print(it_name)
            # Open the .json file.
            it_json = open_json(it_path)
            # TODO A bit more hardcoding, since we're not 100% settled on:
            # - Why organization IDs are hardcoded in `organizations`.
            # - `equipment_models` has the column `manufacturer_name` which
            #    needs to be converted to
            #    `model_manufacturer_id` (`organization_id`).
            with odmx_db_con.transaction():
                if it_name in ingestion_mapping_dict:
                    ingestion_mapping_dict[it_name](odmx_db_con,
                                                    it_name, it_json)
                else:
                    ingest_generic_table(odmx_db_con, it_name, it_json)

# unique columns that are not in the db TODO add this to the model
special_unique_columns = {
    'extension_properties': ['property_name'],
    # TODO this is supposed to correspond to equipment_code I guess?
    'equipment_models': ['equipment_model_part_number'],
    'features_of_interest': ['features_of_interest_name'],
    'variable_mapping': ['project_variable_name'],
    'specimen_collection': ['specimen_collection_name'],
}

UPDATE_ONLY = os.environ.get('UPDATE_ONLY', False)

def ingest_generic_table(con, it_name, it_json):
    """
    Ingest a JSON object.

    @param create The ODMX API createService object.
    @param it_name The name of the ingestion table.
    @param it_json The ingestion table from the appropriate .json file.
    """

    # Create a list of objects from the .json file.
    table_type = odmx.get_table_class(it_name)
    if not table_type:
        raise ValueError(f'No table class found for {it_name}')
    count = db.get_table_count(con, it_name)
    unique_columns = []
    if count > 0:
        # Get unique columns for this table
        unique_columns = db.table_get_unique_columns(con, it_name)
        # Add unique columns not captured by the schema
        if it_name in special_unique_columns:
            unique_columns += special_unique_columns[it_name]
    vprint(f'Ingesting {it_name} with unique columns: {unique_columns}')
    # Try to parse any date time columns
    objects = \
        [table_type.create_from_json_dict(dict_obj) for dict_obj in it_json]
    objects_without_ids = \
        [obj for obj in objects if getattr(obj,
                                           table_type.PRIMARY_KEY) is None]
    objects_with_ids = \
        [obj for obj in objects if getattr(obj,
                                           table_type.PRIMARY_KEY) is not None]
    # For each entry without a primary key id, check if it exists in the
    # database already. If it does, update the object with the existing id.
    # If it doesn't, add it to the list of objects to be inserted as new rows.
    if objects_without_ids:
        new_objects = []
        for obj in objects_without_ids:
            if not unique_columns:
                new_objects.append(obj)
                continue
            for col in unique_columns:
                kwargs = {col: getattr(obj, col)}
                existing = table_type.read_one_or_none(con, **kwargs)
                if existing:
                    setattr(obj, table_type.PRIMARY_KEY,
                            getattr(existing, table_type.PRIMARY_KEY))
                    objects_with_ids.append(obj)
                else:
                    new_objects.append(obj)
        if new_objects:
            if UPDATE_ONLY:
                raise ValueError((f'Found {len(new_objects)} new rows to '
                                  f'insert into {it_name} but UPDATE_ONLY is '
                                  'set'))
            num_inserted = table_type.write_many(con, new_objects)
            vprint(f'Inserted {num_inserted} new rows into {it_name}')
    if objects_with_ids:
        num_inserted = table_type.write_many(con, objects_with_ids,
                                             upsert=True)
        vprint(f'Inserted/Updated {num_inserted} rows with IDs into {it_name}')

def ingest_extension_properties(con, it_name, it_json):
    """
    Ingest the extension properties .json file.

    @param read The ODMX API readService object.
    @param create The ODMX API createService object.
    @param it_name The name of the ingestion table.
    @param it_json The ingestion table from the extension properties .json
                   file.
    """

    # We need to look up the `property_units_id` based on the
    # `property_units_term`.
    ep_list = it_json.copy()
    for extension_property in ep_list:
        property_units_term = extension_property['property_units_term']
        if property_units_term:
            units = odmx.read_cv_units_one_or_none(con,
                                                   term=property_units_term)
            if units is None:
                raise ValueError(f'Could not find units for '
                                 f'property_units_term: {property_units_term}')
            units_id = units.units_id
            extension_property['property_units_id'] = units_id
        else:
            extension_property['property_units_id'] = None
        extension_property.pop('property_units_term')
    # Ingest the table.
    ingest_generic_table(con, it_name, ep_list)


def ingest_equipment_models(con, it_name, it_json):
    """
    Ingest the equipment models .json file.

    @param read The ODMX API readService object.
    @param create The ODMX API createService object.
    @param it_name The name of the ingestion table.
    @param it_json The ingestion table from the equipment models .json file.
    """

    # We need to look up the `equipment_model_manufacturer_id` based on
    # the `equipment_manufacturer_name`.
    em_list = it_json.copy()
    for eq_model in em_list:
        org = odmx.read_organizations_one_or_none(
            con, organization_name=eq_model['equipment_manufacturer_name']
        )
        if org is None:
            org = odmx.read_organizations_one_or_none(
                con, organization_code=eq_model['equipment_manufacturer_name']
            )
        if org is None:
            raise KeyError("No organization found in the database matching"
                           f" \"{eq_model['equipment_manufacturer_name']}\"")
        org_id = org.organization_id
        eq_model['equipment_model_manufacturer_id'] = org_id
        eq_model.pop('equipment_manufacturer_name')
    # Ingest the table.
    ingest_generic_table(con, it_name, em_list)


def ingest_variable_mapping(con, it_name, it_json):
    """
    Ingest the variable mapping .json file.

    @param read The ODMX API readService object.
    @param create The ODMX API createService object.
    @param it_name The name of the ingestion table.
    @param it_json The ingestion table from the variable mapping .json file.
    """

    # We need to look up the `varibale_id` based on the `odmx_variable_term`.
    mapping_list = it_json.copy()
    for map_dict in mapping_list:
        var = odmx.read_variables_one_or_none(con,
            variable_term=map_dict['odmx_variable_term'])
        var_id = var.variable_id if var else None
        map_dict['variable_id'] = var_id
        map_dict.pop('odmx_variable_term')
    ingest_generic_table(con, it_name, mapping_list)


def ingest_sampling_features(con, it_name, it_json, check_consistency=True):
    """
    Ingest sampling features into ODMX via preorder tree traversal.

    @param con_maker The SQLAlchemy con maker object.
    @param create The ODMX API createService object.
    @param it_name The name of the ingestion table.
    @param it_json The ingestion table from the sampling features .json file.
    """
    if check_consistency:
        sampling_features_by_code = {}
        sampling_features_by_uuid = {}
        stack = it_json.copy()
        while sf_item := stack.pop() if len(stack) != 0 else None:
            code = sf_item['sampling_feature_code']
            uuid = sf_item['sampling_feature_uuid']
            if code in sampling_features_by_code:
                first = sampling_features_by_code[code]
                second = sf_item
                diff = deepdiff.DeepDiff(first, second)
                if diff:
                    raise ValueError(f'Different sampling features with the '
                                     f'same code: {code}\n{diff}')
                raise ValueError(('Duplicate sampling feature entries under '
                                  f'code: {code}'))
            if uuid in sampling_features_by_uuid:
                first = sampling_features_by_uuid[uuid]
                second = sf_item
                diff = deepdiff.DeepDiff(first, second)
                if diff:
                    raise ValueError(f'Different sampling features with the '
                                     f'same uuid: {uuid}\n{diff}')
                raise ValueError(('Duplicate sampling feature entries under '
                                  f'uuid: {uuid}'))
            sampling_features_by_code[code] = sf_item
            sampling_features_by_uuid[uuid] = sf_item
            if sf_item['child_sampling_features']:
                stack.extend(sf_item['child_sampling_features'])
    stack = it_json.copy()
    sf_item = stack.pop()
    while sf_item:
        current_parent = sf_item.get('parent')
        entry = odmx.read_sampling_features_one_or_none(con,
            sampling_feature_uuid=sf_item['sampling_feature_uuid'])
        if entry:
            vprint("Updating sampling feature "
                   f"{sf_item['sampling_feature_name']} to "
                   f"{entry.sampling_feature_name}")
            sf_id = entry.sampling_feature_id
        # If the sampling feature entry doesn't exist, we write it.
        else:
            sf_id = None
        vprint(('Inserting sampling '
                f'feature {sf_item["sampling_feature_code"]}'))
        sf_id = odmx.write_sampling_features(con,
            sampling_feature_id=sf_id,
            sampling_feature_uuid=sf_item['sampling_feature_uuid'],
            sampling_feature_type_cv=sf_item['sampling_feature_type_cv'],
            sampling_feature_code=sf_item['sampling_feature_code'],
            sampling_feature_name=sf_item['sampling_feature_name'],
            sampling_feature_description=\
                sf_item['sampling_feature_description'],
            sampling_feature_geotype_cv=\
                sf_item['sampling_feature_geotype_cv'],
            feature_geometry=sf_item['feature_geometry_wkt'],
            feature_geometry_wkt=sf_item['feature_geometry_wkt'],
            elevation_m=sf_item['elevation_m'],
            elevation_datum_cv=sf_item['elevation_datum_cv'],
            latitude=sf_item['latitude'],
            longitude=sf_item['longitude'],
            epsg=sf_item['epsg'])
        sf_item['sampling_feature_id'] = sf_id

        # Now add data into the `sampling_features_aliases` table if
        # needed.
        if sf_item['sampling_feature_alias'] is not None:
            for alias_entry in sf_item['sampling_feature_alias']:
                alias = alias_entry['alias']
                alias_category = alias_entry['alias_category']
                display_priority = alias_entry['display_priority']
                existing_alias = \
                    odmx.read_sampling_features_aliases_one_or_none(
                    con,
                    alias_category=alias_category,
                    sampling_feature_id=sf_id
                )
                existing_alias_id = None
                if existing_alias:
                    vprint(f"Updating alias {existing_alias.alias} to "
                           f"{alias}")
                    existing_alias_id = \
                        existing_alias.sampling_features_aliases_id
                odmx.write_sampling_features_aliases(
                    con,
                    sampling_features_aliases_id=existing_alias_id,
                    alias=alias,
                    alias_category=alias_category,
                    sampling_feature_id=sf_id,
                    display_priority=display_priority
                )
        # Now add data into the `related_features` table if needed.
        if current_parent:
            parent_sf_id = current_parent['sampling_feature_id']
            relation_to_parent = sf_item['relation_to_parent']
            if relation_to_parent is None:
                raise ValueError(
                        f"Sampling feature {sf_item['sampling_feature_code']} "
                        f"has parent {current_parent['sampling_feature_code']}"
                        " but no relation_to_parent value.")
            existing_related_feature = odmx.read_related_features_one_or_none(
                    con,
                    sampling_feature_id=sf_id,
                    relationship_type_cv=relation_to_parent,
                    related_feature_id=parent_sf_id
            )
            existing_relation_id = None
            if existing_related_feature:
                existing_relation_id = existing_related_feature.relation_id
            odmx.write_related_features(
                con,
                relation_id=existing_relation_id,
                sampling_feature_id=sf_id,
                relationship_type_cv=relation_to_parent,
                related_feature_id=parent_sf_id,
                spatial_offset_id=None
            )
        if 'related_features' in sf_item:
            for related_feature in sf_item['related_features']:
                relation_to_related_feature = \
                    related_feature['relation_to_related_feature']
                related_feature_id = related_feature['related_feature_id']
                existing_related_feature = \
                    odmx.read_related_features_one_or_none(
                        con,
                        sampling_feature_id=sf_id,
                        relationship_type_cv=relation_to_related_feature,
                        related_feature_id=related_feature_id
                )
                existing_relation_id = None
                if existing_related_feature:
                    existing_relation_id = existing_related_feature.relation_id
                odmx.write_related_features(
                    con,
                    relation_id=existing_relation_id,
                    sampling_feature_id=sf_id,
                    relationship_type_cv=relation_to_related_feature,
                    related_feature_id=related_feature_id,
                    spatial_offset_id=None
                )

        # Now add data into the
        # `sampling_feature_extension_property_values` table if needed.
        # Does this sampling feature have extension properties?
        if ('extension_properties' in sf_item
                and sf_item['extension_properties'] is not None):
            # If it does, run through them.
            for extension_property in sf_item['extension_properties']:
                    # Start by making sure that the extension property type
                    # exists in the database already.
                ext_property = odmx.read_extension_properties_one_or_none(
                    con,
                    property_name=extension_property['property_name']
                )
                property_id = None
                if ext_property:
                    property_id = ext_property.property_id
                else:
                    raise ValueError(
                        "The extension property"
                        f" {extension_property['property_name']} is"
                        " not present in the system. Please"
                        " investigate."
                    )
                bridge = odmx.read_sampling_feature_extension_property_values_one_or_none(  # pylint: disable=line-too-long
                    con,
                    sampling_feature_id=sf_id,
                    property_id=property_id
                )
                bridge_id = None
                if bridge:
                    bridge_id = bridge.bridge_id
                assert property_id is not None
                odmx.write_sampling_feature_extension_property_values(
                    con,
                    bridge_id=bridge_id,
                    sampling_feature_id=sf_id,
                    property_id=property_id,
                    property_value=extension_property['property_value']
                )
        # If there are "children", we process them next.
        if ('child_sampling_features' in sf_item
                and sf_item['child_sampling_features'] is not None):
            for child_sf in sf_item['child_sampling_features']:
                child_sf['parent'] = sf_item
            stack.extend(sf_item['child_sampling_features'])
            # Save `current_sf` on the stack.
        # Now we process the next item in the stack.
        if len(stack) > 0:
            sf_item = stack.pop()
        else:
            sf_item = None
        db.adjust_autoincrement_cols(con, 'sampling_features')
        db.adjust_autoincrement_cols(con, 'sampling_feature_extension_property_values')  # pylint: disable=line-too-long
        db.adjust_autoincrement_cols(con, 'related_features')
        db.adjust_autoincrement_cols(con, 'sampling_features_aliases')


# Run the main function in the script.
if __name__ == '__main__':
    # Define the config object.
    config = ssiconf.Config()
    # Set up the argparser, and add parameters to the config object.
    parser = argparse.ArgumentParser(description="Ingest base tables into an"
                                     " ODMX database.")
    config.add_config_param('project_name', help="The name of the project to"
                            " work with. E.g., \"testing\".")
    config.add_config_param('db_user', optional=True, help="Optionally provide"
                            " a user for the ODMX PostgreSQL database.")
    config.add_config_param('db_pass', optional=True, help="Optionally provide"
                            " a password for the ODMX PostgreSQL database.")
    config.add_config_param('db_name', optional=True, help="Optionally provide"
                            " a name for the ODMX PostgreSQL database.")
    config.add_config_param('db_host', validator='hostname', optional=True,
                            help="Optionally provide a host for the ODMX"
                            " PostgreSQL database.")
    config.add_config_param('db_port', validator='port', optional=True,
                            help="Optionally provide a port for the ODMX"
                            " PostgreSQL database.")
    config.add_config_param('db_type', optional=True, help="Optionally provide"
                            " a type for the ODMX PostgreSQL database.")
    config.add_config_param('projects_path', optional=True, help="Path to the"
                            " projects path containing JSON files.",
                            default='/opt/ssi/projects')
    config.add_config_param('pg_path', optional=True, help="Path to the"
                            " PostgreSQL config file.",
                            default='/opt/ssi/config/odmx/pgsql_config.yml')
    # Add the config parameters from the CL to the parser.
    config.add_args_to_argparser(parser)
    # Add the PostgreSQL config file.
    config.add_yaml_file(config.pg_path)
    # Validate the config object.
    config.validate_config(parser.parse_args())

    populate_base_tables(config)
