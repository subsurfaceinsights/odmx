#!/usr/bin/env python3

"""
A script to house common timeseries processing functions.
"""

import os
import uuid
from calendar import timegm
import datetime
import json
from typing import Optional
import pytz
import numpy as np
import pandas as pd
import odmx.support.qaqc as ssiqa
from odmx.support import db
from odmx.support.db import quote_id as qid
from odmx.support.db import Connection
from odmx.log import vprint
from odmx.json_validation import open_json
from odmx.abstract_data_source import DataSource
import odmx.data_model as odmx


def general_timeseries_processing(ds: DataSource,
                                  feeder_db_con: Connection,
                                  odmx_db_con: Connection,
                                  project_path: Optional[str] = None,
                                  data_source_timezone: Optional[str] = None,
                                  feeder_table: Optional[str] = None,
                                  sampling_feature_code: Optional[str] = None,
                                  equipment_directory: Optional[str] = None
                                  ):

    """
    Perform the general processing routine for timeseries data.

    @param obj The class object for the data type in question.
    """

    def _check_attr(attr_name: str):
        if not hasattr(ds, attr_name):
            raise ValueError(
                f"No {attr_name} was provided and no {attr_name} was "
                f"found in the data source object."
            )
        return getattr(ds, attr_name)

    # Fill our required parameters
    if not project_path:
        project_path = _check_attr('project_path')
        assert project_path is not None
    if not data_source_timezone:
        data_source_timezone = _check_attr('data_source_timezone')
        assert data_source_timezone is not None
    if not feeder_table:
        feeder_table = _check_attr('feeder_table')
        assert feeder_table is not None
    if not sampling_feature_code:
        sampling_feature_code = _check_attr('sampling_feature_code')
        assert sampling_feature_code is not None
    if not equipment_directory:
        equipment_directory = _check_attr('equipment_directory')
        assert equipment_directory is not None

    sf = odmx.read_sampling_features_one(
        odmx_db_con,
        sampling_feature_code=sampling_feature_code
    )
    sf_id = sf.sampling_feature_id if sf else None

    # Find TOC Property ID for calculated channels
    toc_relative_to_ground_id = None
    toc_relative_to_ground_prop = odmx.read_extension_properties_one_or_none(
        odmx_db_con,
        property_name = 'topOfCasingRelativeToGround'
    )
    if toc_relative_to_ground_prop:
        toc_relative_to_ground_id = toc_relative_to_ground_prop.property_id

    # Before we start the data processing, grab the equipment models to use
    # throughout the process.
    equipment_models_list = odmx.read_equipment_models_all(odmx_db_con)
    equipment_models_df = pd.DataFrame(equipment_models_list)

    # Now actually process the data source.
    print("Beginning processing of the data sources.")
    # Print some initial info about the data source in question.
    # Before we do anything else, we ingest the equipment for this data source.
    # Open and validate the appropriate equipment .json file.
    if equipment_directory.startswith('/'):
        eqp_dir_path = equipment_directory
    else:
        eqp_dir_path = os.path.join(project_path, 'odmx', 'equipment',
                                equipment_directory)
    equipment_path = os.path.join(eqp_dir_path, 'equipment.json')
    vprint(f"Opening the equipment file: {equipment_path}")
    equipment_json = open_json(equipment_path)
    # Ingest the equipment.
    ingest_equipment(odmx_db_con, equipment_json,
                     equipment_models_df, sf_id)


    # Check to make sure pertinent tables exist.
    feeder_table_exists = db.does_table_exist(
        feeder_db_con,
        feeder_table,
    )
    if not feeder_table_exists:
        raise ValueError(
            f"Feeder table {feeder_table} does not exist."
        )

    # Open the data to equipment mapping .json file.
    vprint("Opening the data to equipment mapping .json file.")
    d2e_path = os.path.join(eqp_dir_path, 'data_to_equipment_map.json')
    d2e_json = open_json(d2e_path)
    d2e_df = pd.DataFrame(d2e_json)
    # Cut the DataFrame down only to rows we want to expose, where
    # `expose_as_datastream` is True.
    d2e_df = d2e_df.query('expose_as_datastream')
    assert d2e_df is not None
    if d2e_df.empty:
        print("WARNING: No data is set to be exposed for the current data "
            f"source: {feeder_table}")
    # Convert back to a list of dictionaries, and run through the entries.
    d2e_list = d2e_df.to_dict(orient='records')
    for entry in d2e_list:
        d2e_column_name = entry['column_name']
        d2e_acquiring_instrument_uuid = entry['acquiring_instrument_uuid']
        d2e_variable_term = entry['variable_term']
        d2e_units_term = entry['units_term']
        d2e_unit_conversion = entry['unit_conversion']
        d2e_sensor_type = entry['sensor_type']
        d2e_ds_manual_qa_list = None
        if 'datastream_manual_qa_list' in entry:
            d2e_ds_manual_qa_list = (
                entry['datastream_manual_qa_list'])

        print(f"Working on column: {d2e_column_name}")
        # Look up the variable and units ID.
        # TODO wouldn't it be better to not have an unknown map?
        if d2e_variable_term is None:
            d2e_variable_term = 'unknown'
        d2e_variable = odmx.read_variables_one(
            odmx_db_con,
            variable_term = d2e_variable_term)
        if not d2e_variable:
            raise ValueError(
                f"Could not find variable term {d2e_variable_term} as "
                f"defined in {d2e_path}")
        d2e_variable_id = d2e_variable.variable_id
        if d2e_units_term is None:
            d2e_units_term = 'unknown'
        d2e_units = odmx.read_cv_units_one(
                odmx_db_con,
                term = d2e_units_term)
        if not d2e_units:
            raise ValueError(
                f"Could not find units term {d2e_units_term} as "
                f"defined in {d2e_path}")
        d2e_units_id = d2e_units.units_id
        assert d2e_units_id is not None

        # Define the view.
        vprint(f"{d2e_column_name} should be exposed as a view. Continuing.")
        mat_view_name = f"{feeder_table}_{d2e_column_name}"
        # At this point we need to find out how many bytes the materialized
        # view's name is. PostgreSQL has a 63 byte limit for table names, and
        # we want to add 10 bytes to the name shortly, so here it can be no
        # more than 53 bytes.
        byte_len = len(mat_view_name.encode('utf-8'))
        while (byte_len > 53 or mat_view_name.endswith(' ')
               or mat_view_name.endswith('_')):
            mat_view_name = mat_view_name[:-1]
            byte_len = len(mat_view_name.encode('utf-8'))
        if d2e_sensor_type == 'measured':
            mat_view_name = f'{mat_view_name}_meas'
        elif d2e_sensor_type == 'calculated':
            mat_view_name = f'{mat_view_name}_calc'
        view_name = f"{mat_view_name}_view"
        vprint(f"Working with view: {view_name}")

        # Before we define the view, find the units it should use.
        unit = odmx.read_cv_units_by_id(odmx_db_con, d2e_units_id)
        assert unit, f"Could not find units with ID {d2e_units_id}"
        if d2e_unit_conversion:
            quantity_kind_cv = unit.quantity_kind_cv
            quantity_kind = odmx.read_cv_quantity_kind_one(
                odmx_db_con, term=quantity_kind_cv)
            assert quantity_kind, ("Could not find quantity "
                                   f"kind {quantity_kind_cv}")
            default_unit = quantity_kind.default_unit
            assert default_unit, ("Could not find default unit for quantity "
                                  f"kind {quantity_kind_cv}")
            d2e_units = odmx.read_cv_units_one(
                odmx_db_con, term=default_unit)
            assert d2e_units, f"Could not find default unit {default_unit}"
            d2e_units_id = d2e_units.units_id

        # If the feeder db is foreign, we need to pull the data down
        # from the foreign db.
        if (odmx_db_con.info.host != feeder_db_con.info.host or
            odmx_db_con.info.dbname != feeder_db_con.info.dbname or
            odmx_db_con.info.port != feeder_db_con.info.port):
            vprint("Feeder db is foreign. Pulling data from foreign db.")
            # Create the table if necessary.
            with (db.schema_scope(odmx_db_con, 'feeder'),
                  db.schema_scope(feeder_db_con, 'feeder')):
                count = db.cross_con_table_copy(feeder_db_con,
                                                feeder_table,
                                                odmx_db_con,
                                            feeder_table)
                if count:
                    vprint(f"Inserted {count} rows into {feeder_table}")
                else:
                    vprint(f"Table {feeder_table} already updated.")

        # Create the view if necessary.
        create_view(
            odmx_db_con,
            feeder_table, view_name, unit,
            d2e_column_name, d2e_unit_conversion
        )
        # Create/update the materialized view.
        view_df = materialize(odmx_db_con, mat_view_name, view_name,
                              data_source_timezone,
                              d2e_variable_id, d2e_units_id,
                              d2e_ds_manual_qa_list)
        if not view_df.empty:
            create_datastream_entry(
                odmx_db_con, mat_view_name, view_df, sf_id,
                d2e_acquiring_instrument_uuid, d2e_variable_id, d2e_units_id
            )

        # This bit is hardcoded for the two calculated datastreams we currently
        # do for LBNL.
        # TODO support hooks from the actual data source class
        if view_name == 'm1_10_at200_m1_wl_avg_meas_view':
            m1_10_calc_datastream(
                odmx_db_con,
                data_source_timezone,
                mat_view_name,
                sf_id, d2e_acquiring_instrument_uuid
            )
        elif view_name == 'm6_60_at200_m6_wl_avg_meas_view':
            m6_60_calc_datastream(
                odmx_db_con,
                data_source_timezone,
                mat_view_name,
                sf_id, d2e_acquiring_instrument_uuid
            )
        if d2e_variable_term == 'waterDepthBelowTopOfCasing':
            # Check if we have a TOC extension property to trigger a
            # calculated datastream.
            extension_property_values = \
                odmx.read_sampling_feature_extension_property_values_all(
                    odmx_db_con, sampling_feature_id=sf_id)
            toc_value = None
            for ext_prop in extension_property_values:
                if ext_prop.property_id == toc_relative_to_ground_id:
                    vprint("Found TOC extension property. Creating calculated "
                           "datastream.")
                    toc_value = float(ext_prop.property_value)
                    vprint(f"TOC value: {toc_value}")
                    # Note: We don't need the ds_timezone because we are using
                    # the already converted mat_view_name and not just
                    # view_name which is unconverted
                    generic_toc_calc_datastream(
                        odmx_db_con,
                        mat_view_name,
                        sf,
                        d2e_acquiring_instrument_uuid,
                        toc_value
                    )
            if toc_value is None:
                msg = ("No TOC extension property found, but found a "
                       "waterDepthBelowTopOfCasing datastream on "
                       f"{sf.sampling_feature_code}. ")
                vprint(msg)
                raise ValueError(msg)
    print("Materialized view creation complete.\n")

def ingest_equipment(con, equipment_json,
                     equipment_models_df, sampling_feature_id):
    """
    Ingest equipment into ODMX via preorder tree traversal.

    @param read The ODMX read service object.
    @param create The ODMX create service object.
    @param session_maker THe project's db session maker
    @param equipment_json A list that comes from the equipment .json file.
    @param equipment_models_df A DataFrame of the `equipment_models` table.
    @param sampling_feature_id The sampling feature ID for this equipment.
    """

    # Start by defining the two equipment stacks we'll traverse.
    equipment_stack = [equipment_json.copy()]
    parents_stack = [None]
    while equipment_stack:
        # Get the top item to process on the stack. The first time this
        # will be the entire list.
        current_equipment = equipment_stack.pop()
        current_parent = parents_stack.pop()
        # Now while we have things in the list.
        while current_equipment:
            # Get the first item and remove it.
            equipment_item = current_equipment.pop(0)
            vprint(f"Ingesting equipment: {equipment_item['equipment_code']}.")
            # First, add data into the `equipment` table.
            # TODO it's not obvious that these are supposed to be correlated
            equipment_model_id = equipment_models_df.query(
                'equipment_model_part_number'
                f' == "{equipment_item["equipment_code"]}"'
            )
            try:
                equipment_model_id =\
                        equipment_model_id['equipment_model_id'].item()
            except ValueError as e:
                raise ValueError(
                    f"Equipment code '{equipment_item['equipment_code']}' "
                    "not found in equipment_models table under "
                    "equipment_model_part_number. This is what's available:\n "
                    f"{equipment_models_df['equipment_model_part_number']}")\
                    from e
            if equipment_item['equipment_vendor'] is None:
                equipment_vendor = None
            else:
                equipment_vendor = odmx.read_organizations_one_or_none(
                    con, organization_code=equipment_item['equipment_vendor'])
            if equipment_vendor is None:
                equipment_vendor_id = odmx.read_organizations_one(
                    con, organization_code='unknown').organization_id
            else:
                equipment_vendor_id = equipment_vendor.organization_id
            equipment_id = None
            existing_equipment = odmx.read_equipment_one_or_none(
                    con,
                    equipment_uuid=equipment_item['equipment_uuid'])
            if existing_equipment is not None:
                equipment_id = existing_equipment.equipment_id
            equipment_id = odmx.write_equipment(
                con,
                equipment_id=equipment_id,
                equipment_uuid=equipment_item['equipment_uuid'],
                equipment_code=equipment_item['equipment_code'],
                equipment_name=equipment_item['equipment_name'],
                equipment_model_id=equipment_model_id,
                equipment_serial_number=\
                    equipment_item['equipment_serial_number'],
                equipment_vendor_id=equipment_vendor_id,
                equipment_purchase_date=\
                    equipment_item['equipment_purchase_date'],
                equipment_purchase_order_number=\
                    equipment_item['equipment_purchase_order_number'],
                equipment_description=equipment_item['equipment_description'],
                equipment_documentation_link=\
                    equipment_item['equipment_documentation_link'])

            # Next, add data into the `equipment_position` table. Check to see
            # if the current equipment already exists in the database with a
            # position.
            positions = odmx.read_equipment_position_all(
                    con,
                    equipment_id = equipment_id,
                    position_start_date_utc=\
                        equipment_item['position_start_date_utc'])
            # If the equipment position already exists, we need to find out
            # if this is a new position, or if it needs updating.
            equipment_position_id = None
            if len(positions) > 0:
                if len(positions) > 1:
                    vprint("Found multiple positions for equipment "
                           f"{equipment_item['equipment_code']} on same date, "
                           "this is unexpected")
                equipment_position_id = positions[0].equipment_position_id
            # Make sure all offsets_m are floats
            for key in ['equipment_z_offset_m', 'equipment_ns_offset_m',
                        'equipment_ew_offset_m']:
                if equipment_item[key] is not None:
                    equipment_item[key] = float(equipment_item[key])
            equipment_position_id = odmx.write_equipment_position(
                con,
                equipment_position_id=equipment_position_id,
                equipment_id=equipment_id,
                position_start_date_utc=\
                    equipment_item['position_start_date_utc'],
                position_end_date_utc=equipment_item['position_end_date_utc'],
                equipment_z_offset_m=equipment_item['equipment_z_offset_m'],
                equipment_ns_offset_m=equipment_item['equipment_ns_offset_m'],
                equipment_ew_offset_m=equipment_item['equipment_ew_offset_m'],
                equipment_height_note=equipment_item['equipment_height_note']
            )

            # Next, add data into the `equipment_persons_bridge` table. As
            # above, we need to see if a match has already occurred between
            # this equipment entry and a given person.
            person_id = odmx.read_persons_one(
                    con,
                    person_first_name = \
                        equipment_item['equipment_owner_first_name'],
                    person_last_name = \
                        equipment_item['equipment_owner_last_name']
            ).person_id
            assert person_id is not None
            equipment_persons_bridge_id = None
            existing_bridge = odmx.read_equipment_persons_bridge_one_or_none(
                    con,
                    equipment_id = equipment_id,
                    person_id = person_id
                    )
            if existing_bridge:
                equipment_persons_bridge_id = existing_bridge.bridge_id
            odmx.write_equipment_persons_bridge(
                    con,
                    bridge_id = equipment_persons_bridge_id,
                    equipment_id = equipment_id,
                    person_id = person_id,
                    person_role_cv = equipment_item['equipment_owner_role'])
            # Next, add data into the `related_equipment` table if needed.
            if current_parent:
                parent_equipment_id = current_parent['equipment_id']
                # Check to see if the equipment already has an entry in the
                # `related_equipment` table.
                relation_id = None
                related_equipment = odmx.read_related_equipment_one_or_none(
                    con,
                    equipment_id = equipment_id,
                    relationship_type_cv = 'isAttachedTo',
                    related_equipment_id = parent_equipment_id,
                    relationship_start_date_time_utc =\
                        equipment_item['relationship_start_date_time_utc']
                )
                if related_equipment:
                    relation_id = related_equipment.relation_id
                odmx.write_related_equipment(
                    con,
                    relation_id=relation_id,
                    equipment_id=equipment_id,
                    relationship_type_cv='isAttachedTo',
                    related_equipment_id=parent_equipment_id,
                    relationship_start_date_time_utc=\
                        equipment_item['relationship_start_date_time_utc'],
                    relationship_end_date_time_utc=\
                        equipment_item['relationship_end_date_time_utc'])

            # Next, add data into the `external_connection` table. Check to see
            # if the equipment already has an entry in the
            # `external_connection` table.
            external_connection_id = None
            external_connections = odmx.read_external_connection_all(
                    con,
                    equipment_id = equipment_id,
                    sampling_feature_id = sampling_feature_id
                    )
            for external_connection in external_connections:
                if external_connection.action_id is None:
                    external_connection_id = external_connection.connection_id
            odmx.write_external_connection(
                con,
                connection_id=external_connection_id,
                sampling_feature_id=sampling_feature_id,
                equipment_id=equipment_id,
                action_id=None,
                description=("Link between a piece of equipment and a sampling"
                             " feature")
            )
            # If there are "children", we process them first.
            if equipment_item['equipment'] is not None:
                # Save `current_equipment` on the stack.
                equipment_stack.append(current_equipment)
                # Save `current_parent` on the stack.
                parents_stack.append(current_parent)
                # Set `current_equipment` to its children.
                current_equipment = equipment_item['equipment']
                # Set `current_parent` to the current `equipment_item` and
                # add in the current `equipment_id`.
                current_parent = equipment_item
                current_parent['equipment_id'] = equipment_id
    vprint("Equipment ingestion complete.")


def create_view(odmx_con, feeder_table, view_name, unit,
                d2e_column_name, d2e_unit_conversion):
    """
    Create project database views on feeder database data.

    @param odmx_con The sqlalchemy connection for connecting to the ODMX
                       database.
    @param mat_view_schema The ODMX materialized view schema.
    @param feeder_table The name of the datastream's table in ODMX. This is the
                        "raw" data table.
    @param view_name The name of the view we want to create/check.
    @param unit The ODMX read service return of the unit for this datastream.
    @param d2e_column_name The name of the column for this datastream.
    @param d2e_unit_conversion Whether or not unit conversion happens for this
                               datastream.
    """

    with db.schema_scope(odmx_con, "datastreams"):
        vprint(f"Checking to see if the view {view_name} exists.")
        table_exists = db.does_table_exist(odmx_con, view_name)
        # If the view exits, move on.
        if table_exists:
            vprint("The view exists. Moving on.")
        # If the view doesn't exist, create it.
        else:
            vprint("The view does not exist. Creating it.")
            # Check if the view should include the offset/multiplier unit
            # conversion or not. If so, find the conversion.
            if d2e_unit_conversion:
                multiplier = str(unit.conversion_multiplier)
                offset = str(unit.conversion_offset)
            # If not, multiply by 1 and offset by 0.
            else:
                multiplier = str(1)
                offset = str(0)
            # Create the actual view. This should account for any text "NAN"
            # and such.
            query = f'''
                CREATE VIEW {qid(odmx_con, view_name)}
                AS SELECT timestamp AS utc_time, ({multiplier} * ({offset}
                    + {qid(odmx_con, d2e_column_name)}
                    ::double precision))
                AS data_value FROM
                    feeder.{qid(odmx_con, feeder_table)}
                ORDER BY timestamp
            '''
            try:
                odmx_con.execute(query)
            except ValueError as exception:
                with db.schema_scope(odmx_con, "odmx"):
                    columns = db.get_columns(odmx_con,
                            feeder_table)
                columns_str = '\t' + ('\n\t'.join(columns)) + '\n'
                raise RuntimeError(
                    f"The column `{d2e_column_name}` does not exist "
                    f"in the feeder db table: `{feeder_table}`."
                    "\nThis is typically a problem with the "
                    "sensor map definition. The following columns "
                    f"exist:\n{columns_str}") from exception
            vprint("View created.")


def materialize(odmx_con, mat_view_name, view_name,
                ds_timezone, d2e_variable_id, d2e_units_id,
                d2e_ds_manual_qa_list):
    """
    Create/update the materialized view.

    @param odmx_con The sqlalchemy connection for connecting to the ODMX
                       database.
    @param mat_view_name The name of the materialized view.
    @param view_name The name of the view.
    @param ds_timezone The timezone of the specific datasource.
    @param d2e_variable_id The ODMX variable ID for this datastream.
    @param d2e_units_id The ODMX units ID for this datastream.
    @param d2e_ds_manual_qa_list The list of manual QA codes and ranges for
            this datastream.
    @return The materialized view DataFrame.
    """

    # Check to see if the materialized view exists or not.
    vprint("Checking to see if the materialized view exists.")
    with db.schema_scope(odmx_con, 'datastreams'):
        last_mat_view_time = \
            create_mat_view_table_or_return_latest(odmx_con, mat_view_name)
        last_mat_view_time = last_mat_view_time or 0
        # The latest materialized view time is UTC in Unix format, so we
        # need to convert it to local time and "normal" format.
        local_tz = pytz.timezone(ds_timezone)
        last_mat_view_time = datetime.datetime.utcfromtimestamp(
            last_mat_view_time
        )
        last_mat_view_time = last_mat_view_time.replace(tzinfo=pytz.utc)
        last_mat_view_time = last_mat_view_time.astimezone(local_tz)
        last_mat_view_time = last_mat_view_time.replace(tzinfo=None)
        query = f'''
            SELECT * FROM
                {qid(odmx_con, view_name)}
            WHERE utc_time > %s
        '''
        result = odmx_con.execute(query, [last_mat_view_time])

        # Since we now have the appropriate data from the view itself, we can
        # turn it into a DataFrame so that we can do QA/QC on it.
        view_df = pd.DataFrame(result.fetchall(), dtype='object')
        # Need to check to make sure that view_df has any entries. If not, it
        # means no new data exists and we can move on.
        if view_df.empty:
            vprint("No new data exists to materialize.")
        else:
            # Add a qa_flag column
            if view_df.shape[1] != 3:
                view_df[2] = 'z'
            view_df.columns = ['utc_time', 'data_value', 'qa_flag']
            # First, do the timezone conversion.
            view_df.sort_values(by='utc_time')
            view_df['utc_time'] = (pd.to_datetime(view_df['utc_time'])
                                   .dt.tz_localize(ds_timezone,
                                                   ambiguous='infer')
                                   .dt.tz_convert('UTC'))
            # Turn it into Unix time, as that's what the database table takes.
            view_df['utc_time'] = view_df['utc_time'].astype(np.int64) // 10**9
            # Now apply any and all QA/QC checks.
            view_df['data_value'] = view_df['data_value'].astype(float)
            view_df = qa_checks(view_df, odmx_con, d2e_variable_id,
                                d2e_units_id, d2e_ds_manual_qa_list)
            # Write the materialized view to the database.
            vprint("Writing the materialized view to the database.")
            num_inserted = db.insert_many_df(
                    odmx_con, mat_view_name, view_df, upsert=False)
            vprint((f"Inserted {num_inserted} rows into "
                    f"datastreams.{mat_view_name}"))
            # The following is the old mechanism
            #check_cols = ['utc_time']
            #write_cols = ['utc_time', 'data_value', 'qa_flag']
            #update_cols = write_cols.copy()

            #db.df_to_sql(view_df, odmx_con, mat_view_schema, mat_view_name,
            #                 check_cols, write_cols, update_cols)
            vprint("Materialized view complete.")

    return view_df

def get_lastest_equipment_position(con,
                            equipment_id) -> Optional[odmx.EquipmentPosition]:
    """ Get latest equipment postiion"""
    positions = odmx.read_equipment_position_all(
            con, equipment_id=equipment_id)
    latest_position = None
    for position in positions:
        if position.position_start_date_utc is None:
            continue
        if latest_position is None:
            latest_position = position
            continue
        if (position.position_start_date_utc >
            latest_position.position_start_date_utc):
            latest_position = position
    return latest_position

def check_datastream_entries(con, fix=True, check_empty=True):
    """
    Does a consistency check on datastream entries to ensure that the entry
    metadata like start date, end date, and total measurement number are
    consistent with the materialized view
    @param con_maker The project con maker
    @param fix Whether to fix entries if there are inconsistencies or just
        report them
    @return True if all checks pass, False otherwise
    """
    passed = True
    result = con.execute(
        "SELECT * FROM sampling_feature_timeseries_datastreams")
    for row in result:
        first_measurement_date = row['first_measurement_date']
        last_measurement_date = row['last_measurement_date']
        if first_measurement_date is not None:
            first_measurement_date = int(timegm(
                first_measurement_date.timetuple()))
        if last_measurement_date is not None:
            last_measurement_date = int(timegm(
                last_measurement_date.timetuple()))
        total_measurement_numbers = row['total_measurement_numbers']
        datastream_id = row['datastream_id']
        datastream_database = row['datastream_database']
        datastream_tablename = row['datastream_tablename']
        datastream_str = f"{datastream_id} ({datastream_tablename})"
        vprint(f"Checking {datastream_str}")
        query = f"""
            SELECT MIN(utc_time) AS first_measurement_date,
                   MAX(utc_time) AS last_measurement_date,
                   COUNT(utc_time) AS total_measurement_numbers
            FROM "{datastream_database}"."{datastream_tablename}"
            WHERE qa_flag = 'z'"""
        result = con.execute(query)
        query = f"""
            SELECT count(*) AS rowcount
            FROM "{datastream_database}"."{datastream_tablename}"
            """
        raw_total = con.execute(query).fetchone()[0]
        assert result.rowcount == 1
        row = result.fetchone()
        if (first_measurement_date != row['first_measurement_date'] or
            last_measurement_date != row['last_measurement_date'] or
                total_measurement_numbers != row['total_measurement_numbers']):
            print(
                f"Datastream {datastream_str} has inconsistent "
                "metadata.\n\t - First measurement date is "
                f"{first_measurement_date}, but the materialized view "
                f"says it should be {row['first_measurement_date']}. "
                f"\n\t - Last measurement date is {last_measurement_date}, "
                "but the materialized view says it should be "
                f"{row['last_measurement_date']}.\n\t - Total measurement "
                f"count is {total_measurement_numbers}, but the "
                "materialized view says it should be "
                f"{row['total_measurement_numbers']}.")
            passed = False
            if fix:
                if row['first_measurement_date']:
                    first_measurement_date_datetime = \
                        datetime.datetime.utcfromtimestamp(
                        row['first_measurement_date'])
                else:
                    first_measurement_date_datetime = None
                if row['last_measurement_date']:
                    last_measurement_date_datetime = \
                    datetime.datetime.utcfromtimestamp(
                        row['last_measurement_date'])
                else:
                    last_measurement_date_datetime = None
                query = """
                    UPDATE sampling_feature_timeseries_datastreams
                    SET first_measurement_date = %s,
                        last_measurement_date = %s,
                        total_measurement_numbers = %s
                    WHERE datastream_id = %s"""
                con.execute(query, [first_measurement_date_datetime,
                                   last_measurement_date_datetime,
                                   row['total_measurement_numbers'],
                                   datastream_id])
        if row['total_measurement_numbers'] == 0 and raw_total >= 10:
            print(
                f"Datastream {datastream_str} has no measurements that "
                f"pass qa/qc out of {raw_total} measurements.")
            units = con.execute(
                """
                SELECT term
                FROM cv_units
                WHERE units_id = (
                    SELECT units_id
                    FROM sampling_feature_timeseries_datastreams
                    WHERE datastream_id = %s)
                """, [datastream_id]).fetchone()[0]
            variable = con.execute(
                """
                SELECT variable_term
                FROM variables
                WHERE
                    variable_id = (
                        SELECT variable_id
                        FROM sampling_feature_timeseries_datastreams
                        WHERE datastream_id = %s)""",
                    [datastream_id]).fetchone()[0]
            variable_min, variable_max = con.execute(
                """
                SELECT
                    min_valid_range, max_valid_range
                FROM
                    variable_qa_min_max
                WHERE variable_term = %s
                """,
                [variable]).fetchone()
            if variable_min is not None and variable_max is not None:
                print(
                    f"\t - The variable is {variable} "
                    f"and the units are {units}, "
                    f"variable range is supposed to be {variable_min} "
                    f"to {variable_max}.")
                last_ten_datapoints = con.execute(
                    f'''
                    SELECT utc_time, data_value, qa_flag
                    FROM "{datastream_database}"."{datastream_tablename}"
                    WHERE data_value is not null
                     ORDER BY utc_time DESC LIMIT 10
                    ''').fetchall()

                no_data = True

                for datapoint in last_ten_datapoints:
                    if no_data:
                        no_data = False
                        print("\t - The last non-null ten datapoints are:")
                    utc_datetime = datetime.datetime.utcfromtimestamp(
                        datapoint[0])
                    print(f"\t\t {utc_datetime} {datapoint[1]} "
                          f"({datapoint[2]})")
                if no_data:
                    print("\t - There are no non-null datapoints.")

            if check_empty:
                passed = False
    return passed

def create_datastream_entry(con, mat_view_name, view_df,
                            sf_id,
                            d2e_acquiring_instrument_uuid, d2e_variable_id,
                            d2e_units_id):
    """
    Add/edit rows in the `sampling_feature_timeseries_datastream` table in
    ODMX.

    @param con The connection object for the db
    @param mat_view_name The name of the materialized view.
    @param view_df The DataFrame containing the materialized view.
    @param sf_id The sampling feature ID associated with the datastream.
    @param d2e_acquiring_instrument_uuid The UUID of the piece of equipment in
                                         question.
    @param d2e_variable_id The ODMX variable ID for this datastream.
    @param d2e_units_id The ODMX units ID for this datastream.
    """
    with db.schema_scope(con, 'odmx'):
        vprint(("Adding/editing the sampling_feature_timeseries_datastreams "
                "entry."))
        # Start by trying to find the current entry in the database table.
        datastream = \
            odmx.read_sampling_feature_timeseries_datastreams_one_or_none(
                con, datastream_tablename=mat_view_name)

        # Get the UUID.
        if datastream is not None:
            datastream_uuid = datastream.datastream_uuid
        else:
            datastream_uuid = str(uuid.uuid4())

        # Get the datastream type.
        if datastream is not None:
            datastream_type = datastream.datastream_type
            first_meas_date = datastream.first_measurement_date
        else:
            datastream_type = 'physicalsensor'
            first_meas_date = None

        # Filter the view df by good data
        if not view_df.empty:
            filtered_df = view_df.loc[view_df['qa_flag'] == 'z', 'utc_time']
        else:
            filtered_df = view_df
        # Get the last measurement date and maybe the first
        if filtered_df.empty:
            # If the view is empty, the last date should be in the datastream
            if datastream is not None:
                last_meas_date = datastream.last_measurement_date
            else:
                last_meas_date = None
        else:
            # If the view isn't empty, the last date should be the max of the
            # filtered list
            last_meas_date = datetime.datetime.utcfromtimestamp(
                filtered_df.max())
            # If first meas is none it means we are creating a new data stream
            # and we need to set the first measurement date
            if first_meas_date is None:
                first_meas_date = datetime.datetime.utcfromtimestamp(
                    filtered_df.min())

        # Get the total number of measurements. Try finding it in the database.
        # If it's there, add it to the length of the view.
        if datastream is not None:
            total_measurement_numbers = \
                (datastream.total_measurement_numbers if
                    datastream.total_measurement_numbers is not None else 0)
            total_measurement_numbers += len(filtered_df)
        # If it isn't in the database, set it to the length of the view.
        else:
            filtered_df = view_df.loc[view_df['qa_flag'] == 'z']
            total_measurement_numbers = len(filtered_df)

        # Construct the `datastream_attribute`. To start, we find the depth
        # offset info. Begin with an offset of zero.
        offset = 0
        # Find the equipment ID based on the UUID.
        equipment_entry = odmx.read_equipment_one(
                con, equipment_uuid=d2e_acquiring_instrument_uuid)
        equipment_id = equipment_entry.equipment_id
        equipment_code = equipment_entry.equipment_code
        # Find the z-offset for this equipment.
        position = get_lastest_equipment_position(con, equipment_id)
        if position is not None:
            offset = position.equipment_z_offset_m
            if offset is None:
                offset = 0
        # Start climbing the related equipment ladder to find any other offsets
        new_equipment_id = equipment_id
        while True:
            # Find a piece of related `isAttachedTo` equipment.
            related_equipment = odmx.read_related_equipment_all(
                    con, equipment_id=new_equipment_id,
                    relationship_type_cv='isAttachedTo')
            if len(related_equipment) == 0:
                break
            latest_start_time = None
            for re in related_equipment:
                if re.relationship_start_date_time_utc is None:
                    continue
                if latest_start_time is None:
                    latest_start_time = re.relationship_start_date_time_utc
                    new_equipment_id = re.related_equipment_id
                if re.relationship_start_date_time_utc > latest_start_time:
                    latest_start_time = re.relationship_start_date_time_utc
                    new_equipment_id = re.related_equipment_id
            # Find the z-offset for this equipment.
            position = get_lastest_equipment_position(con, new_equipment_id)
            if position is not None:
                assert position.equipment_z_offset_m is not None
                offset += position.equipment_z_offset_m
        # Make datastream attribute text human readable on the frontend.
        if offset < 0:
            depth_elevation_text = 'sensor_depth'
            depth_elevation_units_text = 'sensor_depth_units'
            offset *= -1
        else:
            # TODO probably shouldn't just add an offset even when there is no
            # elevation
            depth_elevation_text = 'sensor_elevation'
            depth_elevation_units_text = 'sensor_elevation_units'
        # Finally, write the datastream attribute.
        attribute_dict = {
            depth_elevation_text: offset,
            depth_elevation_units_text: 'meter',
            'instrument': equipment_code,
        }
        datastream_attribute = json.dumps(attribute_dict)

        # Now add/edit a row in sampling_feature_timeseries_datastreams.
        sftsds_name = 'sampling_feature_timeseries_datastreams'
        vprint(f"Adding/editing a row in {sftsds_name}.")
        datastream_id = datastream.datastream_id if datastream else None
        odmx.write_sampling_feature_timeseries_datastreams(
            con,
            datastream_id=datastream_id,
            datastream_uuid=datastream_uuid,
            equipment_id=equipment_id,
            sampling_feature_id=sf_id,
            datastream_type=datastream_type,
            variable_id=d2e_variable_id,
            units_id=d2e_units_id,
            datastream_database='datastreams',
            datastream_tablename=mat_view_name,
            first_measurement_date=first_meas_date,
            last_measurement_date=last_meas_date,
            total_measurement_numbers=total_measurement_numbers,
            datastream_attribute=datastream_attribute
        )
        vprint("sampling_feature_timeseries_datastreams entry complete.")

def create_mat_view_table_or_return_latest(
        con, mat_view_table) -> Optional[datetime.datetime]:
    """
    Check if a materialized view table exists if it does return the latest
    datetime value or None if the table is empty.
    If it doesn't exist, create it and return None
    """
    with db.schema_scope(con, 'datastreams'):
        # If the materialized view table doesn't exist, create it from the
        # template.
        table_exists = db.does_table_exist(con,
                                               mat_view_table)
        mat_view_table_quote = qid(con, mat_view_table)
        latest_utc_time = None
        if not table_exists:
            vprint(f"Creating new mat view table {mat_view_table}")
            query = f'''
                CREATE TABLE
                    {mat_view_table_quote}
                (LIKE "odmx"."timeseries_datastream_template")
            '''
            result = con.execute(query)
        # Write the materialized view to the database.
        else:
            query = f'''
                SELECT MAX(utc_time) FROM {mat_view_table_quote}
            '''
            result = con.execute(query)
            latest_utc_time = result.fetchone()[0]
    return latest_utc_time


def generic_toc_calc_datastream(odmx_con,
                                mat_view_name,
                                sampling_feature,
                                d2e_acquiring_instrument_uuid,
                                toc_value):
    """
    Create a calculated datastream based on a TOC level source.
    """
    with db.schema_scope(odmx_con, 'datastreams'):
        sf = sampling_feature
        sf_id = sf.sampling_feature_id
        vprint(f"Creating calculated channels for {mat_view_name}.")

        # Create mat view table or get the latest entry
        calc_view_name = f'{mat_view_name}_toc_wl_calc'
        last_mat_view_time = \
            create_mat_view_table_or_return_latest(odmx_con, calc_view_name)
        last_mat_view_time = last_mat_view_time or 0

        # Turn the view into a DataFrame so that we can do QA/QC on it.
        vprint("Starting the process of materializing the view.")
        query = f'''
            SELECT * FROM {qid(odmx_con, mat_view_name)}
            WHERE qa_flag >= 'z'
            AND utc_time > %s
        '''
        result = odmx_con.execute(query, [last_mat_view_time])
        view_df = pd.DataFrame(result.fetchall(), dtype='object')
        if view_df.empty:
            vprint("The source materialized view is empty. Nothing to do.")
            return
        view_df.columns = ['utc_time', 'data_value', 'qa_flag']
        view_df.sort_values(by='utc_time')
        view_df['data_value'] =\
            (toc_value + sf.elevation_m) - view_df['data_value']


        with db.schema_scope(odmx_con, 'odmx'):
            # (waterLevel)
            variable_id = odmx.read_variables_one(
                    odmx_con, variable_term='waterLevel').variable_id
            # (meter)
            units_id = odmx.read_cv_units_one(
                odmx_con, term='meter').units_id


        # Now apply any and all QA/QC checks.
        view_df['data_value'] = view_df['data_value'].astype(float)
        view_df = qa_checks(view_df, odmx_con, variable_id,
                            units_id)
        # Write the materialized view to the database.
        vprint("Writing the materialized view to the database.")
        db.insert_many_df(odmx_con, calc_view_name, view_df, upsert=False)
        create_datastream_entry(odmx_con, calc_view_name, view_df,sf_id,
                                d2e_acquiring_instrument_uuid, variable_id,
                                units_id)


def qa_checks(df, con, variable_id, units_id,
              manual_qa_list=None):
    """
    Perform appropriate QA/QC checks on data.

    @param df The DataFrame on which to perform the checks.
    @param odmx_con The sqlalchemy connection for connecting to the ODMX
                       database.
    @param variable_id The ODMX variable ID for this datastream.
    @param units_id The ODMX units ID for this datastream.
    @param manual_qa_list A list of manual QA/QC checks to apply. The list is a
        list of dictinaries with entries 'datetime_start', 'datetime_end', and
        'qa_flag'. The datetimes are in isoformat and the qa_flag is a
        character (see the ODMX qa_flag_cv table)
    @return The DataFrame with appropriate QA/QC flags in place.
    """
    with db.schema_scope(con, 'odmx'):
        # Get the appropriate values from the table.
        min_max = odmx.read_variable_qa_min_max_one(
            con, variable_id=variable_id)
        var_min = min_max.min_valid_range
        var_max = min_max.max_valid_range
        # Make sure that the units of the data match the standard for the
        # variable in question. If they don't, it means the units are
        # intentionally non-standard (unconverted), and so we need to match
        # the min/max values to the desired units.
        # Get the quantity kind via the variable to check this.
        variable = odmx.read_variables_one(con, variable_id=variable_id)
        quantity_kind_cv = variable.quantity_kind_cv
        quantity_kind = odmx.read_cv_quantity_kind_one(
            con, term=quantity_kind_cv)
        default_unit = quantity_kind.default_unit
        # With the default unit, check it against the provided `units_id`.
        cv_units = odmx.read_cv_units_one(con, term=default_unit)
        # If they're not equal, we convert the min/max values.
        if units_id != cv_units.units_id:
            multiplier = cv_units.conversion_multiplier
            offset = cv_units.conversion_offset
            var_min = (var_min / multiplier) - offset
            var_max = (var_max / multiplier) - offset

        # Perform the different checks.
        df = ssiqa.set_flags_to_a(df)
        df = ssiqa.check_min_max(df, var_min, var_max)
        df = ssiqa.check_infinity(df)
        df = ssiqa.check_nans(df)
        df = ssiqa.check_nulls(df)
        df = ssiqa.set_flags_to_z(df)
        # Apply manual qa/qc
        if manual_qa_list is not None:
            for manual_qa in manual_qa_list:
                start = manual_qa['datetime_start']
                end = manual_qa['datetime_end']
                if start is None:
                    start = df.index.min()
                else:
                    # convert to unixtime from isoformat
                    start = datetime.datetime.fromisoformat(start).timestamp()
                if end is None:
                    end = df.index.max()
                else:
                    end = datetime.datetime.fromisoformat(end).timestamp()
                qa_flag = manual_qa['qa_flag']
                # TODO better checks on the qa_flag? Maybe take the CV table?
                if qa_flag not in set(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
                                       'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p',
                                       'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
                                       'y', 'z']):
                    raise ValueError(f"Invalid qa_flag {qa_flag} in "
                                     "manual_qa_list")
                vprint(f"QA/QC: Marking data between {start} and {end} with "
                       f"flag '{qa_flag}'.")
                df.set_index('utc_time', inplace=True)
                df.loc[(df.index >= start) & (df.index <= end),
                       'qa_flag'] = qa_flag
                df['utc_time'] = df.index
        return df


def m1_10_calc_datastream(odmx_con, ds_timezone,
                          view_name, sf_id,
                          d2e_acquiring_instrument_uuid):
    """
    Create a calculated datastream for the m1_10 data source. This is meant to
    be temporary, and replaced with a different methodology eventually.
    """
    with db.schema_scope(odmx_con, 'datastreams'):
        print(f"Creating calculated channels for {view_name}.")
        # Find out if the measured view exists.
        calc_view_name = 'm1_10_at200_m1_wl_avg_calc'
        last_mat_view_time = \
            create_mat_view_table_or_return_latest(odmx_con, calc_view_name)
        last_mat_view_time = last_mat_view_time or 0
        vprint(f"Last view time was {last_mat_view_time}")
        # Turn the view into a DataFrame so that we can do QA/QC on it.
        vprint("Starting the process of materializing the view.")
        query = f'''
            SELECT * FROM {qid(odmx_con, view_name)}
            WHERE utc_time > {last_mat_view_time}
        '''
        result = odmx_con.execute(query)
        view_df = pd.DataFrame(result.fetchall(), dtype='object')
        view_df.columns = ['utc_time', 'data_value', 'qa_flag']
        view_df.sort_values(by='utc_time')
        # Do the timezone conversion.
        view_df['utc_time'] = (pd.to_datetime(view_df['utc_time'], unit='s')
                               .dt.tz_localize(ds_timezone, ambiguous='infer')
                               .dt.tz_convert('UTC'))
        # Turn it into Unix time, as that's what the db table takes.
        view_df['utc_time'] = view_df['utc_time'].astype(np.int64) // 10**9
        # Try to pare down to only data we care about, if possible.
        try:
            view_df = view_df[view_df['utc_time'] > last_mat_view_time]
        except NameError:
            pass

        # Hardcode lots of constants.
        plm1_toc = 2777.70
        plm1_list = []
        plm1_time1_start = datetime.datetime(
            1980, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        ).timestamp()
        plm1_time1_end = datetime.datetime(
            2017, 5, 6, 13, 30, 50, tzinfo=datetime.timezone.utc
        ).timestamp()
        plm1_time1_dot = 9119.59 * 0.3048
        plm1_list.append([plm1_time1_start, plm1_time1_end, plm1_time1_dot])
        plm1_time2_start = datetime.datetime(
            2017, 5, 6, 13, 31, 0, tzinfo=datetime.timezone.utc
        ).timestamp()
        plm1_time2_end = datetime.datetime(
            2017, 9, 20, 13, 51, 0, tzinfo=datetime.timezone.utc
        ).timestamp()
        plm1_time2_dot = 9119.42 * 0.3048
        plm1_list.append([plm1_time2_start, plm1_time2_end, plm1_time2_dot])
        plm1_time3_start = datetime.datetime(
            2017, 9, 20, 13, 52, 0, tzinfo=datetime.timezone.utc
        ).timestamp()
        plm1_time3_end = datetime.datetime(
            2017, 10, 29, 13, 42, 0, tzinfo=datetime.timezone.utc
        ).timestamp()
        plm1_time3_dot = 9119.38 * 0.3048
        plm1_list.append([plm1_time3_start, plm1_time3_end, plm1_time3_dot])
        plm1_time4_start = datetime.datetime(
            2017, 10, 29, 13, 42, 1, tzinfo=datetime.timezone.utc
        ).timestamp()
        plm1_time4_end = datetime.datetime(
            2017, 10, 31, 15, 2, 0, tzinfo=datetime.timezone.utc
        ).timestamp()
        plm1_time4_dot = 9119.22 * 0.3048
        plm1_list.append([plm1_time4_start, plm1_time4_end, plm1_time4_dot])
        plm1_time5_start = datetime.datetime(
            2017, 10, 31, 15, 4, 0, tzinfo=datetime.timezone.utc
        ).timestamp()
        plm1_time5_end = datetime.datetime(
            2017, 12, 2, 12, 42, 0, tzinfo=datetime.timezone.utc
        ).timestamp()
        plm1_time5_dot = 9119.38 * 0.3048
        plm1_list.append([plm1_time5_start, plm1_time5_end, plm1_time5_dot])
        plm1_time6_start = datetime.datetime(
            2017, 12, 2, 12, 42, 1, tzinfo=datetime.timezone.utc
        ).timestamp()
        plm1_time6_end = datetime.datetime(
            2200, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        ).timestamp()
        plm1_time6_dot = 9119.20 * 0.3048
        plm1_list.append([plm1_time6_start, plm1_time6_end, plm1_time6_dot])
        vprint(f"Calculated data stream contains: {plm1_list}")

        # Do the actual calculation.
        vprint("Performing calculations.")
        for i in plm1_list:
            mask = \
                (view_df['utc_time'] >= i[0]) & (view_df['utc_time'] <= i[1])
            view_df['data_value'] = np.where(
                mask,
                plm1_toc - (plm1_toc - i[2]) + view_df['data_value'],
                view_df['data_value']
            )
        # Now apply any and all QA/QC checks.
        view_df['data_value'] = view_df['data_value'].astype(float)
        with db.schema_scope(odmx_con, 'odmx'):
            # (waterLevel)
            variable_id = odmx.read_variables_one(
                    odmx_con, variable_term='waterLevel').variable_id
            # (meter)
            units_id = odmx.read_cv_units_one(odmx_con, term='meter').units_id
        view_df = qa_checks(view_df, odmx_con, variable_id,
                            units_id)
        # Write the materialized view to the database.
        vprint("Writing the materialized view to the database.")
        db.insert_many_df(odmx_con, calc_view_name, view_df, upsert=False)

        create_datastream_entry(odmx_con, calc_view_name, view_df,
                                sf_id,
                                d2e_acquiring_instrument_uuid, variable_id,
                                units_id)


def m6_60_calc_datastream(odmx_con, ds_timezone,
                          view_name,
                          sf_id,
                          d2e_acquiring_instrument_uuid):
    """
    Create a calculated datastream for the m6_60 data source. This is meant to
    be temporary, and replaced with a different methodology eventually.
    """

    print(f"Creating calculated channels for {view_name}.")
    # Find out if the measured materialized view exists.
    calc_view_name = 'm6_60_at200_m6_wl_avg_calc'
    last_mat_view_time = create_mat_view_table_or_return_latest(
            odmx_con, calc_view_name)
    last_mat_view_time = last_mat_view_time or 0
    # Turn the view into a DataFrame so that we can do QA/QC on it.
    vprint("Starting the process of materializing the view.")
    query = f'''
        SELECT * FROM {qid(odmx_con, view_name)}
        WHERE utc_time > {last_mat_view_time}
    '''
    result = odmx_con.execute(query)
    view_df = pd.DataFrame(result.fetchall(), dtype='object')
    view_df.columns = ['utc_time', 'data_value', 'qa_flag']
    view_df.sort_values(by='utc_time')
    # Do the timezone conversion.
    view_df['utc_time'] = (pd.to_datetime(view_df['utc_time'], unit='s')
                           .dt.tz_localize(ds_timezone, ambiguous='infer')
                           .dt.tz_convert('UTC'))
    # Turn it into Unix time, as that's what the db table takes.
    view_df['utc_time'] = view_df['utc_time'].astype(np.int64) // 10**9
    # Try to pare down to only data we care about, if possible.
    try:
        view_df = view_df[view_df['utc_time'] > last_mat_view_time]
    except NameError:
        pass

    # Hardcode lots of constants.
    plm6_toc = 2750.77
    plm6_list = []
    plm6_time1_start = datetime.datetime(
        1980, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
    ).timestamp()
    plm6_time1_end = datetime.datetime(
        2017, 6, 6, 16, 20, 50, tzinfo=datetime.timezone.utc
    ).timestamp()
    plm6_time1_dot = 9023.61 * 0.3048
    plm6_list.append([plm6_time1_start, plm6_time1_end, plm6_time1_dot])
    plm6_time2_start = datetime.datetime(
        2017, 6, 6, 16, 20, 55, tzinfo=datetime.timezone.utc
    ).timestamp()
    plm6_time2_end = datetime.datetime(
        2017, 9, 4, 11, 50, 50, tzinfo=datetime.timezone.utc
    ).timestamp()
    plm6_time2_dot = 9023.51 * 0.3048
    plm6_list.append([plm6_time2_start, plm6_time2_end, plm6_time2_dot])
    plm6_time3_start = datetime.datetime(
        2017, 9, 4, 11, 50, 55, tzinfo=datetime.timezone.utc
    ).timestamp()
    plm6_time3_end = datetime.datetime(
        2017, 9, 7, 17, 20, 50, tzinfo=datetime.timezone.utc
    ).timestamp()
    plm6_time3_dot = 9023.86 * 0.3048
    plm6_list.append([plm6_time3_start, plm6_time3_end, plm6_time3_dot])
    plm6_time4_start = datetime.datetime(
        2017, 9, 7, 17, 20, 55, tzinfo=datetime.timezone.utc
    ).timestamp()
    plm6_time4_end = datetime.datetime(
        2017, 9, 20, 11, 20, 50, tzinfo=datetime.timezone.utc
    ).timestamp()
    plm6_time4_dot = 9026.65 * 0.3048
    plm6_list.append([plm6_time4_start, plm6_time4_end, plm6_time4_dot])
    plm6_time5_start = datetime.datetime(
        2017, 9, 20, 11, 20, 55, tzinfo=datetime.timezone.utc
    ).timestamp()
    plm6_time5_end = datetime.datetime(
        2017, 10, 12, 16, 55, 5, tzinfo=datetime.timezone.utc
    ).timestamp()
    plm6_time5_dot = 9026.11 * 0.3048
    plm6_list.append([plm6_time5_start, plm6_time5_end, plm6_time5_dot])
    plm6_time6_start = datetime.datetime(
        2017, 10, 12, 16, 55, 10, tzinfo=datetime.timezone.utc
    ).timestamp()
    plm6_time6_end = datetime.datetime(
        2017, 10, 24, 11, 5, 10, tzinfo=datetime.timezone.utc
    ).timestamp()
    plm6_time6_dot = 9026.24 * 0.3048
    plm6_list.append([plm6_time6_start, plm6_time6_end, plm6_time6_dot])
    plm6_time7_start = datetime.datetime(
        2017, 10, 24, 11, 5, 15, tzinfo=datetime.timezone.utc
    ).timestamp()
    plm6_time7_end = datetime.datetime(
        2200, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
    ).timestamp()
    plm6_time7_dot = 9026.11 * 0.3048
    plm6_list.append([plm6_time7_start, plm6_time7_end, plm6_time7_dot])

    # Do the actual calculation.
    vprint("Performing calculations.")
    for i in plm6_list:
        mask = (view_df['utc_time'] >= i[0]) & (view_df['utc_time'] <= i[1])
        view_df['data_value'] = np.where(
            mask,
            plm6_toc - (plm6_toc - i[2]) + view_df['data_value'],
            view_df['data_value']
        )

    with db.schema_scope(odmx_con, 'odmx'):
        # (waterLevel)
        variable_id = odmx.read_variables_one(
                odmx_con, variable_term='waterLevel').variable_id
        # (meter)
        units_id = odmx.read_cv_units_one(
                odmx_con, term='meter').units_id

    # Now apply any and all QA/QC checks.
    view_df['data_value'] = view_df['data_value'].astype(float)
    view_df = qa_checks(view_df, odmx_con, variable_id,
                        units_id)
    # Materialize it.
    # Write the materialized view to the database.
    vprint("Writing the materialized view to the database.")
    db.insert_many_df(odmx_con, calc_view_name, view_df, upsert=False)
    create_datastream_entry(odmx_con, calc_view_name, view_df,
                            sf_id,
                            d2e_acquiring_instrument_uuid, variable_id,
                            units_id)
