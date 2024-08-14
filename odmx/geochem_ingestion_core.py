#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Core functions for geochemical data ingestion
"""
import datetime
import uuid
import odmx.data_model as odmx
from odmx.log import vprint

_child_sf_routine_cache = {}


def child_sf_routine(con, timestamp, parent_sf_code, child_sf_code,
                     relation, specimen_collection_id):
    """
    Check if a child sampling feature already exists, and if not, con it.

    @param con The connection object.
    @param timestamp The timestamp of the sample.
    @param parent_sf_code The parent's sampling feature code.
    @param child_sf_code The child's sampling feature code.
    @param relation So far either `wasCollectedAt` or `isSubSpecimenOf`.
    @param specimen_collection_id The specimen collection ID associated with
                                  the sample.
    @return The sampling feature ID of the child.
    """
    if child_sf_code in _child_sf_routine_cache:
        return _child_sf_routine_cache[child_sf_code]
    # See if the child exists.
    child_sf = odmx.read_sampling_features_one_or_none(con,
                                                       sampling_feature_code=child_sf_code)

    # If we do not yet have a sampling feature with this code, we con it.
    if child_sf:
        _child_sf_routine_cache[child_sf_code] = (child_sf.sampling_feature_id,
                                                  False)
        # TODO We should add additional checks here to update data and such.
        return (child_sf.sampling_feature_id, False)
    vprint(f"Creating a sampling feature with code {child_sf_code}.")
    # Write the child sampling feature.
    child_sf_id = write_child_sampling_feature(
        con, child_sf_code, parent_sf_code, relation)
    vprint(f"Created sampling feature ID {child_sf_id}.")
    # Write to the `specimens` table.
    specimen_type_cv = 'grab'
    specimen_medium_cv = 'liquidAqueous'
    is_field_specimen = True
    odmx.write_specimens(con, child_sf_id, specimen_type_cv,
                         specimen_medium_cv, is_field_specimen, timestamp)

    # Write to the `specimen_collection_bridge` table.
    odmx.write_specimen_to_specimen_collection_bridge(con, child_sf_id,
                                                      specimen_collection_id)
    return (child_sf_id, True)


_fieldspecimen_child_sf_creation_cache = {}


def fieldspecimen_child_sf_creation(con, timestamp, parent_sf_code,
                                    child_sf_code, relation, specimen_type_cv,
                                    specimen_medium_cv, specimen_collection_id):
    """
    Check if a child sampling feature already exists, and if not, con it.

    @param con The connection object.
    @param timestamp The timestamp of the sample.
    @param parent_sf_code The parent's sampling feature code.
    @param child_sf_code The child's sampling feature code.
    @param relation So far either `wasCollectedAt` or `isSubSpecimenOf`.
    @param specimen_collection_id The specimen collection ID associated with
                                  the sample.
    @return The sampling feature ID of the child.
    """
    if child_sf_code in _child_sf_routine_cache:
        return _child_sf_routine_cache[child_sf_code]
    # See if the child exists.
    child_sf = odmx.read_sampling_features_one_or_none(con,
                                                       sampling_feature_code=child_sf_code)

    # If we do not yet have a sampling feature with this code, we con it.
    if child_sf:
        _child_sf_routine_cache[child_sf_code] = (child_sf.sampling_feature_id,
                                                  False)
        # TODO We should add additional checks here to update data and such.
        return (child_sf.sampling_feature_id, False)
    vprint(f"Creating a sampling feature with code {child_sf_code}.")
    # Write the child sampling feature.
    child_sf_id = write_child_sampling_feature(
        con, child_sf_code, parent_sf_code, relation)
    vprint(f"Created sampling feature ID {child_sf_id}.")
    # Write to the `specimens` table.
    is_field_specimen = True
    odmx.write_specimens(con, child_sf_id, specimen_type_cv, specimen_medium_cv,
                         is_field_specimen, timestamp)
    # Write to the `specimen_collection_bridge` table.
    odmx.write_specimen_to_specimen_collection_bridge(con, child_sf_id,
                                                      specimen_collection_id)
    return (child_sf_id, True)


def sampleaction_routine(con, analyst_name, affiliation_id, analysis_date,
                         analysis_timezone, action_file_link):
    """
    @param con The connection object.
    @param analyst_name The name of the person who analyzed the sample.
    @param analysis_date The date on which the sample was analyzed.
    @param analysis_timezone The timezone for the date.
    @param action_file_link Some sort of link to the sample.
    @return The action ID of the cond/updated action.
    """

    # Write to the `actions` table.
    action_type_cv = 'specimenAnalysis'
    action_name = f"Analysis of samples by {analyst_name}."
    action_description = "Specimen laboratory analysis."
    method_id = \
        odmx.read_methods_one(con, method_type_cv='specimenAnalysis').method_id
    timezone_dict = {
        'eastern standard time': -4,
        'est': -4,
        'mountain standard time': -7,
        'mst': -7,
        'pacific standard time': -8,
        'pst': -8,
    }
    try:
        utc_offset = timezone_dict[analysis_timezone.lower()]
    except KeyError:
        utc_offset = 0
    if not isinstance(analysis_date, datetime.datetime):
        try:
            analysis_date = datetime.datetime.fromisoformat(analysis_date)
        except ValueError:
            # Python is a little more forgiving about missing leading zeros.
            analysis_date = datetime.datetime.strptime(analysis_date,
                                                       '%Y-%m-%dT%H:%M:%S')

    action_id = odmx.write_actions(
        con,
        action_type_cv=action_type_cv,
        action_name=action_name,
        action_description=action_description,
        method_id=method_id,
        begin_date_time=analysis_date,
        begin_date_time_utc_offset=utc_offset,
        end_date_time=analysis_date,
        end_date_time_utc_offset=utc_offset,
        action_file_link=action_file_link)
    # Write to the `action_by` table.
    is_action_lead = True
    role_description = None
    odmx.write_action_by(
        con,
        action_id,
        affiliation_id,
        is_action_lead,
        role_description=role_description)
    # Write to the `related_actions` table.
    relationship_type_cv = 'isAnalysisOf'
    odmx.write_related_actions(con, action_id, relationship_type_cv, action_id)

    return action_id


def write_sample_results(con, units_id, variable_id,
                         data_entry_value, timestamp, timezone, depth, data_type,
                         passed_result_id, feature_action_id, stddev,
                         censor_code, quality_code, stderr=False):
    """
    Write the results of a sample to appropriate tables.

    @param con The connection object.
    @param session_maker The session maker object.
    @param units_id - the id for the units.
    @param variable_id - the id for the variable
    @param data_entry_value The column data value for a given data entry "row".
    @param timestamp The timestamp value for a given data entry "row".
    @param timezone The timezone for a given data entry "row".
    @param depth The depth of the data entry.
    @param data_type The data type of the data entry.
    @param passed_result_id If it exists, the previous result ID from this
                            function.
    @param feature_action_id The related feature action ID associated with this
                             data entry.
    @return The result ID yielded from this function.
    """

    # Define an internal function to help handle logical flow.
    def result_values(con, result_id, data_value, result_date_time,
                      result_date_time_utc_offset, aggregation_statistic_cv,
                      censorcode, qualitycode):
        """
        We do this bit at the end of this routine regardless of whether the
        data is an instrument reading or a standard deviation.
        """

        # Define the censor code CV.
        if str(data_entry_value).lower().replace('.', '') == 'nd':
            censor_code_cv = 'nonDetect'
            mod_data_value = 0.0
        else:
            censor_code_cv = 'notCensored'
            mod_data_value = data_value
        # TODO Big to-do right here. Need to add in QA/QC.
        quality_code_cv = 'notAssessed'
        if (quality_code != ''):
            quality_code_cv = quality_code
        # Write to `measurement_result_values`.
        odmx.write_measurement_result_values(
            con, result_id, mod_data_value, result_date_time,
            result_date_time_utc_offset, aggregation_statistic_cv,
            censor_code_cv, quality_code_cv
        )

    # Get the data value.
    try:
        data_value = float(data_entry_value)
    except ValueError:
        data_value = 0.0
    # Another recipe for time zone.
    timezone_dict = {
        'eastern standard time': -4,
        'est': -4,
        'mountain standard time': -7,
        'mst': -7,
        'pacific standard time': -8,
        'pst': -8,
    }
    try:
        result_date_time_utc_offset = timezone_dict[timezone.lower()]
    except KeyError:
        result_date_time_utc_offset = 0
    # Get the aggregation_statistic_cv.
    # LBNL data has value/stdev pairs, or just a value, so we've designed the
    # system around this concept.
    if stddev:
        aggregation_statistic_cv = 'standardDeviation'
    elif stderr:
        aggregation_statistic_cv = 'standardErrorOfMean'
    else:
        aggregation_statistic_cv = 'instrumentReading'

    # At this point, we know if we're dealing with an instrument reading or a
    # standard deviation. If it's standard deviation, we use the passed along
    # result ID (which would've come from a previous instrument reading) and
    # just write the `measurement_result_values`.
    if aggregation_statistic_cv == 'standardDeviation':
        result_values(con, passed_result_id, data_value, timestamp,
                      result_date_time_utc_offset, aggregation_statistic_cv,
                      censor_code, quality_code)

        return None
    if aggregation_statistic_cv == 'standardErrorOfMean':
        result_values(con, passed_result_id, data_value, timestamp,
                      result_date_time_utc_offset, aggregation_statistic_cv,
                      censor_code, quality_code)
        return None
    # If it's an instrument reading, we gather more details and write to
    # `results`, `measurement_results`, and `measurement_result_values`.
    if aggregation_statistic_cv == 'instrumentReading':
        # Create the rest of the object's parameters.
        # The UUID.
        result_uuid = str(uuid.uuid4())
        # The result type CV.
        result_type_cv = 'measurement'
        # The units ID is similarly tricky.
        # processing_level_id.
        processing_level_id = odmx.read_processing_levels_one(
            con,
            definition='unknown').processing_level_id
        # valid_date_time.
        valid_date_time = None
        # valid_date_time_utc_offset.
        valid_date_time_utc_offset = None
        # status_cv.
        status_cv = 'complete'
        # value_count.
        value_count = 1
        # no_data_value.
        no_data_value = -999.999
        # Write to `results`.
        assert variable_id is not None
        assert units_id is not None
        result_id = odmx.write_results(
            con,
            result_uuid=result_uuid,
            feature_action_id=feature_action_id,
            result_type_cv=result_type_cv,
            variable_id=variable_id,
            units_id=units_id,
            processing_level_id=processing_level_id,
            result_date_time=timestamp,
            result_date_time_utc_offset=result_date_time_utc_offset,
            valid_date_time=valid_date_time,
            valid_date_time_utc_offset=valid_date_time_utc_offset,
            status_cv=status_cv,
            value_count=value_count,
            no_data_value=no_data_value)

        # Move on to the `measurement_results`.
        # x_location.
        x_location = None
        # x_location_units_id.
        x_location_units_id = None
        # y_location.
        y_location = None
        # y_location_units_id.
        y_location_units_id = None
        # z_location and z_location_units_id.
        if depth is not None:
            z_location = float(depth)
            z_location_units_id = \
                odmx.read_cv_units_one(con, term='meter').units_id
        else:
            z_location = None
            z_location_units_id = None
        # spatial_reference_id.
        spatial_reference_id = None
        # time_aggregation_interval.
        time_aggregation_interval = None
        # time_aggregation_interval_units_id.
        time_aggregation_interval_units_id = None
        # Write to `measurement_results`.
        odmx.write_measurement_results(
            con,
            result_id=result_id,
            x_location=x_location,
            x_location_units_id=x_location_units_id,
            y_location=y_location,
            y_location_units_id=y_location_units_id,
            z_location=z_location,
            z_location_units_id=z_location_units_id,
            spatial_reference_id=spatial_reference_id,
            time_aggregation_interval=time_aggregation_interval,
            time_aggregation_interval_units_id=time_aggregation_interval_units_id)

        # Write to `measurement_result_values`.
        result_values(con, result_id, data_value, timestamp,
                      result_date_time_utc_offset, aggregation_statistic_cv,
                      censor_code, quality_code)

        return result_id
    # Should never get here, but including it as a failsafe.
    raise ValueError("The aggregation_statistic_cv is neither an instrument"
                     " reading nor a standard deviation or a standard error. This should be"
                     " impossible.")


def feature_action(odmx_db_con, action_id, subspecimen_sf_id):
    # Now write to `feature_actions`, `results`, `measurement_results`,
    # and `mesurement_result_values` tables.
    related_features_relation_id = odmx.read_related_features_one(
        odmx_db_con, sampling_feature_id=subspecimen_sf_id).relation_id
    feature_action_id = odmx.write_feature_actions(
        odmx_db_con, subspecimen_sf_id, action_id,
        related_features_relation_id
    )
    return feature_action_id


def write_child_sampling_feature(con, child_sampling_feature_code,
                                 parent_sampling_feature_code, relation):
    """
    Write to the `sampling_features` table, where the new entry is a child of
    a pre-existing sampling feature.

    @param con The database connection
    @param child_sampling_feature_code The sampling feature code for the child.
    @param parent_sampling_feature_code The sampling feature code for the
                                        parent.
    @param relation How the child and the parent are related.
    @return The newly created/pre-existing sampling feature ID for the child.
    """

    # Get parent sampling feature information.
    parent_sampling_feature = odmx.read_sampling_features_one(
        con,
        sampling_feature_code=parent_sampling_feature_code
    )
    # Define object attributes.
    sampling_feature_uuid = str(uuid.uuid4())
    if relation in ['wasCollectedAt', 'isSubSpecimenOf']:
        sampling_feature_type_cv = 'specimen'
    elif relation == 'isPartOf':
        sampling_feature_type_cv = 'depthInterval'
    else:
        raise ValueError(f"Invalid relation: {relation}")
    sampling_feature_code = child_sampling_feature_code
    sampling_feature_name = child_sampling_feature_code
    phrase_dict = {
        'wasCollectedAt': "Specimen collected at",
        'isSubSpecimenOf': "Subspecimen of",
        'isPartOf': "Depth interval of",
    }
    sampling_feature_description = (f"{phrase_dict[relation]}"
                                    f" {parent_sampling_feature_code}")
    sampling_feature_geotype_cv = 'point'
    feature_geometry = parent_sampling_feature.feature_geometry
    feature_geometry_wkt = parent_sampling_feature.feature_geometry_wkt
    elevation_m = parent_sampling_feature.elevation_m
    elevation_datum_cv = parent_sampling_feature.elevation_datum_cv
    latitude = parent_sampling_feature.latitude
    longitude = parent_sampling_feature.longitude
    epsg = parent_sampling_feature.epsg
    vprint(f"Writing sampling feature {sampling_feature_code}...")
    # Create the object.
    child_sampling_feature_id = odmx.write_sampling_features(
        con,
        sampling_feature_uuid=sampling_feature_uuid,
        sampling_feature_type_cv=sampling_feature_type_cv,
        sampling_feature_code=sampling_feature_code,
        sampling_feature_name=sampling_feature_name,
        sampling_feature_description=sampling_feature_description,
        sampling_feature_geotype_cv=sampling_feature_geotype_cv,
        feature_geometry=feature_geometry,
        feature_geometry_wkt=feature_geometry_wkt,
        elevation_m=elevation_m,
        elevation_datum_cv=elevation_datum_cv,
        latitude=latitude,
        longitude=longitude,
        epsg=epsg
    )
    # Add the association that this was collected at the parent location.
    parent_sampling_feature_id = parent_sampling_feature.sampling_feature_id
    assert parent_sampling_feature_id is not None
    spatial_offset_id = None
    odmx.write_related_features(
        con,
        sampling_feature_id=child_sampling_feature_id,
        relationship_type_cv=relation,
        related_feature_id=parent_sampling_feature_id,
        spatial_offset_id=spatial_offset_id)

    return child_sampling_feature_id
