import odmx.support.db as db
from odmx.support.config import Config
from odmx.shared_conf import setup_base_config, validate_config
from typing import TextIO
import json
import os
import argparse
import odmx.data_model as odmx

# These are covered by special handlers
excluded_tables = set([
    'related_features',
    'sampling_feature_aliases',
    'sampling_feature_extension_properties'
    'variable_qa_min_max'])


# THese are tables that we treat as CVs
pseudo_cvs = set([
    'variables',
    'processing_levels'])

def handle_variables(con: db.Connection, output: TextIO):
    """
    We combine the variables table with the variable_qa_min_max table for
    convenience. TODO unclear why these are separate tables if they are
    always joined.
    """
    variables = odmx.read_variables(con)
    variable_qa_min_max = odmx.read_variable_qa_min_max(con)
    variables_by_id = {}
    for variable in variables:
        variables_by_id[variable.variable_id] = variable.to_json_dict()
    for variable_qa in variable_qa_min_max:
        variable = variables_by_id[variable_qa.variable_id]
        qa = variable_qa.to_json_dict()
        variable['min_valid_range'] = qa['min_valid_range']
        variable['max_valid_range'] = qa['max_valid_range']
    json.dump(list(variables_by_id.values()), output, indent=4)
    return len(variables_by_id)

def handle_sampling_features(con: db.Connection, output: TextIO):
    """
    Sampling features are a special case by virtue of the fact that they are
    formatted as a tree. This function reads the sampling features table and
    writes it to a file in JSON format, but with an explicit tree structure.
    """
    sampling_features = odmx.read_sampling_features(con)
    sampling_features_by_id = {}
    for sampling_feature in sampling_features:
        sampling_features_by_id[
                sampling_feature.sampling_feature_id] = \
                        sampling_feature.to_json_dict()
    sampling_features = list(sampling_features_by_id.values())
    related_sampling_features = odmx.read_related_features(con)
    for related_sampling_feature in related_sampling_features:
        sampling_feature = sampling_features_by_id[
                related_sampling_feature.sampling_feature_id]
        relation_to_parent = related_sampling_feature.relationship_type_cv
        if relation_to_parent == 'isChildOf':
            sampling_feature['parent_sampling_feature_id'] = \
                    related_sampling_feature.related_feature_id
            sampling_feature['relation_to_parent'] = relation_to_parent
        else:
            if 'related_features' not in sampling_feature:
                sampling_feature['related_features'] = []
            sampling_feature['related_features'].append(
                    related_sampling_feature.to_json_dict())
    aliases = odmx.read_sampling_features_aliases(con)
    for alias in aliases:
        sampling_feature = sampling_features_by_id[
                alias.sampling_feature_id]
        if 'sampling_feature_aliases' not in sampling_feature:
            sampling_feature['sampling_feature_aliases'] = []
        alias = alias.to_json_dict()
        del alias['sampling_feature_id']
        del alias['sampling_features_aliases_id']
        sampling_feature['sampling_feature_aliases'].append(alias)
    extension_properties = odmx.read_sampling_feature_extension_property_values(con)
    for extension_property in extension_properties:
        sampling_feature = sampling_features_by_id[
                extension_property.sampling_feature_id]
        if 'extension_properties' not in sampling_feature:
            sampling_feature['extension_properties'] = []
        extension_property = extension_property.to_json_dict()
        property_name = odmx.read_extension_properties_one(
                con, property_id=extension_property['property_id']).property_name
        extension_property['property_name'] = property_name
        del extension_property['sampling_feature_id']
        del extension_property['property_id']
        del extension_property['bridge_id']
        sampling_feature['extension_properties'].append(
                extension_property)
    sampling_features_tree = []
    written = 0
    for sampling_feature in sampling_features:
        del sampling_feature['feature_geometry']
        if 'relation_to_parent' not in sampling_feature:
            sampling_feature['relation_to_parent'] = None
        if 'sampling_feature_alias' not in sampling_feature:
            sampling_feature['sampling_feature_alias'] = []
        if 'extension_properties' not in sampling_feature:
            sampling_feature['extension_properties'] = []
        if 'child_sampling_features' not in sampling_feature:
            sampling_feature['child_sampling_features'] = []
    for sampling_feature in sampling_features:
        written += 1
        if 'parent_sampling_feature_id' not in sampling_feature or sampling_feature['parent_sampling_feature_id'] is None:
            sampling_features_tree.append(sampling_feature)
        else:
            parent = sampling_features_by_id[sampling_feature['parent_sampling_feature_id']]
            parent['child_sampling_features'].append(sampling_feature)
            del sampling_feature['parent_sampling_feature_id']
    json.dump(sampling_features_tree, output, indent=4)
    return written

special_table_handlers = {
    "sampling_features": handle_sampling_features,
    "variables": handle_variables
}

def snapshot_table(con: db.Connection, table_name: str, output: TextIO):
    """Snapshot a table to a file in JSON format."""
    if table_name in special_table_handlers:
        return special_table_handlers[table_name](con, output)
    table_class = odmx.get_table_class(table_name)
    if table_class is None:
        raise ValueError(f"Unknown table {table_name}")
    output.write("[\n")
    first = True
    written = 0
    for row in table_class.read(con):
        if not first:
            output.write(",\n")
        first = False
        json_string = json.dumps(row.to_json_dict(), indent=4)
        # Add an indent to each line
        json_string = "    " + json_string.replace("\n", "\n    ")
        output.write(json_string)
        written += 1
    output.write("\n]\n")
    return written



def main():
    config = Config("Snapshot a table to a file in JSON format.")
    parser = argparse.ArgumentParser()
    setup_base_config(config, parser)
    config.add_config_param("output_dir",
                            "Output directory for JSON files snapshotting "
                            "tables. This is equivalent to the odmx directory "
                            "of a project directory")
    config.add_config_param("overwrite",
                            "Overwrite output directory if it exists",
                            validator='bool', default=False)
    config.add_config_param("tables",
                            "Tables to snapshot (comma separated)",
                            optional=True)
    config.add_config_param("exclude_tables",
                            "Tables to exclude from snapshot "
                            "(comma separated)", optional=True)
    config.add_config_param("dump_feeder",
                            "Also dump feeder tables (under feeder)",
                            optional=True)
    validate_config(config, parser)
    if config.tables and config.exclude_tables:
        raise ValueError("Cannot specify both tables and exclude_tables")
    tables = None
    if config.tables:
        tables = config.tables.split(",")
    odmx_db_con = db.connect(config,
                                  db_name=f"odmx_{config.project_name}")
    db.set_current_schema(odmx_db_con, "odmx")
    output_dir = config.output_dir
    if os.path.exists(output_dir):
        if not config.overwrite:
            raise ValueError(f"Output directory {output_dir} already exists. "
                            "Set --overwrite True to overwrite it.")
        if not os.path.isdir(output_dir):
            raise ValueError(f"Output directory {output_dir} exists but is "
                              "not a directory.")
    else:
        os.mkdir(output_dir)
        os.mkdir(os.path.join(output_dir, "cvs"))
        os.mkdir(os.path.join(output_dir, "ingestion_tables"))
    if not tables:
        tables = db.get_tables(odmx_db_con)
    if config.exclude_tables:
        tables = [
                table for table in tables if table not in config.exclude_tables.split(",")]
    for table in tables:
        if table in excluded_tables:
            continue
        if table.startswith("cv_") or table in pseudo_cvs:
            dir = os.path.join(output_dir, "cvs")
        else:
            dir = os.path.join(output_dir, "ingestion_tables")
        filename = os.path.join(dir, f"{table}.json")
        with open(filename, "w") as output:
            print(f"Snapshotting table {table}")
            written = snapshot_table(odmx_db_con, table, output)
        if written == 0:
            os.remove(filename)
        print(f"Snapshot of table {table} written to "
              f"{filename} with {written} rows")
    if config.dump_feeder:
        dump_dir = os.path.join(output_dir, "feeder")
        if not os.path.exists(dump_dir):
            os.mkdir(dump_dir)
        db.set_current_schema(odmx_db_con, "feeder")
        feeder_tables = db.get_tables(odmx_db_con)
        if config.exclude_tables:
            feeder_tables = [
                    table for table in feeder_tables if table not in config.exclude_tables.split(",")]
        if config.tables:
            feeder_tables = [
                    table for table in feeder_tables if table in config.tables.split(",")]
        for table in feeder_tables:
            filename = os.path.join(dump_dir, f"{table}.csv")
            with open(filename, "wb") as output:
                print(f"Dumping feeder table {table}")
                rows = db.dump_table_as_csv(odmx_db_con, table, output)
            print(f"Feeder table {table} dumped to "
                  f"{filename} with {rows} rows")
if __name__ == "__main__":
    main()
