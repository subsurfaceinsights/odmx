import os
import shutil
import pandas as pd
import json
import argparse
from odmx.support.file_utils import open_csv

# TODO: don't create a subdirectory if there is only one file in the directory


def setup_csv_data_source_config_json(csv_path):
    """
    Initialize data_source_config.json with data column names from csv.
    @param csv_path Path to the csv file to write the config json for.
    """
    data_file_type = None
    directory = os.path.dirname(csv_path)
    # TODO write args as the header
    args = {'comment': '#'}
    df = open_csv(csv_path, args)
    if "microbes" in csv_path.lower():
        data_file_type = "microbes"
        time_col = "sample_date"
    elif "yield" in csv_path.lower():
        data_file_type = "yield"
        if "residual_stover_biomass_(2015_to_2021)" in csv_path.lower():
            time_col = "sample_date"
        else:
            time_col = "harvest_date"
    elif "soil" in csv_path.lower():
        time_col = "sample_date"
        if "chem" in csv_path.lower():
            data_file_type = "soil-chemical"
        elif "phys" in csv_path.lower():
            data_file_type = "soil-physical"
    elif "plant" in csv_path.lower():
        if "tissue" in csv_path.lower():
            data_file_type = 'tissue'
        elif "root" in csv_path.lower():
            data_file_type = 'root'
        else:
            data_file_type = 'plant'
        time_col = 'sample_date'
        if "poplar_stem_diameter_and_biomass_measurements" in csv_path.lower():
            time_col = 'date'
        elif "leaf_area_index" in csv_path.lower():
            time_col = 'sample_datetime'
        elif "feedstock_quality" in csv_path.lower():
            time_col = 'date'
    elif "weather" in csv_path.lower():
        time_col = 'date'
        data_file_type = 'weather'

    elif "fluxes" in csv_path.lower():
        data_file_type = 'flux'
        if "icos" in csv_path.lower():
            time_col = 'datetime'
        else:
            time_col = 'sample_date'

    elif "remote" in csv_path.lower():
        data_file_type = 'remote_sensing'
        time_col = 'date'

    else:
        time_col = 'SampleDate'
        data_file_type = 'water_sampling'

    cols = []
    for col in df.columns.tolist():
        cols.append({"name": col,
                     "variable_cv": "",
                     "unit_cv": "",
                     "expose": True})

    data_split = [{"sampling_feature": "",
                   "equipment_metadata": None,
                   "equipment_path": "",
                   "base_equipment": {
                       "time_column": time_col
                   },
                   "columns": cols}]

    config = {"data_file_extension": "csv",
              "data_file_tabs": None,
              "data_file_name": os.path.basename(csv_path),
              "data_file_type": data_file_type,
              "data_split": data_split, }

    with open(f'{directory}/data_source_config.json', 'w',
              encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=4)


def process_directory(directory):
    """
    Generate a subdirectory, move csv file, and write a datasource config json
    for all csv files in a directory.
    @param directory Path to the directory that should be processed.
    """
    # List all files in the directory
    for filename in os.listdir(directory):
        # Check if the file is a CSV
        if filename.endswith(".csv"):
            # Create a subdirectory name: lowercase, no spaces
            subdirectory_name = filename.lower().replace(" ", "_").replace(".csv", "")
            subdirectory_path = os.path.join(directory, subdirectory_name)

            # Create the subdirectory
            os.makedirs(subdirectory_path, exist_ok=True)

            # Move the CSV file to the subdirectory
            src_file_path = os.path.join(directory, filename)
            dst_file_path = os.path.join(subdirectory_path, filename)
            shutil.move(src_file_path, dst_file_path)

            # Generate the datasource config
            setup_csv_data_source_config_json(dst_file_path)

            print(f"Processed {filename} into {subdirectory_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Initialize data_source_config.json for each CSV file in a directory')
    parser.add_argument('directory_path', type=str,
                        help='Path to the directory to process')
    args = parser.parse_args()

    process_directory(args.directory_path)


if __name__ == "__main__":
    main()
