"""
ODMX json validation
"""
import os
from importlib.util import find_spec
import json
import jsonschema
from odmx.log import vprint

json_schema_files = find_spec("odmx.json_schema").submodule_search_locations[0]

def open_json(json_file, validate=True, json_schema=None):
    """
    Open a json file and return the data as a dict.
    """
    with open(json_file, 'r', encoding='utf-8') as f:
        vprint(f'Opening {json_file}')
        data = json.load(f)
        if validate:
            if json_schema is None:
                json_schema = os.path.join(
                        json_schema_files,
                        os.path.basename(json_file).replace(
                            '.json', '_schema.json'))
            with open(json_schema, 'r', encoding='utf-8') as f:
                schema = json.load(f)
                jsonschema.validate(data, schema)
                vprint(f'Validated {json_file} against {json_schema}')
        return data
