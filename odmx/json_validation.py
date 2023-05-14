import os
import json
import jsonschema
from odmx.log import vprint

json_schema_files = os.path.join(os.path.dirname(__file__), '..', 'json_schema')

def open_json(json_file, validate=True, json_schema=None):
    """
    Open a json file and return the data as a dict.
    """
    with open(json_file, 'r') as f:
        vprint(f'Opening {json_file}')
        data = json.load(f)
        if validate:
            if json_schema is None:
                json_schema = os.path.join(
                        json_schema_files,
                        os.path.basename(json_file).replace(
                            '.json', '_schema.json'))
            with open(json_schema, 'r') as f:
                schema = json.load(f)
                jsonschema.validate(data, schema)
                vprint(f'Validated {json_file} against {json_schema}')
        return data

