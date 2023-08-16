"""
Basic configuration tools
"""
import os
from argparse import ArgumentParser
import beartype
from odmx.support.config import Config
from odmx.log import set_verbose, vprint
from odmx.support import db

@beartype.beartype
def setup_base_config(config: Config, parser: ArgumentParser):
    """ Setup base config object"""
    parser.add_argument('work_dir', help="Path to the work directory")
    config.add_config_param('project_name', help="The name of the project to"
                            " work with. E.g., \"testing\".")
    config.add_config_param('verbose', default=False, validator='bool',
                            optional=True, help="Increase output verbosity.")
    db.add_db_parameters_to_config(config)

def validate_config(config, parser):
    """ Validate config object"""
    config.add_args_to_argparser(parser)
    args = parser.parse_args()
    config_file_path = os.path.join(args.work_dir, 'config.yaml')
    if os.path.exists(config_file_path):
        vprint(f"Loading config from {config_file_path}.")
        config.add_yaml_file(config_file_path)
    # Validate the config object.
    config.validate_config(args)
    # Define verbosity.
    set_verbose(config.verbose)
    return args.work_dir
