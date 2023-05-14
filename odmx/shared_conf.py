from odmx.support.config import Config
from argparse import ArgumentParser
from odmx.log import set_verbose, vprint
import odmx.support.db as db
import beartype
import os

@beartype.beartype
def setup_base_config(config: Config, parser: ArgumentParser):
    parser.add_argument('work_dir', help="Path to the work directory")
    config.add_config_param('project_name', help="The name of the project to"
                            " work with. E.g., \"testing\".")
    config.add_config_param('verbose', default=False, validator='bool',
                            optional=True, help="Increase output verbosity.")
    db.add_db_parameters_to_config(config)

def validate_config(config, parser):
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
