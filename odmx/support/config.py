#!/usr/bin/env python3

"""
A subpackage containing a class for handling configuration options
"""

import sys
import os
import json
import argparse
from collections import namedtuple
from beartype import beartype
from beartype.typing import Union, Optional, Callable
import yaml
import ssi.validators

def map_to_cli_name(name: str):
    """
    Converts a parameter name to a cli argument name, lower case and starts
    with -- and _ are replaced with -
    """
    return '--' + name.lower().replace("_", "-")

def map_to_environ_name(name: str):
    """
    Converts a parameter name to an environment variable name, upper case and -
    is converted to _
    """
    return name.upper().replace("-", "_")

def map_to_cli_arg_name(name: str):
    """
    Converts a parameter name to a cli argument name as defined by the argparse
    module, so --foo-bar is parsed into foo_bar
    """
    return name.lower().replace("-", "_")

class ConfigurationError(Exception):
    """
    Class for all exceptions thrown by the ssi.config library
    """

class ConfigurationNotSetError(ConfigurationError):
    """
    Thrown when a configuration value was not set in any source
    """

class ConfigurationNotDefinedError(ConfigurationError):
    """
    Thrown when a configuration key was not defined
    """

#pylint: disable=too-many-instance-attributes
class ConfigParam():
    """
    Class representing a single configuration argument
    """
    VALIDATORS={
            'integer': ssi.validators.validate_int,
            'int': ssi.validators.validate_int,
            'float': ssi.validators.validate_float,
            'url': ssi.validators.validate_url,
            'port': ssi.validators.validate_port,
            'hostname': ssi.validators.validate_hostname,
            'bool': ssi.validators.validate_bool,
            'boolean': ssi.validators.validate_bool,
            'enum': ssi.validators.validate_enum,
        }

    @beartype
    #pylint: disable=redefined-builtin disable=too-many-arguments
    def __init__(self, name: str, help: Optional[str], default=None,
                 validator: Optional[Union[Callable, str]] = None,
                 optional: bool=False, alternative=Optional[str],
                 validation_args=None):
        """
        See documentation for the Config class's add_config_param
        """
        self.name = name
        self.env_name = map_to_environ_name(name)
        self.cli_name = map_to_cli_name(name)
        self.help = help
        self.default = default
        if isinstance(validator, str):
            if validator not in self.VALIDATORS:
                raise ValueError(f"Invalid validator reference {validator}")
            validator = self.VALIDATORS[validator]
        self.validator = validator
        self.validation_args = {}
        self.value = None
        self.validation_args = validation_args
        self.optional = self.default is not None or optional
        self.alternative = alternative


    def set_and_validate_value(self, value):
        """
        Validates and stores the parameter value
        """
        if self.validator:
            self.value = self.validator(
                param=value,
                param_name=self.name,
                **self.validation_args)
        else:
            self.value = value

    def get_value(self):
        """
        Gets the config parameter value. Note that for non-optional parameters,
        an error is thrown at validation time
        """
        if self.value is None:
            if self.default is not None:
                return self.default
            if self.optional:
                return None
            raise ConfigurationNotSetError(
                f"'{self.name}' not set. Use SSI_CONFIG_TRACE=1 for details")
        return self.value

ConfigFile = namedtuple("ConfigFile", ["filename", "contents", "check_unused"])

class Config():
    """
    Class for defining and retriving configuration values from various sources,
    for now json/yaml files, cli arguments, and environment variables
    """
    @beartype
    def __init__(
            self,
            name=None,
            trace=False):
        """
        @param name The name of the configuration set
        @param trace Whether to print a trace of the configuration evaluation.
                     Useful to figure out the source of a configuration value.
                     This is also enabled with the environment variable
                     'SSI_CONFIG_TRACE' set to '1'.
        """
        self._name = name
        self._trace_enabled = os.environ.get(
            "SSI_CONFIG_TRACE") == '1' or trace
        self._params = {}
        self.config_files: list(ConfigFile) = []
        self._validated = False

    def trace_print(self, *args, **kwargs):
        """
        Wraps a print function for tracing configuration information
        """
        if self._trace_enabled:
            print("CONFIG: ", *args, file=sys.stderr, **kwargs)

    def add_config_file(
            self,
            filename: str,
            must_exist: bool,
            deserializer: Callable,
            check_unused = True):
        """
        Add the contents of the configuration file to the configuration set.
        The configuration file is deserialized by a callable deserializer which
        must take a file handle
        """
        self.trace_print(f"Adding file '{filename}'")
        if not os.path.isfile(filename):
            if not must_exist:
                self.trace_print(f"Optional file '{filename}' not found")
                return
            if os.path.exists('/.in-container'):
                self.trace_print(f"'{filename}' does not exist, but we are "
                    "running in container, should be using env vars")
            raise FileNotFoundError(f"Config File '{filename}' does not "
                                     "exist")
        with open(filename, encoding='UTF-8') as file_descriptor:
            self.config_files.append(
                    ConfigFile(filename=filename,
                               contents=deserializer(file_descriptor),
                               check_unused=check_unused))


    def add_yaml_file(self, yml_file: str, must_exist=True,
            check_unused=True):
        """
        Adds the contents of the yaml file to the configuration set. Any values
        set by previously added files are overwritten
        """
        self.add_config_file(filename=yml_file, must_exist=must_exist,
                check_unused=check_unused, deserializer=yaml.safe_load)

    def add_json_file(self, json_file: str, must_exist=True,
            check_unused=True):
        """
        Adds the contents of the json file to the configuration set. Any values
        set by previously added files are overwritten
        """
        self.add_config_file(filename=json_file, must_exist=must_exist,
                check_unused=check_unused, deserializer=json.load)


    #pylint: disable=redefined-builtin
    #pylint: disable=too-many-arguments
    def add_config_param(self, name, help=None, default=None,
                         validator=None, optional=False, alternative=None,
                         **kwargs):
        """
        @param Name The name of the configuration parameter. The configuration
        parameter name is normalized to CAP_CASE form, so if 'hello-world' is
        provided then it will become 'HELLO_WORLD'. THe corresponding cli
        configuration parameter becomes '--hello-world'

        @param help A help string for indicating what the parameter is for.
        This is passed into a argparse help for generating cli arguments

        @param default Optional default for a config parameter
        @param validator A callable validator which is a function which takes
        at minimum a named parameter 'param' which is the contents of the
        parameter to validate as well as additional parameters to the
        validation. Extra parameters to this method are passed to the
        validator so as to allow such things as max, min and so forth.
        Validator can also be a string of built-in validators, one of
        'integer','float','url','port','hostname' which are common
        configuration parameter types. Validators can also act to change the
        configuration parameter, which allows for them to also sanitize the
        parameter under some circumstances.
        @param optional Whether the configuration parameter is optional. If
        it's optional, then the parameter will return None when not set by any
        configuration source. Otherwise, an exception is raised (the default)
        @param alternative An alternative name for the configuration parameter.
        This is useful for backwards compatibility or for allowing multiple
        names for the same parameter.
        """
        # Some simple heuristics for common cases
        if 'choices' in kwargs and validator is None:
            validator = 'enum'
            kwargs['enum'] = kwargs['choices']
            del kwargs['choices']
        self._params[name] = ConfigParam(
            name=name,
            help=help,
            default=default,
            optional=optional,
            validator=validator,
            alternative=alternative,
            validation_args=kwargs)

    def add_args_to_argparser(self, parser: argparse.ArgumentParser):
        """
        Adds the configuration arguments previously defined to an existing
        argparse object, using the format --test-foo for a variable TEST_FOO
        and so forth
        """
        #parser._action_groups.pop()
        required = parser.add_argument_group('required config parameters')
        optional = parser.add_argument_group('optional config parameters')
        for param in self._params.values():
            cli_name = map_to_cli_name(param.name)
            help=param.help
            param_required = not param.optional
            if param.default is not None:
                # NOte: We don't set default here because we want to be able to
                # trace the default through the config and not argparse
                help += (" (if not found in env/config file, default is "
                         f"'{param.default}')")
                param_required = False
            group = required if param_required else optional
            group.add_argument(cli_name, type=str, help=help)

    def _trace(self, param_name, value, source):
        if value:
            self.trace_print(
                f"\t'{param_name}' is set to '{value}' in {source}")


    def _trace_final(self, param_name, source):
        self.trace_print(f"\t'{param_name}' from {source} will be used")

    def _validate_param(self, param_name, param_obj, args):
        param = param_obj
        self.trace_print(f"Evaluating parameter '{param_name}'")
        env_param_name = map_to_environ_name(param_name)
        env_param_name_lcase = env_param_name.lower()
        cli_arg_param_name = map_to_cli_arg_name(param_name)
        env_value = os.environ.get(env_param_name)
        self._trace(env_param_name, env_value, "environment")

        cli_value = None
        file_value = None
        if args and cli_arg_param_name in args:
            cli_value = args.getattr(self, cli_arg_param_name)
            self._trace(param_name, cli_value, "command line")
        for config_file in self.config_files:
            file = config_file.filename
            config = config_file.contents
            if param_name in config:
                file_value = config[param_name]
                # Delete so we can trace the unused later if required
                del config[param_name]
                source_file = file
                self._trace(param_name, file_value, source_file)
            if env_param_name in config:
                file_value = config[env_param_name]
                del config[env_param_name]
                source_file = file
                self._trace(env_param_name, file_value, source_file)
            if env_param_name_lcase in config:
                file_value = config[env_param_name_lcase]
                del config[env_param_name_lcase]
                source_file = file
                self._trace(env_param_name_lcase, file_value, source_file)
        if cli_value:
            self._trace_final(param_name, 'command line')
            param.set_and_validate_value(cli_value)
        elif env_value:
            param.set_and_validate_value(env_value)
            self._trace_final(param_name, 'environment')
        elif file_value:
            param.set_and_validate_value(file_value)
            self._trace_final(param_name, f"'{source_file}'")
        else:
            if self._trace_enabled:
                self.trace_print(
                    f"\tNo value found for parameter '{param_name}'")
                if param.default is not None:
                    self.trace_print(
                        f"\tUsing default of '{param.default}'")
            if not param.optional:
                return False
        return True

    def validate_config(self, args=None):
        """
        Goes through all defined parameters and pulls them from sources in the
        following priority order: CLI, Environment, and YML/JSON file
        @param args Parsed arguments from the argparse.ArgumentParser object
        """
        if self._validated:
            raise ConfigurationError("Configuration set is already validated")
        unset_params = []
        for param_name, param in self._params.items():
            if not self._validate_param(param_name, param, args):
                if param.alternative:
                    if not self._validate_param(param.alternative, param, args):
                        unset_params.append(param_name)
                else:
                    unset_params.append(param_name)

        # Report parameters that were not set
        if unset_params:
            raise ConfigurationNotSetError(
                f"Parameters {','.join(unset_params)} not set. Use "
                "SSI_CONFIG_TRACE=1 for details")

        # Check unused keys
        for config_file in self.config_files:
            if config_file.check_unused:
                unused_keys = list(config_file.contents.keys())
                if unused_keys:
                    raise ConfigurationError(
                        f"{config_file.filename} has keys that are not used "
                        "in configuration: " + ",".join(unused_keys))
        self._validated = True

    def _check_validated(self):
        """
        Check if a configuration value is validated before it is used
        """
        if not self._validated:
            raise ConfigurationError(
                "Configuration must be validated before it can be used")


    def __getattr__(self, key):
        """
        Gets the configuration value as an object attribute. If the value does
        not exist either because it was not defined or because it was not set
        in any known configuration, an exception is raised
        """
        self._check_validated()
        if key not in self._params:
            raise ConfigurationNotDefinedError(
                f"Configuration parameter {key} was not defined. "
                "This is an internal error, the software is "
                "referencing a configuration key which has not "
                "been specified to exist")
        config = self._params[key]
        return config.get_value()

    def get(self, key):
        """
        Returns a configuration attribute if it exists, otherwise returns None.
        This is useful to use instead of __getattr__ for optional values.
        """
        self._check_validated()
        if key not in self._params:
            return None
        config = self._params[key]
        return config.value

    def to_dict(self):
        """
        Returns configuration parameters as a dictionary
        """
        self._check_validated()
        return {key: val.get_value() for key, val in self._params.items()}

    def __iter__(self):
        return iter(self.to_dict().items())

    def __repr__(self):
        if not self._validated:
            return "Config(<Not Validated> " + \
           ",".join(list(self._params.keys())) + ")"
        return "Config(" + ",".join([f"{key}={val}" for key, val in self])+ ")"
