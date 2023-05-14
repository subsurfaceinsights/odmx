#!/usr/bin/env python3

"""
Subpackage to handle validation for functions.
"""

import sys
import inspect
import json
import datetime
import math
import pytz
import warnings

class InvalidParameter(Exception):
    """
    A class for throwing an exception when an API parameter fails validation.
    """

    def __init__(self, param_name, param_value, param_type):
        super().__init__("Invalid parameter:"
                         f" '{param_name}'='{param_value}'."
                         f" Expected: {param_type}")


def is_number(x):
    """
    Quick internal function to determine if a variable is float-able or not.
    """

    try:
        float(x)
        return True
    except ValueError:
        return False


def var_name_as_passed(var):
    """
    This function returns the name of the variable as passed into the function
    called by it. It's really hacky and everyone says don't do it. I do it
    anyway.

    @param var The variable to pass to find a name.
    @return The name of the variable, or `None` if no name was found.
    """

    lcls = inspect.stack()[2][0].f_locals
    for name in lcls:
        if id(var) == id(lcls[name]):
            return name
    return None


def validate_int(param, param_name: str = None, lower: int = None,
                 upper: int = None):
    """
    Validates that the parameter given is an integer and that is in the range
    optionally given. Accepts integers, floats with `.0`, or string
    representations of either.

    @param param The parameter to test.
    @param param_name The name of the parameter.
    @param lower A lower bound for the integer parameter.
    @param upper An upper bound for the integer parameter.
    @return The validated parameter.
    """

    # Define the parameter name.
    if param_name is None:
        param_name = var_name_as_passed(param)
    # If the parameter is a boolean, raise an exception.
    if isinstance(param, bool):
        raise InvalidParameter(param_name, param, "Integer")
    # If the parameter is a float (or string representation) but is not a whole
    # number, raise an exception.
    if (isinstance(param, (float, str)) and is_number(param)
        and float(param) % 1 != 0):
        raise InvalidParameter(param_name, param, "Integer")
    # Try assigning the parameter as an integer. This will weed out invalid
    # parameters at this point.
    try:
        param = int(float(param))
    except (TypeError, ValueError) as exception:
        raise InvalidParameter(param_name, param, "Integer") from exception
    except OverflowError as exception:
        raise InvalidParameter(param_name, param, "Integer must be in range"
                               f" {sys.float_info.min} <= integer <="
                               f" {sys.float_info.max}") from exception
    # If the lower bound was assigned, check it.
    if lower is not None and param < lower:
        raise InvalidParameter(param_name, param,
                               f"Integer greater than or equal to {lower}")
    # If the upper bound was assigned, check it.
    if upper is not None and param > upper:
        raise InvalidParameter(param_name, param,
                               f"Integer less than or equal to {upper}")
    return param


def validate_float(param, param_name: str = None, lower: float = None,
                   upper: float = None):
    """
    Validates that the parameter given is valid as a floating point number in
    the range optionally given. Returns a float type if passed. Candidate may
    be a float or a string representation of a float.

    @param param The parameter to test.
    @param param_name The name of the parameter.
    @param lower A lower bound for the float parameter.
    @param upper An upper bound for the float parameter.
    @return The validated parameter.
    """

    # Define the parameter name.
    if param_name is None:
        param_name = var_name_as_passed(param)
    # If the parameter is a boolean, raise an exception.
    if isinstance(param, bool):
        raise InvalidParameter(param_name, param, "Float")
    # Try assigning the parameter as a float. This will weed out invalid
    # parameters at this point.
    try:
        param = float(param)
    except (TypeError, ValueError) as exception:
        raise InvalidParameter(param_name, param, "Float") from exception
    except OverflowError as exception:
        raise InvalidParameter(param_name, param, "Float must be in range"
                               f" {sys.float_info.min} <= float <="
                               f" {sys.float_info.max}") from exception
    # If the lower bound was assigned, check it.
    if lower is not None and param < lower:
        raise InvalidParameter(param_name, param,
                               f"Float greater than or equal to {lower}")
    # If the upper bound was assigned, check it.
    if upper is not None and param > upper:
        raise InvalidParameter(param_name, param,
                               f"Float less than or equal to {upper}")
    return param


def validate_bool(param, param_name: str = None):
    """
    Validates the given parameter as a boolean.
    Interprets the following as `True`:
    - boolean: `True`
    - strings: `'true'` (with any capitalization), `'1'`
    - integers: 1
    Interprets the following as `False`:
    - boolean: `False`
    - strings: `'false'` (with any capitalization), `'0'`
    - integers: 0

    @param param The parameter to test.
    @param param_name The name of the parameter.
    @return The validated parameter.
    """

    # Define the parameter name.
    if param_name is None:
        param_name = var_name_as_passed(param)
    # Check if the parameter is already boolean. If so, simply return it.
    if isinstance(param, bool):
        return param
    # If it's a string, check if it's some version of a boolean or 1 or 0.
    if isinstance(param, str):
        if param.lower() == "true" or param == '1':
            return True
        if param.lower() == "false" or param == '0':
            return False
        raise InvalidParameter(param_name, param,
                               "Boolean (True, False, 0, 1)")
    # If it's an integer, return the corresponding boolean.
    if isinstance(param, int):
        if param == 1:
            return True
        if param == 0:
            return False
        raise InvalidParameter(param_name, param,
                               "Boolean (True, False, 0, 1)")
    # When none of the above criteria are met, raise an exception.
    raise InvalidParameter(param_name, param, "Boolean (True, False, 0, 1)")


def validate_json_object(param, param_name: str = None):
    """
    Validates the given parameter as a JSON object. If the parameter is already
    a list/dictionary then it is returned as is. Otherwise, it is parsed as a
    JSON string and returned.

    @param param The parameter to test.
    @param param_name The name of the parameter.
    @return The validated parameter.
    """

    # Define the parameter name.
    if param_name is None:
        param_name = var_name_as_passed(param)
    # If the parameter is a dictionary or list, simply return it.
    if isinstance(param, (dict, list)):
        return param
    # If the parameter is a string, see if it's JSON complaint and return it.
    if isinstance(param, str):
        try:
            return json.loads(param)
        except (TypeError, json.JSONDecodeError) as exception:
            raise InvalidParameter(param_name, param,
                    f"JSON String. Parsing Error: {exception}") \
                                           from exception
    # When none of the above criteria are met, raise an exception.
    raise InvalidParameter(param_name, param,
                           "JSON String encoding a Dict or List")


def validate_enum(param, enum, param_name: str = None,
                  case_insensitive: bool = True):
    """
    Validates that a parameter is in a given enum list.

    @param param The parameter to test.
    @param enum The list of items that might contain the parameter.
    @param param_name The name of the parameter.
    @param case_insensitive Determines whether or not case matters for the
                            parameter or the items in the enum list.
    @return The validated parameter.
    """

    # Define the parameter name.
    if param_name is None:
        param_name = var_name_as_passed(param)
    # If `case_insensitive` is `True`, then we make all strings lower case.
    if case_insensitive:
        if isinstance(param, str):
            param = param.lower()
        enum = [i.lower() if isinstance(i, str) else i for i in enum]
    # If the parameter isn't in the enum list, raise an exception.
    if param not in enum:
        joined_enum = "', '".join([str(i) for i in enum])
        raise InvalidParameter(param_name, param, f"One of '{joined_enum}'")
    return param


def validate_id(param, param_name: str = None, lower: int = 1,
                upper: int = None):
    """
    Validates that the parameter given is an integer ID by the same rules as
    given in the validate_int call.

    @param param The parameter to test.
    @param param_name The name of the parameter.
    @param lower A lower bound for the ID parameter. Defaults to `1`.
    @param upper An upper bound for the ID parameter.
    @return The validated parameter.
    """

    # Define the parameter name.
    if param_name is None:
        param_name = var_name_as_passed(param)
    # Validate the ID in the same way we validate an integer, but with
    # (usually) a set lower limit of one.
    return validate_int(param, param_name, lower=lower, upper=upper)


def validate_id_list(param, param_name: str = None,
                     enforce_list: bool = False):
    """
    Sanitizes an input into a list of IDs by using a simple heuristic:
    - If already a list, check that the items are IDs.
    - If an integer or single numeric string, place it into a sized 1 list
      (based on the `enforce_list` parameter)
    - If a string and not numeric then try to JSON decode, and then check again
      for typing.

    @param param The parameter to test.
    @param param_name The name of the parameter.
    @param enforce_list Whether or not the parameter needs to be a list.
    @return The validated parameter.
    """

    # Define the parameter name.
    if param_name is None:
        param_name = var_name_as_passed(param)
    # If the parameter is a list, validate each list item as an ID.
    if isinstance(param, list):
        return [validate_id(item, param_name=param_name, lower=1)
                for item in param]
    # If the parameter is an integer or a numeric string, and we're not forcing
    # it to come as a list, check it and turn it into a list.
    if (isinstance(param, (int, float, str)) and is_number(param)
        and not isinstance(param, bool)):
        if not enforce_list:
            return [validate_id(param, param_name=param_name, lower=1)]
        raise InvalidParameter(param_name, param, "Must be a list containing"
                               " at least one integer")
    # If the parameter is some other form of string (ostensibly a list), check
    # if it's JSON compliant, and then see if it's a list containing IDs.
    if isinstance(param, str):
        try:
            param = json.loads(param)
            if isinstance(param, list):
                return [validate_id(item, param_name=param_name, lower=1)
                        for item in param]
            raise InvalidParameter(param_name, param,
                                   "Integer ID or list thereof")
        except (TypeError, json.JSONDecodeError) as exception:
            raise InvalidParameter(param_name, param,
                    "Integer ID or list thereof") from exception
    raise InvalidParameter(param_name, param, "Integer ID or list thereof")


def validate_datetime(param, param_name: str = None, lower: int = None,
                      upper: int = None, date_time_fmt: str = '%Y-%m-%d',
                      return_date_time: bool = False):
    """
    Validates that the parameter given is a valid datetime. The default
    validation format is `YYYY-MM-DD`. The return format by default is a
    Unix time UTC, but a datetime may be returned with the `return_date_time`
    parameter set to `True`.

    @param param The parameter to test.
    @param param_name The name of the parameter.
    @param lower A lower bound for the datetime parameter, as a Unix time
                 integer.
    @param upper An upper bound for the datetime parameter, as a Unix time
                 integer.
    @param date_time_fmt The formatting for how the datetime should be
                         interpreted.
    @param return_date_time Boolean determining whether or not the return is a
                            datetime object or a Unix timestamp.
    @return The validated parameter.
    """

    # Define the parameter name.
    if param_name is None:
        param_name = var_name_as_passed(param)
    # If the parameter is a datetime object, redefine it based on the given
    # format and get the Unix time.
    if isinstance(param, datetime.datetime):
        param_date_time = datetime.datetime.strftime(param, date_time_fmt)
        param_date_time = datetime.datetime.strptime(param_date_time,
                                                     date_time_fmt)
        param_unix_time = param_date_time.timestamp()
    # If the parameter is an integer, float, or a numeric string, treat it as a
    # Unix time and find the datetime.
    elif (isinstance(param, (int, float, str)) and is_number(param)
          and not isinstance(param, bool)):
        try:
            param_unix_time = int(math.floor(float(param)))
            param_date_time = datetime.datetime.utcfromtimestamp(param_unix_time)
        except (ValueError, OSError, OverflowError) as exception:
            min_time = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc).timestamp()
            max_time = datetime.datetime.max.replace(tzinfo=datetime.timezone.utc).timestamp()
            raise InvalidParameter(param_name, param, "Unix timestamp must be"
                                   f" in range {min_time} <= timestamp <"
                                   f" {max_time}") from exception
    # If the parameter is a non-numeric string, try to treat it as a datetime
    # string.
    elif isinstance(param, str) and not is_number(param):
        try:
            param_date_time = datetime.datetime.strptime(param, date_time_fmt)
            param_unix_time = param_date_time.timestamp()
        except ValueError as exception:
            raise InvalidParameter(param_name, param,
                                   f"Datetime with format {date_time_fmt}") \
                                        from exception
    # If the parameter doesn't fit any of the above cases, raise an exception.
    else:
        raise InvalidParameter(param_name, param,
                               f"Datetime with format {date_time_fmt}")
    # Now that we have a Unix timestamp, if a lower limit was provided, we can
    # check it.
    if lower is not None and param_unix_time < lower:
        raise InvalidParameter(param_name, param,
                               f"Datetime greater than or equal to {lower}"
                               " Unix time")
    # Similarly, if an upper limit was provided, we can check it.
    if upper is not None and param_unix_time > upper:
        raise InvalidParameter(param_name, param,
                               f"Datetime less than or equal to {upper} Unix"
                               " time")
    # Finally, return the parameter in whichever format has been specified.
    if return_date_time:
        return param_date_time
    return param_unix_time


def validate_datetime_range(start, end, start_name: str = None,
                            end_name: str = None):
    """
    Validates a datetime range.

    @param start The starting datetime parameter to test.
    @param end The ending datetime parameter to test.
    @param start_name The name of the start parameter.
    @param end_name The name of the end parameter.
    @return The validated parameter.
    """

    # Define the parameter name.
    if start_name is None:
        start_name = var_name_as_passed(start)
    # Define the parameter name.
    if end_name is None:
        end_name = var_name_as_passed(end)
    # Validate both the start and end datetimes.
    start = validate_datetime(start)
    end = validate_datetime(end)
    # Check that the times are a valid range.
    if start > end:
        raise InvalidParameter(f"{start_name},{end_name}", f"{start}>{end}",
                               "start less than end date")
    return (start, end)


def validate_timezone(param, param_name: str = None):
    """
    Validates that the passed string is in fact a timezone string present in
    the pytz python package.

    @param param The parameter to test.
    @param param_name The name of the parameter.
    @return The validated parameter.
    """

    # Define the parameter name.
    if param_name is None:
        param_name = var_name_as_passed(param)
    # Find the list of acceptable timezone strings.
    acceptable_tzs = list(pytz.all_timezones)
    # Validate the parameter.
    return validate_enum(param, acceptable_tzs, param_name,
                         case_insensitive=False)


def validate_length(param, length, param_name: str = None,
                    min_length: int = None, max_length: int = None):
    """
    Validates the length of a parameter, or optionally instead a minimum and/or
    maximum lengths.

    @param param The parameter to test.
    @param length The desired length of the parameter.
    @param param_name The name of the parameter.
    @param min_length Optionally, the minimum length that the parameter should
                      reach or exceed. Supersedes `length` if defined.
    @param max_length Optionally, the maximum length that the parameter should
                      reach but not exceed. Supersedes `length` if defined.
    @return The validated parameter.
    """

    # Define the parameter name.
    if param_name is None:
        param_name = var_name_as_passed(param)
    # Try to find the length of the parameter.
    try:
        param_length = len(param)
    except TypeError:
        raise InvalidParameter(param_name, param,
                     "Parameter must have a measurable length") from TypeError
    # If no min length or max length were defined, we simply test the required
    # length.
    if min_length is None and max_length is None:
        if param_length == length:
            return param
        raise InvalidParameter(param_name, param,
                               f"Parameter length does not equal {length}")
    # Otherwise, we make sure it fits within the min length and max length.
    if min_length is not None:
        if param_length < min_length:
            raise InvalidParameter(param_name, param, "Parameter length is"
                                   f" less than {min_length}")
    if max_length is not None:
        if param_length > max_length:
            raise InvalidParameter(param_name, param, "Parameter length is"
                                   f" greater than {max_length}")
    return param

#pylint: disable=unused-argument
def validate_url(param, param_name):
    """
    TODO validates a URL
    """
    warnings.warn("URL validation is currently not implemented TODO")
    return param

def validate_port(param, param_name):
    """
    Validates a port integer value. Param may be a str, int, or float but it
    must represent a whole number between and including 1 and 65535
    """
    return validate_int(param, param_name, lower=1, upper=65535)

#pylint: disable=unused-argument
def validate_hostname(param, param_name):
    """
    TODO Validates a hostname
    """
    warnings.warn("Hostname validation is currently not implemented TODO")
    return param
