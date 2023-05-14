#!/usr/bin/env python3

"""
Set the value of "verbose print" for the data pipeline.
"""
import inspect
import os

_VPRINT = None

_print = print


def print_with_log_prefix(*args, call_levels=1, **kwargs,):
    """
    Print with a log prefix.
    """
    # Get the name of the calling function.
    caller = inspect.stack()[call_levels]
    module = inspect.getmodule(caller[0])
    caller_name = module.__name__ + "." + caller[3]
    _print(f"[{caller_name}]", *args, **kwargs)


def set_verbose(verbose: bool):
    """
    Set the value of vprint based on passed verbosity (usually from CL).
    """
    global _VPRINT
    # We wrap vprint so that we can set the call level to 3 which avoids
    # printing the wrapper function name.
    if verbose:
        _VPRINT = lambda *a, **k: print_with_log_prefix(call_levels=3, *a, **k)
    else:
        _VPRINT = lambda *a, **k: None
    vprint(f"Verbosity set to {verbose}")


def vprint(*args, **kwargs):
    """
    vprint wrapper.
    """
    _VPRINT(*args, **kwargs)


# Not verbose by default.
set_verbose(False)

# Replace the built-in print with our verbose print.
prefix_log = os.getenv("ODMX_PREFIX_LOG", "1")
if prefix_log == "1":
    if 'oldprint' not in __builtins__:
        __builtins__['oldprint'] = __builtins__['print']
    __builtins__['print'] = print_with_log_prefix
    print("Notice: print() has been replaced with print_with_log_prefix()")
    print("To disable this, set ODMX_PREFIX_LOG=0")
