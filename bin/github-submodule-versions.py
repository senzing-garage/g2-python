#! /usr/bin/env python3

# -----------------------------------------------------------------------------
# github-submodule-versions.py
#
# Work with GitHub.
#
# References:
# - GitHub
#   - https://github.com/PyGithub/PyGithub
#   - https://pygithub.readthedocs.io/
#   - https://pygithub.readthedocs.io/en/latest/github_objects.html
# -----------------------------------------------------------------------------

import argparse
import configparser
from github import Github
import json
import linecache
import logging
import os
import signal
import sys
import time

__all__ = []
__version__ = "1.0.0"  # See https://www.python.org/dev/peps/pep-0396/
__date__ = '2021-08-14'
__updated__ = '2022-01-27'

# See https://github.com/Senzing/knowledge-base/blob/master/lists/senzing-product-ids.md
SENZING_PRODUCT_ID = "5023"
log_format = '%(asctime)s %(message)s'

# The "configuration_locator" describes where configuration variables are in:
# 1) Command line options, 2) Environment variables, 3) Configuration files, 4) Default values

configuration_locator = {
    "debug": {
        "default": False,
        "env": "SENZING_DEBUG",
        "cli": "debug"
    },
    "github_access_token": {
        "default": None,
        "env": "GITHUB_ACCESS_TOKEN",
        "cli": "github-access-token"
    },
    "organization": {
        "default": "Senzing",
        "env": "GITHUB_ORGANIZATION",
        "cli": "organization"
    },
    "sleep_time_in_seconds": {
        "default": 0,
        "env": "SENZING_SLEEP_TIME_IN_SECONDS",
        "cli": "sleep-time-in-seconds"
    },
    "subcommand": {
        "default": None,
        "env": "SENZING_SUBCOMMAND",
    }
}

# Enumerate keys in 'configuration_locator' that should not be printed to the log.

keys_to_redact = [
    "github_access_token",
]

repositories = {
    "compressedfile": {
        "artifacts": ["CompressedFile.py"]
    },
    "dumpstack": {
        "artifacts": ["DumpStack.py"]
    },
    "g2audit": {
        "artifacts": ["G2Audit.py"]
    },
    "g2command": {
        "artifacts": ["G2Command.py"]
    },
    "g2config": {
        "artifacts": ["G2Config.py"]
    },
    "g2configmgr": {
        "artifacts": ["G2ConfigMgr.py"]
    },
    "g2configtables": {
        "artifacts": ["G2ConfigTables.py"]
    },
    "g2configtool": {
        "artifacts": ["G2ConfigTool.py", "G2ConfigTool.readme"]
    },
    "g2createproject": {
        "artifacts": ["G2CreateProject.py"]
    },
    "g2database": {
        "artifacts": ["G2Database.py"]
    },
    "g2diagnostic": {
        "artifacts": ["G2Diagnostic.py"]
    },
    "g2engine": {
        "artifacts": ["G2Engine.py"]
    },
    "g2exception": {
        "artifacts": ["G2Exception.py"]
    },
    "g2explorer": {
        "artifacts": ["G2Explorer.py"]
    },
    "g2export": {
        "artifacts": ["G2Export.py"]
    },
    "g2health": {
        "artifacts": ["G2Health.py"]
    },
    "g2hasher": {
        "artifacts": ["G2Hasher.py"]
    },
    "g2iniparams": {
        "artifacts": ["G2IniParams.py"]
    },
    "g2loader": {
        "artifacts": ["G2Loader.py"]
    },
    "g2paths": {
        "artifacts": ["G2Paths.py"]
    },
    "g2product": {
        "artifacts": ["G2Product.py"]
    },
    "g2project": {
        "artifacts": ["G2Project.py"]
    },
    "g2s3": {
        "artifacts": ["G2S3.py"]
    },
    "g2setupconfig": {
        "artifacts": ["G2SetupConfig.py"]
    },
    "g2snapshot": {
        "artifacts": ["G2Snapshot.py"]
    },
    "g2updateproject": {
        "artifacts": ["G2UpdateProject.py"]
    }
}

# -----------------------------------------------------------------------------
# Define argument parser
# -----------------------------------------------------------------------------


def get_parser():
    ''' Parse commandline arguments. '''

    subcommands = {
        'list-gitmodules-for-bash': {
            "help": 'Print xxxx',
            "arguments": {},
        },
        'list-submodule-versions': {
            "help": 'Print modules.sh',
            "argument_aspects": ["common"],
            "arguments": {},
        },
        'version': {
            "help": 'Print version of program.',
        },
    }

    # Define argument_aspects.

    argument_aspects = {
        "common": {
            "--debug": {
                "dest": "debug",
                "action": "store_true",
                "help": "Enable debugging. (SENZING_DEBUG) Default: False"
            },
            "--github-access-token": {
                "dest": "github_access_token",
                "metavar": "GITHUB_ACCESS_TOKEN",
                "help": "GitHub Personal Access token. See https://github.com/settings/tokens"
            },
            "--organization": {
                "dest": "organization",
                "metavar": "GITHUB_ORGANIZATION",
                "help": "GitHub account/organization name."
            },
        },
    }

    # Augment "subcommands" variable with arguments specified by aspects.

    for subcommand, subcommand_value in subcommands.items():
        if 'argument_aspects' in subcommand_value:
            for aspect in subcommand_value['argument_aspects']:
                if 'arguments' not in subcommands[subcommand]:
                    subcommands[subcommand]['arguments'] = {}
                arguments = argument_aspects.get(aspect, {})
                for argument, argument_value in arguments.items():
                    subcommands[subcommand]['arguments'][argument] = argument_value

    parser = argparse.ArgumentParser(
        description="Reports from GitHub. For more information, see https://github.com/Senzing/github-util")
    subparsers = parser.add_subparsers(
        dest='subcommand', help='Subcommands (SENZING_SUBCOMMAND):')

    for subcommand_key, subcommand_values in subcommands.items():
        subcommand_help = subcommand_values.get('help', "")
        subcommand_arguments = subcommand_values.get('arguments', {})
        subparser = subparsers.add_parser(subcommand_key, help=subcommand_help)
        for argument_key, argument_values in subcommand_arguments.items():
            subparser.add_argument(argument_key, **argument_values)

    return parser

# -----------------------------------------------------------------------------
# Message handling
# -----------------------------------------------------------------------------

# 1xx Informational (i.e. logging.info())
# 3xx Warning (i.e. logging.warning())
# 5xx User configuration issues (either logging.warning() or logging.err() for Client errors)
# 7xx Internal error (i.e. logging.error for Server errors)
# 9xx Debugging (i.e. logging.debug())


MESSAGE_INFO = 100
MESSAGE_WARN = 300
MESSAGE_ERROR = 700
MESSAGE_DEBUG = 900

message_dictionary = {
    "100": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}I",
    "101": "Added   Repository: {0} Label: {1}",
    "102": "Updated Repository: {0} Label: {1}",
    "103": "Deleted Repository: {0} Label: {1}",
    "104": "Repository '{0}' has been archived.  Not modifying its labels.",
    "293": "For information on warnings and errors, see https://github.com/Senzing/github-util",
    "294": "Version: {0}  Updated: {1}",
    "295": "Sleeping infinitely.",
    "296": "Sleeping {0} seconds.",
    "297": "Enter {0}",
    "298": "Exit {0}",
    "299": "{0}",
    "300": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}W",
    "499": "{0}",
    "500": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "696": "Bad SENZING_SUBCOMMAND: {0}.",
    "697": "No processing done.",
    "698": "Program terminated with error.",
    "699": "{0}",
    "700": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}E",
    "701": "GITHUB_ACCESS_TOKEN is required",
    "899": "{0}",
    "900": "senzing-" + SENZING_PRODUCT_ID + "{0:04d}D",
    "998": "Debugging enabled.",
    "999": "{0}",
}


def message(index, *args):
    index_string = str(index)
    template = message_dictionary.get(
        index_string, "No message for index {0}.".format(index_string))
    return template.format(*args)


def message_generic(generic_index, index, *args):
    index_string = str(index)
    return "{0} {1}".format(message(generic_index, index), message(index, *args))


def message_info(index, *args):
    return message_generic(MESSAGE_INFO, index, *args)


def message_warning(index, *args):
    return message_generic(MESSAGE_WARN, index, *args)


def message_error(index, *args):
    return message_generic(MESSAGE_ERROR, index, *args)


def message_debug(index, *args):
    return message_generic(MESSAGE_DEBUG, index, *args)


def get_exception():
    ''' Get details about an exception. '''
    exception_type, exception_object, traceback = sys.exc_info()
    frame = traceback.tb_frame
    line_number = traceback.tb_lineno
    filename = frame.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, line_number, frame.f_globals)
    return {
        "filename": filename,
        "line_number": line_number,
        "line": line.strip(),
        "exception": exception_object,
        "type": exception_type,
        "traceback": traceback,
    }

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------


def get_configuration(args):
    ''' Order of precedence: CLI, OS environment variables, INI file, default. '''
    result = {}

    # Copy default values into configuration dictionary.

    for key, value in list(configuration_locator.items()):
        result[key] = value.get('default', None)

    # "Prime the pump" with command line args. This will be done again as the last step.

    for key, value in list(args.__dict__.items()):
        new_key = key.format(subcommand.replace('-', '_'))
        if value:
            result[new_key] = value

    # Copy OS environment variables into configuration dictionary.

    for key, value in list(configuration_locator.items()):
        os_env_var = value.get('env', None)
        if os_env_var:
            os_env_value = os.getenv(os_env_var, None)
            if os_env_value:
                result[key] = os_env_value

    # Copy 'args' into configuration dictionary.

    for key, value in list(args.__dict__.items()):
        new_key = key.format(subcommand.replace('-', '_'))
        if value:
            result[new_key] = value

    # Add program information.

    result['program_version'] = __version__
    result['program_updated'] = __updated__

    # Special case: subcommand from command-line

    if args.subcommand:
        result['subcommand'] = args.subcommand

    # Special case: Change boolean strings to booleans.

    booleans = [
        'debug',
    ]
    for boolean in booleans:
        boolean_value = result.get(boolean)
        if isinstance(boolean_value, str):
            boolean_value_lower_case = boolean_value.lower()
            if boolean_value_lower_case in ['true', '1', 't', 'y', 'yes']:
                result[boolean] = True
            else:
                result[boolean] = False

    # Special case: Change integer strings to integers.

    integers = [
        'sleep_time_in_seconds'
    ]
    for integer in integers:
        integer_string = result.get(integer)
        result[integer] = int(integer_string)

    return result


def validate_configuration(config):
    ''' Check aggregate configuration from commandline options, environment variables, config files, and defaults. '''

    user_warning_messages = []
    user_error_messages = []

    # Perform subcommand specific checking.

    subcommand = config.get('subcommand')

    if subcommand in ['comments']:

        if not config.get('github_access_token'):
            user_error_messages.append(message_error(701))

    # Log warning messages.

    for user_warning_message in user_warning_messages:
        logging.warning(user_warning_message)

    # Log error messages.

    for user_error_message in user_error_messages:
        logging.error(user_error_message)

    # Log where to go for help.

    if len(user_warning_messages) > 0 or len(user_error_messages) > 0:
        logging.info(message_info(293))

    # If there are error messages, exit.

    if len(user_error_messages) > 0:
        exit_error(697)


def redact_configuration(config):
    ''' Return a shallow copy of config with certain keys removed. '''
    result = config.copy()
    for key in keys_to_redact:
        try:
            result.pop(key)
        except:
            pass
    return result

# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------


def bootstrap_signal_handler(signal, frame):
    sys.exit(0)


def create_signal_handler_function(args):
    ''' Tricky code.  Uses currying technique. Create a function for signal handling.
        that knows about "args".
    '''

    def result_function(signal_number, frame):
        logging.info(message_info(298, args))
        sys.exit(0)

    return result_function


def entry_template(config):
    ''' Format of entry message. '''
    debug = config.get("debug", False)
    config['start_time'] = time.time()
    if debug:
        final_config = config
    else:
        final_config = redact_configuration(config)
    config_json = json.dumps(final_config, sort_keys=True)
    return message_info(297, config_json)


def exit_template(config):
    ''' Format of exit message. '''
    debug = config.get("debug", False)
    stop_time = time.time()
    config['stop_time'] = stop_time
    config['elapsed_time'] = stop_time - config.get('start_time', stop_time)
    if debug:
        final_config = config
    else:
        final_config = redact_configuration(config)
    config_json = json.dumps(final_config, sort_keys=True)
    return message_info(298, config_json)


def exit_error(index, *args):
    ''' Log error message and exit program. '''
    logging.error(message_error(index, *args))
    logging.error(message_error(698))
    sys.exit(1)


def exit_silently():
    ''' Exit program. '''
    sys.exit(0)

# -----------------------------------------------------------------------------
# do_* functions
#   Common function signature: do_XXX(args)
# -----------------------------------------------------------------------------


def do_list_gitmodules_for_bash(args):

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)
    validate_configuration(config)

    # Prolog.

    logging.info(entry_template(config))

    # Read .gitmodules file.

    filename = "{0}/.gitmodules".format("..")
    config_parser = configparser.ConfigParser()
    config_parser.optionxform = str  # Maintain case of keys.
    config_parser.read(filename)

    # Transform to python dictionary.

    gitmodules_dict = {}
    sections = config_parser.sections()
    for section in sections:
        gitmodules_dict[section] = {}
        for key in config_parser[section]:
            value = config_parser[section][key]
            gitmodules_dict[section][key] = value

    # Print body.

    for module, module_metadata in gitmodules_dict.items():
        artifacts = repositories.get(module_metadata.get("path", {}),{}).get("artifacts", [])
        for artifact in artifacts:
            print("    {0};{1};{2}".format(
                module_metadata.get("path"),
                module_metadata.get("branch", "main"),
                artifact
            ))

def do_list_submodule_versions(args):

    # Get context from CLI, environment variables, and ini files.

    config = get_configuration(args)
    validate_configuration(config)

    # Prolog.

    logging.info(entry_template(config))

    # Pull values from configuration.

    github_access_token = config.get("github_access_token")
    organization = config.get("organization")

    # Log into GitHub.

    github = Github(github_access_token)

    # Determine current version.

    github_organization = github.get_organization(organization)
    for repository in repositories.keys():
        repo = github_organization.get_repo(repository)
        release = repo.get_latest_release()
        repositories[repository]['version'] = release.title

    # Print output.

    for key, value in repositories.items():
        version = value.get('version', '0.0.0')
        artifacts = value.get('artifacts', [])
        for artifact in artifacts:
            print('    {0};{1};{2}'.format(key, version, artifact))

    # Epilog.

    logging.info(exit_template(config))


def do_version(args):
    ''' Log version information. '''

    logging.info(message_info(294, __version__, __updated__))

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


if __name__ == "__main__":

    # Configure logging. See https://docs.python.org/2/library/logging.html#levels

    log_level_map = {
        "notset": logging.NOTSET,
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "fatal": logging.FATAL,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }

    log_level_parameter = os.getenv("SENZING_LOG_LEVEL", "info").lower()
    log_level = log_level_map.get(log_level_parameter, logging.INFO)
    logging.basicConfig(format=log_format, level=log_level)
    logging.debug(message_debug(998))

    # Trap signals temporarily until args are parsed.

    signal.signal(signal.SIGTERM, bootstrap_signal_handler)
    signal.signal(signal.SIGINT, bootstrap_signal_handler)

    # Parse the command line arguments.

    subcommand = os.getenv("SENZING_SUBCOMMAND", None)
    parser = get_parser()
    if len(sys.argv) > 1:
        args = parser.parse_args()
        subcommand = args.subcommand
    elif subcommand:
        args = argparse.Namespace(subcommand=subcommand)
    else:
        parser.print_help()
        exit_silently()

    # Catch interrupts. Tricky code: Uses currying.

    signal_handler = create_signal_handler_function(args)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Transform subcommand from CLI parameter to function name string.

    subcommand_function_name = "do_{0}".format(subcommand.replace('-', '_'))

    # Test to see if function exists in the code.

    if subcommand_function_name not in globals():
        logging.warning(message_warning(696, subcommand))
        parser.print_help()
        exit_silently()

    # Tricky code for calling function based on string.

    globals()[subcommand_function_name](args)
