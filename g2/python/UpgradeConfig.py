# ! /usr/bin/env bash

import argparse
import json
import os.path
import time


def get_parser():
    '''Parse commandline arguments.'''
    parser = argparse.ArgumentParser(description="Migrate Senzing configuration")
    parser.add_argument("--existing-config-file", dest="existing_config_filename", required=True, help="Input file pathname for existing configuration file")
    parser.add_argument("--new-config-template-file", dest="new_config_template_filename", required=True, help="Input file pathname for the new configuration template")
    parser.add_argument("--output-file", dest="output_filename", help="Output file pathname")
    return parser


def main(existing_config_filename, new_config_template_filename, out_filename):
    '''Main program.'''
    
    # Load the existing configuration.
    
    with open(existing_config_filename) as existing_file:
        existing_dictionary = json.load(existing_file)
        
    # Load the new configuration template.
    
    with open(new_config_template_filename) as config_file:
        new_template_dictionary = json.load(config_file)
        
    # Do the transformation.
    
    new_template_dictionary["G2_CONFIG"]["CFG_DSRC"] = existing_dictionary.get("G2_CONFIG", {}).get("CFG_DSRC", {})
    new_template_dictionary["G2_CONFIG"]['CFG_ETYPE'] = existing_dictionary.get("G2_CONFIG", {}).get("CFG_ETYPE", {})
    
    # Write out the "merged" configuration.
    
    with open(out_filename, "w") as out_file:
        json.dump(new_template_dictionary, out_file, sort_keys=True, indent=4)

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


if __name__ == "__main__":

    # Parse the command line arguments.

    parser = get_parser()
    args = parser.parse_args()

    existing_config_filename = args.existing_config_filename
    new_config_template_filename = args.new_config_template_filename
    output_filename = args.output_filename or "new-config-{0}.json".format(int(time.time()))

    # Verify existence of files.

    if not os.path.isfile(existing_config_filename):
        print("Error: --existing-config-file {0} does not exist".format(existing_config_filename))
        os._exit(1)

    if not os.path.isfile(new_config_template_filename):
        print("Error: --new-config-template-file {0} does not exist".format(new_config_template_filename))
        os._exit(1)

    # Call the main entry point.

    main(existing_config_filename, new_config_template_filename, output_filename)

    print("Output file: {0}".format(output_filename))
