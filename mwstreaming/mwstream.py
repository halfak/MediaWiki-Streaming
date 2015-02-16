"""
Provides access to a set of utilities for stream-processing MediaWiki data.

Data processing utilities:

* diffs2persistence     Generates token persistence statistics using revision
                        JSON blobs with diff information.

* dump2json             Converts an XML dump to a stream of revision JSON blobs

* json2diffs            Computes and adds a "diff" field to a stream of revision
                        JSON blobs

* persistence2stats     Aggregates a token persistence statistics to revision
                        statistics

* wikihadoop2json       Converts a Wikihadoop-processed stream of XML pages to
                        JSON blobs

General utilities:

* json2tsv              Converts a stream of JSON blobs to tab-separated values
                        based a set of /fieldnames/.

* normalize             Normalizes old versions of RevisionDocument json schemas
                        to correspond to the most recent schema version.

* validate              Validates JSON against a provided schema.

* truncate_text         Truncates the 'text' field of JSON blobs to a limited
                        length in unicode characters.  (addresses content dump
                        vandalism issues) and adds a boolean 'truncated' field.

Usage:
    mwstream (-h | --help)
    mwstream <utility> [-h|--help]
"""

import sys
import traceback
from importlib import import_module

import docopt


USAGE = """Usage:
    mwstream (-h | --help)
    mwstream <utility> [-h|--help]\n"""


def main():
    
    if len(sys.argv) < 2:
        sys.stderr.write(USAGE)
        sys.exit(1)
    elif sys.argv[1] in ("-h", "--help"):
        sys.stderr.write(__doc__ + "\n")
        sys.exit(1)
    elif sys.argv[1][:1] == "-":
        sys.stderr.write(USAGE)
        sys.exit(1)
    
    module_name = sys.argv[1]
    try:
        module = import_module("mwstreaming.utilities." + module_name)
    except ImportError as e:
        sys.stderr.write(traceback.format_exc())
        sys.stderr.write("Could not load utility {0}.\n".format(module_name))
        sys.exit(1)
    
    module.main(sys.argv[2:])

if __name__ == "__main__": main()
