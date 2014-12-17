"""
Validates a stream of JSON documents against a JSON schema and writes them to
stdout if they validate -- otherwise, complains noisily.

Usage:
    validate (-h|--help)
    validate <schema>

Options:
    -h|--help      Print this documentation
    <schema>       The path of a JSON schema to use for validation
"""
import json
import sys

import docopt

from jsonschema import validate

from .util import read_docs


def main(argv=None):
    args = docopt.docopt(__doc__, argv=argv)
    
    schema = json.load(open(args['<schema>']))
    
    run(read_docs(sys.stdin), schema)

def run(docs, schema):
    
    for doc in jsonvalidate(docs, schema):
        json.dump(doc, sys.stdout)
        sys.stdout.write("\n")

def jsonvalidate(docs, schema):
    for doc in docs:
        validate(doc, schema)
        yield doc
