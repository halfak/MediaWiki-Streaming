"""
Adds missing diff operations to revision documents by looking for documents
with diff.ops == null.  This script uses the API to gather text.

Usage:
    add_missing_diffs -h | --help
    add_missing_diffs --api=<url> --config=<config> [--verbose]

Options:
    -h --help        Prints this documentation
    --api=<url>      URL of a MediaWiki API to request data from
    --config=<path>  The path to difference detection configuration
    --verbose        Print progress information to stderr
"""
import json
import sys

import docopt
from deltas import DiffEngine
from mw import api

import yamlconf

from .util import op2doc, read_docs


def main(argv=None):
    args = docopt.docopt(__doc__, argv=argv)

    diff_docs = read_docs(sys.stdin)

    session = api.Session(args['--api'])

    config_doc = yamlconf.load(open(args['--config']))
    diff_engine = DiffEngine.from_config(config_doc, config_doc["diff_engine"])

    run(diff_docs, session, diff_engine)

def run(diff_docs, session, diff_engine):

    for diff_doc in diff_docs:
        if 'diff' not in diff_doc:
            raise Exception("Documents must have a 'diff' field.")

        if diff_doc['diff']['ops'] is None:
            sys.stderr.write("\nProcessing {0}: ... ".format(diff_doc['id']))
            sys.stderr.flush()
            diff = generate_diff(diff_doc, session, diff_engine)
            diff_doc['diff'] = diff
            sys.stderr.write("DONE!\n");sys.stderr.flush()
        else:
            sys.stderr.write(".");sys.stderr.flush()

        json.dump(diff_doc, sys.stdout)
        sys.stdout.write("\n")

def generate_diff(diff_doc, session, diff_engine):
    last_id = diff_doc['diff']['last_id']
    rev_ids = [diff_doc['id']]
    if last_id is not None:
        rev_ids.append(last_id)

    texts = {r['revid']:r.get('*', "") for r in
             session.revisions.query(revids=rev_ids, properties={'ids',"content"})}

    processor = diff_engine.processor(last_text=texts.get(last_id))

    diff = diff_doc['diff']
    operations, a, b = processor.process(texts[diff_doc['id']])
    diff['ops'] = [op2doc(op, a, b) for op in operations]

    return diff
