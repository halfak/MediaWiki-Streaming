"""
Mends revision diffs.  This script will take a sequence of revisions with diff
information that were generated in blocks and mend the missing diff information
at the block seams.  This utility is useful when used in conjunction with
json2diffs in in a hadoop setting with json2diffs as the mapper and mend_diffs
as the reducer.

Usage:
    mend_diffs (-h|--help)
    mend_diffs --config=<path> [--drop-text] [--timeout=<secs>]
                               [--verbose]

Options:
    --config=<path>        The path to difference detection configuration
    --drop-text            Drops the 'text' field from the JSON blob
    --timeout=<secs>       The maximum time a diff can run in seconds before
                           being cancelled.  [default: <infinity>]
    --namespaces=<ns>      A comma separated list of page namespaces to be
                           processed [default: <all>]
    --verbose              Print out progress information
"""
import json
import sys
from itertools import groupby

import docopt
from deltas import DiffEngine
from more_itertools import peekable

import yamlconf

from .json2diffs import diff_revisions
from .util import read_docs


def main(argv=None):
    args = docopt.docopt(__doc__, argv=argv)

    config_doc = yamlconf.load(open(args['--config']))
    diff_engine = DiffEngine.from_config(config_doc, config_doc["diff_engine"])

    drop_text = bool(args['--drop-text'])

    if args['--timeout'] == "<infinity>":
        timeout = None
    else:
        timeout = float(args['--timeout'])

    verbose = bool(args['--verbose'])

    run(read_docs(sys.stdin), diff_engine, timeout, drop_text, verbose)

def run(diff_docs, diff_engine, timeout, drop_text, verbose):

    for mended_doc in mend_diffs(diff_docs, diff_engine, timeout, verbose):
        if drop_text:
            del mended_doc['text']

        json.dump(mended_doc, sys.stdout)
        sys.stdout.write("\n")

def mend_diffs(diff_docs, diff_engine, timeout=None, verbose=False):

    page_diff_docs = groupby(diff_docs, key=lambda r:r['page']['title'])

    for page_title, page_docs in page_diff_docs:
        if verbose: sys.stderr.write(page_title + ": ")

        page_docs = peekable(page_docs)

        while page_docs.peek(None) is not None:

            diff_doc = next(page_docs)

            if 'text' not in diff_doc:
                raise RuntimeError("Revision documents must contain a 'text' " +
                                   "field for mending.")
            elif 'diff' not in diff_doc:
                raise RuntimeError("Revision documents must contain a 'diff' " +
                                   "field for mending.")

            last_text = diff_doc['text']
            yield diff_doc
            if verbose: sys.stderr.write(".");sys.stderr.flush()

            # Check if we're going to need to mend the next revision
            if page_docs.peek(None) is not None and \
               page_docs.peek()['diff']['last_id'] != diff_doc['id']:
                processor = diff_engine.processor(last_text=last_text)
                broken_docs = read_broken_docs(page_docs)
                mended_docs = diff_revisions(broken_docs, processor,
                                             last_id=diff_doc['id'],
                                             timeout=timeout)

                for mended_doc in mended_docs:
                    yield mended_doc
                    if verbose: sys.stderr.write("M");sys.stderr.flush()


        if verbose: sys.stderr.write("\n")

    if verbose: sys.stderr.write("\n")


def read_broken_docs(page_docs):
    """
    Reads broken diff docs.  This method assumes that the first doc was already
    determined to be broken.
    """
    page_doc = next(page_docs)
    yield page_doc

    while page_docs.peek(None) is not None and \
          page_docs.peek()['diff']['last_id'] != page_doc['id']:

          page_doc = next(page_docs)
          yield page_doc
