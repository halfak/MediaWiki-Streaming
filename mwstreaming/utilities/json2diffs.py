"""
Converts a sequence of MediaWiki Dump JSON'd revisions into diffs.  Assumes
that input to <stdin> is partitioned by page (<page.id>) and sorted in the order the
revisions were saved (ORDER BY <timestamp> ASC, <id> ASC).

Produces identical JSON with an additional 'diff' field to <stdout>.  You can
save space with `--drop-text`.

Usage:
    json2diffs (-h|--help)
    json2diffs --config=<path> [--drop-text] [--verbose]

Options:
    --config=<path>    The path to difference detection configuration
    --drop-text        Drops the 'text' field from the JSON blob
    --verbose          Print out progress information
"""
import json
import sys
from itertools import groupby

import docopt
from deltas import DiffEngine

import yamlconf

from .util import op2doc, read_docs


def main(argv=None):
    args = docopt.docopt(__doc__, argv=argv)

    config_doc = yamlconf.load(open(args['--config']))
    diff_engine = DiffEngine.from_config(config_doc, config_doc["diff_engine"])

    drop_text = bool(args['--drop-text'])
    verbose = bool(args['--verbose'])

    run(read_docs(sys.stdin), diff_engine, drop_text, verbose)

def run(revision_docs, diff_engine, drop_text, verbose):

    for revision_doc in json2diffs(revision_docs, diff_engine, verbose):
        if drop_text:
            del revision_doc['text']

        json.dump(revision_doc, sys.stdout)
        sys.stdout.write("\n")

def json2diffs(revision_docs, diff_engine, verbose=False):

    page_revision_docs = groupby(revision_docs, key=lambda r:r['page']['title'])

    for page_title, revision_docs in page_revision_docs:
        if verbose: sys.stderr.write(page_title + ": ")

        processor = diff_engine.processor()
        for revision_doc in revision_docs:
            if verbose: sys.stderr.write("."); sys.stderr.flush()

            # Diff processing uses a lot of CPU.
            operations, a, b = processor.process(revision_doc['text'] or "")
            revision_doc['diff'] = [op2doc(op, a, b) for op in operations]

            yield revision_doc

        if verbose: sys.stderr.write("\n")

if __name__ == "__main__": main()
