"""
Converts a sequence of MediaWiki Dump JSON'd revisions into diffs.  Assumes
that input to <stdin> is partitioned by page (<page.id>) and sorted in the order the
revisions were saved (ORDER BY <timestamp> ASC, <id> ASC).

Produces identical JSON with an additional 'diff' field to <stdout>.  You can
save space with `--drop-text`.

Usage:
    json2diffs (-h|--help)
    json2diffs --config=<path> [--drop-text] [--diff-timeout=<secs>]
                               [--namespaces=<ns>] [--verbose]

Options:
    --config=<path>        The path to difference detection configuration
    --drop-text            Drops the 'text' field from the JSON blob
    --diff-timeout=<secs>  The maximum time a diff can run in seconds before
                           being cancelled.  [default: <infinity>]
    --namespaces=<ns>      A comma separated list of page namespaces to be
                           processed [default: <all>]
    --verbose              Print out progress information
"""
import json
import sys
import time
from itertools import groupby

import docopt
from deltas import DiffEngine
from stopit import ThreadingTimeout as Timeout
from stopit import TimeoutException

import yamlconf

from .util import op2doc, read_docs


def main(argv=None):
    args = docopt.docopt(__doc__, argv=argv)

    config_doc = yamlconf.load(open(args['--config']))
    diff_engine = DiffEngine.from_config(config_doc, config_doc["diff_engine"])

    drop_text = bool(args['--drop-text'])

    if args['--diff-timeout'] == "<infinity>":
        timeout = None
    else:
        timeout = float(args['--diff-timeout'])

    if args['--namespaces'] == "<all>":
        namespaces = None
    else:
        namespaces = set(int(ns) for ns in args['--namespaces'].split(","))

    verbose = bool(args['--verbose'])

    run(read_docs(sys.stdin), diff_engine, timeout, namespaces, drop_text, verbose)

def run(revision_docs, diff_engine, timeout, namespaces, drop_text, verbose):

    revision_docs = json2diffs(revision_docs, diff_engine, timeout, namespaces,
                               verbose)
    for revision_doc in revision_docs:
        if drop_text:
            del revision_doc['text']

        json.dump(revision_doc, sys.stdout)
        sys.stdout.write("\n")

def json2diffs(revision_docs, diff_engine, timeout=None, namespaces=None, verbose=False):

    relevant_revision_doc = \
        (r for r in revision_docs
           if (namespaces is None or r['page']['namespace'] in namespaces))
    page_revision_docs = groupby(relevant_revision_doc,
                                 key=lambda r:r['page']['title'])

    for page_title, revision_docs in page_revision_docs:
        if verbose: sys.stderr.write(page_title + ": ")

        processor = diff_engine.processor()
        for revision_doc in revision_docs:

            # Diff processing uses a lot of CPU.  So we set a timeout for
            # crazy revisions and record a timer for analysis later.
            if timeout is None:
                with Timer() as t:
                    operations, a, b = \
                            processor.process(revision_doc['text'] or "")
            else:
                try:
                    with Timeout(timeout) as ctx, Timer() as t:
                        operations, a, b = \
                                processor.process(revision_doc['text'] or "")
                except TimeoutException:
                    pass

            revision_doc['diff_stats'] = {'time': t.interval}
            if ctx.state != ctx.TIMED_OUT:
                revision_doc['diff'] = [op2doc(op, a, b) for op in operations]
                if verbose: sys.stderr.write("."); sys.stderr.flush()
            else:
                # We timed out.  That means we don't have operations to record
                revision_doc['diff'] = None

                # We also need to make sure that the processor state is right
                processor.update(last_text=(revision_doc['text'] or ""))
                if verbose: sys.stderr.write("T"); sys.stderr.flush()


            yield revision_doc

        if verbose: sys.stderr.write("\n")

class Timer:
    """
    From:
    http://preshing.com/20110924/timing-your-code-using-pythons-with-statement/
    """
    def __enter__(self):
        self.start = time.clock()
        self.interval = None
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start


if __name__ == "__main__": main()
