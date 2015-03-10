"""
Computes diffs from an XML dump.  This script expects either be
given a decompressed dump to <stdin> (single thread) or to have <dump file>s
specified as command-line arguments (multi-threaded).

If no <dump files>s are specified, this script expects to read a decompressed
dump from <stdin>.

$ bzcat dump.xml.bz2 | dump2diffs --config=conf.yaml > diffs.json

In the case that <dump files>s are specified, this utility can process them
multi-threaded.  You can customize the number of parallel `--threads`.

$ dump2diffs pages-meta-history*.xml.bz2 --config=conf.yaml > diffs.json

Usage:
    dump2diffs (-h|--help)
    dump2diffs [<dump_file>...] --config=<path> [--drop-text] [--threads=<num>]
                                               [--verbose]

Options:
    -h|--help          Print this documentation
    --config=<path>    The path to difference detection configuration
    --drop-text        Drops the 'text' field from the JSON blob
    --threads=<num>    If a collection of files are provided, how many processor
                       threads should be prepare? [default: <cpu_count>]
    --verbose          Print progress information to stderr.  Kind of a mess
                       when running multi-threaded.
"""
import json
import sys
from multiprocessing import cpu_count

import docopt
from deltas import DiffEngine
from mw import xml_dump

import yamlconf

from .util import op2doc, revision2doc


def main(argv=None):
    args = docopt.docopt(__doc__, argv=argv)

    if len(args['<dump_file>']) == 0:
        dump_files = []
    else:
        dump_files = args['<dump_file>']

    config_doc = yamlconf.load(open(args['--config']))
    diff_engine = DiffEngine.from_config(config_doc, config_doc['diff_engine'])

    drop_text = bool(args['--drop-text'])

    if args['--threads'] == "<cpu_count>":
        threads = cpu_count()
    else:
        threads = int(args['--threads'])

    verbose = bool(args['--verbose'])

    run(dump_files, diff_engine, threads, drop_text, verbose)

def run(dump_files, diff_engine, threads, drop_text, verbose):

    if len(dump_files) == 0:
        revision_docs = dump2diffs(xml_dump.Iterator.from_file(sys.stdin),
                                   diff_engine, verbose=verbose)

    else:
        dump_processor = lambda d, p: dump2diffs(d, diff_engine,
                                                 verbose=verbose)
        revision_docs = xml_dump.map(dump_files, dump_processor,
                                     threads=threads)


    for revision_doc in revision_docs:
        if drop_text:
            del revision_doc['text']

        json.dump(revision_doc, sys.stdout)
        sys.stdout.write("\n")

def dump2diffs(dump, diff_engine, verbose=False):

    for page in dump:

        if verbose: sys.stderr.write(page.title + ": ")

        processor = diff_engine.processor()
        for revision in page:
            revision_doc = revision2doc(revision, page)
            if verbose: sys.stderr.write("."); sys.stderr.flush()

            # Diff processing uses a lot of CPU.
            operations, a, b = processor.process(revision_doc['text'] or "")

            revision_doc['diff'] = [op2doc(op, a, b) for op in operations]

            yield revision_doc

        if verbose: sys.stderr.write("\n")

if __name__ == "__main__": main()
