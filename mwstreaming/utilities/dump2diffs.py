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
from deltas.detectors import Detector
from deltas.tokenizers import Tokenizer
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
    detector = Detector.from_config(config_doc, config_doc['detector'])
    tokenizer = yamlconf.import_module(config_doc['tokenizer'])
    
    drop_text = bool(args['--drop-text'])
    
    if args['--threads'] == "<cpu_count>":
        threads = cpu_count()
    else:
        threads = int(args['--threads'])
    
    verbose = bool(args['--verbose'])
    
    run(dump_files, detector, tokenizer, threads, verbose)

def run(dump_files, detector, tokenizer, threads, verbose):
    
    if len(dump_files) == 0:
        revision_docs = dump2diffs(xml_dump.Iterator.from_file(sys.stdin),
                                   detector, tokenizer, verbose=verbose)
        
    else:
        dump_processor = lambda d, p: dump2diffs(d, detector, tokenizer,
                                                 verbose=verbose)
        revision_docs = xml_dump.map(dump_files, dump_processor,
                                     threads=threads)
    
        
    for revision_doc in revision_docs:
        json.dump(revision_doc, sys.stdout)
        sys.stdout.write("\n")

def dump2diffs(dump, detector, tokenizer, verbose):
    
    for page in dump:
        
        if verbose: sys.stderr.write(page.title + ": ")
        
        last_tokens = []
        for revision in page:
            revision_doc = revision2doc(revision, page)
            if verbose: sys.stderr.write("."); sys.stderr.flush()
            
            # Diff detection uses a lot of CPU.  This will be the hottest part
            # of the code.
            tokens = list(tokenizer.tokenize(revision_doc['text'] or ""))
            operations = detector.diff(last_tokens, tokens)
            
            revision_doc['diff'] = [op2doc(op, last_tokens, tokens)
                                    for op in operations]
            
            yield revision_doc
            
            last_tokens = tokens
        
        if verbose: sys.stderr.write("\n")

if __name__ == "__main__": main()
