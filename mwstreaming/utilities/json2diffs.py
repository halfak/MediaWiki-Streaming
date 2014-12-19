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
from deltas.detectors import Detector
from deltas.tokenizers import Tokenizer

import yamlconf

from .util import read_docs


def main(argv=None):
    args = docopt.docopt(__doc__, argv=argv)
    
    config_doc = yamlconf.load(open(args['--config']))
    detector = Detector.from_config(config_doc, config_doc['detector'])
    tokenizer = Tokenizer.from_config(config_doc, config_doc['tokenizer'])
    
    drop_text = bool(args['--drop-text'])
    verbose = bool(args['--verbose'])
    
    run(read_docs(sys.stdin), detector, tokenizer, drop_text, verbose)

def run(revision_docs, detector, tokenizer, drop_text, verbose):
    
    for revision_doc in json2diffs(revision_docs, detector, tokenizer, verbose):
        if drop_text:
            del revision_doc['text']
        
        json.dump(revision_doc, sys.stdout)
        sys.stdout.write("\n")

def json2diffs(revision_docs, detector, tokenizer, verbose):
    
    page_revision_docs = groupby(revision_docs, key=lambda r:r['page']['title'])
    
    for page_title, revision_docs in page_revision_docs:
        
        if verbose: sys.stderr.write(page_title + ": ")
        
        last_tokens = []
        for revision_doc in revision_docs:
            if verbose: sys.stderr.write("."); sys.stderr.flush()
            
            # Diff detection uses a lot of CPU.  This will be the hottest part
            # of the code.
            tokens = tokenizer.tokenize(revision_doc['text'] or "")
            operations = detector.diff(last_tokens, tokens)
            
            revision_doc['diff'] = [op2doc(op, last_tokens, tokens)
                                    for op in operations]
            
            yield revision_doc
            
            last_tokens = tokens
        
        
        if verbose: sys.stderr.write("\n")

def op2doc(operation, a, b):
    
    name, a1, a2, b1, b2 = operation
    doc = {
        'name': name,
        'a1': a1,
        'a2': a2,
        'b1': b1,
        'b2': b2
    }
    if name == "insert": doc['tokens'] = b[b1:b2]
    elif name == "delete": doc['tokens'] = a[a1:a2]
    else: pass
    
    return doc

if __name__ == "__main__": main()
