"""
Converts a sequence of MediaWiki Dump JSON'd revisions into diffs.  Assumes
that input to <stdin> is partitioned by page (<page.id>) and sorted in the order the
revisions were saved (<timestamp> ASC, <id> ASC).

Produces identical JSON with a new 'diff' field to <stdout>.

Usage:
    ./json2diffs --config=<path> [--drop-text]
"""
import json
import sys
from itertools import groupby

import docopt
from deltas.detectors import Detector
from deltas.tokenizers import Tokenizer

import yamlconf


def read_revisions(f):
    for line in f:
        yield json.loads(line.strip())


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

def main():
    args = docopt.docopt(__doc__)
    
    config_doc = yamlconf.load(open(args['--config']))
    drop_text = bool(args['--drop-text'])
    
    detector = Detector.from_config(config_doc, config_doc['detector'])
    tokenizer = Tokenizer.from_config(config_doc, config_doc['tokenizer'])
    
    run(read_revisions(sys.stdin), detector, tokenizer, drop_text)

def run(revisions, detector, tokenizer, drop_text):
    
    page_revisions = groupby(revisions, key=lambda r:r['page']['id'])
    
    for page_id, revisions in page_revisions:
        
        last_tokens = []
        for revision in revisions:
            
            # Diff detection uses a lot of CPU.  This will be the hottest part
            # of the code.
            print(revision)
            tokens = tokenizer.tokenize(revision['text'] or "")
            operations = detector.diff(last_tokens, tokens)
            
            # Drop the text field
            if drop_text: del revision['text']
            
            revision['diff'] = [op2doc(op, last_tokens, tokens)
                                for op in operations]
            
            print(json.dumps(revision))
            
            last_tokens = tokens

if __name__ == "__main__": main()
