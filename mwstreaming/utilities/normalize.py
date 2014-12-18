"""
Converts a stream of RevisionDocument JSON blobs to JSON blobs that will
validate against the latest schema (v0.0.2).

Usage:
    normalize (-h | --help)
    normalize

Options:
    -h|--help          Prints this documentation
"""
import json
import sys

import docopt

from .util import read_docs


def main(argv=None):
    args = docopt.docopt(__doc__, argv=argv)
    
    run(read_docs(sys.stdin), schema)
    

def run(revision_docs):
    
    for revision_doc in normalize(revision_docs):
        json.dump(revision_doc, sys.stdout)
        sys.stdout.write("\n")

def normalize(revision_docs):
    
    for revision_doc in revision_docs:
        
        # Converts page.redirect.title to page.redirect_title
        if 'redirect' in revision_doc['page']:
            if revision_doc['page']['redirect'] is not None:
                redirect_title = revision_doc['page']['redirect']['title']
            else:
                redirect_title = None
            
            del revision_doc['page']['redirect']
            
            revision_doc['page']['redirect_title'] = redirect_title
        
        yield revision_doc
