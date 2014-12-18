"""
Converts Wikihadoop XML page pairs to JSON revision documents.

Usage:
    wikihadoop2json (-h | --help)
    wikihadoop2json [--validate=<path>] [--verbose]

Options:
    -h|--help          Print this documentation
    --verbose          Print progress information to stderr.  Kind of a mess
                       when running multi-threaded.
"""
import json
import sys

import docopt
from mw import xml_dump

from .util import revision2doc


def main(argv=None):
    args = docopt.docopt(__doc__, argv=argv)
    
    verbose = bool(args['--verbose'])
    
    run(verbose)

def run(verbose):
    
    dump = xml_dump.Iterator.from_page_xml(sys.stdin)
        
    for revision_doc in wikihadoop2json(dump, verbose=verbose):
        json.dump(revision_doc, sys.stdout)
        sys.stdout.write("\n")

def wikihadoop2json(dump, verbose=False):
    
    for page in dump:
        
        if verbose: sys.stderr.write(page.title + ": ")
        
        revisions = [r for r in page]
        
        if len(revisions) == 2:
            revision = revisions[1]
            
            if verbose: sys.stderr.write(".")
            
            yield revision2doc(revision, page)
        
        if verbose: sys.stderr.write("\n")

if __name__ == "__main__": main()
