"""
Converts an XML dump to JSON revision documents.  This script expects either be
given a decompressed dump to <stdin> (single thread) or to have <dump file>s
specified as command-line arguments (multi-threaded).

If no <dump files>s are specified, this script expects to read a decompressed
dump from <stdin>.

$ bzcat pages-meta-history1.xml.bz2 | dump2json | bzip2 -c > revisions.json.bz2

In the case that <dump files>s are specified, this utility can process them
multi-threaded.  You can customize the number of parallel `--threads`.

$ dump2json pages-meta-history*.xml.bz2 | bzip2 -c > revisions.json.bz2

Usage:
    dump2json (-h|--help)
    dump2json [--threads=<num>] [--verbose] [<dump_file>...]

Options:
    -h|--help          Print this documentation
    --threads=<num>    If a collection of files are provided, how many processor
                       threads should be prepare? [default: <cpu_count>]
    --verbose          Print progress information to stderr.  Kind of a mess
                       when running multi-threaded.
"""
import json
import sys
from multiprocessing import cpu_count

import docopt
from mw import xml_dump

from .util import revision2doc


def main(argv=None):
    args = docopt.docopt(__doc__, argv=argv)
    
    if len(args['<dump_file>']) == 0:
        dump_files = []
    else:
        dump_files = args['<dump_file>']
    
    if args['--threads'] == "<cpu_count>":
        threads = cpu_count()
    else:
        threads = int(args['--threads'])
    
    verbose = bool(args['--verbose'])
    
    run(dump_files, threads, verbose)

def run(dump_files, threads, verbose):
    
    if len(dump_files) == 0:
        revision_docs = dump2json(xml_dump.Iterator.from_file(sys.stdin),
                                  verbose=verbose)
        
    else:
        revision_docs = xml_dump.map(dump_files,
                                     lambda d, p: dump2json(d, verbose=verbose),
                                     threads=threads)
    
        
    for revision_doc in revision_docs:
        json.dump(revision_doc, sys.stdout)
        sys.stdout.write("\n")

def dump2json(dump, verbose=False):
    
    for page in dump:
        
        if verbose: sys.stderr.write(page.title + ": ")
        
        for revision in page:
            
            if verbose: sys.stderr.write(".")
            
            yield revision2doc(revision, page)
        
        if verbose: sys.stderr.write("\n")

if __name__ == "__main__": main()
