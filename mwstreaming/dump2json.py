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
    demo [--validate=<path>] [--threads=<num>] [--verbose] [<dump_file>...]

Options:
    -h|--help          Print this documentation
    --validate=<path>  Validate json output against a schema.  Skip validation
                       if not set.
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

try:
    from jsonschema import validate
except AttributeError: # Happens with pypy
    sys.stderr.write("Notice: Can't validate schemas because pypy is broken.\n")
    validate = lambda d, s: True

def main():
    args = docopt.docopt(__doc__)
    
    if len(args['<dump_file>']) == 0:
        dump_files = []
    else:
        dump_files = args['<dump_file>']
    
    if args['--threads'] == "<cpu_count>":
        threads = cpu_count()
    else:
        threads = int(args['--threads'])
    
    if args['--validate'] is not None:
        schema = json.load(open(args['--validate']))
    else:
        schema = None
    
    verbose = bool(args['--verbose'])
    
    run(dump_files, threads, schema, verbose)

def run(dump_files, threads, schema, verbose):
    
    def process_dump(dump, path):
        
        for revision_doc in dump2json(dump, verbose=verbose):
            if schema is not None: validate(revision_doc, schema)
            yield revision_doc
    
    if len(dump_files) == 0:
        
        revision_docs = process_dump(xml_dump.Iterator.from_file(sys.stdin),
                                     "<stdin>")
        
    else:
        
        revision_docs = xml_dump.map(dump_files, process_dump, threads=threads)
    
        
    for revision_doc in revision_docs:
        json.dump(revision_doc, sys.stdout)
        sys.stdout.write("\n")

def dump2json(dump, verbose=False):
    
    for page in dump:
        
        if verbose: sys.stderr.write(page.title + ": ")
        
        redirect_doc = None
        if page.redirect is not None:
            redirect_doc = {'title': page.redirect.title}
        
        page_doc = {
            'id': page.id,
            'title': page.title,
            'namespace': page.namespace,
            'redirect': redirect_doc,
            'restrictions': page.restrictions
        }
        
        for revision in page:
            
            if verbose: sys.stderr.write(".")
            
            if revision.contributor is not None:
                contributor_doc = {
                    'id': revision.contributor.id,
                    'user_text': revision.contributor.user_text
                }
            else:
                contributor_doc = None
            
            revision_doc = {
                'page': page_doc,
                'id': revision.id,
                'timestamp': revision.timestamp.long_format(),
                'contributor': contributor_doc,
                'minor': revision.minor,
                'comment': str(revision.comment) \
                           if revision.comment is not None \
                           else None,
                'text':str(revision.text) \
                       if revision.text is not None \
                       else None,
                'bytes': revision.bytes,
                'sha1': revision.sha1,
                'parent_id': revision.parent_id,
                'model': revision.model,
                'format': revision.format
            }
            
            yield revision_doc
        
        if verbose: sys.stderr.write("\n")

if __name__ == "__main__": main()
