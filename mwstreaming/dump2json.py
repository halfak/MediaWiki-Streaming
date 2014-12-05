"""
Converts an XML dump to JSON revision documents.  This script expects to be
a MediaWiki XML dump to <stdin> and will write revision JSON documents to
<stdout>

Usage:
    dump2json [--verbose] [--validate=<path>]

Options:
    -h|--help          Print this documentation
    --verbose          Print progress information to stderr
    --validate=<path>  Validate json output against a schema.  Skip validation
                       if not set.
"""
import json
import sys

import docopt
from mw import xml_dump

from jsonschema import validate


def main():
    args = docopt.docopt(__doc__)
    
    input = sys.stdin
    output = sys.stdout
    
    verbose = bool(args['--verbose'])
    if args['--validate'] is not None:
        schema = json.load(open(args['--validate']))
    else:
        schema = None
    
    run(input, output, verbose, schema)

def run(input, output, verbose, schema):
    
    dump = xml_dump.Iterator.from_file(input)
    
    for revision_doc in dump2json(dump, verbose=verbose):
        if schema is not None: validate(revision_doc, schema)
        json.dump(revision_doc, output)
        output.write("\n")

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
