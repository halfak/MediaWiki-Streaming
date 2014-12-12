"""
Extracts fields from a JSON blob and places them in-front of the blob in a
TSV.

Usage:
    extract_fields (-h|--help)
    extract_fields [--header] <fieldname>...

Options:
    -h|--help       Print this documentation
    --header        Print out a header row
    <fieldname>...  Fields from the JSON blob to extract as escaped strings
"""
import json
import sys

import docopt

from .util import read_docs


def main():
    args = docopt.docopt(__doc__)
    
    header = bool(args['--header'])
    
    run(read_docs(sys.stdin), args['<fieldname>'], header)

def run(json_docs, fieldnames, header):
    
    if header:
        print("\t".join(encode(fn) for fn in fieldnames))
    
    field_keys = [fn.split('.') for fn in fieldnames]
    
    for doc in json_docs:
        sys.stdout.write("\t".join(encode(apply_keys(doc, keys))
                                   for keys in field_keys))
        sys.stdout.write("\n")

def apply_keys(doc, keys):
    
    if keys == ["-"]:
        return json.dumps(doc)
    else:
        val = doc
        for key in keys:
            if isinstance(val, dict) and key in val:
                val = val[key]
            else:
                return None
            
        return val

def encode(val):
    if isinstance(val, bytes):
        val = str(val, "utf-8", "replace")
    elif val is None:
        return "NULL"
    else:
        val = str(val)
    
    return val.replace("\t", "\\t").replace("\n", "\\n")

if __name__ == "__main__": main()
