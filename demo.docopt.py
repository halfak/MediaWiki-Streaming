"""
Converts an XML dump to JSON revision documents.  This script expects either be
given a decompressed dump to <stdin> (single thread) or to have <dump file>s
specified as command-line arguments (multi-threaded).

If no <dump files>s are specified, this script expects to read a decompressed
dump to <stdin> and write JSON documents to <stdout>.

$ bzcat enwiki-pages-meta-history1.xml.bz2 | dump2json | bzip2 -c > enwiki.json

In the case that <dump files>s are specified, this utility can process them
multi-threaded.  You can customize the number of parallel `--threads`.

$ dump2json enwiki-pages-meta-history*.xml.bz2

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
import sys

import docopt

print(docopt.docopt(__doc__))
