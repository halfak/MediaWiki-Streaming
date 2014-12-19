r"""
Aggregates a stream of token persistence stats into revision statistics.
RevisionDocument JSON blobs are printed to <stdout> with an additional
'stats' field.

TODO: Include time visible cutoff

Usage:
    persistence2revstats (-h | --help)
    persistence2revstats [--min-persistence=<num>]
                         [--include=<regex>] [--exclude=<regex>]
                         [--verbose]

Options:
    -h|--help                Print this documentation
    --min-persistence=<num>  The minimum number of revisions a token must
                             survive before being considered "persisted"
                             [default: 5]
    --include=<regex>        A regex matching tokens to include
                             [default: <all>]
    --exclude=<regex>        A regex matching tokens to exclude
                             [default: <none>]
    --verbose                Print out progress information
"""
import json
import sys
from itertools import groupby
from math import log

import docopt

from .util import read_docs


def main(argv=None):
    args = docopt.docopt(__doc__, argv=argv)
    
    verbose = bool(args['--verbose'])
    
    min_persistence = int(args['--min-persistence'])
    
    if args['--include'] == "<all>":
        include = lambda t: True
    else:
        include_re = re.compile(args['--include'], re.UNICODE)
        include = lambda t: bool(include_re.search(t))
        
    if args['--exclude'] == "<none>":
        exclude = lambda t: False
    else:
        exclude_re = re.compile(args['--exclude'], re.UNICODE)
        exclude = lambda t: bool(exclude_re.search(t))
    
    run(read_docs(sys.stdin), min_persistence, include, exclude, verbose)

def run(persistence_docs, min_persistence, include, exclude, verbose):
    
    revision_persistence_docs = groupby(persistence_docs,
                                        key=lambda p:p['revision'])
    
    for revision_doc, persistence_docs in revision_persistence_docs:
        if verbose:
            sys.stderr.write("{0} ({1}): " \
                             .format(revision_doc['page']['title']),
                                     revision_doc['id'])
        stats_doc = {
            'tokens_added': 0,
            'tokens_persisted': 0,
            'tokens_non_self_persisted': 0,
            'sum_log_persisted': 0,
            'sum_log_non_self_persisted': 0,
            'censored': False,
            'non_self_censored': False
        }
        
        filtered_docs = (p for p in persistence_docs
                         if include(p['token']) and not exclude(p['token']))
        for persistence_doc in filtered_docs:
            if verbose: sys.stderr.write(".")
            
            stats_doc['tokens_added'] += 1
            stats_doc['sum_log_persisted'] += log(persistence_doc['persisted']+1)
            stats_doc['sum_log_non_self_persisted'] += \
                    log(persistence_doc['non_self_persisted']+1)
            
            # Count persisting and check for censoring
            if persistence_doc['persisted'] >= min_persistence:
                stats_doc['tokens_persisted'] += 1
            elif persistence_doc['processed'] < min_persistence:
                stats_doc['censored'] = True
            
            # Count non-self persistence and check for censoring
            if persistence_doc['non_self_persisted'] >= min_persistence:
                stats_doc['tokens_non_self_persisted'] += 1
            elif persistence_doc['non_self_processed'] < min_persistence:
                stats_doc['non_self_censored'] = True
            
        if verbose: sys.stderr.write("\n")
        
        revision_doc['stats'] = stats_doc
        
        json.dump(revision_doc, sys.stdout)
        sys.stdout.write("\n")
