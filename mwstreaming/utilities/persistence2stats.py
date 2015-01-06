r"""
Aggregates a stream of token persistence stats into revision statistics.
RevisionDocument JSON blobs are printed to <stdout> with an additional
'stats' field.

TODO: Include time visible cutoff

Usage:
    persistence2stats (-h | --help)
    persistence2stats [--min-persisted=<num>] [--min-visible=<days>]
                         [--include=<regex>] [--exclude=<regex>]
                         [--verbose]

Options:
    -h|--help              Print this documentation
    --min-persisted=<num>  The minimum number of revisions a token must
                           survive before being considered "persisted"
                           [default: 5]
    --min-visible=<days>   The minimum amount of time a token must survive
                           before being considered "persisted" (in days)
                           [default: 14]
    --include=<regex>      A regex matching tokens to include
                           [default: <all>]
    --exclude=<regex>      A regex matching tokens to exclude
                           [default: <none>]
    --verbose              Print out progress information
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
    
    min_persisted = int(args['--min-persisted'])
    min_visible = float(args['--min-visible'])
    min_visible_secs = min_visible * (60*60*24)
    
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
    
    run(read_docs(sys.stdin), min_persisted, min_visible_secs, include, exclude, verbose)

def run(persistence_docs, min_persisted, min_visible_secs, include, exclude, verbose):
    
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
            
            # Look for time threshold
            if persistence_doc['seconds_visible'] >= min_visible_secs:
                stats_doc['tokens_persisted'] += 1
                stats_doc['tokens_non_self_persisted'] += 1
            else:
                # Look for review threshold
                stats_doc['tokens_persisted'] += \
                        persistence_doc['persisted'] >= min_persisted
                
                stats_doc['tokens_non_self_persisted'] += \
                        persistence_doc['non_self_persisted'] >= min_persisted
                
                # Check for censoring
                if persistence_doc['seconds_possible'] < min_visible_secs:
                    stats_doc['censored'] = True
                    stats_doc['non_self_censored'] = True
                    
                else:
                    if persistence_doc['processed'] < min_persisted:
                        stats_doc['censored'] = True
                    
                    if persistence_doc['non_self_processed'] < min_persisted:
                        stats_doc['non_self_censored'] = True
            
        if verbose: sys.stderr.write("\n")
        
        revision_doc['persistence_stats'] = stats_doc
        
        json.dump(revision_doc, sys.stdout)
        sys.stdout.write("\n")
