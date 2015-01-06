r"""
Generates token persistence statistics by reading revision diffs and applying
them to a token list.

Expects to get revision diff JSON blobs via <stdin> that are partitioned by
page_id and otherwise sorted chronologically.  Outputs token persistence
statistics JSON blobs.

Uses a 'window' to limit memory usage.  New revisions enter the head of the
window and old revisions fall off the tail.  Stats are generated at the tail of
the window.

::
                           window
                      .------+------.
                                         
    revisions ========[=============]=============>
                                    
                    /                \
                [tail]              [head]


Usage:
    diffs2persistence (-h|--help)
    diffs2persistence --sunset=<date>
                      [--window=<revs>] [--revert-radius=<revs>]
                      [--keep-diff] [--verbose]
    
Options:
    -h|--help                Prints this documentation
    --sunset=<date>          The date of the database dump we are generating
                             from.  This is used to apply a 'time visible'
                             statistic.  Expects %Y-%m-%dT%H:%M:%SZ".
    --window=<revs>          The size of the window of revisions from which
                             persistence data will be generated.
                             [default: 50]
    --revert-radius=<revs>   The number of revisions back that a revert can
                             reference. [default: 15]
                             [default: <now>]
    --keep-diff              Do not drop 'diff' field data from the json blobs.
    --verbose                Print out progress information
"""
import json
import sys
import time
from collections import deque
from itertools import groupby

import docopt
from mw import Timestamp
from mw.lib import reverts

from .util import read_docs


def main(argv=None):
    args = docopt.docopt(__doc__, argv=argv)
    
    window_size = int(args['--window'])
    
    revert_radius = int(args['--revert-radius'])
    
    if args['--sunset'] == "<now>":
        sunset = Timestamp(time.time())
    else:
        sunset = Timestamp(args['--sunset'])
    
    keep_diff = bool(args['--keep-diff'])
    verbose = bool(args['--verbose'])
    
    run(read_docs(sys.stdin), window_size, revert_radius, sunset, keep_diff, verbose)

def run(diff_docs, window_size, revert_radius, sunset, keep_diff, verbose):
    
    for doc, token_stats in token_persistence(diff_docs, window_size,
                                              revert_radius, sunset, verbose):
        for ts in token_stats:
            if not keep_diff: doc.pop("diff", None)
            ts['revision'] = doc
            json.dump(ts, sys.stdout)
            sys.stdout.write("\n")
    
def token_persistence(diff_docs, window_size, revert_radius, sunset, verbose):
    page_diff_docs = groupby(diff_docs, key=lambda d: d['page']['title'])
    
    for page_title, diff_docs in page_diff_docs:
        
        if verbose: sys.stderr.write(page_title + ": ")
        
        revert_detector = reverts.Detector(revert_radius)
        last_tokens = Tokens()
        window = deque(maxlen=window_size)
        
        for doc in diff_docs:
            if verbose: sys.stderr.write(".")
            
            # Check for revert
            revert = revert_detector.process(doc['sha1'], doc)
            if revert is None:
                tokens, tokens_added, tokens_removed = \
                        last_tokens.apply(doc['diff'])
                
            else:
                _, _, revert_to = revert
                #sys.stderr.write(str(revert_to) + "\n")
                tokens = revert_to['tokens']
                tokens_added = Tokens(set(tokens) - set(last_tokens))
                tokens_removed = Tokens(set(last_tokens) - set(tokens))
                
            
            # Makes this available when the revision is reverted back to.
            doc['tokens'] = tokens
            
            # Mark the new tokens visible
            tokens_added.visible_at(doc['timestamp'])
            
            # Mark the removed tokens as invisible
            tokens_removed.invisible_at(doc['timestamp'])
            
            tokens.persist(doc['contributor'])
            
            if len(window) == window_size: # Time to start writing some stats
                old_doc, old_added = window[0]
                window.append((doc, tokens_added))
                del old_doc['tokens']
                yield old_doc, generate_stats(old_doc, old_added, window, None)
            else:
                window.append((doc, tokens_added))
            
            last_tokens = tokens # THIS LINE IS SUPER IMPORTANT.  NOTICE ME!
                
            
        while len(window) > 0:
            old_doc, old_added = window.popleft()
            del old_doc['tokens']
            yield old_doc, generate_stats(old_doc, old_added, window, sunset)
        
        if verbose: sys.stderr.write("\n")
    

def generate_stats(doc, tokens_added, window, sunset):
    revisions_processed = len(window)
    
    if sunset is None:
        sunset = window[-1][0]['timestamp'] # Use the last revision in the window
    
    seconds_possible = max(Timestamp(sunset) -
                           Timestamp(doc['timestamp']), 0)
    
    for token in tokens_added:
        non_self_persisted = sum(doc['contributor'] != c
                                 for c in token.revisions)
        non_self_processed = sum(doc['contributor'] != d['contributor']
                                 for d, ts in window)
        yield {
            "token": str(token),
            "persisted": len(token.revisions[1:]),
            "processed": revisions_processed,
            "non_self_persisted": non_self_persisted,
            "non_self_processed": non_self_processed,
            "seconds_visible": token.seconds_visible(sunset),
            "seconds_possible": seconds_possible
        }


class Tokens(list):

    def persist(self, revision):
        for token in self:
            token.persist(revision)
            
    
    def visible_at(self, timestamp):
        for token in self:
            token.visible_at(timestamp)
        
    
    def invisible_at(self, timestamp):
        for token in self:
            token.invisible_at(timestamp)
        
    
    def apply(self, operations):
        tokens = Tokens()
        tokens_added = Tokens()
        tokens_removed = Tokens()
        
        for op in operations:
            
            if op['name'] == "insert":
                
                new_tokens = [Token(t) for t in op['tokens']]
                tokens.extend(new_tokens)
                tokens_added.extend(new_tokens)
            
            elif op['name'] == "replace":
                
                new_tokens = [Token(t) for t in op['tokens']]
                tokens.extend(new_tokens)
                tokens_added.extend(new_tokens)
                
                tokens_removed.extend(self[op['a1']:op['a2']])
            
            elif op['name'] == "delete":
                tokens_removed.extend(self[op['a1']:op['a2']])
                
            elif op['name'] == "equal":
                tokens.extend(self[op['a1']:op['a2']])
                
            else:
                assert False, \
                       "encounted an unrecognized operation code: " + \
                       repr(op['op'])
            
        
        return (tokens, tokens_added, tokens_removed)
    

class Token(str):
    
    def __new__(cls, string, revisions=None, visible=0, visible_since=None):
        inst = super().__new__(cls, string)
        inst.initialize(string, revisions or [], visible, visible_since)
        return inst
    
    def __init__(self, *args, **kawrgs): pass
    
    def initialize(self, string, revisions, visible, visible_since):
        self.revisions = revisions if revisions is not None else []
        self.visible = visible
        self.visible_since = visible_since
        
    def visible_at(self, timestamp):
        if self.visible_since is None:
            self.visible_since = Timestamp(timestamp)
    
    def persist(self, revision):
        self.revisions.append(revision)
    
    def invisible_at(self, timestamp):
        timestamp = Timestamp(timestamp)
        if self.visible_since is not None:
            self.visible += max(timestamp - self.visible_since, 0)
        else:
            # This happens with diff algorithms that will detect content
            # duplication
            pass
        
        self.visible_since = None
    
    def seconds_visible(self, sunset):
        sunset = Timestamp(sunset)
        if self.visible_since != None:
            return self.visible + (sunset - self.visible_since)
        else:
            return self.visible
    
    def __hash__(self):
        return id(self)
    
    def __repr__(self):
        return "{0}({1}, {2})".format(self.__class__.__name__,
                                      repr(str(self)),
                                      self.revisions,
                                      self.visible,
                                      self.visible_since)


if __name__ == "__main__": main()
