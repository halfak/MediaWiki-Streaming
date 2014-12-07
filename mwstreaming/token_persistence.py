"""
Generates token persistence statistics by reading revision diffs and applying
them to a token list.

Expects to get revision diff JSON blobs via <stdin> that are partitioned by
page_id and otherwise sorted chronologically.  Outputs token persistence JSON
blobs.

Uses a 'window' to limit memory usage.  New revisions enter the beginning of the
window and old revisions fall off the end.

::
                           window
                      .------+------.
                                         
    revisions ========[=============]=============>
                                    
                    /                \
                [tail]              [head]


Usage:
    token_persistence [--window=<revs>] [--revert-radius=<revs>]
                      [--sunset=<date>] [--verbose]
    
Options:
    -h|--help                Prints this documentation
    --window=<revs>          The size of the window of revisions from which
                             persistence data will be generated.
    --revert-radius=<revs>   The number of revisions back that a revert can
                             reference.
    --sunset=<date>          The date of the database dump we are generating
                             from.  This is used to apply a 'time visible'
                             statistic.  Expects %Y-%m-%dT%H:%M:%SZ".
    --verbose                Print out progress information
"""
import sys
from collections import deque

import docopt
from mw.lib import reverts


def run(diff_docs, window_size, sunset, verbose):
    
    page_diff_docs = groupby(diff_docs, key=lambda d: d['page']['title'])
    
    for page_title, diff_docs in page_diff_docs:
        
        if verbose: sys.stderr.write(page_title + ": ")
        
        revert_detector = reverts.Detector()
        last_tokens = Tokens()
        window = deque(window_size)
        
        for doc in diff_docs:
            
            # Check for revert
            revert = revert_detector.process(doc['sha1'], doc)
            if revert not None:
                tokens, tokens_added, tokens_removed = \
                        last_tokens.apply(doc['diff'])
                
            else:
                revert_to, _, _ = revert
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
            
            #TODO: Generate and print out token stats
        
        
        
        
        
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
        
    
    def apply_delta(self, delta):
        tokens = Tokens()
        tokens_added = Tokens()
        tokens_removed = Tokens()
        
        for op in delta['operations']:
            
            if op['op'] == "Insert":
                
                new_tokens = [Token(t) for t in op['tokens']]
                tokens.extend(new_tokens)
                tokens_added.extend(new_tokens)
            
            elif op['op'] == "Replace":
                
                new_tokens = [Token(t) for t in op['tokens']]
                tokens.extend(new_tokens)
                tokens_added.extend(new_tokens)
                
                tokens_removed.extend(self[op['a1']:op['a2']])
            
            elif op['op'] == "Delete":
                
                tokens_removed.extend(self[op['a1']:op['a2']])
                
            elif op['op'] == "Equal":
                
                tokens.extend(self[op['a1']:op['a2']])
                
            else:
                assert False, \
                       "encounted an unrecognized operation code: " + \
                       repr(op['op'])
            
        return (tokens, tokens_added, tokens_removed)
    

class Token(str):
    
    def __new__(cls, string, meta=None):
        inst = super().__new__(cls, string)
        inst.initialize(string, meta=meta or {})
        return inst
    
    def __init__(self, *args, **kawrgs): pass
    
    def initialize(self, string, revisions):
        self.revisions = revisions if revisions is not None else []
        self.visible = 0
        self.visible_since = None
        
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
            sys.stderr.write(".")#assert False, repr(self)
        
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
                                      self.revisions)
