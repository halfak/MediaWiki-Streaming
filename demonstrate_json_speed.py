"""
Demonstrates the encode/decode speed of JSON processing utilities

Usage:
    demonstrate_json_speed (json|ujson)
"""
import json
import sys
import time

import docopt

import ujson


def main():
    args = docopt.docopt(__doc__)
    if args['json']:
        loads = json.loads
        dumps = json.dumps
    elif args['ujson']:
        loads = ujson.loads
        dumps = ujson.dumps
    
    run(sys.stdin, loads, dumps, sys.stdout)

def run(input, loads, dumps, output):
    
    time_spent_loading = 0
    time_spent_dumping = 0
    
    for line in input:
        start = time.time()
        doc = loads(line)
        time_spent_loading += time.time() - start
        
        start = time.time()
        output.write(dumps(doc) + "\n")
        time_spent_dumping += time.time() - start
    
    sys.stderr.write("Time spent loading: {0}\n".format(time_spent_loading))
    sys.stderr.write("Time spent dumping: {0}\n".format(time_spent_dumping))

if __name__ == "__main__": main()
