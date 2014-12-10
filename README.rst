MediaWiki Streaming
===================

A collection of scripts and utilities to support the stream-processing of
MediaWiki data.

* dump2json -- Converts an XML dump to a stream of revision JSON blobs
* json2tsv -- Converts a stream of JSON blobs to tab-separated values
* json2diffs -- Computes and adds a "diff" field to a stream of revision JSON
                blobs
* diffs2persistence -- Computes token persistence from a stream of JSON revision
                       diff blobs and adds a "persistence" field.
* persistence2revstats -- Aggregates a stream of token persistence to revision
                          statistics
