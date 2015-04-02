from deltas import Delete, Insert
from more_itertools import peekable
from nose.tools import eq_, raises

from ..mend_diffs import mend_diffs, read_broken_docs


class FakeDiffEngine:
    def processor(self, last_text=''):
        return FakeProcessor(last_text)

class FakeProcessor:
    def __init__(self, last_text):
        self.last_text = last_text

    def process(self, text):
        ops = [Delete(0,1,0,0),
               Insert(0,0,0,1)]
        a = [self.last_text]
        b = [text]
        self.last_text = text

        return ops, a, b

def test_broken_diffs():
    revision_docs = [
        {'id': 2, 'text': "Apples are blue.", 'page': {'title': "Foo"},
         'diff': {'last_id': 3, 'ops': []}},
        {'id': 3, 'text': "Apples are red.", 'page': {'title': "Foo"},
         'diff': {'last_id': 1, 'ops': []}},
        {'id': 4, 'text': "Apples are a red fruit", 'page': {'title': "Foo"},
         'diff': {'last_id': 2, 'ops': []}},
        {'id': 5, 'text': "Apples are a lame fruit", 'page': {'title': "Foo"},
         'diff': {'last_id': 4, 'ops': []}}
    ]

    revision_docs = peekable(revision_docs)

    broken_docs = list(read_broken_docs(revision_docs))
    print([d['id'] for d in broken_docs])
    eq_(len(broken_docs), 3)

def test_mend_diffs():

    revision_docs = [
        {'id': 1, 'text': "Apples are red.", 'page': {'title': "Foo"},
         'diff': {'last_id': None, 'ops': []}},
        {'id': 2, 'text': "Apples are blue.", 'page': {'title': "Foo"},
         'diff': {'last_id': 1, 'ops': []}},
        {'id': 3, 'text': "Apples are red.", 'page': {'title': "Foo"},
         'diff': {'last_id': None, 'ops': []}},
        {'id': 4, 'text': "Apples are a red fruit", 'page': {'title': "Foo"},
         'diff': {'last_id': 3, 'ops': []}},
        {'id': 5, 'text': "Apples are a lame fruit", 'page': {'title': "Foo"},
         'diff': {'last_id': 4, 'ops': []}},
        {'id': 10, 'text': "Bar text", 'page': {'title': "Bar"},
         'diff': {'last_id': None, 'ops': []}}
    ]
    diff_engine = FakeDiffEngine()
    new_docs = [r for r in mend_diffs(revision_docs, diff_engine)]

    eq_(new_docs[0]['diff']['ops'], [])
    eq_(new_docs[0]['diff']['last_id'], None)

    eq_(new_docs[1]['diff']['ops'], [])
    eq_(new_docs[1]['diff']['last_id'], 1)

    eq_(new_docs[2]['diff']['last_id'], 2)
    eq_(new_docs[2]['diff']['ops'],
        [{'name': 'delete', 'b1': 0, 'a1': 0, 'b2': 0, 'a2': 1,
          'tokens': ['Apples are blue.']},
         {'name': 'insert', 'b1': 0, 'a1': 0, 'b2': 1, 'a2': 0,
          'tokens': ['Apples are red.']}])

    eq_(new_docs[3]['diff']['ops'], [])
    eq_(new_docs[3]['diff']['last_id'], 3)

    eq_(new_docs[4]['diff']['ops'], [])
    eq_(new_docs[4]['diff']['last_id'], 4)

    eq_(new_docs[5]['diff']['ops'], [])
    eq_(new_docs[5]['diff']['last_id'], None)

def test_drop_text():
    revision_docs = [
        {'id': 1, 'text': "Apples are red.", 'page': {'title': "Foo"},
         'diff': {'last_id': None, 'ops': []}},
        {'id': 2, 'text': "Apples are blue.", 'page': {'title': "Foo"},
         'diff': {'last_id': 1, 'ops': []}},
        {'id': 3, 'text': "Apples are red.", 'page': {'title': "Foo"},
         'diff': {'last_id': None, 'ops': []}},
        {'id': 4, 'text': "Apples are a red fruit", 'page': {'title': "Foo"},
         'diff': {'last_id': 3, 'ops': []}},
        {'id': 5, 'text': "Apples are a lame fruit", 'page': {'title': "Foo"},
         'diff': {'last_id': 4, 'ops': []}},
        {'id': 10, 'text': "Bar text", 'page': {'title': "Bar"},
         'diff': {'last_id': None, 'ops': []}}
    ]

    diff_engine = FakeDiffEngine()
    # The following bit shouldn't cause an error
    for doc in mend_diffs(revision_docs, diff_engine):
        del doc['text']


@raises(RuntimeError)
def test_mend_diffs_missing_text():
    revision_docs = [
        {'id': 1, 'page': {'title': "Foo"}, # Missing 'text' field
         'diff': {'last_id': None, 'ops': []}},
    ]

    diff_engine = FakeDiffEngine()
    new_docs = [r for r in mend_diffs(revision_docs, diff_engine)]
