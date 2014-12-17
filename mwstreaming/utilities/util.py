import json


def read_docs(f, field=1):
    for line in f:
        yield json.loads(line.strip().split("\t")[field-1])

def revision2doc(revision, page):
    """
    Implements RevisionDocument v0.0.2
    """
    redirect = None
    if page.redirect is not None:
        redirect = page.redirect.title
    
    page_doc = {
        'id': page.id,
        'title': page.title,
        'namespace': page.namespace,
        'redirect_title': redirect,
        'restrictions': page.restrictions
    }
    
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
        'text': str(revision.text) \
               if revision.text is not None \
               else None,
        'bytes': revision.bytes,
        'sha1': revision.sha1,
        'parent_id': revision.parent_id,
        'model': revision.model,
        'format': revision.format
    }
    
    return revision_doc
