import time


def extract(doc, fieldname):
    
    if fieldname == "-":
        return json.dumps(doc)
    else:
        parts = fieldname.split(".", 1)
        
        if len(parts) == 2:
            field, fieldname = parts
            return extract(doc[field], fieldname)
        elif len(parts) == 1:
            field = parts[0]
            return doc[field]
        else:
            raise Exception("Ran out of field name parts.")
    

def extract_fast(doc, keys, i=0):
    
    if len(keys) == i:
        return doc[keys[i]]
    elif len(keys) > i:
        if isinstance(doc[keys[i]], dict):
            return extract_fast(doc[keys[i]], keys, i+1)
        else:
            return None
    elif len(keys) == 0:
        raise KeyError("Must specify at least one key.")
    else:
        assert False, "Shouldn't be possible."


def extract_direct(doc, keys):
    val = doc
    for key in keys:
        if isinstance(val, dict) and key in val:
            val = val[key]
        else:
            return None
        
    return val
    

d = {"id": 3, "format": "text/x-wiki", "bytes": None, "comment": None,
     "contributor": {"id": None, "user_text": "222.152.210.22"},
     "text": "Revision 3 text", "page": {"redirect": None, "id": 2,
     "title": "Bar", "restrictions": [], "namespace": 1}, "model": "wikitext",
     "minor": False, "parent_id": None,
     "sha1": "g9chqqg94myzq11c56ixvq7o1yg75n9",
     "timestamp": "2004-08-11T09:04:08Z"}

fieldnames = ["id", "page.id", "timestamp"]

start = time.time()
for i in range(100000):
    [extract(d, fn) for fn in fieldnames]

print("extract() X 100000 = {0} seconds".format(time.time() - start))

start = time.time()
field_keys = [fn.split(".") for fn in fieldnames]
for i in range(100000):
    [extract_fast(d, keys) for keys in field_keys]

print("extract_fast() X 100000 = {0} seconds".format(time.time() - start))


start = time.time()
field_keys = [fn.split(".") for fn in fieldnames]
for i in range(100000):
    [extract_direct(d, keys) for keys in field_keys]

print("extract_direct() X 100000 = {0} seconds".format(time.time() - start))
