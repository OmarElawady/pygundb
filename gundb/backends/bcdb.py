from ..consts import STATE, METADATA, SOUL

import re

SCHEME_UID_PAT = "(?P<schema>(.+)?)://(?P<id>(.+))"

def get_schema_by_url(url):
    schema = j.data.schema.get_from_url_latest(url=url)
    return schema

def get_model_by_schema_and_id(schema_url, id):
    pass

def parse_schema_and_id(s):
    m = re.match(SCHEME_UID_PAT, s)
    if m:
        return m.groupdict()['schema'], m.groupdict()['id'] 
    else:
        return None

class BCDB:
    def __init__(self):
        self.db = {}
    
    def put(self, soul, key, value, state):
        schema, obj_id = parse_schema_and_id(soul)
        model = get_model_by_schema_and_id(schema, obj_id)
        setattr(model, key, value)

        model.save()

        ## BUG: need to think about how are we gonna represent the state for conflict resoultion (they need to be encoded somehow)

        if soul not in self.db:
            self.db[soul] = {METADATA:{}}
        self.db[soul][key] = value
        self.db[soul][METADATA][key] = state

    def get(self, soul, key=None):
        # print("SOUL: ", soul, " KEY : ", key)
        ret = {SOUL: soul, METADATA:{SOUL:soul, STATE:{}}}
        schema, obj_id = parse_schema_and_id(soul)
        model = get_model_by_schema_and_id(schema, obj_id)
        

        if key:
            res = {**ret, key:getattr(obj_id, key)}
        else:
            data = json.loads(model._json)
            res = {**ret,  **data}


        ## TODO: if we are going to enable caching.
        # res = None
        # if soul in self.db:
        #     if key and isinstance(key, str):
        #         res = {**ret, **self.db.get(soul)}
        #         return res.get(key, {})
        #     else:
        #         res = {**ret, **self.db.get(soul)}
        #         return res

        return ret 

    def __setitem__(self, k, v):
        self.db[k] = v

    def __getitem__(self, k):
        return self.db[k]

    def list(self):
        return self.db.items()
