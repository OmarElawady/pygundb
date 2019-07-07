from ..consts import STATE, METADATA, SOUL
import re
import json

from .memory import Memory
try:
    from Jumpscale import j
except ImportError as e:
    print("error loading jumpscale.")
    from .memory import Memory as BCDB
else:
        
    SCHEME_UID_PAT = "(?P<schema>.+?)://(?P<id>.+)"
    bcdb = j.data.bcdb.new(name="test")

    j.data.schema.add_from_text("""
@url = proj.todo
title* = "" (S)
done* = False (B)
    
    """)

    j.data.schema.add_from_text("""
@url = proj.todolist
name* = "" (S)
todos* = (LO) !proj.todo
    
    """)
    j.data.schema.add_from_text("""
@url = proj.simple
attr1* = "" (S)
attr2* = 0 (I)
chars* = (LS) 
    """)


    def get_schema_by_url(url):
        schema = j.data.schema.get_from_url_latest(url=url)
        return schema

    def get_model_by_schema_url(schema_url):
        return bcdb.model_get_from_url(schema_url)   

    def parse_schema_and_id(s):
        m = re.match(SCHEME_UID_PAT, s)
        if m:
            return m.groupdict()['schema'], m.groupdict()['id'] 
        else:
            return None

    class BCDB:
        def __init__(self):
            self.db = {}
            # self.m = Memory()
        
        def put(self, soul, key, value, state, graph):
            print("put bcdb => soul {} key {} value {} state {}".format(soul, key, value, state))
            try:
                schema, obj_id = parse_schema_and_id(soul)
            except Exception as e:
                print("e => ", e)
                resolved_soul = None
                root_soul = None
                import ipdb; ipdb.set_trace()
                for k, rootnode in graph.items():
                    for attrname, attrval in rootnode.items():
                        if attrval["#"] == k:
                            resolved_soul = attrval["#"] ## FIXME urgently
                            root_soul = k
                            break
                if resolved_soul and root_soul:
                    schema, obj_id = parse_schema_and_id(root_soul)
                    model = get_model_by_schema_url(schema)
                    obj = model.get(obj_id)
                    if resolved_soul.startswith("list/"):
                        attrname_in_model = resolved_soul.strip("list/")
                        thelist = getattr(obj, attrname_in_model)
                        thelist.append(value)
                        setattr(obj, attrname_in_model, thelist)
                    else:
                        setattr(obj, attrname_in_model, value)
                    
            else:

                model = get_model_by_schema_url(schema)
                obj = None
                try:
                    obj = model.get(obj_id)
                except:
                    obj = model.new()
                print(":::=> object update setting attr {} with value {}".format(key, value))
                setattr(model, key, value)
                obj.save()

                ## BUG: need to think about how are we gonna represent the state for conflict resoultion (they need to be encoded somehow)
                if soul not in self.db:
                    self.db[soul] = {METADATA:{}}
                self.db[soul][key] = value
                self.db[soul][METADATA][key] = state



        def get(self, soul, key=None):
            # resm = self.m.get(soul, key)
            # print(resm)
            print(" get bcdb => soul {} key {} ".format(soul, key))

            ret = {SOUL: soul, METADATA:{SOUL:soul, STATE:{}}}
            try:
                schema, obj_id = parse_schema_and_id(soul)
                model = get_model_by_schema_url(schema)
                obj = None
                obj = model.get(obj_id)

                obj_dict = obj._ddict


                if key:
                    return {**ret, key:obj_ddict[key]}
                else:
                    return {**ret,  **obj_dict}
            except Exception as e:
                print("Err get: ", e)
                return ret



            ## TODO: if we are going to enable caching.
            # res = None
            # if soul in self.db:
            #     if key and isinstance(key, str):
            #         res = {**ret, **self.db.get(soul)}
            #         return res.get(key, {})
            #     else:
            #         res = {**ret, **self.db.get(soul)}
            #         return res


        def __setitem__(self, k, v):
            self.db[k] = v

        def __getitem__(self, k):
            return self.db[k]

        def list(self):
            return self.db.items()
