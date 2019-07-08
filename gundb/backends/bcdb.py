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
    bcdb = j.data.bcdb.get(name="test4")
    bcdb.reset()
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
mychars* = (LS) 
    """)

    j.data.schema.add_from_text("""
@url = proj.email
addr* = "" (S)
    """)
    j.data.schema.add_from_text("""
@url = proj.person
name* = "" (S)
email* = "" !proj.email
    """)




    def get_schema_by_url(url):
        schema = j.data.schema.get_from_url_latest(url=url)
        return schema

    def get_model_by_schema_url(schema_url):
        return bcdb.model_get_from_url(schema_url)   

    def parse_schema_and_id(s):
        m = re.match(SCHEME_UID_PAT, s)
        if m:
            return m.groupdict()['schema'], int(m.groupdict()['id']) 
        return None, None
    class BCDB:
        def __init__(self):
            self.db = {}
        
        def put(self, soul, key, value, state, graph):
            print("put bcdb => soul {} key {} value {} state {}".format(soul, key, value, state))

            def is_root_soul(s):
                return "://" in s

            def is_nested(s):
                return not is_root_soul(s)

            def get_nested_soul_node(s, graph):
                return graph.get(s, {})

            def resolve_key(k, graph):
                pass

            def filter_root_objects(graph):
                for kroot, rootnode in graph.items():
                    if "://" in kroot:
                        yield kroot, rootnode

            ignore = ["_", "#", ">"]

            def resolve_v(v, graph):
                if not isinstance(v, dict):
                    return v
                if not v["#"]:
                    return v
                else:
                    ret = {}
                    soul_id = v["#"]
                    for subk,subv in graph[soul_id].items():
                        if subk in ignore:
                            continue
                        res_v = resolve_v(subv, graph)
                        ret[subk] = res_v
                return ret
                    
            def search(k, graph):
                # DON'T CHANGE: CHECK WITH ME OR ANDREW
                visited = []
                roots = list(rootobjects)

                def inner(k, current_key, current_node, graph, path=None):
                    print("path in inner: ", path)
                
                    if not isinstance(current_node, dict):
                        return []   
                    if not path:
                        path = []
                
                    if current_key:
                        path.append(current_key)

                    for key, node in current_node.items():
                        
                        print("node: {} ".format(node))
                        print("key {}".format(key))
                        if key in ">_":
                            continue

                        if isinstance(node, dict) and node.get("#") == k:
                            path.append(key)
                            return path
                        
                        res = inner(k, key, node, graph, path)
                        
                        if res:
                            return res
                        else:
                            print("path now : ", path)
                            
                    if current_key:
                        path.pop()
                    return []
                    
                return inner(k, None, graph, roots, None)

            rootobjects = list(filter_root_objects(graph))
            # find its parent to get
            def do(soul, key, value, graph):
                if is_root_soul(soul):
                    schema, obj_id = parse_schema_and_id(soul)
                    print("soul <- {} schema <- {} ".format(soul, schema))
                    model = get_model_by_schema_url(schema)
                    try:
                        obj = model.get(obj_id)
                    except:
                        obj = model.new()
                    print(":::=> object update setting attr {} with value {}".format(key, value))
                    setattr(obj, key, resolve_v(value, graph))
                    return obj
                else:
                    objpath = path = search(soul, graph)
                    if not objpath:
                        print("[---]can't find :", soul, key, value) 
                        return
                    objcontent = path + [{"#":soul}, graph]

                    schema, obj_id = parse_schema_and_id(objpath[0])
                    model = get_model_by_schema_url(schema)
                    

                    objdata = do(*objcontent)

                    print(objpath)

                    objinfo = objpath[0]
                    objpath = objpath[1:]

                    obj = None
                    try:
                        obj = model.get(obj_id)
                    except:
                        obj = model.new()

                    attr = None
                    while objpath:
                        attr = objpath.pop(0)
                        obj = getattr(obj, attr)
                    
                    obj = objdata
                    obj.save()
                    print("success.....!!!!!", obj)


            do(soul, key, value, graph)
    
            graph[soul][key] = value
            print(graph)

            ## BUG: need to think about how are we gonna represent the state for conflict resoultion (they need to be encoded somehow)
            if soul not in self.db:
                self.db[soul] = {METADATA:{}}
            self.db[soul][key] = value
            self.db[soul][METADATA][key] = state



        def get(self, soul, key=None):
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
