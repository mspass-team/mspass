def delete_attributes(db,collection,keylist,query={},verbose=False):
    """
    Deletes all occurrences of attributes linked to keys defined 
    in a list of keywords passed as (required) keylist argument.  
    If a key is not in a given document no action is taken. 
    
    :param db:  Database handle to be updated
    :param collection:  MongoDB collection to be updated
    :param keylist:  list of keys for elements of each document 
      that are to be deleted.   key are not test against schema 
      but all matches will be deleted.
    :param query: optional query string passed to find database 
      collection method.  Can be used to limit edits to documents 
      matching the query.  Default is the entire collection.
    :param verbose:  when true edit will produce a line of printed 
      output describing what was deleted.  Use this option only if 
      you know from dbverify the number of changes to be made are small.
      
    :return:  dict keyed by the keys of all deleted entries.  The value 
      of each entry is the number of documents the key was deleted from.
    """
    dbcol=db[collection]
    cursor=dbcol.find(query)
    counts=dict()
    # preload counts to 0 so we get a return saying 0 when no changes 
    # are made
    for k in keylist:
        counts[k]=0
    for doc in cursor:
        id=doc.pop('_id')
        n=0
        todel=dict()
        for k in keylist:
            if k in doc:
                todel[k]=doc[k]
                val=doc.pop(k)
                if verbose:
                    print('Deleted ',val,' with key=',k,' from doc with id=',id)
                counts[k]+=1
                n+=1
        if n>0:
            dbcol.update_one({'_id':id},{'$unset' : todel})
    return counts
    
def rename_attributes(db,collection,rename_map,query={},verbose=False):
    """
    Renames specified keys for all or a subset of documents in a 
    MongoDB collection.   The updates are driven by an input python 
    dict passed as the rename_map argument. The keys of rename_map define
    doc keys that should be changed.  The values of the key-value 
    pairs in rename_map are the new keys assigned to each match.  
    
    
    :param db:  Database handle to be updated
    :param collection:  MongoDB collection to be updated
    :param rename_map:  remap definition dict used as described above.
    :param query: optional query string passed to find database 
      collection method.  Can be used to limit edits to documents 
      matching the query.  Default is the entire collection.
    :param verbose:  when true edit will produce a line of printed 
      output describing what was deleted.  Use this option only if 
      you know from dbverify the number of changes to be made are small.
      When false the function runs silently.
      
    :return:  dict keyed by the keys of all changed entries.  The value 
      of each entry is the number of documents changed.  The keys are the 
      original keys.  displays of result should old and new keys using 
      the rename_map.
    """
    dbcol=db[collection]
    cursor=dbcol.find(query)
    counts=dict()
    # preload counts to 0 so we get a return saying 0 when no changes 
    # are made
    for k in rename_map:
        counts[k]=0
    for doc in cursor:
        id=doc.pop('_id')
        n=0
        for k in rename_map:
            n=0
            if k in doc:
                val=doc.pop(k)
                newkey=rename_map[k]
                if verbose:
                    print('Document id=',id)
                    print('Changed attribute with key=',k,' to have new key=',newkey)
                    print('Attribute value=',val)
                doc[newkey]=val
                counts[k]+=1
                n+=1
        dbcol.replace_one({'_id':id},doc)
    return counts
def fix_attribute_types(db,collection,query={},verbose=False):
    """
    This function attempts to fix type collisions in the schema defined 
    for the specified database and collection.  It tries to fix any 
    type mismatch that can be repaired by the python equivalent of a 
    type cast (an obscure syntax that can be seen in the actual code).  
    Known examples are it can cleanly convert something like an int to 
    a float or vice-versa, but it cannot do something like convert an 
    alpha string to a number. Note, however, that python does cleanly 
    convert simple number strings to number.  For example:  x=int('10')
    will yield an "int" class number of 10.  x=int('foo'), however, will
    not work.   Impossible conversions will not abort the function but 
    will generate an error message printed to stdout.  The function 
    continues on so if there are a large number of such errors the 
    output could become voluminous.  ALWAYS run dbverify before trying 
    this function (directly or indirectly through the command line 
    tool dbclean).   
    
    :param db:  Database handle to be updated
    :param collection:  MongoDB collection to be updated
    :param query: optional query string passed to find database 
      collection method.  Can be used to limit edits to documents 
      matching the query.  Default is the entire collection.
    :param verbose:  when true edit will produce one or more lines of 
      printed output for each change it makes.  The default is false.
      Needless verbose should be avoided unless you are certain the 
      number of changes it will make are small.  
    """
    dbcol=db[collection]
    schema=db.database_schema
    col_schema=schema[collection]
    counts=dict()
    cursor=dbcol.find(query)
    for doc in cursor:
        n=0
        id=doc.pop('_id')
        if verbose:
            print("////////Document id=",id,'/////////')
        up_d=dict()
        for k in doc:
            val=doc[k]
            if not col_schema.is_defined(k):
                if verbose:
                    print('Warning:  in doc with id=',id,
                          'found key=',k,' that is not defined in the schema')
                    print('Value of key-value pair=',val)
                    print('Cannot check type for an unknown attribute name')
                continue
            if not isinstance(val,col_schema.type(k)):
                try:
                    newval=col_schema.type(k)(val)
                    up_d[k]=newval
                    if verbose:
                        print('Changed data for key=',k,' from ',val,' to ',newval)
                    if k in counts:
                        counts[k]+=1
                    else:
                        counts[k]=1
                    n+=1
                except Exception as err:
                    print("////////Document id=",id,'/////////')
                    print('WARNING:  could not convert attribute with key=',
                          k,' and value=',val,' to required type=',
                          col_schema.type(k))
                    print('This error was thrown and handled:  ')
                    print(err)
         
        if n>0:
            dbcol.update_one({'_id' : id},{'$set' : up_d})
            
    return counts
