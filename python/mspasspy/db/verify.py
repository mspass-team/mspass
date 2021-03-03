from mspasspy.ccore.utility import MsPASSError
"""
This python module contains functions that will run a particular test on 
a mongodb database collection in mspass.  The command line tool 
dbverify is a front end to build more readable output from these
functions.
"""
def check_links(db,normalize='site',
                wf="wf_TimeSeries",
                wfquery={},
                verbose=False,
                error_limit=1000):
    """
    This function checks for missing cross-referencing ids in a 
    specified wf collection (i.e. wf_TimeSeries or wf_Seismogram)
    It scans the wf collection to detect two potential errors:
    (1) documents with the normalization key completely missing 
    and (2) documents where the key is present does not match any 
    document in normalization collection.   By default this
    function operates silently assuming the caller will 
    create a readable report from the return that defines 
    the documents that had errors.  This function is used in the 
    verify standalone program that acts as a front end to tests
    in this module.  The function can be run in independently 
    so there is a verbose option to print errors as they are encountered.
    
    :param db:  required MongoDB database handle.
    :param wf:  mspass waveform collection on which the normalization
      check is to be performed.  default is wf_TimeSeries.  
      Currently only accepted alternative is wf_Seismogram.
    :param wfquery:  optional dict passed as a query to limit the 
      documents scanned by the function.   Default will process the 
      entire wf collection.
    :param verbose:  when True errors will be printed.  By default 
      the function works silently and you should use the output to 
      interact with any errors returned.  
    :param error_limit: Is a sanity check on the number of errors logged.
      Errors of any type are limited to this number (default 1000).
      The idea is errors should be rare and if this number is exceeded 
      you have a big problem you need to fix before scanning again.  
      The number should be large enough to catch all condition but 
      not so huge it become cumbersome.  With no limit or a memory 
      fault is even possible on a huge dataset.
    :return:  returns a tuple with two lists.  Both lists are ObjectIds
      of the scanned wf collection that have errors.  component 0 
      of the tuple contains ids of wf entries that have the normalization 
      id set but the id does not resolve with the normalization collection.
      component 1 contains the ids of documents in the wf collection that
      do not contain the normalization id key at all (a more common problem)

    """
    # schema doesn't currently have a way to list normalized 
    # collection names.  For now we just freeze the names 
    # and put them in this one place for maintainability
    norm_collection_list=['site','channel','source']
    wf_collection_list=['wf_TimeSeries','wf_Seismogram']

    if not (normalize in norm_collection_list):
        raise MsPASSError('check_links:  illegal value for normalize arg='+normalize,
                          'Fatal')
    if not (wf in wf_collection_list):
        raise MsPASSError('check_links:  illegal value for wf arg='+wf,
                          'Fatal')
    # this uses our convention - we need a standard method for to 
    # define this key
    idkey=normalize+'_id'
    dbnorm=db[normalize]
    dbwf=db[wf]
    n=dbwf.count_documents(wfquery)
    if n==0:
        raise MsPASSError('checklinks:  '+wf
            +' collection has no data matching query=',str(wfquery),
            'Fatal')
    if verbose:
        print('Starting cross reference link check for ',wf,
              ' collection using id=',idkey)
        print('This should resolve links to ',normalize,' collection')
    # We accumulate bad ids in this list that is returned
    bad_id_list=list()
    missing_id_list=list()
    cursor=dbwf.find(wfquery)
    for doc in cursor:
        wfid=doc['_id']
        if idkey in doc:
            nrmid=doc[idkey]
            n_nrm=dbnorm.count_documents({'_id' : nrmid})
            if n_nrm==0:
                bad_id_list.append(wfid)
                if verbose:
                    print(str(wfid),' link with ',str(nrmid),' failed')
                if len(bad_id_list) > error_limit:
                    raise MsPASSError('checklinks:  number of bad id errors exceeds internal limit',
                                      'Fatal')
        else:
            missing_id_list.append(wfid)
            if verbose:
                print(str(wfid),' is missing required key=',idkey)
            if len(missing_id_list) > error_limit:
                    raise MsPASSError('checklinks:  number of missing id errors exceeds internal limit',
                                      'Fatal')
        if len(bad_id_list)>=error_limit or len(missing_id_list)>=error_limit:
            break
    return tuple([bad_id_list,missing_id_list])

def check_attribute_types(db,
                collection="wf_TimeSeries",
                query={},
                verbose=False,
                error_limit=1000):
    """
    This function checks the integrity of all attributes 
    found in a specfied collection.  It is designed to detect two 
    kinds of problems:  (1) type mismatches between what is stored 
    in the database and what is defined for the schema, and (2) 
    data with a key that is not recognized.  Both tests are necessary 
    because unlike a relational database MongoDB is very promiscuous 
    about type and exactly what goes into a document.  MongoDB pretty 
    much allow type it knows about to be associated with any key 
    you choose.   In MsPASS we need to enforce some type restrictions 
    to prevent C++ wrapped algorithms from aborting with type mismatches. 
    Hence, it is important to run this test on all collections needed 
    by a workflow before starting a large job.  
    
    :param db:  mspass Database handle.   Note this must be 
      a Database class defined by "from mspasspy.db.database import Database".
      The reason is that the mspass handle is an extension of MongoDB's 
      handle that includes the Schema class used to run the tests in 
      this function. This arg is required
    :param collection:  MongoDB collection that is to be scanned 
      for errors.  Note with normalized data this function should be 
      run on the appropriate wf collection and all normalization 
      collections the wf collection needs to link to. 
    :param query:  optional dict passed as a query to limit the 
      documents scanned by the function.   Default will process the 
      entire collection requested.
    :param verbose:  when True errors will be printed.   The default is
      False and the function will do it's work silently.   Verbose is 
      most useful in an interactive python session where the function 
      is called directly.  Most users will run this function 
      as part of tests driven by the dbverify program. 
    :param error_limit: Is a sanity check the number of errors logged
      The number of any type are limited to this number (default 1000).
      The idea is errors should be rare and if this number is exceeded 
      you have a big problem you need to fix before scanning again.  
      The number should be large enough to catch all condition but 
      not so huge it become cumbersome.  With no limit or a memory 
      fault is even possible on a huge dataset.
    :return:  returns a tuple with two python dict containers.  
      The component 0 python dict contains details of type mismatch errors.
      Component 1 contains details for data with undefined keys.  
      Both python dict containers are keyed by the ObjectId of the 
      document from which they were retrieved.  The values associated
      with each entry are like MongoDB subdocuments.  That is, the value
      return is itself a dict. The dict value contains key-value pairs
      that defined the error (type mismatch for 0 and undefined for 1)

    """
    # The following two can throw MsPASS errors but we let them 
    # do so. Callers should have a handler for MsPASSError
    dbschema=db.database_schema
    # This holds the schema for the collection to be scanned
    # dbschema is mostly an index to one of these
    col_schema=dbschema[collection]
    dbcol=db[collection]
    n=dbcol.count_documents(query)
    if n == 0:
        raise MsPASSError('check_attribute_types:  query='
                          +str(query)+' yields zero matching documents',
                          'Fatal')
    cursor=dbcol.find(query)
    bad_type_docs=dict()
    undefined_key_docs=dict()
    for doc in cursor:
        bad_types=dict()
        undefined_keys=dict()
        id=doc['_id']
        for k in doc:
            if col_schema.is_defined(k):
                val=doc[k]
                if type(val)!=col_schema.type(k):
                    bad_types[k]=doc[k]
                    if(verbose):
                        print('doc with id=',id,' type mismatch for key=',k)
                        print('value=',doc[k],' does not match expected type=',
                              col_schema.type(k))
            else:
                undefined_keys[k]=doc[k]
                if(verbose):
                    print('doc with id=',id,' has undefined key=',k,
                          ' with value=',doc[k])
        if len(bad_types)>0:
            bad_type_docs[id]=bad_types
        if len(undefined_keys)>0:
            undefined_key_docs[id]=undefined_keys;
        if len(undefined_key_docs)>=error_limit or len(bad_type_docs)>=error_limit:
            break

    return tuple([bad_type_docs,undefined_key_docs])
            
    
def check_required(db,
                collection='site',
                keys=['lat','lon','elev','starttime','endtime'],
                query={},
                verbose=False,
                error_limit=100):
    """
    This function applies a test to assure a list of attributes 
    are defined and of the right type.   This function is needed 
    because certain attributes are essential in two different contexts.
    First, for waveform data there are some attributes that are 
    required to construct the data object (e.g. sample interal or 
    sample rate, start time, etc.).  Secondly, workflows generally 
    require certain Metadata and what is required depends upon the 
    workflow.  For example, any work with sources normally requires
    information about both station and instrument properties as well 
    as source.  The opposite is noise correlation work where only 
    station information is essential.  

    :param db:  mspass Database handle.   Note this must be 
      a Database class defined by "from mspasspy.db.database import Database".
      The reason is that the mspass handle is an extension of MongoDB's 
      handle that includes the Schema class used to run the tests in 
      this function. This arg is required
    :param collection:  MongoDB collection that is to be scanned 
      for errors.  Note with normalized data this function should be 
      run on the appropriate wf collection and all normalization 
      collections the wf collection needs to link to. 
    :param keys:  is a list of strings that are to be checked 
      against the contents of the collection.  Note one of the first 
      things the function does is test for the validity of the keys.  
      If they are not defined in the schema the function will throw 
      a MsPASSError exception. 
    :param query:  optional dict passed as a query to limit the 
      documents scanned by the function.   Default will process the 
      entire collection requested.
    :param verbose:  when True errors will be printed.   The default is
      False and the function will do it's work silently.   Verbose is 
      most useful in an interactive python session where the function 
      is called directly.  Most users will run this function 
      as part of tests driven by the dbverify program. 
    :param error_limit: Is a sanity check the number of errors logged
      The number of any type are limited to this number (default 1000).
      The idea is errors should be rare and if this number is exceeded 
      you have a big problem you need to fix before scanning again.  
      The number should be large enough to catch all condition but 
      not so huge it become cumbersome.  With no limit or a memory 
      fault is even possible on a huge dataset.
    :return:  tuple with two components. Both components contain a 
      python dict container keyed by ObjectId of problem documents. 
      The values in the component 0 dict are themselves python dict
      containers that are like MongoDB subdocuments).  The key-value
      pairs in that dict are required data with a type mismatch with the schema.
      The values in component 1 are python lists of keys that had 
      no assigned value but were defined as required.   
    """
    if len(keys)==0:
        raise MsPASSError('check_required:  list of required keys is empty '
                          + '- nothing to test','Fatal')
    # The following two can throw MsPASS errors but we let them 
    # do so. Callers should have a handler for MsPASSError
    dbschema=db.database_schema
    # This holds the schema for the collection to be scanned
    # dbschema is mostly an index to one of these
    col_schema=dbschema[collection]
    dbcol=db[collection]
    # We first make sure the user didn't make a mistake in giving an 
    # invalid key for the required list
    for k in keys:
        if not col_schema.is_defined(k):
            raise MsPASSError('check_required:  schema has no definition for key='
                              + k,'Fatal')

    n=dbcol.count_documents(query)
    if n == 0:
        raise MsPASSError('check_required:  query='
                          +str(query)+' yields zero matching documents',
                          'Fatal')
    undef=dict()
    wrong_types=dict()
    cursor=dbcol.find(query)        
    for doc in cursor:
        id=doc['_id']
        undef_this=list()
        wrong_this=dict()
        for k in keys:
            if not k in doc:
                undef_this.append(k)
            else:
                val=doc[k]
                if type(val)!=col_schema.type(k):
                    wrong_this[k]=val
        if len(undef_this)>0:
            undef[id]=undef_this
        if len(wrong_this)>0:
            wrong_types[id]=wrong_this
        if len(wrong_types)>=error_limit or len(undef)>=error_limit:
            break
    return tuple([wrong_types,undef])
                
