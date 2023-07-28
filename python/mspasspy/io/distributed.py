import os
import pandas as pd
import json


from mspasspy.db.database import (Database,
                                  md2doc,
                                  elog2doc,
                                  history2doc,
                                  )
from mspasspy.util.Undertaker import Undertaker
from mspasspy.db.client import DBClient


from mspasspy.ccore.seismic import (
    TimeSeries,
    Seismogram,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
)
from mspasspy.ccore.utility import (
    MsPASSError,
    ErrorLogger,
    ErrorSeverity,
)


import dask.bag as daskbag
from dask.dataframe.core import DataFrame as daskDF
from pyspark.sql.dataframe import DataFrame as sparkDF

def read_ensemble_parallel(query,
                           db,
                           collection="wf_TimeSeries",
                           mode="promiscuous",
                           normalize=None,
                           load_history=False,
                           exclude_keys=None,
                           data_tag=None,
                           aws_access_key_id=None,
                           aws_secret_access_key=None,
                           ):
    """
    Special function used in read_distributed_data to handle ensembles. 
    
    Ensembles need to be read via a cursor which is not serializable.  
    Here we query a Database class, which is serializable, and 
    call it's read_data method to construct an ensemble that it returns. 
    Defined as a function instead of using a lambda due largely to the 
    complexity of the argument list passed to  read_data.
    """
    cursor = db[collection].find(query)
    ensemble = db.read_data(cursor,
                            collection=collection,
                            mode=mode,
                            normalize=normalize,
                            load_history=load_history,
                            exclude_keys=exclude_keys,
                            data_tag=data_tag,
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            )
    kill_me = True
    for d in ensemble.member:
        if d.live:
            kill_me = False
            break
    if kill_me:
        ensemble.kill()
    return ensemble


def read_distributed_data(
    data,
    db=None,
    query=None,
    scratchfile=None,
    collection="wf_TimeSeries",
    mode="promiscuous",
    normalize=None,
    load_history=False,
    exclude_keys=None,
    format="dask",
    npartitions=None,
    spark_context=None,
    data_tag=None,
    aws_access_key_id=None,
    aws_secret_access_key=None,
):
    """
    Parallel data reader.
    
    In MsPASS seismic data objects need to be loaded into a Spark RDD or 
    Dask bag for parallel processing. This function abstracts that
    process parallelizing the read operation where it can do so.   
    In MsPASS all data objects are created by constructors driven by 
    one or more documents stored in a MongoDB database collection 
    we will refer to here as a "waveform collection".  Atomic data 
    are built from single documents that may or may not be extended 
    by "normalization" (option defined by normalize parameter).
    The constructors can get the sample data associated with a 
    waveform document by a variety of storage methods that 
    are abstracted to be "under the hood".  That is relevant to understanding
    this function because absolutely required input is a handle to 
    the waveform collection OR an image of it in another format.   
    Furthermore, the other absolute is that the output is ALWAYS a 
    parallel container;  a spark RDD or a Dask bag.   This reader 
    is the standard way to load a dataset into one of these parallel 
    containers.  
    
    A complexity arises because of two properties of any database 
    including MongoDB.  First, queries to any database are almost always
    very slow compared to processor speed.  Initiating a workflow with 
    a series of queries is possible, but we have found it ill advised 
    as it can represent a throttle on speed for many workflows where the 
    time for the query is large compared to the processing time for a 
    single object.   Second, in contrast all database engines can 
    work linearly through a table (collection in MongoDB) very fast 
    because they cache blocks of records send from the server to the client. 
    In MongoDB that means a "cursor" returned by a "find" operation 
    can be iterated very fast compared to one-by-one queries of the 
    same data.  Why that is relevant is that arg0 if this function is 
    required to be one of three things to work well within these constraints:
        1. An instance of a mspass `Database` handle 
           (:class:`mspasspy.db.database.Database`).  Default for 
           with this input is to read an entire collection.  Use the 
           query parameter to limit input.
        2. One of several implementations of a "Dataframe", that are 
           an image or a substitute for a MongoDB waveform collection.  
           A Dataframe, for example, is a natural output from any 
           sql (or, for seismologists, an Antelope) database.   This 
           reader supports input through a pandas Dataframe, a Dask 
           Dataframe, or a pyspark Dataframe.  THE KEY POINT about 
           dataframe input, however, is that the attribute names 
           must match schema constraints on the MongoDB database 
           that is required as an auxiliary input when reading 
           directly from a dataframe.   Note also that Dataframe input 
           also only makes sense for Atomic data with each tuple 
           mapping to one atomic data object to be created by the reader. 
        3. Ensembles represent a fundamentally different problem in this 
           context.  An ensemble, by definition, is a group of atomic data 
           with an optional set of common Metadata.  The data model for this 
           function for ensembles is it needs to return a RDD/bag 
           containing a dataset organized into ensembles.  The approach used 
           here is that a third type of input for arg0 (data) is a list 
           of python dict containers that are ASSUMED to be a set of 
           queries that defined the grouping of the ensembles.   
           For example, a data set you want to process as a set of 
           common source gathers (ensmebles) might be created 
           using a list something like this:
           [{"source_id" : ObjectId('64b917ce9aa746564e8ecbfd')}, 
            {"source_id" : ObjectId('64b917d69aa746564e8ecbfe')}, 
            ... ]
           
    This function is more-or-less three algorithms that are run for 
    each of the three cases above.   In the same order as above they are:
        1. With a Database input the function first iterates through the entire 
           set of records defined for specified collection 
           (passed via collection argument) constrained by the 
           (optional) query argument.  Note that processing is run 
           serial but fast because it working through a cursor is 
           optimized in MongoDB and the proceessing is little more than 
           reformatting the data. 
        2. The dataframe is passed through a map operator 
           (dask or spark depending on the setting of the format argument)
           that constructs the output bag/RDD in parallel.  The atomic 
           operation is calls to db.read_data, but the output is a bag/RDD
           of atomic mspass data objects.   
        3. Ensembles are read in parallel with granularity defined by 
           a partitioning of the list of queries set by the npartitions 
           parameter.  Parallelism is achieved by calling the function 
           interal to this function called `read_ensemble_parallel` 
           in a map operator.   That function queries the database 
           using the query derived form arg0 of this function and uses 
           the return to call the `Database.read_data` method.   
           
    A final complexity users need to be aware of is how this reader handles 
    any errors that happen during construction of all the objects in the 
    output bag/rdd.  All errors that create invalid data objects produce 
    what we call "abortions" in the User Manual and docstring for the 
    :class:`mspasspy.util.Undertaker.Undertaker` class.   Invalid, atomic data 
    will be the same type as the other bag/rdd components but will have 
    the following properties that can be used to distinguish them:
        1.  They will have the Metadata field "is_live" set False.  
        2.  The data object itself will have the interal attribute "live"
            set False.
        3.  The Metadata field with key "is_abortion" will be defined and set True.
        4.  The sample array will be zero length (datum.npts==0)
    
    Ensembles are still ensembles but they may contain dead data with 
    the properties of atomic data noted above EXCEPT that the "is_live".   
    attribute will not be set - that is used only inside this function.  
    An ensemble return will be marked dead only if all its members are found 
    to be marked dead.
    
    Normalization with normalizing collections like source, site, and channel 
    are possible through the normalize argument.   Normalizers using 
    data cached to memory can be used but are likely better handled 
    after a bag/rdd is created with this function via one or more 
    map calls following this reader.  Database-driven normalizers 
    are (likely) best done through this function to reduce unnecessary 
    serialization of the database handle essential to this function
    (i.e. the handle is already in the workspace of this function).
    Avoid the form of normalize used prior to version 2.0 that allowed
    the use of a list of collection names.   It was retained for 
    backward compatibility but is slow.  


    :param data: variable type arguement used to drive construction as 
      described above.  See above for how this argument drives the
      functions behavor.  Note when set as a Database handle the cursor 
      argument must be set.  Otherwise it is ignored.
    :type data: :class:`mspasspy.db.database.Database` or :class:`pandas.DataFrame`
    or :class:`dask.dataframe.core.DataFrame` or :class:`pyspark.sql.dataframe.DataFrame`
    for atomic data.  List of python dicts defining queries to read a 
    dataset of ensembles.
    
    :param db:  Database handle for loading data.   Required input if 
      reading from a dataframe or with ensemble reading via list of queries. 
      Ignored if the "data" parameter is a Database handle.  
    :type db:  :class:`mspasspy.db.Database`.  Can be None (default)
      only if the data parameter contains the database handle.  Other uses 
      require this argument to be set.
      
    :param collection:  waveform collection name for reading.  Default is 
      "wf_TimeSeries". 
    :type collection: string

    :param mode: reading mode that controls how the function interacts with
      the schema definition for the data type.   Must be one of
      ['promiscuous','cautious','pedantic'].   See user's manual for a
      detailed description of what the modes mean.  Default is 'promiscuous'
      which turns off all schema checks and loads all attributes defined for
      each object read.
    :type mode: :class:`str`
    
    :param normalize: List of normalizers.   This parameter is passed 
      directly to the `Database.read_data` method internally.  See the 
      docstring for that method for how this parameter is handled.  
    :type normalize: a :class:`list`   The contents of the list are 
     expected to be one of two things:  (1)  The faster form is 
     to have the list define one or more subclasses of BasicMatcher, 
     which is a generic cross-referencing interface.  Examples 
     include Id matchers and miniseed net:sta:chan:loc:time matching.   
     Can also be a list of strings defining collection names but be 
     warned that will be slow as a new matcher will be constructed for 
     each atomic datum read. 
    
    :param load_history: boolean (True or False) switch used to enable or
      disable object level history mechanism.   When set True each datum
      will be tagged with its origin id that defines the leaf nodes of a
      history G-tree.  See the User's manual for additional details of this
      feature.  Default is False.
    :param exclude_keys: Sometimes it is helpful to remove one or more
      attributes stored in the database from the data's Metadata (header)
      so they will not cause problems in downstream processing.
    :type exclude_keys: a :class:`list` of :class:`str`
    
    :param format: Set the format of the parallel container to define the
      dataset.   Must be either "spark" or "dask" or the job will abort
      immediately with an exception
    :type format: :class:`str`
    
    :param spark_context: If using spark this argument is required.  Spark
      defines the concept of a "context" that is a global control object that
      manages schduling.  See online Spark documentation for details on
      this concept.
    :type spark_context: :class:`pyspark.SparkContext`
    
    :param npartitions: The number of desired partitions for Dask or the number
      of slices for Spark. By default Dask will use 100 and Spark will determine
      it automatically based on the cluster.
    :type npartitions: :class:`int`
    
    :param data_tag:  The definition of a dataset can become ambiguous
      when partially processed data are saved within a workflow.   A common
      example would be windowing long time blocks of data to shorter time
      windows around a particular seismic phase and saving the windowed data.
      The windowed data can be difficult to distinguish from the original
      with standard queries.  For this reason we make extensive use of "tags"
      for save and read operations to improve the efficiency and simplify
      read operations.   Default turns this off by setting the tag null (None).
    :type data_tag: :class:`str`
    
    :param aws_access_key_id: A part of the credentials to authenticate the user
    :param aws_secret_access_key: A part of the credentials to authenticate the user
    :return: container defining the parallel dataset.  A spark `RDD` if format
      is "Spark" and a dask 'bag' if format is "dask"
    """
    # This is a base error message that is an initialization for 
    # any throw error.  We first two type checking of arg0
    message = "read_distributed_data:  "
    if isinstance(data,list):
        ensemble_mode=True
        i = 0
        for x in list:
            if not isinstance(x,dict):
                message += "arg0 is a list, but component {} has illegal type={}\n".format(i,str(type(x)))
                message += "list must contain only python dict defining queries that define each ensemble to be loaded"
                raise TypeError(message)
    elif isinstance(data,Database):
        ensemble_mode = False
        dataframe_input = False
        db = data
    elif isinstance(data,(pd.Dataframe, sparkDF, daskDF)):
        ensemble_mode = False
        dataframe_input = True
        if isinstance(db,Database):
            db = db 
        else:
            if db:
                message += "Illegal type={} for db argument - required with dataframe input".format(str(type(db)))
                raise TypeError(message)
            else:
                message += "Usage error. An instance of Database class is required for db argument when input is a dataframe"
                raise TypeError(message)
    else:
        message += "Illegal type={} for arg0\n".format(str(type(data)))
        message += "Must be a Dataframe (pandas, spark, or dask), Database, or a list of query dictionaries"
        raise TypeError(message)
        
    # This has a fundamentally different algorithm for handling 
    # ensembles than atomic data.  Ensembles are driven by a list of 
    # queries while atomic data are driven by a dataframe.
    # the dataframe is necessary in this context because MongoDB 
    # cursors can not be serialized while Database can.
    if ensemble_mode:
        if format == "spark":
            plist = spark_context.parallelize(data, numSlices=npartitions)
            plist = plist.map(lambda q : read_ensemble_parallel(
                                        q,
                                        db,
                                        collection=collection,
                                        normalize=normalize,
                                        load_history=load_history,
                                        exclude_keys=exclude_keys,
                                        data_tag=data_tag,
                                        aws_access_key_id=aws_access_key_id,
                                        aws_secret_access_key=aws_secret_access_key,
                                        )
                                )
        else:
            plist = daskbag.from_sequence(data, npartitions=npartitions)
            plist = daskbag.map(read_ensemble_parallel,
                    db,
                    collection=collection,
                    normalize=normalize,
                    load_history=load_history,
                    exclude_keys=exclude_keys,
                    data_tag=data_tag,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    )
        
    else:
        # Logic here gets a bit complex to handle the multiple inputs 
        # possible for atomic data.  That is, multiple dataframe 
        # implementations and a direct database reading mechanism. 
        # The database instance is assumed to converted to a bag of docs
        # above.  We use a set of internal variables to control the 
        # input block used. 
        # TODO:  clean up symbol - plist is an awful name
        if dataframe_input:
            if format == "spark":
                plist = spark_context.parallelize(
                    data.to_dict("records"), numSlices=npartitions
                )
            else:
                plist = data.to_bag()
        else:
            # logic above should guarantee data is a Database 
            # object that can be queried and used to generate the bag/rdd
            # needed to read data in parallel immediately after this block
            if query:
                cursor=data[collection].find(query)
            else:
                cursor=data[collection].find({})
            if scratchfile:
                # here we write the documents all to a scratch file in 
                # json and immediately read them back to create a bag or RDD
                with open(scratchfile, "w") as outfile:
                    for doc in cursor:
                        json.dump(doc,outfile)
                if format == "spark":
                    # the only way I could find to load json data in pyspark 
                    # is to use an intermediate dataframe.   This should 
                    # still parallelize, but will probably be slower
                    # if there is a more direct solution should be done here.
                    plist = spark_context.read.json(scratchfile)
                    # this is wrong above also don't see how to do partitions
                    # this section is broken until I (glp) can get help
                    #plist = plist.map(to_dict,"records")
                else:
                    plist=daskbag.read_text(scratchfile).map(json.loads)
                # Intentionally omit error handler here.  Assume 
                # system will throw an error if file open files or write files
                # that will be sufficient for user to understand the problem.
                os.remove(scratchfile)

            else:
                doclist=[]
                for doc in cursor:
                    doclist.append(doc)
                plist = daskbag.from_sequence(doclist)
                del doclist

        # Earlier logic make list a bag/rdd of docs - above converts dataframe to same     
        if format == "spark":
            plist = plist.map(lambda doc : db.read_data(
                                    doc,
                                    collection=collection,
                                    normalize=normalize,
                                    load_history=load_history,
                                    exclude_keys=exclude_keys,
                                    data_tag=data_tag,
                                    aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key,
                                )
                            )
        else:
            plist = plist.map(
                db.read_data,
                collection=collection,
                normalize=normalize,
                load_history=load_history,
                exclude_keys=exclude_keys,
                data_tag=data_tag,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                )
    
        return plist



def _partioned_save_wfdoc(doclist,
                         db,
                         collection="wf_TimeSeries",
                         dbname=None,
        ):
    """
    Internal function used to save the core wf document data for mspass 
    data objects. 
    
    This function is intended for internal use only as a component of 
    the function write_distributed_data.   It uses a bulk write 
    method (pymongo's insert_many method) to reduce database  
    transaction delays.   
    
    The function has a feature not used at preent, but was preserved here 
    as an idea.  At present there is a workaround to provide a 
    mechanism to serialize a mspasspy.db.Database class.   When the 
    function is called with default parameters it assumes the db 
    argument will serialize in dask/spark.   If db is set to None 
    each call to the function will construct a 
    :class:`mspasspy.db.Database` object for call to the function. 
    Because it is done by partition the cost scales by the numbe rof 
    partitions not the numbe of data items to use that algorithm.  
    
    :param doclist:  list of python dict containers that are assumed created 
    by chopping up a bag/rdd of python dictionaries.   No testing is 
    done on the type as this is considered a low-level function.
    :type doclist:  list of python dictionaries
    
    :param db:   Should normally be a database handle that is to be used to 
    save the documents stored in doclist.  Set to None if you want to 
    have the function use the feature of creating a new handle for 
    each instance to avoid serialization. 
    :type db:  :class:`mspasspy.db.Database` or None.   Type is not tested
    for efficiency so the function would likely abort with ambiguous messages 
    when used in a parallel workflow.
    
    :param collection:  wf collection name. Default "wf_TimeSeries".
    
    :param dbname:  database name to save data into.  This parameter is 
    referenced ONLY if db is set to None.   
    :type dbname:  string
    
    """
    if db is None:
        dbclient = DBClient()
        # needs to throw an exception of both db and dbname are none
        db = dbclient.get_database(dbname)
    dbcol = db[collection]
    # note plural ids.  insert_one uses "inserted_id".
    # proper english usage but potentially confusing - beware
    wfids = dbcol.insert_many(doclist).inserted_ids
    return wfids


def _save_ensemble_wfdocs(
        ensemble_data,
        db,
        save_schema,
        exclude_keys,
        mode,
        undertaker,
        cremate=False):
    if cremate:
        ensemble_data = undertaker.cremate(ensemble_data)
    else:
        ensemble_data, bodies = undertaker.bring_out_your_dead(ensemble_data)
        undertaker.bury_the_dead(bodies)
        del bodies

    doclist=[]
    # we don't have to test for dead data in the loop below above removes 
    # them so we just use md2doc 
    for d in ensemble_data.member:
        doc, aok, elog = md2doc(d,save_schema=save_schema,exclude_keys=exclude_keys,mode=mode)
        doclist.append(doc)
    # weird trick to get waveform collection name - borrowed from 
    # :class:`mspasspy.db.Database` save_data method code
    wf_collection_name = save_schema.collection("_id")
    # note plural ids.  insert_one uses "inserted_id".
    # proper english usage but potentially confusing - beware
    wfids = db[wf_collection_name].insert_many(doclist).inserted_ids
    return wfids

def _extract_wfdoc(
        mspass_object,
        save_schema,
        exclude_keys,
        mode,
        post_elog=True,
        elog_key="error_log",
        post_history=False,
        history_key="history_data"):
    """
    Wrapper for md2doc to handle posting of error log messages that 
    can be returned by md2doc.  should function correctly with live or dead
    data.  careful to not duplicate error log entries posted for dead data.
    before finalizing be sure that logic is right gp
    """
    doc, aok, elog_md2doc = md2doc(mspass_object,save_schema=save_schema,exclude_keys=exclude_keys,mode=mode)
    if post_elog:
        elog = ErrorLogger()
        if mspass_object.elog.size() > 0:
            elog = mspass_object.elog
            if elog_md2doc.size() > 0:
                elog += elog_md2doc
        elif elog_md2doc.size()>0:
            elog = elog_md2doc
        if elog.size()>0:
            elogdoc = elog2doc(elog) 
            doc[elog_key] = elogdoc
    if not aok or mspass_object.dead():
        # may want to handle this differently
        doc["live"] = False
    else:
        doc["live"] = True
    if post_history:
        # is_empty is part of ProcessingHistory
        if not mspass_object.is_empty():
            doc = history2doc(mspass_object)
            doc[history_key] = doc
    return doc
            
def write_distributed_data( 
    data,
    db,
    save_schema,
    mode="promiscuous",
    storage_mode="gridfs",
    format=None,
    file_format=None,
    overwrite=False,
    exclude_keys=None,
    collection=None,
    data_tag=None,
    post_elog=True,
    post_history=False,
    cremate=False,
    alg_name="write_distributed_data",
    alg_id="0",
):
    """
    Parallel save function for data in a dask bag or pyspark rdd.  
    
    Saving data from a parallel container (i.e. bag/rdd) is a different 
    problem from a serial writer.  Bottlenecks are likely with 
    communication delays talking to the MonboDB server and 
    complexities of file based io.   This function may evalve but is 
    intended as the approach one should use for saving data at the end 
    of a workflow computation.  It currently has no option for 
    an intermediate save as the bag/rdd are always reduced to list of 
    ObjectIDs of saved wf documents.  


    :param data: the data to be written
    :type data: :class:`dask.bag.Bag` or :class:`pyspark.RDD`.
    :param db: the database from which the data are to be written.
    :type db: :class:`mspasspy.db.database.Database`.
    :param mspass_object: the object you want to save.
    :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
    :param mode: This parameter defines how attributes defined with
        key-value pairs in MongoDB documents are to be handled on reading.
        By "to be handled" we mean how strongly to enforce name and type
        specification in the schema for the type of object being constructed.
        Options are ['promiscuous','cautious','pedantic'] with 'promiscuous'
        being the default.  See the User's manual for more details on
        the concepts and how to use this option.
    :type mode: :class:`str`
    :param storage_mode: Must be either "gridfs" or "file.  When set to
        "gridfs" the waveform data are stored internally and managed by
        MongoDB.  If set to "file" the data will be stored in a file system
        with the dir and dfile arguments defining a file name.   The
        default is "gridfs".
    :type storage_mode: :class:`str`
    :param format: the format of the file. This can be one of the
        `supported formats <https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.write.html#supported-formats>`__
        of ObsPy writer. The default the python None which the method
        assumes means to store the data in its raw binary form.  The default
        should normally be used for efficiency.  Alternate formats are
        primarily a simple export mechanism.  See the User's manual for
        more details on data export.  Used only for "file" storage mode.
    :type format: :class:`str`
    :param overwrite:  If true gridfs data linked to the original
        waveform will be replaced by the sample data from this save.
        Default is false, and should be the normal use.  This option
        should never be used after a reduce operator as the parents
        are not tracked and the space advantage is likely minimal for
        the confusion it would cause.   This is most useful for light, stable
        preprocessing with a set of map operators to regularize a data
        set before more extensive processing.  It can only be used when
        storage_mode is set to gridfs.
    :type overwrite:  boolean
    :param exclude_keys: Metadata can often become contaminated with
        attributes that are no longer needed or a mismatch with the data.
        A type example is the bundle algorithm takes three TimeSeries
        objects and produces a single Seismogram from them.  That process
        can, and usually does, leave things like seed channel names and
        orientation attributes (hang and vang) from one of the components
        as extraneous baggage.   Use this of keys to prevent such attributes
        from being written to the output documents.  Not if the data being
        saved lack these keys nothing happens so it is safer, albeit slower,
        to have the list be as large as necessary to eliminate any potential
        debris.
    :type exclude_keys: a :class:`list` of :class:`str`
    :param collection: The default for this parameter is the python
        None.  The default should be used for all but data export functions.
        The normal behavior is for this writer to use the object
        data type to determine the schema is should use for any type or
        name enforcement.  This parameter allows an alernate collection to
        be used with or without some different name and type restrictions.
        The most common use of anything other than the default is an
        export to a diffrent format.
    :param data_tag: a user specified "data_tag" key.  See above and
        User's manual for guidance on how the use of this option.
    :type data_tag: :class:`str`
    """
    if not isinstance(db,Database):
        message = "write_distributed_data:  required arg1 (db) must be an instance of mspasspy.db.Database\n"
        message += "Type of arg1 received is {}".format(str(type(db)))
        raise TypeError(message)
    if storage_mode not in ["file", "gridfs"]:
        raise TypeError("write_distributed_data:  Unsupported storage_mode={}".format(storage_mode))
    if mode not in ["promiscuous", "cautious", "pedantic"]:
        message = "write_distributed_data:  Illegal value of mode={}\n".format(mode)
        message += "Must be one one of the following:  promiscuous, cautious, or pedantic"
        raise MsPASSError(message,ErrorSeverity.Fatal)
    # This uses extracts the first member of the input container and 
    # interogates its type to establish the collection and schema to use.
    testval = data.map(lambda x : x).take(1)
    # take method always returns a list so we need to subscript testval
    # Note tests show the above does not alter the data container and 
    # is more or less a read operation of the first element

    if isinstance(testval[0], (TimeSeries, TimeSeriesEnsemble)):
        save_schema = db.metadata_schema.TimeSeries
    elif isinstance(testval[0], (Seismogram, SeismogramEnsemble)):
        save_schema = db.metadata_schema.Seismogram
    else:
        message = "parallel container appears to have illegal type={}\n".format(str(type(testval[0])))
        message += "Must be a MsPASS seismic data object.   Cannot proceed"
        raise TypeError(message)
    if isinstance(testval[0],(TimeSeries,Seismogram)):
        data_are_atomic = True
    else:
        data_are_atomic = False
     # release this memory - testval could be big for some ensembles
    del testval
    
        
    stedronsky=Undertaker(db)
    # Note although this is a database method it will not refernce 
    # db at all if writing to files.  gridfs, of course, always requries 
    # the database to be defined
    if format == "spark":
        data = data.map(lambda d : db._save_sample_data(
                    d,
                    storage_mode=storage_mode,
                    dir=None,
                    dfile=None,
                    format=format,
                    overwrite=overwrite,
                    )
            )
        if cremate:
            data = data.map(lambda d : stedronsky.cremate(d))
        else:
            # note we could do bury_the_dead as an option here but it 
            # seems ill advised as a possible bottleneck
            # we don't save elog or history here deferring that the next step
            data = data.map(lambda d : stedronsky.mummify(
                                d,
                                post_elog=False,
                                post_history=False,
                                )
                )

        if data_are_atomic:
            data = data.map(lambda  d : _extract_wfdoc(
                            d,save_schema,
                            exclude_keys,
                            mode,
                            post_elog=post_elog,
                            post_history=post_history,
                            )
                )
            data = data.map_partition(lambda d : _partioned_save_wfdoc(
                            d,
                            db,
                            collection=collection,
                            )
                )
        else:
            data = data.map(lambda d : _save_ensemble_wfdocs(
                            d,
                            db,
                            save_schema,
                            exclude_keys,
                            mode,
                            cremate=cremate)
                )
        data = data.collect()
    else:
        data = data.map(db._save_sample_data,
                    storage_mode=storage_mode,
                    dir=None,
                    dfile=None,
                    format=format,
                    overwrite=overwrite,
                    )

        if cremate:
            data = data.map(stedronsky.cremate)
        else:
            # note we could do bury_the_dead as an option here but it 
            # seems ill advised as a possible bottleneck
            # we don't save elog or history here deferring that the next step
            data = data.map(stedronsky.mummify,post_elog=False,post_history=False)

        if data_are_atomic:
            data = data.map(_extract_wfdoc,save_schema,exclude_keys,mode,post_elog=post_elog,post_history=post_history)
            data = data.map_partition(_partioned_save_wfdoc,db,collection=collection)
        else:
            data = data.map(_save_ensemble_wfdocs,db,save_schema,exclude_keys,mode,cremate=cremate)
        data = data.compute()
        
    return data
