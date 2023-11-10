import os
import pandas as pd
import json


from mspasspy.db.database import (Database,
                                  md2doc,
                                  doc2md,
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
    AtomicType,
    Metadata,
    MsPASSError,
    ErrorLogger,
    ErrorSeverity,
    ProcessingHistory,
)

import dask
import pyspark


def read_ensemble_parallel(query,
                           db,
                           collection="wf_TimeSeries",
                           mode="promiscuous",
                           normalize=None,
                           load_history=False,
                           exclude_keys=None,
                           data_tag=None,
                           sort_clause=None,
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

    Arguments are all passed directly from values set within
    read_distributed_data.  See that function for parameter descriptions.
    """
    if sort_clause:
        cursor = db[collection].find(query).sort(sort_clause)
    else:
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

def post2metadata(mspass_object,doc):
    """
    Posts content of key-value pair container doc to Metadata container
    of mspass_object.  Operation only requires that doc be iterable
    over keys and both doc and mspass_object act like associative
    arrays.   In MsPASS that means doc can be either a dict or a
    Metadata container.  Note the contents of doc[k] will overwrite
    any existing content defined by mspass_object[k].  It returns the edited
    copy of mspass_object to mesh with map operator usage in
    read_distributed_data.   Note that Metadata "is a" fundamental component
    of all atomic data. Ensemble objects contain a special instance of
    Metadata we normally refer to as "ensemble metadata".   When handling
    ensembles the content of doc are pushed to the ensemble metadata and
    member attributes are not altered by this function.

    This function is the default for the read_distributed_data
    argument container_merge_function which provides the means to push
    a list of Metadata or dict containers held in a conguent bag/rdd
    with a set of seismic data objects.   Because of that usage it has
    no safeties for efficiency.
    """
    for k in doc:
        mspass_object[k] = doc[k]
    return mspass_object


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
    sort_clause=None,
    container_merge_function=post2metadata,
    container_to_merge=None,
    ensemble_metadata_list=None,
    aws_access_key_id=None,
    aws_secret_access_key=None,
):
    """
    Parallel data reader for seismic data objects.

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
    this function because an absolutely required input is a handle to
    the waveform db collection OR an image of it in another format.
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
    single object.   Second, all database engines, in contrast, can
    work linearly through a table (collection in MongoDB) very fast
    because they cache blocks of records send from the server to the client.
    In MongoDB that means a "cursor" returned by a "find" operation
    can be iterated very fast compared to one-by-one queries of the
    same data.  Why all that is relevant is that arg0 if this function is
    required to be one of three things to work well within these constraints:
        1. An instance of a mspass `Database` handle
           (:class:`mspasspy.db.database.Database`).  Default
           with this input is to read an entire collection.  Use the
           query parameter to limit input.  This mode also implicitly
           implies the result will be a bag/RDD of atomic data.  Note
           that for efficiency when run in this mode the first step
           the algorithm does is load the entire set of documents
           defined by query (or the default of all documents) into a
           pandas DataFrame.  If that approach causes memory issues
           use the `scratchfile` option to buffer the large table into
           a scratch file and then construct a dask or spark DataFrame
           that are designed as containers that can overflow memory.
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
           of atomic mspass data objects.   That means reads will be
           parallel with one reader per worker.
        3. Ensembles are read in parallel with granularity defined by
           a partitioning of the list of queries set by the npartitions
           parameter.  Parallelism is achieved by calling the function
           internal to this function called `read_ensemble_parallel`
           in a map operator.   That function queries the database
           using the query derived form arg0 of this function and uses
           the return to call the `Database.read_data` method.

    Reading all types in a parallel context have a more subtle complexity
    that arises in a number of situations.  That is, many algorithms
    are driven by external lists of attributes with one item in the list
    for each datum to be constructed and posted to the parallel container
    output by this function.   An example is a list of arrival times
    used to create a dataset of waveform segments windowed relative to
    the list of arrival times from continuous data or a dataset built
    from longer time windows (e.g. extracting P wave windows from hour
    long segments created for teleseismic data.).  That requires a special
    type of matching that is very inefficient (i.e. slow) to implement
    with MongoDB (It requires a query for every datum.)   One-to-one
    matching attributes can handled by this function with proper
    use of the two attributes `container_to_merge` and
    `container_merge_function`.   `container_to_merge` must be either
    a dask bag or pyspark RDD depending on the setting of the
    argument `spark_context` (i.e. if `spark_context` is defined the
    function requires the container be an RDD while a dask bag is the default.)
    The related argument, `container_merge_function`, is an optional function
    to use to merge the content of the components of `container_to_merge`.
    The default assumes `container_to_merge` is a bag/RDD of `Metadata`
    or python dict containers that are to be copied (overwriting) the
    Metadata container of each result bag/RDD component.  Note that for
    atomic data that means the Metadata container for each
    `TimeSeries` or `Seismogram` constructed by the reader while for
    ensembles the attributes are posted to the ensemble's `Metadata` container.
    A user supplied function to replace the default must have two arguments
    where arg0 is the seismic data to receive the edits defined by
    `container_to_merge` while arg1 should contain the comparable component
    of `container_to_merge`.   Note this capability is not at all limited to
    `Metadata`.  This function can contain anything that provides input
    for applying an algorithm that alters the datum given a set of parameters
    passed through the `container_to_merge`.

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

    :param query:  optional query to apply to input collection when using
      a :class:`mspasspy.db.Database` as input.  Ignored for dataframe or
      a list input.  Default is None which means no query is used.
    :type query:  python dict defining a valid MongoDB query.

    :param scratchfile: This argument is referenced only when input is
      drive by a :class:`mspasspy.db.Database` handle.  For very large
      datasets loading the entire set of documents that define the
      dataset into memory can be an issue on a system with smaller
      "RAM" memory available.  This optional argument makes this function
      scalable to the largest conceivable seismic data sets.  When defined
      the documents retreived from the database are reformatted and pushed to
      a scratch file with the name defined by this argument.  The contents
      of the file are then reloaded into a dask or spark DataFrame that
      allow the same data to be handled within a more limited memory footprint.
      Note use of this feature is rare and should never be necessary in an
      HPC or cloud cluster.   The default us None which means this that
      database input is loaded directly into memory to initiate construction
      of the parallel container output.
    :type scratchfile:  str

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
      immediately with a ValueError exception
    :type format: :class:`str`

    :param spark_context: If using spark this argument is required.  Spark
      defines the concept of a "context" that is a global control object that
      manages schduling.  See online Spark documentation for details on
      this concept.
    :type spark_context: :class:`pyspark.SparkContext`

    :param npartitions: The number of desired partitions for Dask or the number
      of slices for Spark. By default Dask will use 100 and Spark will determine
      it automatically based on the cluster.  If using this parameter and
      a container_to_merge make sure the number used here matches the partitioning
      of container_to_merge.
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

    :param sort_clause:  When reading ensembles it is sometimes helpful to
      apply a sort clause to each database query.  The type example is
      reading continuous data where there it is necessary to sort the
      data into channels in time order.  If defined this should be a clause
      that can be inserted in the MongoDB sort method commonly applied in
      a line like this:  `cursor=db.wf_miniseed.find(query).sort(sort_clause)`.
      This argument is tested for existence only when reading ensembles
      (implied with list of dict input).
    :type sort_clause:  if None (default) no sorting is invoked when reading
      ensembles.   Other wise should be a python list of tuples
      defining a sort order.
      e.g. [("sta",pymongo.ASCENDING),()"time",pymongo.ASCENDING)]

    :param container_to_merge:  bag/RDD containing data packaged with
      one item per datum this reader is asked to read.   See above for
      details and examples of how this feature can be used.  Default is
      None which turns off this option.
    :type container_to_merge:  dask bag or pyspark RDD.   The number of
      partitions in the input must match the explicit
      (i.e. set with `npartitions`) or implict (defaulted) number of
      partitions.

    :param container_merge_function:  function that defines what to do
      with components of the `container_to_merge` if it is defined.
      Default is a Metadata merge function, which is defined internally
      in this module.  That function assumes `container_to_merge`
      is a bag/RDD of either `Metadata` or python dict containers that
      define key-value pairs to be posted to the output.  (i.e. it
      act like the Metadata += operator.)  A custom function can be
      used here.  A custom function must have only two arguments
      with arg0 the target seismic datum (component the reader is
      creating) and arg1 defining attributes to use to edit the
      datum being created.   Note the default only alters Metadata but
      that is not at all a restriction.  The function must simply return
      a (normally modified) copy of the component it receives as arg0.

    :param ensemble_metadata_list:  When reading ensembles in parallel it is
      often necessary to load global attributes for the ensemble into the
      ensemble's Metadata container.   That data is often not held in atomic
      wf collection documents and needs to be supplied externally.   For that
      reason the Database method called `read_data` has an ensemble_md
      argument for the same purpose when loading one ensemble at a time.
      This argument, in fact, is assumed to be an array of containers
      that can be passed to the "read_data" method
      :py:meth:`mspasspy.db.database.read_data` called by this function.
      The contents of each container are then posted as ensemble Metadata
      for each bag/rdd component returned.  Note this list is assumed to
      not present a memory or serialization issue as the number of ensembles
      in a typical data set are not usually that huge and it would be rare
      to post large objects to ensemble Metadata.  Note this argument is
      referenced ONLY IF reading ensembles, which means arg0 is a list of
      queries.
    :type ensemble_metadata_list:  array-like container (must be subscriptable)
      of python dict or Metadata containers holding attributes to be posted
      to each ensemble's Metadata.  Note the length of this array must
      match the number of components query list or the function will
      abort immediately with an exception.

    :param aws_access_key_id: A part of the credentials to authenticate the user
    :param aws_secret_access_key: A part of the credentials to authenticate the user
    :return: container defining the parallel dataset.  A spark `RDD` if format
      is "Spark" and a dask 'bag' if format is "dask"
    """
    # This is a base error message that is an initialization for
    # any throw error.  We first two type checking of arg0
    message = "read_distributed_data:  "
    if not format in ["dask","spark"]:
        message += "Unsupported value for format={}\n".format(format)
        message += "Must be either 'dask' or 'spark'"
        raise ValueError(message)

    if isinstance(data,list):
        ensemble_mode=True
        i = 0
        for x in list:
            if not isinstance(x,dict):
                message += "arg0 is a list, but component {} has illegal type={}\n".format(i,str(type(x)))
                message += "list must contain only python dict defining queries that define each ensemble to be loaded"
                raise TypeError(message)
        if sort_clause:
            # TODO - conflicting examples of the type of this clause
            # may have a hidden bug in TimeIntervalReader as it has usage
            # differnt from this restricteion
            if not isinstance(sort_clause,[list,str]):
                message += "sort_clause argument is invalid\n"
                message += "Must be either a list or a single string"
                raise TypeError(message)
            if isinstance(sort_clause,list):
                for x in sort_clause:
                    if not isinstance(x,dict):
                        message += "sort_clause value = " + str(sort_clause)
                        message += " is invalid input for MongoDB"
                        raise TypeError(message)
        if ensemble_metadata_list:
            if len(ensemble_metadata_list) != len(data):
                message += "Size mismatch in argument received by this function\n"
                message += "Query list length received as arg0={}\n".format(len(data))
                message += "ensemble_metadata_list argument array length={}\n".format(len(ensemble_metadata_list))
                raise MsPASSError(message,ErrorSeverity.Fatal)
    elif isinstance(data,Database):
        ensemble_mode = False
        dataframe_input = False
        db = data
    elif isinstance(data,
                    (pd.DataFrame, 
                     pyspark.sql.dataframe.DataFrame, 
                     dask.dataframe.core.DataFrame),
                    ):
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

    if container_to_merge:
        if format =="spark":
            if not isinstance(container_to_merge,pyspark.RDD):
                message += "container_to_merge must define a pyspark RDD with format==spark"
                raise TypeError(message)
        else:
            if not isinstance(container_to_merge,dask.bag.core.Bag):
                message += "container_to_merge must define a dask bag with format==dask"
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
                                        mode=mode,
                                        normalize=normalize,
                                        load_history=load_history,
                                        exclude_keys=exclude_keys,
                                        data_tag=data_tag,
                                        sort_clause=sort_clause,
                                        aws_access_key_id=aws_access_key_id,
                                        aws_secret_access_key=aws_secret_access_key,
                                        )
                                )

        else:
            plist = dask.bag.from_sequence(data, npartitions=npartitions)
            plist = dask.bag.map(read_ensemble_parallel,
                    db,
                    collection=collection,
                    mode=mode,
                    normalize=normalize,
                    load_history=load_history,
                    exclude_keys=exclude_keys,
                    data_tag=data_tag,
                    sort_clause=sort_clause,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    )

    else:
        # Logic here gets a bit complex to handle the multiple inputs
        # possible for atomic data.  That is, multiple dataframe
        # implementations and a direct database reading mechanism.
        # The database instance is assumed to converted to a bag/rdd of docs
        # above.  We use a set of internal variables to control the
        # input block used.
        # TODO:  clean up symbol - plist is an awful name
        if dataframe_input:
            if format == "spark":
                plist = spark_context.parallelize(
                    data.to_dict("records"), numSlices=npartitions
                )
            else:
                # Seems we ahve to convert a pandas df to a dask 
                # df to have access to the "to_bag" method of dask 
                # DataFrame.   It may be better to write a small 
                # converter run with a map operator row by row
                if isinstance(data, pd.DataFrame):
                    data = dask.dataframe.from_pandas(data,npartitions=npartitions)
                # format arg s essential as default is tuple
                plist = data.to_bag(format="dict")
        else:
            # logic above should guarantee data is a Database
            # object that can be queried and used to generate the bag/rdd
            # needed to read data in parallel immediately after this block
            if query:
                fullquery=query
            else:
                fullquery=dict()
            if data_tag:
                fullquery['data_tag'] = data_tag
            cursor = data[collection].find(fullquery)
 
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
                    plist=dask.bag.read_text(scratchfile).map(json.loads)
                # Intentionally omit error handler here.  Assume
                # system will throw an error if file open files or write files
                # that will be sufficient for user to understand the problem.
                os.remove(scratchfile)

            else:
                doclist=[]
                for doc in cursor:
                    doclist.append(doc)
                if format == "spark":
                    plist=spark_context.parallelize(doclist)
                else:
                    plist = dask.bag.from_sequence(doclist)
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
    # all cases create plist.   Run the container_merge_function function
    # if requested
    if container_to_merge:
        if format =="spark":
            plist = plist.zip(container_to_merge).map(lambda x: container_merge_function(x[0],x[1]))
        else:
            plist = dask.bag.map(container_merge_function,plist,container_to_merge)

    return plist



def _partitioned_save_wfdoc(doclist,
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

    The function has a feature not used at present, but was preserved here
    as an idea.  At present there is a workaround to provide a
    mechanism to serialize a mspasspy.db.Database class.   When the
    function is called with default parameters it assumes the db
    argument will serialize in dask/spark.   If db is set to None
    each call to the function will construct a
    :class:`mspasspy.db.Database` object for call to the function.
    Because it is done by partition the cost scales by the number of
    partitions not the number of data items to use that algorithm.

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
    # This makes the function more bombproof in the event a database 
    # handle can't be serialized - db should normally be defined
    if db is None:
        dbclient = DBClient()
        # needs to throw an exception of both db and dbname are none
        db = dbclient.get_database(dbname)
    dbcol = db[collection]
    # test for the existence of any dead data.  Handle that case specially
    has_bodies=False
    docarray=[]
    for doc in doclist:
        # clear the wfid if it exists or mongo may overwrite
        if '_id' in doc:
            doc.pop('_id')
        docarray.append(doc)
        if not doc["live"]:
            has_bodies=True
    if has_bodies:
        lifelist=[]
        cleaned_doclist=[]
        for doc in docarray:
            if doc["live"]:
                cleaned_doclist.append(doc)
                lifelist.append(True)
            else:
                lifelist.append(False)
        if len(cleaned_doclist)>0:
            wfids_inserted = dbcol.insert_many(cleaned_doclist).inserted_ids
            wfids=[]
            ii=0
            for i in range(len(lifelist)):
                if lifelist[i]:
                    wfids.append(wfids_inserted[ii])
                    ii += 1
                else:
                    wfids.append(None)
        else:
            wfids=[]
            for i in range(len(doclist)):
                wfids.append(None)
                
    else:
        # this case is much simpler
        # note plural ids.  insert_one uses "inserted_id".
        # proper english usage but potentially confusing - beware
        wfids = dbcol.insert_many(docarray).inserted_ids
    return wfids


def _save_ensemble_wfdocs(
        ensemble_data,
        db,
        save_schema,
        exclude_keys,
        mode,
        undertaker,
        cremate=False,
        data_tag=None,
        return_data=False,
        ):
    """
    Equivalent of _save_wfdocs for ensembles.   It is mostly a loop over
    the member data of an input ensemble.  The only difference is the
    added argument `cremate`.  When set true dead member data will be
    handled with the :py:meth:`mspasspy.util.Undertaker.cremate` method.
    See the docstring of that method for the behavior.
    """
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
        if data_tag:
            doc["data_tag"] = data_tag
        doclist.append(doc)
    # weird trick to get waveform collection name - borrowed from
    # :class:`mspasspy.db.Database` save_data method code
    wf_collection_name = save_schema.collection("_id")
    # note plural ids.  insert_one uses "inserted_id".
    # proper english usage but potentially confusing - beware
    wfids = db[wf_collection_name].insert_many(doclist).inserted_ids
    if return_data:
        return ensemble_data
    else:
        return wfids



def atomic_save_wf_documents(d,
            db,
            save_schema,
            exclude_keys,
            mode,
            data_tag=None,
            undertaker=None,
            cremate=False,
            storage_mode=None,
            save_history=False,
            ):
    """
    """
    if undertaker:
        stedronsky = undertaker
    else:
        stedronsky = Undertaker(db)

    doc, aok, elog_md2doc = md2doc(d,
                                      save_schema=save_schema,
                                      exclude_keys=exclude_keys,
                                      mode=mode,
                                      )
    # cremate or bury dead data. 
    # both return an edited data object reduced to ashes or a skeleton
    # doc and elog contents are handled separately.  When cremate is 
    # true nothing will be saved in the database.  Default will 
    # bury the body leaving a cemetery record.

    if d.dead() or (not aok):
        if cremate:
            d=stedronsky.cremate(d)
        else:
            d=stedronsky.bury(d)
        d.kill()
    else:
        # weird trick to get waveform collection name - borrowed from
        # :class:`mspasspy.db.Database` save_data method code
        wf_collection_name = save_schema.collection("_id")
        # Use save_data's method of handling storage_mode if it isn't set
        #TODO - there are complexities here - sort them out before continuing
        d = db.save_data(d,
                     return_data=True,
                     mode=mode,
                     storage_mode=storage_mode,
                     exclude_keys=exclude_keys,
                     collection=wf_collection_name,
                     data_tag=data_tag,
                     cremate=cremate,
                     save_history=save_history,
                     alg_name="write_distributed_data",
                     )
                     
    return d
    
def atomic_extract_wf_document(d,
                     db,
                     save_schema,
                     exclude_keys,
                     mode,
                     post_elog=True,
                     elog_key="error_log",
                     post_history=False,
                     history_key="history_data",
                     data_tag=None,
                     undertaker=None,
                     cremate=False,
                     ):
    """

    :return:   Edited copy of mspass_obj_list.   See above for 
      description of what "edited" means.
    """

    if undertaker:
        stedronsky = undertaker
    else:
        stedronsky = Undertaker(db)

    doc, aok, elog_md2doc = md2doc(d,
                                      save_schema=save_schema,
                                      exclude_keys=exclude_keys,
                                      mode=mode,
                                      )
    # cremate or bury dead data. 
    # both return an edited data object reduced to ashes or a skeleton
    # doc and elog contents are handled separately.  When cremate is 
    # true nothing will be saved in the database.  Default will 
    # bury the body leaving a cemetery record.

    if d.dead() or (not aok):
        if cremate:
            d=stedronsky.cremate(d)
        else:
            d=stedronsky.bury(d)
        # make sure this is set as it is used in logic below
        # we use this instead of d to handle case with aok False
        doc['live']=False
        d.kill()
    else:
        doc['live']=True
        if post_elog:
            elog = ErrorLogger()
            if d.elog.size() > 0:
                elog = d.elog
                if elog_md2doc.size() > 0:
                    elog += elog_md2doc
            elif elog_md2doc.size()>0:
                elog = elog_md2doc
            if elog.size()>0:
                elogdoc = elog2doc(elog)
                doc[elog_key] = elogdoc
        if post_history:
            # is_empty is part of ProcessingHistory
            if not d.is_empty():
                doc = history2doc(d)
                doc[history_key] = doc
        if data_tag:
            doc["data_tag"] = data_tag
            d['data_tag'] = data_tag
    return doc
   
        


#TODO:   update docstring
def write_distributed_data(
    data,
    db,
    data_are_atomic=True,
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
    return_data=False,
    alg_name="write_distributed_data",
    alg_id="0",
):
    """
    Parallel save function for data in a dask bag or pyspark rdd.

    Saving data from a parallel container (i.e. bag/rdd) is a different
    problem from a serial writer.  Bottlenecks are likely with
    communication delays talking to the MonboDB server and
    complexities of file based io.   Further, there are efficiency issues 
    in the need to reduce transactions that case delays with the MongoDB 
    server.   This function tries to address these issues with two 
    approaches:
        1.   After v2 it uses single function that handles writing the 
             sample data for a datum.   For atomic data that means a 
             single array but for ensembles it is the combination of all
             arrays in the ensemble "member" containers.   The writing 
             functions are passed through a map operator
             so writing is a parallelized per worker.  Note the function 
             also abstracts how the data are written with different 
             things done depending on the "storage_mode" argument.  
        2.   After the sample data are saved we use a MongoDB 
             "update_many" operator with the "many" defined by the 
             partition size.  That reduces database transaction delays 
             by 1/object_per_partition.   
             
    The function also handles data marked dead in a standized way 
    though the use of the :class:`mspasspy.util.Undertaker` now 
    defined within the Database class handle.   The default will 
    call the `bury` method on all dead data which leaves a document 
    containing the datum's Metadata and and error log messages in 
    a collection called "cemetery".   If the `cremate` argument is 
    set True dead data will be vaporized and no trace of them will 
    appear in output.  
    
    Normal behavior for this function is to return only a list of 
    the ObjectIds of the documents saved in the collection defined by 
    the collection argument.   Set the return_data boolean true 
    for intermediate saves.  In that case a bag/RDD of data instead of 
    bag/RDD of ObjectIds will be returned.   Beware memory overflow 
    problems if you return the data - i.e. don't call the compute(dask)
    or collect(spark) method on the result unless you know it will fit 
    in memory.
    
    Return has complexity depending upon the input.  Return is always 
    a bag when format is "dask" and an RDD if format is "spark".  
    Content is the complexity.   When writing atomic data the 
    container by default is a list of ObjectIds of saved waveforms.
    Atomic data marked dead will, by default, be "buried" in the 
    cemetery collection.  Dead data entries in the returned container 
    will contain a None that must be handled by caller if you want to 
    examine the content.  However, when the `return_data` argument is 
    set True dead data will be returned as "mummies", which means the 
    Metadata is retained but the sample arrays are cleared to reduce 
    memory (e.g. TimeSeries are returned as TimeSeries but marked dead and 
    containing no sample data).  With ensembles the default return is 
    a bag/RDD of lists of ObjectIds of the ensembled members successfully 
    saved.   Ensemble members marked dead are always "buried" or 
    "cremated" (cremate argument set True) before attempting to save the 
    wf documents.   When return_data is set True, the ensembles returned 
    will have the dead members removed.  
 

    :param data: parallel container of data to be written
    :type data: :class:`dask.bag.Bag` or :class:`pyspark.RDD`.
    :param db: database handle to manage data saved by this function.
    :type db: :class:`mspasspy.db.database.Database`.
    :param mode: This parameter defines how attributes defined with
        key-value pairs in MongoDB documents are to be handled for writes.
        By "to be handled" we mean how strongly to enforce name and type
        specification in the schema for the type of object being constructed.
        Options are ['promiscuous','cautious','pedantic'] with 'promiscuous'
        being the default.  See the User's manual for more details on
        the concepts and how to use this option.
    :type mode: :class:`str`
    :param storage_mode: Must be either "gridfs" or "file.  When set to
        "gridfs" the waveform data are stored internally and managed by
        MongoDB.  If set to "file" the data will be stored in a file system.
        File names are derived from attributes with the tags "dir" and 
        "dfile" in the standard way.   Any datum for which dir or dfile 
        aren't defined will default to the behaviour of the Database 
        class method `save_data`.  See the docstring for details but the 
        concept is it will always be bombproof even if not ideal.
    :type storage_mode: :class:`str` 
    :param file_format: the format of the file. This can be one of the
        `supported formats <https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.write.html#supported-formats>`__
        of ObsPy writer. The default the python None which the method
        assumes means to store the data in its raw binary form.  The default
        should normally be used for efficiency.  Alternate formats are
        primarily a simple export mechanism.  See the User's manual for
        more details on data export.  Used only for "file" storage mode.
    :type file_format: :class:`str`
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
    :param collection: MongoDB collection name where the Metadata component
        of each datum is to be saved.  It is crucial this name matches 
        the data type of the input as the name is used to infer what 
        type of data is being handled.  That means collection for 
        TimeSeries and TimeSeriesEnsemble data must be wf_TimeSeries and 
        collection for Seismogram and SeismogramEnsemble data must be 
        wf_Seismogram.
    :type collection:  :class:`str`
    :param data_tag: a user specified "data_tag" key.  See above and
        User's manual for guidance on how the use of this option.
    :type data_tag: :class:`str`
    :param post_elog:   boolean controlling how error log messages are 
       handled.  When False (default) error log messages get posted in 
       single transactions with MongoDB to the "elog" collection.   
       When set true error log entries will be posted to as subdocuments to 
       the wf collection entry for each datum.   Setting post_elog True 
       is most useful if you anticipate a run will generate a large number of 
       error that will throttle with processing with a large number of 
       one-at-a-time document saves.  For normal use with small number of 
       errors it is easier to review error issue by inspecting the cemetery 
       collection than having to query the larger wf collection.
     :param post_history:  boolean similar to post_elog for handling 
       object-level history data.   When False (default) all object-level 
       history data is ignored in the save.   It will effectively be lost 
       unless return_data is also set True and the something downstream 
       handles the data.  At present that means the only way to save history 
       data is in a separate save at a later stage of a workflow 
       (note it is possible to only save history without a full data save).
       When set True the object-level history data is posted as a subdocument 
       to the document saved to the specified  wf collection 
       (collection argument value). 
     :param cremate:  boolean controlling handling of dead data.  
       When True dead data will be passed to the `cremate` 
       method of :class:`mspasspy.util.Undertaker` which leaves only 
       ashes to nothing in the return.   When False (default) the 
       `bury` method will be called instead which saves a skeleton 
       (error log and Metadata content) of the results in the "cemetery" 
       collection.
     :param return_data:  When True a bag/RDD will be returned with a 
       slightly modified version of the input.   What "modified" means 
       could change, but at present that means only adding the value of 
       the data_tag argument if it is defined.  When False (default) the 
       bag/RDD will contain only the ObjectId of the wf collection 
       documents saved.  
     :param alg_name:  do not change
     :param alg_id:  algorithm id for object-level history.  Normally
       assigned by global history manager.
       
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
    # This use of the collection name to establish the schema is 
    # a bit fragile as it depends upon the mspass schema naming 
    # convention.  Once tried using take(1) and probing the content of 
    # the container but that has bad memory consequences at least for pyspark
    if collection is None or collection=="wf_TimeSeries":
        save_schema = db.metadata_schema.TimeSeries
    elif collection=="wf_Seismogram":
        save_schema = db.metadata_schema.Seismogram
    else:
        message = "write_distributed_data:   illegal value of argument collection={}\n".format(collection)
        message += "Currently must be either wf_TimeSeries, wf_Seismogram, or default that implies wf_TimeSeries"
        raise ValueError(message)


    stedronsky=Undertaker(db)
    # Note although this is a database method it will not reference
    # db at all if writing to files.  gridfs, of course, always requries
    # the database to be defined.  Assumes if writing to files that 
    # dir and dfile are defined for each datum.  If not default will 
    # not fail but will generate a lot of weird file names with uuid generator
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
        # Undertaker handles all data types.  Live data are ignored so 
        # we don't need to test for evidence of life
        if cremate:
            data = data.map(lambda d : stedronsky.cremate(d))
        else:
            # note we could do bury as an option here but mummify 
            # has a small cost and can resuce memory footprint for 
            # following steps.  Note that with we normally still 
            # call bury later in the database save functions for both 
            # atomic data and ensembles
            data = data.map(lambda d : stedronsky.mummify(
                                d,
                                post_elog=False,
                                post_history=False,
                                )
                )

        if data_are_atomic:
            # With atomic data dead in this implementation we handle 
            # any dead datum with the map operators that save the 
            # wf documents.   Dead data return a None instead of an id 
            # by default and leave a body in the cemetery collection.
            # when return_data is True return either ashes from 
            # cremate (default constructed shell) or the resul of the 
            # muffify method
            if return_data:
                data = data.map(atomic_save_wf_documents,
                                db,
                                save_schema, 
                                exclude_keys, 
                                mode,
                                post_elog=post_elog,
                                post_history=post_history,
                                data_tag=data_tag,
                                undertaker=stedronsky,
                                cremate=cremate,
                                )
            else:
                data = data.map(lambda d : atomic_extract_wf_document(d, 
                                db, 
                                save_schema, 
                                exclude_keys, 
                                mode,
                                post_elog=post_elog,
                                post_history=post_history,
                                data_tag=data_tag,
                                undertaker=stedronsky,
                                cremate=cremate,
                                ))
                data = data.mapPartitions(lambda d : _partitioned_save_wfdoc(
                    d,
                    db,
                    collection=collection,
                    ))
                data = data.collect()                
        else:
            # This step adds some minor overhead, but it can reduce 
            # memory use at a small cost.  Ensembles are particularly 
            # prone to memory problems so for now view this as worth doing
            # Note _save_ensemble_wfdocs is assumed to handle the bodies 
            # cleanly when cremate is False (note when true dead members 
            # are vaporized with no trace)
            if cremate:
                data = data.map(stedronsky.cremate)
            else:
                # note we could do bury_the_dead as an option here but it
                # seems ill advised as a possible bottleneck
                # we don't save elog or history here deferring that the next step
                data = data.map(stedronsky.mummify,post_elog=False,post_history=False)
            data = data.map(lambda d : _save_ensemble_wfdocs(
                            d,
                            db,
                            save_schema,
                            exclude_keys,
                            mode,
                            cremate=cremate,
                            data_tag=data_tag,
                            return_data=return_data,
                            )
                            
                )
            if not return_data:
                data = data.collect()
    else:
        data = data.map(db._save_sample_data,
                    storage_mode=storage_mode,
                    dir=None,
                    dfile=None,
                    format=format,
                    overwrite=overwrite,
                    )

        if data_are_atomic:
            # With atomic data dead in this implementation we handle 
            # any dead datum with the map operators that save the 
            # wf documents.   Dead data return a None instead of an id 
            # by default and leave a body in the cemetery collection.
            # when return_data is True return either ashes from 
            # cremate (default constructed shell) or the resul of the 
            # muffify method
            if return_data:
                data = data.map(atomic_save_wf_documents,
                                db,
                                save_schema, 
                                exclude_keys, 
                                mode,
                                post_elog=post_elog,
                                post_history=post_history,
                                data_tag=data_tag,
                                undertaker=stedronsky,
                                cremate=cremate,
                                )
            else:
                data = data.map(atomic_extract_wf_document,
                                db, 
                                save_schema, 
                                exclude_keys, 
                                mode,
                                post_elog=post_elog,
                                post_history=post_history,
                                data_tag=data_tag,
                                undertaker=stedronsky,
                                cremate=cremate,
                                )
                data = data.map_partitions(_partitioned_save_wfdoc,db,collection=collection)
                # necessary here or the map_partition function will fail 
                # because it will receive a DAG structure instead of a list
                data = data.compute()
        else:
            # This step adds some minor overhead, but it can reduce 
            # memory use at a small cost.  Ensembles are particularly 
            # prone to memory problems so for now view this as worth doing
            # Note _save_ensemble_wfdocs is assumed to handle the bodies 
            # cleanly when cremate is False (note when true dead members 
            # are vaporized with no trace)
            if cremate:
                data = data.map(stedronsky.cremate)
            else:
                # note we could do bury_the_dead as an option here but it
                # seems ill advised as a possible bottleneck
                # we don't save elog or history here deferring that the next step
                data = data.map(stedronsky.mummify,post_elog=False,post_history=False)
            data = data.map(_save_ensemble_wfdocs,
                            db,
                            save_schema,
                            exclude_keys,
                            mode,
                            cremate=cremate,
                            data_tag=data_tag,
                            return_data=return_data,
                            )
            # needed for consistencey with the atomic version when 
            # return_data is False - returns list of wfids on for each ensemble
            # returns the bag if return_data is true
            if not return_data:
                data = data.compute()
    return data


def read_to_dataframe(
    db,
    cursor,
    mode="promiscuous",
    normalize=None,
    load_history=False,
    exclude_keys=None,
    data_tag=None,
    alg_name="read_to_dataframe",
    alg_id="0",
    define_as_raw=False,
    retrieve_history_record=False,
):
    """
    Read the documents defined by a MongoDB cursor into a panda DataFrame.

    The data stucture called a panda DataFrame is heavily used by
    many python user's.  This is convenience function for users wanting to
    use that api to do pure metadata operations.

    Be warned this function originated as a prototype where we experimented
    with using a dask or pyspark DataFrame as an intermediatry for parallel
    readers.   We developed an alternative algorithm that made the
    baggage of the intermediary unnecessary.   The warning is the
    function is not mainstream and may be prone to issues.

    :param db: the database from which the data are to be read.
    :type db: :class:`mspasspy.db.database.Database`.
    :param object_id: MongoDB object id of the wf document to be constructed from
        data defined in the database.  The object id is guaranteed unique and provides
        a unique link to a unique document or nothing.   In the later case the
        function will return a None.
    :type cursor: :class:`pymongo.cursor.CursorType`
    :param mode: reading mode that controls how the function interacts with
        the schema definition for the data type.   Must be one of
        ['promiscuous','cautious','pedantic'].   See user's manual for a
        detailed description of what the modes mean.  Default is 'promiscuous'
        which turns off all schema checks and loads all attributes defined for
        each object read.
    :type mode: :class:`str`
    :param normalize: list of collections that are to used for data
        normalization. (see User's manual and MongoDB documentation for
        details on this concept)  Briefly normalization means common
        metadata like source and receiver geometry are defined in separate
        smaller collections that are linked through this mechanism
        during reads. Default uses no normalization.
    :type normalize: a :class:`list` of :class:`str`
    :param load_history: boolean (True or False) switch used to enable or
        disable object level history mechanism.   When set True each datum
        will be tagged with its origin id that defines the leaf nodes of a
        history G-tree.  See the User's manual for additional details of this
        feature.  Default is False.
    :param exclude_keys: Sometimes it is helpful to remove one or more
        attributes stored in the database from the data's Metadata (header)
        so they will not cause problems in downstream processing.
    :type exclude_keys: a :class:`list` of :class:`str`
    :param collection:  Specify an alternate collection name to
        use for reading the data.  The default sets the collection name
        based on the data type and automatically loads the correct schema.
        The collection listed must be defined in the schema and satisfy
        the expectations of the reader.  This is an advanced option that
        is indended only to simplify extensions to the reader.
    :param data_tag:  The definition of a dataset can become ambiguous
        when partially processed data are saved within a workflow.   A common
        example would be windowing long time blocks of data to shorter time
        windows around a particular seismic phase and saving the windowed data.
        The windowed data can be difficult to distinguish from the original
        with standard queries.  For this reason we make extensive use of "tags"
        for save and read operations to improve the efficiency and simplify
        read operations.   Default turns this off by setting the tag null (None).
    :type data_tag: :class:`str`
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param define_as_raw: a boolean control whether we would like to set_as_origin when loading processing history
    :type define_as_raw: :class:`bool`
    :param retrieve_history_record: a boolean control whether we would like to load processing history
    :type retrieve_history_record: :class:`bool`
    """
    collection = cursor.collection.name
    try:
        wf_collection = db.database_schema.default_name(collection)
    except MsPASSError as err:
        raise MsPASSError(
            "collection {} is not defined in database schema".format(collection),
            "Invalid",
        ) from err
    dbschema = db.database_schema
    mdschema = db.metadata_schema
    this_elog = ErrorLogger()
    md_list=list()
    for doc in cursor:
        # Use the databhase module function doc2md that standardizes 
        # handling of schema constraints and exlcude_keys

        md,aok,elog = doc2md(doc,
                  dbschema,
                  mdschema,
                  collection,
                  exclude_keys,
                  mode,
                  )
        if aok:
            md_list.append(md)
        else:
            this_elog += elog
        
    if elog.size()>0:
        print("WARNING(read_to_dataframe): ",
              elog.size(), 
              " errors were handled during dataframe construction")
        print("All data associated with these errors were dropped.   Error log entries from doc2md follow")
        errorlist=elog.get_error_log()
        for entry in errorlist:
            print(entry.algorithm, entry.message, entry.badness)
    # convert the metadata list to a dataframe
    return pd.json_normalize(map(lambda cur: cur.todict(), md_list))
