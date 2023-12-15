import os
import pandas as pd
import json


from mspasspy.db.database import (
    Database,
    md2doc,
    doc2md,
    elog2doc,
    history2doc,
)
from mspasspy.db.normalize import BasicMatcher

# name collision here requires this alias
from mspasspy.db.normalize import normalize as normalize_function
from mspasspy.util.Undertaker import Undertaker
from mspasspy.db.client import DBClient


from mspasspy.ccore.utility import (
    ErrorLogger,
)

import dask
import pyspark


def read_ensemble_parallel(
    query,
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
    ensemble = db.read_data(
        cursor,
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


def post2metadata(mspass_object, doc):
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
    normalize_ensemble=None,
    load_history=False,
    exclude_keys=None,
    scheduler="dask",
    npartitions=None,
    spark_context=None,
    data_tag=None,
    sort_clause=None,
    container_merge_function=post2metadata,
    container_to_merge=None,
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
           (dask or spark depending on the setting of the scheduler argument)
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
      For atomic data each component is used with the `normalize`
      function to apply one or more normalization operations to each
      datum.   For ensembles, the same operation is done in a loop
      over all ensembles members (i.e. the member objects are atomic
      and normalized in a loop.).  Use `normalize_ensemble` to
      set values in the ensemble Metadata container.
    :type normalize: must be a python list of subclasses of
      the abstract class :class:`mspasspy.db.normalize.BasicMatcher`
      that can be used as the normalization operator in the
      `normalize` function.

     param normalize_ensemble:  This parameter should be used to
       apply normalization to ensemble Metadata (attributes common to
       the entire ensemble.) It will be ignored if reading
       atomic data.  Otherwise it behaves like normalize and is
       assumed to a list of subclasses of :class:`BasicMatcher` objects.
       If using this option you must also specify a valid value for
       the `container_to_merge` argument.  The reason is that currently
       the only efficient way to post any Metadata components to
       an ensemble's Metadata container is via the algorithm used by
       if the `container_to_merge` option is used.  This feature was
       designed with ids in mind where the ids would link to a collection
       that are contain defining properties for what the ensemble is.
       For example, if the ensemble is a "common-source gather"
       the `container_to_merge` could be a bag/RDD of ObjectIds defining
       the `source_id` attribute.  Then the normalize_ensemble list
       could contain an instance of :class:`mspasspy.db.normalize.ObjectIdMatcher`
       created to match and load source data.
     :type normalize_ensemble:  a :class:`list` of :class:`BasicMatcher`.
       :class:`BasicMatchers` are applied sequentialy with the
       `normalize` function with the list of attributes loaded defined
       by the instance.

    :param load_history: boolean (True or False) switch used to enable or
      disable object level history mechanism.   When set True each datum
      will be tagged with its origin id that defines the leaf nodes of a
      history G-tree.  See the User's manual for additional details of this
      feature.  Default is False.

    :param exclude_keys: Sometimes it is helpful to remove one or more
      attributes stored in the database from the data's Metadata (header)
      so they will not cause problems in downstream processing.
    :type exclude_keys: a :class:`list` of :class:`str`

    :param scheduler: Set the format of the parallel container to define the
      dataset.   Must be either "spark" or "dask" or the job will abort
      immediately with a ValueError exception
    :type scheduler: :class:`str`

    :param spark_context: If using spark this argument is required.  Spark
      defines the concept of a "context" that is a global control object that
      manages schduling.  See online Spark documentation for details on
      this concept.
    :type spark_context: :class:`pyspark.SparkContext`

    :param npartitions: The number of desired partitions for Dask or the number
      of slices for Spark. By default Dask will use 100 and Spark will determine
      it automatically based on the cluster.  If using this parameter and
      a container_to_merge make sure the number used here matches the partitioning
      of container_to_merge.  If specified and container_to_merge
      is defined this function will test them for consistency and throw a
      ValueError exception if they don't match.  If not set (i.e. left
      default of None) that test is not done and the function assumes
      container_to_merge also uses default partitioning.
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

    :param aws_access_key_id: A part of the credentials to authenticate the user
    :param aws_secret_access_key: A part of the credentials to authenticate the user
    :return: container defining the parallel dataset.  A spark `RDD` if scheduler
      is "Spark" and a dask 'bag' if scheduler is "dask"
    """
    # This is a base error message that is an initialization for
    # any throw error.  We first two type checking of arg0
    message = "read_distributed_data:  "
    if not scheduler in ["dask", "spark"]:
        message += "Unsupported value for scheduler={}\n".format(scheduler)
        message += "Must be either 'dask' or 'spark'"
        raise ValueError(message)

    if isinstance(data, list):
        ensemble_mode = True
        i = 0
        for x in data:
            if not isinstance(x, dict):
                message += (
                    "arg0 is a list, but component {} has illegal type={}\n".format(
                        i, str(type(x))
                    )
                )
                message += "list must contain only python dict defining queries that define each ensemble to be loaded"
                raise TypeError(message)
        if sort_clause:
            # TODO - conflicting examples of the type of this clause
            # may have a hidden bug in TimeIntervalReader as it has usage
            # differnt from this restricteion
            if not isinstance(sort_clause, [list, str]):
                message += "sort_clause argument is invalid\n"
                message += "Must be either a list or a single string"
                raise TypeError(message)
            if isinstance(sort_clause, list):
                for x in sort_clause:
                    if not isinstance(x, dict):
                        message += "sort_clause value = " + str(sort_clause)
                        message += " is invalid input for MongoDB"
                        raise TypeError(message)
    elif isinstance(data, Database):
        ensemble_mode = False
        dataframe_input = False
        db = data
    elif isinstance(
        data,
        (pd.DataFrame, pyspark.sql.dataframe.DataFrame, dask.dataframe.core.DataFrame),
    ):
        ensemble_mode = False
        dataframe_input = True
        if isinstance(db, Database):
            db = db
        else:
            if db:
                message += "Illegal type={} for db argument - required with dataframe input".format(
                    str(type(db))
                )
                raise TypeError(message)
            else:
                message += "Usage error. An instance of Database class is required for db argument when input is a dataframe"
                raise TypeError(message)
    else:
        message += "Illegal type={} for arg0\n".format(str(type(data)))
        message += "Must be a Dataframe (pandas, spark, or dask), Database, or a list of query dictionaries"
        raise TypeError(message)
    if normalize:
        if isinstance(normalize, list):
            i = 0
            for nrm in normalize:
                if not isinstance(nrm, BasicMatcher):
                    message += (
                        "Illegal type={} for component {} of normalize list\n".format(
                            type(nrm), i
                        )
                    )
                    message += "Must be subclass of BasicMatcher to allow use in normalize function"
                    raise TypeError(message)
        else:
            message += "Illegal type for normalize argument = {}\n".format(
                type(normalize)
            )
            message += "Must be a python list of implementations of BasicMatcher"
            raise TypeError(message)

    if container_to_merge:
        if scheduler == "spark":
            if not isinstance(container_to_merge, pyspark.RDD):
                message += (
                    "container_to_merge must define a pyspark RDD with scheduler==spark"
                )
                raise TypeError(message)
            container_partitions = container_to_merge.getNumPartitions()

        else:
            if not isinstance(container_to_merge, dask.bag.core.Bag):
                message += (
                    "container_to_merge must define a dask bag with scheduler==dask"
                )
                raise TypeError(message)
            container_partitions = container_to_merge.npartitions
        if npartitions:
            # This error handler only works if npartitions is set in the
            # arg list.  Can't test the data container here as it doesn't
            # exist yet and putting it inside the logic below would be awkward
            # and could slow execution
            if container_partitions != npartitions:
                message += "container_to_merge number of partitions={}\n".format(
                    container_partitions
                )
                message += (
                    "must match value of npartitions passed to function={}".format(
                        npartitions
                    )
                )
                raise ValueError(message)
    if normalize_ensemble:
        if container_merge_function is None:
            message += "normalize_ensemble option requires specifying a bag/RDD passed via container_to_merge argument\n"
            message += "Received a (default) None value for container_to_merge argument"
            raise ValueError(message)
        if isinstance(normalize_ensemble, list):
            i = 0
            for nrm in normalize_ensemble:
                if not isinstance(nrm, BasicMatcher):
                    message += "Illegal type={} for component {} of normalize_ensemble list\n".format(
                        type(nrm), i
                    )
                    message += "Must be subclass of BasicMatcher to allow use in normalize function"
                    raise TypeError(message)
        else:
            message += "Illegal type for normalize_ensemble argument = {}\n".format(
                type(normalize)
            )
            message += "Must be a python list of implementations of BasicMatcher"
            raise TypeError(message)

    # This has a fundamentally different algorithm for handling
    # ensembles than atomic data.  Ensembles are driven by a list of
    # queries while atomic data are driven by a dataframe.
    # the dataframe is necessary in this context because MongoDB
    # cursors can not be serialized while Database can.
    if ensemble_mode:
        if normalize_ensemble:
            if isinstance(normalize_ensemble, list):
                i = 0
                for nrm in normalize_ensemble:
                    if not isinstance(nrm, BasicMatcher):
                        message += "Illegal type={} for component {} of normalize list\n".format(
                            type(nrm), i
                        )
                        message += "Must be subclass of BasicMatcher to allow use in normalize function"
                        raise TypeError(message)
            else:
                message += "Illegal type for normallze_ensemble argument = {}\n".format(
                    type(normalize_ensemble)
                )
                message += "Must be a python list of implementations of BasicMatcher"
                raise TypeError(message)
        if scheduler == "spark":
            # note this works only because parallelize treats a None as default
            # and we use None as our default too - could break with version change
            plist = spark_context.parallelize(data, numSlices=npartitions)
            plist = plist.map(
                lambda q: read_ensemble_parallel(
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
            # note same maintenance issue as with parallelize above
            plist = dask.bag.from_sequence(data, npartitions=npartitions)
            plist = plist.map(
                read_ensemble_parallel,
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
            if scheduler == "spark":
                plist = spark_context.parallelize(
                    data.to_dict("records"), numSlices=npartitions
                )
            else:
                # Seems we ahve to convert a pandas df to a dask
                # df to have access to the "to_bag" method of dask
                # DataFrame.   It may be better to write a small
                # converter run with a map operator row by row
                if isinstance(data, pd.DataFrame):
                    data = dask.dataframe.from_pandas(data, npartitions=npartitions)
                # format arg s essential as default is tuple
                plist = data.to_bag(format="dict")
        else:
            # logic above should guarantee data is a Database
            # object that can be queried and used to generate the bag/rdd
            # needed to read data in parallel immediately after this block
            if query:
                fullquery = query
            else:
                fullquery = dict()
            if data_tag:
                fullquery["data_tag"] = data_tag
            cursor = data[collection].find(fullquery)

            if scratchfile:
                # here we write the documents all to a scratch file in
                # json and immediately read them back to create a bag or RDD
                with open(scratchfile, "w") as outfile:
                    for doc in cursor:
                        json.dump(doc, outfile)
                if scheduler == "spark":
                    # the only way I could find to load json data in pyspark
                    # is to use an intermediate dataframe.   This should
                    # still parallelize, but will probably be slower
                    # if there is a more direct solution should be done here.
                    plist = spark_context.read.json(scratchfile)
                    # this is wrong above also don't see how to do partitions
                    # this section is broken until I (glp) can get help
                    # plist = plist.map(to_dict,"records")
                else:
                    plist = dask.bag.read_text(scratchfile).map(json.loads)
                # Intentionally omit error handler here.  Assume
                # system will throw an error if file open files or write files
                # that will be sufficient for user to understand the problem.
                os.remove(scratchfile)

            else:
                doclist = []
                for doc in cursor:
                    doclist.append(doc)
                if scheduler == "spark":
                    plist = spark_context.parallelize(doclist, numSlices=npartitions)
                else:
                    plist = dask.bag.from_sequence(doclist, npartitions=npartitions)
                del doclist

        # Earlier logic make list a bag/rdd of docs - above converts dataframe to same
        if scheduler == "spark":
            plist = plist.map(
                lambda doc: db.read_data(
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
        if scheduler == "spark":
            plist = plist.zip(container_to_merge).map(
                lambda x: container_merge_function(x[0], x[1])
            )
        else:
            plist = dask.bag.map(container_merge_function, plist, container_to_merge)
    if normalize_ensemble:
        for nrm in normalize_ensemble:
            if scheduler == "spark":
                plist = plist.map(lambda d: normalize_function(d, nrm))
            else:
                plist = plist.map(normalize_function, nrm)

    return plist


def _partitioned_save_wfdoc(
    partition_iterator,
    db,
    collection="wf_TimeSeries",
    dbname=None,
) -> list:
    """
    Internal function used to save the core wf document data for atomic mspass
    data objects.

    This function is intended for internal use only as a component of
    the function write_distributed_data.   It uses a bulk write
    method (pymongo's insert_many method) to reduce database
    transaction delays.

    This function has a strong dependency on a method for handling
    dead data within `write_distributed_data`.   That is, each document
    it processes is ASSUMED (there are no error handlers since
    this is an internal function) to have a boolean value set with the
    key "llve".   When True that document is pushed to MongoDB.  When
    False the contents of the document are ignored.  The return of this
    function is a list of MongoDB `ObjectId`s of live data inserted
    with a None in the list for any datum marked dead ("live":False).
    This causes the size of the return list to be the same as the
    input iterator length (normally partition size, but usually
    truncated at the last partition).   dask and pyspark handle the
    partitioned list and concatenate the sequence of lists into a single
    long list.   Counting None values in that list provides a way to
    cross-check the number of data killed in a workflow.

    The function has a feature not used at present, but was preserved here
    as an idea.  At present there is a workaround to provide a
    mechanism to serialize a mspasspy.db.Database class.   When the
    function is called with default parameters it assumes the db
    argument will serialize in dask/spark.   If db is set to None
    each call to the function will construct a
    :class:`mspasspy.db.Database` object for call to the function.
    Because it is done by partition the cost scales by the number of
    partitions not the number of data items to use that algorithm.

    :param partition_iterator :  This parameter is an iterator
      that is expected to contain a series of python dict containers
      defining the translation of data Metadata to the format needed
      by pymong (a python dict).  It could be used as a regular function
      with a python list, but normal use with dask map_partition or
      pyspark's mapPartition send the function an iterable that can
      only be traversed ONCE.   In that situation, the iterator value
      is a component of the bag/RDD that is being handled and the entire
      range received in a single call is defined by the partition size of
      the bag/RDD.
    :type partition_iterator:  iterable container of python dict
      translated from seismic data Metadata.

    :param db:   Should normally be a database handle that is to be used to
    save the documents stored in partition_iterator .  Set to None if you want to
    have the function use the feature of creating a new handle for
    each instance to avoid serialization.
    :type db:  :class:`mspasspy.db.Database` or None.   Type is not tested
    for efficiency so the function would likely abort with ambiguous messages
    when used in a parallel workflow.

    :param collection:  wf collection name. Default "wf_TimeSeries".

    :param dbname:  database name to save data into.  This parameter is
    referenced ONLY if db is set to None.
    :type dbname:  string

    :return:  a python list of ObjectIds of the inserted documents

    """
    # This makes the function more bombproof in the event a database
    # handle can't be serialized - db should normally be defined
    if db is None:
        dbclient = DBClient()
        # needs to throw an exception of both db and dbname are none
        db = dbclient.get_database(dbname)
    dbcol = db[collection]
    # test for the existence of any dead data.  Handle that case specially
    # Very important to realize the algorithm is complicated by the fact that
    # partition_iterator can normally only be traversed once.  Hence
    # we copy documents to a new container (cleaned doclit) and define the pattern of
    # anty dead data with the boolean list lifelist
    has_bodies = False
    docarray = []
    for doc in partition_iterator:
        # clear the wfid if it exists or mongo may overwrite
        if "_id" in doc:
            doc.pop("_id")
        docarray.append(doc)
        if not doc["live"]:
            has_bodies = True
    if has_bodies:
        lifelist = []
        cleaned_doclist = []
        for doc in docarray:
            if doc["live"]:
                cleaned_doclist.append(doc)
                lifelist.append(True)
            else:
                lifelist.append(False)
        if len(cleaned_doclist) > 0:
            wfids_inserted = dbcol.insert_many(cleaned_doclist).inserted_ids
            wfids = []
            ii = 0
            for i in range(len(lifelist)):
                if lifelist[i]:
                    wfids.append(wfids_inserted[ii])
                    ii += 1
                else:
                    wfids.append(None)
        else:
            wfids = []
            for i in range(len(docarray)):
                wfids.append(None)

    else:
        # this case is much simpler
        # note plural ids.  insert_one uses "inserted_id".
        # proper english usage but potentially confusing - beware
        wfids = dbcol.insert_many(docarray).inserted_ids

    return wfids


class pyspark_mappartition_interface:
    """
    Interface class required for pyspark mapPartition.

    This class is a workaround for a limitation of pyspark's api for
    mapPartition that is a legacy of its scala foundation.   mapPartition
    only accepts a function name as arg0 and has no provision for
    any optional arguments to the function.   An alternative solution
    found in web sources was "currying" and it might have been possible
    to do this with just a function wrapper with fixed kwarg values
    defined with in the `write_distributed_data` function.   In this case,
    however, because `write_distributed_data` handles an entire bag/rdd
    as input a class can be created at the initialization stage of that
    function.  Otherwise the overhead of a wrapper would likely be smaller.

    Perhaps the feature most useful to make this a class is the
    safety valve if db is a None.  (see below)

    The method `partioned_save_wfdoc` is just an alias for
    the file scope function `_partitioned_save_wfdoc` with the parameters
    for that function defined by self content.
    """

    def __init__(
        self,
        db,
        collection,
        dbname=None,
    ):
        """
        Constructor - called in write_distributed_data.

        :param db:  database handle to use for saving wf documents with
          by partition.  If passed a None for this value the constructor
          will attempt to create a default connection to MongoDB and
          use the dbname argument to fetch the database to use.

        :collection:  name of MongoDB collection where documents are to be
          written.
        """
        if db is None:
            if dbname is None:
                message = "pyspark_mappartion_interface constructor:  invalid parameter combination\n"
                message += "Both db (arg0) and dbname (arg2) values are None.  One or the other must be defined"
                raise ValueError(message)
            dbclient = DBClient()
            self.db = dbclient.get_database(dbname)
        else:
            self.db = db
        self.collection = collection

    def partitioned_save_wfdoc(self, iterator):
        """
        Method used as a wrapper to pass on to pyspark's mapPartitions
        operator.  iterator is assumed to be the iterator passed to
        the function per partition by mapPartitions.
        """
        wfidlist = _partitioned_save_wfdoc(
            iterator, self.db, collection=self.collection
        )
        return wfidlist


def post_error_log(d, doc, other_elog=None, elog_key="error_log") -> dict:
    """
    Posts error log data as a subdocument to arg0 datum (symbol d).

    write_distributed_data has a "post_elog" boolean.  By default
    elog entries for any atomic seismic datum will be posted
    one-at-a-time with insert_one calls to the "elog" collection.
    If a workflow expects a large number of error log entries that
    high database traffic can be a bottleneck.  This function is
    used within `write_distributed_data` to avoid that by posting
    the same data to subdocuments attached to wf documents saved
    with live data.

    Note dead data are never handled by this mechanism.  They always
    end up in either the abortions or cemetery collections.
    """
    elog = ErrorLogger()
    if d.elog.size() > 0:
        elog = d.elog
    elif other_elog:
        if other_elog.size() > 0:
            elog += other_elog
    if elog.size() > 0:
        elogdoc = elog2doc(elog)
        doc[elog_key] = elogdoc
    return doc


def _save_ensemble_wfdocs(
    ensemble_data,
    db,
    save_schema,
    exclude_keys,
    mode,
    undertaker,
    normalizing_collections,
    cremate=False,
    post_elog=True,
    save_history=False,
    post_history=False,
    history_key="history_data",
    data_tag=None,
):
    """
    Internal function that saves wf documents for all live members of
    ensemkble objects.


    Ensembles have a built in efficiency in database transactions
    not possible with atomic data.  That is, bulk inserts defined
    by the number of live members in the ensemble are a natural way to
    save the documents extracted from ensemble member Metadata.

    Enembles have a complextity in handling data marked dead.  First, if
    an ensemble itself is marked dead the function assumes earlier logic
    marked all the members dead.   That assumption is true in this context
    as this is an internal function, but be cautious if this code is
    reused elsewhere.   For normal use, the Undertaker is called
    in a way that separates the living and the dead
    (a application of the best python joke ever:  bring_out_your_dead).
    The dead are buried by default.  If cremate is set true dead members
    are vaporized leaving no trace in the database.

    The booleans post_elog and post_history can impact performance of
    this function.   Default is the fastest mode where post_elog is
    set True and save_history is off (False).  When post_elog is set
    False, any error log entries will will cause error log data to
    be saved to the "elog" collection one document at a time.
    Similarly, if save_history is set True and the history feature is
    enabled every datum will generate a call to save a document in the
    "history" collection.  The idea is one would not do that unless the
    dataset is fairly small or other steps are a bigger throttle on the
    throughput.  For large data sets with history, one should also set
    post_history True.  Then the history data will be posted as a
    subdocument with the wf documents saved by this function for
    each live ensemble member.

    :param ensemble_data:   ensemble object containing data to
    be saved.
    :type ensemble_data:  assumed to be a TimeSeriesEsnemble or
    SeismgoramEnsembles.  Because this is an internal function there
    are no safeties to test that assumption.
    :param db:  Database handle for all database saves
    :param save_schema:  schema object - passed directly to md2doc.  See
    that function's docstring for description.
    :param exclude_keys:  list of metadata keys to discard when translating
      metadata to python dict (document).  Sent verbatim to md2doc.
    :param mode:  one of "promiscuous", "cautious", or "pedantic" used to
      define handling of mismatches between schema definitions defined by
      the save_schema argument and Metadata.  See Database docstring and
      User's manual for description of this common argument.
    :poram undertaker:  Instance of :class:`mspasspy.util.Undertaker`
      to handle dead data (see above)
    :param normalizing_collections: see docstring for `write_distributed_data`.
    :param cremate:  tells Undertaker how to handle dead data (see above)
    :param post_elog:  see above
    :param save_history:  see above
    :param post_history:  see above
    :history_key:  name to use for posting history data if post_history is
    set True.  Ignored if False.,
    :param data_tag:  Most data saved with `write_distributed_data` should
    use a data tag string to define the state of processing of that data.
    Default is None, but normal use should set it as an appropriate string
    defining what this dataset is.  D
    """
    if cremate:
        ensemble_data = undertaker.cremate(ensemble_data)
    else:
        ensemble_data, bodies = undertaker.bring_out_your_dead(ensemble_data)
        undertaker.bury(bodies)
        del bodies

    # Need to handle empty ensembles.  Undertaker removes dead bodies
    # for both bury and cremate from ensembles so we can end up
    # with an empty ensemble.  Normal return will be an empty list
    # for this case
    if len(ensemble_data.member) == 0 or ensemble_data.dead():
        wfids = []
    else:
        doclist = []
        # we don't have to test for dead data in the loop below above removes
        # them so we just use md2doc.  We do, however, have to handle
        # md2doc failure signaled with aok False
        for d in ensemble_data.member:
            doc, aok, elog = md2doc(
                d,
                save_schema=save_schema,
                exclude_keys=exclude_keys,
                mode=mode,
                normalizing_collections=normalizing_collections,
            )
            if aok:
                if data_tag:
                    doc["data_tag"] = data_tag
                if "_id" in doc:
                    doc.pop("_id")
                # Handle the error log if it is not empty
                # Either post it to the doc or push the entry to the database
                if d.elog.size() > 0 or elog.size() > 0:
                    doc = post_error_log(d, doc, other_elog=elog)
                else:
                    if elog.size() > 0:
                        d.elog += elog
                    elog_id = db._save_elog(d)
                    doc["elog_id"] = elog_id
                if save_history:
                    if post_history:
                        # is_empty is part of ProcessingHistory
                        if not d.is_empty():
                            doc = history2doc(d)
                            doc[history_key] = doc
                    else:
                        history_id = db._save_history(d)
                        doc["history_id"] = history_id
                if data_tag:
                    doc["data_tag"] = data_tag
                doclist.append(doc)
            else:
                d.elog += elog
                d.kill()
                if cremate:
                    d = undertaker.cremate(d)
                else:
                    d = undertaker.bury(d)

        # weird trick to get waveform collection name - borrowed from
        # :class:`mspasspy.db.Database` save_data method code
        wf_collection_name = save_schema.collection("_id")
        # note plural ids.  insert_one uses "inserted_id".
        # proper english usage but potentially confusing - beware
        if len(doclist) > 0:
            wfids = db[wf_collection_name].insert_many(doclist).inserted_ids
        else:
            wfids = []
    return wfids


def _atomic_extract_wf_document(
    d,
    db,
    save_schema,
    exclude_keys,
    mode,
    normalizing_collections,
    post_elog=True,
    elog_key="error_log",
    post_history=False,
    save_history=False,
    history_key="history_data",
    data_tag=None,
    undertaker=None,
    cremate=False,
):
    """
    This is an internal function used in a map operator to
    extract the Metadata contents of atomic MsPASS seismic
    data objects returning an edited version of the contents
    as a python dictionary that write_distributed_data later passes
    to MongoDB to be saved as wf documents.

    This function does only half of the steps in the related function
    _save_ensemble_wfdocs.  That is it only does the metadata extraction
    and handling of dead data.  It leaves saving the wf documents to
    a the partitioned save function.

    If the datum received is marked dead it is handled by the
    instance of :class:`mspasspy.util.Undertaker` passed with the
    undertaker argument.  If cremate is set True the returned contents
    will be minimal.  All dead data will have the attribute "live"
    set to a boolean False.  All live data, will have tha value set True.

    Error log and history data handling are as described in the
    docstring for `write_distributed_data` with which this function is
    intimately linked.  Similarly all the argument descriptions can
    be found in that docstring.

    :return:   python dict translation of Metadata container of input
    datum d.   Note the return always has a boolean value associated
    with the key "live".  That value is critical downstream from this
    function in the partition-based writer to assure the contents of
    dead data are not store in a wf collection.
    """

    if undertaker:
        stedronsky = undertaker
    else:
        stedronsky = Undertaker(db)

    doc, aok, elog_md2doc = md2doc(
        d,
        save_schema=save_schema,
        exclude_keys=exclude_keys,
        mode=mode,
        normalizing_collections=normalizing_collections,
    )
    # cremate or bury dead data.
    # both return an edited data object reduced to ashes or a skeleton
    # doc and elog contents are handled separately.  When cremate is
    # true nothing will be saved in the database.  Default will
    # bury the body leaving a cemetery record.

    if d.dead() or (not aok):
        # this posts any elog content to error of d so bury will
        # save it
        d.elog += elog_md2doc
        d.kill()
        if cremate:
            d = stedronsky.cremate(d)
        else:
            d = stedronsky.bury(d)
        # make sure this is set as it is used in logic below
        # we use this instead of d to handle case with aok False
        doc["live"] = False
        # d.kill()
    else:
        doc["live"] = True
        if post_elog:
            doc = post_error_log(d, doc, other_elog=elog_md2doc)
        else:
            if d.elog.size() > 0 or elog_md2doc.size() > 0:
                d.elog += elog_md2doc  # does nothing if rhs is empty
                elog_id = db._save_elog(d)
                doc["elog_id"] = elog_id
        if save_history:
            if post_history:
                # is_empty is part of ProcessingHistory
                if not d.is_empty():
                    doc = history2doc(d)
                    doc[history_key] = doc
            else:
                history_id = db._save_history(d)
                doc["history_id"] = history_id
        if data_tag:
            doc["data_tag"] = data_tag
    return doc


def write_distributed_data(
    data,
    db,
    data_are_atomic=True,
    mode="promiscuous",
    storage_mode="gridfs",
    scheduler=None,
    file_format=None,
    overwrite=False,
    exclude_keys=None,
    collection="wf_TimeSeries",
    data_tag=None,
    post_elog=False,
    save_history=False,
    post_history=False,
    cremate=False,
    normalizing_collections=["channel", "site", "source"],
    alg_name="write_distributed_data",
    alg_id="0",
) -> list:
    """
    Parallel save function for termination of a processing script.

    Saving data from a parallel container (i.e. bag/rdd) is a different
    problem from a serial writer.  Bottlenecks are likely with
    communication delays talking to the MonboDB server and
    complexities of file based io.   Further, there are efficiency issues 
    in the need to reduce transactions that cause delays with the MongoDB 
    server.   This function tries to address these issues with two 
    approaches:
        1.   Since v2 it uses single function that handles writing the 
             sample data for a datum.   For atomic data that means a 
             single array but for ensembles it is the combination of all
             arrays in the ensemble "member" containers.   The writing 
             functions are passed through a map operator
             so writing is a parallelized per worker.  Note the function 
             also abstracts how the data are written with different 
             things done depending on the "storage_mode" attribute 
             that can optionally be defined for each atomic object.
        2.   After the sample data are saved we use a MongoDB 
             "update_many" operator with the "many" defined by the 
             partition size.  That reduces database transaction delays 
             by 1/object_per_partition.  For ensembles the partitioning 
             is natural with bulk writes controlled by the number of 
             ensemble members. 

    The function also handles data marked dead in a standardized way 
    though the use of the :class:`mspasspy.util.Undertaker` now 
    defined within the Database class handle.   The default will 
    call the `bury` method on all dead data which leaves a document 
    containing the datum's Metadata and and error log messages in 
    a collection called "cemetery".   If the `cremate` argument is 
    set True dead data will be vaporized and no trace of them will 
    appear in output.  

    To further reduce database traffic the function has two 
    (boolean) options called `post_elog` and `post_history`.  
    When set True the elog and/or object-level history data will 
    be posted as subdocuments in the output collection instead of 
    the normal (at least as defined by the Database handle) way 
    these data are saved (In Database the error log is saved to the 
    "elog" collection and the history data is saved to "history".)
    Note post_history is ignored unless the related `save_history`
    argument is changed from the default False ot True.

    A peculiarity that is a consequence of python's "duck typing" is that 
    the user must give the writer some hints about the type of data objects 
    it is expected to handle.  Rather than specify a type argument, 
    the type is inferred from two arguments that are necessary anyway:
    (1)  the `collection` argument value, and (2) the boolean 
    `data_are_atomic`.   The idea is that `collection` is used to 
    determine of the writer is handling TimeSeries or Seismogram 
    objects ("wf_TimeSeries" or "wf_Seismogram" values respectively)\
    and the boolean is used, as the name implies, to infer if the 
    data are atomic or ensembles.  

    This function should only be used as the terminal step 
    of a parallel workflow (i.e. a chain of map/reduce operators).
    This function will ALWAYS initiate a lazy computation on such a chain of 
    operators because it calls the "compute" method for dask and the 
    "collect" method of spark before returning.  It then always returns 
    a list of ObjectIds of  live, saved data.   The function is dogmatic 
    about that because the return can never be a bag/RDD of the the 
    data.

    If your workflow requires an intermediate save (i.e. saving data 
    in an intermediate step within a chain of map/reduce opeators)
    the best approach at present is to use the `save_data` method of 
    the `Database` class in a variant of the following (dask) example
    that also illustrates how this function is used as the terminator 
    of a chain of map-reduce operators.

    ```
       mybag = read_distributed_data(db,collection='wf_TimeSeries')
       mybag = mybag.map(detrend)   # example
       # intermediate save
       mybag = mybag.map(db.save_data,collection="wf_TimeSeries")
       # more processing - trivial example
       mybag = mybag.map(filter,'lowpass',freq=1.0)
       # termination with this function
       wfids = write_distributed_data(mybag,db,collection='wf_TimeSeries')
    ```   

    The `storage_mode` argument is a constant that defines how the 
    SAMPLE DATA are to be stored.  Currently this can be "files" or 
    "gridfs", but be aware future evolution may extend the options.  
    "gridfs" is the default as the only complexity it has is a speed 
    throttle by requiring the sample data to move through MongoDB and 
    the potential to overflow the file system where the database is stored. 
    (See User's Manual for more on this topic.).   Most users, however, 
    likely want to use the "files" option for that parameter.  There are, 
    however, some caveats in that use that users MUST be aware of before
    using that option with large data sets.   Since V2 of MsPASS 
    the file save process was made more robust by allowing a chain of 
    options for how the actual file name where data is stored is set.  
    The algorithm used here is a private method in 
    :class:`mspasspy.db.Database` called `_save_sample_data_to_file`. 
    When used here that that function is passed a None type for dir and 
    dfile.   The EXPECTED use is that you as a user should set the 
    dir and dfile attribute for EVERY datum in the bag/RDD this function is 
    asked to handle.  That allows each atomic datum to define what the 
    file destination is.  For ensembles normal behavior is to require the 
    entire ensemble content to be saved in one file defined by the dir 
    and dfile values in the ensemble's Metadata container.  
    THE WARNING is that to be robust the file writer will alway default 
    a value for dir and dfile.  The default dir is the run directory. 
    The default dfile (if not set) is a unique name created by a 
    uuid generator.  Care must be taken in file writes to make sure 
    you don't create huge numbers of files that overflow directories or 
    similar file system errors that are all to easy to do with large 
    data set saves.  See the User Manual for examples of how to set 
    output file names for a large data set.

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

    :param scheduler:  name of parallel scheduler being used by this writer. 
      MsPASS currently support pyspark and dask.  If arg0 is an RDD 
      scheduler must be "spark" and arg0 defines dask bag schduler must 
      be "dask".   The function will raise a ValueError exception of 
      scheduler and the type of arg0 are not consistent or if the 
      value of scheduler is illegal.  Note with spark the context is 
      not required because of how this algorithm is structured.
    :type scheduler:  string  Must be either "dask" or "spark".  Default 
      is None which is is equivalent to the value of "dask".

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

    :param collectiion:   name of wf collection where the documents 
       derived from the data are to be saved.  Standard values are 
       "wf_TimeSeries" and "wf_Seismogram" for which a schema is defined in 
       MsPASS.   Normal use should specify one or the other.   The default is 
       "wf_TimeSeries"  but normal usage should specify this argument 
       explicitly for clarity in reuse. 
    :type collection:  :class:`str`

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

    :param data_tag: a user specified "data_tag" key.  See above and
        User's manual for guidance on how the use of this option.
    :type data_tag: :class:`str`

    :param post_elog:   boolean controlling how error log messages are 
       handled.  When False (default) error log messages get posted in 
       single transactions with MongoDB to the "elog" collection.   
       When set True error log entries will be posted to as subdocuments to 
       the wf collection entry for each datum.   Setting post_elog True 
       is most useful if you anticipate a run will generate a large number of 
       error that could throttle processing with a large number of 
       one-at-a-time document saves.  For normal use with small number of 
       errors it is easier to review error issue by inspecting the elog
       collection than having to query the larger wf collection.

     :param save_history:  When set True (default is False) write will 
       save any object-level history data saved within the input data objects.
       The related boolean (described below) called post_history controls 
       how such data is saved if this option is enable.  Note post_history 
       is ignored unless save_history is True.

     :param post_history:  boolean similar to post_elog for handling 
       object-level history data.  It is, however, only handled if the 
       related boolean "save_history" is set True. When post_history is 
       set True the history data will be saved as a subdocument in the wf
       document saved for each live, atomic datum (note for ensembles 
       that means all live members).   When False each atomic datum 
       will generate a insert_one transaction with MongoDB and save the 
       history data in  the "history" collection.  It then sets the 
       attribute with key "history_id" to the ObjectId of the saved 
       document.  The default for this argument is True to avoid 
       accidentally throttling workflows on large data sets.  The default 
       for save_history is False so overall default behavior is to drop 
       any history data. 

     :param cremate:  boolean controlling handling of dead data.  
       When True dead data will be passed to the `cremate` 
       method of :class:`mspasspy.util.Undertaker` which leaves only 
       ashes to nothing in the return.   When False (default) the 
       `bury` method will be called instead which saves a skeleton 
       (error log and Metadata content) of the results in the "cemetery" 
       collection.

     :param normalizing_collections:  list of collection names dogmatically treated
          as normalizing collection names.  The keywords in the list are used 
          to always (i.e. for all modes) erase any attribute with a key name 
          of the form `collection_attribute where `collection` is one of the collection 
          names in this list and attribute is any string.  Attribute names with the "_" 
          separator are saved unless the collection field matches one one of the 
          strings (e.g. "channel_vang" will be erased before saving to the 
          wf collection while "foo_bar" will not be erased.)  This list should 
          ONLY be changed if a different schema than the default mspass schema 
          is used and different names are used for normalizing collections.  
          (e.g. if one added a "shot" collection to the schema the list would need 
          to be changed to at least add "shot".)
     :type normalizing_collection:  list if strings defining collection names.

     :param alg_name:  do not change
     :param alg_id:  algorithm id for object-level history.  Normally
       assigned by global history manager.

    """
    # We don't do type check on the data argument assuming dask or
    # spark will throw errors that make the mistake clear.
    # Too awkward to use an isinstance test so for now at least we don't
    # test the type of data
    if not isinstance(db, Database):
        message = "write_distributed_data:  required arg1 (db) must be an instance of mspasspy.db.Database\n"
        message += "Type of arg1 received is {}".format(str(type(db)))
        raise TypeError(message)
    if storage_mode not in ["file", "gridfs"]:
        raise TypeError(
            "write_distributed_data:  Unsupported storage_mode={}".format(storage_mode)
        )
    if mode not in ["promiscuous", "cautious", "pedantic"]:
        message = "write_distributed_data:  Illegal value of mode={}\n".format(mode)
        message += (
            "Must be one one of the following:  promiscuous, cautious, or pedantic"
        )
        raise ValueError(message)
    if scheduler:
        if scheduler not in ["dask", "spark"]:
            message = "write_distributed_data:  Illegal value of scheduler={}\n".format(
                scheduler
            )
            message += "Must be either dask or spark"
            raise ValueError(message)
    else:
        scheduler = "dask"
    # This use of the collection name to establish the schema is
    # a bit fragile as it depends upon the mspass schema naming
    # convention.  Once tried using take(1) and probing the content of
    # the container but that has bad memory consequences at least for pyspark
    if collection is None or collection == "wf_TimeSeries":
        save_schema = db.metadata_schema.TimeSeries
    elif collection == "wf_Seismogram":
        save_schema = db.metadata_schema.Seismogram
    else:
        message = "write_distributed_data:   Illegal value of collection={}\n".format(
            collection
        )
        message += "Currently must be either wf_TimeSeries, wf_Seismogram, or default that implies wf_TimeSeries"
        raise ValueError(message)

    if overwrite:
        if storage_mode != "gridfs":
            message = "write_distributed_data:  overwrite mode is set True with storage_mode={}\n".format(
                storage_mode
            )
            message += "overwrite is only allowed with gridfs storage_mode"
            raise ValueError(message)

    stedronsky = Undertaker(db)
    # Note we save sample data first.  That function will refuse to
    # save sample data for dead data and not leave junk.  However,
    # failures in conversion of Metadata attributes below can leave
    # orphan sample data in files or on gridfs.   The assumption is
    # that that kind of problem will be rare or so common you need to
    # clear out everthing and start over
    if scheduler == "spark":
        data = data.map(
            lambda d: db._save_sample_data(
                d,
                storage_mode=storage_mode,
                dir=None,
                dfile=None,
                format=file_format,
                overwrite=overwrite,
            )
        )
        if data_are_atomic:
            pyspark_interface = pyspark_mappartition_interface(db, collection)
            # With atomic data dead in this implementation we handle
            # any dead datum with the map operators that saves the
            # wf documents.   Dead data return a None instead of an id
            # by default and leave a body in the cemetery collection
            # unless cremate is set true
            data = data.map(
                lambda d: _atomic_extract_wf_document(
                    d,
                    db,
                    save_schema,
                    exclude_keys,
                    mode,
                    normalizing_collections,
                    post_elog=post_elog,
                    save_history=save_history,
                    post_history=post_history,
                    data_tag=data_tag,
                    undertaker=stedronsky,
                    cremate=cremate,
                )
            )
            data = data.mapPartitions(pyspark_interface.partitioned_save_wfdoc)
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
                data = data.map(
                    lambda d: stedronsky.mummify(d, post_elog=False, post_history=False)
                )
            data = data.map(
                lambda d: _save_ensemble_wfdocs(
                    d,
                    db,
                    save_schema,
                    exclude_keys,
                    mode,
                    stedronsky,
                    normalizing_collections,
                    cremate=cremate,
                    post_elog=post_elog,
                    save_history=save_history,
                    post_history=post_history,
                    data_tag=data_tag,
                )
            )
            data = data.collect()
    else:
        data = data.map(
            db._save_sample_data,
            storage_mode=storage_mode,
            dir=None,
            dfile=None,
            format=file_format,
            overwrite=overwrite,
        )

        if data_are_atomic:
            # See comment at top of spark section  - this code is exactly
            # the same by in the dask dialect
            data = data.map(
                _atomic_extract_wf_document,
                db,
                save_schema,
                exclude_keys,
                mode,
                normalizing_collections,
                post_elog=post_elog,
                save_history=save_history,
                post_history=post_history,
                data_tag=data_tag,
                undertaker=stedronsky,
                cremate=cremate,
            )
            data = data.map_partitions(
                _partitioned_save_wfdoc, db, collection=collection
            )
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
                data = data.map(stedronsky.mummify, post_elog=False, post_history=False)
            data = data.map(
                _save_ensemble_wfdocs,
                db,
                save_schema,
                exclude_keys,
                mode,
                stedronsky,
                normalizing_collections,
                cremate=cremate,
                post_elog=post_elog,
                save_history=save_history,
                post_history=post_history,
                data_tag=data_tag,
            )
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
    dbschema = db.database_schema
    mdschema = db.metadata_schema
    this_elog = ErrorLogger()
    md_list = list()
    for doc in cursor:
        # Use the databhase module function doc2md that standardizes
        # handling of schema constraints and exlcude_keys

        md, aok, elog = doc2md(
            doc,
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

    if elog.size() > 0:
        print(
            "WARNING(read_to_dataframe): ",
            elog.size(),
            " errors were handled during dataframe construction",
        )
        print(
            "All data associated with these errors were dropped.   Error log entries from doc2md follow"
        )
        errorlist = elog.get_error_log()
        for entry in errorlist:
            print(entry.algorithm, entry.message, entry.badness)
    # convert the metadata list to a dataframe
    return pd.json_normalize(map(lambda cur: cur.todict(), md_list))
