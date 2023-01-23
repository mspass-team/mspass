"""
Distributed Reader and Writer using DataFrame
"""
import os
import io
import copy
import pathlib
import pickle
import struct
import urllib.request
from array import array
import pandas as pd

import gridfs
import pymongo
import pymongo.errors
import numpy as np
import obspy
from obspy.clients.fdsn import Client
from obspy import Inventory
from obspy import UTCDateTime
import boto3, botocore
import json
import base64
import uuid

from mspasspy.db.database import Database
from mspasspy.ccore.io import _mseed_file_indexer, _fwrite_to_file, _fread_from_file
from mspasspy.util.converter import Trace2TimeSeries, Stream2Seismogram

from mspasspy.ccore.seismic import (
    TimeSeries,
    Seismogram,
    _CoreSeismogram,
    DoubleVector,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
)
from mspasspy.ccore.utility import (
    Metadata,
    MsPASSError,
    AtomicType,
    ErrorSeverity,
    dmatrix,
    ProcessingHistory,
)
from mspasspy.db.collection import Collection
from mspasspy.db.schema import DatabaseSchema, MetadataSchema
from mspasspy.util.converter import Textfile2Dataframe
import dask.bag as daskbag
import dask.dataframe as daskdf
import dask
import pyspark
from pyspark.sql import SQLContext


def read_distributed_data_df(
    df,
    db,
    cursor,
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
    This function should be used to read an entire dataset that is to be handled
    by subsequent parallel operations.  The function can be thought of as
    loading the entire data set into a parallel container (rdd for spark
    implementations or bag for a dask implementatio).  If a dataframe is given,
    it will read from files distributedly and then read from the database
    sequentially. The normal use of this function is to construct a query of
    the desired waveform collection with the MongoDB find operation.
    MongoDB find returns the cursor object that is the expected input.
    All other arguments are options that change behavior as described below.

    :param df: the dataframe from which the data are to be read.
    :type df: :class:`dask.bag.Bag` or :class:`pyspark.RDD`.
    :param db: the database from which the data are to be read.
    :type db: :class:`mspasspy.db.database.Database`.
    :param cursor: mongodb cursor defining what "the dataset" is.  It would
      normally be the output of the find method with a workflow dependent
      query.
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
    :return: container defining the parallel dataset.  A spark `RDD` if format
      is "Spark" and a dask 'bag' if format is "dask"
    """
    if df == None and db != None:
        collection = cursor.collection.name
        if format == "spark":
            list_ = spark_context.parallelize(cursor, numSlices=npartitions)
            return list_.map(
                lambda cur: db.read_data(
                    cur,
                    mode=mode,
                    normalize=normalize,
                    load_history=load_history,
                    exclude_keys=exclude_keys,
                    collection=collection,
                    data_tag=data_tag,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                )
            )
        elif format == "dask":
            list_ = daskbag.from_sequence(cursor, npartitions=npartitions)
            return list_.map(
                lambda cur: db.read_data(
                    cur,
                    mode=mode,
                    normalize=normalize,
                    load_history=load_history,
                    exclude_keys=exclude_keys,
                    collection=collection,
                    data_tag=data_tag,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                )
            )
        else:
            raise TypeError("Only spark and dask are supported")
    elif isinstance(df, pd.DataFrame):
        if format == "spark":
            df = SQLContext.createDataFrame(df).rdd
        else:
            df = daskdf.from_pandas(df).to_bag()
    return df.map(
        lambda cur: read_data_file(
            cur["mspass_object"],
            db,
            cur["object_doc"],
        )
    )


def read_data_db(
    db,  # db
    object_id,
    mode="promiscuous",
    normalize=None,
    load_history=False,
    exclude_keys=None,
    collection="wf",
    data_tag=None,
    alg_name="read_data",
    alg_id="0",
    define_as_raw=False,
    retrieve_history_record=False,
):
    """
    This is the MsPASS reader for constructing metadata of Seismogram or TimeSeries
    objects from data managed with MondoDB through MsPASS. It only construct the
    skeleton of object but doesn't read from the storage. Return type is a dict
    of mspass object and object_doc.

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
    try:
        wf_collection = db.database_schema.default_name(collection)
    except MsPASSError as err:
        raise MsPASSError(
            "collection {} is not defined in database schema".format(collection),
            "Invalid",
        ) from err
    object_type = db.database_schema[wf_collection].data_type()

    if object_type not in [TimeSeries, Seismogram]:
        raise MsPASSError(
            "only TimeSeries and Seismogram are supported, but {} is requested. Please check the data_type of {} collection.".format(
                object_type, wf_collection
            ),
            "Fatal",
        )

    if mode not in ["promiscuous", "cautious", "pedantic"]:
        raise MsPASSError(
            "only promiscuous, cautious and pedantic are supported, but {} is requested.".format(
                mode
            ),
            "Fatal",
        )

    if normalize is None:
        normalize = []
    if exclude_keys is None:
        exclude_keys = []

    # This assumes the name of a metadata schema matches the data type it defines.
    read_metadata_schema = db.metadata_schema[object_type.__name__]

    # We temporarily swap the main collection defined by the metadata schema by
    # the wf_collection. This ensures the method works consistently for any
    # user-specified collection argument.
    metadata_schema_collection = read_metadata_schema.collection("_id")
    if metadata_schema_collection != wf_collection:
        temp_metadata_schema = copy.deepcopy(db.metadata_schema)
        temp_metadata_schema[object_type.__name__].swap_collection(
            metadata_schema_collection, wf_collection, db.database_schema
        )
        read_metadata_schema = temp_metadata_schema[object_type.__name__]

    # find the corresponding document according to object id
    col = db[wf_collection]
    try:
        oid = object_id["_id"]
    except:
        oid = object_id
    object_doc = col.find_one({"_id": oid})
    if not object_doc:
        return None

    if data_tag:
        if "data_tag" not in object_doc or object_doc["data_tag"] != data_tag:
            return None

    # 1. build metadata as dict
    md = dict()

    # 1.1 read in the attributes from the document in the database
    for k in object_doc:
        if k in exclude_keys:
            continue
        if mode == "promiscuous":
            md[k] = object_doc[k]
            continue
        # FIXME: note that we do not check whether the attributes' type in the database matches the schema's definition.
        # This may or may not be correct. Should test in practice and get user feedbacks.
        if read_metadata_schema.is_defined(k) and not read_metadata_schema.is_alias(k):
            md[k] = object_doc[k]

    # 1.2 read the attributes in the metadata schema
    # col_dict is a hashmap used to store the normalized records by the normalized_id in object_doc
    col_dict = {}
    # log_error_msg is used to record all the elog entries generated during the reading process
    # After the mspass_object is created, we would post every elog entry with the messages in the log_error_msg.
    log_error_msg = []
    for k in read_metadata_schema.keys():
        col = read_metadata_schema.collection(k)
        # explanation of the 4 conditions in the following if statement
        # 1.2.1. col is not None and is a normalized collection name
        # 1.2.2. normalized key id exists in the wf document
        # 1.2.3. k is not one of the exclude keys
        # 1.2.4. col is in the normalize list provided by user
        if (
            col
            and col != wf_collection
            and col + "_id" in object_doc
            and k not in exclude_keys
            and col in normalize
        ):
            # try to find the corresponding record in the normalized collection from the database
            if col not in col_dict:
                col_dict[col] = db[col].find_one({"_id": object_doc[col + "_id"]})
            # might unable to find the normalized document by the normalized_id in the object_doc
            # we skip reading this attribute
            if not col_dict[col]:
                continue
            # this attribute may be missing in the normalized record we retrieve above
            # in this case, we skip reading this attribute
            # however, if it is a required attribute for the normalized collection
            # we should post an elog entry to the associated wf object created after.
            unique_k = db.database_schema[col].unique_name(k)
            if not unique_k in col_dict[col]:
                if db.database_schema[col].is_required(unique_k):
                    log_error_msg.append(
                        "Attribute {} is required in collection {}, but is missing in the document with id={}.".format(
                            unique_k, col, object_doc[col + "_id"]
                        )
                    )
                continue
            md[k] = col_dict[col][unique_k]

    # 1.3 schema check normalized data according to the read mode
    is_dead = False
    fatal_keys = []
    if mode == "cautious":
        for k in md:
            if read_metadata_schema.is_defined(k):
                col = read_metadata_schema.collection(k)
                unique_key = db.database_schema[col].unique_name(k)
                if not isinstance(md[k], read_metadata_schema.type(k)):
                    # try to convert the mismatch attribute
                    try:
                        # convert the attribute to the correct type
                        md[k] = read_metadata_schema.type(k)(md[k])
                    except:
                        if db.database_schema[col].is_required(unique_key):
                            fatal_keys.append(k)
                            is_dead = True
                            log_error_msg.append(
                                "cautious mode: Required attribute {} has type {}, forbidden by definition and unable to convert".format(
                                    k, type(md[k])
                                )
                            )

    elif mode == "pedantic":
        for k in md:
            if read_metadata_schema.is_defined(k):
                if not isinstance(md[k], read_metadata_schema.type(k)):
                    fatal_keys.append(k)
                    is_dead = True
                    log_error_msg.append(
                        "pedantic mode: {} has type {}, forbidden by definition".format(
                            k, type(md[k])
                        )
                    )

    # 1.4 create a mspass object by passing MetaData
    # if not changing the fatal key values, runtime error in construct a mspass object
    for k in fatal_keys:
        if read_metadata_schema.type(k) is str:
            md[k] = ""
        elif read_metadata_schema.type(k) is int:
            md[k] = 0
        elif read_metadata_schema.type(k) is float:
            md[k] = 0.0
        elif read_metadata_schema.type(k) is bool:
            md[k] = False
        elif read_metadata_schema.type(k) is dict:
            md[k] = {}
        elif read_metadata_schema.type(k) is list:
            md[k] = []
        elif read_metadata_schema.type(k) is bytes:
            md[k] = b"\x00"
        else:
            md[k] = None

    try:
        # Note a CRITICAL feature of the Metadata constructors
        # for both of these objects is that they allocate the
        # buffer for the sample data and initialize it to zero.
        # This allows sample data readers to load the buffer without
        # having to handle memory management.
        if object_type is TimeSeries:
            mspass_object = TimeSeries(md)
        else:
            # api mismatch here.  This ccore Seismogram constructor
            # had an ancestor that had an option to read data here.
            # we never do that here
            mspass_object = Seismogram(md, False)
    except MsPASSError as merr:
        # if the constructor fails mspass_object will be invalid
        # To preserve the error we have to create a shell to hold the error
        if object_type is TimeSeries:
            mspass_object = TimeSeries()
        else:
            mspass_object = Seismogram()
        # Default constructors leaves result marked dead so below should work
        mspass_object.elog.log_error(merr)
        r = dict()
        r["mspass_object"] = mspass_object
        r["object_doc"] = object_doc
        return r

    # not continue step 2 & 3 if the mspass object is dead
    if is_dead:
        mspass_object.kill()
        for msg in log_error_msg:
            mspass_object.elog.log_error("read_data", msg, ErrorSeverity.Invalid)
    else:
        mspass_object.live = True

        # 3.load history
        if load_history:
            history_obj_id_name = (
                db.database_schema.default_name("history_object") + "_id"
            )
            if history_obj_id_name in object_doc:
                db._load_history(
                    mspass_object,
                    object_doc[history_obj_id_name],
                    alg_name,
                    alg_id,
                    define_as_raw,
                    retrieve_history_record,
                )

        mspass_object.clear_modified()

        # 4.post complaint elog entries if any
        for msg in log_error_msg:
            mspass_object.elog.log_error("read_data", msg, ErrorSeverity.Complaint)

    r = dict()
    r["mspass_object"] = mspass_object
    r["object_doc"] = object_doc
    return r


def read_data_file(
    mspass_object,
    db,
    object_doc,
):
    """
    This is the reader for constructing the object from storage. Return type is a
    complete mspass object.

    :param db: the database from which the data are to be read.
    :type db: :class:`mspasspy.db.database.Database`.
    :param mspass_object: the object you want to read.
    :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
    :param object_doc: document of the object in the database
    :type object_doc: class:`dict`.
    """
    if mspass_object.live:
        # 2.load data from different modes
        storage_mode = object_doc["storage_mode"]
        if storage_mode == "file":
            if "format" in object_doc:
                Database._read_data_from_dfile(
                    mspass_object,
                    object_doc["dir"],
                    object_doc["dfile"],
                    object_doc["foff"],
                    nbytes=object_doc["nbytes"],
                    format=object_doc["format"],
                )
            else:
                Database._read_data_from_dfile(
                    mspass_object,
                    object_doc["dir"],
                    object_doc["dfile"],
                    object_doc["foff"],
                )
        elif storage_mode == "gridfs":
            db._read_data_from_gridfs(mspass_object, object_doc["gridfs_id"])
        elif storage_mode == "url":
            Database._read_data_from_url(
                mspass_object,
                object_doc["url"],
                format=None if "format" not in object_doc else object_doc["format"],
            )
        elif storage_mode == "s3_continuous":
            db._read_data_from_s3_continuous(mspass_object)
        elif storage_mode == "s3_lambda":
            db._read_data_from_s3_lambda(mspass_object)
        elif storage_mode == "fdsn":
            db._read_data_from_fdsn(mspass_object)
        else:  # add another parameter
            raise TypeError("Unknown storage mode: {}".format(storage_mode))

    return mspass_object


def write_distributed_data(
    db,
    df,
    mode="promiscuous",
    storage_mode="gridfs",
    spark_context=None,
    dir=None,
    dfile=None,
    format=None,
    overwrite=False,
    exclude_keys=None,
    collection=None,
    data_tag=None,
    alg_name="save_data",
    alg_id="0",
):
    """
    This function should be used to write an entire dataset that is to be handled
    by subsequent parallel operations.  The function can be thought of as
    writing the entire data set from a parallel container (rdd for spark
    implementations or bag for a dask implementatio).  From a dataframe,
    it will read from files distributedly and then read from the database
    sequentially.

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
    :param dir: file directory for storage.  This argument is ignored if
        storage_mode is set to "gridfs".  When storage_mode is "file" it
        sets the directory in a file system where the data should be saved.
        Note this can be an absolute or relative path.  If the path is
        relative it will be expanded with the standard python library
        path functions to a full path name for storage in the database
        document with the attribute "dir".  As for any io we remind the
        user that you much have write permission in this directory.
        The writer will also fail if this directory does not already
        exist.  i.e. we do not attempt to
    :type dir: :class:`str`
    :param dfile: file name for storage of waveform data.  As with dir
        this parameter is ignored if storage_mode is "gridfs" and is
        required only if storage_mode is "file".   Note that this file
        name does not have to be unique.  The writer always calls positions
        the write pointer to the end of the file referenced and sets the
        attribute "foff" to that position. That allows automatic appends to
        files without concerns about unique names.
    :type dfile: :class:`str`
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
    # 1. write to file system
    t = df.map(lambda cur: write_data_file(cur, dir, dfile, format))
    # 2. write to database
    if format == "spark":
        r = map(
            lambda x: write_data_db(
                db,
                x["mspass_object"],
                x["foff"],
                x["nbytes"],
                mode=mode,
                storage_mode=storage_mode,
                dir=dir,
                dfile=dfile,
                format=format,
                overwrite=overwrite,
                exclude_keys=exclude_keys,
                collection=collection,
                data_tag=data_tag,
                alg_name=alg_name,
                alg_id=alg_id,
            ),
            t.collect(),  # rdd -> list
        )
        return spark_context.parallelize(r)
    else:
        r = map(
            lambda x: write_data_db(
                db,
                x["mspass_object"],
                x["foff"],
                x["nbytes"],
                mode=mode,
                storage_mode=storage_mode,
                dir=dir,
                dfile=dfile,
                format=format,
                overwrite=overwrite,
                exclude_keys=exclude_keys,
                collection=collection,
                data_tag=data_tag,
                alg_name=alg_name,
                alg_id=alg_id,
            ),
            t.compute(),  # bag -> list
        )
        return daskbag.from_sequence(r)


def write_data_db(
    db,
    mspass_object,
    foff,
    nbytes,
    mode="promiscuous",
    storage_mode="gridfs",
    dir=None,
    dfile=None,
    format=None,
    overwrite=False,
    exclude_keys=None,
    collection=None,
    data_tag=None,
    alg_name="save_data",
    alg_id="0",
):
    """
    Use this method to save an atomic data object (TimeSeries or Seismogram)
    to be managed with MongoDB.  The Metadata are stored as documents in
    a MongoDB collection.  This method will not write data to file system,
    it only writes to the doc and to the database. Return type is a dict
    of mspass object and object_doc.

    Any errors messages held in the object being saved are always
    written to documents in MongoDB is a special collection defined in
    the schema.   Saving object level history is optional.

    There are multiple options described below.  One worth emphasizing is
    "data_tag".   Such a tag is essential for intermediate saves of
    a dataset if there is no other unique way to distinguish the
    data in is current state from data saved earlier.  For example,
    consider a job that did nothing but read waveform segments spanning
    a long time period (e.g. day files),cutting out a shorter time window,
    and then saving windowed data.  Crafting an unambiguous query to
    find only the windowed data in that situation could be challenging
    or impossible.  Hence, we recommend a data tag always be used for
    most saves.

    The mode parameter needs to be understood by all users of this
    function.  All modes enforce a schema constraint for "readonly"
    attributes.   An immutable (readonly) attribute by definition
    should not be changed during processing.   During a save
    all attributes with a key defined as readonly are tested
    with a method in the Metadata container that keeps track of
    any Metadata changes.  If a readonly attribute is found to
    have been changed it will be renamed with the prefix
    "READONLYERROR_", saved, and an error posted (e.g. if you try
    to alter site_lat (a readonly attribute) in a workflow when
    you save the waveform you will find an entry with the key
    READONERROR_site_lat.)   In the default 'promiscuous' mode
    all other attributes are blindly saved to the database as
    name value pairs with no safeties.  In 'cautious' mode we
    add a type check.  If the actual type of an attribute does not
    match what the schema expect, this method will try to fix the
    type error before saving the data.  If the conversion is
    successful it will be saved with a complaint error posted
    to elog.  If it fails, the attribute will not be saved, an
    additional error message will be posted, and the save
    algorithm continues.  In 'pedantic' mode, in contrast, all
    type errors are considered to invalidate the data.
    Similar error messages to that in 'cautious' mode are posted
    but any type errors will cause the datum passed as arg 0
    to be killed. The lesson is saves can leave entries that
    may need to be examined in elog and when really bad will
    cause the datum to be marked dead after the save.

    This method can throw an exception but only for errors in
    usage (i.e. arguments defined incorrectly)

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
    :param dir: file directory for storage.  This argument is ignored if
        storage_mode is set to "gridfs".  When storage_mode is "file" it
        sets the directory in a file system where the data should be saved.
        Note this can be an absolute or relative path.  If the path is
        relative it will be expanded with the standard python library
        path functions to a full path name for storage in the database
        document with the attribute "dir".  As for any io we remind the
        user that you much have write permission in this directory.
        The writer will also fail if this directory does not already
        exist.  i.e. we do not attempt to
    :type dir: :class:`str`
    :param dfile: file name for storage of waveform data.  As with dir
        this parameter is ignored if storage_mode is "gridfs" and is
        required only if storage_mode is "file".   Note that this file
        name does not have to be unique.  The writer always calls positions
        the write pointer to the end of the file referenced and sets the
        attribute "foff" to that position. That allows automatic appends to
        files without concerns about unique names.
    :type dfile: :class:`str`
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
    if not isinstance(mspass_object, (TimeSeries, Seismogram)):
        raise TypeError("only TimeSeries and Seismogram are supported")
    if storage_mode not in ["file", "gridfs"]:
        raise TypeError("Unknown storage mode: {}".format(storage_mode))
    if mode not in ["promiscuous", "cautious", "pedantic"]:
        raise MsPASSError(
            "only promiscuous, cautious and pedantic are supported, but {} is requested.".format(
                mode
            ),
            "Fatal",
        )
    # below we try to capture permission issue before writing anything to the database.
    # However, in the case that a storage is almost full, exceptions can still be
    # thrown, which could mess up the database record.
    if storage_mode == "file":
        if not dfile and not dir:
            # Note the following uses the dir and dfile defined in the data object.
            # It will ignore these two keys already in the collection in an update
            # transaction, and the dir and dfile in the collection will be replaced.
            if ("dir" not in mspass_object) or ("dfile" not in mspass_object):
                raise ValueError("dir or dfile is not specified in data object")
            dir = os.path.abspath(mspass_object["dir"])
            dfile = mspass_object["dfile"]
        if dir is None:
            dir = os.getcwd()
        else:
            dir = os.path.abspath(dir)
        if dfile is None:
            dfile = db._get_dfile_uuid(
                format
            )  #   If dfile name is not given, or defined in mspass_object, a new uuid will be generated
        fname = os.path.join(dir, dfile)
        if os.path.exists(fname):
            if not os.access(fname, os.W_OK):
                raise PermissionError(
                    "No write permission to the save file: {}".format(fname)
                )
        else:
            # the following loop finds the top level of existing parents to fname
            # and check for write permission to that directory.
            for path_item in pathlib.PurePath(fname).parents:
                if os.path.exists(path_item):
                    if not os.access(path_item, os.W_OK | os.X_OK):
                        raise PermissionError(
                            "No write permission to the save directory: {}".format(dir)
                        )
                    break

    schema = db.metadata_schema
    if isinstance(mspass_object, TimeSeries):
        save_schema = schema.TimeSeries
    else:
        save_schema = schema.Seismogram

    # should define wf_collection here because if the mspass_object is dead
    if collection:
        wf_collection_name = collection
    else:
        # This returns a string that is the collection name for this atomic data type
        # A weird construct
        wf_collection_name = save_schema.collection("_id")
    wf_collection = db[wf_collection_name]

    if mspass_object.live:
        if exclude_keys is None:
            exclude_keys = []

        # FIXME starttime will be automatically created in this function
        db._sync_metadata_before_update(mspass_object)

        # This method of Metadata returns a list of all
        # attributes that were changed after creation of the
        # object to which they are attached.
        changed_key_list = mspass_object.modified()

        copied_metadata = Metadata(mspass_object)

        # clear all the aliases
        # TODO  check for potential bug in handling clear_aliases
        # and modified method - i.e. keys returned by modified may be
        # aliases
        save_schema.clear_aliases(copied_metadata)

        # remove any values with only spaces
        for k in copied_metadata:
            if not str(copied_metadata[k]).strip():
                copied_metadata.erase(k)

        # remove any defined items in exclude list
        for k in exclude_keys:
            if k in copied_metadata:
                copied_metadata.erase(k)
        # the special mongodb key _id is currently set readonly in
        # the mspass schema.  It would be cleard in the following loop
        # but it is better to not depend on that external constraint.
        # The reason is the insert_one used below for wf collections
        # will silently update an existing record if the _id key
        # is present in the update record.  We want this method
        # to always save the current copy with a new id and so
        # we make sure we clear it
        if "_id" in copied_metadata:
            copied_metadata.erase("_id")
        # Now remove any readonly data
        for k in copied_metadata.keys():
            if save_schema.is_defined(k):
                if save_schema.readonly(k):
                    if k in changed_key_list:
                        newkey = "READONLYERROR_" + k
                        copied_metadata.change_key(k, newkey)
                        mspass_object.elog.log_error(
                            "Database.save_data",
                            "readonly attribute with key="
                            + k
                            + " was improperly modified.  Saved changed value with key="
                            + newkey,
                            ErrorSeverity.Complaint,
                        )
                    else:
                        copied_metadata.erase(k)
        # Done editing, now we convert copied_metadata to a python dict
        # using this Metadata method or the long version when in cautious or pedantic mode
        insertion_dict = dict()
        if mode == "promiscuous":
            # A python dictionary can use Metadata as a constructor due to
            # the way the bindings were defined
            insertion_dict = dict(copied_metadata)
        else:
            # Other modes have to test every key and type of value
            # before continuing.  pedantic kills data with any problems
            # Cautious tries to fix the problem first
            # Note many errors can be posted - one for each problem key-value pair
            for k in copied_metadata:
                if save_schema.is_defined(k):
                    if isinstance(copied_metadata[k], save_schema.type(k)):
                        insertion_dict[k] = copied_metadata[k]
                    else:
                        if mode == "pedantic":
                            mspass_object.kill()
                            message = "pedantic mode error:  key=" + k
                            value = copied_metadata[k]
                            message += (
                                " type of stored value="
                                + str(type(value))
                                + " does not match schema expectation="
                                + str(save_schema.type(k))
                            )
                            mspass_object.elog.log_error(
                                "Database.save_data",
                                "message",
                                ErrorSeverity.Invalid,
                            )
                        else:
                            # Careful if another mode is added here.  else means cautious in this logic
                            try:
                                # The following convert the actual value in a dict to a required type.
                                # This is because the return of type() is the class reference.
                                insertion_dict[k] = save_schema.type(k)(
                                    copied_metadata[k]
                                )
                            except Exception as err:
                                #  cannot convert required keys -> kill the object
                                if save_schema.is_required(k):
                                    mspass_object.kill()
                                    message = "cautious mode error:  key=" + k
                                    message += (
                                        " Required key value could not be converted to required type="
                                        + str(save_schema.type(k))
                                        + " actual type="
                                        + str(type(copied_metadata[k]))
                                    )
                                    message += (
                                        "\nPython error exception message caught:\n"
                                    )
                                    message += str(err)
                                    mspass_object.elog.log_error(
                                        "Database.save",
                                        message,
                                        ErrorSeverity.Invalid,
                                    )
                                # cannot convert normal keys -> erase the key
                                # TODO should we post a Complaint entry to the elog?
                                else:
                                    copied_metadata.erase(k)

    # Note we jump here immediately if mspass_object was marked dead
    # on entry.  Data can, however, be killed in metadata section
    # above so we need repeat the test for live
    if mspass_object.live:
        insertion_dict["storage_mode"] = storage_mode
        gridfs_id = None

        if storage_mode == "file":
            # TODO:  be sure this can't throw an exception
            """
            foff, nbytes = self._save_data_to_dfile(
                mspass_object, dir, dfile, format=format
            )
            """
            insertion_dict["dir"] = dir
            insertion_dict["dfile"] = dfile
            insertion_dict["foff"] = foff
            if format:
                insertion_dict["nbytes"] = nbytes
                insertion_dict["format"] = format
        elif storage_mode == "gridfs":
            if overwrite and "gridfs_id" in insertion_dict:
                gridfs_id = db._save_data_to_gridfs(
                    mspass_object, insertion_dict["gridfs_id"]
                )
            else:
                gridfs_id = db._save_data_to_gridfs(mspass_object)
            insertion_dict["gridfs_id"] = gridfs_id
            # TODO will support url mode later
            # elif storage_mode == "url":
            #    pass

        # save history if not empty
        history_obj_id_name = db.database_schema.default_name("history_object") + "_id"
        history_object_id = None
        if mspass_object.is_empty():
            # Use this trick in update_metadata too. None is needed to
            # avoid a TypeError exception if the name is not defined.
            # could do this with a conditional as an alternative
            insertion_dict.pop(history_obj_id_name, None)
        else:
            # optional history save - only done if history container is not empty
            history_object_id = db._save_history(mspass_object, alg_name, alg_id)
            insertion_dict[history_obj_id_name] = history_object_id

        # add tag
        if data_tag:
            insertion_dict["data_tag"] = data_tag
        else:
            # We need to clear data tag if was previously defined in
            # this case or a the old tag will be saved with this datum
            if "data_tag" in insertion_dict:
                insertion_dict.erase("data_tag")
        # We don't want an elog_id in the insertion at this point.
        # A option to consider is if we need an update after _save_elog
        # section below to post elog_id back.

        # test will fail here because there might be some Complaint elog post to the wf above
        # we need to save the elog and get the elog_id
        # then associate with the wf document so that we could insert in the wf_collection

        # save elogs if the size of elog is greater than 0
        elog_id = None
        if mspass_object.elog.size() > 0:
            elog_id_name = db.database_schema.default_name("elog") + "_id"
            # elog ids will be updated in the wf col when saving metadata
            elog_id = db._save_elog(mspass_object, elog_id=None)
            insertion_dict[elog_id_name] = elog_id

        # finally ready to insert the wf doc - keep the id as we'll need
        # it for tagging any elog entries
        wfid = wf_collection.insert_one(insertion_dict).inserted_id
        # Put wfid into the object's meta as the new definition of
        # the parent of this waveform
        mspass_object["_id"] = wfid

        # we may probably set the gridfs_id field in the mspass_object
        if gridfs_id:
            mspass_object["gridfs_id"] = gridfs_id
        # we may probably set the history_object_id field in the mspass_object
        if history_object_id:
            mspass_object[history_obj_id_name] = history_object_id
        # we may probably set the elog_id field in the mspass_object
        if elog_id:
            mspass_object[elog_id_name] = elog_id

        # Empty error logs are skipped.  When nonzero tag them with tid
        # just returned
        if mspass_object.elog.size() > 0:
            # elog_id_name = self.database_schema.default_name('elog') + '_id'
            # _save_elog uses a  null id as a signal to add a new record
            # When we land here the record must be new since it is
            # associated with a new wf document.  elog_id=None is default
            # but set it explicitly for clarity

            # This is comment out becuase we need to save it before inserting into the wf_collection
            # elog_id = self._save_elog(mspass_object, elog_id=None)

            # cross reference for elog entry, assoicate the wfid to the elog entry
            elog_col = db[db.database_schema.default_name("elog")]
            wf_id_name = wf_collection_name + "_id"
            filter_ = {"_id": insertion_dict[elog_id_name]}
            elog_col.update_one(filter_, {"$set": {wf_id_name: mspass_object["_id"]}})
        # When history is enable we need to do an update to put the
        # wf collection id as a cross-reference.    Any value stored
        # above with saave_history may be incorrect.  We use a
        # stock test with the is_empty method for know if history data is present
        if not mspass_object.is_empty():
            history_object_col = db[db.database_schema.default_name("history_object")]
            wf_id_name = wf_collection_name + "_id"
            filter_ = {"_id": history_object_id}
            update_dict = {wf_id_name: wfid}
            history_object_col.update_one(filter_, {"$set": update_dict})

    else:
        # We land here when the input is dead or was killed during a
        # cautious or pedantic mode edit of the metadata.
        elog_id_name = db.database_schema.default_name("elog") + "_id"
        if elog_id_name in mspass_object:
            old_elog_id = mspass_object[elog_id_name]
        else:
            old_elog_id = None
        elog_id = db._save_elog(mspass_object, elog_id=old_elog_id)
    # Both live and dead data land here.
    r = dict()
    r["mspass_object"] = mspass_object
    r["object_doc"] = insertion_dict
    return r


def write_data_file(
    mspass_object,
    dir,
    dfile,
    format,
):
    """
    This is the writer for writing the object to storage. Return type is a
    dataframe, rdd for spark implementations or bag for a dask implementatio.

    :param mspass_object: the object you want to read.
    :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
    :param object_doc: document of the object in the database
    :type object_doc: class:`dict`.
    :type dir: :class:`str`
    :param dfile: file name for storage of waveform data.  As with dir
        this parameter is ignored if storage_mode is "gridfs" and is
        required only if storage_mode is "file".   Note that this file
        name does not have to be unique.  The writer always calls positions
        the write pointer to the end of the file referenced and sets the
        attribute "foff" to that position. That allows automatic appends to
        files without concerns about unique names.
    :type dfile: :class:`str`
    :param format: the format of the file. This can be one of the
        `supported formats <https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.write.html#supported-formats>`__
        of ObsPy writer. The default the python None which the method
        assumes means to store the data in its raw binary form.  The default
        should normally be used for efficiency.  Alternate formats are
        primarily a simple export mechanism.  See the User's manual for
        more details on data export.  Used only for "file" storage mode.
    :type format: :class:`str`

    """

    foff, nbytes = Database._save_data_to_dfile(
        mspass_object, dir, dfile, format=format
    )
    r = dict()
    r["foff"] = foff
    r["nbytes"] = nbytes
    r["mspass_object"] = mspass_object
    return r
