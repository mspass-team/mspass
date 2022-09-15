"""
Tools for connecting to MongoDB.
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
import dask.bag as daskbag
import dask.dataframe as daskdf
import gridfs
import pymongo
import numpy as np
import obspy
from obspy.clients.fdsn import Client
from obspy import Inventory
from obspy import UTCDateTime
import boto3, botocore
import json
import base64
import uuid

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
from mspasspy.db.schema import DatabaseSchema, MetadataSchema
from mspasspy.util.converter import Textfile2Dataframe


def read_distributed_data(
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
    implementations or bag for a dask implementatio).   It doesn't really do
    that to be scalable but conceptually the container that is the output of
    this method is a handle to the entire data set defined by the input
    cursor.   The normal use of this function is to construct a query of
    the desired waveform collection with the MongoDB find operation.
    MongoDB find returns the cursor object that is the expected input.
    All other arguments are options that change behavior as described below.

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


class Database(pymongo.database.Database):
    """
    MsPASS core database handle.  All MongoDB database operation in MsPASS
    should utilize this object.  This object is a subclass of the
    Database class of pymongo.  It extends the class in several ways
    fundamental to the MsPASS framework:

    1. It abstracts read and write operations for seismic data managed
       by the framework.   Note reads and writes are for atomic objects.
       Use the distributed read and write functions for parallel handling of
       complete data sets.
    2. It contains methods for managing the most common seismic Metadata
       (namely source and receiver geometry and receiver response data).
    3. It adds a schema that can (optionally) be used to enforce type
       requirements and/or provide aliasing.
    4. Manages error logging data.
    5. Manages (optional) processing history data

    The class currently has only one constructor normally called with
    a variant of the following:
      db=Database(dbclient,'mydatabase')
    where dbclient is either a MongoDB database client instance or
    (recommended) the MsPASS DBClient wrapper (a subclass of the
    pymongo client).  The second argument is the database "name"
    passed to the MongoDB server that defines your working database.
    Optional parameters are:

    :param db_schema: Set the name for the database schema to use with this
      handle.  Default is the MsPASS schema. (See User's Manual for details)
    :param md_schema:  Set the name for the Metadata schema.   Default is
      the MsPASS definitions.  (see User's Manual for details)

    As a subclass of pymongo Database the constructor accepts any
    parameters defined for the base class (see pymongo documentation)
    """

    def __init__(self, *args, db_schema=None, md_schema=None, **kwargs):
        super(Database, self).__init__(*args, **kwargs)
        if isinstance(db_schema, DatabaseSchema):
            self.database_schema = db_schema
        elif isinstance(db_schema, str):
            self.database_schema = DatabaseSchema(db_schema)
        else:
            self.database_schema = DatabaseSchema()

        if isinstance(md_schema, MetadataSchema):
            self.metadata_schema = md_schema
        elif isinstance(md_schema, str):
            self.metadata_schema = MetadataSchema(md_schema)
        else:
            self.metadata_schema = MetadataSchema()

    def __getstate__(self):
        ret = self.__dict__.copy()
        ret["_Database__client"] = self.client.__repr__()
        return ret

    def __setstate__(self, data):
        # somewhat weird that this import is requiired here but it won't
        # work without it.  Not sure how the symbol MongoClient is required
        # here but it is - ignore if a lint like ide says MongoClient is not used
        from pymongo import MongoClient

        data["_Database__client"] = eval(data["_Database__client"])
        self.__dict__.update(data)

    def set_metadata_schema(self, schema):
        """
        Use this method to change the metadata schema defined for this
        instance of a database handle.  This method sets the metadata
        schema (interal namespace).  Use set_database_schema to change
        the stored data schema.

        :param schema: an instance of :class:`mspsspy.db.schema.MetadataSchema.
          WARNING this is not a name - is a MsPASS object consructed from a name.
        """
        self.metadata_schema = schema

    def set_database_schema(self, schema):
        """
        Use this method to change the database schema defined for this
        instance of a database handle.  This method sets the database
        schema (namespace for attributes saved in MongoDB).  Use metadata_schema
        to change the in memory namespace.

        :param schema: an instance of :class:`mspsspy.db.schema.DatabaseSchema.
          WARNING this is not a name - is a MsPASS object consructed from  aname.
        """
        self.database_schema = schema

    def read_data(
        self,
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
        merge_method=0,
        merge_fill_value=None,
        merge_interpolation_samples=0,
        aws_access_key_id=None,
        aws_secret_access_key=None,
    ):
        """
        This is the core MsPASS reader for constructing Seismogram or TimeSeries
        objects from data managed with MondoDB through MsPASS.   It is the
        core reader for serial processing where a typical algorithm would be:

          query= { ... properly constructed MondoDB query or just '{}'}
          cursor=db.collection.find(query)  # collection is wf_TimeSeries or wf_Seismogram
          for doc in cursor:
            id=doc['_id']
            d=db.read_data(id)
              ...   # additional processing here

        The above loop will construct one Seismogram or TimeSeries (depending on
        which collection is referenced) for handling within the for loop.
        Use the read_distributed_data function to read a dataset for
        parallel processing.  This function is designed to handle one atomic
        object at a time.  It is, in fact, called in read_distributed_data
        Optional parameters control read behavior and additional options
        not always needed.  An important one for handling reads from a
        a dataset saved partway through a workflow is data_tag.

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
        :param method: Methodology to handle overlaps/gaps of traces. Defaults to 0.
            See `__add__ <https://docs.obspy.org/packages/autogen/obspy.core.trace.Trace.__add__.html#obspy.core.trace.Trace.__add__>` for details on methods 0 and 1,
            see `_cleanup <https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream._cleanup.html#obspy.core.stream.Stream._cleanup>` for details on method -1.
            Any merge operation performs a cleanup merge as a first step (method -1).
        :type method: :class:`int`
        :param fill_value: Fill value for gaps. Defaults to None.
        :type fill_value: :class:`int`, :class:`float` or None
        :param interpolation_samples: Used only for method=1. It specifies the number of samples
            which are used to interpolate between overlapping traces. Default to 0.
            If set to -1 all overlapping samples are interpolated.
        :type interpolation_samples: :class:`int`
        :return: either :class:`mspasspy.ccore.seismic.TimeSeries`
          or :class:`mspasspy.ccore.seismic.Seismogram`
        """
        try:
            wf_collection = self.database_schema.default_name(collection)
        except MsPASSError as err:
            raise MsPASSError(
                "collection {} is not defined in database schema".format(collection),
                "Invalid",
            ) from err
        object_type = self.database_schema[wf_collection].data_type()

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
        read_metadata_schema = self.metadata_schema[object_type.__name__]

        # We temporarily swap the main collection defined by the metadata schema by
        # the wf_collection. This ensures the method works consistently for any
        # user-specified collection argument.
        metadata_schema_collection = read_metadata_schema.collection("_id")
        if metadata_schema_collection != wf_collection:
            temp_metadata_schema = copy.deepcopy(self.metadata_schema)
            temp_metadata_schema[object_type.__name__].swap_collection(
                metadata_schema_collection, wf_collection, self.database_schema
            )
            read_metadata_schema = temp_metadata_schema[object_type.__name__]

        # find the corresponding document according to object id
        col = self[wf_collection]
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
        md = Metadata()

        # 1.1 read in the attributes from the document in the database
        for k in object_doc:
            if k in exclude_keys:
                continue
            if mode == "promiscuous":
                md[k] = object_doc[k]
                continue
            # FIXME: note that we do not check whether the attributes' type in the database matches the schema's definition.
            # This may or may not be correct. Should test in practice and get user feedbacks.
            if read_metadata_schema.is_defined(k) and not read_metadata_schema.is_alias(
                k
            ):
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
                    col_dict[col] = self[col].find_one({"_id": object_doc[col + "_id"]})
                # might unable to find the normalized document by the normalized_id in the object_doc
                # we skip reading this attribute
                if not col_dict[col]:
                    continue
                # this attribute may be missing in the normalized record we retrieve above
                # in this case, we skip reading this attribute
                # however, if it is a required attribute for the normalized collection
                # we should post an elog entry to the associated wf object created after.
                unique_k = self.database_schema[col].unique_name(k)
                if not unique_k in col_dict[col]:
                    if self.database_schema[col].is_required(unique_k):
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
                    unique_key = self.database_schema[col].unique_name(k)
                    if not isinstance(md[k], read_metadata_schema.type(k)):
                        # try to convert the mismatch attribute
                        try:
                            # convert the attribute to the correct type
                            md[k] = read_metadata_schema.type(k)(md[k])
                        except:
                            if self.database_schema[col].is_required(unique_key):
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
            return mspass_object

        # not continue step 2 & 3 if the mspass object is dead
        if is_dead:
            mspass_object.kill()
            for msg in log_error_msg:
                mspass_object.elog.log_error("read_data", msg, ErrorSeverity.Invalid)
        else:
            # 2.load data from different modes
            mspass_object.live = True
            storage_mode = object_doc["storage_mode"]
            if storage_mode == "file":
                if "format" in object_doc:
                    self._read_data_from_dfile(
                        mspass_object,
                        object_doc["dir"],
                        object_doc["dfile"],
                        object_doc["foff"],
                        nbytes=object_doc["nbytes"],
                        format=object_doc["format"],
                        merge_method=merge_method,
                        merge_fill_value=merge_fill_value,
                        merge_interpolation_samples=merge_interpolation_samples,
                    )
                else:
                    self._read_data_from_dfile(
                        mspass_object,
                        object_doc["dir"],
                        object_doc["dfile"],
                        object_doc["foff"],
                        merge_method=merge_method,
                        merge_fill_value=merge_fill_value,
                        merge_interpolation_samples=merge_interpolation_samples,
                    )
            elif storage_mode == "gridfs":
                self._read_data_from_gridfs(mspass_object, object_doc["gridfs_id"])
            elif storage_mode == "url":
                self._read_data_from_url(
                    mspass_object,
                    object_doc["url"],
                    format=None if "format" not in object_doc else object_doc["format"],
                )
            elif storage_mode == "s3_continuous":
                self._read_data_from_s3_continuous(
                    mspass_object, aws_access_key_id, aws_secret_access_key
                )
            elif storage_mode == "s3_lambda":
                self._read_data_from_s3_lambda(
                    mspass_object, aws_access_key_id, aws_secret_access_key
                )
            elif storage_mode == "fdsn":
                self._read_data_from_fdsn(mspass_object)
            else:
                raise TypeError("Unknown storage mode: {}".format(storage_mode))

            # 3.load history
            if load_history:
                history_obj_id_name = (
                    self.database_schema.default_name("history_object") + "_id"
                )
                if history_obj_id_name in object_doc:
                    self._load_history(
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

        return mspass_object

    def save_data(
        self,
        mspass_object,
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
        a MongoDB collection.  The waveform data can be stored in a conventional
        file system or in MongoDB's gridfs system.   At the time this docstring
        was written testing was still in progress to establish the relative
        performance of file system io versus gridfs, but our working hypothesis
        is the answer of which is faster will be configuration dependent. In
        either case the goal of this function is to make a save operation as
        simple as possible by abstracting the complications involved in
        the actual save.

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
        :return: Data object as saved (if killed it will be dead)
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
                dfile = self._get_dfile_uuid(
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
                                "No write permission to the save directory: {}".format(
                                    dir
                                )
                            )
                        break

        schema = self.metadata_schema
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
        wf_collection = self[wf_collection_name]

        if mspass_object.live:
            if exclude_keys is None:
                exclude_keys = []

            # FIXME starttime will be automatically created in this function
            self._sync_metadata_before_update(mspass_object)

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
                foff, nbytes = self._save_data_to_dfile(
                    mspass_object, dir, dfile, format=format
                )
                insertion_dict["dir"] = dir
                insertion_dict["dfile"] = dfile
                insertion_dict["foff"] = foff
                if format:
                    insertion_dict["nbytes"] = nbytes
                    insertion_dict["format"] = format
            elif storage_mode == "gridfs":
                if overwrite and "gridfs_id" in insertion_dict:
                    gridfs_id = self._save_data_to_gridfs(
                        mspass_object, insertion_dict["gridfs_id"]
                    )
                else:
                    gridfs_id = self._save_data_to_gridfs(mspass_object)
                insertion_dict["gridfs_id"] = gridfs_id
                # TODO will support url mode later
                # elif storage_mode == "url":
                #    pass

            # save history if not empty
            history_obj_id_name = (
                self.database_schema.default_name("history_object") + "_id"
            )
            history_object_id = None
            if mspass_object.is_empty():
                # Use this trick in update_metadata too. None is needed to
                # avoid a TypeError exception if the name is not defined.
                # could do this with a conditional as an alternative
                insertion_dict.pop(history_obj_id_name, None)
            else:
                # optional history save - only done if history container is not empty
                history_object_id = self._save_history(mspass_object, alg_name, alg_id)
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
                elog_id_name = self.database_schema.default_name("elog") + "_id"
                # elog ids will be updated in the wf col when saving metadata
                elog_id = self._save_elog(mspass_object, elog_id=None)
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
                elog_col = self[self.database_schema.default_name("elog")]
                wf_id_name = wf_collection_name + "_id"
                filter_ = {"_id": insertion_dict[elog_id_name]}
                elog_col.update_one(
                    filter_, {"$set": {wf_id_name: mspass_object["_id"]}}
                )
            # When history is enable we need to do an update to put the
            # wf collection id as a cross-reference.    Any value stored
            # above with saave_history may be incorrect.  We use a
            # stock test with the is_empty method for know if history data is present
            if not mspass_object.is_empty():
                history_object_col = self[
                    self.database_schema.default_name("history_object")
                ]
                wf_id_name = wf_collection_name + "_id"
                filter_ = {"_id": history_object_id}
                update_dict = {wf_id_name: wfid}
                history_object_col.update_one(filter_, {"$set": update_dict})

        else:
            # We land here when the input is dead or was killed during a
            # cautious or pedantic mode edit of the metadata.
            elog_id_name = self.database_schema.default_name("elog") + "_id"
            if elog_id_name in mspass_object:
                old_elog_id = mspass_object[elog_id_name]
            else:
                old_elog_id = None
            elog_id = self._save_elog(mspass_object, elog_id=old_elog_id)
        # Both live and dead data land here.
        return mspass_object

    # clean the collection fixing any type errors and removing any aliases using the schema currently defined for self

    def clean_collection(
        self,
        collection="wf",
        query=None,
        rename_undefined=None,
        delete_undefined=False,
        required_xref_list=None,
        delete_missing_xref=False,
        delete_missing_required=False,
        verbose=False,
        verbose_keys=None,
    ):
        """
        This method can be used to fix a subset of common database problems
        created during processing that can cause problems if the user tries to
        read back data saved with such blemishes.   The subset of problems
        are defined as those that are identified by the "dbverify" command
        line tool or it's Database equivalent (the verify method of this class).
        This method is an alternative to the command line tool dbverify.  Use
        this method for more surgical fixes that require a query to limit
        the application of the fix.

        :param collection: the collection you would like to clean. If not specified, use the default wf collection
        :type collection: :class:`str`
        :param query: this is a python dict that is assumed to be a query
         to MongoDB to limit suite of documents to which the requested cleanup
         methods are applied.  The default will process the entire database
         collection specified.
        :type query: :class:`dict`
        :param rename_undefined: when set the options is assumed to contain
          a python dict defining how to rename a set of attributes. Each member
          of the dict is assumed to be of the form ``{original_key:new_key}``
          Each document handled will change the key from "original_key" to
          "new_key".
        :type rename_undefined: :class:`dict`
        :param delete_undefined: attributes undefined in the schema can
          be problematic.  As a minimum they waste storage and memory if they
          are baggage.  At worst they may cause a downstream process to abort
          or mark some or all data dead.  On the other hand, undefined data may
          contain important information you need that for some reason is
          not defined in the schema.  In that case do not run this method to
          clear these.  When set true all attributes matching the query will
          have undefined attributes cleared.
        :param required_xref_list: in MsPASS we use "normalization" of some common
          attributes for efficiency.  The stock ones currently are "source", "site",
          and "channel".  This list is a intimately linked to the
          delete_missing_xref option.  When that is true this list is
          enforced to clean debris.  Typical examples are ['site_id','source_id']
          to require source and receiver geometry.
        :type required_xref_list: :class:`list`
        :param delete_missing_xref: Turn this option on to impose a brutal
          fix for data with missing (required) cross referencing data.  This
          clean operation, for example, might be necessary to clean up a
          data set with some channels that defy all attempts to find valid
          receiver metadata (stationxml stuff for passive data, survey data for
          active source data).  This clean method is a blunt instrument that
          should be used as a last resort.  When true the is list of xref
          keys defined by required_xref_list are tested any document that lacks
          of the keys will be deleted from the database.
        :param delete_missing_required: Set to ``True`` to delete this document if any required keys in the database schema is missing. Default is ``False``.
        :param verbose: Set to ``True`` to print all the operations. Default is ``False``.
        :param verbose_keys: a list of keys you want to added to better identify problems when error happens. It's used in the print messages.
        :type verbose_keys: :class:`list` of :class:`str`
        """
        if query is None:
            query = {}
        if verbose_keys is None:
            verbose_keys = []
        if required_xref_list is None:
            required_xref_list = []
        if rename_undefined is None:
            rename_undefined = {}

        fixed_cnt = {}
        # fix the queried documents in the collection
        col = self[self.database_schema.default_name(collection)]
        matchsize = col.count_documents(query)
        # no match documents return
        if matchsize == 0:
            return fixed_cnt
        else:
            docs = col.find(query)
            for doc in docs:
                if "_id" in doc:
                    fixed_attr_cnt = self.clean(
                        doc["_id"],
                        collection,
                        rename_undefined,
                        delete_undefined,
                        required_xref_list,
                        delete_missing_xref,
                        delete_missing_required,
                        verbose,
                        verbose_keys,
                    )
                    for k, v in fixed_attr_cnt.items():
                        if k not in fixed_cnt:
                            fixed_cnt[k] = 1
                        else:
                            fixed_cnt[k] += v

        return fixed_cnt

    # clean a single document in the given collection atomically
    def clean(
        self,
        document_id,
        collection="wf",
        rename_undefined=None,
        delete_undefined=False,
        required_xref_list=None,
        delete_missing_xref=False,
        delete_missing_required=False,
        verbose=False,
        verbose_keys=None,
    ):
        """
        This is the atomic level operation for cleaning a single database
        document with one or more common fixes.  The clean_collection method
        is mainly a loop that calls this method for each document it is asked to
        handle.  Most users will likely not need or want to call this method
        directly but use the clean_collection method instead.  See the docstring
        for clean_collection for a less cyptic description of the options as
        the options are identical.

        :param document_id: the value of the _id field in the document you want to clean
        :type document_id: :class:`bson.objectid.ObjectId`
        :param collection: the name of collection saving the document. If not specified, use the default wf collection
        :param rename_undefined: Specify a :class:`dict` of ``{original_key:new_key}`` to rename the undefined keys in the document.
        :type rename_undefined: :class:`dict`
        :param delete_undefined: Set to ``True`` to delete undefined keys in the doc. ``rename_undefined`` will not work if this is ``True``. Default is ``False``.
        :param required_xref_list: a :class:`list` of xref keys to be checked.
        :type required_xref_list: :class:`list`
        :param delete_missing_xref: Set to ``True`` to delete this document if any keys specified in ``required_xref_list`` is missing. Default is ``False``.
        :param delete_missing_required: Set to ``True`` to delete this document if any required keys in the database schema is missing. Default is ``False``.
        :param verbose: Set to ``True`` to print all the operations. Default is ``False``.
        :param verbose_keys: a list of keys you want to added to better identify problems when error happens. It's used in the print messages.
        :type verbose_keys: :class:`list` of :class:`str`

        :return: number of fixes applied to each key
        :rtype: :class:`dict`
        """
        if verbose_keys is None:
            verbose_keys = []
        if required_xref_list is None:
            required_xref_list = []
        if rename_undefined is None:
            rename_undefined = {}

        # validate parameters
        if type(verbose_keys) is not list:
            raise MsPASSError(
                "verbose_keys should be a list , but {} is requested.".format(
                    str(type(verbose_keys))
                ),
                "Fatal",
            )
        if type(rename_undefined) is not dict:
            raise MsPASSError(
                "rename_undefined should be a dict , but {} is requested.".format(
                    str(type(rename_undefined))
                ),
                "Fatal",
            )
        if type(required_xref_list) is not list:
            raise MsPASSError(
                "required_xref_list should be a list , but {} is requested.".format(
                    str(type(required_xref_list))
                ),
                "Fatal",
            )

        print_messages = []
        fixed_cnt = {}

        # if the document does not exist in the db collection, return
        collection = self.database_schema.default_name(collection)
        object_type = self.database_schema[collection].data_type()
        col = self[collection]
        doc = col.find_one({"_id": document_id})
        if not doc:
            if verbose:
                print(
                    "collection {} document _id: {}, is not found".format(
                        collection, document_id
                    )
                )
            return fixed_cnt

        # access each key
        log_id_dict = {}
        # get all the values of the verbose_keys
        for k in doc:
            if k in verbose_keys:
                log_id_dict[k] = doc[k]
        log_helper = "collection {} document _id: {}, ".format(collection, doc["_id"])
        for k, v in log_id_dict.items():
            log_helper += "{}: {}, ".format(k, v)

        # 1. check if the document has all the required fields
        missing_required_attr_list = []
        for k in self.database_schema[collection].keys():
            if self.database_schema[collection].is_required(k):
                keys_for_checking = []
                if self.database_schema[collection].has_alias(k):
                    # get the aliases list of the key
                    keys_for_checking = self.database_schema[collection].aliases(k)
                keys_for_checking.append(k)
                # check if any key appear in the doc
                key_in_doc = False
                for key in keys_for_checking:
                    if key in doc:
                        key_in_doc = True
                if not key_in_doc:
                    missing_required_attr_list.append(k)
        if missing_required_attr_list:
            error_msg = "required attribute: "
            for missing_attr in missing_required_attr_list:
                error_msg += "{} ".format(missing_attr)
            error_msg += "are missing."

            # delete this document
            if delete_missing_required:
                self.delete_data(doc["_id"], object_type.__name__, True, True, True)
                if verbose:
                    print("{}{} the document is deleted.".format(log_helper, error_msg))
                return fixed_cnt
            else:
                print_messages.append("{}{}".format(log_helper, error_msg))

        # 2. check if the document has all xref keys in the required_xref_list list provided by user
        missing_xref_key_list = []
        for xref_k in required_xref_list:
            # xref_k in required_xref_list list should be defined in schema first
            if self.database_schema[collection].is_defined(xref_k):
                unique_xref_k = self.database_schema[collection].unique_name(xref_k)
                # xref_k should be a reference key as well
                if self.database_schema[collection].is_xref_key(unique_xref_k):
                    keys_for_checking = []
                    if self.database_schema[collection].has_alias(unique_xref_k):
                        # get the aliases list of the key
                        keys_for_checking = self.database_schema[collection].aliases(
                            unique_xref_k
                        )
                    keys_for_checking.append(unique_xref_k)
                    # check if any key appear in the doc
                    key_in_doc = False
                    for key in keys_for_checking:
                        if key in doc:
                            key_in_doc = True
                    if not key_in_doc:
                        missing_xref_key_list.append(unique_xref_k)
        # missing required xref keys, should be deleted
        if missing_xref_key_list:
            error_msg = "required xref keys: "
            for missing_key in missing_xref_key_list:
                error_msg += "{} ".format(missing_key)
            error_msg += "are missing."

            # delete this document
            if delete_missing_xref:
                self.delete_data(doc["_id"], object_type.__name__, True, True, True)
                if verbose:
                    print("{}{} the document is deleted.".format(log_helper, error_msg))
                return fixed_cnt
            else:
                print_messages.append("{}{}".format(log_helper, error_msg))

        # 3. try to fix the mismtach errors in the doc
        update_dict = {}
        for k in doc:
            if k == "_id":
                continue
            # if not the schema keys, ignore schema type check enforcement
            if not self.database_schema[collection].is_defined(k):
                # delete undefined attributes in the doc if delete_undefined is True
                if not delete_undefined:
                    # try to rename the user specified keys
                    if k in rename_undefined:
                        update_dict[rename_undefined[k]] = doc[k]
                    else:
                        update_dict[k] = doc[k]
                continue
            # to remove aliases, get the unique key name defined in the schema
            unique_k = self.database_schema[collection].unique_name(k)
            if not isinstance(doc[k], self.database_schema[collection].type(unique_k)):
                try:
                    update_dict[unique_k] = self.database_schema[collection].type(
                        unique_k
                    )(doc[k])
                    print_messages.append(
                        "{}attribute {} conversion from {} to {} is done.".format(
                            log_helper,
                            unique_k,
                            doc[k],
                            self.database_schema[collection].type(unique_k),
                        )
                    )
                    if k in fixed_cnt:
                        fixed_cnt[k] += 1
                    else:
                        fixed_cnt[k] = 1
                except:
                    print_messages.append(
                        "{}attribute {} conversion from {} to {} cannot be done.".format(
                            log_helper,
                            unique_k,
                            doc[k],
                            self.database_schema[collection].type(unique_k),
                        )
                    )
            else:
                # attribute values remain the same
                update_dict[unique_k] = doc[k]

        # 4. update the fixed attributes in the document in the collection
        filter_ = {"_id": doc["_id"]}
        # use replace_one here because there may be some aliases in the document
        col.replace_one(filter_, update_dict)

        if verbose:
            for msg in print_messages:
                print(msg)

        return fixed_cnt

    def verify(self, document_id, collection="wf", tests=["xref", "type", "undefined"]):
        """
        This is an atomic-level operation to search for known issues in
        Metadata stored in a database and needed to construct a valid
        data set for starting a workflow.  By "atomic" we main the operation
        is for a single document in MongoDB linked to an atomic data
        object (currently that means TimeSeries or Seismogram objects).
        The tests are the same as those available through the command
        line tool dbverify.  See the man page for that tool and the
        user's manual for more details about the tests this method
        enables.

        :param document_id: the value of the _id field in the document you want to verify
        :type document_id: :class:`bson.objectid.ObjectId` of document to be tested
        :param collection: the name of collection to which document_id is
         expected to provide a unique match.  If not specified, uses the default wf collection
        :param tests: this should be a python list of test to apply by
         name keywords.  Test nams allowed are 'xref', 'type',
         and 'undefined'.   Default runs all tests.   Specify a subset of those
         keywords to be more restrictive.
        :type tests: :class:`list` of :class:`str`

        :return: a python dict keyed by a problematic key.  The value in each
          entry is the name of the failed test (i.e. 'xref', 'type', or 'undefined')
        :rtype: :class:`dict`
        :excpetion: This method will throw a fatal error exception if the
          id received does no match any document in the database.  That is
          intentional as the method should normally appear in a loop over
          ids found after query and the ids should then always be valid.
        """
        # check tests
        for test in tests:
            if test not in ["xref", "type", "undefined"]:
                raise MsPASSError(
                    "only xref, type and undefined are supported, but {} is requested.".format(
                        test
                    ),
                    "Fatal",
                )
        # remove redundant if happens
        tests = list(set(tests))

        problematic_keys = {}

        collection = self.database_schema.default_name(collection)
        col = self[collection]
        doc = col.find_one({"_id": document_id})

        if not doc:
            raise MsPASSError(
                "Database.verify:  objectid="
                + str(document_id)
                + " has no matching document in "
                + collection,
                "Fatal",
            )

        # run the tests
        for test in tests:
            if test == "xref":
                # test every possible xref keys in the doc
                for key in doc:
                    is_bad_xref_key, is_bad_wf = self._check_xref_key(
                        doc, collection, key
                    )
                    if is_bad_xref_key:
                        if key not in problematic_keys:
                            problematic_keys[key] = []
                        problematic_keys[key].append(test)

            elif test == "undefined":
                undefined_keys = self._check_undefined_keys(doc, collection)
                for key in undefined_keys:
                    if key not in problematic_keys:
                        problematic_keys[key] = []
                    problematic_keys[key].append(test)

            elif test == "type":
                # check if there are type mismatch between keys in doc and keys in schema
                for key in doc:
                    if self._check_mismatch_key(doc, collection, key):
                        if key not in problematic_keys:
                            problematic_keys[key] = []
                        problematic_keys[key].append(test)

        return problematic_keys

    def _check_xref_key(self, doc, collection, xref_key):
        """
        This atmoic function checks for a single xref_key in a single document

        :param doc:  the wf document, which is a type of dict
        :param collection: the name of collection saving the document.
        :param xref_key:  the xref key we need to check in the document

        :return: (is_bad_xref_key, is_bad_wf)
        :rtype: a :class:`tuple` of two :class:`bool`s
        """

        is_bad_xref_key = False
        is_bad_wf = False

        # if xref_key is not defind -> not checking
        if not self.database_schema[collection].is_defined(xref_key):
            return is_bad_xref_key, is_bad_wf

        # if xref_key is not a xref_key -> not checking
        unique_xref_key = self.database_schema[collection].unique_name(xref_key)
        if not self.database_schema[collection].is_xref_key(unique_xref_key):
            return is_bad_xref_key, is_bad_wf

        # if the xref_key is not in the doc -> bad_wf
        if xref_key not in doc and unique_xref_key not in doc:
            is_bad_wf = True
            return is_bad_xref_key, is_bad_wf

        if xref_key in doc:
            xref_key_val = doc[xref_key]
        else:
            xref_key_val = doc[unique_xref_key]

        # if we can't find document in the normalized collection/invalid xref_key naming -> bad_xref_key
        if "_id" in unique_xref_key and unique_xref_key.rsplit("_", 1)[1] == "id":
            normalized_collection_name = unique_xref_key.rsplit("_", 1)[0]
            normalized_collection_name = self.database_schema.default_name(
                normalized_collection_name
            )
            normalized_col = self[normalized_collection_name]
            # try to find the referenced docuement
            normalized_doc = normalized_col.find_one({"_id": xref_key_val})
            if not normalized_doc:
                is_bad_xref_key = True
            # invalid xref_key name
        else:
            is_bad_xref_key = True

        return is_bad_xref_key, is_bad_wf

    def _check_undefined_keys(self, doc, collection):
        """
        This atmoic function checks for if there are undefined required keys in a single document

        :param doc:  the wf document, which is a type of dict
        :param collection: the name of collection saving the document.

        :return: undefined_keys
        :rtype: :class:`list`
        """

        undefined_keys = []
        # check if doc has every required key in the collection schema
        unique_doc_keys = []
        # change possible aliases to unique keys
        for key in doc:
            if self.database_schema[collection].is_defined(key):
                unique_doc_keys.append(
                    self.database_schema[collection].unique_name(key)
                )
            else:
                unique_doc_keys.append(key)
            # check every required keys in the collection schema
        for key in self.database_schema[collection].required_keys():
            if key not in unique_doc_keys:
                undefined_keys.append(key)

        return undefined_keys

    def _check_mismatch_key(self, doc, collection, key):
        """
        This atmoic function checks for if the key is mismatch with the schema

        :param doc:  the wf document, which is a type of dict
        :param collection: the name of collection saving the document.
        :param key:  the key we need to check in the document

        :return: is_mismatch_key, if True, it means key is mismatch with the schema
        :rtype: :class:`bool`
        """

        is_mismatch_key = False
        if self.database_schema[collection].is_defined(key):
            unique_key = self.database_schema[collection].unique_name(key)
            val = doc[key] if key in doc else doc[unique_key]
            if not isinstance(val, self.database_schema[collection].type(unique_key)):
                is_mismatch_key = True

        return is_mismatch_key

    def _delete_attributes(self, collection, keylist, query=None, verbose=False):
        """
        Deletes all occurrences of attributes linked to keys defined
        in a list of keywords passed as (required) keylist argument.
        If a key is not in a given document no action is taken.

        :param collection:  MongoDB collection to be updated
        :param keylist:  list of keys for elements of each document
        that are to be deleted.   key are not test against schema
        but all matches will be deleted.
        :param query: optional query string passed to find database
        collection method.  Can be used to limit edits to documents
        matching the query.  Default is the entire collection.
        :param verbose:  when ``True`` edit will produce a line of printed
        output describing what was deleted.  Use this option only if
        you know from dbverify the number of changes to be made are small.

        :return:  dict keyed by the keys of all deleted entries.  The value
        of each entry is the number of documents the key was deleted from.
        :rtype: :class:`dict`
        """
        dbcol = self[collection]
        cursor = dbcol.find(query)
        counts = dict()
        # preload counts to 0 so we get a return saying 0 when no changes
        # are made
        for k in keylist:
            counts[k] = 0
        for doc in cursor:
            id = doc.pop("_id")
            n = 0
            todel = dict()
            for k in keylist:
                if k in doc:
                    todel[k] = doc[k]
                    val = doc.pop(k)
                    if verbose:
                        print(
                            "Deleted ", val, " with key=", k, " from doc with id=", id
                        )
                    counts[k] += 1
                    n += 1
            if n > 0:
                dbcol.update_one({"_id": id}, {"$unset": todel})
        return counts

    def _rename_attributes(self, collection, rename_map, query=None, verbose=False):
        """
        Renames specified keys for all or a subset of documents in a
        MongoDB collection.   The updates are driven by an input python
        dict passed as the rename_map argument. The keys of rename_map define
        doc keys that should be changed.  The values of the key-value
        pairs in rename_map are the new keys assigned to each match.


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
        dbcol = self[collection]
        cursor = dbcol.find(query)
        counts = dict()
        # preload counts to 0 so we get a return saying 0 when no changes
        # are made
        for k in rename_map:
            counts[k] = 0
        for doc in cursor:
            id = doc.pop("_id")
            n = 0
            for k in rename_map:
                n = 0
                if k in doc:
                    val = doc.pop(k)
                    newkey = rename_map[k]
                    if verbose:
                        print("Document id=", id)
                        print(
                            "Changed attribute with key=",
                            k,
                            " to have new key=",
                            newkey,
                        )
                        print("Attribute value=", val)
                    doc[newkey] = val
                    counts[k] += 1
                    n += 1
            dbcol.replace_one({"_id": id}, doc)
        return counts

    def _fix_attribute_types(self, collection, query=None, verbose=False):
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

        :param collection:  MongoDB collection to be updated
        :param query: optional query string passed to find database
        collection method.  Can be used to limit edits to documents
        matching the query.  Default is the entire collection.
        :param verbose:  when true edit will produce one or more lines of
        printed output for each change it makes.  The default is false.
        Needless verbose should be avoided unless you are certain the
        number of changes it will make are small.
        """
        dbcol = self[collection]
        schema = self.database_schema
        col_schema = schema[collection]
        counts = dict()
        cursor = dbcol.find(query)
        for doc in cursor:
            n = 0
            id = doc.pop("_id")
            if verbose:
                print("////////Document id=", id, "/////////")
            up_d = dict()
            for k in doc:
                val = doc[k]
                if not col_schema.is_defined(k):
                    if verbose:
                        print(
                            "Warning:  in doc with id=",
                            id,
                            "found key=",
                            k,
                            " that is not defined in the schema",
                        )
                        print("Value of key-value pair=", val)
                        print("Cannot check type for an unknown attribute name")
                    continue
                if not isinstance(val, col_schema.type(k)):
                    try:
                        newval = col_schema.type(k)(val)
                        up_d[k] = newval
                        if verbose:
                            print(
                                "Changed data for key=",
                                k,
                                " from ",
                                val,
                                " to ",
                                newval,
                            )
                        if k in counts:
                            counts[k] += 1
                        else:
                            counts[k] = 1
                        n += 1
                    except Exception as err:
                        print("////////Document id=", id, "/////////")
                        print(
                            "WARNING:  could not convert attribute with key=",
                            k,
                            " and value=",
                            val,
                            " to required type=",
                            col_schema.type(k),
                        )
                        print("This error was thrown and handled:  ")
                        print(err)

            if n > 0:
                dbcol.update_one({"_id": id}, {"$set": up_d})

        return counts

    def _check_links(
        self,
        xref_key=None,
        collection="wf",
        wfquery=None,
        verbose=False,
        error_limit=1000,
    ):
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

        :param xref_key: the normalized key you would like to check
        :param collection:  mspass waveform collection on which the normalization
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

        # undefined collection name in the schema
        try:
            wf_collection = self.database_schema.default_name(collection)
        except MsPASSError as err:
            raise MsPASSError(
                "check_links:  collection {} is not defined in database schema".format(
                    collection
                ),
                "Invalid",
            ) from err

        if wfquery is None:
            wfquery = {}

        # get all the xref_keys in the collection by schema
        xref_keys_list = self.database_schema[wf_collection].xref_keys()

        # the list of xref_key that should be checked
        xref_keys = xref_key

        # if xref_key is not defined, check all xref_keys
        if not xref_key:
            xref_keys = xref_keys_list

        # if specified as a single key, wrap it as a list for better processing next
        if type(xref_key) is str:
            xref_keys = [xref_key]

        # check every key in the xref_key list if it is legal
        for xref_key in xref_keys:
            unique_xref_key = self.database_schema[wf_collection].unique_name(xref_key)
            if unique_xref_key not in xref_keys_list:
                raise MsPASSError(
                    "check_links:  illegal value for normalize arg=" + xref_key, "Fatal"
                )

        # We accumulate bad ids in this list that is returned
        bad_id_list = list()
        missing_id_list = list()
        # check for each xref_key in xref_keys
        for xref_key in xref_keys:
            if "_id" in xref_key and xref_key.rsplit("_", 1)[1] == "id":
                normalize = xref_key.rsplit("_", 1)[0]
                normalize = self.database_schema.default_name(normalize)
            else:
                raise MsPASSError(
                    "check_links:  illegal value for normalize arg="
                    + xref_key
                    + " should be in the form of xxx_id",
                    "Fatal",
                )

            dbwf = self[wf_collection]
            n = dbwf.count_documents(wfquery)
            if n == 0:
                raise MsPASSError(
                    "checklinks:  "
                    + wf_collection
                    + " collection has no data matching query="
                    + str(wfquery),
                    "Fatal",
                )
            if verbose:
                print(
                    "Starting cross reference link check for ",
                    wf_collection,
                    " collection using id=",
                    xref_key,
                )
                print("This should resolve links to ", normalize, " collection")

            cursor = dbwf.find(wfquery)
            for doc in cursor:
                wfid = doc["_id"]
                is_bad_xref_key, is_bad_wf = self._check_xref_key(
                    doc, wf_collection, xref_key
                )
                if is_bad_xref_key:
                    if wfid not in bad_id_list:
                        bad_id_list.append(wfid)
                    if verbose:
                        print(str(wfid), " link with ", str(xref_key), " failed")
                    if len(bad_id_list) > error_limit:
                        raise MsPASSError(
                            "checklinks:  number of bad id errors exceeds internal limit",
                            "Fatal",
                        )
                if is_bad_wf:
                    if wfid not in missing_id_list:
                        missing_id_list.append(wfid)
                    if verbose:
                        print(str(wfid), " is missing required key=", xref_key)
                    if len(missing_id_list) > error_limit:
                        raise MsPASSError(
                            "checklinks:  number of missing id errors exceeds internal limit",
                            "Fatal",
                        )
                if (
                    len(bad_id_list) >= error_limit
                    or len(missing_id_list) >= error_limit
                ):
                    break

        return tuple([bad_id_list, missing_id_list])

    def _check_attribute_types(
        self, collection, query=None, verbose=False, error_limit=1000
    ):
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
        if query is None:
            query = {}
        # The following two can throw MsPASS errors but we let them
        # do so. Callers should have a handler for MsPASSError
        dbschema = self.database_schema
        # This holds the schema for the collection to be scanned
        # dbschema is mostly an index to one of these
        col_schema = dbschema[collection]
        dbcol = self[collection]
        n = dbcol.count_documents(query)
        if n == 0:
            raise MsPASSError(
                "check_attribute_types:  query="
                + str(query)
                + " yields zero matching documents",
                "Fatal",
            )
        cursor = dbcol.find(query)
        bad_type_docs = dict()
        undefined_key_docs = dict()
        for doc in cursor:
            bad_types = dict()
            undefined_keys = dict()
            id = doc["_id"]
            for k in doc:
                if col_schema.is_defined(k):
                    val = doc[k]
                    if type(val) != col_schema.type(k):
                        bad_types[k] = doc[k]
                        if verbose:
                            print("doc with id=", id, " type mismatch for key=", k)
                            print(
                                "value=",
                                doc[k],
                                " does not match expected type=",
                                col_schema.type(k),
                            )
                else:
                    undefined_keys[k] = doc[k]
                    if verbose:
                        print(
                            "doc with id=",
                            id,
                            " has undefined key=",
                            k,
                            " with value=",
                            doc[k],
                        )
            if len(bad_types) > 0:
                bad_type_docs[id] = bad_types
            if len(undefined_keys) > 0:
                undefined_key_docs[id] = undefined_keys
            if (
                len(undefined_key_docs) >= error_limit
                or len(bad_type_docs) >= error_limit
            ):
                break

        return tuple([bad_type_docs, undefined_key_docs])

    def _check_required(
        self,
        collection="site",
        keys=["lat", "lon", "elev", "starttime", "endtime"],
        query=None,
        verbose=False,
        error_limit=100,
    ):
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
        if len(keys) == 0:
            raise MsPASSError(
                "check_required:  list of required keys is empty "
                + "- nothing to test",
                "Fatal",
            )
        if query is None:
            query = {}
        # The following two can throw MsPASS errors but we let them
        # do so. Callers should have a handler for MsPASSError
        dbschema = self.database_schema
        # This holds the schema for the collection to be scanned
        # dbschema is mostly an index to one of these
        col_schema = dbschema[collection]
        dbcol = self[collection]
        # We first make sure the user didn't make a mistake in giving an
        # invalid key for the required list
        for k in keys:
            if not col_schema.is_defined(k):
                raise MsPASSError(
                    "check_required:  schema has no definition for key=" + k, "Fatal"
                )

        n = dbcol.count_documents(query)
        if n == 0:
            raise MsPASSError(
                "check_required:  query="
                + str(query)
                + " yields zero matching documents",
                "Fatal",
            )
        undef = dict()
        wrong_types = dict()
        cursor = dbcol.find(query)
        for doc in cursor:
            id = doc["_id"]
            undef_this = list()
            wrong_this = dict()
            for k in keys:
                if not k in doc:
                    undef_this.append(k)
                else:
                    val = doc[k]
                    if type(val) != col_schema.type(k):
                        wrong_this[k] = val
            if len(undef_this) > 0:
                undef[id] = undef_this
            if len(wrong_this) > 0:
                wrong_types[id] = wrong_this
            if len(wrong_types) >= error_limit or len(undef) >= error_limit:
                break
        return tuple([wrong_types, undef])

    def update_metadata(
        self,
        mspass_object,
        collection=None,
        mode="cautious",
        exclude_keys=None,
        force_keys=None,
        alg_name="Database.update_metadata",
    ):
        """
        Use this method if you want to save the output of a processing algorithm
        whose output is only posted to metadata.   That can be something as
        simple as a little python function that does some calculations on other
        metadata field, or as elaborate as a bound FORTRAN or C/C++ function
        that computes something, posts the results to Metadata, but doesn't
        actually alter the sample data.   A type example of the later is an amplitude
        calculation that posts the computed amplitude to some metadata key value.

        This method will ONLY attempt to update Metadata attributes stored in the
        data passed (mspass_object) that have been marked as having been
        changed since creation of the data object.  The default mode will
        check entries against the schema and attempt to fix any type
        mismatches (mode=='cautious' for this algorithm).  In cautious or
        pedantic mode this method can end up posting a lot of errors in
        elog for data object (mspass_object) being handled.  In
        promiscuous mode there are no safeties and the any values
        that are defined in Metadata as having been changed will be
        posted as an update to the parent wf document to the data object.

        A feature of the schema that is considered an unbreakable rule is
        that any attribute marked "readonly" in the schema cannot by
        definition be updated with this method.  It utilizes the same
        method for handling this as the save_data method.  That is,
        for all "mode" parameters if an key is defined in the schema as
        readonly and it is listed as having been modified, it will
        be save with a new key creating by adding the prefix
        "READONLYERROR_" .  e.g. if we had a site_sta read as
        'AAK' but we changed it to 'XYZ' in a workflow, when we tried
        to save the data you will find an entry in the document
        of {'READONLYERROR_site_sta' : 'XYZ'}

        :param mspass_object: the object you want to update.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param exclude_keys: a list of metadata attributes you want to exclude from being updated.
        :type exclude_keys: a :class:`list` of :class:`str`
        :param force_keys: a list of metadata attributes you want to force
         to be updated.   Normally this method will only update attributes
         that have been marked as changed since creation of the parent data
         object.  If data with these keys is found in the mspass_object they
         will be added to the update record.
        :type force_keys: a :class:`list` of :class:`str`
        :param collection: the collection name you want to use. If not specified, use the defined collection in the metadata schema.
        :param mode: This parameter defines how attributes defines how
          strongly to enforce schema constraints. As described above
          'promiscuous' justs updates all changed values with no schema
          tests.  'cautious', the default, enforces type constraints and
          tries to convert easily fixed type mismatches (e.g. int to floats
          of vice versa).  Both 'cautious' and 'pedantic' may leave one or
          more complaint message in the elog of mspass_object on how the
          method did or did not fix mismatches with the schema.  Both
          also will drop any key-value pairs where the value cannot be
          converted to the type defined in the schema.

        :type mode: :class:`str`
        :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
        :type alg_name: :class:`str`
        :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
        :type alg_id: :class:`bson.objectid.ObjectId`
        :return: mspass_object data.  Normally this is an unaltered copy
          of the data passed through mspass_object.  If there are errors,
          however, the elog will contain new messages.  Note any such
          messages are volatile and will not be saved to the database
          until the save_data method is called.
        """
        if not isinstance(mspass_object, (TimeSeries, Seismogram)):
            raise TypeError(
                alg_name
                + ":  only TimeSeries and Seismogram are supported\nReceived data of type="
                + str(type(mspass_object))
            )
        # Return a None immediately if the data is marked dead - signal it did nothing
        if mspass_object.dead():
            return None
        if exclude_keys is None:
            exclude_keys = []
        if mode not in ["promiscuous", "cautious", "pedantic"]:
            raise MsPASSError(
                alg_name
                + ": only promiscuous, cautious and pedantic are supported, but {} was requested.".format(
                    mode
                ),
                "Fatal",
            )
        if isinstance(mspass_object, TimeSeries):
            save_schema = self.metadata_schema.TimeSeries
        else:
            save_schema = self.metadata_schema.Seismogram
        if "_id" in mspass_object:
            wfid = mspass_object["_id"]
        else:
            raise MsPASSError(
                alg_name
                + ": input data object is missing required waveform object id value (_id) - update is not possible without it",
                "Fatal",
            )
        if collection:
            wf_collection_name = collection
        else:
            # This returns a string that is the collection name for this atomic data type
            # A weird construct
            wf_collection_name = save_schema.collection("_id")
        wf_collection = self[wf_collection_name]
        # One last check.  Make sure a document with the _id in mspass_object
        # exists.  If it doesn't exist, post an elog about this
        test_doc = wf_collection.find_one({"_id": wfid})
        if not test_doc:  # find_one returns None if find fails
            mspass_object.elog.log_error(
                "Database.update_metadata",
                "Cannot find the document in the wf collection by the _id field in the object",
                ErrorSeverity.Complaint,
            )
        # FIXME starttime will be automatically created in this function
        self._sync_metadata_before_update(mspass_object)

        # This method of Metadata returns a list of all
        # attributes that were changed after creation of the
        # object to which they are attached.
        changed_key_list = mspass_object.modified()
        if force_keys:
            for k in force_keys:
                changed_key_list.add(k)
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
        # Now remove any readonly data
        for k in copied_metadata.keys():
            if k == "_id":
                continue
            if save_schema.is_defined(k):
                if save_schema.readonly(k):
                    if k in changed_key_list:
                        newkey = "READONLYERROR_" + k
                        copied_metadata.change_key(k, newkey)
                        mspass_object.elog.log_error(
                            "Database.update_metadata",
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
            # In this case we blindly update all entries that show
            # as modified that aren't in the exclude list
            for k in changed_key_list:
                if k in copied_metadata:
                    insertion_dict[k] = copied_metadata[k]
        else:
            # Other modes have to test every key and type of value
            # before continuing.  pedantic logs an error for all problems
            # Both attempt to fix type mismatches before update.  Cautious
            # is silent unless the type problem cannot be repaired.  In that
            # case both will not attempt to update the offending key-value
            # Note many errors can be posted - one for each problem key-value pair
            for k in copied_metadata:
                if k in changed_key_list:
                    if save_schema.is_defined(k):
                        if isinstance(copied_metadata[k], save_schema.type(k)):
                            insertion_dict[k] = copied_metadata[k]
                        else:
                            if mode == "pedantic":
                                message = "pedantic mode error:  key=" + k
                                value = copied_metadata[k]
                                message += (
                                    " type of stored value="
                                    + str(type(value))
                                    + " does not match schema expectation="
                                    + str(save_schema.type(k))
                                    + "\nAttempting to correct type mismatch"
                                )
                                mspass_object.elog.log_error(
                                    alg_name, message, ErrorSeverity.Complaint
                                )
                            # Note we land here for both pedantic and cautious but not promiscuous
                            try:
                                # The following convert the actual value in a dict to a required type.
                                # This is because the return of type() is the class reference.
                                old_value = copied_metadata[k]
                                insertion_dict[k] = save_schema.type(k)(
                                    copied_metadata[k]
                                )
                                new_value = insertion_dict[k]
                                message = (
                                    "Had to convert type of data with key="
                                    + k
                                    + " from "
                                    + str(type(old_value))
                                    + " to "
                                    + str(type(new_value))
                                )
                                mspass_object.elog.log_error(
                                    alg_name, message, ErrorSeverity.Complaint
                                )
                            except Exception as err:
                                message = "Cannot update data with key=" + k + "\n"
                                if mode == "pedantic":
                                    # pedantic mode
                                    mspass_object.kill()
                                    message += (
                                        "pedantic mode error: key value could not be converted to required type="
                                        + str(save_schema.type(k))
                                        + " actual type="
                                        + str(type(copied_metadata[k]))
                                        + ", the object is killed"
                                    )
                                else:
                                    # cautious mode
                                    if save_schema.is_required(k):
                                        mspass_object.kill()
                                        message += (
                                            " Required key value could not be converted to required type="
                                            + str(save_schema.type(k))
                                            + " actual type="
                                            + str(type(copied_metadata[k]))
                                            + ", the object is killed"
                                        )
                                    else:
                                        message += (
                                            " Value stored has type="
                                            + str(type(copied_metadata[k]))
                                            + " which cannot be converted to type="
                                            + str(save_schema.type(k))
                                            + "\n"
                                        )
                                message += "Data for this key will not be changed or set in the database"
                                message += "\nPython error exception message caught:\n"
                                message += str(err)
                                # post elog entry into the mspass_object
                                if mode == "pedantic" or save_schema.is_required(k):
                                    mspass_object.elog.log_error(
                                        alg_name, message, ErrorSeverity.Invalid
                                    )
                                else:
                                    mspass_object.elog.log_error(
                                        alg_name, message, ErrorSeverity.Complaint
                                    )
                                # The None arg2 cause pop to not throw
                                # an exception if k isn't defined - a bit ambigous in the try block
                                insertion_dict.pop(k, None)
                    else:
                        if mode == "pedantic":
                            mspass_object.elog.log_error(
                                alg_name,
                                "cannot update data with key="
                                + k
                                + " because it is not defined in the schema",
                                ErrorSeverity.Complaint,
                            )
                        else:
                            mspass_object.elog.log_error(
                                alg_name,
                                "key="
                                + k
                                + " is not defined in the schema.  Updating record, but this may cause downstream problems",
                                ErrorSeverity.Complaint,
                            )
                            insertion_dict[k] = copied_metadata[k]

        # ugly python indentation with this logic.  We always land here in
        # any mode when we've passed over the entire metadata dict
        # if it is dead, it's considered something really bad happens, we should not update the object
        if mspass_object.live:
            wf_collection.update_one(
                {"_id": wfid}, {"$set": insertion_dict}, upsert=True
            )
        return mspass_object

    def update_data(
        self,
        mspass_object,
        collection=None,
        mode="cautious",
        exclude_keys=None,
        force_keys=None,
        alg_id="0",
        alg_name="Database.update_data",
    ):
        """
        Updates both metadata and sample data corresponding to an input data
        object.

        Since storage of data objects in MsPASS is broken into multiple
        collections and storage methods, doing a full data update has some
        complexity.   This method handles the problem differently for the
        different pieces:
            1. An update is performed on the parent wf collection document.
               That update makes use of the related Database method
               called update_metadata.
            2. If the error log is not empty it is saved.
            3. If the history container has contents it is saved.
            4. The sample data is the thorniest problem. Currently this
               method will only do sample updates for data stored in
               the mongodb gridfs system.   With files containing multiple
               waveforms it would be necessary to append to the files and
               this could create a blaat problem with large data sets so
               we do not currently support that type of update.

        :param mspass_object: the object you want to update.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param exclude_keys: a list of metadata attributes you want to exclude from being updated.
        :type exclude_keys: a :class:`list` of :class:`str`
        :param force_keys: a list of metadata attributes you want to force
         to be updated.   Normally this method will only update attributes
         that have been marked as changed since creation of the parent data
         object.  If data with these keys is found in the mspass_object they
         will be added to the update record.
        :type force_keys: a :class:`list` of :class:`str`
        :param collection: the collection name you want to use. If not specified, use the defined collection in the metadata schema.
        :param mode: This parameter defines how attributes defines how
          strongly to enforce schema constraints. As described above
          'promiscuous' justs updates all changed values with no schema
          tests.  'cautious', the default, enforces type constraints and
          tries to convert easily fixed type mismatches (e.g. int to floats
          of vice versa).  Both 'cautious' and 'pedantic' may leave one or
          more complaint message in the elog of mspass_object on how the
          method did or did not fix mismatches with the schema.  Both
          also will drop any key-value pairs where the value cannot be
          converted to the type defined in the schema.
        :type mode: :class:`str`
        :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
          (defaults to 'Database.update_data' and should not normally need to be changed)
        :type alg_name: :class:`str`
        :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
        :type alg_id: :class:`bson.objectid.ObjectId`
        :return: mspass_object data.  Normally this is an unaltered copy
          of the data passed through mspass_object.  If there are errors,
          however, the elog will contain new messages.  All such messages,
          howevever, should be saved in the elog collection because elog
          is the last collection updated.
        """
        schema = self.metadata_schema
        if isinstance(mspass_object, TimeSeries):
            save_schema = schema.TimeSeries
        else:
            save_schema = schema.Seismogram
        # First update metadata.  update_metadata will throw an exception
        # only for usage errors.   We test the elog size to check if there
        # are other warning messages and add a summary if there were any
        logsize0 = mspass_object.elog.size()
        try:
            self.update_metadata(
                mspass_object,
                collection=collection,
                mode=mode,
                exclude_keys=exclude_keys,
                force_keys=force_keys,
                alg_name=alg_name,
            )
        except:
            raise
        logsize = mspass_object.elog.size()
        # A bit verbose, but we post this warning to make it clear the
        # problem originated from update_data - probably not really needed
        # but better to be though I think
        if logsize > logsize0:
            mspass_object.elog.log_error(
                alg_name,
                "update_metadata posted {nerr} messages during update".format(
                    nerr=(logsize - logsize0)
                ),
                ErrorSeverity.Complaint,
            )

        # optional handle history - we need to update the wf record later with this value
        # if it is set
        update_record = dict()
        history_obj_id_name = (
            self.database_schema.default_name("history_object") + "_id"
        )
        history_object_id = None
        if not mspass_object.is_empty():
            history_object_id = self._save_history(mspass_object, alg_name, alg_id)
            update_record[history_obj_id_name] = history_object_id

        # Now handle update of sample data.  The gridfs method used here
        # handles that correctly based on the gridfs id.
        if mspass_object.live:
            if "storage_mode" in mspass_object:
                storage_mode = mspass_object["storage_mode"]
                if not storage_mode == "gridfs":
                    mspass_object.elog.log_error(
                        alg_name,
                        "found storage_mode="
                        + storage_mode
                        + "  Only support update to gridfs.  Changing to gridfs storage for sample data update",
                        ErrorSeverity.Complaint,
                    )
                    mspass_object["storage_mode"] = "gridfs"
            else:
                mspass_object.elog.log_error(
                    alg_name,
                    "storage_mode attribute was not set in Metadata of this object - setting as gridfs for update",
                    ErrorSeverity.Complaint,
                )
                mspass_object["storage_mode"] = "gridfs"
                update_record["storage_mode"] = "gridfs"
            # This logic depends upon a feature of _save_data_to_gridfs.
            # if the gridfs_id parameter is defined it does an update
            # when it is not defined it creates a new gridfs "file". In both
            # cases the id needed to get the right datum is returned
            if "gridfs_id" in mspass_object:
                gridfs_id = self._save_data_to_gridfs(
                    mspass_object, mspass_object["gridfs_id"]
                )
            else:
                gridfs_id = self._save_data_to_gridfs(mspass_object)
            mspass_object["gridfs_id"] = gridfs_id
            # There is a possible efficiency gain right here.  Not sure if
            # gridfs_id is altered when the sample data are updated in place.
            # if we can be sure the returned gridfs_id is the same as the
            # input in that case, we would omit gridfs_id from the update
            # record and most data would not require the final update
            # transaction below
            update_record["gridfs_id"] = gridfs_id
            # should define wf_collection here because if the mspass_object is dead
            if collection:
                wf_collection_name = collection
            else:
                # This returns a string that is the collection name for this atomic data type
                # A weird construct
                wf_collection_name = save_schema.collection("_id")
            wf_collection = self[wf_collection_name]

            elog_id = None
            if mspass_object.elog.size() > 0:
                elog_id_name = self.database_schema.default_name("elog") + "_id"
                # FIXME I think here we should check if elog_id field exists in the mspass_object
                # and we should update the elog entry if mspass_object already had one
                if elog_id_name in mspass_object:
                    old_elog_id = mspass_object[elog_id_name]
                else:
                    old_elog_id = None
                # elog ids will be updated in the wf col when saving metadata
                elog_id = self._save_elog(mspass_object, elog_id=old_elog_id)
                update_record[elog_id_name] = elog_id

                # update elog collection
                # we have to do the xref to wf collection like this too
                elog_col = self[self.database_schema.default_name("elog")]
                wf_id_name = wf_collection_name + "_id"
                filter_ = {"_id": elog_id}
                elog_col.update_one(
                    filter_, {"$set": {wf_id_name: mspass_object["_id"]}}
                )
            # finally we need to update the wf document if we set anything
            # in update_record
            if len(update_record):
                filter_ = {"_id": mspass_object["_id"]}
                wf_collection.update_one(filter_, {"$set": update_record})
                # we may probably set the elog_id field in the mspass_object
                if elog_id:
                    mspass_object[elog_id_name] = elog_id
                # we may probably set the history_object_id field in the mspass_object
                if history_object_id:
                    mspass_object[history_obj_id_name] = history_object_id
        else:
            # Dead data land here
            elog_id_name = self.database_schema.default_name("elog") + "_id"
            if elog_id_name in mspass_object:
                old_elog_id = mspass_object[elog_id_name]
            else:
                old_elog_id = None
            elog_id = self._save_elog(mspass_object, elog_id=old_elog_id)

        return mspass_object

    def read_ensemble_data(
        self,
        objectid_list,
        ensemble_metadata={},
        mode="promiscuous",
        normalize=None,
        load_history=False,
        exclude_keys=None,
        collection="wf",
        data_tag=None,
        alg_name="read_ensemble_data",
        alg_id="0",
    ):
        """
        Reads an subset of a dataset with some logical grouping into an Ensemble container.

        Ensembles are a core concept in MsPASS that are a generalization of
        fixed "gather" types frozen into every seismic reflection processing
        system we know of.  This reader is driven by a python list of
        MongoDB object ids.  The method calls the atomic read_data
        method for object_id to assemble the members of the ensemble.
        All arguments except the objectid_list and ensemble_metadata are
        passed directly to the read_data method in that loop.  The read_data
        method and the User's manual have more information about how those
        common arguments are used.

        :param objectid_list: a :class:`list` of :class:`bson.objectid.ObjectId`,
          of the ids defining the ensemble members or a :class:`pymongo.cursor.Cursor`
        :param ensemble_metadata:  is a dict or dict like container containing
          metadata to be stored in the ensemble's Metadata (common to the group)
          container.  A common choice would be to post the query used to
          define this ensemble, but there are not restictions.  The contents are
          simply copied verbatim to the ensemble metadata container. The default
          is an empty dict which translates to an empty ensemble metadata container.
        :type ensemble_metadata: python dict or any container that is iterable
          and supports the [key] key associative array syntax will work.
          (Note this means both Metadata containers or mongodb docs both can
          be used) The type of this arg is not tested so you may get a
          mysterious exception if the data the arg defines does no meet the
          two rules.
        :param mode: reading mode regarding schema checks, should be one of ['promiscuous','cautious','pedantic']
        :type mode: :class:`str`
        :param normalize: normalized collection you want to read into a mspass object
        :type normalize: a :class:`list` of :class:`str`
        :param load_history: ``True`` to load object-level history into the mspasspy object.
        :param exclude_keys: the metadata attributes you want to exclude from being read.
        :type exclude_keys: a :class:`list` of :class:`str`
        :param collection: the collection name in the database that the object is stored. If not specified, use the default wf collection in the schema.
        :param data_tag: a user specified "data_tag" key to filter the read. If not match, the record will be skipped.
        :type data_tag: :class:`str`
        :return: either :class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` or
            :class:`mspasspy.ccore.seismic.SeismogramEnsemble`.
        """
        wf_collection = self.database_schema.default_name(collection)
        object_type = self.database_schema[wf_collection].data_type()

        if object_type not in [TimeSeries, Seismogram]:
            raise MsPASSError(
                "only TimeSeries and Seismogram are supported, but {} is requested. Please check the data_type of {} collection.".format(
                    object_type, wf_collection
                ),
                "Fatal",
            )

        # if objectid_list is a cursor, convert the cursor to a list
        if isinstance(objectid_list, pymongo.cursor.Cursor):
            objectid_list = list(objectid_list)

        if object_type is TimeSeries:
            ensemble = TimeSeriesEnsemble(len(objectid_list))
        else:
            ensemble = SeismogramEnsemble(len(objectid_list))
        # Here we post the ensemble metdata - see docstring notes on this feature
        for k in ensemble_metadata:
            ensemble[k] = ensemble_metadata[k]

        for i in objectid_list:
            data = self.read_data(
                i,
                mode=mode,
                normalize=normalize,
                load_history=load_history,
                exclude_keys=exclude_keys,
                collection=wf_collection,
                data_tag=data_tag,
                alg_name=alg_name,
                alg_id=alg_id,
            )
            if data:
                ensemble.member.append(data)

        # explicitly mark empty ensembles dead.  Otherwise assume if
        # we got this far we can mark it live
        if len(ensemble.member) > 0:
            ensemble.set_live()
        else:
            ensemble.kill()
        return ensemble

    def save_ensemble_data(
        self,
        ensemble_object,
        mode="promiscuous",
        storage_mode="gridfs",
        dir_list=None,
        dfile_list=None,
        exclude_keys=None,
        exclude_objects=None,
        collection=None,
        data_tag=None,
        alg_name="save_ensemble_data",
        alg_id="0",
    ):
        """
        Save an Ensemble container of a group of data objecs to MongoDB.

        Ensembles are a core concept in MsPASS that are a generalization of
        fixed "gather" types frozen into every seismic reflection processing
        system we know of.   This is is a writer for data stored in such a
        container.  It is little more than a loop over each "member" of the
        ensemble calling the Database.save_data method for each member.
        For that reason most of the arguments are passed downstream directly
        to save_data.   See the save_data method and the User's manual for
        more verbose descriptions of their behavior and expected use.

        The only complexity in handling an ensemble is that our implementation
        has a separate Metadata container associated with the overall group
        that is assumed to be constant for every member of the ensemble.  For this
        reason before entering the loop calling save_data on each member
        the method calls the objects sync_metadata method that copies (overwrites if
        previously defined) the ensemble attributes to each member.  That assure
        atomic saves will not lose their association with a unique ensemble
        indexing scheme.

        A final feature of note is that an ensemble can be marked dead.
        If the entire ensemble is set dead this function returns
        immediately and does nothing.


        :param ensemble_object: the ensemble you want to save.
        :type ensemble_object: either :class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` or
            :class:`mspasspy.ccore.seismic.SeismogramEnsemble`.
        :param mode: reading mode regarding schema checks, should be one of ['promiscuous','cautious','pedantic']
        :type mode: :class:`str`
        :param storage_mode: "gridfs" stores the object in the mongodb grid file system (recommended). "file" stores
            the object in a binary file, which requires ``dfile`` and ``dir``.
        :type storage_mode: :class:`str`
        :param dir_list: A :class:`list` of file directories if using "file" storage mode. File directory is ``str`` type.
        :param dfile_list: A :class:`list` of file names if using "file" storage mode. File name is ``str`` type.
        :param exclude_keys: the metadata attributes you want to exclude from being stored.
        :type exclude_keys: a :class:`list` of :class:`str`
        :param exclude_objects: A list of indexes, where each specifies a object in the ensemble you want to exclude from being saved. Starting from 0.
        :type exclude_objects: :class:`list`
        :param collection: the collection name you want to use. If not specified, use the defined collection in the metadata schema.
        :param data_tag: a user specified "data_tag" key to tag the saved wf document.
        :type data_tag: :class:`str`
        """
        if ensemble_object.dead():
            return
        if not dfile_list:
            dfile_list = [None for _ in range(len(ensemble_object.member))]
        if not dir_list:
            dir_list = [None for _ in range(len(ensemble_object.member))]
        if exclude_objects is None:
            exclude_objects = []
        # sync_metadata is a ccore method of the Ensemble  template
        ensemble_object.sync_metadata()

        if storage_mode in ["file", "gridfs"]:
            j = 0
            for i in range(len(ensemble_object.member)):
                if i not in exclude_objects:
                    self.save_data(
                        ensemble_object.member[i],
                        mode=mode,
                        storage_mode=storage_mode,
                        dir=dir_list[j],
                        dfile=dfile_list[j],
                        exclude_keys=exclude_keys,
                        collection=collection,
                        data_tag=data_tag,
                        alg_name=alg_name,
                        alg_id=alg_id,
                    )
                    j += 1
        elif storage_mode == "url":
            pass
        else:
            raise TypeError("Unknown storage mode: {}".format(storage_mode))

    def update_ensemble_metadata(
        self,
        ensemble_object,
        mode="promiscuous",
        exclude_keys=None,
        exclude_objects=None,
        collection=None,
        alg_name="update_ensemble_metadata",
        alg_id="0",
    ):
        """
        Updates (or save if it's new) the mspasspy ensemble object, including saving the processing history, elogs
        and metadata attributes.

        This method is a companion to save_ensemble_data. The relationship is
        comparable to that between the save_data and update_metadata methods.
        In particular, this method is mostly for internal use to save the
        contents of the Metadata container in each ensemble member.  Like
        save_ensemble_data it is mainly a loop over ensemble members calling
        update_metadata on each member.  Also like update_metadata it is
        advanced usage to use this method directly.  Most users will apply
        it under the hood as part of calls to save_ensemble_data.

        :param ensemble_object: the ensemble you want to update.
        :type ensemble_object: either :class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` or
            :class:`mspasspy.ccore.seismic.SeismogramEnsemble`.
        :param mode: reading mode regarding schema checks, should be one of ['promiscuous','cautious','pedantic']
        :type mode: :class:`str`
        :param exclude_keys: the metadata attributes you want to exclude from being updated.
        :type exclude_keys: a :class:`list` of :class:`str`
        :param exclude_objects: a list of indexes, where each specifies a object in the ensemble you want to
        exclude from being saved. The index starts at 0.
        :type exclude_objects: :class:`list`
        :param collection: the collection name you want to use. If not specified, use the defined collection in the metadata
        schema.
        :param ignore_metadata_changed_test: if specify as ``True``, we do not check the whether attributes we want to update are in the Metadata.modified() set. Default to be ``False``.
        :param data_tag: a user specified "data_tag" key to tag the saved wf document.
        :type data_tag: :class:`str`
        """
        if exclude_objects is None:
            exclude_objects = []

        for i in range(len(ensemble_object.member)):
            # Skip data listed for exclusion and those that are marked dead
            if i not in exclude_objects and ensemble_object.member[i].live:
                self.update_metadata(
                    ensemble_object.member[i],
                    mode=mode,
                    exclude_keys=exclude_keys,
                    collection=collection,
                    alg_name=alg_name,
                )

    def delete_data(
        self,
        object_id,
        object_type,
        remove_unreferenced_files=False,
        clear_history=True,
        clear_elog=True,
    ):
        """
        Delete method for handling mspass data objects (TimeSeries and Seismograms).

        Delete is one of the basic operations any database system should
        support (the last letter of the acronymn CRUD is delete).  Deletion is
        nontrivial with seismic data stored with the model used in MsPASS.
        The reason is that the content of the objects are spread between
        multiple collections and sometimes use storage in files completely
        outside MongoDB.   This method, however, is designed to handle that
        and when given the object id defining a document in one of the wf
        collections, it will delete the wf document entry and manage the
        waveform data.  If the data are stored in gridfs the deletion of
        the waveform data will be immediate.  If the data are stored in
        disk files the file will be deleted when there are no more references
        in the wf collection for the exact combination of dir and dfile associated
        an atomic deletion.  Error log and history data deletion linked to
        a datum is optional.


        :param object_id: the wf object id you want to delete.
        :type object_id: :class:`bson.objectid.ObjectId`
        :param object_type: the object type you want to delete, must be one of ['TimeSeries', 'Seismogram']
        :type object_type: :class:`str`
        :param remove_unreferenced_files: if ``True``, we will try to remove the file that no wf data is referencing. Default to be ``False``
        :param clear_history: if ``True``, we will clear the processing history of the associated wf object, default to be ``True``
        :param clear_elog: if ``True``, we will clear the elog entries of the associated wf object, default to be ``True``
        """
        if object_type not in ["TimeSeries", "Seismogram"]:
            raise TypeError("only TimeSeries and Seismogram are supported")

        # get the wf collection name in the schema
        schema = self.metadata_schema
        if object_type == "TimeSeries":
            detele_schema = schema.TimeSeries
        else:
            detele_schema = schema.Seismogram
        wf_collection_name = detele_schema.collection("_id")

        # user might pass a mspass object by mistake
        try:
            oid = object_id["_id"]
        except:
            oid = object_id

        # fetch the document by the given object id
        object_doc = self[wf_collection_name].find_one({"_id": oid})
        if not object_doc:
            raise MsPASSError(
                "Could not find document in wf collection by _id: {}.".format(oid),
                "Invalid",
            )

        # delete the document just retrieved from the database
        self[wf_collection_name].delete_one({"_id": oid})

        # delete gridfs/file depends on storage mode, and unreferenced files
        storage_mode = object_doc["storage_mode"]
        if storage_mode == "gridfs":
            gfsh = gridfs.GridFS(self)
            if gfsh.exists(object_doc["gridfs_id"]):
                gfsh.delete(object_doc["gridfs_id"])

        elif storage_mode in ["file"] and remove_unreferenced_files:
            dir_name = object_doc["dir"]
            dfile_name = object_doc["dfile"]
            # find if there are any remaining matching documents with dir and dfile
            match_doc_cnt = self[wf_collection_name].count_documents(
                {"dir": dir_name, "dfile": dfile_name}
            )
            # delete this file
            if match_doc_cnt == 0:
                fname = os.path.join(dir_name, dfile_name)
                os.remove(fname)

        # clear history
        if clear_history:
            history_collection = self.database_schema.default_name("history_object")
            history_obj_id_name = history_collection + "_id"
            if history_obj_id_name in object_doc:
                self[history_collection].delete_one(
                    {"_id": object_doc[history_obj_id_name]}
                )

        # clear elog
        if clear_elog:
            wf_id_name = wf_collection_name + "_id"
            elog_collection = self.database_schema.default_name("elog")
            elog_id_name = elog_collection + "_id"
            # delete the one by elog_id in mspass object
            if elog_id_name in object_doc:
                self[elog_collection].delete_one({"_id": object_doc[elog_id_name]})
            # delete the documents with the wf_id equals to obejct['_id']
            self[elog_collection].delete_many({wf_id_name: oid})

    def _load_collection_metadata(
        self, mspass_object, exclude_keys, include_undefined=False, collection=None
    ):
        """
        Master Private Method

        Reads metadata from a requested collection and loads standard attributes from collection to the data passed as mspass_object.
        The method will only work if mspass_object has the collection_id attribute set to link it to a unique document in source.

        :param mspass_object:   data where the metadata is to be loaded
        :type mspass_object:  :class:`mspasspy.ccore.seismic.TimeSeries`,
            :class:`mspasspy.ccore.seismic.Seismogram`,
            :class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` or
            :class:`mspasspy.ccore.seismic.SeismogramEnsemble`.
        :param exclude_keys: list of attributes that should not normally be loaded. Ignored if include_undefined is set ``True``.
        :type exclude_keys: a :class:`list` of :class:`str`
        :param include_undefined:  when ``True`` all data in the matching document are loaded.
        :param collection: requested collection metadata should be loaded
        :type collection: :class:`str`

        :raises mspasspy.ccore.utility.MsPASSError: any detected errors will cause a MsPASSError to be thrown
        """
        if not mspass_object.live:
            raise MsPASSError(
                "only live mspass object can load metadata", ErrorSeverity.Invalid
            )

        if not isinstance(mspass_object, (TimeSeries, Seismogram)):
            raise MsPASSError(
                "only TimeSeries and Seismogram are supported", ErrorSeverity.Invalid
            )

        if collection == "channel" and isinstance(
            mspass_object, (Seismogram, SeismogramEnsemble)
        ):
            raise MsPASSError(
                "channel data can not be loaded into Seismogram", ErrorSeverity.Invalid
            )

        # 1. get the metadata schema based on the mspass object type
        if isinstance(mspass_object, TimeSeries):
            metadata_def = self.metadata_schema.TimeSeries
        else:
            metadata_def = self.metadata_schema.Seismogram

        wf_collection = metadata_def.collection("_id")
        object_type = self.database_schema[wf_collection].data_type()
        if object_type not in [TimeSeries, Seismogram]:
            raise MsPASSError(
                "only TimeSeries and Seismogram are supported, but {} is requested. Please check the data_type of {} collection.".format(
                    object_type, wf_collection
                ),
                "Fatal",
            )
        wf_collection_metadata_schema = self.metadata_schema[object_type.__name__]

        collection_id = collection + "_id"
        # 2. get the collection_id from the current mspass_object
        if not mspass_object.is_defined(collection_id):
            raise MsPASSError(
                "no {} in the mspass object".format(collection_id),
                ErrorSeverity.Invalid,
            )
        object_doc_id = mspass_object[collection_id]

        # 3. find the unique document associated with this source id in the source collection
        object_doc = self[collection].find_one({"_id": object_doc_id})
        if object_doc == None:
            raise MsPASSError(
                "no match found in {} collection for source_id = {}".format(
                    collection, object_doc_id
                ),
                ErrorSeverity.Invalid,
            )

        # 4. use this document to update the mspass object
        key_dict = set()
        for k in wf_collection_metadata_schema.keys():
            col = wf_collection_metadata_schema.collection(k)
            if col == collection:
                if k not in exclude_keys and not include_undefined:
                    key_dict.add(self.database_schema[col].unique_name(k))
                    mspass_object.put(
                        k, object_doc[self.database_schema[col].unique_name(k)]
                    )

        # 5. add extra keys if include_undefined is true
        if include_undefined:
            for k in object_doc:
                if k not in key_dict:
                    mspass_object.put(k, object_doc[k])

    def load_source_metadata(
        self,
        mspass_object,
        exclude_keys=["serialized_event", "magnitude_type"],
        include_undefined=False,
    ):
        """
        Reads metadata from the source collection and loads standard attributes in source collection to the data passed as mspass_object.
        The method will only work if mspass_object has the source_id attribute set to link it to a unique document in source.

        Note the mspass_object can be either an atomic object (TimeSeries or Seismogram) with a Metadata container base class
        or an ensemble (TimeSeriesEnsemble or SeismogramEnsemble).
        Ensembles will have the source data posted to the ensemble Metadata and not the members.
        This should be the stock way to assemble the generalization of a shot gather.

        :param mspass_object:   data where the metadata is to be loaded
        :type mspass_object:  :class:`mspasspy.ccore.seismic.TimeSeries`,
            :class:`mspasspy.ccore.seismic.Seismogram`,
            :class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` or
            :class:`mspasspy.ccore.seismic.SeismogramEnsemble`.
        :param exclude_keys: list of attributes that should not normally be loaded.
            Default are attributes not normally need that are loaded from QuakeML.  Ignored if include_undefined is set ``True``.
        :type exclude_keys: a :class:`list` of :class:`str`
        :param include_undefined:  when ``True`` all data in the matching source document are loaded.

        :raises mspasspy.ccore.utility.MsPASSError: any detected errors will cause a MsPASSError to be thrown
        """
        if isinstance(mspass_object, (TimeSeries, Seismogram)):
            self._load_collection_metadata(
                mspass_object, exclude_keys, include_undefined, "source"
            )
        if isinstance(mspass_object, (TimeSeriesEnsemble, SeismogramEnsemble)):
            for member_object in mspass_object.member:
                self._load_collection_metadata(
                    member_object, exclude_keys, include_undefined, "source"
                )

    def load_site_metadata(
        self, mspass_object, exclude_keys=None, include_undefined=False
    ):
        """
        Reads metadata from the site collection and loads standard attributes in site collection to the data passed as mspass_object.
        The method will only work if mspass_object has the site_id attribute set to link it to a unique document in source.

        Note the mspass_object can be either an atomic object (TimeSeries or Seismogram) with a Metadata container base class or an ensemble (TimeSeriesEnsemble
        or SeismogramEnsemble).
        Ensembles will have the site data posted to the ensemble Metadata and not the members.
        This should be the stock way to assemble the generalization of a common-receiver gather.

        :param mspass_object:   data where the metadata is to be loaded
        :type mspass_object:  :class:`mspasspy.ccore.seismic.TimeSeries`,
            :class:`mspasspy.ccore.seismic.Seismogram`,
            :class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` or
            :class:`mspasspy.ccore.seismic.SeismogramEnsemble`.
        :param exclude_keys: list of attributes that should not normally be loaded.
            Default is None.  Ignored if include_undefined is set ``True``.
        :type exclude_keys: a :class:`list` of :class:`str`
        :param include_undefined:  when ``True`` all data in the matching site document are loaded.

        :raises mspasspy.ccore.utility.MsPASSError: any detected errors will cause a MsPASSError to be thrown
        """
        if exclude_keys is None:
            exclude_keys = []
        if isinstance(mspass_object, (TimeSeries, Seismogram)):
            self._load_collection_metadata(
                mspass_object, exclude_keys, include_undefined, "site"
            )
        if isinstance(mspass_object, (TimeSeriesEnsemble, SeismogramEnsemble)):
            for member_object in mspass_object.member:
                self._load_collection_metadata(
                    member_object, exclude_keys, include_undefined, "site"
                )

    def load_channel_metadata(
        self,
        mspass_object,
        exclude_keys=["serialized_channel_data"],
        include_undefined=False,
    ):
        """
        Reads metadata from the channel collection and loads standard attributes in channel collection to the data passed as mspass_object.
        The method will only work if mspass_object has the site_id attribute set to link it to a unique document in source.

        Note the mspass_object can be either an atomic object (TimeSeries or Seismogram) with a Metadata container base class or an ensemble (TimeSeriesEnsemble
        or SeismogramEnsemble).
        Ensembles will have the site data posted to the ensemble Metadata and not the members.
        This should be the stock way to assemble the generalization of a common-receiver gather of TimeSeries data for a common sensor component.

        :param mspass_object:   data where the metadata is to be loaded
        :type mspass_object:  :class:`mspasspy.ccore.seismic.TimeSeries`,
            :class:`mspasspy.ccore.seismic.Seismogram`,
            :class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` or
            :class:`mspasspy.ccore.seismic.SeismogramEnsemble`.
        :param exclude_keys: list of attributes that should not normally be loaded.
            Default excludes the serialized obspy class that is used to store response data.   Ignored if include_undefined is set ``True``.
        :param include_undefined:  when ``True`` all data in the matching channel document are loaded

        :raises mspasspy.ccore.utility.MsPASSError: any detected errors will cause a MsPASSError to be thrown
        """
        if isinstance(mspass_object, (TimeSeries, Seismogram)):
            self._load_collection_metadata(
                mspass_object, exclude_keys, include_undefined, "channel"
            )
        if isinstance(mspass_object, (TimeSeriesEnsemble, SeismogramEnsemble)):
            for member_object in mspass_object.member:
                self._load_collection_metadata(
                    member_object, exclude_keys, include_undefined, "channel"
                )

    @staticmethod
    def _sync_metadata_before_update(mspass_object):
        """
        MsPASS data objects are designed to cleanly handle what we call relative
        and UTC time.  This small helper function assures the Metadata of
        mspass_object are consistent with the internal contents.  That
        involves posting some special attributes seen below to handle this issue.
        Since Metadata is volatile we need to be sure these are consistent or
        timing can be destroyed on data.
        """
        # this adds a small overhead but it guarantees Metadata and internal t0
        # values are consistent.  Shouldn't happen unless the user messes with them
        # incorrectly, but this safety is prudent to reduce the odds of mysterious
        # timing errors in data
        t0 = mspass_object.t0
        mspass_object.set_t0(t0)
        # This will need to be modified if we ever expand time types beyond two
        if mspass_object.time_is_relative():
            if mspass_object.shifted():
                mspass_object["startime_shift"] = mspass_object.time_reference()
                mspass_object["utc_convertible"] = True
            else:
                mspass_object["utc_convertible"] = False
            mspass_object["time_standard"] = "Relative"
        else:
            mspass_object["utc_convertible"] = True
            mspass_object["time_standard"] = "UTC"
        # If it is a seismogram, we need to update the tmatrix in the metadata to be consistent with the internal tmatrix
        if isinstance(mspass_object, Seismogram):
            t_matrix = mspass_object.tmatrix
            mspass_object.tmatrix = t_matrix
            # also update the cardinal and orthogonal attributes
            mspass_object["cardinal"] = mspass_object.cardinal()
            mspass_object["orthogonal"] = mspass_object.orthogonal()

    def _save_history(self, mspass_object, alg_name=None, alg_id=None, collection=None):
        """
        Save the processing history of a mspasspy object.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param prev_history_object_id: the previous history object id (if it has).
        :type prev_history_object_id: :class:`bson.objectid.ObjectId`
        :param collection: the collection that you want to store the history object. If not specified, use the defined
        collection in the schema.
        :return: current history_object_id.
        """
        if isinstance(mspass_object, TimeSeries):
            update_metadata_def = self.metadata_schema.TimeSeries
            atomic_type = AtomicType.TIMESERIES
        elif isinstance(mspass_object, Seismogram):
            update_metadata_def = self.metadata_schema.Seismogram
            atomic_type = AtomicType.SEISMOGRAM
        else:
            raise TypeError("only TimeSeries and Seismogram are supported")
        # get the wf id name in the schema
        wf_id_name = update_metadata_def.collection("_id") + "_id"

        # get the wf id in the mspass object
        oid = None
        if "_id" in mspass_object:
            oid = mspass_object["_id"]

        if not collection:
            collection = self.database_schema.default_name("history_object")
        history_col = self[collection]

        proc_history = ProcessingHistory(mspass_object)
        current_uuid = proc_history.id()  # uuid in the current node
        current_nodedata = proc_history.current_nodedata()
        # get the alg_name and alg_id of current node
        if not alg_id:
            alg_id = current_nodedata.algid
        if not alg_name:
            alg_name = current_nodedata.algorithm

        history_binary = pickle.dumps(proc_history)
        # todo save jobname jobid when global history module is done
        try:
            # construct the insert dict for saving into database
            insert_dict = {
                "_id": current_uuid,
                "processing_history": history_binary,
                "alg_id": alg_id,
                "alg_name": alg_name,
            }
            if oid:
                insert_dict[wf_id_name] = oid
            # insert new one
            history_col.insert_one(insert_dict)
        except pymongo.errors.DuplicateKeyError as e:
            raise MsPASSError(
                "The history object to be saved has a duplicate uuid", "Fatal"
            ) from e

        # clear the history chain of the mspass object
        mspass_object.clear_history()
        # set_as_origin with uuid set to the newly generated id
        mspass_object.set_as_origin(alg_name, alg_id, current_uuid, atomic_type)

        return current_uuid

    def _load_history(
        self,
        mspass_object,
        history_object_id,
        alg_name=None,
        alg_id=None,
        define_as_raw=False,
        collection=None,
        retrieve_history_record=False,
    ):
        """
        Load (in place) the processing history into a mspasspy object.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param history_object_id: :class:`bson.objectid.ObjectId`
        :param collection: the collection that you want to load the processing history. If not specified, use the defined
        collection in the schema.
        """
        # get the atomic type of the mspass object
        if isinstance(mspass_object, TimeSeries):
            atomic_type = AtomicType.TIMESERIES
        else:
            atomic_type = AtomicType.SEISMOGRAM
        if not collection:
            collection = self.database_schema.default_name("history_object")
        # load history if set True
        res = self[collection].find_one({"_id": history_object_id})
        # retrieve_history_record
        if retrieve_history_record:
            mspass_object.load_history(pickle.loads(res["processing_history"]))
        else:
            # set the associated history_object_id as the uuid of the origin
            if not alg_name:
                alg_name = "0"
            if not alg_id:
                alg_id = "0"
            mspass_object.set_as_origin(
                alg_name, alg_id, history_object_id, atomic_type, define_as_raw
            )

    def _save_elog(self, mspass_object, elog_id=None, collection=None):
        """
        Save error log for a data object. Data objects in MsPASS contain an error log object used to post any
        errors handled by processing functions. This function will delete the old elog entry if `elog_id` is given.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param elog_id: the previous elog object id to be appended with.
        :type elog_id: :class:`bson.objectid.ObjectId`
        :param collection: the collection that you want to save the elogs. If not specified, use the defined
        collection in the schema.
        :return: updated elog_id.
        """
        if isinstance(mspass_object, TimeSeries):
            update_metadata_def = self.metadata_schema.TimeSeries
        elif isinstance(mspass_object, Seismogram):
            update_metadata_def = self.metadata_schema.Seismogram
        else:
            raise TypeError("only TimeSeries and Seismogram are supported")
        wf_id_name = update_metadata_def.collection("_id") + "_id"

        if not collection:
            collection = self.database_schema.default_name("elog")

        # TODO: Need to discuss whether the _id should be linked in a dead elog entry. It
        # might be confusing to link the dead elog to an alive wf record.
        oid = None
        if "_id" in mspass_object:
            oid = mspass_object["_id"]

        elog = mspass_object.elog
        n = elog.size()
        if n != 0:
            logdata = []
            docentry = {"logdata": logdata}
            errs = elog.get_error_log()
            jobid = elog.get_job_id()
            for x in errs:
                logdata.append(
                    {
                        "job_id": jobid,
                        "algorithm": x.algorithm,
                        "badness": str(x.badness),
                        "error_message": x.message,
                        "process_id": x.p_id,
                    }
                )
            if oid:
                docentry[wf_id_name] = oid

            if not mspass_object.live:
                docentry["tombstone"] = dict(mspass_object)

            if elog_id:
                # append elog
                elog_doc = self[collection].find_one({"_id": elog_id})
                # only append when previous elog exists
                if elog_doc:
                    # if the same object was updated twice, the elog entry will be duplicated
                    # the following list comprehension line removes the duplicates and preserves
                    # the order. May need some practice to see if such a behavior makes sense.
                    [
                        elog_doc["logdata"].append(x)
                        for x in logdata
                        if x not in elog_doc["logdata"]
                    ]
                    docentry["logdata"] = elog_doc["logdata"]
                    self[collection].delete_one({"_id": elog_id})
                # note that is should be impossible for the old elog to have tombstone entry
                # so we ignore the handling of that attribute here.
                ret_elog_id = self[collection].insert_one(docentry).inserted_id
            else:
                # new insertion
                ret_elog_id = self[collection].insert_one(docentry).inserted_id
            return ret_elog_id

    @staticmethod
    def _read_data_from_dfile(
        mspass_object,
        dir,
        dfile,
        foff,
        nbytes=0,
        format=None,
        merge_method=0,
        merge_fill_value=None,
        merge_interpolation_samples=0,
    ):
        """
        Read the stored data from a file and loads it into a mspasspy object.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param dir: file directory.
        :type dir: :class:`str`
        :param dfile: file name.
        :type dfile: :class:`str`
        :param foff: offset that marks the starting of the data in the file.
        :param nbytes: number of bytes to be read from the offset. This is only used when ``format`` is given.
        :param format: the format of the file. This can be one of the `supported formats <https://docs.obspy.org/packages/autogen/obspy.core.stream.read.html#supported-formats>`__ of ObsPy writer. By default (``None``), the format will be the binary waveform.
        :type format: :class:`str`
        :param fill_value: Fill value for gaps. Defaults to None. Traces will be converted to NumPy masked arrays if no value is given and gaps are present.
        :type fill_value: :class:`int`, :class:`float` or None
        """
        if not isinstance(mspass_object, (TimeSeries, Seismogram)):
            raise TypeError("only TimeSeries and Seismogram are supported")

        if not format:
            if isinstance(mspass_object, TimeSeries):
                try:
                    count = _fread_from_file(mspass_object, dir, dfile, foff)
                    if count != mspass_object.npts:
                        message = "fread count mismatch.  Expected to read {npts} but fread returned a count of {count}".format(
                            npts=mspass_object.npts, count=count
                        )
                        mspass_object.elog.log_error(
                            "_read_data_from_dfile", message, ErrorSeverity.Complaint
                        )
                except MsPASSError as merr:
                    # Errors thrown must always cause a failure
                    raise MsPASSError(
                        "Error while read data from files.", "Fatal"
                    ) from merr
            else:
                # We can only get here if this is a Seismogram
                try:
                    nsamples = 3 * mspass_object.npts
                    count = _fread_from_file(mspass_object, dir, dfile, foff)
                    if count != nsamples:
                        message = "fread count mismatch.  Expected to read {nsamples} doubles but fread returned a count of {count}".format(
                            nsamples=nsamples, count=count
                        )
                        mspass_object.elog.log_error(
                            "_read_data_from_dfile", message, ErrorSeverity.Complaint
                        )
                except MsPASSError as merr:
                    # Errors thrown must always cause a failure
                    raise MsPASSError(
                        "Error while read data from files.", "Fatal"
                    ) from merr

        else:
            fname = os.path.join(dir, dfile)
            with open(fname, mode="rb") as fh:
                if foff > 0:
                    fh.seek(foff)
                flh = io.BytesIO(fh.read(nbytes))
                st = obspy.read(flh, format=format)
                if isinstance(mspass_object, TimeSeries):
                    # st is a "stream" but it may contains multiple Trace objects gaps
                    # but here we want only one TimeSeries, we merge these Trace objects and fill values for gaps
                    # we post a complaint elog entry to the mspass_object if there are gaps in the stream
                    if len(st) > 1:
                        mspass_object.elog.log_error(
                            "read_data",
                            "There are gaps in this stream when reading file by obspy and they are merged into one Trace object by filling value in the gaps.",
                            ErrorSeverity.Complaint,
                        )
                        st = st.merge(
                            method=merge_method,
                            fill_value=merge_fill_value,
                            interpolation_samples=merge_interpolation_samples,
                        )
                    tr = st[0]
                    # These two lines are needed to properly initialize
                    # the DoubleVector before calling Trace2TimeSeries
                    tr_data = tr.data.astype(
                        "float64"
                    )  #   Convert the nparray type to double, to match the DoubleVector
                    mspass_object.npts = len(tr_data)
                    mspass_object.data = DoubleVector(tr_data)
                    mspass_object = Trace2TimeSeries(tr)
                elif isinstance(mspass_object, Seismogram):
                    # This was previous form.   The toSeismogram run as a
                    # method is an unnecessary confusion and I don't think
                    # settign npts or data are necessary given the code of
                    # Stream2Seismogram - st.toSeismogram is an alias for that
                    # This is almost but not quite equivalent to this:
                    # mspass_object = Stream2Seismogram(st,cardinal=True)
                    # Seems Stream2Seismogram does not properly handle
                    # the data pointer
                    sm = st.toSeismogram(cardinal=True)
                    mspass_object.npts = sm.data.columns()
                    mspass_object.data = sm.data

    @staticmethod
    def _save_data_to_dfile(
        mspass_object, dir, dfile, format=None, kill_on_failure=False
    ):
        """
        This is a private method used under the hood to save the sample data
        of atomic MsPASS data objects.  How the save happens is highly
        dependent upon the format argument.  When None, which is the
        default, the data are written to the file specified by dir and dfile
        using low-level C fwrite calls.  The function used always appends
        data to eof if the file already exists to allow accumation of data
        in "gather" files to reduce file name overhead in hpc systems.
        If format is anything else the function attempts to use one of
        the obspy formatted writers.  That means the format string must be
        one of the `supported formats <https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.write.html#supported-formats>`__ of ObsPy reader.


        Writing to a file can fail for any number of reasons.   write errors
        are trapped internally in this function any errors posted to elog of
        mspass_object.   If the kill_on_failure boolean is set true the
        function will call the kill method of the data function and the
        that datum will be marked dead.   That is not normally a good idea
        if there is any additional work to do on the data so the default
        for that parameter is false.

        Because we use a stream model for file storage be aware
        that insertion and deletion in a file are not possible.  If you
        need lots of editing functionality with files you should use the
        one file to one object model or (better yet) use gridfs storage.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param dir: file directory.
        :type dir: :class:`str`
        :param dfile: file name.
        :type dfile: :class:`str`
        :param format: the format of the file. This can be one of the `supported formats <https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.write.html#supported-formats>`__ of ObsPy reader. By default (``None``), the format will be the binary waveform.
        :type format: :class:`str`
        :param kill_on_failure:  When true if an io error occurs the data object's
          kill method will be invoked.  When false (the default) io errors are
          logged and left set live.  (Note data already marked dead are return
          are ignored by this function. )
        :type kill_on_failure: boolean
        :return: Position of first data sample (foff) and the size of the saved chunk.
           If the input is flagged as dead return (-1,0)
        """
        if not isinstance(mspass_object, (TimeSeries, Seismogram)):
            raise TypeError("only TimeSeries and Seismogram are supported")
        if mspass_object.dead():
            return -1, 0

        if not format:
            try:
                # This function has overloading.  this might not work
                foff = _fwrite_to_file(mspass_object, dir, dfile)
            except MsPASSError as merr:
                mspass_object.elog.log_error(merr)
                if kill_on_failure:
                    mspass_object.kill()
            # we can compute the number of bytes written for this case
            if isinstance(mspass_object, TimeSeries):
                nbytes_written = 8 * mspass_object.npts
            else:
                # This can only be a Seismogram currently so this is not elif
                nbytes_written = 24 * mspass_object.npts
        else:
            fname = os.path.join(dir, dfile)
            os.makedirs(os.path.dirname(fname), exist_ok=True)
            with open(fname, mode="a+b") as fh:
                foff = fh.seek(0, 2)
                f_byte = io.BytesIO()
                if isinstance(mspass_object, TimeSeries):
                    mspass_object.toTrace().write(f_byte, format=format)
                elif isinstance(mspass_object, Seismogram):
                    mspass_object.toStream().write(f_byte, format=format)
                f_byte.seek(0)
                ub = f_byte.read()
                fh.write(ub)
                nbytes_written = len(ub)
        return foff, nbytes_written

    def _save_data_to_gridfs(self, mspass_object, gridfs_id=None):
        """
        Save a mspasspy object sample data to MongoDB grid file system. We recommend to use this method
        for saving a mspasspy object inside MongoDB.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param gridfs_id: if the data is already stored and you want to update it, you should provide the object id
        of the previous data, which will be deleted. A new document will be inserted instead.
        :type gridfs_id: :class:`bson.objectid.ObjectId`.
        :return inserted gridfs object id.
        """
        gfsh = gridfs.GridFS(self)
        if gridfs_id and gfsh.exists(gridfs_id):
            gfsh.delete(gridfs_id)
        if isinstance(mspass_object, Seismogram):
            ub = bytes(np.array(mspass_object.data).transpose())
        else:
            ub = bytes(mspass_object.data)
        return gfsh.put(ub)

    def _read_data_from_gridfs(self, mspass_object, gridfs_id):
        """
        Read data stored in gridfs and load it into a mspasspy object.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param gridfs_id: the object id of the data stored in gridfs.
        :type gridfs_id: :class:`bson.objectid.ObjectId`
        """
        gfsh = gridfs.GridFS(self)
        fh = gfsh.get(file_id=gridfs_id)
        if isinstance(mspass_object, TimeSeries):
            # fh.seek(16)
            float_array = array("d")
            if not mspass_object.is_defined("npts"):
                raise KeyError("npts is not defined")
            float_array.frombytes(fh.read(mspass_object.get("npts") * 8))
            mspass_object.data = DoubleVector(float_array)
        elif isinstance(mspass_object, Seismogram):
            if not mspass_object.is_defined("npts"):
                raise KeyError("npts is not defined")
            npts = mspass_object["npts"]
            np_arr = np.frombuffer(fh.read(npts * 8 * 3))
            file_size = fh.tell()
            if file_size != npts * 8 * 3:
                # Note we can only detect the cases where given npts is larger than
                # the number of points in the file
                emess = (
                    "Size mismatch in sample data. Number of points in gridfs file = %d but expected %d"
                    % (file_size / 8, (3 * mspass_object["npts"]))
                )
                raise ValueError(emess)
            np_arr = np_arr.reshape(npts, 3).transpose()
            mspass_object.data = dmatrix(np_arr)
        else:
            raise TypeError("only TimeSeries and Seismogram are supported")

    def _read_data_from_s3_continuous(
        self, mspass_object, aws_access_key_id=None, aws_secret_access_key=None
    ):
        """
        Read data stored in s3 and load it into a mspasspy object.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        """
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        BUCKET_NAME = "scedc-pds"
        year = mspass_object["year"]
        day_of_year = mspass_object["day_of_year"]
        network = ""
        station = ""
        channel = ""
        location = ""
        if "net" in mspass_object:
            network = mspass_object["net"]
        if "sta" in mspass_object:
            station = mspass_object["sta"]
        if "chan" in mspass_object:
            channel = mspass_object["chan"]
        if "loc" in mspass_object:
            location = mspass_object["loc"]
        KEY = "continuous_waveforms/" + year + "/" + year + "_" + day_of_year + "/"
        if len(network) < 2:
            network += "_" * (2 - len(network))
        if len(station) < 5:
            station += "_" * (5 - len(station))
        if len(channel) < 3:
            channel += "_" * (3 - len(channel))
        if len(location) < 2:
            location += "_" * (2 - len(location))

        mseed_file = (
            network + station + channel + location + "_" + year + day_of_year + ".ms"
        )
        KEY += mseed_file

        try:
            obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=KEY)
            st = obspy.read(
                io.BytesIO(obj["Body"].read()), format=mspass_object["format"]
            )
            st.merge()
            if isinstance(mspass_object, TimeSeries):
                # st is a "stream" but it only has one member here because we are
                # reading single net,sta,chan,loc grouping defined by the index
                # We only want the Trace object not the stream to convert
                tr = st[0]
                # Now we convert this to a TimeSeries and load other Metadata
                # Note the exclusion copy and the test verifying net,sta,chan,
                # loc, and startime all match
                tr_data = tr.data.astype(
                    "float64"
                )  #   Convert the nparray type to double, to match the DoubleVector
                mspass_object.npts = len(tr_data)
                mspass_object.data = DoubleVector(tr_data)
            elif isinstance(mspass_object, Seismogram):
                sm = st.toSeismogram(cardinal=True)
                mspass_object.npts = sm.data.columns()
                mspass_object.data = sm.data

        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                # the object does not exist
                print(
                    "Could not find the object by the KEY: {} from the BUCKET: {} in s3"
                ).format(KEY, BUCKET_NAME)
            else:
                raise

        except Exception as e:
            raise MsPASSError("Error while read data from s3.", "Fatal") from e

    def _read_data_from_s3_lambda(
        self, mspass_object, aws_access_key_id=None, aws_secret_access_key=None
    ):
        year = mspass_object["year"]
        day_of_year = mspass_object["day_of_year"]
        network = ""
        station = ""
        channel = ""
        location = ""
        if "net" in mspass_object:
            network = mspass_object["net"]
        if "sta" in mspass_object:
            station = mspass_object["sta"]
        if "chan" in mspass_object:
            channel = mspass_object["chan"]
        if "loc" in mspass_object:
            location = mspass_object["loc"]

        try:
            st = self._download_windowed_mseed_file(
                aws_access_key_id,
                aws_secret_access_key,
                year,
                day_of_year,
                network,
                station,
                channel,
                location,
                2,
            )
            if isinstance(mspass_object, TimeSeries):
                # st is a "stream" but it only has one member here because we are
                # reading single net,sta,chan,loc grouping defined by the index
                # We only want the Trace object not the stream to convert
                tr = st[0]
                # Now we convert this to a TimeSeries and load other Metadata
                # Note the exclusion copy and the test verifying net,sta,chan,
                # loc, and startime all match
                tr_data = tr.data.astype(
                    "float64"
                )  #   Convert the nparray type to double, to match the DoubleVector
                mspass_object.npts = len(tr_data)
                mspass_object.data = DoubleVector(tr_data)
            elif isinstance(mspass_object, Seismogram):
                sm = st.toSeismogram(cardinal=True)
                mspass_object.npts = sm.data.columns()
                mspass_object.data = sm.data

        except Exception as e:
            raise MsPASSError("Error while read data from s3_lambda.", "Fatal") from e

    def _read_data_from_s3_event(
        self,
        mspass_object,
        dir,
        dfile,
        foff,
        nbytes=0,
        format=None,
        aws_access_key_id=None,
        aws_secret_access_key=None,
    ):
        """
        Read the stored data from a file and loads it into a mspasspy object.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param dir: file directory.
        :type dir: :class:`str`
        :param dfile: file name.
        :type dfile: :class:`str`
        :param foff: offset that marks the starting of the data in the file.
        :param nbytes: number of bytes to be read from the offset. This is only used when ``format`` is given.
        :param format: the format of the file. This can be one of the `supported formats <https://docs.obspy.org/packages/autogen/obspy.core.stream.read.html#supported-formats>`__ of ObsPy writer. By default (``None``), the format will be the binary waveform.
        :type format: :class:`str`
        """
        if not isinstance(mspass_object, (TimeSeries, Seismogram)):
            raise TypeError("only TimeSeries and Seismogram are supported")
        fname = os.path.join(dir, dfile)

        # check if fname exists
        if not os.path.exists(fname):
            # fname might now exist, but could download from s3
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
            BUCKET_NAME = "scedc-pds"
            year = mspass_object["year"]
            day_of_year = mspass_object["day_of_year"]
            filename = mspass_object["filename"]
            KEY = (
                "event_waveforms/"
                + year
                + "/"
                + year
                + "_"
                + day_of_year
                + "/"
                + filename
                + ".ms"
            )
            # try to download the mseed file from s3 and save it locally
            try:
                obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=KEY)
                mseed_content = obj["Body"].read()
                # temporarily write data into a file
                with open(fname, "wb") as f:
                    f.write(mseed_content)

            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    # the object does not exist
                    print(
                        "Could not find the object by the KEY: {} from the BUCKET: {} in s3"
                    ).format(KEY, BUCKET_NAME)
                else:
                    raise

            except Exception as e:
                raise MsPASSError("Error while read data from s3.", "Fatal") from e

        with open(fname, mode="rb") as fh:
            fh.seek(foff)
            flh = io.BytesIO(fh.read(nbytes))
            st = obspy.read(flh, format=format)
            # there could be more than 1 trace object in the stream, merge the traces
            st.merge()
            if isinstance(mspass_object, TimeSeries):
                tr = st[0]
                tr_data = tr.data.astype(
                    "float64"
                )  #   Convert the nparray type to double, to match the DoubleVector
                mspass_object.npts = len(tr_data)
                mspass_object.data = DoubleVector(tr_data)
            elif isinstance(mspass_object, Seismogram):
                sm = st.toSeismogram(cardinal=True)
                mspass_object.npts = sm.data.columns()
                mspass_object.data = sm.data

    def _read_data_from_fdsn(self, mspass_object):
        provider = mspass_object["provider"]
        year = mspass_object["year"]
        day_of_year = mspass_object["day_of_year"]
        network = mspass_object["net"]
        station = mspass_object["sta"]
        channel = mspass_object["chan"]
        location = ""
        if "loc" in mspass_object:
            location = mspass_object["loc"]

        client = Client(provider)
        t = UTCDateTime(year + day_of_year, iso8601=True)
        st = client.get_waveforms(
            network, station, location, channel, t, t + 60 * 60 * 24
        )
        # there could be more than 1 trace object in the stream, merge the traces
        st.merge()

        if isinstance(mspass_object, TimeSeries):
            tr = st[0]
            tr_data = tr.data.astype(
                "float64"
            )  #   Convert the nparray type to double, to match the DoubleVector
            mspass_object.npts = len(tr_data)
            mspass_object.data = DoubleVector(tr_data)
        elif isinstance(mspass_object, Seismogram):
            sm = st.toSeismogram(cardinal=True)
            mspass_object.npts = sm.data.columns()
            mspass_object.data = sm.data

    @staticmethod
    def _read_data_from_url(mspass_object, url, format=None):
        """
        Read a file from url and loads it into a mspasspy object.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param url: the url that points to a :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`.
        :type url: :class:`str`
        :param format: the format of the file. This can be one of the `supported formats <https://docs.obspy.org/packages/autogen/obspy.core.stream.read.html#supported-formats>`__ of ObsPy reader. If not specified, the ObsPy reader will try to detect the format automatically.
        :type format: :class:`str`
        """
        try:
            response = urllib.request.urlopen(url)
            flh = io.BytesIO(response.read())
        # Catch HTTP errors.
        except Exception as e:
            raise MsPASSError("Error while downloading: %s" % url, "Fatal") from e

        st = obspy.read(flh, format=format)
        if isinstance(mspass_object, TimeSeries):
            # st is a "stream" but it only has one member here because we are
            # reading single net,sta,chan,loc grouping defined by the index
            # We only want the Trace object not the stream to convert
            tr = st[0]
            # Now we convert this to a TimeSeries and load other Metadata
            # Note the exclusion copy and the test verifying net,sta,chan,
            # loc, and startime all match
            tr_data = tr.data.astype(
                "float64"
            )  #   Convert the nparray type to double, to match the DoubleVector
            mspass_object.npts = len(tr_data)
            mspass_object.data = DoubleVector(tr_data)
        elif isinstance(mspass_object, Seismogram):
            # Note that the following convertion could be problematic because
            # it assumes there are three traces in the file, and they are in
            # the order of E, N, Z.
            sm = st.toSeismogram(cardinal=True)
            mspass_object.npts = sm.data.columns()
            mspass_object.data = sm.data
        else:
            raise TypeError("only TimeSeries and Seismogram are supported")

    @staticmethod
    def _extract_locdata(chanlist):
        """
        Parses the list returned by obspy channels attribute
        for a Station object and returns a dict of unique
        edepth values keyed by loc code.  This algorithm
        would be horribly inefficient for large lists with
        many duplicates, but the assumption here is the list
        will always be small
        """
        alllocs = {}
        for chan in chanlist:
            alllocs[chan.location_code] = [
                chan.start_date,
                chan.end_date,
                chan.latitude,
                chan.longitude,
                chan.elevation,
                chan.depth,
            ]
        return alllocs

    def _site_is_not_in_db(self, record_to_test):
        """
        Small helper functoin for save_inventory.
        Tests if dict content of record_to_test is
        in the site collection.  Inverted logic in one sense
        as it returns true when the record is not yet in
        the database.  Uses key of net,sta,loc,starttime
        and endtime.  All tests are simple equality.
        Should be ok for times as stationxml uses nearest
        day as in css3.0.

        originally tried to do the time interval tests with a
        query, but found it was a bit cumbersone to say the least.
        Because this particular query is never expected to return
        a large number of documents we resort to a linear
        search through all matches on net,sta,loc rather than
        using a confusing and ugly query construct.
        """
        dbsite = self.site
        queryrecord = {}
        queryrecord["net"] = record_to_test["net"]
        queryrecord["sta"] = record_to_test["sta"]
        queryrecord["loc"] = record_to_test["loc"]
        matches = dbsite.find(queryrecord)
        # this returns a warning that count is depricated but
        # I'm getting confusing results from google search on the
        # topic so will use this for now
        nrec = dbsite.count_documents(queryrecord)
        if nrec <= 0:
            return True
        else:
            # Now do the linear search on time for a match
            st0 = record_to_test["starttime"]
            et0 = record_to_test["endtime"]
            time_fudge_factor = 10.0
            stp = st0 + time_fudge_factor
            stm = st0 - time_fudge_factor
            etp = et0 + time_fudge_factor
            etm = et0 - time_fudge_factor
            for x in matches:
                sttest = x["starttime"]
                ettest = x["endtime"]
                if sttest > stm and sttest < stp and ettest > etm and ettest < etp:
                    return False
            return True

    def _channel_is_not_in_db(self, record_to_test):
        """
        Small helper functoin for save_inventory.
        Tests if dict content of record_to_test is
        in the site collection.  Inverted logic in one sense
        as it returns true when the record is not yet in
        the database.  Uses key of net,sta,loc,starttime
        and endtime.  All tests are simple equality.
        Should be ok for times as stationxml uses nearest
        day as in css3.0.
        """
        dbchannel = self.channel
        queryrecord = {}
        queryrecord["net"] = record_to_test["net"]
        queryrecord["sta"] = record_to_test["sta"]
        queryrecord["loc"] = record_to_test["loc"]
        queryrecord["chan"] = record_to_test["chan"]
        matches = dbchannel.find(queryrecord)
        # this returns a warning that count is depricated but
        # I'm getting confusing results from google search on the
        # topic so will use this for now
        nrec = dbchannel.count_documents(queryrecord)
        if nrec <= 0:
            return True
        else:
            # Now do the linear search on time for a match
            st0 = record_to_test["starttime"]
            et0 = record_to_test["endtime"]
            time_fudge_factor = 10.0
            stp = st0 + time_fudge_factor
            stm = st0 - time_fudge_factor
            etp = et0 + time_fudge_factor
            etm = et0 - time_fudge_factor
            for x in matches:
                sttest = x["starttime"]
                ettest = x["endtime"]
                if sttest > stm and sttest < stp and ettest > etm and ettest < etp:
                    return False
            return True

    def _handle_null_starttime(self, t):
        if t == None:
            return UTCDateTime(0.0)
        else:
            return t

    def _handle_null_endtime(self, t):
        # This constant is used below to set endtime to a time
        # in the far future if it is null
        DISTANTFUTURE = UTCDateTime(2051, 1, 1, 0, 0)
        if t == None:
            return DISTANTFUTURE
        else:
            return t

    def save_inventory(self, inv, networks_to_exclude=["SY"], verbose=False):
        """
        Saves contents of all components of an obspy inventory
        object to documents in the site and channel collections.
        The site collection is sufficient for Seismogram objects but
        TimeSeries data will normally need to be connected to the
        channel collection.   The algorithm used will not add
        duplicates based on the following keys:

        For site:
            net
            sta
            chan
            loc
            starttime::endtime - this check is done cautiously with
              a 10 s fudge factor to avoid the issue of floating point
              equal tests.   Probably overly paranoid since these
              fields are normally rounded to a time at the beginning
              of a utc day, but small cost to pay for stabilty because
              this function is not expected to be run millions of times
              on a huge collection.

        for channels:
            net
            sta
            chan
            loc
            starttime::endtime - same approach as for site with same
               issues - note especially 10 s fudge factor.   This is
               necessary because channel metadata can change more
               frequently than site metadata (e.g. with a sensor
               orientation or sensor swap)

        The channel collection can contain full response data
        that can be obtained by extracting the data with the key
        "serialized_inventory" and running pickle loads on the returned
        string.

        A final point of note is that not all Inventory objects are created
        equally.   Inventory objects appear to us to be designed as an image
        of stationxml data.  The problem is that stationxml, like SEED, has to
        support a lot of complexity faced by data centers that end users
        like those using this package do not need or want to know.   The
        point is this method tries to untangle the complexity and aims to reduce the
        result to a set of documents in the site and channel collection
        that can be cross referenced to link the right metadata with all
        waveforms in a dataset.

        :param inv: is the obspy Inventory object of station data to save.
        :networks_to_exclude: should contain a list (or tuple) of
            SEED 2 byte network codes that are to be ignored in
            processing.   Default is SY which is used for synthetics.
            Set to None if if all are to be loaded.
        :verbose:  print informational lines if true.  If false
        works silently)

        :return:  tuple with
          0 - integer number of site documents saved
          1 -integer number of channel documents saved
          2 - number of distinct site (net,sta,loc) items processed
          3 - number of distinct channel items processed
        :rtype: tuple
        """

        # site is a frozen name for the collection here.  Perhaps
        # should be a variable with a default
        # to do: need to change source_id to be a copy of the _id string.

        dbcol = self.site
        dbchannel = self.channel
        n_site_saved = 0
        n_chan_saved = 0
        n_site_processed = 0
        n_chan_processed = 0
        for x in inv:
            # Inventory object I got from webservice download
            # makes the sta variable here a net:sta combination
            # We can get the net code like this
            net = x.code
            # This adds feature to skip data for any net code
            # listed in networks_to_exclude
            if networks_to_exclude != None:
                if net in networks_to_exclude:
                    continue
            # Each x now has a station field, BUT tests I ran
            # say for my example that field has one entry per
            # x.  Hence, we can get sta name like this
            stalist = x.stations
            for station in stalist:
                sta = station.code
                starttime = station.start_date
                endtime = station.end_date
                starttime = self._handle_null_starttime(starttime)
                endtime = self._handle_null_endtime(endtime)
                latitude = station.latitude
                longitude = station.longitude
                # stationxml files seen to put elevation in m. We
                # always use km so need to convert
                elevation = station.elevation / 1000.0
                # an obnoxious property of station xml files obspy is giving me
                # is that the start_dates and end_dates on the net:sta section
                # are not always consistent with the channel data.  In particular
                # loc codes are a problem. So we pull the required metadata from
                # the chans data and will override locations and time ranges
                # in station section with channel data
                chans = station.channels
                locdata = self._extract_locdata(chans)
                # Assume loc code of 0 is same as rest
                # loc=_extract_loc_code(chanlist[0])
                # TODO Delete when sure we don't need to keep the full thing
                # picklestr = pickle.dumps(x)
                all_locs = locdata.keys()
                for loc in all_locs:
                    # If multiple loc codes are present on the second pass
                    # rec will contain the objectid of the document inserted
                    # in the previous pass - an obnoxious property of insert_one
                    # This initialization guarantees an empty container
                    rec = dict()
                    rec["loc"] = loc
                    rec["net"] = net
                    rec["sta"] = sta
                    lkey = loc
                    loc_tuple = locdata[lkey]
                    # We use these attributes linked to loc code rather than
                    # the station data - experience shows they are not
                    # consistent and we should use this set.
                    loc_lat = loc_tuple[2]
                    loc_lon = loc_tuple[3]
                    loc_elev = loc_tuple[4]
                    # for consistency convert this to km too
                    loc_elev = loc_elev / 1000.0
                    loc_edepth = loc_tuple[5]
                    loc_stime = loc_tuple[0]
                    loc_stime = self._handle_null_starttime(loc_stime)
                    loc_etime = loc_tuple[1]
                    loc_etime = self._handle_null_endtime(loc_etime)
                    rec["lat"] = loc_lat
                    rec["lon"] = loc_lon
                    # This is MongoDBs way to set a geographic
                    # point - allows spatial queries.  Note longitude
                    # must be first of the pair
                    rec["coords"] = [loc_lat, loc_lon]
                    rec["elev"] = loc_elev
                    rec["edepth"] = loc_edepth
                    rec["starttime"] = starttime.timestamp
                    rec["endtime"] = endtime.timestamp
                    if (
                        latitude != loc_lat
                        or longitude != loc_lon
                        or elevation != loc_elev
                    ):
                        print(
                            net,
                            ":",
                            sta,
                            ":",
                            loc,
                            " (Warning):  station section position is not consistent with loc code position",
                        )
                        print("Data in loc code section overrides station section")
                        print(
                            "Station section coordinates:  ",
                            latitude,
                            longitude,
                            elevation,
                        )
                        print(
                            "loc code section coordinates:  ",
                            loc_lat,
                            loc_lon,
                            loc_elev,
                        )
                    if self._site_is_not_in_db(rec):
                        result = dbcol.insert_one(rec)
                        # Note this sets site_id to an ObjectID for the insertion
                        # We use that to define a duplicate we tag as site_id
                        site_id = result.inserted_id
                        self.site.update_one(
                            {"_id": site_id}, {"$set": {"site_id": site_id}}
                        )
                        n_site_saved += 1
                        if verbose:
                            print(
                                "net:sta:loc=",
                                net,
                                ":",
                                sta,
                                ":",
                                loc,
                                "for time span ",
                                starttime,
                                " to ",
                                endtime,
                                " added to site collection",
                            )
                    else:
                        if verbose:
                            print(
                                "net:sta:loc=",
                                net,
                                ":",
                                sta,
                                ":",
                                loc,
                                "for time span ",
                                starttime,
                                " to ",
                                endtime,
                                " is already in site collection - ignored",
                            )
                    n_site_processed += 1
                    # done with site now handle channel
                    # Because many features are shared we can copy rec
                    # note this has to be a deep copy
                    chanrec = copy.deepcopy(rec)
                    # We don't want this baggage in the channel documents
                    # keep them only in the site collection
                    # del chanrec['serialized_inventory']
                    for chan in chans:
                        chanrec["chan"] = chan.code
                        # the Dip attribute in a stationxml file
                        # is like strike-dip and relative to horizontal
                        # line with positive down.  vang is the
                        # css30 attribute that is spherical coordinate
                        # theta angle
                        chanrec["vang"] = chan.dip + 90.0
                        chanrec["hang"] = chan.azimuth
                        chanrec["edepth"] = chan.depth
                        st = chan.start_date
                        et = chan.end_date
                        # as above be careful of null values for either end of the time range
                        st = self._handle_null_starttime(st)
                        et = self._handle_null_endtime(et)
                        chanrec["starttime"] = st.timestamp
                        chanrec["endtime"] = et.timestamp
                        n_chan_processed += 1
                        if self._channel_is_not_in_db(chanrec):
                            picklestr = pickle.dumps(chan)
                            chanrec["serialized_channel_data"] = picklestr
                            result = dbchannel.insert_one(chanrec)
                            # insert_one has an obnoxious behavior in that it
                            # inserts the ObjectId in chanrec.  In this loop
                            # we reuse chanrec so we have to delete the id field
                            # howeveer, we first want to update the record to
                            # have chan_id provide an  alternate key to that id
                            # object_id - that makes this consistent with site
                            # we actually use the return instead of pulling from
                            # chanrec
                            idobj = result.inserted_id
                            dbchannel.update_one(
                                {"_id": idobj}, {"$set": {"chan_id": idobj}}
                            )
                            del chanrec["_id"]
                            n_chan_saved += 1
                            if verbose:
                                print(
                                    "net:sta:loc:chan=",
                                    net,
                                    ":",
                                    sta,
                                    ":",
                                    loc,
                                    ":",
                                    chan.code,
                                    "for time span ",
                                    st,
                                    " to ",
                                    et,
                                    " added to channel collection",
                                )
                        else:
                            if verbose:
                                print(
                                    "net:sta:loc:chan=",
                                    net,
                                    ":",
                                    sta,
                                    ":",
                                    loc,
                                    ":",
                                    chan.code,
                                    "for time span ",
                                    st,
                                    " to ",
                                    et,
                                    " already in channel collection - ignored",
                                )

        # Tried this to create a geospatial index.   Failing
        # in later debugging for unknown reason.   Decided it
        # should be a done externally anyway as we don't use
        # that feature now - thought of doing so but realized
        # was unnecessary baggage
        # dbcol.create_index(["coords",GEOSPHERE])
        #
        # For now we will always print this summary information
        # For expected use it would be essential information
        #
        print("Database.save_inventory processing summary:")
        print("Number of site records processed=", n_site_processed)
        print("number of site records saved=", n_site_saved)
        print("number of channel records processed=", n_chan_processed)
        print("number of channel records saved=", n_chan_saved)
        return tuple([n_site_saved, n_chan_saved, n_site_processed, n_chan_processed])

    def read_inventory(self, net=None, sta=None, loc=None, time=None):
        """
        Loads an obspy inventory object limited by one or more
        keys.   Default is to load the entire contents of the
        site collection.   Note the load creates an obspy
        inventory object that is returned.  Use load_stations
        to return the raw data used to construct an Inventory.

        :param net:  network name query string.  Can be a single
        unique net code or use MongoDB's expression query
        mechanism (e.g. "{'$gt' : 42}).  Default is all
        :param sta: statoin name query string.  Can be a single
        station name or a MongoDB query expression.
        :param loc:  loc code to select.  Can be a single unique
        location (e.g. '01') or a MongoDB expression query.
        :param time:   limit return to stations with
        startime<time<endtime.  Input is assumed an
        epoch time NOT an obspy UTCDateTime. Use a conversion
        to epoch time if necessary.
        :return:  obspy Inventory of all stations matching the
        query parameters
        :rtype:  obspy Inventory
        """
        dbsite = self.site
        query = {}
        if net != None:
            query["net"] = net
        if sta != None:
            query["sta"] = sta
        if loc != None:
            query["loc"] = loc
        if time != None:
            query["starttime"] = {"$lt": time}
            query["endtime"] = {"$gt": time}
        matchsize = dbsite.count_documents(query)
        result = Inventory()
        if matchsize == 0:
            return None
        else:
            stations = dbsite.find(query)
            for s in stations:
                serialized = s["serialized_inventory"]
                netw = pickle.loads(serialized)
                # It might be more efficient to build a list of
                # Network objects but here we add them one
                # station at a time.  Note the extend method
                # if poorly documented in obspy
                result.extend([netw])
        return result

    def get_seed_site(self, net, sta, loc="NONE", time=-1.0, verbose=True):
        """
        The site collection is assumed to have a one to one
        mapping of net:sta:loc:starttime - endtime.
        This method uses a restricted query to match the
        keys given and returns the MongoDB document matching the keys.
        The (optional) time arg is used for a range match to find
        period between the site startime and endtime.
        Returns None if there is no match.

        An all to common metadata problem is to have duplicate entries in
        site for the same data.   The default behavior of this method is
        to print a warning whenever a match is ambiguous
        (i.e. more than on document matches the keys).  Set verbose false to
        silence such warnings if you know they are harmless.

        The seed modifier in the name is to emphasize this method is
        for data originating as the SEED format that use net:sta:loc:chan
        as the primary index.

        :param net:  network name to match
        :param sta:  station name to match
        :param loc:   optional loc code to made (empty string ok and common)
        default ignores loc in query.
        :param time: epoch time for requested metadata.  Default undefined
          and will cause the function to simply return the first document
          matching the name keys only.   (This is rarely what you want, but
          there is no standard default for this argument.)
        :param verbose:  When True (the default) this method will issue a
          print warning message when the match is ambiguous - multiple
          docs match the specified keys.   When set False such warnings
          will be suppressed.  Use false only if you know the duplicates
          are harmless and you are running on a large data set and
          you want to reduce the log size.

        :return: MongoDB doc matching query
        :rtype:  python dict (document) of result.  None if there is no match.
        """
        dbsite = self.site
        query = {}
        query["net"] = net
        query["sta"] = sta
        if loc != "NONE":
            query["loc"] = loc
        if time > 0.0:
            query["starttime"] = {"$lt": time}
            query["endtime"] = {"$gt": time}
        matchsize = dbsite.count_documents(query)
        if matchsize == 0:
            return None
        else:
            if matchsize > 1 and verbose:
                print("get_seed_site (WARNING):  query=", query)
                print("Returned ", matchsize, " documents - should be exactly one")
                print("Returning first entry found")
            stadoc = dbsite.find_one(query)
            return stadoc

    def get_seed_channel(self, net, sta, chan, loc=None, time=-1.0, verbose=True):
        """
        The channel collection is assumed to have a one to one
        mapping of net:sta:loc:chan:starttime - endtime.
        This method uses a restricted query to match the
        keys given and returns the document matching the specified keys.

        The optional loc code is handled specially.  The reason is
        that it is common to have the loc code empty.  In seed data that
        puts two ascii blank characters in the 2 byte packet header
        position for each miniseed blockette.  With pymongo that
        can be handled one of three ways that we need to handle gracefully.
        That is, one can either set a literal two blank character
        string, an empty string (""), or a MongoDB NULL.   To handle
        that confusion this algorithm first queries for all matches
        without loc defined.  If only one match is found that is
        returned immediately.  If there are multiple matches we
        search though the list of docs returned for a match to
        loc being conscious of the null string oddity.

        The (optional) time arg is used for a range match to find
        period between the site startime and endtime.  If not used
        the first occurence will be returned (usually ill adivsed)
        Returns None if there is no match.  Although the time argument
        is technically option it usually a bad idea to not include
        a time stamp because most stations saved as seed data have
        time variable channel metadata.

        :param net:  network name to match
        :param sta:  station name to match
        :param chan:  seed channel code to match
        :param loc:   optional loc code to made (empty string ok and common)
        default ignores loc in query.
        :param time: epoch time for requested metadata
        :param verbose:  When True (the default) this method will issue a
          print warning message when the match is ambiguous - multiple
          docs match the specified keys.   When set False such warnings
          will be suppressed.  Use false only if you know the duplicates
          are harmless and you are running on a large data set and
          you want to reduce the log size.

        :return: handle to query return
        :rtype:  MondoDB Cursor object of query result.
        """
        dbchannel = self.channel
        query = {}
        query["net"] = net
        query["sta"] = sta
        query["chan"] = chan
        if loc != None:
            query["loc"] = loc

        if time > 0.0:
            query["starttime"] = {"$lt": time}
            query["endtime"] = {"$gt": time}
        matchsize = dbchannel.count_documents(query)
        if matchsize == 0:
            return None
        if matchsize == 1:
            return dbchannel.find_one(query)
        else:
            # Note we only land here when the above yields multiple matches
            if loc == None:
                # We could get here one of two ways.  There could
                # be multiple loc codes and the user didn't specify
                # a choice or they wanted the empty string (2 cases).
                # We also have to worry about the case where the
                # time was not specified but needed.
                # The complexity below tries to unravel all those possibities
                testquery = query
                testquery["loc"] = None
                matchsize = dbchannel.count_documents(testquery)
                if matchsize == 1:
                    return dbchannel.find_one(testquery)
                elif matchsize > 1:
                    if time > 0.0:
                        if verbose:
                            print(
                                "get_seed_channel:  multiple matches found for net=",
                                net,
                                " sta=",
                                sta,
                                " and channel=",
                                chan,
                                " with null loc code\n"
                                "Assuming database problem with duplicate documents in channel collection\n",
                                "Returning first one found",
                            )
                        return dbchannel.find_one(testquery)
                    else:
                        raise MsPASSError(
                            "get_seed_channel:  "
                            + "query with "
                            + net
                            + ":"
                            + sta
                            + ":"
                            + chan
                            + " and null loc is ambiguous\n"
                            + "Specify at least time but a loc code if is not truly null",
                            "Fatal",
                        )
                else:
                    # we land here if a null match didn't work.
                    # Try one more recovery with setting loc to an emtpy
                    # string
                    testquery["loc"] = ""
                    matchsize = dbchannel.count_documents(testquery)
                    if matchsize == 1:
                        return dbchannel.find_one(testquery)
                    elif matchsize > 1:
                        if time > 0.0:
                            if verbose:
                                print(
                                    "get_seed_channel:  multiple matches found for net=",
                                    net,
                                    " sta=",
                                    sta,
                                    " and channel=",
                                    chan,
                                    " with null loc code tested with empty string\n"
                                    "Assuming database problem with duplicate documents in channel collection\n",
                                    "Returning first one found",
                                )
                            return dbchannel.find_one(testquery)
                        else:
                            raise MsPASSError(
                                "get_seed_channel:  "
                                + "recovery query attempt with "
                                + net
                                + ":"
                                + sta
                                + ":"
                                + chan
                                + " and null loc converted to empty string is ambiguous\n"
                                + "Specify at least time but a loc code if is not truly null",
                                "Fatal",
                            )

    def get_response(self, net=None, sta=None, chan=None, loc=None, time=None):
        """
        Returns an obspy Response object for seed channel defined by
        the standard keys net, sta, chan, and loc and a time stamp.
        Input time can be a UTCDateTime or an epoch time stored as a float.

        :param db:  mspasspy Database handle containing a channel collection
          to be queried
        :param net: seed network code (required)
        :param sta: seed station code (required)
        :param chan:  seed channel code (required)
        :param loc:  seed net code.  If None loc code will not be
          included in the query.  If loc is anything else it is passed
          as a literal.  Sometimes loc codes are not defined by in the
          seed data and are literal two ascii space characters.  If so
          MongoDB translates those to "".   Use loc="" for that case or
          provided the station doesn't mix null and other loc codes use None.
        :param time:  time stamp for which the response is requested.
          seed metadata has a time range for validity this field is
          required.   Can be passed as either a UTCDateTime object or
          a raw epoch time stored as a python float.
        """
        if sta == None or chan == None or net == None or time == None:
            raise MsPASSError(
                "get_response:  missing one of required arguments:  "
                + "net, sta, chan, or time",
                "Invalid",
            )
        query = {"net": net, "sta": sta, "chan": chan}
        if loc != None:
            query["loc"] = loc
        else:
            loc = "  "  # set here but not used
        if isinstance(time, UTCDateTime):
            t0 = time.timestamp
        else:
            t0 = time
        query["starttime"] = {"$lt": t0}
        query["endtime"] = {"$gt": t0}
        n = self.channel.count_documents(query)
        if n == 0:
            print(
                "No matching documents found in channel for ",
                net,
                ":",
                sta,
                ":",
                "chan",
                chan,
                "->",
                loc,
                "<-",
                " at time=",
                UTCDateTime(t0),
            )
            return None
        elif n > 1:
            print(
                n,
                " matching documents found in channel for ",
                net,
                ":",
                sta,
                ":",
                "chan",
                "->",
                loc,
                "<-",
                " at time=",
                UTCDateTime(t0),
            )
            print("There should be just one - returning the first one found")
        doc = self.channel.find_one(query)
        s = doc["serialized_channel_data"]
        chan = pickle.loads(s)
        return chan.response

    def save_catalog(self, cat, verbose=False):
        """
        Save the contents of an obspy Catalog object to MongoDB
        source collection.  All contents are saved even with
        no checking for existing sources with duplicate
        data.   Like the comparable save method for stations,
        save_inventory, the assumption is pre or post cleanup
        will be preformed if duplicates are a major issue.

        :param cat: is the Catalog object to be saved
        :param verbose: Print informational data if true.
        When false (default) it does it's work silently.

        :return: integer count of number of items saved
        """
        # perhaps should demand db is handle to the source collection
        # but I think the cost of this lookup is tiny
        # to do: need to change source_id to be a copy of the _id string.

        dbcol = self.source
        nevents = 0
        for event in cat:
            # event variable in loop is an Event object from cat
            o = event.preferred_origin()
            m = event.preferred_magnitude()
            picklestr = pickle.dumps(event)
            rec = {}
            # rec['source_id']=source_id
            rec["lat"] = o.latitude
            rec["lon"] = o.longitude
            rec["coords"] = [o.latitude, o.longitude]
            # It appears quakeml puts source depths in meter
            # convert to km
            # also obspy's catalog object seesm to allow depth to be
            # a None so we have to test for that condition to avoid
            # aborts
            if o.depth == None:
                depth = 0.0
            else:
                depth = o.depth / 1000.0
            rec["depth"] = depth
            otime = o.time
            # This attribute of UTCDateTime is the epoch time
            # In mspass we only story time as epoch times
            rec["time"] = otime.timestamp
            rec["magnitude"] = m.mag
            rec["magnitude_type"] = m.magnitude_type
            rec["serialized_event"] = picklestr
            result = dbcol.insert_one(rec)
            # the return of an insert_one has the object id of the insertion
            # set as inserted_id.  We save taht as source_id as a more
            # intuitive key that _id
            idobj = result.inserted_id
            dbcol.update_one({"_id": idobj}, {"$set": {"source_id": idobj}})
            nevents += 1
        return nevents

    def load_event(self, source_id):
        """
        Return a bson record of source data matching the unique id
        defined by source_id.   The idea is that magic string would
        be extraced from another document (e.g. in an arrival collection)
        and used to look up the event with which it is associated in
        the source collection.

        This function is a relic and may be depricated.  I originally
        had a different purpose.
        """
        dbsource = self.source
        x = dbsource.find_one({"source_id": source_id})
        return x

    #  Methods for handling miniseed data
    @staticmethod
    def _convert_mseed_index(index_record):
        """
        Helper used to convert C++ struct/class mseed_index to a dict
        to use for saving to mongod.  Note loc is only set if it is not
        zero length - consistent with mspass approach

        :param index_record:  mseed_index record to convert
        :return: dict containing index data converted to dict.

        """
        o = dict()
        o["sta"] = index_record.sta
        o["net"] = index_record.net
        o["chan"] = index_record.chan
        if index_record.loc:
            o["loc"] = index_record.loc
        o["sampling_rate"] = index_record.samprate
        o["delta"] = 1.0 / (index_record.samprate)
        o["starttime"] = index_record.starttime
        o["last_packet_time"] = index_record.last_packet_time
        o["foff"] = index_record.foff
        o["nbytes"] = index_record.nbytes
        # FIXME: The following are commented out because some simple tests showed that
        #   the numbers are very different from the data being read by ObsPy's reader.
        # o['endtime'] = index_record.endtime
        # o['npts'] = index_record.npts
        return o

    def index_mseed_file(
        self,
        dfile,
        dir=None,
        collection="wf_miniseed",
        segment_time_tears=False,
        elog_collection="elog",
        return_ids=False,
        normalize_channel=False,
        verbose=False,
    ):
        """
        This is the first stage import function for handling the import of
        miniseed data.  This function scans a data file defined by a directory
        (dir arg) and dfile (file name) argument.  I builds an index
        for the file and writes the index to mongodb
        in the collection defined by the collection
        argument (wf_miniseed by default).   The index is bare bones
        miniseed tags (net, sta, chan, and loc) with a starttime tag.
        The index is appropriate ONLY if the data on the file are created
        by concatenating data with packets sorted by net, sta, loc, chan, time
        AND the data are contiguous in time.   The original
        concept for this function came from the need to handle large files
        produced by concanentation of miniseed single-channel files created
        by obpsy's mass_downloader.   i.e. the basic model is the input
        files are assumed to be something comparable to running the unix
        cat command on a set of single-channel, contingous time sequence files.
        There are other examples that do the same thing (e.g. antelope's
        miniseed2days).

        We emphasize this function only builds an index - it does not
        convert any data.   It has to scan the entire file deriving the
        index from data retrieved from miniseed packets with libmseed so
        for large data sets this can take a long time.

        Actual seismic data stored as miniseed are prone to time tears.
        That can happen at the instrument level in at least two common
        ways: (1) dropped packets from telemetry issues, or (2) instrument
        timing jumps when a clock loses external lock to gps or some
        other standard and the rock is restored.  The behavior is this
        function in gap handling is controlled by the input parameter
        segment_time_tears.  When true a new index entry is created
        any time the start time of a packet differs from that computed
        from the endtime of the last packet by more than one sample
        AND net:sta:chan:loc are constant.  The default for this
        parameter is false because data with many dropped packets from
        telemetry are common and can create overwhelming numbers of
        index entries quickly.  When false the scan only creates a new
        index record when net, sta, chan, or loc change between successive
        packets.  Our reader has gap handling functions to handle
        time tears.  Set segment_time_tears true only when you are
        confident the data set does not contain a large number of dropped
        packets.

        Note to parallelize this function put a list of files in a Spark
        RDD or a Dask bag and parallelize the call the this function.
        That can work because MongoDB is designed for parallel operations
        and we use the thread safe version of the libmseed reader.

        Finally, note that cross referencing with the channel and/or
        source collections should be a common step after building the
        index with this function.  The reader found elsewhere in this
        module will transfer linking ids (i.e. channel_id and/or source_id)
        to TimeSeries objects when it reads the data from the files
        indexed by this function.

        :param dfile:  file name of data to be indexed.  Asssumed to be
          the leaf node of the path - i.e. it contains no directory information
          but just the file name.
        :param dir:  directory name.  This can be a relative path from the
          current directory be be advised it will always be converted to an
          fully qualified path.  If it is undefined (the default) the function
          assumes the file is in the current working directory and will use
          the result of the python getcwd command as the directory path
          stored in the database.
        :param collection:  is the mongodb collection name to write the
          index data to.  The default is 'wf_miniseed'.  It should be rare
          to use anything but the default.
        :param segment_time_tears: boolean controlling handling of data gaps
          defined by constant net, sta, chan, and loc but a discontinuity
          in time tags for successive packets.  See above for a more extensive
          discussion of how to use this parameter.  Default is False.
        :param elog_collection:  name to write any error logs messages
          from the miniseed reader.  Default is "elog", which is the
          same as for TimeSeries and Seismogram data, but the cross reference
          keys here are keyed by "wf_miniseed_id".
        :param return_ids:  if set True the function will return a tuple
          with two id lists.  The 0 entry is an array of ids from the
          collection (wf_miniseed by default) of index entries saved and
          the 1 entry will contain the ids in the elog_collection of
          error log entry insertions.  The 1 entry will be empty if the
          reader found no errors and the error log was empty (the hopefully
          normal situation).  When this argument is False (the default) it
          returns None.  Set true if you need to build some kind of cross
          reference to read errors to build some custom cleaning method
          for specialized processing that can be done more efficiently.
          By default it is fast only to associate an error log entry with
          a particular waveform index entry. (we store the saved index
          MongoDB document id with each elog entry)
        :param normalize_channel:  boolean controlling normalization with
          the channel collection.   When set True (default is false) the
          method will call the Database.get_seed_channel method, extract
          the id from the result, and set the result as "channel_id" before
          writing the wf_miniseed document.  Set this argument true if
          you have a relatively complete channel collection assembled
          before running a workflow to index a set of miniseed files
          (a common raw data starting point).
        :param verbose:  boolean passed to get_seed_channel.  This
          argument has no effect unless normalize_channel is set True.
          It is necessary because the get_seed_channel function has no
          way to log errors except calling print.  A very common metadata
          error is duplicate and/or time overlaps in channel metadata.
          Those are usually harmless so the default for this parameter is
          False.  Set this True if you are using inline normalization
          (normalize_channel set True) and you aren't certain your
          channel collection has no serious inconsistencies.
        :exception: This function can throw a range of error types for
          a long list of possible io issues.   Callers should use a
          generic handler to avoid aborts in a large job.
        """
        dbh = self[collection]
        # If dir is not define assume current directory.  Otherwise
        # use realpath to make sure the directory is the full path
        # We store the full path in mongodb
        if dir is None:
            odir = os.getcwd()
        else:
            odir = os.path.abspath(dir)
        if dfile is None:
            dfile = self._get_dfile_uuid("mseed")
        fname = os.path.join(odir, dfile)
        (ind, elog) = _mseed_file_indexer(fname)
        ids_affected = []
        for i in ind:
            doc = self._convert_mseed_index(i)
            doc["storage_mode"] = "file"
            doc["format"] = "mseed"
            doc["dir"] = odir
            doc["dfile"] = dfile
            thisid = dbh.insert_one(doc).inserted_id
            ids_affected.append(thisid)
        if normalize_channel:
            # these quantities are always defined unless there was a read error
            # and I don't think we can get here if we had a read error.
            net = doc["net"]
            sta = doc["sta"]
            chan = doc["chan"]
            stime = doc["starttime"]
            if "loc" in doc:
                loc = doc["loc"]
                chandoc = self.get_seed_channel(
                    net, sta, chan, loc, time=stime, verbose=False
                )
            else:
                chandoc = self.get_seed_channel(
                    net, sta, chan, time=stime, verbose=False
                )
            if chandoc != None:
                doc["channel_id"] = chandoc["_id"]

        # log_ids is created here so it is defined but empty in
        # the tuple returned when return_ids is true
        log_ids = []
        if elog.size() > 0:
            elog_col = self[elog_collection]

            errs = elog.get_error_log()
            jobid = elog.get_job_id()
            logdata = []
            for x in errs:
                logdata.append(
                    {
                        "job_id": jobid,
                        "algorithm": x.algorithm,
                        "badness": str(x.badness),
                        "error_message": x.message,
                        "process_id": x.p_id,
                    }
                )
            docentry = {"logdata": logdata}
            # To mesh with the standard elog collection we add a copy of the
            # error messages with a tag for each id in the ids_affected list.
            # That should make elog connection to wf_miniseed records exactly
            # like wf_TimeSeries records but with a different collection link
            for wfid in ids_affected:
                docentry["wf_miniseed_id"] = wfid
                elogid = elog_col.insert_one(docentry).inserted_id
                log_ids.append(elogid)
        if return_ids:
            return [ids_affected, log_ids]
        else:
            return None

    def save_dataframe(
        self, df, collection, null_values=None, one_to_one=True, parallel=False
    ):
        """
        Tansfer a dataframe into a set of documents, and store them
        in a specified collection. In one_to_one mode every row in the
        dataframe will be saved into the mongodb, otherwise duplicates
        would be discarded.
        The dataframe will first call dropna to eliminate None values stored
        in it, so that after it is transfered into a document, the memory
        usage will be reduced.
        :param df: Pandas.Dataframe object, the input to be transfered into mongodb
        documents
        :param collection:  MongoDB collection name to be used to save the
        (often subsetted) tuples of filename as documents in this collection.
        :param null_values:  is an optional dict defining null field values.
        When used an == test is applied to each attribute with a key
        defined in the null_vlaues python dict.  If == returns True, the
        value will be set as None in dataframe. If your table has a lot of null
        fields this option can save space, but readers must not require the null
        field.  The default is None which it taken to mean there are no null
        fields defined.
        :param one_to_one: a boolean to control if the set should be filtered by
        rows.  The default is True which means every row in the dataframe will
        create a single MongoDB document. If False the (normally reduced) set
        of attributes defined by attributes_to_use will be filtered with the
        panda/dask dataframe drop_duplicates method before converting the
        dataframe to documents and saving them to MongoDB.  That approach
        is important, for example, to filter things like Antelope "site" or
        "sitechan" attributes created by a join to something like wfdisc and
        saved as a text file to be processed by this function.
        :param parallel:  a boolean that determine if dask api will be used for
        operations on the dataframe, default is false.
        :return:  integer count of number of documents added to collection
        """
        dbcol = self[collection]

        if parallel:
            df = daskdf.from_pandas(df, chunksize=1, sort=False)

        if not one_to_one:
            df = df.drop_duplicates()

        #   For those null values, set them to None
        df = df.astype(object)
        if null_values is not None:
            for key, val in null_values.items():
                if key not in df:
                    continue
                else:
                    df[key] = df[key].mask(df[key] == val, None)

        """
        if parallel:
            df = daskdf.from_pandas(df, chunksize=1, sort=False)
            df = df.apply(lambda x: x.dropna(), axis=1).compute()
        else:
            df = df.apply(pd.Series.dropna, axis=1)
        """
        if parallel:
            df = df.compute()

        df = df.apply(pd.Series.dropna, axis=1)
        doc_list = df.to_dict(orient="records")
        if len(doc_list):
            dbcol.insert_many(doc_list)
        return len(doc_list)

    def save_textfile(
        self,
        filename,
        collection="textfile",
        separator="\\s+",
        type_dict=None,
        header_line=0,
        attribute_names=None,
        rename_attributes=None,
        attributes_to_use=None,
        null_values=None,
        one_to_one=True,
        parallel=False,
        insert_column=None,
    ):
        """
        Import and parse a textfile into set of documents, and store them
        into a mongodb collection. This function consists of two steps:
        1. Textfile2Dataframe: Convert the input textfile into a Pandas dataframe
        2. save_dataframe: Insert the documents in that dataframe into a mongodb
        collection

        :param filename:  path to text file that is to be read to create the
        table object that is to be processed (internally we use pandas or
        dask dataframes)
        :param collection:  MongoDB collection name to be used to save the
        (often subsetted) tuples of filename as documents in this collection.
        :param separator: The delimiter used for seperating fields,
        the default is "\s+", which is the regular expression of "one or more
        spaces".
            For csv file, its value should be set to ','.
            This parameter will be passed into pandas.read_csv or dask.dataframe.read_csv.
            To learn more details about the usage, check the following links:
            https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
            https://docs.dask.org/en/latest/generated/dask.dataframe.read_csv.html
        :param type_dict: pairs of each attribute and its type, usedd to validate
        the type of each input item
        :param header_line: defines the line to be used as the attribute names for
        columns, if is < 0, an attribute_names is required. Please note that if an
        attribute_names is provided, the attributes defined in header_line will
        always be override.
        :param attribute_names: This argument must be either a list of (unique)
        string names to define the attribute name tags for each column of the
        input table.   The length of the array must match the number of
        columns in the input table or this function will throw a MsPASSError
        exception.   This argument is None by default which means the
        function will assume the line specified by the "header_line" argument as
        column headers defining the attribute name.  If header_line is less
        than 0 this argument will be required.  When header_line is >= 0
        and this argument (attribute_names) is defined all the names in
        this list will override those stored in the file at the specified
        line number.
        :param  rename_attributes:   This is expected to be a python dict
        keyed by names matching those defined in the file or attribute_names
        array (i.e. the panda/dataframe column index names) and values defining
        strings to use to override the original names.   That usage, of course,
        is most common to override names in a file.  If you want to change all
        the name use a custom attributes_name array as noted above.  This
        argument is mostly to rename a small number of anomalous names.
        :param attributes_to_use:  If used this argument must define a list of
        attribute names that define the subset of the dataframe dataframe
        attributes that are to be saved.  For relational db users this is
        effectively a "select" list of attribute names.  The default is
        None which is taken to mean no selection is to be done.
        :param one_to_one: is an important boolean use to control if the
        output is or is not filtered by rows.  The default is True
        which means every tuple in the input file will create a single row in
        dataframe. (Useful, for example, to construct an wf_miniseed
        collection css3.0 attributes.)  If False the (normally reduced) set
        of attributes defined by attributes_to_use will be filtered with the
        panda/dask dataframe drop_duplicates method.  That approach
        is important, for example, to filter things like Antelope "site" or
        "sitechan" attributes created by a join to something like wfdisc and
        saved as a text file to be processed by this function.
        :param parallel:  When true we use the dask dataframe operation.
        The default is false meaning the simpler, identical api panda
        operators are used.
        :param insert_column: a dictionary of new columns to add, and their value(s).
        If the content is a single value, it can be passedto define a constant value
        for the entire column of data. The content can also be a list, in that case,
        the list should contain values that are to be set, and it must be the same
        length as the number of tuples in the table.
        :return:  Integer count of number of documents added to collection
        """
        df = Textfile2Dataframe(
            filename=filename,
            separator=separator,
            type_dict=type_dict,
            header_line=header_line,
            attribute_names=attribute_names,
            rename_attributes=rename_attributes,
            attributes_to_use=attributes_to_use,
            one_to_one=one_to_one,
            parallel=parallel,
            insert_column=insert_column,
        )
        return self.save_dataframe(
            df=df,
            collection=collection,
            null_values=null_values,
            one_to_one=one_to_one,
            parallel=parallel,
        )

    @staticmethod
    def _download_windowed_mseed_file(
        aws_access_key_id,
        aws_secret_access_key,
        year,
        day_of_year,
        network="",
        station="",
        channel="",
        location="",
        duration=-1,
        t0shift=0,
    ):
        """
        A helper function to download the miniseed file from AWS s3.
        A lambda function will be called, and do the timewindowing on cloud. The output file
        of timewindow will then be downloaded and parsed by obspy.
        Finally return an obspy stream object.
        An example of using lambda is in /scripts/aws_lambda_examples.

        :param aws_access_key_id & aws_secret_access_key: credential for aws, used to initialize lambda_client
        :param year:  year for the query mseed file(4 digit).
        :param day_of_year:  day of year for the query of mseed file(3 digit [001-366])
        :param network:  network code
        :param station:  station code
        :param channel:  channel code
        :param location:  location code
        :param duration:  window duration, default value is -1, which means no window will be performed
        :param t0shift: shift the start time, default is 0
        """
        lambda_client = boto3.client(
            service_name="lambda",
            region_name="us-west-2",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        s3_input_bucket = "scedc-pds"
        s3_output_bucket = "mspass-scedcdata"  #   The output file can be saved to this bucket, user might want to change it into their own bucket
        year = str(year)
        day_of_year = str(day_of_year)
        if len(day_of_year) < 3:
            day_of_year = "0" * (3 - len(day_of_year)) + day_of_year
        source_key = (
            "continuous_waveforms/" + year + "/" + year + "_" + day_of_year + "/"
        )
        if len(network) < 2:
            network += "_" * (2 - len(network))
        if len(station) < 5:
            station += "_" * (5 - len(station))
        if len(channel) < 3:
            channel += "_" * (3 - len(channel))
        if len(location) < 2:
            location += "_" * (2 - len(location))

        mseed_file = (
            network + station + channel + location + "_" + year + day_of_year + ".ms"
        )
        source_key += mseed_file

        event = {
            "src_bucket": s3_input_bucket,
            "dst_bucket": s3_output_bucket,
            "src_key": source_key,
            "dst_key": source_key,
            "save_to_s3": False,
            "duration": duration,
            "t0shift": t0shift,
        }

        response = lambda_client.invoke(
            FunctionName="TimeWindowFunction",  #   The name of lambda function on cloud
            InvocationType="RequestResponse",
            LogType="Tail",
            Payload=json.dumps(event),
        )

        response_payload = json.loads(response["Payload"].read())
        ret_type = response_payload["ret_type"]

        if (
            ret_type == "key"
        ):  # If the ret_type is "key", the output file is stored in another s3 bucket,
            # we have to fetch it again.
            try:
                ret_bucket = response_payload["ret_value"].split("::")[0]
                ret_key = response_payload["ret_value"].split("::")[1]
                s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                )
                obj = s3_client.get_object(Bucket=ret_bucket, Key=ret_key)
                st = obspy.read(io.BytesIO(obj["Body"].read()))
                return st

            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    # the object does not exist
                    print(
                        "Could not find the object by the KEY: {} from the BUCKET: {} in s3"
                    ).format(ret_key, ret_bucket)
                else:
                    raise

            except Exception as e:
                raise MsPASSError(
                    "Error while downloading the output object.", "Fatal"
                ) from e

        filecontent = base64.b64decode(response_payload["ret_value"].encode("utf-8"))
        stringio_obj = io.BytesIO(filecontent)
        st = obspy.read(stringio_obj)

        return st

    def index_mseed_s3_continuous(
        self,
        s3_client,
        year,
        day_of_year,
        network="",
        station="",
        channel="",
        location="",
        collection="wf_miniseed",
        storage_mode="s3_continuous",
    ):
        """
        This is the first stage import function for handling the import of
        miniseed data. However, instead of scanning a data file defined by a directory
        (dir arg) and dfile (file name) argument, it reads the miniseed content from AWS s3.
        It builds and index it writes to mongodb in the collection defined by the collection
        argument (wf_miniseed by default). The index is bare bones
        miniseed tags (net, sta, chan, and loc) with a starttime tag.

        :param s3_client:  s3 Client object given by user, which contains credentials
        :param year:  year for the query mseed file(4 digit).
        :param day_of_year:  day of year for the query of mseed file(3 digit [001-366])
        :param network:  network code
        :param station:  station code
        :param channel:  channel code
        :param location:  location code
        :param collection:  is the mongodb collection name to write the
            index data to.  The default is 'wf_miniseed'.  It should be rare
            to use anything but the default.
        :exception: This function will do nothing if the obejct does not exist. For other
            exceptions, it would raise a MsPASSError.
        """

        dbh = self[collection]
        BUCKET_NAME = "scedc-pds"
        year = str(year)
        day_of_year = str(day_of_year)
        if len(day_of_year) < 3:
            day_of_year = "0" * (3 - len(day_of_year)) + day_of_year
        KEY = "continuous_waveforms/" + year + "/" + year + "_" + day_of_year + "/"
        if len(network) < 2:
            network += "_" * (2 - len(network))
        if len(station) < 5:
            station += "_" * (5 - len(station))
        if len(channel) < 3:
            channel += "_" * (3 - len(channel))
        if len(location) < 2:
            location += "_" * (2 - len(location))

        mseed_file = (
            network + station + channel + location + "_" + year + day_of_year + ".ms"
        )
        KEY += mseed_file

        try:
            obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=KEY)
            mseed_content = obj["Body"].read()
            stringio_obj = io.BytesIO(mseed_content)
            st = obspy.read(stringio_obj)
            # there could be more than 1 trace object in the stream, merge the traces
            st.merge()

            stats = st[0].stats
            doc = dict()
            doc["year"] = year
            doc["day_of_year"] = day_of_year
            doc["sta"] = stats["station"]
            doc["net"] = stats["network"]
            doc["chan"] = stats["channel"]
            if "location" in stats and stats["location"]:
                doc["loc"] = stats["location"]
            doc["sampling_rate"] = stats["sampling_rate"]
            doc["delta"] = 1.0 / stats["sampling_rate"]
            doc["starttime"] = stats["starttime"].timestamp
            if "npts" in stats and stats["npts"]:
                doc["npts"] = stats["npts"]
            doc["storage_mode"] = storage_mode
            doc["format"] = "mseed"
            dbh.insert_one(doc)

        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                # the object does not exist
                print(
                    "Could not find the object by the KEY: {} from the BUCKET: {} in s3"
                ).format(KEY, BUCKET_NAME)
            else:
                print(
                    "An ClientError occur when tyring to get the object by the KEY("
                    + KEY
                    + ")"
                    + " with error: ",
                    e,
                )

        except Exception as e:
            raise MsPASSError("Error while index mseed file from s3.", "Fatal") from e

    def index_mseed_s3_event(
        self,
        s3_client,
        year,
        day_of_year,
        filename,
        dfile,
        dir=None,
        collection="wf_miniseed",
    ):
        """
        This is the first stage import function for handling the import of
        miniseed data. However, instead of scanning a data file defined by a directory
        (dir arg) and dfile (file name) argument, it reads the miniseed content from AWS s3.
        It builds and index it writes to mongodb in the collection defined by the collection
        argument (wf_miniseed by default). The index is bare bones
        miniseed tags (net, sta, chan, and loc) with a starttime tag.

        :param s3_client:  s3 Client object given by user, which contains credentials
        :param year:  year for the query mseed file(4 digit).
        :param day_of_year:  day of year for the query of mseed file(3 digit [001-366])
        :param filename:  SCSN catalog event id for the event
        :param collection:  is the mongodb collection name to write the
            index data to.  The default is 'wf_miniseed'.  It should be rare
            to use anything but the default.
        :exception: This function will do nothing if the obejct does not exist. For other
            exceptions, it would raise a MsPASSError.
        """

        dbh = self[collection]
        BUCKET_NAME = "scedc-pds"
        year = str(year)
        day_of_year = str(day_of_year)
        filename = str(filename)
        if len(day_of_year) < 3:
            day_of_year = "0" * (3 - len(day_of_year)) + day_of_year
        KEY = (
            "event_waveforms/"
            + year
            + "/"
            + year
            + "_"
            + day_of_year
            + "/"
            + filename
            + ".ms"
        )

        try:
            obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=KEY)
            mseed_content = obj["Body"].read()
            # specify the file path
            if dir is None:
                odir = os.getcwd()
            else:
                odir = os.path.abspath(dir)
            if dfile is None:
                dfile = self._get_dfile_uuid("mseed")
            fname = os.path.join(odir, dfile)
            # temporarily write data into a file
            with open(fname, "wb") as f:
                f.write(mseed_content)
            # immediately read data from the file
            (ind, elog) = _mseed_file_indexer(fname)
            for i in ind:
                doc = self._convert_mseed_index(i)
                doc["storage_mode"] = "s3_event"
                doc["format"] = "mseed"
                doc["dir"] = odir
                doc["dfile"] = dfile
                doc["year"] = year
                doc["day_of_year"] = day_of_year
                doc["filename"] = filename
                dbh.insert_one(doc)

        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                # the object does not exist
                print(
                    "Could not find the object by the KEY: {} from the BUCKET: {} in s3"
                ).format(KEY, BUCKET_NAME)
            else:
                print(
                    "An ClientError occur when tyring to get the object by the KEY("
                    + KEY
                    + ")"
                    + " with error: ",
                    e,
                )

        except Exception as e:
            raise MsPASSError("Error while index mseed file from s3.", "Fatal") from e

    def index_mseed_FDSN(
        self,
        provider,
        year,
        day_of_year,
        network,
        station,
        location,
        channel,
        collection="wf_miniseed",
    ):
        dbh = self[collection]

        client = Client(provider)
        year = str(year)
        day_of_year = str(day_of_year)
        if len(day_of_year) < 3:
            day_of_year = "0" * (3 - len(day_of_year)) + day_of_year
        t = UTCDateTime(year + day_of_year, iso8601=True)

        st = client.get_waveforms(
            network, station, location, channel, t, t + 60 * 60 * 24
        )
        # there could be more than 1 trace object in the stream, merge the traces
        st.merge()

        stats = st[0].stats
        doc = dict()
        doc["provider"] = provider
        doc["year"] = year
        doc["day_of_year"] = day_of_year
        doc["sta"] = station
        doc["net"] = network
        doc["chan"] = channel
        doc["loc"] = location
        doc["sampling_rate"] = stats["sampling_rate"]
        doc["delta"] = 1.0 / stats["sampling_rate"]
        doc["starttime"] = stats["starttime"].timestamp
        if "npts" in stats and stats["npts"]:
            doc["npts"] = stats["npts"]
        doc["storage_mode"] = "fdsn"
        doc["format"] = "mseed"
        dbh.insert_one(doc)

    def _get_dfile_uuid(self, format=None):
        """
        This is a helper function to generate a uuid for the file name,
        when dfile is not given or defined. The format can be 'mseed' or
        other formats.
        The return value is random uuid + "-" + format, example:
        '247ce5d7-06bc-4ccc-8084-5b3ff621d2d4-mseed'

        :param format: format of the file
        """
        if format is None:
            format = "unknown"
        temp_uuid = str(uuid.uuid4())
        dfile = temp_uuid + "-" + format
        return dfile
