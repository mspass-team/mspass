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


def read_distributed_data_df(
    df,
    load_history=False,
    format="dask",
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

    :param df: the dataframe from which the data are to be read.
    :type df: :class:`dask.bag.Bag` or :class:`pyspark.RDD`.
    :param load_history: boolean (True or False) switch used to enable or
      disable object level history mechanism.   When set True each datum
      will be tagged with its origin id that defines the leaf nodes of a
      history G-tree.  See the User's manual for additional details of this
      feature.  Default is False.
    :param format: Set the format of the parallel container to define the
      dataset.   Must be either "spark" or "dask" or the job will abort
      immediately with an exception
    :type format: :class:`str`
    :return: container defining the parallel dataset.  A spark `RDD` if format
      is "Spark" and a dask 'bag' if format is "dask"
    """
    if format == "spark":
        return df.map(
            lambda cur: read_df_data(
                cur["md"],
                cur["object_type"],
                cur["is_dead"],
                cur["object_doc"],
                cur["log_error_msg"],
                load_history,
                cur["history_obj_id_name"],
                cur["history_object_id"],
                cur["res"],
            )
        )
    elif format == "dask":
        return df.map(
            lambda cur: read_df_data(
                cur["md"],
                cur["object_type"],
                cur["is_dead"],
                cur["object_doc"],
                cur["log_error_msg"],
                load_history,
                cur["history_obj_id_name"],
                cur["history_object_id"],
                cur["res"],
            )
        )
    else:
        raise TypeError("Only spark and dask are supported")


def read_metadata(
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
):
    """
    This is the MsPASS reader for constructing metadata of Seismogram or TimeSeries
    objects from data managed with MondoDB through MsPASS.   Return type is a dict
    with parameters, which can be used in read_df_data().

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

    history_obj_id_name = self.database_schema.default_name("history_object") + "_id"
    collection = self.database_schema.default_name("history_object")
    history_object_id = object_doc[history_obj_id_name]
    res = self[collection].find_one({"_id": history_object_id})

    r = {}
    r["md"] = md
    r["object_type"] = object_type
    r["is_dead"] = is_dead
    r["object_doc"] = object_doc
    r["log_error_msg"] = log_error_msg
    r["history_obj_id_name"] = history_obj_id_name
    r["history_object_id"] = history_object_id
    r["res"] = res

    return r


def read_df_data(
    md,
    object_type,
    is_dead,
    object_doc,
    log_error_msg,
    load_history,
    history_obj_id_name,
    history_object_id,
    res,
    alg_name="read_data",
    alg_id="0",
    define_as_raw=False,
    retrieve_history_record=False,
    merge_method=0,
    merge_fill_value=None,
    merge_interpolation_samples=0,
):
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
                Database._read_data_from_dfile(
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
                Database._read_data_from_dfile(
                    mspass_object,
                    object_doc["dir"],
                    object_doc["dfile"],
                    object_doc["foff"],
                    merge_method=merge_method,
                    merge_fill_value=merge_fill_value,
                    merge_interpolation_samples=merge_interpolation_samples,
                )
        elif storage_mode == "url":
            Database._read_data_from_url(
                mspass_object,
                object_doc["url"],
                format=None if "format" not in object_doc else object_doc["format"],
            )
        else:
            raise TypeError("Unknown storage mode: {}".format(storage_mode))

        # 3.load history
        if load_history:
            if history_obj_id_name in object_doc:
                if isinstance(mspass_object, TimeSeries):
                    atomic_type = AtomicType.TIMESERIES
                else:
                    atomic_type = AtomicType.SEISMOGRAM

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

        mspass_object.clear_modified()

        # 4.post complaint elog entries if any
        for msg in log_error_msg:
            mspass_object.elog.log_error("read_data", msg, ErrorSeverity.Complaint)

    return mspass_object
