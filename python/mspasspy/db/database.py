import os
import io
import copy
import pathlib
import pickle
import urllib.request
from array import array
import pandas as pd

# WARNING fcntl is unix specific.
# Will fail if run in windows.  Mspass uses docker container
# so this will not be an issue there but if anyone tries to use this
# module outside mspass beware.
import fcntl

try:
    import dask.dataframe as daskdf

    _mspasspy_has_dask = True
except ImportError:
    _mspasspy_has_dask = False


import gridfs
import pymongo
import pymongo.errors
from bson import ObjectId
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

from mspasspy.ccore.seismic import (
    TimeSeries,
    Seismogram,
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
    ProcessingStatus,
    ErrorLogger,
)
from mspasspy.db.collection import Collection
from mspasspy.db.schema import DatabaseSchema, MetadataSchema
from mspasspy.util.converter import Textfile2Dataframe


class Database(pymongo.database.Database):
    """
    MsPASS core database handle.  All MongoDB database operation in MsPASS
    should normally utilize this object.  This object is a subclass of the
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

    .. code-block:: python

      db=Database(dbclient,'mydatabase')

    where dbclient is either a MongoDB database client instance or
    (recommended) the MsPASS DBClient wrapper (a subclass of the
    pymongo client).  The second argument is the database "name"
    passed to the MongoDB server that defines your working database.

    The constructor should normally be used only with serial workflows.
    In cluster environments the recommended way to obtain a
    Database handle is via the DBClient method called `get_database`.
    The typical construct is:

        dbclient = DBClient("dbhostname")
        db = dbclient.get_database("mydatabase")

    where `dbhostname` is the hostname of the node running the MongoDB
    server and `mydatabase` is the name you assign your database.
    Serial workflows can and should use a similar construct but can
    normally default construct DBClient.

    Optional parameters are:

    :param schema: a :class:`str` of the yaml file name that defines
      both the database schema and the metadata schema. If this parameter
      is set, it will override the following two.
    :param db_schema: Set the name for the database schema to use with this
      handle.  Default is the MsPASS schema. (See User's Manual for details)
    :param md_schema:  Set the name for the Metadata schema.   Default is
      the MsPASS definitions.  (see User's Manual for details)

    As a subclass of pymongo Database the constructor accepts any
    parameters defined for the base class (see pymongo documentation)
    """

    def __init__(self, *args, schema=None, db_schema=None, md_schema=None, **kwargs):
        # super(Database, self).__init__(*args, **kwargs)
        super().__init__(*args, **kwargs)
        if schema:
            self.database_schema = DatabaseSchema(schema)
            self.metadata_schema = MetadataSchema(schema)
        else:
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
        # This import has to appear here to avoid a circular import problem
        # the name stedronsky is a programming joke - name of funeral director
        # in the home town of glp
        from mspasspy.util.Undertaker import Undertaker

        self.stedronsky = Undertaker(self)

    def __getstate__(self):
        ret = self.__dict__.copy()
        ret["_BaseObject__codec_options"] = self.codec_options.__repr__()

        # Store connection info to recreate DBClient lazily
        ret["_mspass_db_host"] = self.client._mspass_db_host

        # Don't pickle the client or stedronsky (Undertaker) objects
        if "_Database__client" in ret:
            del ret["_Database__client"]
        if "stedronsky" in ret:
            del ret["stedronsky"]

        return ret

    def __setstate__(self, data):
        from bson.codec_options import CodecOptions, TypeRegistry, DatetimeConversion
        from bson.binary import UuidRepresentation
        from mspasspy.db.client import DBClient
        from mspasspy.util.Undertaker import Undertaker

        # Extract connection info before updating __dict__
        db_host = data.pop("_mspass_db_host", None)

        # CRITICAL: Pop ALL codec_options related fields and eval them
        # pymongo Database has both _codec_options and _BaseObject__codec_options
        codec_options_repr = data.pop("_BaseObject__codec_options", None)
        base_codec_options_repr = data.pop("_codec_options", None)

        # Eval the codec_options repr string
        if codec_options_repr and isinstance(codec_options_repr, str):
            codec_options_obj = eval(codec_options_repr)
        else:
            codec_options_obj = codec_options_repr

        # Update all other attributes EXCEPT codec_options
        self.__dict__.update(data)

        # Set both codec_options attributes with the same object
        # pymongo uses _codec_options internally, but we also need _BaseObject__codec_options
        self._BaseObject__codec_options = codec_options_obj
        self._codec_options = codec_options_obj

        # Recreate DBClient
        if db_host is not None:
            self._Database__client = DBClient(db_host)
        else:
            self._Database__client = DBClient("localhost")

        # Recreate Undertaker
        self.stedronsky = Undertaker(self)

    def __getitem__(self, name):
        """
        Get a collection of this database by name.
        Raises InvalidName if an invalid collection name is used.
        :Parameters:
          - `name`: the name of the collection to get
        """
        return Collection(self, name)

    def get_collection(
        self,
        name,
        codec_options=None,
        read_preference=None,
        write_concern=None,
        read_concern=None,
    ):
        """
        Get a :class:`mspasspy.db.collection.Collection` with the given name
        and options.

        This method is
        useful for creating a :class:`mspasspy.db.collection.Collection` with
        different codec options, read preference, and/or write concern from
        this :class:`Database`.  Useful mainly for advanced users tuning
        a polished workflow.

        :Parameters:
          :param name: The name of the collection - a string.
          :param codec_options: (optional): An instance of
            :class:`bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`Database` is
            used.
          :param read_preference: (optional): The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`Database` is used. See :mod:`pymongo.read_preferences`
            for options.
          :param write_concern: (optional): An instance of
            :class:`pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`Database` is
            used.
          :param read_concern:  An (optional) instance of
            :class:`pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`Database` is
            used.
        """
        return Collection(
            self,
            name,
            False,
            codec_options,
            read_preference,
            write_concern,
            read_concern,
        )

    def create_collection(
        self,
        name,
        codec_options=None,
        read_preference=None,
        write_concern=None,
        read_concern=None,
        session=None,
        **kwargs,
    ):
        """
        Create a new :class:`mspasspy.db.collection.Collection` in this
        database.
        Normally collection creation is automatic. This method should
        only be used to specify options on
        creation. :class:`~pymongo.errors.CollectionInvalid` will be
        raised if the collection already exists.   Useful mainly for advanced users tuning
        a polished workflow.

        Parameters:
          :param name: the name of the collection to create
          :param codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`Database` is
            used.
          :param read_preference: (optional): The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`Database` is used.
          :param write_concern: (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`Database` is
            used.
          :param read_concern: (optional): An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`Database` is
            used.
          :param collation: (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          :param session: (optional): a
            :class:`~pymongo.client_session.ClientSession`.
          :param **kwargs: (optional): additional keyword arguments will
            be passed as options for the `create collection command`_

        All optional `create collection command`_ parameters should be passed
        as keyword arguments to this method. Valid options include, but are not
        limited to:

          ``size``: desired initial size for the collection (in
            bytes). For capped collections this size is the max
            size of the collection.
          ``capped``: if True, this is a capped collection
          ``max``: maximum number of objects if capped (optional)
          ``timeseries``: a document specifying configuration options for
            timeseries collections
          ``expireAfterSeconds``: the number of seconds after which a
            document in a timeseries collection expires
        """
        with self.__client._tmp_session(session) as s:
            # Skip this check in a transaction where listCollections is not
            # supported.
            if (not s or not s.in_transaction) and name in self.list_collection_names(
                filter={"name": name}, session=s
            ):
                raise pymongo.errors.CollectionInvalid(
                    "collection %s already exists" % name
                )

            return Collection(
                self,
                name,
                True,
                codec_options,
                read_preference,
                write_concern,
                read_concern,
                session=s,
                **kwargs,
            )

    def set_metadata_schema(self, schema):
        """
        Change the metadata schema defined for this handle.

        Normal use sets the schema at the time handle is created.
        In rare instances it can be useful to change the schema on the
        fly.   Use this method to do that for the Metadata component.
        An alternative is to create
        a new instance of Database with the new schema, but that approach
        is much slower than using this method.  Whether that matters is
        dependent on the number of times that operation is required.

        :param schema: Specification of the schema to use.  Can be
          a string defining a path to a yaml file defining the schema or
          an instance of :class:`mspsspy.db.schema.MetadataSchema` that
          was previously created by reading such a file.
        :type schema:  :class:`mspsspy.db.schema.MetadataSchema` or a :class:`str`

        """
        if isinstance(schema, MetadataSchema):
            self.metadata_schema = schema
        elif isinstance(schema, str):
            self.metadata_schema = MetadataSchema(schema)
        else:
            raise MsPASSError(
                "Error: argument schema is of type {}, which is not supported".format(
                    type(schema)
                ),
                "Fatal",
            )

    def set_database_schema(self, schema):
        """
        Change the database schema defined for this handle.

        Normal use sets the schema at the time handle is created.
        In rare instances it can be useful to change the schema on the
        fly.   Use this method to do that for the database schema component.
        An alternative is to create
        a new instance of Database with the new schema, but that approach
        is much slower than using this method.  Whether that matters is
        dependent on the number of times that operation is required.

        :param schema: Specification of the schema to use.  Can be
          a string defining a path to a yaml file defining the schema or
          an instance of :class:`mspsspy.db.schema.MetadataSchema` that
          was previously created by reading such a file.
        :type schema:  :class:`mspsspy.db.schema.MetadataSchema` or a :class:`str`
        """
        if isinstance(schema, DatabaseSchema):
            self.database_schema = schema
        elif isinstance(schema, str):
            self.database_schema = DatabaseSchema(schema)
        else:
            raise MsPASSError(
                "Error: argument schema is of type {}, which is not supported".format(
                    type(schema)
                ),
                "Fatal",
            )

    def set_schema(self, schema):
        """
        Use this method to change both the database and metadata schema defined for this
        instance of a database handle.  This method sets the database
        schema (namespace for attributes saved in MongoDB) and the metadata
        schema (interal namespace).

        :param schema: a :class:`str` of the yaml file name.
        """
        self.database_schema = DatabaseSchema(schema)
        self.metadata_schema = MetadataSchema(schema)

    def read_data(
        self,
        id_doc_or_cursor,
        mode="promiscuous",
        normalize=None,
        normalize_ensemble=None,
        load_history=False,
        exclude_keys=None,
        collection="wf",
        data_tag=None,
        ensemble_metadata={},
        alg_name="read_data",
        alg_id="0",
        define_as_raw=False,
        merge_method=0,
        merge_fill_value=None,
        merge_interpolation_samples=0,
        aws_access_key_id=None,
        aws_secret_access_key=None,
    ):
        """
        Top-level MsPASS reader for seismic waveform data objects.

        Most MsPASS processing scripts use this method directly
        or indirectly via the parallel version called
        `read_distributed_data`.  This function will return
        one of four seismic data objects defined in MsPASS:
        `TimeSeries`,`Seismogram`, `TimeSeriesEnsemble`, or `SeismogramEnsemble`.
        What is retrieved is driven by the type of arg0, which in the
        implementation has the symbol `id_doc_or_cursor`.  As the symbol
        name implies it can be one of three things.  Each of the three
        types have implied assumptions:

        1.  If arg0 is a MongoDB `ObjectId` it is presumed to be the ObjectId
            of a document in the collection defined by the `collection`
            argument (see below).  When used in this way a query is
            always required to retrieve the document needed to construct
            the desired datum.  This method always implies you want to
            construct one, atomic datum.  This functionality is not
            recommended as it is the slowest reader due to the implicit
            database query required to implement that approach.
        2.  If arg0 is a python dictionary (dict) it is assumed to be a
            MongoDB "document" retrieved previously through the pymongo
            interface.  This usage is the best use for serial jobs driven
            by a for loop over a MongoDB cursor returned following a find
            query.   (See User's manual and tutorials for specific examples)
            The reason is that a cursor is effectively a buffered interface
            into the MongoDB database.  That is a loop over a cursor
            requires communication with the MongoDB server only when the
            buffer drains to a low water mark.   Consequently, cursor
            loops using this interface are much faster than atomic
            reads with ObjectIds.
        3.  If arg0 is a pymongo `Cursor` object the reader assumes you are
            asking it to construct an ensemble object.  Whether that is a
            `TimeSeriesEnsemble` or `SeismogramEnsemble` is dependent upon the
            setting of the "collection" argument.  The entire content of the
            implied list of documents returned by iterating over the cursor
            are used to construct ensembles of the atomic members.  In fact,
            the basic algorithm is to call this method recursively by
            sequentially reading one document at a time, constructing the
            atomic datum, and posting it to the member vector of the ensemble.

        As noted above arg0 interacts with the "collection" argument.
        The default of "wf_TimeSeries" can be used where appropriate but
        good practice is to be explicit and specify a value for "collection"
        in all alls to this method.

        This reader accepts data in any mix of what are defined by
        the database attribute tags `storage_mode` and `format`.   If those
        attributes are not defined for a retrieved document they default to
        "storage_mode"=="gridfs" and "format"=="binary".  The `storage_mode`
        attribute can currently be one of the following:
        - `gridfs` is taken to mean the data are stored in the MongoDB
           gridfs file system.
        - 'files' implies the data are stored in conventional computer files
          stored with two attributes that are required with this storage
          mode:  "dir" and "dfile".
        - `URL` implies the data can be retrieved by some form of web service
          request through a url defined by other attributes in the document.
          This mode is volatile and currently not recommended due to the very
          slow and unreliable response to FDSN data queries.  It is likely,
          however, to become a major component with FDSN services moving to
          cloud systems.
        - This reader has prototype support for reading SCEC data stored on
          AWS s3.   The two valid values for defining those "storage_mode"s
          are "s3_continuous" and "s3_event", which map to two different
          areas of SCEC's storage on AWS.   We emphasize the readers for
          this storage mode are prototypes and subject to change.  A similar
          interface may evolve for handling FDSN cloud data storage depending
          on how that interface is actually implemented.  It was in development
          at the time this docstring was written.

        The reader can read data in multiple formats. The actual format
        expected is driven by the database attribute called "format".
        As noted above it defaults to "binary", which means the data are
        stored in contiguous binary blocks on files that can be read with
        the low-level C function fread.   That is the fastest reader
        currently available, but comes at storage cast as the data are
        uncompressed doubles.  If the data are in some nonnative format
        (seed is considered not native), the format is cracked using
        obspy.   The list of formats accepted match those defined for
        obspy's `read` function.   The format value stored in the database
        is passed as the format argument to that function.  The miniseed
        format reader has been heavily exercised.  Other formats will be
        an adventure.  Be aware there will most definitely be namespace
        collisions of Metadata attributes with non-native formats other than
        miniseed.

        There is a special case for working with ensemble data that can
        be expected to provide the highest performance with a conventional
        file system.  That is the case of ensembles with the format of
        "binary" and data saved in files where all the waveforms of each
        ensemble are in the same file.  That structure is the default for
        ensembles saved to files with the `save_data` method.  This method
        is fast because the sample data are read with a C++ function that
        uses the stock fread function that for conventional file input is
        about as fast a reader as possible.

        An additional critical parameter is "mode".  It must be one of
        "promiscuous", "cautious", or "pedantic".   Default is "promiscuous"
        which more-or-less ignores the schema and loads the entire content
        of the document(s) to Metadata containers.   For ensembles that
        means the Metadata for the ensemble and all members.  See the
        User Manual section on "CRUD Operations" for details.

        This reader also has an option to normalize with one or more sets of
        data during reading.  For backward compatibility with versions of
        MsPASS prior to 2.0 the "normalize" parameter will accept a list of
        strings that are assumed to be collection names containing
        Metadata that is to be "normalized".    That case, however, invokes the
        slowest algorithm possible to do that operation.  Not only does it
        use a query-based matching algorithm that requires a MongoDB find_one
        query for each datum, but it requires construction of an instance of
        the python class used in MsPASS for each calls and each collection listed.
        Better performance is normally possible with the v2 approach where
        the list passed via "normalize" is a list of subclasses of the
        :class:`mspasspy.db.normalize.BasicMatcher` class.  That interface
        provides a generic matching functionality and is most useful for
        improving performance when using one of the cache algorithms.
        For more detail see the User Manual section on normalization.

        Finally, this reader has to handle a special case of errors that
        cause the result to be invalid.  We do NOT use the standard
        exception mechanism for this reader because as noted in our User
        Manual throwing exceptions can abort a large parallel job.  Hence,
        we must try to minimize the cases when a single read operation will
        kill a larger workflow.  (That is reserved for truly unrecoverable
        errors like a malloc error.)  Throughout MsPASS we handle this issue
        with the `live` attribute of data objects.   All data objects,
        including ensembles, can be killed (marked data or live==False) as
        a signal they can be moved around but should be ignored in any
        processing workflows.  Reading data in is a special case.  A
        single read may fail for a variety of reasons but killing the entire
        job for something like one document containing an invalid attribute
        is problematic.  For that reason, this reader defines the concept
        of a special type of dead datum it calls an "abortion".   This
        concept is discussed in greater detail in the User's Manual section on
        "CRUD Operations", but for this context there are three key points:

        -  Any return from this function that is marked dead (live==False)
           is by definition an abortion.
        -  This reader adds a boolean attribute not stored in the database
           with the key "is_abortion".  That value will be True if the datum
           is returned dead but should be False if it is set live.
        -  An entire ensemble may be marked dead only if reading of all the
           members defined by a cursor input fail.   Note handling of
           failures when constructing ensembles is different than atomic
           data because partial success is assumed to be acceptable.
           Hence, when a given datum in an ensemble fails the body is not
           added to the ensemble but processed the body is buried in a
           special collection called "abortions".   See User's Manual
           for details.

        :param id_doc_or_cursor: required key argument that drives
          what the reader will return.  The type of this argument is a
          top-level control on what is read.   See above for details.
        :type id_doc_or_cursor:  As the name implies should be one of
          the following:  (1) MongoDB `ObjectId` of wf document to be
          retrieved to define this read, (2) MongoDB document (python dict)
          of data defining the datum to be constructed, or (3) a pymongo
          `Cursor` object that can be iterated to load an enemble.  Note
          the "doc" can also be a Metadata subclass.  That means you can
          use a seismic data object as input and it will be accepted.
          The read will work, however, only if the contents of the Metadata
          container are sufficient.  Use of explicit or implicit
          Metadata container input is not advised even it it might work as it
          violates an implicit assumption of the function that the input is
          closely linked to MongoDB.  A doc from a cursor or one retrieved
          through an ObjectId match that assump0tion but an Metadata
          container does not.

        :param mode: read mode that controls how the function interacts with
          the schema definition.   Must be one of
          ['promiscuous','cautious','pedantic'].   See user's manual for a
          detailed description of what the modes mean.  Default is 'promiscuous'
          which turns off all schema checks and loads all attributes defined for
          each object read.
        :type mode: :class:`str`

        :param normalize: Use this parameter to do normalization during read.
          From version 2 onward the preferred input is a list of
          concrete instances of the base class :class:`BasicMatcher`.
          For backward compatibility this parameter may also defined a
          list of collection names defined by a list of strings.
          Note all normalizers will, by default,
          normally kill any datum for which matching fails.  Note for
          ensembles this parameter defines matching to be applied to all
          enemble members.  Use `normalize_ensemble` to normalize the enemble's
          `Metadata` container.  Member ensemble normalziation can be different
          for each member while ensemble Metadata is assume  the same for
          all members.
        :type normalize: a :class:`list` of :class:`BasicMatcher` or :class:`str`.
          :class:`BasicMatchers` are applied sequentialy with the
          `normalize` function using this matcher.   When a list of strings is given each
          call to this function initiates construction of a database Id
          matcher using the collection name.   e.g. if the list has "source"
          the wf read is expected to contain the attribute "source_id"
          that resolves to an id in the source collection.  With string
          input that always happens only through database transactions.

        :param normalize_ensemble:  This parameter should be used to
          apply normalization to ensemble Metadata (attributes common to
          all ensemble "members". )  It will be ignored if reading
          atomic data.  Otherwise it behaves like normalize
        :type normalize_ensemble:  a :class:`list` of :class:`BasicMatcher` or :class:`str`.
          :class:`BasicMatchers` are applied sequentialy with the
          `normalize` function.   When a list of strings is given each
          call to this function initiates construction of an database Id
          matcher using the collection name.   e.g. if the list has "source"
          the wf read is expected to contain the attribute "source_id"
          that resolves to an id in the source collection.  With string
          input that always happens only through database transactions.

        :param load_history: boolean switch controlling handling of the
          object-level history mechanism.  When True if the data were
          saved previously with a comparable switch to save the
          history, this reader will load the history data.   This feature allows
          preserving a complete object-level history tree in a workflow
          with an intermediate save.   If no history data are found the
          history tree is initialized. When this parameter is set False
          the history tree will be left null.

        :param exclude_keys: Sometimes it is helpful to remove one or more
          attributes stored in the database from the data's Metadata (header)
          so they will not cause problems in downstream processing.  Use this
          argument to supply a list of keys that should be deleted from
          the datum before it is returned.  With ensembles the editing
          is applied to all members.   Note this same functionality
          can be accomplished within a workflow with the trace editing
          module.
        :type exclude_keys: a :class:`list` of :class:`str` defining
          the keys to be cleared.

        :param collection:  Specify the collection name for this read.
          In MsPASS the equivalent of header attributes are stored in
          MongoDB documents contained in a "collection".   The assumption
          is a given collection only contains documents for one of the
          two atomic types defined in MsPASS:  `TimeSeries` or `Seismogram`.
          The data type for collections defined by the schema is an
          attribute that the reader tests to define the type of atomic
          objects to be constructed (Note ensembles are assembled from
          atomic objects by recursive calls the this same read method.)
          The default is "wf_TimeSeries", but we recommend always defining
          this argument for clarity and stability in the event the default
          would change.
        :type collection:   :class:`str` defining a collection that must be
          defined in the schema.  If not, the function will abort the job.

        :param data_tag:  The definition of a dataset can become ambiguous
          when partially processed data are saved within a workflow.   A common
          example would be windowing long time blocks of data to shorter time
          windows around a particular seismic phase and saving the windowed data.
          The windowed data can be difficult to distinguish from the original
          with standard queries.  For this reason we make extensive use of "tags"
          for save operations to avoid such ambiguities.   Reads present
          a different problem as selection for such a tag is best done with
          MongoDB queries.   If set this argument provides a cross validation
          that the data are consistent with a particular tag.  In particular,
          if a datum does not have the attribute "data_tag" set to the value
          defined by the argument a null, dead datum will be returned.
          For ensembles any document defined by the cursor input for which
          the "data_tag" attribute does not match will be silenetly dropped.
          The default is None which disables the cross-checking.
        :type data_tag: :class:`str` used for the filter.  Can be None
          in which case the cross-check test is disable.

        :param ensemble_metadata:  Optional constant attributes to
          assign to ensemble Metadata.  Ignored for atomic data.
          It is important to stress that the contents of this dict are
          applied last.  Use with caution, particularly when using
          normalizatoin, to avoid accidentally overwriting some
          attribute loaded from the database.
        :type ensemble_metadata:  python dict.   The contents are copied
          verbatim to the ensemble Metadata container with a loop over the
          dict keys.

        :param alg_name: optional alternative name to assign for this algorithm
          as a tag for the origin node of the history tree.   Default is the
          function's name ("read_data") and should rarely if ever be changed.
          Ignored unless `load_history` is set True.
        :type alg_name: :class:`str`

        :param alg_id: The object-level history mechanism uses a string to
          tag a specific instance of running a particular function in a workflow.
          If a workflow has multiple instances of read_data with different
          parameters, for example, one might specify a value of this argument.
          Ignored unless `load_history` is set True.
        :type alg_id: :class:`str`

        :param define_as_raw: boolean to control a detail of the object-level
          history definition of the tree node created for this process.
          The functions by definition an "origin" but a special case is a
          "raw" origin meaning the sample data are unaltered (other than type)
          from the field data.  Most miniseed data, for example, would
          want to set this argument True.   Ignored unless `load_history` is set True.

        :param merge_method:  when reading miniseed data implied gaps can
          be present when packets are missing from a sequence of packets
          having a common net:sta:chan:loc codes.  We use obspy's
          miniseed reader to crack miniseed data.  It breaks such data into
          multiple "segments".  We then use their
          `merge<https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.merge.html>`__
          method of the "Stream" object to glue any such segments together.
          This parameter is passed as the "method" argument to that function.
          For detail see
          `__add__ <https://docs.obspy.org/packages/autogen/obspy.core.trace.Trace.__add__.html#obspy.core.trace.Trace.__add__>`
          for details on methods 0 and 1,
          See `_cleanup <https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream._cleanup.html#obspy.core.stream.Stream._cleanup>`
          for details on method -1.  Note this argument is ignored unless
          the reader is trying to read miniseed data.
        :type method: :class:`int` with one of three values: -1, 0, or 1

        :param merge_fill_value: Fill value for gap processing when obspy's merge
          method is invoked.  (see description for "merge_method" above).
          The value given here is passed used as the "fill" argument to
          the obspy merge method.  As with merge_method this argument is
          relevant only when reading miniseed data.
        :type merge_fill_value: :class:`int`, :class:`float` or None (default)

        :param merge_interpolation_samples: when merge_method is set to
          -1 the obspy merge function requires a value for an
          argument they call "interpolate_samples".  See their documentation
          for details, but this argument controls how "overlaps", as opposed
          to gaps, are handled by merge.  See the function documentation
          `here<https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.merge.html>`__
          for details.
        :type interpolation_samples: :class:`int`

        :return: for ObjectId python dictionary values of arg0 will return
          either a :class:`mspasspy.ccore.seismic.TimeSeries`
          or :class:`mspasspy.ccore.seismic.Seismogram` object.
          If arg0 is a pymongo Cursor the function will return a
          :class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` or
          :class:`mspasspy.ccore.seismic.SeismogramEnsemble` object.
          As noted above failures in reading atomic data will result in
          an object be marked dead and the Metadata attribute "is_abortion"
          to be set True.  When reading ensembles any problem members will
          be excluded from the output and the bodies buried in a special
          collection called "abortions".
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
            normalizer_list = []
        elif len(normalize) == 0:
            normalizer_list = []
        else:
            normalizer_list = parse_normlist(normalize, self)
        # same for ensemble
        if normalize_ensemble is None:
            normalize_ensemble_list = []
        elif len(normalizer_list) == 0:
            normalize_ensemble_list = []
        else:
            normalize_ensemble_list = parse_normlist(normalize_ensemble, self)

        if exclude_keys is None:
            exclude_keys = []

        # This assumes the name of a metadata schema matches the data type it defines.
        read_metadata_schema = self.metadata_schema[object_type.__name__]
        database_schema = self.database_schema[wf_collection]

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
        doclist = []
        # We could use a zero length doclist as switch to define
        # atomic reads, but this boolean will make that logic
        # clearer below
        reading_atomic_data = True
        col = self[wf_collection]
        # This logic allows object_id to be either a document or
        # an object id
        if isinstance(id_doc_or_cursor, (dict, Metadata)):
            # note because all seismic data objects inherit Metadata
            # this block will be entered if passed a seismic data object.
            if isinstance(id_doc_or_cursor, Metadata):
                object_doc = dict(id_doc_or_cursor)
            else:
                object_doc = id_doc_or_cursor
            if data_tag:
                if "data_tag" not in object_doc or object_doc["data_tag"] != data_tag:
                    if object_type is TimeSeries:
                        return TimeSeries()
                    else:
                        return Seismogram()
        elif isinstance(id_doc_or_cursor, ObjectId):
            oid = id_doc_or_cursor
            query = {"_id": oid}
            if data_tag:
                query["data_tag"] = data_tag
            object_doc = col.find_one(query)
            if not object_doc:
                if object_type is TimeSeries:
                    d = TimeSeries()
                else:
                    d = Seismogram()
                message = "No matching document for the followng query:  {}".format(
                    query
                )
                message = "Returning default constructed datum"
                d.elog.log_error("Database.read_data", message, ErrorSeverity.Invalid)
                return d
        elif isinstance(id_doc_or_cursor, pymongo.cursor.Cursor):
            for doc in id_doc_or_cursor:
                if data_tag:
                    # silently drop any documents that do not have data tag
                    # and match input value
                    if "data_tag" in doc and doc["data_tag"] == data_tag:
                        doclist.append(doc)
                else:
                    doclist.append(doc)
            reading_atomic_data = False
            # return default constructed ensemble with a error message if
            # editing for data tag above threw out all data
            if len(doclist) == 0:
                message = "Found no data matching data_tag={} in list created from cursor\n".format(
                    data_tag
                )
                message += "Returning an empty ensemble marked dead"
                if object_type is TimeSeries:
                    val = TimeSeriesEnsemble()
                else:
                    val = SeismogramEnsemble()
                val.elog.log_error("Database.read_data", message, ErrorSeverity.Invalid)
                return val
        else:
            message = "Database.read_data:  arg0 has unsupported type={}\n".format(
                str(type(id_doc_or_cursor))
            )
            message += "Must be a dict (document), ObjectId, or pymongo cursor\n"
            raise TypeError(message)

        if reading_atomic_data:
            md, live, elog = doc2md(
                object_doc,
                database_schema,
                read_metadata_schema,
                wf_collection,
                exclude_keys,
                mode,
            )
            if not live:
                # Default construct datum and add Metadata afterward.
                # Metadata needs to be added to body bag to allow idenficiation
                # of the body downstream
                if object_type is TimeSeries:
                    d = TimeSeries()
                else:
                    d = Seismogram()
                for k in md:
                    d[k] = md[k]
                d.elog += elog
                # A simple way to signal this is an abortion downstream
                d["is_abortion"] = True
                # not necessary because default constructor seets
                # live false but safer to be explicit this is most sincerely dead
                d.kill()
                return d
            mspass_object = self._construct_atomic_object(
                md,
                object_type,
                merge_method,
                merge_fill_value,
                merge_interpolation_samples,
                aws_access_key_id,
                aws_secret_access_key,
            )
            if elog.size() > 0:
                # Any messages posted here will be out of order if there are
                # also messages posted by _construct_atomic_object but
                # that is a minor blemish compared to the hassle required
                # to fix it
                mspass_object.elog += elog
            if mspass_object.live:
                mspass_object["is_abortion"] = False
                # We normalize after construction to allow use of normalize
                # function feature of kill and error logging
                # this obnoxious import is necessary because of a name
                # collsion of the function normalize and the argument
                # normalize in this method
                import mspasspy.db.normalize as normalize_module

                for matcher in normalizer_list:
                    if mspass_object.live:
                        # scope qualifier needed to avoid name collsion with normalize argument
                        mspass_object = normalize_module.normalize(
                            mspass_object, matcher
                        )
                    else:
                        break
                # normalizers can kil
                if mspass_object.dead():
                    mspass_object["is_abortion"] = True
                mspass_object.clear_modified()
                # Since the above can do a kill, we don't bother with the
                # history if the datum was killed during normalization
                if mspass_object.live and load_history:
                    history_obj_id_name = (
                        self.database_schema.default_name("history_object") + "_id"
                    )
                    if history_obj_id_name in object_doc:
                        history_id = object_doc[history_obj_id_name]
                    else:
                        # None is used as a signal this is the
                        # initialization of a workflow so the
                        # result will be marked such that the
                        # is_origin method returns True
                        history_id = None
                    self._load_history(
                        mspass_object,
                        history_id,
                        alg_name=alg_name,
                        alg_id=alg_id,
                        define_as_raw=define_as_raw,
                    )

            else:
                mspass_object["is_abortion"] = True

        else:
            mdlist, live, elog, abortions = doclist2mdlist(
                doclist,
                database_schema,
                read_metadata_schema,
                wf_collection,
                exclude_keys,
                mode,
            )

            # This is a special method of Undertaker to handle the
            # kind of data returned in the "abortions" list.
            # See Undertaker docstring or User Manual for concepts
            if len(abortions) > 0:
                for md in abortions:
                    self.stedronsky.handle_abortion(md)
            if live and len(mdlist) > 0:
                # note this function returns a clean ensemble with
                # no dead data.  It will be considered bad only if is empty
                # note it send any datum that failed during construction
                # to abortions using the Undertaker method "handle_abortions"
                # as done immediatley above.  Also set ensemble dead if
                # there are no live mebmers
                ensemble = self._construct_ensemble(
                    mdlist,
                    object_type,
                    merge_method,
                    merge_fill_value,
                    merge_interpolation_samples,
                    aws_access_key_id,
                    aws_secret_access_key,
                )
                if normalize or normalize_ensemble:
                    import mspasspy.db.normalize as normalize_module

                    # first handle enemble Metadata
                    if normalize_ensemble:
                        for matcher in normalize_ensemble_list:
                            # use this conditional because normalizers can kill
                            if mspass_object.live:
                                # scope qualifier needed to avoid name collsion with normalize argument
                                ensemble = normalize_module.normalize(ensemble, matcher)
                            else:
                                break
                    # these are applied to all live members
                    if normalize:
                        for d in ensemble.member:
                            # note this silently skipss dead data
                            if d.live:
                                for matcher in normalizer_list:
                                    d = normalize_module.normalize(d, matcher)
                                    if d.dead():
                                        d["is_abortion"] = True
                                        break
                    if load_history:
                        history_obj_id_name = (
                            self.database_schema.default_name("history_object") + "_id"
                        )
                        for d in ensemble.member:
                            if d.live:
                                if d.is_defined(history_obj_id_name):
                                    history_id = d[history_obj_id_name]
                                else:
                                    history_id = None
                                self._load_history(
                                    mspass_object,
                                    history_id,
                                    alg_name=alg_name,
                                    alg_id=alg_id,
                                    define_as_raw=define_as_raw,
                                )
                    # make sure we didn't kill all members
                    kill_me = True
                    for d in ensemble.member:
                        if d.live:
                            kill_me = False
                            break
                    if kill_me:
                        ensemble.kill()

            else:
                # Default constructed container assumed marked dead
                if object_type is TimeSeries:
                    ensemble = TimeSeriesEnsemble()
                else:
                    ensemble = SeismogramEnsemble()
            # elog referenced here comes from doclist2mdlist - a bit confusing
            # but delayed to here to construct ensemble first
            if elog.size() < 0:
                ensemble.elog += elog

            # We post this even to dead ensmebles
            if len(ensemble_metadata) > 0:
                ensmd = Metadata(ensemble_metadata)
                ensemble.update_metadata(ensmd)
            mspass_object = ensemble  # ths just creates an alias not a copy

        return mspass_object

    def save_data(
        self,
        mspass_object,
        return_data=False,
        return_list=["_id"],
        mode="promiscuous",
        storage_mode="gridfs",
        dir=None,
        dfile=None,
        format=None,
        overwrite=False,
        exclude_keys=None,
        collection=None,
        data_tag=None,
        cremate=False,
        save_history=True,
        normalizing_collections=["channel", "site", "source"],
        alg_name="save_data",
        alg_id="0",
    ):
        """
        Standard method to save all seismic data objects to be managed by
        MongoDB.

        This method provides a unified interface for saving all seismic
        data objects in MsPASS.   That is, what we call atomic data
        (:class:`mspasspy.ccore.seismic.TimeSeries` and
        :class:`mspasspy.ccore.seismic.Seismogram`) and ensembles of the
        two atomic types ():class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` and
        :class:`mspasspy.ccore.seismic.SeismogramEnsemble`).  Handling
        multiple types while simultaneously supporting multiple abstractions
        of how the data are stored externally has some complexities.

        1.  All MsPASS data have multiple containers used internally
            to define different concepts.   In particular atomic data
            have a Metadata container we map to MongoDB documents directly,
            sample data that is the largest data component that we handle
            multiple ways, error log data, and (optional) object-level
            history data.  Ensembles are groups of atomic data but they also
            contain a Metadata and error log container common with content
            common to the group.
        2.  With seismic data the size is normally dominated by the
            sample data that are stored internally as a vector container
            for `TimeSeries` objects and a matrix for `Seismogram` objects.
            Handling moving that data to storage as fast as possible is thus the
            key to optimize write performance.  The write process is further
            complicated, however, by the fact that "write/save" is itself
            an abstraction that even in FORTRAN days hid a lot of complexity.
            A call to this function supports multiple save mechanisms we
            define through the `storage_mode` keyword. At present
            "storage_mode" can be only one of two options:  "file" and
            "gridfs".  Note this class has prototype code for reading data
            in AWS s3 cloud storage that is not yet part of this interface.
            The API, however, was designed to allow adding one or more
            "storage_mode" options that allow other mechanisms to save
            sample data.  The top priority for a new "storage_mode" when
            the details are finalized is cloud storage of FDSN data by
            Earthscope and other data centers.  We have also experimented
            with parallel file containers like HDF5 for improved IO performance
            but have not yet produced a stable implementation.   In the
            future look for all such improvements.  What is clear is the
            API will define these alternatives through the "storage_mode"
            concept.
        3.  In MsPASS we make extensive use of the idea from seismic reflection
            processing of marking problem data "dead".   This method
            handles all dead data through a standard interface with the
            memorable/colorful name :py:class:`mspasspy.util.Undertaker`.
            The default save calls the "bury" method of that class, but
            there is an optional "cremate" argument that calls that
            method instead.  The default of "bury" writes error log data
            to a special collection ("cemetery") while the "cremate" method
            causes dead data to vanish without a trace on save.
        4.  There are two types of save concepts this method has to support.
            That is, sometimes one needs to save intermediate results of
            a workflow and continue on doing more work on the same data.
            The more common situation is to terminate a workflow with
            a call to this method.  The method argument `return_data`
            is an implicit switch for these two concepts.  When
            `return_data` is False, which is the default, only the
            ObjectId(s) of thw wf document(s) are returned by the function.
            We use that as the default to avoid memory overflow in a
            final save when the `compute` method is called in dask or the
            `collect` method is called in pyspark.  Set the `return_data`
            argument True if you are doing an intermediate save and want
            to reuse the content of what is returned.
        5.  There is a long history in seismology of debate on data formats.
            The fundamental reason for much of the debate has to do with
            the namespace and concepts captured in data "headers" for
            different formats seismologists have invented over several
            decades.   A type example is that two "standard" formats
            are SEGY that is the universal standard in seismic reflection
            data exchange and SEED which is now the standard earthquakek
            data format (Technically most data is now "miniseed" which
            differs from SEED by defining only minimal header attributes
            compared to the abomination of SEED that allows pretty much
            anything stored in an excessively complex data structure.)
            Further debate occurs regarding how data sets are stored in
            a particular format.   These range from the atomic level file
            model of SAC to the opposite extreme of an entire data set
            stored in one file, which is common for SEGY.   Because of the
            complexity of multiple formats seismologists have used for
            the external representation of data, this writer has limits
            on what it can and cannot do.  Key points about how formatting
            is handled are:
            -  The default save is a native format that is fast and efficient.
               You can select alternative formats for data by setting a valid
               value (string) for the "format" argument to this method.
               What is "valid" for the format argument is currently simple
               to state:   the value received for format is passed directly
               to obspy's  Stream write method's format argument.  That is,
               realize that when saving to a nonnative format the first thing
               this method does is convert the data to a Stream.  It then
               calls obspy's write method with the format specified.
            -  Given the above, it follows that if you are writing data to
               any nonnative format before calling this function you will
               almost certainly have to edit the Metadata of each datum to
               match the namespace for the format you want to write.
               If not, obspy's writer will almost certainly fail.  The only
               exception is miniseed where if the data being processed originate
               as miniseed they can often be saved as miniseed without edits.
            -  This writer only saves atomic data on single ensembles and
               knows nothing about assumptions a format makes about the
               larger scale layout of a dataset.  e.g. you could theoretically
               write an ensemble in SAC format, but the output will be
               unreadable by SAC because multiple records could easily be written
               in a single file.  Our perspective is that MsPASS is a framework
               and we do not have resources to sort out all the complexities of
               all the formats out there.   We request the user community to
               share an development for nonstandard formats.   Current the
               only format options that are not "use at your own risk" are
               the native "binary" format and miniseed.

        Given the complexity just described, users should not be surprised
        that there are limits to what this single method can do and that
        evolution of its capabilities is inevitable as the IT world evolves.
        We reiterate the entire point of this method (also the related
        read_data method) is to abstract the save process to make it
        as simple as possible while providing mechanisms to make it work
        as efficiently as possible.

        There are some important implementation details related to
        the points above that are important to understand if you encounter
        issues using this algorithm.

        1.  Writing is always atomic.  Saving ensemble data is little more
            than an enhanced loop over data members.  By "enhanced" we mean
            two things:  (a)  any ensemble Metadata attributes are copied
            to the documents saved for each member, and (b) dead data have
            to be handled differently but the handling is a simple conditional
            of "if live" with dead data handled the same as atomic bodies.
        2.  Atomic saves handle four components of the MsPASS data objects
            differently.   The first thing saved is always the sample data.
            How that is done depends upon the storage mode and format
            discussed in more detail below.  The writer then saves the
            content of the datum's Metadata container returning a copy of
            the value of the ObjectId of the document saved.   That ObjectID
            is used as a cross reference for saving the final two fundamentally
            different components:  (a) any error log entries and (b) object-level
            history data (optional).
        3.  As noted above how the sample data is stored is driven by the
            "storage_mode" argument.  Currently two values are accepted.
            "gridfs", which is the default, pushes the sample data to a
            MongoDB managed internal file space the documents refer to with
            that name.  There are two important caveats about gridfs storage.
            First, the data saved will use the same file system as that used
            by the database server.  That can easily cause a file-system full
            error or a quota error if used naively.   Second, gridfs IO is
            prone to a serious preformance issue in a parallel workflow as
            all workers can be shouting at the database server simultaneously
            filling the network connection and/or overloading the server.
            For those reasons, "file" should be used for the storage
            mode for most applications.   It is not the default because
            storing data in files always requires the user to implement some
            organizational scheme through the "dir" and "dfile" arguments.
            There is no one-size-fits-all solution for organizing how a
            particular project needs to organize the files it produces.
            Note, however, that one can set storage_mode to "file" and this
            writer will work.  By default is sets "dir" to the current directory
            and "dfile" to a random string created with a uuid generator.
            (For ensembles the default is to write all member data to
            the file defined explicitly or implicitly by the "dir" and "dfile"
            arguments.)   An final implementation detail is that if "dir"
            and "dfile" are left at the default None, the algorithm will
            attempt to extract the value of "dir" and "dfile" from the
            Metadata of an atomic datum referenced in the save (i.e. for
            ensembles each datum will be treated independently.).  That
            feature allows more complex data organization schemes managed
            externally from the writer for ensembles.  All of that was
            designed to make this method as bombproof as possible, but
            users need to be aware naive usage can create a huge mess.
            e.g. with "file" and null "dir" and "dfile" saving a million
            atomic data will create a million files with random names in
            your current directory.  Good luck cleaning that mess up.
            Finally, we reiterate that when Earthscope finishes their
            design work for cloud storage access there will probably be
            a new storage mode added to support that access.  The "s3"
            methods in `Database` should be viewed only as prototypes.
        4.  If any datum has an error log entry it is always saved by
            this method is a collection called "elog".  That save is not
            optional as we view all error log entries as significant.
            Each elog document contains an ObjectId of the wf document
            with which it is associated.
        5.  Handling of the bodies of dead data has some complexity.
            Since v2 of `Database` all dead data are passed through an
            instance of :py:class:`mspasspy.util.Undertaker`.  By default
            atomic dead data are passed through the
            :py:meth:`mspasspy.util.Undertaker.bury` method.
            If the "cremate" argument is set True the
            :py:meth:`mspasspy.util.Undertaker.cremate` method will be called.
            For ensembles the writer calls the
            :py:meth:`mspasspy.util.Undertaker.bring_out_your_dead`.
            If cremate is set True the bodies are discarded. Otherwise
            the `bury` method is called in a loop over all bodies.
            Note the default `bury` method creates
            a document for each dead datum in one of two
            special collections:  (a) data killed by a processing algorithm
            are saved to the "cemetery" collection, and (b) data killed by
            a reader are saved to "abortions" (An abortion is a datum
            that was never successfully constructed - born.)  The documents
            for the two collections contain the same name-value pairs but
            we split them because action required by an abortion is
            very different from a normal kill.   See the User Manual
            section on "CRUD Operations" for information on this topic.
        6.  Object-level history is not saved unless the argument
            "save_history" is set True (default is False).   When enabled
            the history data (if defined) is saved to a collection called
            "history".   The document saved, like elog, has the ObjectId of
            the wf document with which it is associated.  Be aware that there
            is a signficant added cost to creating a history document entry.
            Because calling this method is part of the history chain one
            would want to preserve, an atomic database operation is
            required for each datum saved.   That is, the algorithm
            does an update of the wf document with which it it is associated
            containing the ObjectId of the history document it saved.
        7.  As noted above saving data with "format" set to anything but
            the default "binary" or "MSEED" is adventure land.
            We reiterate saving other formats is a development frontier we
            hope to come as contributions from users who need writers for a particular
            format and are intimately familiar with that format's idiosyncracies.
        8.  The "mode" argument has a number of subtle idiosyncracies
            that need to be recognized.  In general we recommend always
            writing data (i.e. calling this method) with the default
            mode of "promiscuous".  That guarantees you won't mysteriously
            lose data being saved or abort the workflow when it is finished.
            In general it is better to use one of the verify methods or the
            dbverify command line tool to look for problem attributes in
            any saved documents than try to enforce rules setting
            mode to "cautious" or "pedantic".   On the other hand, even
            if running in "promiscuous" mode certain rules are enforced:
             -  Any aliases defined in the schema are always reset to the
                key defined for the schema.  e.g. if you used the alias
                "dt" for the data sample interval this writer will always
                change it to "delta" (with the standard schema anyway)
                before saving the document created from that datum.
             -  All modes enforce a schema constraint for "readonly"
                attributes.   An immutable (readonly) attribute by definition
                should not be changed during processing.   During a save
                all attributes with a key defined as readonly are tested
                with a method in the Metadata container that keeps track of
                any Metadata changes.  If a readonly attribute is found to
                have been changed it will be renamed with the prefix
                "READONLYERROR_", saved, and an error posted (e.g. if you try
                to alter site_lat (a readonly attribute) in a workflow when
                you save the waveform you will find an entry with the key
                READONERROR_site_lat.)  We emphasize the issue happens if
                the value associated with such a key was altered after the
                datum was constructed.  If the attribute does not
                change it is ERASED and will not appear in the document.
                The reason for that is this feature exists to handle
                attributes loaded in normalization.   Be aware this
                feature can bloat the elog collection if an entire dataset
                share a problem this algorithm flags as a readonly error.

        This method can throw an exception but only for errors in
        usage (i.e. arguments defined incorrectly)

        :param mspass_object: the seismic object you want to save.
        :type mspass_object: :class:`mspasspy.ccore.seismic.TimeSeries`,
          :class:`mspasspy.ccore.seismic.Seismogram`,
          :class:`mspasspy.ccore.seismic.TimeSeriesEnsemble`, or
          :class:`mspasspy.ccore.seismic.SeismogramEnsemble`
        :param return_data: When True return a (usually edited) copy
          of the data received.   When False, the default, return only
          a requested subset of the attributes in the wf document saved
          for this datum.s
        :param return_list:  list of keys for attributes to extract
          and set for return python dictionary when return_data is False.
          Ignored if return_data is True.   Be warned for ensembles the
          attributes are expected to be in the ensemble Metadata container.
          Missing attributes will be silentely ignored so an empty dict
          will be returned if none of the attributes is this list are defined.
        :param mode: This parameter defines how attributes defined with
          key-value pairs in MongoDB documents are to be handled on reading.
          By "to be handled" we mean how strongly to enforce name and type
          specification in the schema for the type of object being constructed.
          Options are ['promiscuous','cautious','pedantic'] with 'promiscuous'
          being the default.  See the User's manual for more details on
          the concepts and how to use this option.
        :type mode: :class:`str`
        :param storage_mode: Current must be either "gridfs" or "file.  When set to
          "gridfs" the waveform data are stored internally and managed by
          MongoDB.  If set to "file" the data will be stored in a file system
          with the dir and dfile arguments defining a file name.   The
          default is "gridfs".  See above for more details.
        :type storage_mode: :class:`str`
        :param dir: file directory for storage.  This argument is ignored if
          storage_mode is set to "gridfs".  When storage_mode is "file" it
          sets the directory in a file system where the data should be saved.
          Note this can be an absolute or relative path.  If the path is
          relative it will be expanded with the standard python library
          path functions to a full path name for storage in the database
          document with the attribute "dir".  As for any io we remind the
          user that you much have write permission in this directory.
          Note if this argument is None (default) and storage_mode is "file"
          the algorithm will first attempt to extract "dir" from the
          Metadata of mspass_object.  If that is defined it will be used
          as the write directory. If it is not defined it will default to
          the current directory.
        :type dir: :class:`str`
        :param dfile: file name for storage of waveform data.  As with dir
          this parameter is ignored if storage_mode is "gridfs" and is
          required only if storage_mode is "file".   Note that this file
          name does not have to be unique.  The writer always positions
          the write pointer to the end of the file referenced and sets the
          attribute "foff" to that position. That allows automatic appends to
          files without concerns about unique names.  Like the dir argument
          if this argument is None (default) and storage_mode is "file"
          the algorithm will first attempt to extract "dfile" from the
          Metadata of mspass_object.  If that is defined it will be used
          as the output filename. If it is not defined a uuid generator
          is used to create a file name with a random string of characters.
          That usage is never a good idea, but is a feature added to make
          this method more bombproof.
        :type dfile: :class:`str`
        :param format: the format of the file. This can be one of the
          `supported formats <https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.write.html#supported-formats>`__
          of ObsPy writer. The default the python None which the method
          assumes means to store the data in its raw binary form.  The default
          should normally be used for efficiency.  Alternate formats are
          primarily a simple export mechanism.  See the User's manual for
          more details on data export.  Used only for "file" storage mode.
          A discussion of format caveats can be found above.
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
          from being written to the output documents.  Note if the data being
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
        :param cremate:  boolean controlling how dead data are handled.
          When True a datum marked dead is ignored and the body or a
          shell of the body (what depends on the return_list value) is
          returned.  If False (default) the
          :py:meth:`mspasspy.util.Undertaker.bury` method is called
          with body as input.  That creates a document for each dead
          datum either in the "cemetery" or "abortions" collection.
          See above for more deails.
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
        :param save_history:   When True the optional history data will
          be saved to the database if it was actually enabled in the workflow.
          If the history container is empty will silently do nothing.
          Default is False meaning history data is ignored.
        :return: python dict of requested attributes by default.  Edited
          copy of input when return_data is True.
        """
        if not isinstance(
            mspass_object,
            (TimeSeries, Seismogram, TimeSeriesEnsemble, SeismogramEnsemble),
        ):
            message = "Database.save_data:  arg0 is illegal type={}\n".format(
                str(type(mspass_object))
            )
            message += "Must be a MsPASS seismic data object"
            raise TypeError(message)
        # WARNING - if we add a storage_mode this will need to change
        if storage_mode not in ["file", "gridfs"]:
            raise TypeError(
                "Database.save_data:  Unsupported storage_mode={}".format(storage_mode)
            )
        if mode not in ["promiscuous", "cautious", "pedantic"]:
            message = "Database.save_data:  Illegal value of mode={}\n".format(mode)
            message += (
                "Must be one one of the following:  promiscuous, cautious, or pedantic"
            )
            raise MsPASSError(message, "Fatal")

        if mspass_object.live:
            # We remove dead bodies from ensembles to simplify saving
            # data below.
            if isinstance(mspass_object, (TimeSeriesEnsemble, SeismogramEnsemble)):
                mspass_object, bodies = self.stedronsky.bring_out_your_dead(
                    mspass_object
                )
                if not cremate and len(bodies.member) > 0:
                    self.stedronsky.bury(bodies)
            # schema isn't needed for handling the dead, but we do need it
            # for creating wf documents
            schema = self.metadata_schema
            if isinstance(mspass_object, (TimeSeries, TimeSeriesEnsemble)):
                save_schema = schema.TimeSeries
            else:
                # careful - above test for all alllowed data currently makes
                # this only an else.  If more data types are added this will
                # break
                save_schema = schema.Seismogram

            if collection:
                wf_collection_name = collection
            else:
                # This returns a string that is the collection name for this atomic data type
                # A weird construct
                wf_collection_name = save_schema.collection("_id")
            wf_collection = self[wf_collection_name]
            # We need to make sure storage_mode is set in all live data
            if isinstance(mspass_object, (TimeSeriesEnsemble, SeismogramEnsemble)):
                for d in mspass_object.member:
                    if d.live:
                        d["storage_mode"] = storage_mode
            else:
                # only atomic data can land here
                mspass_object["storage_mode"] = storage_mode

            # Extend this method to add new storge modes
            # Note this implementation alters metadata in mspass_object
            # and all members of ensembles
            mspass_object = self._save_sample_data(
                mspass_object,
                storage_mode=storage_mode,
                dir=dir,
                dfile=dfile,
                format=format,
                overwrite=overwrite,
            )
            # ensembles need to loop over members to do the atomic operations
            # remaining.  Hence, this conditional
            if isinstance(mspass_object, (TimeSeries, Seismogram)):
                mspass_object = self._atomic_save_all_documents(
                    mspass_object,
                    save_schema,
                    exclude_keys,
                    mode,
                    wf_collection,
                    save_history,
                    data_tag,
                    storage_mode,
                    normalizing_collections,
                    alg_name,
                    alg_id,
                )

            else:
                # note else not elif because above guarantees only ensembles land here
                mspass_object.sync_metadata()
                for d in mspass_object.member:
                    d = self._atomic_save_all_documents(
                        d,
                        save_schema,
                        exclude_keys,
                        mode,
                        wf_collection,
                        save_history,
                        data_tag,
                        storage_mode,
                        normalizing_collections,
                        alg_name,
                        alg_id,
                    )
        else:
            # may need to clean Metadata before calling this method
            # to assure there are not values that will cause MongoDB
            # to throw an exception when the tombstone subdocument
            # is written.
            if not cremate:
                mspass_object = self.stedronsky.bury(
                    mspass_object, save_history=save_history
                )

        if return_data:
            return mspass_object
        else:
            # mpte emsembles with default only return "is_live" because
            # ids are only set in members.
            retdict = dict()
            if mspass_object.live:
                retdict["is_live"] = True
                for k in return_list:
                    if mspass_object.is_defined(k):
                        retdict[k] = mspass_object[k]
            else:
                retdict["is_live"] = False
            # note this returns an empty dict for dead data
            return retdict

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
        read data saved with such blemishes.   The subset of problems
        are defined as those that are identified by the "dbverify" command
        line tool or it's Database equivalent (the verify method of this class).
        This method is an alternative to the command line tool dbclean.  Use
        this method for fixes applied as part of a python script.

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
        :type document_id: :class:`bson.ObjectId.ObjectId`
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
        :type document_id: :class:`bson.ObjectId.ObjectId` of document to be tested
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
        :exception: This method will throw a fatal error exception if the
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
                "Database.verify:  ObjectId="
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
        This atomic function checks for a single xref_key in a single document.
        It used by the verify method.

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
        This atmoic function checks for if there are undefined required
        keys in a single document.  It is used by the verify method.

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
        try:
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
        finally:
            cursor.close()
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
        try:
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
        finally:
            cursor.close()
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
        try:
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
        finally:
            cursor.close()

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
            try:
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
            finally:
                cursor.close()

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
        try:
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
        finally:
            cursor.close()

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
        try:
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
        finally:
            cursor.close()
        return tuple([wrong_types, undef])

    def update_metadata(
        self,
        mspass_object,
        collection=None,
        mode="cautious",
        exclude_keys=None,
        force_keys=None,
        normalizing_collections=["channel", "site", "source"],
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
        :type alg_id: :class:`bson.ObjectId.ObjectId`
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

        # always exclude normalization data defined by names like site_lat
        copied_metadata = _erase_normalized(copied_metadata, normalizing_collections)
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
            # first handle readonly constraints
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
        data_tag=None,
        normalizing_collections=["channel", "site", "source"],
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

        A VERY IMPORTANT implicit feature of this method is that
        if the magic key "gridfs_id" exists the sample data in the
        input to this method (mspass_object.data) will overwrite any
        the existing content of gridfs found at the matching id.   This
        is a somewhat hidden feature so beware.

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
        :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
          (defaults to 'Database.update_data' and should not normally need to be changed)
        :type alg_name: :class:`str`
        :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
        :type alg_id: :class:`bson.ObjectId.ObjectId`
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
                normalizing_collections=normalizing_collections,
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
            # This logic overwrites the content if the magic key
            # "gridfs_id" exists in the input. In both cases gridfs_id is set
            # in returned
            if "gridfs_id" in mspass_object:
                mspass_object = self._save_sample_data_to_gridfs(
                    mspass_object,
                    overwrite=True,
                )
            else:
                mspass_object = self._save_sample_data_to_gridfs(
                    mspass_object, overwrite=False
                )

            # There is a possible efficiency gain right here.  Not sure if
            # gridfs_id is altered when the sample data are updated in place.
            # if we can be sure the returned gridfs_id is the same as theresult
            # input in that case, we would omit gridfs_id from the update
            # record and most data would not require the final update
            # transaction below
            update_record["gridfs_id"] = mspass_object["gridfs_id"]
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
                elog_id = self._save_elog(
                    mspass_object, elog_id=old_elog_id, data_tag=data_tag
                )
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
            elog_id = self._save_elog(
                mspass_object, elog_id=old_elog_id, data_tag=data_tag
            )

        return mspass_object

    def read_ensemble_data(
        self,
        cursor,
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
        DEPRICATED METHOD:   do not use except for backward compatility
        in short term.  Will go away in a later release

        """
        print("DEPRICATED METHOD (read_ensemble_data)")
        print("This method has been superceded by read_data method.  Use it instead.")
        print(
            "Method is backward compatible provided normalize is restricted to channel, site, and/or source"
        )
        if isinstance(cursor, pymongo.cursor.Cursor):
            ensemble = self.read_data(
                cursor,
                ensemble_metadata=ensemble_metadata,
                mode=mode,
                normalize=normalize,
                load_history=load_history,
                exclude_keys=exclude_keys,
                collection=collection,
                data_tag=data_tag,
                alg_name=alg_name,
                alg_id=alg_id,
            )
        else:
            message = "Database.read_ensemble_data:  illegal type={} for arg0\n".format(
                type(cursor)
            )
            message += "From version 2 foraward only a pymongo cursor is allowed for defining input\n"
            message += "Fix and use the now standard read_data method instead"
            raise TypeError(message)

        return ensemble

    def read_ensemble_data_group(
        self,
        cursor,
        ensemble_metadata={},
        mode="promiscuous",
        normalize=None,
        load_history=False,
        exclude_keys=None,
        collection="wf",
        data_tag=None,
        alg_name="read_ensemble_data_group",
        alg_id="0",
    ):
        """
        DEPRICATED METHOD:   do not use except for backward compatility
        in short term.  Will go away in a later release

        """
        print("DEPRICATED METHOD (read_ensemble_data_group)")
        print("This method has been superceded by read_data method.  Use it instead.")
        print(
            "Method is backward compatible provided normalize is restricted to channel, site, and/or source"
        )

        if isinstance(cursor, pymongo.cursor.Cursor):
            ensemble = self.read_data(
                cursor,
                ensemble_metadata=ensemble_metadata,
                mode=mode,
                normalize=normalize,
                load_history=load_history,
                exclude_keys=exclude_keys,
                collection=collection,
                data_tag=data_tag,
                alg_name=alg_name,
                alg_id=alg_id,
            )
        else:
            message = (
                "Database.read_ensemble_data_group:  illegal type={} for arg0\n".format(
                    type(cursor)
                )
            )
            message += "From version 2 foraward only a pymongo cursor is allowed for defining input\n"
            message += "Fix and use the now standard read_data method instead"
            raise TypeError(message)

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

        DEPRICATED METHOD:  use save_method instead.

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
        print("WARNING:  save_ensemble_data has been depricated.")
        print("Use save_data method instead.")
        print("This method may disappear in future releases")
        # The new save_data uses dir and dfile from member metadata
        # when sent a None so we just post the list
        if dir_list:
            num_dl = len(dir_list)
            nmembers = len(ensemble_object.member)
            last_valid = nmembers - 1
            if num_dl != nmembers:
                func = "Database.save_enemble_data"
                message = "Inconsistence sizes.  Number of ensemble members={} but size of dir_list argument ={}\n".format(
                    nmembers, num_dl
                )
                if num_dl < nmembers:
                    last_valid = num_dl - 1
                    message += "Using dir={} for all members in slots >= {}".format(
                        dir_list[last_valid - 1], last_valid
                    )
                else:
                    message += "Extra dir entries will be ignored"
                ensemble_object.elog.log_error(func, message, ErrorSeverity.Complaint)
            for i in range(len(ensemble_object.member)):
                if i <= last_valid:
                    ensemble_object.member[i].put_string("dir", dir_list[i])
                else:
                    ensemble_object.member[i].put_string("dir", dir_list[last_valid])

        if dfile_list:
            num_dl = len(dfile_list)
            nmembers = len(ensemble_object.member)
            last_valid = nmembers - 1
            if num_dl != nmembers:
                func = "Database.save_enemble_data"
                message = "Inconsistence sizes.  Number of ensemble members={} but size of dfile_list argument ={}\n".format(
                    nmembers, num_dl
                )
                if num_dl < nmembers:
                    last_valid = num_dl - 1
                    message += "Using dfile={} for all members in slots >= {}".format(
                        dfile_list[last_valid - 1], last_valid
                    )
                else:
                    message += "Extra dfile entries will be ignored"
                ensemble_object.elog.log_error(func, message, ErrorSeverity.Complaint)
            for i in range(len(ensemble_object.member)):
                if i <= last_valid:
                    ensemble_object.member[i].put_string("dfile", dfile_list[i])
                else:
                    ensemble_object.member[i].put_string(
                        "dfile", dfile_list[last_valid]
                    )

        if exclude_objects:
            print(
                "Double WARNING:   save_ensemble_data exclude_objects option will disappear in future releases"
            )
            # we won't do this elegantly since it should not be used
            for i in exclude_objects:
                ensemble_object.member.pop(i)

        ensemble_object = self.save_data(
            ensemble_object,
            mode=mode,
            storage_mode=storage_mode,
            dir=None,
            dfile=None,
            exclude_keys=exclude_keys,
            collection=collection,
            data_tag=data_tag,
            alg_name="save_ensemble_data",
            alg_id=alg_id,
            return_data=True,
        )
        # original did not have a return_data argument so we always return
        # a copy of the data
        return ensemble_object

    def save_ensemble_data_binary_file(
        self,
        ensemble_object,
        mode="promiscuous",
        dir=None,
        dfile=None,
        exclude_keys=None,
        exclude_objects=None,
        collection=None,
        data_tag=None,
        kill_on_failure=False,
        alg_name="save_ensemble_data_binary_file",
        alg_id="0",
    ):
        """
        Save an Ensemble container of a group of data objecs to MongoDB.

        DEPRICATED METHOD:  use save_method instead.

        Ensembles are a core concept in MsPASS that are a generalization of
        fixed "gather" types frozen into every seismic reflection processing
        system we know of.   This is is a writer for data stored in such a
        container.  It saves all the objects in the ensemble to one file.

        Our implementation has a separate Metadata container associated with the overall
        group that is assumed to be constant for every member of the ensemble.  For this
        reason the method calls the objects sync_metadata method that copies (overwrites
        if previously defined) the ensemble attributes to each member.  That assure
        atomic saves will not lose their association with a unique ensemble indexing scheme.

        A final feature of note is that an ensemble can be marked dead.
        If the entire ensemble is set dead this function returns
        immediately and does nothing.


        :param ensemble_object: the ensemble you want to save.
        :type ensemble_object: either :class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` or
            :class:`mspasspy.ccore.seismic.SeismogramEnsemble`.
        :param mode: reading mode regarding schema checks, should be one of ['promiscuous','cautious','pedantic']
        :type mode: :class:`str`
        :param dir: file directory.
        :type dir: :class:`str`
        :param dfile: file name.
        :type dfile: :class:`str`
        :param exclude_keys: the metadata attributes you want to exclude from being stored.
        :type exclude_keys: a :class:`list` of :class:`str`
        :param exclude_objects: A list of indexes, where each specifies a object in the ensemble you want to exclude from being saved. Starting from 0.
        :type exclude_objects: :class:`list`
        :param collection: the collection name you want to use. If not specified, use the defined collection in the metadata schema.
        :param data_tag: a user specified "data_tag" key to tag the saved wf document.
        :type data_tag: :class:`str`
        :param kill_on_failure:  When true if an io error occurs the data object's
          kill method will be invoked.  When false (the default) io errors are
          logged and left set live.  (Note data already marked dead are return
          are ignored by this function. )
        :type kill_on_failure: boolean
        """
        print("WARNING:  save_ensemble_data_binary_file has been deprecated.")
        print("Use save_data method instead.")
        print("This method may disappear in future releases")
        # This method is now indistinguishable from save_ensemble so
        # we just call it

        if exclude_objects:
            print(
                "Double WARNING:   save_ensemble_data_binary_file exclude_objects option will disappear in future releases"
            )
            # we won't do this elegantly since it should not be used
            for i in exclude_objects:
                ensemble_object.member.pop(i)
        if dir:
            for d in ensemble_object.member:
                d["dir"] = dir
        if dfile:
            for d in ensemble_object.member:
                d["dfile"] = dfile
        ensemble_object = self.save_ensemble_data(
            ensemble_object,
            mode=mode,
            storage_mode="file",
            exclude_keys=exclude_keys,
            collection=collection,
            data_tag=data_tag,
            alg_name="save_ensemble_data_binary_file",
            alg_id=alg_id,
        )

        # original did not have a return_data argument so we always return
        # a copy of the data
        return ensemble_object

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

        DEPRICATED METHOD:  Do not use.

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
        print(
            "WARNING:  This function is depricated and may be removed from future releases"
        )
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
        a datum is optional.  Note this is an expensive operation as it
        involves extensive database interactions.   It is best used for
        surgical solutions.   Deletion of large components of a data set
        (e.g. all data with a given data_tag value) are best done with
        custom scripts utilizing file naming conventions and unix shell
        commands to delete waveform files.


        :param object_id: the wf object id you want to delete.
        :type object_id: :class:`bson.ObjectId.ObjectId`
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
        Master Private Method - DEPRICATED

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

        This method is DEPRICATED.  It is an slow alternative for normalization
        and is effectively an alternative to normalization with the
        Database driven id matcher.   Each call to this function requires
        a query with find_ond.

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

        This method is DEPRICATED.  It is an slow alternative for normalization
        and is effectively an alternative to normalization with the
        Database driven id matcher.   Each call to this function requires
        a query with find_ond.

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

        This method is DEPRICATED.  It is an slow alternative for normalization
        and is effectively an alternative to normalization with the
        Database driven id matcher.   Each call to this function requires
        a query with find_ond.

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
        # timing errors in data.  Note it sets the metadata entry starttime
        # as the primary need for interfacing with obspy
        t0 = mspass_object.t0
        mspass_object.set_t0(t0)
        # This will need to be modified if we ever expand time types beyond two
        if mspass_object.time_is_relative():
            if mspass_object.shifted():
                mspass_object["starttime_shift"] = mspass_object.time_reference()
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
        :type prev_history_object_id: :class:`bson.ObjectId.ObjectId`
        :param collection: the collection that you want to store the history object. If not specified, use the defined
          collection in the schema.
        :return: current history_object_id.
        """
        if isinstance(mspass_object, TimeSeries):
            atomic_type = AtomicType.TIMESERIES
        elif isinstance(mspass_object, Seismogram):
            atomic_type = AtomicType.SEISMOGRAM
        else:
            raise TypeError("only TimeSeries and Seismogram are supported")

        if not collection:
            collection = self.database_schema.default_name("history_object")
        history_col = self[collection]

        proc_history = ProcessingHistory(mspass_object)
        current_nodedata = proc_history.current_nodedata()

        # get the alg_name and alg_id of current node
        if not alg_id:
            alg_id = current_nodedata.algid
        if not alg_name:
            alg_name = current_nodedata.algorithm
        # Global History implemetnation should allow adding job_name
        # and job_id to this function call.  For now they are dropped
        insert_dict = history2doc(proc_history, alg_id=alg_id, alg_name=alg_name)
        # We need this below, but history2doc sets it with the "_id" key
        current_uuid = insert_dict["save_uuid"]
        history_id = history_col.insert_one(insert_dict).inserted_id

        # clear the history chain of the mspass object
        mspass_object.clear_history()
        # set_as_origin with uuid set to the newly generated id
        # Note we have to convert to a string to match C++ function type
        mspass_object.set_as_origin(alg_name, alg_id, str(current_uuid), atomic_type)

        return history_id

    def _load_history(
        self,
        mspass_object,
        history_object_id=None,
        alg_name="undefined",
        alg_id="undefined",
        define_as_raw=False,
        collection="history_object",
    ):
        """
        Loads processing history into an atomic data object or initializes
        history tree if one did not exist.

        We store history data on a save in documents in the "history_object"
        collection.  This private method loads that data when it can.
        Top-level behavior is controlled by the history_object_id value.
        If it is not None we query the history_object collection using
        that value as an objectId.   If that fails the tree is
        initialized as an origin but an error message is left on elog
        complaining that the history is likely invalid.

        When a workflow is initialized by a read, which in ProcessingHistory
        is what is called an "origin", this function should be called with
        history_object_id set to None.  In that situation, it initializes
        the history tree (set_as_origin) with the id for the start of the
        chain as the waveform object id.  That is assumed always in
        mspass_object accesible with the key '_id'.  This method will
        throw an exception if that is not so.

        :param mspass_object: the target object - altered in place
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param history_object_id: :class:`bson.ObjectId.ObjectId` or None (see above)
        :param alg_name:  algorithm name that should be the current node
          of the history tree.  this should normaly be the name of th e
          reader method/function.
        :param alg_id:  algorithm id to set for the current node
        :param define_as_raw:  when True and history_object_id is None
          the starting node of the history chain will be tagged as "raw".
          Ignored if history_object_id is used for a query.  Note on
          failure of such a query the chain will always have raw set false.
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
        if mspass_object.dead():
            return
        if history_object_id:
            # load history if set True
            res = self[collection].find_one({"_id": history_object_id})
            if res:
                if "processing_history" in res:
                    mspass_object.load_history(pickle.loads(res["processing_history"]))
                    mspass_object.new_map(
                        alg_name, alg_id, atomic_type, ProcessingStatus.ORIGIN
                    )
                else:
                    message = "Required attribute (processing_history) not found in document retrieved from collection={}\n".format(
                        collection
                    )
                    message += "Object history chain for this run will be incomplete for this datum"
                    mspass_object.elog.log_error(
                        "Database._load_history", message, ErrorSeverity.Complaint
                    )
                    mspass_object.set_as_origin(
                        alg_name,
                        alg_id,
                        str(mspass_object["_id"]),
                        atomic_type,
                        define_as_raw=False,
                    )
            else:
                message = "No matching document found in history collection={} with id={}\n".format(
                    collection, history_object_id
                )
                message += "Object history chain for this run will be incomplete for this datum"
                mspass_object.elog.log_error(
                    "Database._load_history", message, ErrorSeverity.Complaint
                )
                mspass_object.set_as_origin(
                    alg_name,
                    alg_id,
                    str(mspass_object["_id"]),
                    atomic_type,
                    define_as_raw=False,
                )
        else:
            mspass_object.set_as_origin(
                alg_name,
                alg_id,
                str(mspass_object["_id"]),
                atomic_type,
                define_as_raw=define_as_raw,
            )

    def _save_elog(
        self,
        mspass_object,
        elog_id=None,
        collection=None,
        create_tombstone=True,
        data_tag=None,
    ):
        """
        Save error log for a data object. Data objects in MsPASS contain an error log object used to post any
        errors handled by processing functions. This function will delete the old elog entry if `elog_id` is given.

        :param mspass_object: the target object.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param elog_id: the previous elog object id to be appended with.
        :type elog_id: :class:`bson.ObjectId.ObjectId`
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

        if mspass_object.dead() and mspass_object.elog.size() == 0:
            message = (
                "WARNING:  found this datum marked dead but it had not elog entries\n"
            )
            message = "Likely flaw in a custom function that didn't handle kill and elog properly"
            err = MsPASSError("Database._save_elog", message, ErrorSeverity.Complaint)
            mspass_object.elog.log_error(err)

        elog = mspass_object.elog

        if elog.size() > 0:
            docentry = elog2doc(elog)
            if data_tag:
                docentry["data_tag"] = data_tag
            if oid:
                docentry[wf_id_name] = oid

            if mspass_object.dead():
                docentry["tombstone"] = dict(mspass_object)

            if elog_id:
                # append elog
                elog_doc = self[collection].find_one({"_id": elog_id})
                # only append when previous elog exists
                if elog_doc:
                    # extract contents from this datum for comparison to elog_doc
                    # This loop appends to elog_doc logdata list removing
                    # duplicates that can be created if a datum is
                    # saved mulitple times in job
                    logdata = docentry["logdata"]
                    for x in logdata:
                        if x not in elog_doc["logdata"]:
                            elog_doc["logdata"].append(x)

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
        Private method to provide generic reader of atomic data from a standard
        system in any accepted format.

        This method is used by read_data for reading atomic data.   Note
        ensembles are handled by a different set of private methods
        for efficiency.   The algorithm is a bit weird as it assumes
        the skeleton of the datum (mspass_object content received as arg0)
        has already created the storage arrays from sample data.
        That is done, in practice, with the C++ api that provides constructors
        driven by Metadata created from MongoDB documents.   Those constructors
        allocate and initialize the sample data arrays to zeros.

        Binary (default) format uses a C++ function and fread for loading
        sample data.  Other formats are passed to obspy's reader.

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

        alg = "Database._read_data_from_dfile:  "
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
                        mspass_object.kill()
                    else:
                        mspass_object.set_live()
                except MsPASSError as merr:
                    message = "Database._read_data_from_dfile:  "
                    message += "C++ function _fread_from_file failed while reading TimeSeries sample data from file={}".format(
                        dfile
                    )
                    raise MsPASSError(message, ErrorSeverity.Fatal) from merr
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
                        mspass_object.kill()
                    else:
                        mspass_object.set_live()
                except MsPASSError as merr:
                    message = "Database._read_data_from_dfile:  "
                    message += "C++ function _fread_from_file failed while reading Seismogram sample data from file={}".format(
                        dfile
                    )
                    raise MsPASSError(message, ErrorSeverity.Fatal) from merr

        else:
            fname = os.path.join(dir, dfile)
            with open(fname, mode="rb") as fh:
                if foff > 0:
                    fh.seek(foff)
                try:
                    flh = io.BytesIO(fh.read(nbytes))
                    st = obspy.read(flh, format=format)
                except TypeError as te:
                    message = "BytesIO failed reading file={}\n".format(dfile)
                    message += "Threw a TypeError with this content:  "
                    message += str(te)
                    mspass_object.kill()
                    mspass_object.elog.log_error(
                        "Database._read_from_dfile", message, ErrorSeverity.Invalid
                    )
                except Exception as e:
                    # obspy seems to make all exceptions subclasses of this
                    # inermediate, generic class.   The message seems to be
                    # avaialble as here by using the str result.
                    message = "obspy reader failed reading data with format=" + format
                    message += "\nMessage posted:  " + str(e)
                    # assume mspass_object has an elog and can accept kill
                    mspass_object.kill()
                    mspass_object.elog.log_error(
                        "Database._read_from_dfile", message, ErrorSeverity.Invalid
                    )
                else:
                    # land here if there are no read errors - normal data
                    if isinstance(mspass_object, TimeSeries):
                        # st is a "stream" but it may contains multiple Trace objects gaps
                        # but here we want only one TimeSeries, we merge these Trace objects and fill values for gaps
                        # we post a complaint elog entry to the mspass_object if there are gaps in the stream
                        if len(st) > 1:
                            message = "WARNING:  gaps detected while reading file {} with format {} using obspy\n".format(
                                fname, format
                            )
                            message += (
                                "Using specified fill defined in call to read_data"
                            )
                            mspass_object.elog.log_error(
                                "read_data",
                                message,
                                ErrorSeverity.Complaint,
                            )
                            st = st.merge(
                                method=merge_method,
                                fill_value=merge_fill_value,
                                interpolation_samples=merge_interpolation_samples,
                            )
                        tr = st[0]
                        # We can't use Trace2TimeSeries because we loose
                        # all but miniseed metadata if we do that.
                        # We do, however, need to compare post errors
                        # if there is a mismatch
                        if tr.stats.npts != mspass_object.npts:
                            message = "Inconsistent number of data points (npts)\n"
                            message += "Database npts={} but obspy reader created a vector {} points long\n".format(
                                mspass_object.npts, tr.stats.npts
                            )
                            message += "Set to vector size defined by reader."
                            mspass_object.elog.log_error(
                                alg, message, ErrorSeverity.Complaint
                            )
                            mspass_object.set_npts(tr.stats.npts)
                        if tr.stats.starttime.timestamp != mspass_object.t0:
                            message = "Inconsistent starttimes detected\n"
                            message += "Starttime in MongoDB document = {}\n".format(
                                UTCDateTime(mspass_object.t0)
                            )
                            message += (
                                "Starttime returned by obspy reader = {}\n".format(
                                    tr.stats.starttime
                                )
                            )
                            message += "Set to time set by reader"
                            mspass_object.elog.log_error(
                                alg, message, ErrorSeverity.Complaint
                            )
                            mspass_object.set_t0(tr.stats.starttime.timestamp)
                        if tr.stats.delta != mspass_object.dt:
                            message = "Inconsistent sample intervals"
                            message += "Database has delta={} but obspy reader set delta={}\n".format(
                                mspass_object.dt, tr.stats.delta
                            )
                            message += "Set to value set by reader."
                            mspass_object.elog.log_error(
                                alg, message, ErrorSeverity.Complaint
                            )
                            mspass_object.dt = tr.stats.delta
                        # be less dogmatic here as endtime is computed.
                        # error only if computed time difference exceed half a sample
                        if (
                            abs(tr.stats.endtime.timestamp - mspass_object.endtime())
                            > mspass_object.dt / 2.0
                        ):
                            message = "Inconsistent endtimes detected\n"
                            message += (
                                "Endtime expected from MongoDB document = {}\n".format(
                                    UTCDateTime(mspass_object.endtime())
                                )
                            )
                            message += "Endtime set by obspy reader = {}\n".format(
                                tr.stats.endtime
                            )
                            message += "Endtime is derived in mspass and should have been repaired - cannot recover this datum so it was killed"
                            mspass_object.elog.log_error(
                                alg, message, ErrorSeverity.Invalid
                            )
                            mspass_object.kill()

                        # These two lines are needed to properly initialize
                        # the DoubleVector before calling Trace2TimeSeries
                        tr_data = tr.data.astype(
                            "float64"
                        )  #   Convert the nparray type to double, to match the DoubleVector
                        mspass_object.npts = len(tr_data)
                        mspass_object.data = DoubleVector(tr_data)
                        # We can't use Trace2TimeSeries because we loose
                        # all but miniseed metadata if we do that.
                        # We do, however, need to compare post errors
                        # if there is a mismatch

                        if mspass_object.npts > 0:
                            mspass_object.set_live()
                        else:
                            message = "Error during read with format={}\n".format(
                                format
                            )
                            message += "Unable to reconstruct data vector"
                            mspass_object.elog.log_error(
                                alg, message, ErrorSeverity.Invalid
                            )
                            mspass_object.kill()
                    elif isinstance(mspass_object, Seismogram):
                        # This was previous form.   The toSeismogram run as a
                        # method is an unnecessary confusion and I don't think
                        # setting npts or data are necessary given the code of
                        # Stream2Seismogram - st.toSeismogram is an alias for that
                        # This is almost but not quite equivalent to this:
                        # mspass_object = Stream2Seismogram(st,cardinal=True)
                        # Seems Stream2Seismogram does not properly handle
                        # the data pointer
                        sm = st.toSeismogram(cardinal=True)
                        mspass_object.npts = sm.data.columns()
                        mspass_object.data = sm.data
                        if mspass_object.npts > 0:
                            mspass_object.set_live()
                        else:
                            message = "Error during read with format={}\n".format(
                                format
                            )
                            message += "Unable to reconstruct Seismogram data matrix"
                            mspass_object.elog.log_error(
                                alg, message, ErrorSeverity.Invalid
                            )
                            mspass_object.kill()

    def _read_data_from_gridfs(self, mspass_object, gridfs_id):
        """
        Private method used to read sample data from MongoDB's gridfs
        storage for atomic MsPASS data objects.  Like the
        comparable "file" reader this method assumes the skeleton of
        the datum received as mspass_object in arg0 has the array
        memory storage already constructed and initialized.   This function
        then simply moves the sample data from gridfs into the data
        array of mspass_object.   Appropriate behavior for a private
        method but why this method should not be used externally
        by anyone but an expert.

        There is a minor sanity check as the method tests for the existence
        of the (frozen) key "npts" in the Metadata container of mspass_object.
        It will throw a KeyError if that key-value pair is not defined.
        It then does a sanity check against the npts attribute of
        mspass_object.   That safety adds a minor overhead but reduces
        the odds of a seg fault.


        :param mspass_object: skeleton of the data object to load sample
          data into.   Assumes data array exists and is length consistent
          with the "npts" Metadata value.
        :type mspass_object: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
        :param gridfs_id: the object id of the data stored in gridfs.
        :type gridfs_id: :class:`bson.ObjectId.ObjectId`
        """
        gfsh = gridfs.GridFS(self)
        fh = gfsh.get(file_id=gridfs_id)
        if isinstance(mspass_object, (TimeSeries, Seismogram)):
            if not mspass_object.is_defined("npts"):
                message = (
                    "Required key npts is not defined in the document for this datum"
                )
                mspass_object.elog.log_error(
                    "Database._read_data_from_gridfs", message, ErrorSeverity.Invalid
                )
                mspass_object.kill()
                return
            else:
                if mspass_object.npts != mspass_object["npts"]:
                    message = "Database._read_data_from_gridfs: "
                    message += "Metadata value for npts is {} but datum attribute npts is {}\n".format(
                        mspass_object["npts"], mspass_object.npts
                    )
                    message += "Illegal inconsistency that should not happen and is a fatal error"
                    raise ValueError(message)
            npts = mspass_object.npts
        if isinstance(mspass_object, TimeSeries):
            # fh.seek(16)
            float_array = array("d")
            float_array.frombytes(fh.read(mspass_object.get("npts") * 8))
            mspass_object.data = DoubleVector(float_array)
        elif isinstance(mspass_object, Seismogram):
            np_arr = np.frombuffer(fh.read(npts * 8 * 3))
            file_size = fh.tell()
            if file_size != npts * 8 * 3:
                message = "Size mismatch in sample data.\n"
                message += "Number of points in gridfs file=%d but wf document expected %d".format(
                    file_size / 8, (3 * mspass_object["npts"])
                )
                mspass_object.elog.log_error(
                    "Database._read_data_from_gridfs",
                    message,
                    ErrorSeverity.Invalid,
                )
                mspass_object.kill()
                return
            # v1 did a transpose on write that this reversed - unnecessary
            # np_arr = np_arr.reshape(npts, 3).transpose()
            np_arr = np_arr.reshape(3, npts)
            mspass_object.data = dmatrix(np_arr)
        else:
            message = "Database._read_data_from_gridfs:  arg0 must be a TimeSeries or Seismogram\n"
            message += "Actual type=" + str(type(mspass_object))
            raise TypeError(message)
        if mspass_object.npts > 0:
            mspass_object.set_live()
        else:
            mspass_object.kill()

    @staticmethod
    def _read_data_from_s3_continuous(
        mspass_object, aws_access_key_id=None, aws_secret_access_key=None
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

            # this is not a error proof test for validity, but best I can do here
            if mspass_object.npts > 0:
                mspass_object.set_live()
            else:
                mspass_object.kill()

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

    @staticmethod
    def _read_data_from_s3_lambda(
        mspass_object, aws_access_key_id=None, aws_secret_access_key=None
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
            st = Database._download_windowed_mseed_file(
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

            # this is not a error proof test for validity, but best I can do here
            if mspass_object.npts > 0:
                mspass_object.set_live()
            else:
                mspass_object.kill()

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
            # this is not a error proof test for validity, but best I can do here
            if mspass_object.npts > 0:
                mspass_object.set_live()
            else:
                mspass_object.kill()

    @staticmethod
    def _read_data_from_fdsn(mspass_object):
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
        # this is not a error proof test for validity, but best I can do here
        if mspass_object.npts > 0:
            mspass_object.set_live()
        else:
            mspass_object.kill()

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
        # this is not a error proof test for validity, but best I can do here
        if mspass_object.npts > 0:
            mspass_object.set_live()
        else:
            mspass_object.kill()

    @staticmethod
    def _extract_locdata(chanlist):
        """
        Parses the list returned by obspy channels attribute
        for a Station object and returns a dict of unique
        geographic location fields values keyed by loc code.  This algorithm
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
        """
        Somewhat trivial standardized private method for handling null
        (None) starttime values.  Returns UTCDateTime object for 0
        epoch time to mesh with obspy's times where this method is used.
        """
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
                    # rec will contain the ObjectId of the document inserted
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
                    # save coordinates in both geoJSON and "legacy"
                    # format
                    rec["coords"] = [loc_lon, loc_lat]
                    # Illegal lon,lat values will cause this to throw a
                    # ValueError exception.   We let it do that as
                    # it indicates a problem datum
                    rec = geoJSON_doc(loc_lat, loc_lon, doc=rec, key="location")
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
                        # Note this sets site_id to an ObjectId for the insertion
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

        An all to common metadata problem is to have duplicate entries in
        site for the same data.   The default behavior of this method is
        to print a warning whenever a match is ambiguous
        (i.e. more than on document matches the keys).  Set verbose false to
        silence such warnings if you know they are harmless.

        The seed modifier in the name is to emphasize this method is
        for data originating as the SEED format that use net:sta:loc:chan
        as the primary index.

        Note this method may be DEPRICATED in the future as it has been
        largely superceded by BasicMatcher implementations in the
        normalize module.

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

        Note this method may be DEPRICATED in the future as it has been
        largely superceded by BasicMatcher implementations in the
        normalize module.

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
            # save the epicenter data in both legacy format an d
            # geoJSON format.  Either can technically be used in a
            # geospatial query but the geoJSON version is always
            # preferred
            rec["coords"] = [o.longitude, o.latitude]
            # note this function updates rec and returns the upodate
            rec = geoJSON_doc(o.latitude, o.longitude, doc=rec, key="epicenter")
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
        o["npts"] = index_record.npts
        o["endtime"] = index_record.endtime
        return o

    def index_mseed_file(
        self,
        dfile,
        dir=None,
        collection="wf_miniseed",
        segment_time_tears=True,
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
        function is controlled by the input parameter
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
          discussion of how to use this parameter.  Default is True.
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
        # TODO:  should have a way to pass verbose to this function
        # present verbose is not appropriate.
        (ind, elog) = _mseed_file_indexer(fname, segment_time_tears)
        if len(elog.get_error_log()) > 0 and "No such file or directory" in str(
            elog.get_error_log()
        ):
            raise FileNotFoundError(str(elog.get_error_log()))
        ids_affected = []
        for i in ind:
            doc = self._convert_mseed_index(i)
            doc["storage_mode"] = "file"
            doc["format"] = "mseed"
            doc["dir"] = odir
            doc["dfile"] = dfile
            # mseed is dogmatically UTC so we always set it this way
            doc["time_standard"] = "UTC"
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
        dataframe will be saved in the specified mongodb collection.
        Otherwise duplicates would be discarded.

        Note we first call the dropna method of DataFrame to eliminate
        None values to mesh with how MongoDB handles Nulls; not
        consistent with DataFrame implemenations that mimic relational
        database tables where nulls are a hole in the table.

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

        if parallel and _mspasspy_has_dask:
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
        if parallel and _mspasspy_has_dask:
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
    def _kill_zero_length_live(mspass_object):
        """
        This is a small helper function used by _save_sample_data to
        handle the wrong but speecial case of a datum marked live but
        with a zero length data vector.   It kills any such datum
        and posts a standard error message. For ensembles the entire
        ensemble is scanned for data with such errors.   Note this
        function is a necesary evil as save_data will abort in this situation
        if the data are not passed through this filter.
        """
        message = "Nothing to save for this datum.  Datum was marked live but the sample data is zero length\n"
        message += "Nothing saved.  Killing this datum so it will be saved in cemetery"
        if isinstance(mspass_object, (TimeSeries, Seismogram)):
            if mspass_object.live and mspass_object.npts == 0:
                mspass_object.kill()
                mspass_object.elog.log_error(
                    "_save_sample_data", message, ErrorSeverity.Invalid
                )
        else:
            # ensembles only land here - internal use assures that but
            # external use would require more caution
            for d in mspass_object.member:
                if d.live and d.npts <= 0:
                    d.kill()
                    d.elog.log_error(
                        "_save_sample_data", message, ErrorSeverity.Invalid
                    )

        return mspass_object

    def _save_sample_data(
        self,
        mspass_object,
        storage_mode="gridfs",
        dir=None,
        dfile=None,
        format=None,
        overwrite=False,
    ):
        """
        Function to save sample data arrays for any MsPASS data objects.

        The sample data in a seismic data object is normally dominates
        the size of the container.  This function standardizes saving the
        sample data with a single method.   Behavior is dependent upon
        parameters passed in a fairly complex way.   The behavor is best
        understood as a hierarchy summarized as follows:

        1)  The top level switch is the data type.  Atomic types
        (TimeSeries and Seismogram) are handled differently from ensembles.
        The main distinction is atomic saves always involve some version of
        open, write, and close where open and close may be a file handle or
        a network connection.  Ensembles are normally written to a single
        file, but there are options to dismember the ensemble into multiple
        files driven by the "dir" and "dfile" attributes of the members.
        See description below of "use_member_dfile_value" parameter.

        2) The "storage_mode" argument determines what medium will hold the
        sample data.  Currently accepted values are "file" and "gridfs"
        but other options may be added in the near future to support cloud
        computing.

        3) The "format" argument can be used to specify an alternative
        format for the output.  Most formats mix up metadata and sample
        data so be warned there is a mismatch in concept between this
        writer and implementations that write most formatted data.

        The user should be aware of an important issue with ensembles.
        For atomic data marked dead this function simply returns the dead
        datum immediately.  Ensembles do the same if the entire ensemble is
        marked dead, but for efficiency the function ASSUMES that there are
        no data marked dead within the ensemble.  Callers should guarantee
        that restriction by using methods in the Undertaker class to
        remove the dead from the container.

        :param mspass_object:  data to be saved.
        :type mspass_object:  must be one of TimeSeries, Seismogram,
          TimeSeriesEnsemble, or SeismogramEnsemble.  This function does NOT
          test input type for efficiency.  It also assumes the datum
          received was not previously marked dead.  dead data need to
          be handled differently in save.

        :param storage_mode:   is a string used to define the generic
          method for storing the sample data.  Currently must be one of
          two values or the function will throw an exception:
          (1) "file" causes data to be written to external files.
          (2) "gridfs" (the default) causes the sample data to be stored
               within the gridfs file system of MongoDB.
          See User's manuals for guidance on storage option tradeoffs.

        :param dir:  directory file is to be written. Just be writable
          by user or the function will throw an exception.   Default is None
          which initaties the following steps: if dir is defined in
          the object's Metadata (key "dir") that string is fetched and
          used.  If not defined, default to current directory of the process.

          (Ignored when storage_mode == "gridfs")
        :type dir:  string or None

        :param dfile:  file name to save the sample data for this object.
         Default is None which initaties the following steps: if dfile
         is defined in the current object's Metadata (key "dfile") the
         string value retrieved with the dfile key is used as the file
         name.  If not, a unique name will be generated with a uuid
         generator.   (Ignored when storage_mode=="gridfs")
        :type dfile:  string or None

        :param format:  optional format name to write data.  See above for
          limitations and caveats.  Default is None which is aliases to "binary"
          meaning a fwrite of sample array data. (Ignored when storage_mode == "gridfs")
        :type format:  string or None

        :param overwrite:  If True any file name generated form dir and dfile
          that already exists will be overwritten.  Default is Fals which means
          the function alway only appends data recording foff of the start
          position of the write. (Ignored when storage_mode == "gridfs")

        :exception:
          1) will raise a TypeError if mspass_object is not a mspass seismic
          data object (atomic or ensmebles).   That shouldnt' normally happen
          for use within the Database class but Users could experiment with
          custom writers that would use this class method.
        """
        # when datum is an ensemble the caller should use bring_out_your dead
        # before calling this method
        if isinstance(
            mspass_object,
            (TimeSeries, Seismogram, TimeSeriesEnsemble, SeismogramEnsemble),
        ):
            mspass_object = self._kill_zero_length_live(mspass_object)
            if mspass_object.dead():
                # do nothing to any datum marked dead - just return the pointer
                return mspass_object

            if storage_mode == "file":
                mspass_object = self._save_sample_data_to_file(
                    mspass_object,
                    dir,
                    dfile,
                    format,
                    overwrite,
                )
            elif storage_mode == "gridfs":
                mspass_object = self._save_sample_data_to_gridfs(
                    mspass_object,
                    overwrite,
                )
            else:
                message = (
                    "_save_sample_data:  do now know how to handle storage_mode="
                    + storage_mode
                )
                raise ValueError(message)
            return mspass_object
        else:
            message = "_save_sample_data:  arg0 must be a MsPASS data object\n"
            message += "Type arg0 passed=" + str(type(mspass_object))
            raise TypeError(message)

    def _save_sample_data_to_file(
        self,
        mspass_object,
        dir=None,
        dfile=None,
        format=None,
        overwrite=False,
    ):
        """
        Private method to write sample data to a file.  Default is binary
        fwrite output of native vector of 8 byte floats (C double).

        This method puts file-based writing in a single function.   The
        function handles any of the 4 standard MsPASS seismic data objects
        BUT makes some big assumptions that caller did some housecleaning
        before calling this low-level function.   Key assumptions are:
        1.  For atomic data do NOT call this function if the datum is marked
            dead.
        2.  Similarly, ensembles are assumed pre-filtered to remove dead
            data.  A standard way to do that is with the Undertaker class.
        3.  If dir and dfile are defined the resulting path must be writable.
            If it is not the function will throw an exception.
        4.  If dir and dfile are not defined (left Null default) the
            function always tries to extract an attribute with the keys of
            "dir" and "dfile" from the object's metadata.  Note that for
            ensembles that means it will try to pull these from the ensemble
            metadata NOT that in any "member".  For all data if dir is not define
            it default to the current directory.  If dfile is not define a
            unique file name is generated from a uuid generator.
        5.  ensembles should normally be preceded by a call to sync_ensemble_metadata
            to push ensemble metadata entries to all members.

        Be aware normal behavior of this function is to not destroy any
        data in existing files.  By default, if a file already exists the
        sample data related to the current call is appended to the file.
        Set the overwrite argument True to change that behavior.

        Also be aware ensembles are ALWAYS written so all the sample data
        from the ensemble are in the same file.   Readers can extract an
        individual member later by using the foff attribute and npts
        and/or nbytes to know what section of the file is be to be read.

        The functions supports a format option.  By default data are written
        with the low-level C function fwrite dumping the internal array contents
        that hold the sample data.  That is always the fastest way to save
        the sample data.  When the format variable is set the name string
        for the format is passed directly to obspy's generic file writer
        for formatted data.   Be warned that approach may not make sense for
        all data types and all formats.  For example, SAC output only makes
        sense at the atomic level so writing an ensemble in SAC format produces
        output that is not directly readable by SAC but the file must be
        dismembered into pieces in an export.   That is feasible because the
        we store an "nbytes" attribute in the returned object's
        member Metadata containers.

        This function always opens a file and closes it on exit.  Each call
        to the function this always has the overhead of one open, one call
        to seek, and one to close.

        The function should always be called before saving the Metadata.
        The reason is all the information about where it saves data are
        returned as Metadata attributes.  For atomic data it is in the
        return of the data.  For ensembles each member's Metadata contain
        the required attributes needed to reconstruct the object.

        :param mspass_object:  data to be saved.
        :type mspass_object:  must be one of TimeSeries, Seismogram,
          TimeSeriesEnsemble, or SeismogramEnsemble.  This function does NOT
          test input type for efficiency.  It also assumes the datum
          received was not previously marked dead.  dead data need to
          be handled differently in save.

        :param dir:  directory file is to be written. Just be writable
          by user or the function will throw an exception.   Default is None
          which initaties the following steps: if dir is defined in
          the object's Metadata (key "dir") that string is fetched and
          used.  If not defined, default to current directory of the process.
        :type dir:  string or None

        :param dfile:  file name to save the sample data for this object.
           Default is None which initaties the following steps: if dfile
           is defined in the current object's Metadata (key "dfile") the
           string value retrieved with the dfile key is used as the file
           name.  If not, a unique name will be generated with a uuid
           generator
        :type dfile:  string or None

        :param format:  optional format name to write data.  See above for
          limitations and caveats.  Default is None which is aliases to "binary"
          meaning a fwrite of sample array data.
        :type format:  string or None

        :param overwrite:  If True any file name generated form dir and dfile
          that already exists will be overwritten.  Default is Fals which means
          the function alway only appends data recording foff of the start
          position of the write.  WARNING:  default is False and setting
          it True should only be done if you are sure what you are doing.
          Be especially aware that with this option you can clobber files
          needec by other docs to reconstruct data.

        :return:  copy of the input with Metadata modified to contain all
          data required to reconstruct the object from the stored sample data.

        """
        # return immediately if the datum is marked dead
        if mspass_object.dead():
            return mspass_object
        if format is None:
            format = "binary"

        if dir is None:
            if mspass_object.is_defined("dir"):
                dir = mspass_object["dir"]
                # convert to fullpath if necessary
                dir = os.path.abspath(dir)
            else:
                dir = os.getcwd()
        else:
            dir = os.path.abspath(dir)

        # note not else here as it means accept dfile value passed by arg
        if dfile is None:
            if mspass_object.is_defined("dfile"):
                dfile = mspass_object["dfile"]
            else:
                dfile = self._get_dfile_uuid(format)

        if os.path.exists(dir):
            if not os.access(dir, os.W_OK):
                raise PermissionError(
                    "Database._save_sample_data_file:  "
                    + "requested directory for saving data {} exists but is write protected (ownership?)".format(
                        dir
                    )
                )
        else:
            # Try to create dir if it doesn't exist
            newdir = pathlib.Path(dir)
            newdir.mkdir(parents=True, exist_ok=True)
        fname = os.path.join(dir, dfile)
        if os.path.exists(fname):
            if not os.access(fname, os.W_OK):
                raise PermissionError(
                    "Database._save_sample_data_file:  "
                    + "File {} exists but is write protected (ownership?)".format(fname)
                )

        # dir and dfile are now always set if we get here.  Now
        # we write the sample data - different blocks for atomic data
        # and ensembles
        if isinstance(mspass_object, (TimeSeries, Seismogram)):
            # With current logic test for None is unnecessary but
            # better preserved for miniscule cost
            if format == "binary":
                # Note file locks aren't needed for binary writes
                # because C implementation uses fwrite that is known
                # to be thread save (automatic locking unless forced off)
                try:
                    foff = _fwrite_to_file(mspass_object, dir, dfile)
                except MsPASSError as merr:
                    mspass_object.elog.log_error(merr)
                    mspass_object.kill()
                    # Use a dead datum to flag failure - caller must never
                    # call this method with a datum already marked dead
                    return mspass_object
                # we can compute the number of bytes written for this case
                if isinstance(mspass_object, TimeSeries):
                    nbytes_written = 8 * mspass_object.npts
                else:
                    # This can only be a Seismogram currently so this is not elif
                    nbytes_written = 24 * mspass_object.npts
            else:
                with open(fname, mode="a+b") as fh:
                    # this applies a unix lock - only works in linux
                    # note logic here assumes fcntl behavior that the
                    # lock is relesed when the file is closed
                    fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
                    if overwrite:
                        fh.seek(0, 0)
                        foff = 0
                    else:
                        fh.seek(0, 2)
                        foff = fh.tell()
                    f_byte = io.BytesIO()
                    if isinstance(mspass_object, TimeSeries):
                        mspass_object.toTrace().write(f_byte, format=format)
                    elif isinstance(mspass_object, Seismogram):
                        mspass_object.toStream().write(f_byte, format=format)
                        # DEBUG simplication of above
                        # strm = mspass_object.toStream()
                        # strm.write(f_byte,format=format)
                    # We now have to actually dump the buffer of f_byte
                    # to the file.  Is incantation does that
                    f_byte.seek(0)
                    ub = f_byte.read()
                    fh.write(ub)
                    nbytes_written = len(ub)
                    if nbytes_written <= 0:
                        message = "formatted write failure.  Number of bytes written={}".format(
                            nbytes_written
                        )
                        message += "Killing this datum during save will cause it to be buried in the cemetery"
                        mspass_object.kill()
                        mspass_object.elog.log_error(
                            "_save_sample_data_to_file", message, ErrorSeverity.Invalid
                        )

            mspass_object["storage_mode"] = "file"
            mspass_object["dir"] = dir
            mspass_object["dfile"] = dfile
            mspass_object["foff"] = foff
            mspass_object["nbytes"] = nbytes_written
            mspass_object["format"] = format
        elif isinstance(mspass_object, (TimeSeriesEnsemble, SeismogramEnsemble)):
            # this method assumes input is clean with no dead dead
            # againt format is None is not needed but preserved
            # note as for atomic data locking is not needed because
            # the function here is written in C and uses fwrite
            if format == "binary":
                try:
                    # this works because pytbin11 support overloading
                    foff_list = _fwrite_to_file(mspass_object, dir, dfile)
                except MsPASSError as merr:
                    mspass_object.elog.log_error(merr)
                    mspass_object.kill()
                    # Use a dead datum to flag failure.
                    # An ensemble marked dead on input won't get here
                    # but we aim here to make the output in the same
                    # state when this error handler is involked.
                    return mspass_object
                # Update metadata for each memkber
                for i in range(len(mspass_object.member)):
                    # Silently skip data marked dead
                    if mspass_object.member[i].live:
                        # use the strongly typed putters here intentionally
                        mspass_object.member[i].put_string("dir", dir)
                        mspass_object.member[i].put_string("dfile", dfile)
                        mspass_object.member[i].put_string("storage_mode", "file")
                        mspass_object.member[i].put_long("foff", foff_list[i])
                        mspass_object.member[i].put_string("format", "binary")
            else:
                # This probably should have a rejection for some formats
                # for which the algorithm here is problematic.  SAC is
                # the biggest case in point as it it wants one datum per file
                # which is at odds with the approach here
                with open(fname, mode="a+b") as fh:
                    # this applies a unix lock - only works in linux
                    # note logic here assumes fcntl behavior that the
                    # lock is relesed when the file is closed
                    fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
                    if overwrite:
                        fh.seek(0, 0)
                        foff = 0
                    else:
                        fh.seek(0, 2)
                        foff = fh.tell()
                    f_byte = io.BytesIO()
                    for i in range(len(mspass_object.member)):
                        # silently skip any dead members
                        if mspass_object.member[i].dead():
                            continue
                        d = mspass_object.member[i]
                        if isinstance(d, TimeSeries):
                            d.toTrace().write(f_byte, format=format)
                        else:
                            d.toStream().write(f_byte, format=format)
                        # same incantation as above to actually write the buffer
                        f_byte.seek(0)
                        ub = f_byte.read()
                        fh.write(ub)
                        nbytes_written = len(ub)
                        if nbytes_written <= 0:
                            message = "formatted write failure.  Number of bytes written={}".format(
                                nbytes_written
                            )
                            message += "Killing this datum during save will cause it to be buried in the cemetery"
                            d.kill()
                            d.elog.log_error(
                                "_save_sample_data_to_file",
                                message,
                                ErrorSeverity.Invalid,
                            )
                        mspass_object.member[i].put_string("dir", dir)
                        mspass_object.member[i].put_string("dfile", dfile)
                        mspass_object.member[i].put_string("storage_mode", "file")
                        mspass_object.member[i].put_long("foff", foff)
                        mspass_object.member[i].put_string("format", format)
                        mspass_object.member[i].put_long("nbytes", nbytes_written)
                        # make sure we are at the end of file
                        fh.seek(0, 2)
                        foff = fh.tell()
        return mspass_object

    def _save_sample_data_to_gridfs(
        self,
        mspass_object,
        overwrite=False,
    ):
        """
        Saves the sample data array for a mspass seismic data object using
        the gridfs file system.   For an ensemble all members are stored
        with independent gridids saved in the members returned.

        This function shares an assumptionw with the _save_sample_data_to_file
        that no dead data will be passed through the function.  We do that
        for efficiency as this is a private method that should normally be
        used only by the main save_data method.

        :param mspass_object:  is a seismic data object to be saved.  Reiterate
          it must not be marked dead and for ensembles no members should be
          marked dead.  If they are you will, at best, save useless junk.
          Results are better called unpredictable as the state of dead data is
          undefined and could only be a default constructed object.

        :type mspass_object:  one of TimeSeries, Seismogram,
          TimeSeriesEnsemble, or Seismogram Ensemble.

        :param overwrite:  When set True if there is an existing datum
          with a matching id for the attribute "gridfs_id", the existing datum
          will be deleted before the new data is saved.  When False a new
          set of documents will be created to hold the data in the gridfs
          system.  Default is False.

        :return: edited version of input (mspass_object).  Return changed
          only by adding metadata attributes "storage_mode" and "gridfs_id"
        """
        if mspass_object.dead():
            return mspass_object
        gfsh = gridfs.GridFS(self)

        if isinstance(mspass_object, (TimeSeries, Seismogram)):
            if overwrite and mspass_object.is_defined("gridfs_id"):
                gridfs_id = mspass_object["gridfs_id"]
                if gfsh.exists(gridfs_id):
                    gfsh.delete(gridfs_id)
            # verdion 2 had a transpose here that seemed unncessary
            ub = bytes(np.array(mspass_object.data))
            gridfs_id = gfsh.put(ub)
            mspass_object["gridfs_id"] = gridfs_id
            mspass_object["storage_mode"] = "gridfs"
        elif isinstance(mspass_object, (TimeSeriesEnsemble, SeismogramEnsemble)):
            for d in mspass_object.member:
                if d.live:
                    self._save_sample_data_to_gridfs(d, overwrite=overwrite)
        else:
            message = (
                "_save_sample_data_to_gridfs:  arg0 must be a MsPASS data object\n"
            )
            message += "Data type received=" + str(type(mspass_object))
            raise ValueError(message)

        return mspass_object

    def _atomic_save_all_documents(
        self,
        mspass_object,
        save_schema,
        exclude_keys,
        mode,
        wf_collection,
        save_history,
        data_tag,
        storage_mode,
        normalizing_collections,
        alg_name,
        alg_id,
    ):
        """
        Does all the MongoDB operations needed to save an atomic
        MsPASS data object.   That means the wf document,
        elog entries if defined, and history data if defined and requested.

        This is a private method that exists only to eliminate redundant
        code in the top-level save_data method.   Ensembles are handled
        by a loop over atomic data members calling this function for
        each member.   Because it is for only internal use the
        argument list is fixed (in other words no kwargs).   See
        save_data for what the arguments mean.
        """
        self._sync_metadata_before_update(mspass_object)
        insertion_dict, aok, elog = md2doc(
            mspass_object,
            save_schema,
            exclude_keys=exclude_keys,
            mode=mode,
            normalizing_collections=normalizing_collections,
        )
        # exclude_keys edits insertion_dict but we need to do the same to mspass_object
        # to assure whem data is returned it is identical to what would
        # come from reading it back
        if exclude_keys:
            for k in exclude_keys:
                # erase is harmless if k is not defined so we don't
                # guard this with an is_defined conditional
                mspass_object.erase(k)
        if elog.size() > 0:
            mspass_object.elog += elog
        if not aok:
            # aok false currently means the result is invalid and should be killed
            # currently means schema enforcement errors.
            # Warning:  this can lead to sample data save orphans
            mspass_object.kill()

        # Always set starttime and endtime
        if not exclude_keys or "starttime" not in exclude_keys:
            insertion_dict["starttime"] = mspass_object.t0
        if not exclude_keys or "endtime" not in exclude_keys:
            insertion_dict["endtime"] = mspass_object.endtime()

        # add tag - intentionally not set in mspass_object returned
        if data_tag:
            insertion_dict["data_tag"] = data_tag
        else:
            # We need to clear data tag if was previously defined in
            # this case or a the old tag will be saved with this datum
            if "data_tag" in insertion_dict:
                insertion_dict.pop("data_tag")
        if storage_mode:
            insertion_dict["storage_mode"] = storage_mode
        else:
            # gridfs default
            insertion_dict["storage_mode"] = "gridfs"
        # We depend on Undertaker to save history for dead data
        if save_history and mspass_object.live:
            history_obj_id_name = (
                self.database_schema.default_name("history_object") + "_id"
            )
            history_object_id = None
            # is_empty is a method of ProcessingHistory - mame is generic so possibly confusing
            if mspass_object.is_empty():
                # Use this trick in update_metadata too. None is needed to
                # avoid a TypeError exception if the name is not defined.
                # could do this with a conditional as an alternative
                insertion_dict.pop(history_obj_id_name, None)
            else:
                # optional history save - only done if history container is not empty
                # first we need to push the definition of this algorithm
                # to the chain.  Note it is always defined by the special
                # save defined with the C++ enum class mapped to python
                # vi ProcessingStatus
                if isinstance(mspass_object, TimeSeries):
                    atomic_type = AtomicType.TIMESERIES
                elif isinstance(mspass_object, Seismogram):
                    atomic_type = AtomicType.SEISMOGRAM
                else:
                    raise TypeError("only TimeSeries and Seismogram are supported")
                mspass_object.new_map(
                    alg_name, alg_id, atomic_type, ProcessingStatus.SAVED
                )
                history_object_id = self._save_history(mspass_object, alg_name, alg_id)
                insertion_dict[history_obj_id_name] = history_object_id
                mspass_object[history_obj_id_name] = history_object_id
        # save elogs if the size of elog is greater than 0
        elog_id = None
        if mspass_object.elog.size() > 0:
            elog_id_name = self.database_schema.default_name("elog") + "_id"
            # elog ids will be updated in the wf col when saving metadata
            elog_id = self._save_elog(mspass_object, elog_id=None, data_tag=data_tag)
            insertion_dict[elog_id_name] = elog_id
            mspass_object[elog_id_name] = elog_id

        # finally ready to insert the wf doc - keep the id as we'll need
        # it for tagging any elog entries.
        # Note we don't save if something above killed mspass_object.
        # currently that only happens with errors in md2doc, but if there
        # are changes use the kill mechanism
        if mspass_object.live:
            wfid = wf_collection.insert_one(insertion_dict).inserted_id

            # Put wfid into the object's meta as the new definition of
            # the parent of this waveform
            mspass_object["_id"] = wfid
            if save_history and mspass_object.live:
                # When history is enable we need to do an update to put the
                # wf collection id as a cross-reference.    Any value stored
                # above with saave_history may be incorrect.  We use a
                # stock test with the is_empty method for know if history data is present
                if not mspass_object.is_empty():
                    history_object_col = self[
                        self.database_schema.default_name("history_object")
                    ]
                    wf_id_name = wf_collection.name + "_id"
                    filter_ = {"_id": history_object_id}
                    update_dict = {wf_id_name: wfid}
                    history_object_col.update_one(filter_, {"$set": update_dict})
        else:
            mspass_object = self.stedronsky.bury(
                mspass_object, save_history=save_history
            )
        return mspass_object

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
        other formats.  If None treat format as "native_double"
        The return value is random uuid + "-" + format, example:
        '247ce5d7-06bc-4ccc-8084-5b3ff621d2d4-mseed'

        :param format: format of the file
        """
        if format is None:
            format = "native_double"
        temp_uuid = str(uuid.uuid4())
        dfile = temp_uuid + "-" + format
        return dfile

    def _construct_atomic_object(
        self,
        md,
        object_type,
        merge_method,
        merge_fill_value,
        merge_interpolation_samples,
        aws_access_key_id,
        aws_secret_access_key,
    ):
        func = "Database._construct_atomic_object"
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

        if md.is_defined("storage_mode"):
            storage_mode = md["storage_mode"]
        else:
            # hard code this default
            storage_mode = "gridfs"

        if storage_mode == "file":
            if md.is_defined("format"):
                form = md["format"]
            else:
                # Reader uses this as a signal to use raw binary fread C function
                form = None
            # TODO:  really should check for all required md values and
            # do a kill with an elog message instead of depending on this to abort
            if form and form != "binary":
                if md.is_defined("nbytes"):
                    nbytes_expected = md["nbytes"]
                    if nbytes_expected <= 0:
                        message = "Formatted reader received size attribute nbytes={}\n".format(
                            nbytes_expected
                        )
                        message += "Null file caused this datum to be killed"
                        mspass_object.elog.log_error(
                            func, message, ErrorSeverity.Invalid
                        )
                        mspass_object.kill()
                    else:
                        self._read_data_from_dfile(
                            mspass_object,
                            md["dir"],
                            md["dfile"],
                            md["foff"],
                            nbytes=nbytes_expected,
                            format=form,
                            merge_method=merge_method,
                            merge_fill_value=merge_fill_value,
                            merge_interpolation_samples=merge_interpolation_samples,
                        )
                else:
                    message = "Missing required argument nbytes for formatted read"
                    message += "Cannot construct this datum"
                    mspass_object.elog.log_error(func, message, ErrorSeverity.Invalid)
                    mspass_object.kill()
            else:
                self._read_data_from_dfile(
                    mspass_object,
                    md["dir"],
                    md["dfile"],
                    md["foff"],
                    merge_method=merge_method,
                    merge_fill_value=merge_fill_value,
                    merge_interpolation_samples=merge_interpolation_samples,
                )
        elif storage_mode == "gridfs":
            self._read_data_from_gridfs(mspass_object, md["gridfs_id"])
        elif storage_mode == "url":
            self._read_data_from_url(
                mspass_object,
                md["url"],
                format=None if "format" not in md else md["format"],
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
            # Earlier verision raised a TypeError with the line below
            # Changed after V2 to kill and lot as an error - will be an abortion
            # raise TypeError("Unknown storage mode: {}".format(storage_mode))
            # note logic above assures storage_mode is not a None
            message = "Illegal storage mode={}\n".format(storage_mode)
            mspass_object.elog.log_error(func, message, ErrorSeverity.Invalid)
            mspass_object.kill()

        if mspass_object.live:
            mspass_object.clear_modified()
        else:
            mspass_object["is_abortion"] = True
        return mspass_object

    @staticmethod
    def _group_mdlist(mdlist) -> dict:
        """
        Private method to separate the documents in mdlist into groups
        that simplify reading of ensembles.

        We allow ensembles o be created from data retrieved and constucted
        by mulitple methods.    Currently that is defined by three concepts
        we have to separate:
            1.  The "storage_mode" passed to readers defines the generic
                way data are stored.  The list of storage_mode options
                is fairly long.  The point is that different storage modes
                require fundamentally different read strategies.  e.g. reading
                from s3 is not remotely at all like reading from gridfs
                but both can yield valid data objects.
            2.  A "format" can be used to define a format conversion that
                needs to be preformed on the fly to convert the data to
                mspass atomic data object.
            3.  A special case with storage mode == "files is when the
                data are organized into files already defined for this
                enemble being read.  A typical example would be data already
                bundled into n ensemble and saved that way with the
                Database.save_data method in an earlier run.  Ensemble
                files can be read faster by reducing the need for
                unnecessary file open/close pairs that are otherwise
                needed when reading atomic data.

        This function takes the input list of documents converted to
        to a list of `mspasspy.ccore.utility.Metadata` objects and sorts
        the contents into containers that define data that can be
        read by a common method.   We do that by returning a nested
        dictionary.  The top level of the dictionary is keyed by
        the "storage_mode".   Most inputs are likely to only generate
        a single key but this approach allows heterogeous input
        definitions in a database.   Each value then defines a second
        dictionary with a key defined by the "format" field.   The
        leaves of the returned tree of Metadata are then lists with
        common values of "storage_mode" and "format".

        Item 3 above is handled by a related private method called
        `Database._group_by_path`.   It is used in this class to
        handle ensemble file reading as noted in concept 3 above.

        """
        smdict = dict()
        for md in mdlist:
            if "storage_mode" in md:
                sm = md["storage_mode"]
            else:
                # WARNING:  this default needs to stay consistent internally
                # perhaps should have it in self but isn't for now
                sm = "gridfs"
            if "format" in md:
                form = md["format"]
            else:
                # WARNING:  this is also a default that needs to stay
                # in sync with other places in this class it is used
                form = "binary"
            if sm in smdict:
                formdict = smdict[sm]
                if form in formdict:
                    vallist = formdict[form]
                else:
                    vallist = []
                vallist.append(md)
                smdict[sm][form] = vallist
            else:
                # we have to create the secondary dict when we land
                # here because it wouldn't have been previously allocated
                formdict = dict()
                formdict[form] = [md]
                smdict[sm] = formdict

        return smdict

    @staticmethod
    def _group_by_path(mdlist, undefined_key="undefined_filename") -> list:
        """
        Takes input list of Metadata containers and groups them
        by a "path" defined as the string produced by combining
        the "dir" and "dfile" attribute in a standard way.
        The return is a python dict keyed by path and values being
        a list extracted from mdlist of all ehtries defined by the same
        file.

        We handle the case of mdlist entries missing "dir" or "dfile" by
        putting them into a list keyed by the string defined by the
        "udnefined_key" attribute (see above for default.)   Caller should
        treat these as abortions.
        """
        pathdict = dict()
        for md in mdlist:
            if "dir" in md and "dfile" in md:
                key = md["dir"] + "/" + md["dfile"]
                if key in pathdict:
                    pathdict[key].append(md)
                else:
                    pathdict[key] = [md]
            else:
                if undefined_key in pathdict:
                    pathdict[undefined_key].append(md)
                else:
                    pathdict[undefined_key] = [md]
        return pathdict

    def _load_ensemble_file(self, mdlist, object_type, dir, dfile):
        """
        Private method to efficiently load ensembles stored in a file.

        When reading a ensemble stored in one or more files it is much
        faster to open the file containing multiple records and read
        that file sequentially than using an atomic read for each member
        that would open, seek, read, and close the file for each member.
        This method standardizes reading from a single file when
        the data are stored as raw binary samples (C double floating point).
        It can be used to construct either a SeismogramEnsemble or
        TimeSeriesEnsemble driven by an input list of MongoDB
        documents convert to list of Metadata containers.

        The algorithm runs fast by using a C++ function bound to python
        with pybind11 (_fread_from_file).  For that reason this method
        must ONLY be called for a list with all the data from a single
        file.   The algorithm is designed for speed so it does not verify
        that each entry in mdlist has the same dir and dfile as that
        passed as arguments.   Garbage read or seg faults are nearly
        guaranteed if that is violated and this method is used outside
        the normal use as a component of the chain of private methods used
        by the `Database.read_data` method.  i.e. use this method with
        extreme caution if used for a different application than
        internal use for the Database class.

        The algorithm has to do some fairly weird things to match the
        API of _fread_from_file and to handle the range of errors
        that can happen in this composite read operation.   Some points
        to note for maintenance of this function are:
            1.  This utilizes the newer concept in MsPASS of abortions.
                Any error that invalidates a member ends up putting that
                datum into component[1] of the returned tuple (see below).
                The result is a clean emsembled return in component 0
                meaning it has no dead members.
            2.  For efficiency the algorithm produces a list of
                member index numbers. By that it means the
                member[i] has index value i.  What that means is that
                the array passed to _fread_from_file is a list of i
                values telling the reader in which member slot the
                data are to be loaded.   The list is sorted by foff
                to allow fread to do it's job efficiently because
                buffering makes sequential reads with fread about as fast
                as any io operation possible.  The alternative would
                have been to have _fread_from_file read in i order
                (i.e. a normal loop) but that could involve a lot of
                seeks positions outside the buffer.  Future maintainers
                are warned of this rather odd algorithm, but note it
                was done that way for speed.

        :param mdlist: iterable list of Metadata objects.  These are used
         to create the ensemble members using a feature of the C++ api that
         a skeleton of an atomic object can be created form Metadata alone.
         The sample array is allocated and defined from number of points
         defined in the Metadata container and that is used by fread to
         load the sample data when reading.   Needless to say corrupted
         data for npts or foff are guaranteed trouble.  Other Metadata
         attributes are copied verbatim to the atomic data. If any
         constructors using the Metadata fail the error message is posted
         and these are pushed to the list of abortions.
        :type mslist:  iterable container of Metadata objects (normally a list)
        :param object_type: used internally to distinguish TimeSeriesEnsemble
          and SeismogramEnsemble
        :param dir: directory name while for file that is to be openned.
        :param dfile: file name for file that is to be openned and read.

        :return:  tuple with two components.
          0 - ensemble that is the result.  It will be marked dead if no
              members were successfully read
          1 - a list of members (not an ensemble a python list) of
              data that failed during construction for one reason or another.
              We refer to this type of dead data in MsPASS as an abortion
              as it died before it was born.
        """
        # Because this is private method that should only be used
        # interally we do no type checking for speed.
        nmembers = len(mdlist)
        if object_type is TimeSeries:
            ensemble = TimeSeriesEnsemble(nmembers)
        elif object_type is Seismogram:
            ensemble = SeismogramEnsemble(nmembers)
        else:
            message = "Database._load_enemble_file:  Received illegal value for object_type argument\n"
            message += "Received object_type={}\n".format(object_type)
            message += "Must be eithre TimeSeriesEnsemble or SeismogramEnsemble"
            raise ValueError(message)

        abortions = []
        for md in mdlist:
            try:
                # Note a CRITICAL feature of the Metadata constructors
                # for both of these objects is that they allocate the
                # buffer for the sample data and initialize it to zero.
                # This allows sample data readers to load the buffer without
                # having to handle memory management.
                if object_type is TimeSeries:
                    d = TimeSeries(md)
                    ensemble.member.append(d)
                else:
                    # api mismatch here.  This ccore Seismogram constructor
                    # had an ancestor that had an option to read data here.
                    # we never do that here
                    d = Seismogram(md, False)
                    ensemble.member.append(d)
            except MsPASSError as merr:
                # if the constructor fails mspass_object will be invalid
                # To preserve the error we have to create a shell to hold the error
                if object_type is TimeSeries:
                    d = TimeSeries()
                else:
                    d = Seismogram()
                # Default constructors leaves result marked dead so below should work
                d.elog.log_error(merr)
                # there may be a C++ function to copy Metadata like this
                for k in md:
                    d[k] = md[k]
                # We push bad data to the abortions list.  Caller should
                # handle them specially.
                abortions.append(d)

        # this constructs a list of pairs (tuples) with foff and npts
        # values.
        foff_list = list()
        for i in range(len(ensemble.member)):
            # convenient alias only here.  Works because in python this just
            # sets d as a pointer and doesn't create a copy
            d = ensemble.member[i]
            if d.is_defined("foff"):
                t = tuple([d["foff"], i])
                foff_list.append(t)
            else:
                # These members will not be handled by the
                # _fread_from_file C++ function below.  It sets data
                # live on success.  Because these don't generate an
                # entrty in foff_list they will not be handled.
                message = "This datum was missing required foff attribute."
                d.elog.log_error(
                    "Database._load_enemble_file", message, ErrorSeverity.Invalid
                )

        # default for list sort is to use component 0 and sort in ascending
        # order.  That is waht we want for read efficiency
        foff_list.sort()
        # I think pybind11 maps a list of int values to used as
        # input to the fread function below requiring an
        # input of an std::vector<long int> container.  In C++ a vector
        # and list are different concepts but I think that is what pybind11 does
        index = []
        for x in foff_list:
            index.append(x[1])

        # note _fread_from_file extracs foff from each member using index
        # to (potentially) skip around in the container
        # it never throws an exception but can return a negative
        # number used to signal file open error
        count = _fread_from_file(ensemble, dir, dfile, index)
        if count > 0:
            ensemble.set_live()
        else:
            message = "Open failed on dfile={} for dir={}".format(dfile, dir)
            ensemble.elog.log_error(
                "Database._load_ensemble_file", message, ErrorSeverity.Invalid
            )

        # Use this Undertaker method to pull out any data killed during
        # the read.  Add them to the abortions list to create a clean
        # ensemble to return
        cleaned_ensemble, bodies = self.stedronsky.bring_out_your_dead(ensemble)
        if len(bodies) > 0:
            for d in bodies:
                abortions.append(d)

        return cleaned_ensemble, abortions

    def _construct_ensemble(
        self,
        mdlist,
        object_type,
        merge_method,
        merge_fill_value,
        merge_interpolation_samples,
        aws_access_key_id,
        aws_secret_access_key,
    ):
        """
        Private method to create an ensemble from a list of Metadata
        containers. Like the atomic version but for ensembles.


        """
        # Because this is expected to only be used internally there
        # is not type checking or arg validation.
        nmembers = len(mdlist)
        if object_type is TimeSeries:
            ensemble = TimeSeriesEnsemble(nmembers)
        else:
            ensemble = SeismogramEnsemble(nmembers)

        # first group by storage mode
        smdict = self._group_mdlist(mdlist)
        for sm in smdict:
            if sm == "file":
                # only binary currently works for ensemble files
                file_dict = smdict["file"]
                for form in file_dict:
                    if form == "binary":
                        bf_mdlist = file_dict["binary"]
                        # This method, defined above, groups data by
                        # file name
                        bf_dict = self._group_by_path(bf_mdlist)
                        for path in bf_dict:
                            this_mdl = bf_dict[path]
                            if len(this_mdl) > 1:
                                # _group_by_path assures all compomnents of this_mdl
                                # have the same value of dir and file.  Use an iteration to
                                # assure dir and dfile are present in one of the components
                                # TODO:  this may have an issue if there are entries marked undefined
                                # needs a controlled tests.
                                dir = "UNDEFINED"
                                for md in this_mdl:
                                    if md.is_defined("dir") and md.is_defined("dfile"):
                                        dir = md["dir"]
                                        dfile = md["dfile"]
                                        break
                                if dir == "UNDEFINED":
                                    message = "Database.construct_ensemble:  "
                                    message += "binary file reader section could not find values for dir and/or dfile"
                                    message += "This should not happen but was trapped to avoid mysterious errors"
                                    raise MsPASSError(message, ErrorSeverity.Fatal)

                                ens_tmp, abortions = self._load_ensemble_file(
                                    this_mdl, object_type, dir, dfile
                                )

                                # the above guarantees ens_tmp has no dead dead
                                for d in ens_tmp.member:
                                    ensemble.member.append(d)
                                for d in abortions:
                                    self.stedronsky.handle_abortion(d)
                            elif len(this_mdl) == 1:
                                d = self._construct_atomic_object(
                                    this_mdl[0],
                                    object_type,
                                    merge_method,
                                    merge_fill_value,
                                    merge_interpolation_samples,
                                    aws_access_key_id,
                                    aws_secret_access_key,
                                )
                                if d.live:
                                    ensemble.member.append(d)
                                else:
                                    self.stedronsky.handle_abortion(d)
                            else:
                                message = "Database._construct_ensemble:  "
                                message += "Reading binary file section retrieved a zero length"
                                message += " list of Metadata associated with path key={}\n".format(
                                    path
                                )
                                message += "Bug that shouldn't happen but was trapped as a safety"
                                raise MsPASSError(message, ErrorSeverity.Fatal)
                    else:
                        # for all formatted data we currently only use atomic reader
                        this_mdlist = file_dict[form]
                        for md in this_mdlist:
                            d = self._construct_atomic_object(
                                md,
                                object_type,
                                merge_method,
                                merge_fill_value,
                                merge_interpolation_samples,
                                aws_access_key_id,
                                aws_secret_access_key,
                            )
                            if d.live:
                                ensemble.member.append(d)
                            else:
                                self.stedronsky.handle_abortion(d)
            else:
                for form in smdict[sm]:
                    this_mdlist = smdict[sm][form]
                    for md in this_mdlist:
                        d = self._construct_atomic_object(
                            md,
                            object_type,
                            merge_method,
                            merge_fill_value,
                            merge_interpolation_samples,
                            aws_access_key_id,
                            aws_secret_access_key,
                        )
                        if d.live:
                            ensemble.member.append(d)
                        else:
                            self.stedronsky.handle_abortion(d)
        if len(ensemble.member) > 0:
            ensemble.set_live()
        return ensemble


def index_mseed_file_parallel(db, *arg, **kwargs):
    """
    A parallel wrapper for the index_mseed_file method in the Database class.
    We use this wrapper to handle the possible error in the original method,
    where the file dir and name are pointing to a file that doesn't exist.
    User could use this wrapper when they want to run the task in parallel,
    result will then be an RDD/bag of either None or error message strings.
    User would need to scan the RDD/bag to search for thing not None for errors.

    :param db: The MsPass core database handle that we want to index into
    :param arg: All the arguments that users pass into the original
      index_mseed_file method
    :return: None or error message string
    """
    ret = None
    try:
        db.index_mseed_file(*arg, **kwargs)
    except FileNotFoundError as e:
        ret = str(e)
    return ret


def md2doc(
    md,
    save_schema,
    exclude_keys=None,
    mode="promiscuous",
    normalizing_collections=["channel", "site", "source"],
) -> {}:
    """
    Converts a Metadata container to a python dict applying a schema constraints.

    This function is used in all database save operations to guaranteed the
    Metadata container in a mspass data object is consistent with
    requirements for MongDB defined by a specified schema.   It
    dogmatically enforces readonly restrictions in the schema by
    changing the key for any fields marked readonly and found to have
    been set as changed.  Such entries change to "READONLYERROR_" + k
    where k is the original key marked readonly.  See user's manual for
    a discussion of why this is done.

    Other schema constraints are controlled by the setting of mode.
    Mode must be one of "promiscuous","cautious", or "pedantic" or
    the function will raise a MsPASS error marked fatal.   That is the
    only exception this function can throw.  It will never happen when
    used with the Database class method but is possible if a user uses
    this function in a different implementation.

    The contents of the data associated with the md argument (arg0) are assumed
    to have passed through the private database method _sync_metadata_before_update
    before calling this function.  Anyone usingn this function outside the
    Database class should assure a comparable algorithm is not required.

    Note the return is a tuple.  See below for details.

    :param md: contains a Metadata container that is to be converted.
    :type md:  For normal use in mspass md is a larger data object that
      inherits Metadata.  That is, in most uses it is a TimeSeries or
      Seismogram object.  It can be a raw Metadata container and the
      algorithm should work
    :param save_schema:  The Schema class to be used for constraints
      in building the doc for MongoDB use.  See User's Manual for details
      on how a schema is defined and used in MsPASS.
    :type save_schema:  Schema class
    :param exclude_keys: list of keys (strings) to be excluded from
      the output python dict.   Note "_id" is always excluded to mess
      with required MongoDB usage.  Default is None which means no
      values are excluded.  Note is harmless to list keys that are not
      present in md - does nothing except for a minor cost to test for existence.
    :type exclude_keys: list of strings
    :param mode: Must be one of "promiscuous", "caution", or "pedantic".
      See User's Manual for which to use.  Default is "promiscuous".
      The function will throw MsPASSError exception if not one of the three
      keywords list.
    :type mode: str

    :return:  Result is a returned as a tuple appropriate for the
      normal use of this function inside the Database class.
      The contents of the tuple are:

      0 - python dictionary of edited result ready to save as MongoDB document
      1 - boolean equivalent of the "live" attribute of TimeSeries and Seismogram.
          i.e. if True the result can be considered valid.  If False something
          was very wrong with the input and the contents of 0 is invalid and
          should not be used.  When False the error log in 2 will contain
          one or more error messages.
      2 - An ErrorLogger object that may or may not contain any error logging
          messages.  Callers should call the size method of the this entry
          and handle the list of error messages it contains if size is not zero.
          Note the right way to do that for TimeSeries and Seismogram is to
          use operator += for the elog attribute of the datum.
    """
    # this is necessary in case someone uses this outside Database
    # it should never happen when used by Database methods
    if not (mode in ["promiscuous", "cautious", "pedantic"]):
        message = "md2doc:  Illegal value for mode=" + mode
        message += " must be one of: promiscuous, cautious, or pedantic"
        raise MsPASSError(message, ErrorSeverity.Fatal)

    # Result is a tuple made up of these three items.  Return
    # bundles them.  Here we define them as symbols for clarity
    insertion_doc = dict()
    aok = True
    elog = ErrorLogger()

    if exclude_keys is None:
        exclude_keys = []

    # Original code had this - required now to be done by caller
    # self._sync_metadata_before_update(mspass_object)

    # First step in this algorithm is to edit the metadata contents
    # to handle a variety of potential issues.
    # This method of Metadata returns a list of all
    # attributes that were changed after creation of the
    # object to which they are attached.
    changed_key_list = md.modified()

    copied_metadata = Metadata(md)

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
    # the mspass schema.  It would be cleared in the following loop
    # but it is better to not depend on that external constraint.
    # The reason is the insert_one used below for wf collections
    # will silently update an existing record if the _id key
    # is present in the update record.  We want this method
    # to always save the current copy with a new id and so
    # we make sure we clear it
    if "_id" in copied_metadata:
        copied_metadata.erase("_id")

    # always strip normalizing data from standard collections
    # Note this usage puts the definition of "standard collection"
    # to the default of the argument normalizing_collection of this
    # function.   May want to allow callers to set this and add a
    # value for the list to args of this function
    copied_metadata = _erase_normalized(copied_metadata, normalizing_collections)
    # this section creates a python dict from the metadata container.
    # it applies safties based on mode argument (see user's manual)
    if mode == "promiscuous":
        # A python dictionary can use Metadata as a constructor due to
        # the way the bindings were defined
        insertion_doc = dict(copied_metadata)
    else:
        for k in copied_metadata.keys():
            if save_schema.is_defined(k):
                if save_schema.readonly(k):
                    if k in changed_key_list:
                        newkey = "READONLYERROR_" + k
                        copied_metadata.change_key(k, newkey)
                        elog.log_error(
                            "Database.save_data",
                            "readonly attribute with key="
                            + k
                            + " was improperly modified.  Saved changed value with key="
                            + newkey,
                            ErrorSeverity.Complaint,
                        )
                    else:
                        copied_metadata.erase(k)

        # Other modes have to test every key and type of value
        # before continuing.  pedantic kills data with any problems
        # Cautious tries to fix the problem first
        # Note many errors can be posted - one for each problem key-value pair
        for k in copied_metadata:
            if save_schema.is_defined(k):
                if isinstance(copied_metadata[k], save_schema.type(k)):
                    insertion_doc[k] = copied_metadata[k]
                else:
                    if mode == "pedantic":
                        aok = False
                        message = "pedantic mode error:  key=" + k
                        value = copied_metadata[k]
                        message += (
                            " type of stored value="
                            + str(type(value))
                            + " does not match schema expectation="
                            + str(save_schema.type(k))
                        )
                        elog.log_error(
                            "Database.save_data",
                            "message",
                            ErrorSeverity.Invalid,
                        )
                    else:
                        # Careful if another mode is added here.  else means cautious in this logic
                        try:
                            # The following convert the actual value in a dict to a required type.
                            # This is because the return of type() is the class reference.
                            insertion_doc[k] = save_schema.type(k)(copied_metadata[k])
                        except Exception as err:
                            message = "cautious mode error:  key=" + k
                            #  cannot convert required keys -> kill the object
                            if save_schema.is_required(k):
                                aok = False
                                message = "cautious mode error:  key={}\n".format(k)
                                message += "Required key value could not be converted to required type={}\n".format(
                                    str(save_schema.type(k))
                                )
                                message += "Actual type={}\n".format(
                                    str(type(copied_metadata[k]))
                                )
                                message += "\nPython error exception message caught:\n"
                                message += str(err)
                                elog.log_error(
                                    "Database.save",
                                    message,
                                    ErrorSeverity.Invalid,
                                )
                            else:
                                message += "\nValue associated with this key "
                                message += "could not be converted to required type={}\n".format(
                                    str(save_schema.type())
                                )
                                message += "Actual type={}\n".format(
                                    str(type(copied_metadata[k]))
                                )
                                message += (
                                    "Entry for this key will not be saved in database"
                                )
                                elog.log_error(
                                    "md2doc",
                                    message,
                                    ErrorSeverity.Complaint,
                                )
                                copied_metadata.erase(k)
    return [insertion_doc, aok, elog]


def elog2doc(elog) -> dict:
    """
    Extract error log messages for storage in MongoDB

    This function can be thought of as a formatter for an ErrorLogger
    object.   The error log is a list of what is more or less a C struct (class)
    This function converts the log to list of python dictionaries
    with keys being the names of the symbols in the C code.  The list
    of dict objects is then stored in a python dictionary with the
    single key "logdata" that is returned.  If the log is entry
    an empty dict is returned.  That means the return is either empty
    or with one key-value pair with key=="logdata".

    :param elog:   ErrorLogger object to be reformatted.
      (Note for all mspass seismic data objects == self.elog)
    :return:  python dictionary as described above
    """
    if isinstance(elog, ErrorLogger):
        doc = dict()
        if elog.size() > 0:
            logdata = []
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
            doc = {"logdata": logdata}
        # note with this logic if elog is empty returns an empty dict
        return doc
    else:
        message = "elog2doc:  "
        message += "arg0 has unsupported type ={}\n".format(str(type(elog)))
        message += "Must be an instance of mspasspy.ccore.util.ErrorLogger"
        raise TypeError(message)


def history2doc(
    proc_history,
    alg_id=None,
    alg_name=None,
    job_name=None,
    job_id=None,
) -> dict:
    """
    Extract ProcessingHistory data and package into a python dictionary.

    This function can be thought of as a formatter for the ProcessingHistory
    container in a MsPASS data object.  It returns a python dictionary
    that, if retrieved, can be used to reconstruct the ProcessingHistory
    container.  We do that a fairly easy way here by using pickle.dumps
    of the container that is saved with the key "processing_history".

    :param proc_history:   history container to be reformatted
    :type proc_history:  Must be an instance of a
      mspasspy.ccore.util.ProcessingHistory object or a TypeError
      exception will be thrown.

    :param alg_id:   algorithm id for caller.   By default this is
      extracted from the last entry in the history tree.  Use other
      than the default should be necessary only if called from a
      nonstandard writer.
      (See C++ doxygen page on ProcessingHistory for concept ).
    :param alg_id:  string

    :param alg_name:  name of calling algorithm.   By default this is
      extracted from the last entry in the history tree.  Use other
      than the default should be necessary only if called from a
      nonstandard writer.
      (See C++ doxygen page on ProcessingHistory for concept ).
    :type alg_name:  string

    :param job_name:  optional job name string.  If set the value
      will be saved in output with the "job_name".  By default
      there will be no value for the "job_name" key
    :type job_name:  string (default None taken to mean do not save)

    :param job_id:  optional job id string.  If set the value
      will be saved in output with the "job_id".  By default
      there will be no value for the "job_id" key
    :type job_id:  string (default None taken to mean do not save)

    :return:  python dictoinary
    """
    if not isinstance(proc_history, ProcessingHistory):
        message = "history2doc:  "
        message += "arg0 has unsupported type ={}\n".format(str(type(proc_history)))
        message += "Must be an instance of mspasspy.ccore.util.ProcessingHistory"
        raise TypeError(message)
    current_uuid = proc_history.id()  # uuid in the current node
    current_nodedata = proc_history.current_nodedata()
    current_stage = proc_history.stage()
    # get the alg_name and alg_id of current node
    if alg_id is None:
        alg_id = current_nodedata.algid
    if alg_name is None:
        alg_name = current_nodedata.algorithm

    history_binary = pickle.dumps(proc_history)
    doc = {
        "save_uuid": current_uuid,
        "save_stage": current_stage,
        "processing_history": history_binary,
        "alg_id": alg_id,
        "alg_name": alg_name,
    }
    if job_name:
        doc["job_name"] = job_name
    if job_id:
        doc["job_id"] = job_id
    return doc


def doc2md(
    doc, database_schema, metadata_schema, wfcol, exclude_keys=None, mode="promiscuous"
):
    """
    This function is more or less the inverse of md2doc.   md2doc is
    needed by writers to convert Metadata to a python dict for saving
    with pymongo.   This function is similarly needed for readers to
    translate MongoDB documents into the Metadata container used by
    MsPASS data objects.

    This function can optionally apply schema constraints using the
    same schema class used by the Database class.  In fact, normal use
    would pass the schema class from the instance of Database that was
    used in loading the document to be converted (arg0).

    This function was built from a skeleton that was originally part of
    the read_data method of Database.  Its behavior for
    differnt modes is inherited from that implementation for backward
    compatibility.  The returns structure is a necessary evil with that
    change in the implementatoin, but is exactly the same as md2doc for
    consistency.

    The way the mode argument is handled is slightly different than
    for md2doc because of the difference in the way this function is
    expected to be used.  This function builds the Metadata container
    that is used in all readers to drive the construction of atomic
    data objects.  See below for a description of what different settings
    of mode.

    :param doc:  document (dict) to be converted to Metadata
    :type doc:  python dict assumed (there is no internal test for efficiency)
      An associative array with string keys operator [] are the main requirements.
      e.g. this function might work with a Metadata container to apply
      schema constraints.

    :param metadata_schema:  instance of MetadataSchema class that can
      optionally be used to impose schema constraints.
    :type metadata_schema:  :class:`mspasspy.db.schema.MetadataSchema`

    :param wfcol:  Collection name from which doc was retrieved.   It should
      normally alreacy be known by the caller so we require it to be passed
      with this required arg.
    :type wfcol:   string

    :param mode: read mode as described in detail in User's Manual.
      Behavior for this function is as follows:
        "promiscuous" - (default)  no checks are applied to any key-value
           pairs and the result is a one-to-one translation of the input.
        "cautious" - Type constraints in the schema are enforced and
          automatically conveted if possible.  If conversion is needed
          and fails the live/dead boolan in the return will be set to
          signal this datum should be killed.  There will also be elog entries.
        "pedantic" - type conversions are strongly enforced.  If any
          type mismatch of a value occurs the live/dead boolean returned
          will be set to signal a kill and there will be one or more
          error messages in the elog return.
    :type mode: string (must match one of the above or the function will throw
        a ValueError exception.

    :return 3-component tuple:  0 = converted Metadata container,
      1 - boolean equivalent to "live".  i.e. if True the results is valid
      while if False constructing an object from the result is ill advised,
      2 - ErrorLogger object containing in error messages.  Callers should
        test if the result of the size method of the return is > 0 and
        handle the error messages as desired.

    """
    elog = ErrorLogger()
    if mode == "promiscuous":
        md = Metadata(doc)
        if exclude_keys:
            for k in exclude_keys:
                if md.is_defined(k):
                    md.erase(k)
        aok = True
    else:
        md = Metadata()
        dropped_keys = []

        for k in doc:
            if exclude_keys:
                if k in exclude_keys:
                    continue

            if metadata_schema.is_defined(k) and not metadata_schema.is_alias(k):
                md[k] = doc[k]
            else:
                dropped_keys.append(k)
        if len(dropped_keys) > 0:
            message = "While running with mode={} found {} entries not defined in schema\n".format(
                mode, len(dropped_keys)
            )
            message += "The attributes linked to the following keys were dropped from converted Metadata container\n"
            for d in dropped_keys:
                message += d
                message += " "
            elog.log_error("doc2md", message, ErrorSeverity.Complaint)
        aok = True
        fatal_keys = []
        converted_keys = []
        if mode == "cautious":
            for k in md:
                if metadata_schema.is_defined(k):
                    unique_key = database_schema.unique_name(k)
                    if not isinstance(md[k], metadata_schema.type(k)):
                        # try to convert the mismatch attribute
                        try:
                            # convert the attribute to the correct type
                            md[k] = metadata_schema.type(k)(md[k])
                        except:
                            if database_schema.is_required(unique_key):
                                fatal_keys.append(k)
                                aok = False
                                message = "cautious mode: Required attribute {} has type {}\n".format(
                                    k, str(type(md[k]))
                                )
                                message += "Schema requires this attribute have type={}\n".format(
                                    str(metadata_schema.type(k))
                                )
                                message += "Type conversion not possible - datum linked to this document will be killed"
                                elog.log_error("doc2md", message, ErrorSeverity.Invalid)
                        else:
                            converted_keys.append(k)
            if len(converted_keys) > 0:
                message = "WARNING:  while running in cautious mode the value associated with the following keys required an automatic type conversion:\n"
                for c in converted_keys:
                    message += c
                    message += " "
                message += "\nRunning clean_collection method is recommended"
                elog.log_error("doc2md", message, ErrorSeverity.Informational)
            if len(fatal_keys) > 0:
                aok = False

        elif mode == "pedantic":
            for k in md:
                if metadata_schema.is_defined(k):
                    if not isinstance(md[k], metadata_schema.type(k)):
                        fatal_keys.append(k)
                        aok = False
                        message = (
                            "pedantic mode: Required attribute {} has type {}\n".format(
                                k, str(type(md[k]))
                            )
                        )
                        message += (
                            "Schema requires this attribute have type={}\n".format(
                                str(metadata_schema.type(k))
                            )
                        )
                        message += "Type mismatches are not allowed in pedantic mode - datum linked to this document will be killed"
                        elog.log_error("doc2md", message, ErrorSeverity.Invalid)
            if len(fatal_keys) > 0:
                aok = False
        else:
            message = "Unrecognized value for mode=" + str(mode)
            message += " Must be one of promiscuous, cautious, or pedantic"
            raise ValueError(message)

    return [md, aok, elog]


def doclist2mdlist(
    doclist,
    database_schema,
    metadata_schema,
    wfcol,
    exclude_keys,
    mode="promiscuous",
):
    """
    Create a cleaned array of Metadata containers that can be used to
    construct a TimeSeriesEnsemble or SeismogramEnsemble.

    This function is like doc2md but for an input given as a list of docs (python dict).
    The main difference is the return tuple is very different.
    The function has to robustly handle the fact that sometimes converting
    a document to md is problematic.  The issues are defined in the
    related `doc2md` function that is used here for the atomic operation of
    converting a given document to a Metadata object.   The issue we have to
    face is what to do with warning message and documents that have
    fatal flaws (marked dead when passed through doc2md).  Warning
    messages are passed to the ErrorLogger component of the returned tuple.
    Callers should either print those messages or post them to the
    ensemble metadata that is expected to be constructed after calling
    this function.   In "cautious" and "pedantic" mode doc2md may mark
    a datum as bad with a kill return.  When a document is "killed"
    by doc2md it is dropped and two thi

    :param doclist:  list of documents to be converted to Metadata with schema
      constraints
    :type doclist:  any iterable container holding an array of dict containers
      with rational content (i.e. expected to be a MongoDB document with attributes
      defined for a set of seismic data objects.)

    ---other here --

    :return:  array with three components:
        0 - filtered array of Metadata containers
        1 - live boolean.   Set False only if conversion of all the documents
            in doclist failed.
        2 - ErrorLogger where warning and kill messages are posted (see above)
        3 - an array of documents that could not be converted (i.e. marked
            bad when processed with doc2md.)

    """
    mdlist = []
    ensemble_elog = ErrorLogger()
    bodies = []
    for doc in doclist:
        md, live, elog = doc2md(
            doc, database_schema, metadata_schema, wfcol, exclude_keys, mode
        )
        if live:
            mdlist.append(md)
        else:
            bodies.append(doc)
        if elog.size() > 0:
            ensemble_elog += elog

    if len(mdlist) == 0:
        live = False
    else:
        live = True
    return [mdlist, live, ensemble_elog, bodies]


def parse_normlist(input_nlist, db) -> list:
    """
    Parses a list of multiple accepted types to return a list of Matchers.

    This function is more or less a translator to create a list of
    subclasses of the BasicMatcher class used for generic normalization.
    The input list (input_nlist) can be one of two things.  If the
    list is a set of strings the strings are assumed to define
    collection names.  It then constructs a database-driven
    matcher class using the ObjectId method that is the stock
    MongoDB indexing method.   Specifically, for each collection
    name it creates an instance of the generic ObjectIdDBMatcher
    class pointing to the named collection.   The other allowed
    type for the members of the input list are children of the
    base class called `BasicMatcher` defined in spasspy.db.normalize.
    `BasicMatcher` abstracts the task required for normalization
    and provides a generic mechanism to load normalization data
    including data defined outside of MongoDB (e.g. a pandas DataFrame).
    If the list contains anything but a string or a child of
    BasicMatcher the function will abort throwing a TypeError
    exception.  On success it returns a list of children of
    `BasicMatcher` that can be used to normalize any wf
    document retried from MongoDB assuming the matching
    algorithm is valid.
    """
    # This import has to appear here to avoid a circular import
    from mspasspy.db.normalize import BasicMatcher, ObjectIdDBMatcher

    normalizer_list = []
    # Need this for backward compatibility for site, channel, and source
    # These lists are more restricitive than original algorithm but
    # will hopefully not cause a serious issue.
    # We use a dict keyed by collection name with lists passed to
    # matcher constructor
    atl_map = dict()
    lid_map = dict()
    klist = ["lat", "lon", "elev", "hang", "vang", "_id"]
    atl_map["channel"] = klist
    klist = ["net", "sta", "chan", "loc", "starttime", "endtime"]
    lid_map["channel"] = klist

    klist = ["lat", "lon", "elev", "_id"]
    atl_map["site"] = klist
    klist = ["net", "sta", "loc", "starttime", "endtime"]
    lid_map["site"] = klist

    klist = ["lat", "lon", "depth", "time", "_id"]
    atl_map["source"] = klist
    klist = ["magnitude", "mb", "ms", "mw"]
    lid_map["source"] = klist
    # could dogmatically insist input_nlist is a list but all we
    # need to require is it be iterable.

    for n in input_nlist:
        if isinstance(n, str):
            # Assume this is a collection name and use the id normalizer with db
            if n == "channel":
                attributes_to_load = atl_map["channel"]
                load_if_defined = lid_map["channel"]
            elif n == "site":
                attributes_to_load = atl_map["site"]
                load_if_defined = lid_map["site"]
            elif n == "source":
                attributes_to_load = atl_map["source"]
                load_if_defined = lid_map["source"]
            else:
                message = (
                    "Do not have a method to handle normalize with collection name=" + n
                )
                raise MsPASSError("Database.read_data", message, ErrorSeverity.Invalid)
            # intensionally let this constructor throw an exception if
            # it fails - python should unwinde such errors if the happen
            this_normalizer = ObjectIdDBMatcher(
                db,
                collection=n,
                attributes_to_load=attributes_to_load,
                load_if_defined=load_if_defined,
            )
        elif isinstance(n, dict):
            col = n["collection"]
            attributes_to_load = n["attributes_to_load"]
            load_if_defined = n["load_if_defined"]
            this_normalizer = ObjectIdDBMatcher(
                db,
                collection=col,
                attributes_to_load=attributes_to_load,
                load_if_defined=load_if_defined,
            )
        elif isinstance(n, BasicMatcher):
            this_normalizer = n
        else:
            message = "parse_normlist: unsupported type for entry in normalize argument list\n"
            message += "Found item in list of type={}\n".format(str(type(n)))
            message += "Item must be a string, dict, or subclass of BasicMatcher"
            raise TypeError(message)

        normalizer_list.append(this_normalizer)

    return normalizer_list


def _erase_normalized(
    md, normalizing_collections=["channel", "site", "source"]
) -> Metadata:
    """
    Erases data from a Metadata container assumed to come from normalization.

    In MsPASS attributes loaded with data from normalizing collections
    are always of the form collection_key where collection is the name of the
    normalizing collection and key is a simpler named used to store that
    attribute in the normalizing collection.   e.g. the "lat" latitude of
    an entry in the "site" collection would be posted to a waveform
    Metadata as "site_lat".  One does not normally want to copy such
    attributes back to the database when saving as it defeats the purpose of
    normalization and can create confusions about which copy is definitive.
    For that reason Database such attributes are erased before saving
    unless overridden.   This simple function standardizes that process.

    This function is mainly for internal use and has no safties.

    :param md:  input Metadata container to use.   Note this can be a
      MsPASS seismic data object that inherits metadata and the function
      will work as it copies the content to a Metadata container.
    :type md:  :class:`mspasspy.ccore.utility.Metadata` or a C++ data
      object that inherits Metadata (that means all MsPASS seismid data objects)
    :param normalizing_collection:  list of standard collection names that
      are defined for normalization.   These are an argument only to put them
      in a standard place.  They should not be changed unless a new
      normalizing collection name is added or a different schema is used
      that has different names.
    """
    # make a copy - not essential but small cost for stability
    # make a copy - not essential but small cost for stability
    mdout = Metadata(md)
    for k in mdout.keys():
        split_list = k.split("_")
        if len(split_list) >= 2:  # gt allows things like channel_foo_bar
            if split_list[1] != "id":  # never erase any of the form word_id
                if split_list[0] in normalizing_collections:
                    mdout.erase(k)
    return mdout


def geoJSON_doc(lat, lon, doc=None, key="epicenter") -> dict:
    """
    Convenience function to create a geoJSON format point object document
    from a points specified by latitude and longitude. The format
    for a geoJSON point isn't that strange but how to structure it into
    a mongoDB document for use with geospatial queries is not as
    clear from current MongoDB documentation.  This function makes that
    proess easier.

    The required inpput is latitude (lat) and longitude (lon).  The
    values are assumed to be in degrees for compatibility with MongoDB.
    That means latitude must be -90<=lat<=90 and longitude
    must satisfy -180<=lat<=180.  The function will try to handle the
    common situation with 0<=lon<=360 by wrapping 90->180 values to
    -180->0,  A ValueError exception is thrown if
    for any other situation with lot or lon outside those bounds.

    If you specify the optional "doc" argument it is assumed to be
    a python dict to which the geoJSON point data is to be added.
    By default a new dict is created that will contain only the
    geoJSON point data.  The doc options is useful if you want to
    add the geoJSON data to the document before appending it.
    The default is more useful for updates to add geospatial
    query capabilities to a collection with lat-lon data that is
    not properl structure.  In all cases the geoJSON data is a
    itself a python dict but a value associated accessible from
    the output dict with te key defined by the "key" argument
    (default is 'epicenter', which is appropriate for earthquake
    source data.)
    with a
    """
    outval = dict()
    outval["type"] = "Point"
    lon = float(lon)  # make this it is a float for database consistency
    if lon > 180.0 and lon <= 360.0:
        # we do this correction silently
        lon -= 360.0
    lat = float(lat)
    if lat < -90.0 or lat > 90.0 or lon < -180.0 or lon > 180.0:
        message = "geoJSON_doc:  Illegal geographic input\n"
        message += "latitude received={}  MongoDB requires [-90,90] range\n".format(lat)
        message += "longitude received={}  MongoDB requires [-180,180] range".format(
            lon
        )
        raise ValueError(message)

    outval["coordinates"] = [float(lon), float(lat)]
    if doc:
        retdoc = doc
        retdoc[key] = outval
    else:
        retdoc = {key: outval}
    return retdoc
