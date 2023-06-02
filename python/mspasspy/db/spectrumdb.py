#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from mspasspy.ccore.seismic import PowerSpectrum
from mspasspy.ccore.utility import MsPASSError,ErrorSeverity
from mspasspy.db.schema import DatabaseSchema,MetadataSchema
from mspasspy.db.dbclient import DBClient
from bson import ObjectId
import pickle

class BasicObjectDatabase(ABC):
    """
    Abstract base class for database handle to save a single object type.

    This is a base class for an alternative interface to the MsPASS
    database handle used as a core component of the framework.
    The main use of this alternative interface is assumed to be
    as a way to cleanly add additional data objects to the core object
    of MsPASS.   For example, this file contains a concrete implementation
    of this class to save power spectrum estimates in a particular
    MongoDB collection.  Users should use that as an example for how
    to utilize this approach for saving other types of data that
    MsPASS standard objects (TimeSeries, Seismogram, TimeSeriesEnsemble,
    and SeismogramEnsemble).

    A key point of this class it to create a simple, flexible interface
    to build a MongoDB collection that would store one and only one type
    of data object.   An implementation may choose to implement a schema
    if they wish using the schema mechanism of MsPASS.   Research uses
    for different objects that are specialized to one user or group
    may find this class a useful base to build upon.

    """
    def __init__(self,
                 name,
                 type_list,
                 *args,
                 db_schema=None,
                 md_schema=None,
                 **kwargs,
                 ):
        """
        Base class constructor.   Most subclasses should normally
        calls this constructor as part of the __init__ method of the
        subclass.

        This constructor creates an instance of the MsPASS Database
        class internally (self.db).  Subclasses should define what collections(s)
        this handle should reference. A schema can optionally be loaded through
        the two schema arguments but is not required.   That is, the decision
        on whether or not to enforce a schema definition with MongoDB is
        an implementation detail for the type(s) that handle suports.
        Note the required `type_list` arg is a list of python types
        a concrete implementation will support.   The model is used it that
        the a subclass implementation could call this base class constructor
        with the list of types it supports.   An appropriate way to think
        of `type_list` is a virtual attribute.

        :param name:   all database systems use the concept of a string that
        defines a particular instance of a database stored in that system.
        Subclasses should call this constructor to set that name.
        :type name:  string
        :parm type_list:  Because this class is aimed at supporting
        management of one or more data types we neee a clean way to define
        what those types are.  This argument serves that purpose.  It should
        contain a list of python types that can be tested with isintance
        to verify data being handled are of the right type.  (see above)
        :type type_list:  list of python types that will work in a loop of
        isinstance tests.
        :param db_schema:   A concrete instance of this class may want to
        impose a specific schema on the attributes to be stored in the
        database.  This defines the schema to use for attributes stored
        externally in the database.  It sets the class variable
        self.database_schema.  Note a schema is considered optional and
        can be turned off by setting this argument to the magic string
        "DO_NOT_LOAD".
        :type db_schema:  Can be one of three type:
            1.  mspasspy.db.schema.DatabaseSchema - in this case the class
                content is copied to self.db_schema.
            2.  A string defining a specific schema by a keyword name.
                That name is assume to match a yaml file name in the
                mspass data/yaml directory.  e.g. the default mspass
                schema is "mspass" defined with the file data/yaml/mspass.yaml.
                A special case is the magic name "DO_NOT_LOAD".  If
                that name appears no schema is loaded and self.db_schema is
                set to None.
            3.  None - this is the default and used to load the default
                schema file ("mspass")/
        :param md_schema:  A concrete instance of this class may want to
        impose a schema on attributes loaded as Metadata to a MsPASS
        data object.   Most useful for type enforcement.   It defines
        the contents of the class attribute self.metadata_schema.
        Note a schema is considered optional and
        can be turned off by setting this argument to the magic string
        "DO_NOT_LOAD".
        type md_schema:  Can be one of three type:
            1.  mspasspy.db.schema.DatabaseSchema - in this case the class
                content is copied to self.db_schema.
            2.  A string defining a specific schema by a keyword name.
                That name is assume to match a yaml file name in the
                mspass data/yaml directory.  e.g. the default mspass
                schema is "mspass" defined with the file data/yaml/mspass.yaml.
                A special case is the magic name "DO_NOT_LOAD".  If
                that name appears no schema is loaded and self.db_schema is
                set to None.
            3.  None - this is the default and used to load the default
                schema file ("mspass")/

        """
        do_not_load_keyword="DO_NOT_LOAD"
        self.name = name
        dbclient = DBClient()
        self.db = dbclient.get_database(self.name)
        self.type_list = type_list
        if isinstance(db_schema, DatabaseSchema):
            self.database_schema = db_schema
        elif isinstance(db_schema, str):
            if db_schema == do_not_load_keyword:
                self.database_schema = None
            else:
                self.database_schema = DatabaseSchema(db_schema)
        else:
            self.database_schema = DatabaseSchema()

        if isinstance(md_schema, MetadataSchema):
            self.metadata_schema = md_schema
        elif isinstance(md_schema, str):
            if md_schema == do_not_load_keyword:
                self.metadata_schema = None
            else:
                self.metadata_schema = MetadataSchema(md_schema)
        else:
            self.metadata_schema = MetadataSchema()



    def data_valid(self,d)->bool:
        """
        Tests if input datum d has a type supported by this handle.
        Returns a True if the answer is yes and false if the answer is no.
        Callers should handle the condition of false that would almost alway
        be an error.
        """
        for typ in self.type_list:
            if isinstance(d,typ):
                return True
        return False
    @abstractmethod
    def read_data(self):
        """
        Read one datum.

        Concrete implementation must implement this method.  It would
        normally contain some identifier in the arg list to select one an d
        only one entry from the database.   It would then return run an
        algorithm to construct and return the atomic data with wich this
        class is associated.
        """
        pass
    @abstractmethod
    def save_data(self,d):
        """
        Save one datum.

        Concrete implementations must implement this method.  It will save
        datum d by whatever scheme is used for the implementation.
        """
        pass
    @abstractmethod
    def verify(self):
        """
        Verify the validity of the data with which this handle is associated.

        Any real database needs a way to verify the contents are "clean" as
        defined by the needs of the system.   This method should implement
        whatever algorithm is appropriate to verify the contents are valid
        in the sense that read_data operations will not fail or some more
        elaborate requirement is satisified.   Most implementations will
        want to use a list of verify tests that implement different
        algorithms that define what "clean" means.

        Concrete implementations must implement this method.
        """
        pass

class SpectrumDatabase(BasicObjectDatabase):
    """
    Specialized database handle to manage PowerSpectrum data.

    Use this handle to read and write PowerSpectrum objects to a
    MongoDB database.  This interface is much simpler than the
    standard MsPASS database.  The class has only a basic reader and writer
    and a simple verify method required by the abstract base class from
    which it is derived.
    """
    def __init__(self,
                 name,
                 *args,
                 collection="PowerSpectrum",
                 **kwargs,
                 ):
        """
        Constructor for this database handle.   Note because this class
        inherits pymongo.database.Database.   You can pass arguments
        to this constructor recognized by the base class constructor
        for pymongo's handle like  "read_concern" or "write_concern" and
        they will be passed to the pymongo constructor by the standard
        python mechanism of *args and **kwargs.

        :param name:  MongoDB database name to use.
        :type name:  string
        :param collection:  optional alternative collection name to
        use for reads and writes (default is "PowerSpectrum")
        :type collection:  string
        """
        type_list = [PowerSpectrum]
        BasicObjectDatabase.__init__(name,
                               type_list,
                               db_schema="DO_NOT_LOAD",
                               md_schema="DO_NOT_LOAD")
        self.collection = self[collection]

    def save_data(self,datum,exclude=None,metadata2save=None,format="pickle"):
        """
        Saves a single PowerSpectrum object defined through arg0.   Default
        dumps all metadata elements to PowerSpectrum collection document
        and saves pickled version of datum with the key "serialized_data".

        :param datum:  PowerSpectrum to save.  The method will throw a
        MsPASSError if this is not a PowerSpectrum object.   If the datum
        is marked dead it will be silently skipped.
        :param exclude:  list of Metadata keys to not save to the saved
        document.  Default is None which means all attributes will be saved.
        :param metadata2save: list Metadata keys to be saved.  If not None
        (default) only the data fetched with these keys will be saved to
        the document created for this object.   If the key is not actually
        found in the Metadata area of datum it will be silently ignored.
        :param format:  output format of the object.  Currently the only
        accepted value is the default of "pickle".  The default format
        pickles the input datum and saves the result with the key "serialized_data".
        """
        if format!="pickle":
            raise MsPASSError("SpectrumDatabase.save_data:  format can currently only be pickle",ErrorSeverity.Fatal)
        if self.data_valid():
            if datum.dead():
                return datum
            if metadata2save:
                doc=dict()
                for key in metadata2save:
                    # if running this mode silently ignore any
                    # metadata defined in the keep list but not defined
                    # in the datum.   Questionable behavior
                    if datum.is_defined(key):
                        val = datum[key]
                        doc[key] = val
            else:
                doc=dict(datum)
                if exclude:
                    for key in exclude:
                        doc.pop(key)
            doc["serialized_data"] = pickle.dumps(datum)
            recid = self.collection.insert_one(doc).inserted_id
            return recid

        else:
            message = "SpectrumDatabase.save_data:  illegal data type for arg0. Found type={typ} - only support PowerSpectrum".format(typ=type(datum))
            raise MsPASSError(message,ErrorSeverity.Fatal)

    def read_data(self,
                  id_or_doc,
                  required=None,
                  override=None,
                  ):
        """
        Reads one PowerSpectrum using an object id either directly or
        indirectly via an input MongoDB document.   Because this implementation
        uses pickle to restore the PowerSpectrum object from a serialized
        form it may be useful to verify the content of the restored datum
        created by pickle has attributes that are the same as the database.
        The can be necessary if the database was edited after a datum of
        interest was saved.  The "required" and "override" optional arguments
        are used for that purpose.

        :param id_or_doc:  as the name implies this required argument must
        be either a MongoDB ObjectId class or something that at least acts
        like a MongoDB document. An id is used directly to generate a query.
        If the argument is not an ObjectId the method attempts to fetch
        an attribute with the stock key "_id".   If that key is not found
        this method will throw an exception.

        :param required:  list of keys (strings) that will always be fetched
        from the document retrieved in the query.  The content retrieved
        will always override any value that might have been stored with the
        serialized version of the object.  Use this feature to fix attributes
        repaired or added after a datum was saved.

        :param overide:  list of keys (strings) that should be fetched
        from the document retrieved in the query and pushed to the
        constructed object.   Use this feature to fix attributes repaired
        or added after a datum was saved.  It differs from "required" as
        the attribute is treated as optional.  i.e. it a value isn't found
        for a particular key it is simply not posted.
        """
        if isinstance(id_or_doc,ObjectId):
            oid=id_or_doc
        else:
            oid = id_or_doc["_id"]
        doc = self.collection.find_one({'_id' : oid})
        if doc:
            datakey="serialized_data"
            if datakey in doc:
                datum = pickle.loads(doc[datakey])
                if required:
                    for key in required:
                        datum[key] = doc[key]
                if override:
                    for key in override:
                        if key in doc:
                            datum[key] = doc[key]
            else:
                message = "SpectrumDatabase.read_data:  missing required key=serialized_data - expected to contain datum serialized with pickle"
                raise MsPASSError(message,ErrorSeverity.Fatal)
            return datum
        else:
            message="SpectrumDatabase.read_data: no document with ObjectId={oid} was found in PowerSpectrum collection".format(oid=str(oid))
            raise MsPASSError(message,ErrorSeverity.Invalid)

    def verify(self,query=None,required=None):
        """
        Scans PowerSpectrum collection to verify the contents.  An optional
        MongoDB query can be passed to scan a limited subset.  You can also
        pass a list of required keys that must be present in each document
        processed.  The method always tests for the existence of the special
        key "serialized_data" that is used to store a pickled copy of the
        datum.

        :param query:  optional pymongo query (python dict) to apply the
        Power Spectrum collection.  Default scans the entire collection.

        :param required:  optional list of keys every document scan is
        expected to contain.

        :return:  tuple with two integers.  Component 0 will contain the
        number of documents scanned and component 1 will contain the number
        the method considers valid.
        """
        if query:
            cursor = self.collection.find(query)
        else:
            cursor=self.collection.find[{}]
        nprocessed=0
        nvalid=0
        testkey = "serialized_data"
        for doc in cursor:
            if testkey in doc:
                aok = True
                if required:
                    for k in required:
                        if k not in doc:
                            aok = False
                            break

            else:
                aok = False
        if aok:
            nvalid += 1
        nprocessed += 1

        return([nprocessed,nvalid])
