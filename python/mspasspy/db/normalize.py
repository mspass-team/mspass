from abc import ABC, abstractmethod

from mspasspy.ccore.utility import MsPASSError, ErrorSeverity, Metadata
from mspasspy.util.error_logger import PyErrorLogger
from mspasspy.ccore.seismic import (
    TimeSeries,
    Seismogram,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
)
from mspasspy.db.database import Database
from mspasspy.util.decorators import mspass_func_wrapper

from bson import ObjectId

from obspy import UTCDateTime
import pymongo
import copy
import pandas as pd
import dask
import numpy as np

type_pdd = pd.core.frame.DataFrame
type_ddd = dask.dataframe.DataFrame


class BasicMatcher(ABC):
    """
    This base class defines the api for a generic matching capability for
    MongoDB normalization.   The base class is a mostly a skeleton that
    defines on required abstract methods and initializes a set of
    universal attributes all matchers need.  It cannot be instatiated directly.

    Matching is defined as one of two things:  (1)  a one-to-one
    match algorithm is guaranteed to have each search yield either
    exactly one match or none.  That is defined through a find_one
    method following the same concept in MongoDB.  (2) some
    matches are not unique and yield more than one document.
    For that case use the find method.  Unlike the MongoDB
    find method, however, find in this context returns a list of
    Metadata containers holding the set of attributes requested
    in lists defined on constuction.

    Another way of viewing this interface, in fact, is an abstraction of
    the find and find_one methods of MongoDB to a wider class of
    algorithms that may or may not utilize MongoDB directly.
    In particular, intermediate level classes defined below that implement
    different cache data structures allow input either by
    loading data from a MongoDB collection of from a pandas DataFrame.
    That can potentially provide a wide variety of applications of
    matching data to tabular data contained in files loaded into
    pandas by any of long list of standard dataframe read methods.
    Examples are any SQL database or antelope raw tables or views
    loaded as text files.

    """

    def __init__(
        self,
        attributes_to_load=None,
        load_if_defined=None,
        aliases=None,
    ):
        """
        Base class constructor sets only attributes considered necessary for all
        subclasses.   Most if not all subclasses will want to call this
        constructor driven by an arg list passed to the subclass constructor.

        :param attributes_to_load:  is a list of keys (strings) that are to
          be loaded from the normalizing table/collection.   Default for
          this base class is None, but subclasses should normally define
          a default list.  It is important to realize that all subclasses
          should normally treat this list as a set of required attributes.
          Optional values should appear in the load_if_defined list.

        :param load_if_defined:   is a secondary list of keys (strings) that
          should be loaded only if they are defined. Default is None here,
          subclass should set their own default values.

        :param aliases:   should be a python dictionary used to define
          alternative keys to access a data object's Metadata from that
          defining the same attribute in the collection/table being
          matched.   Note carefully the key of the dictionary is the
          collection/table attribute name and the value associated with
          that key is the alias to use to fetch Metadata.  When matchers
          scan the attributes_to_load and load_if_defined list they
          should treat missing entries in alias as meaning the key in
          the collection/table and Metadata are identical.  Default is
          a None which is used as a signal to this constructor to
          create an empty dictionary meaning there are no aliases.
        :type aliases:  python dictionary


        """
        if attributes_to_load is None:
            self.attributes_to_load = []
        else:
            self.attributes_to_load = attributes_to_load
        if load_if_defined is None:
            self.load_if_defined = []
        else:
            # make sure self.load_if_defined and self.attributes_to_load do
            # not contain the same keys.   self.attributes_to_load will supercede
            # a duplicate in self.load_if_defined.
            self.load_if_defined = []
            for item in load_if_defined:
                if item not in self.attributes_to_load:
                    self.load_if_defined.append(item)

        if aliases is None:
            self.aliases = dict()
        elif isinstance(aliases, dict):
            self.aliases = aliases
        else:
            raise TypeError(
                "BasicMatcher constructor: aliases argument must be either None or define a python dictionary"
            )

    def __call__(self, d, *args, **kwargs):
        """
        Convenience method that allows the shorthand of the find_one method
        using the standard python meaning of this symbol.  e.g. if
        we have an concrete instance of a subclass of this base class
        for matching against ObjectIds using the implementation below
        called IDMatcher we assign to the symbol "matcher" we can
        get a Metadata container of the matching content for a MsPASS
        data object with symbol d using md = matcher(d) versus
        md = matcher.find_one(d).  All subclasses have this interface
        define because it is a (nonvirtual) base class method.
        """
        return self.find_one(d, *args, **kwargs)

    @abstractmethod
    def find(self, mspass_object, *args, **kwargs) -> tuple:
        """
        Abstraction of the MongoDB database find method with the
        matching criteria defined when a concrete instance is instantiated.
        Like the MongoDB method implementations it should return a list
        of containers matching the keys found in the data passed through
        the mspass_object.  A key difference from MongoDB, however, is
        that instead of a MongoDB cursor we return a python list of Metadata
        containers.   In some instances that is a
        direct translation of a MongoDB cursor to a list of Metadata
        objects.  The abstraction is useful to allow small collections
        to be accessed faster with a generic cache algorithm (see below)
        and loading of tables of data through a file-based subclass
        of this base.  All can be treated through a common interface.
        WE STRESS STRONGLY that the abstraction assumes returns are
        always small enough to not cause a memory bloating problem.
        If you need the big-memory model of a cursor use it directly.

        All subclasses must implement this virtual
        method to be concrete or they cannot be instantiated.
        If the matching algorithm implemented is always expected to
        be a unique one-to-one match applications may want to have this
        method throw an exception as a use error.  That case should
        use the find_one interface defined below.

        All implementations should return a pair (2 component tuple).
        0 is expected to hold a list of Metadata containers and
        1 is expected to contain either a None type or an PyErrorLogger
        object.   The PyErrorLogger is a convenient way to pass error
        messages back to the caller in a manner that is easier to handle
        with the MsPASS error system than an exception mechanism.
        Callers should handle four cases that are possible for a return
        (Noting [] means an empty list and [...] a list with data)

           1. []  None  - notmatch found
           2. [] ErrorLog - failure with an informational message in the
             ErrorLog that should be preserved.  The presence of an error
             should imply something went wrong and it was simply a null result.
           3. [...] None - all is good with no detected errors
           4. [...] ErrorLog - valid data returned but there is a warning
              or informational message posted.  In this case handlers
              may want to examine the ErrorSeverity components of the log
              and handle different levels differently  (e.g. Fatal and
              Informational should always be treated differently)

        """

    @abstractmethod
    def find_one(self, mspass_object, *args, **kwargs) -> tuple:
        """
        Abstraction of the MongoDB database find_one method with the
        matching criteria defined when a concrete instance is instantiated.
        Like the MongoDB method implementations should return the
        unique document that the keys in mspass_object are expected
        to define with the matching criteria defined by the instance.
        A type example of an always unique match is ObjectIds.
        When a match is found the result should be returned in a
        Metadata container.  The attributes returned are normally
        a subset of the document and are defined by the base class
        attributes "attributes_to_load" and "load_if_defined".
        For database instances this is little more than copying
        desired attributes from the matching document returned by
        MongoDB, but for abstraction more may be involved.
        e.g., implemented below is a generic cached algorithm that
        stores a collection to be matched in memory for efficiency.
        That implementation allows the "collection" to be loaded from
        MongoDB or a pandas DataFrame.

        All implementations should return a pair (2 component tuple).
        0 is expected to hold a Metadata containers that was yielded by
        the match.  It should be returned as None if there is no match.
        1 is expected to contain either a None type or an PyErrorLogger
        object.   The PyErrorLogger is a convenient way to pass error
        messages back to the caller in a manner that is easier to handle
        with the MsPASS error system than an exception mechanism.
        Callers should handle four cases that are possible for a return:

           1, None  None  - no match found
           2. None ErrorLog - failure with an informational message in the
             ErrorLog that the caller may want be preserved or convert to
             an exception.
           3. Metadata None - all is good with no detected errors
           4. Metadata ErrorLog - valid data was returned but there is a warning
              or informational message posted.  In this case handlers
              may want to examine the ErrorSeverity components of the log
              and handle different levels differently  (e.g. Fatal and
              Informational should always be treated differently)

        """
        pass

    def find_doc(self, doc) -> Metadata:
        """
        find a unique match using a python dictionary as input.

        The bulk_normalize function  requires an implementation of a
        method with this name. It is conceptually similar to find_one
        but it uses a python dictionary (the doc argument) as input
        instead of a mspass seismic data object.  It also returns only
        a Metadata container on success or None if it fails to find
        a match.

        This method is little more than a thin wrapper around
        an implementation of the find_one method. It checkes
        the elog for entries marked Invalid and if
        so returns None.  Otherwise it converts the Metdata container
        to a python dictionary it return.  It is part of the
        base class because it depends only on the near equivalence of
        a python dictionary and the MsPASS Metadata containers.

        When find_one returns a ErrorLogger object the contents are
        inspected.   Errors less severe than "Invalid" are ignored
        and dropped.   If the log contains a message tagged "Invalid"
        this function will silently return None.   That could be
        problematic as it is indistinguishable from the return when
        there is no match, but is useful to simply the api.  If an
        entry is tagged "Fatal" a MsPASSError exception will be
        thrown with the message posted to the MsPASSError container.

        Subclasses may wish to override this method if the approach
        used here is inappropriate.   i.e. if this were C++ this
        method would be declared virtual.
        """
        md2test = Metadata(doc)
        [md, elog] = self.find_one(md2test)
        if elog:
            elist = elog.worst_errors()
            # Return no success if Invalid
            worst = elist[0].badness
            if worst == ErrorSeverity.Invalid:
                return None
            elif worst == ErrorSeverity.Fatal:
                message = "find_doc method failed.   Messages posted:\n"
                for e in elist:
                    message += e.message + "\n"
                raise MsPASSError(message, ErrorSeverity.Fatal)

        return dict(md)


class DatabaseMatcher(BasicMatcher):
    """
    Matcher using direct database queries to MongoDB.   Each call to
    the find method of this class constructs a query, calls the MongoDB
    database find method with that query, and extracts desired attributes
    from the return in the form of a Metadata container.  The query
    construction is abstracted by a virtual method called query_generator.
    This is an intermediate class that cannot be instantiated directly
    because it contains a virtual method.   User's should consult
    docstrings for constructors for subclasses of this intermediate class.
    """

    def __init__(
        self,
        db,
        collection,
        attributes_to_load=None,
        load_if_defined=None,
        aliases=None,
        require_unique_match=False,
        prepend_collection_name=False,
    ):
        """
        Constructor for this intermediate class.  It should not be
        used except by subclasses as this intermediate class
        is not concrete.
        """
        super().__init__(
            attributes_to_load=attributes_to_load,
            load_if_defined=load_if_defined,
            aliases=aliases,
        )
        if not isinstance(collection, str):
            raise TypeError(
                "{} constructor:  required arg1 must be a collection name - received invalid type".format(
                    self.__class__.__name__
                )
            )
        self.collection = collection
        if isinstance(db, pymongo.database.Database):
            self.dbhandle = db[collection]
        else:
            message = "DatabaseMatcher constructor:  db argument is not a valid Database handle\n"
            message += "Actual type of db arg={}".format(str(type(db)))
            raise TypeError(message)
        self.require_unique_match = require_unique_match
        self.prepend_collection_name = prepend_collection_name

    @abstractmethod
    def query_generator(self, mspass_object) -> dict:
        """
        Subclasses of this intermediate class MUST implement this
        method.  It should extract content from mspass_object and
        use that content to generate a MongoDB query that is
        passed directly to the find method of the MongoDB database
        handle stored within this object (self) during
        the class construction.  Since pymongo uses a
        python dict for that purpose it must return a valid
        query dict.  Implementations should return None if no
        query could be generated.  Common, for example, if a key
        required to generate the query is missing from mspass_object.
        """
        pass

    def find(self, mspass_object):
        """
        Generic database implementation of the find method for this
        abstraction.   It returns what the base class api specifies.
        That is, normally it returns a tuple with component 0 being
        a python list of Metadata containers.  Each container holds
        the subset of attributes defined by attributes_to_load and
        (if present) load_if_defined.  The list is the set of all
        documents matching the query, which at this level of the class
        structure is abstract.

        The method dogmatically requires data for all keys
        defined by attributes_to_load.  It will throw a MsPASSError
        exception with a Fatal tag if any of the required attributes
        are not defined in any of the documents.  The return matches
        the API specification for BasicMatcher.

        It also handles failures of the abstract query_generator
        through the mechanism the base class api specified:  a None
        return means the method could not create a valid query.
        Failures in the query will always post a message to elog
        tagging the result as "Invalid".

        It also handles the common problem of dead data or accidentally
        receiving invalid data like a None.   The later may cause other
        algorithms to abort, but we handle it here return [None,None].
        We don't return an PyErrorLogger in that situation as the assumption
        is there is no place to put it and something else has gone really
        wrong.
        """
        if not isinstance(mspass_object, Metadata):
            elog = PyErrorLogger()
            message = "received invalid data.  Arg0 must be a valid MsPASS data object"
            elog.log_error(message, ErrorSeverity.Invalid)
        if hasattr(mspass_object, "dead"):
            if mspass_object.dead():
                return [None, None]
        query = self.query_generator(mspass_object)
        if query is None:
            elog = PyErrorLogger()
            message = "query_generator method failed to generate a valid query - required attributes are probably missing"
            elog.log_error(message, ErrorSeverity.Invalid)
            return [None, elog]
        number_hits = self.dbhandle.count_documents(query)
        if number_hits <= 0:
            elog = PyErrorLogger()
            message = "query = " + str(query) + " yielded no documents"
            elog.log_error(message, ErrorSeverity.Complaint)
            return [None, elog]
        cursor = self.dbhandle.find(query)
        elog = PyErrorLogger()
        metadata_list = []
        for doc in cursor:
            try:
                md = _extractData2Metadata(
                    doc,
                    self.attributes_to_load,
                    self.aliases,
                    self.prepend_collection_name,
                    self.collection,
                    self.load_if_defined,
                )
                metadata_list.append(md)
            except MsPASSError as e:
                raise MsPASSError("DatabaseMatcher.find: " + e.message, e.severity)

        if elog.size() <= 0:
            return [metadata_list, None]
        else:
            return [metadata_list, elog]

    def find_one(self, mspass_object):
        """
        Generic database implementation of the find_one method.  The tacit
        assumption is that if you call find_one you are expecting a unique
        match to the algorithm implemented.  The actual behavior for a
        nonunique match is controlled by the class attribute
        require_unique_match.  Subclasses that want to dogmatically enforce
        uniqueness (appropriate for example with ObjectIds) should
        set require_unique_match True.   In that case if a match is not
        unique the method will throw an exception.  When False, which is
        the default, an informational message is posted and the method
        returns the first list element returned by find.  This method is
        actually little more than a wrapper around find to handle that
        uniqueness issue.
        """
        find_output = self.find(mspass_object)
        if find_output[0] is None:
            return [None, find_output[1]]
        mdlist_length = len(find_output[0])
        if mdlist_length == 1:
            return [find_output[0][0], find_output[1]]
        else:
            # somewhat messy logic to handle differnt situations
            # we throw an exception if the constructor set require_unique_match
            # True.  Otherwise we need to handle the distinction on whether or
            # not the return from find had an PyErrorLogger defined with data.
            if self.require_unique_match:
                message = "query of collection {col} did not yield a unique match.  Found {n} matching documents.  Aborting".format(
                    col=self.collection, n=mdlist_length
                )
                raise MsPASSError(
                    "DatabaseMatcher.find_one:  " + message, ErrorSeverity.Fatal
                )
            else:
                if find_output[1] is None:
                    elog = PyErrorLogger()
                else:
                    elog = find_output[1]
                message = "query of collection {col} did not yield a unique match.  Found {n} matching documents.  Using first one in list".format(
                    col=self.collection, n=mdlist_length
                )
                elog.log_error(
                    "DatabaseMatcher.find_one", message, ErrorSeverity.Complaint
                )
                return [find_output[0][0], elog]


class DictionaryCacheMatcher(BasicMatcher):
    """
    Matcher implementing a caching method based on a python dictionary.

    This is an intermediate class for instances where the database collection
    to be matched is small enough that the in-memory model is appropriate.
    It should also only be used if the matching algorithm can be reduced
    to a single string that can serve as a unique id for each tuple.

    The class defines a generic dictionary cache with a string key.   The way that
    key is define is abstracted through two virtual methods:
    (1) The cache_id method creates a match key from a mspass data object.
        That is normally from the Metadata container but it is not
        restricted to that. e.g. start time for TimeSeries or
        Seismogram objects can be obtained from the t0 attribute
        directly.
    (2) The db_make_cache_id is called by the internal method of this
        intermediate class (method name is _load_normalization_cache)
        to build the cache index from MongoDB documents scanned to
        construct the cache.

    Two different methods to define the cache index are necessary as a
    generic way to implement aliases.  A type example is the mspass use
    of names like "channel_id" to refer to the ObjectId of a specific
    document in the channel collection.   When loading channel the name
    key is "_id" but data objects would normally have that same data
    defined with the key "channel_id".   Similarly, if data have had
    aliases applied a key in the data may not match the name in a
    collection to be matched.   The dark side of this is it is very
    easy when running subclasses of this to get null results with
    all members of a dataset.   As always testing with a subset of
    data is strongly recommended before running versions of this on
    a large dataset.

    This class cannot be instantiated because it is not concrete
    (has abstract - virtual - methods that must be defined by subclasses)
    See implementations for constructor argument definitions.
    """

    def __init__(
        self,
        db_or_df,
        collection,
        query=None,
        attributes_to_load=None,
        aliases=None,
        load_if_defined=None,
        require_unique_match=False,
        prepend_collection_name=False,
        use_dataframe_index_as_cache_id=False,
    ):
        """
        Constructor for this intermediate class.  It should not be
        used except by subclasses as this intermediate class
        is not concrete.   It calls the base class constructor
        and then loads two internal attributes:  query and collection.
        It then creates the normalization python dict that applies the
        abstract cache_id method.  Note that only works for
        concrete subclasses of this intermediate class.
        """
        super().__init__(
            attributes_to_load=attributes_to_load,
            load_if_defined=load_if_defined,
            aliases=aliases,
        )
        if not isinstance(collection, str):
            raise TypeError(
                "{} constructor:  required arg1 must be a collection name - received invalid type".format(
                    self.__class__.__name__
                )
            )
        if query == None:
            self.query = dict()
        elif isinstance(query, dict):
            self.query = query
        else:
            raise TypeError(
                "{} constructor:  query argument must be a python dict container".format(
                    self.__class__.__name__
                )
            )
        self.collection = collection
        self.require_unique_match = require_unique_match
        self.prepend_collection_name = prepend_collection_name
        self.use_dataframe_index_as_cache_id = use_dataframe_index_as_cache_id

        # This is a redundant initialization but a minor cost for stability
        self.normcache = dict()
        # Reference the base class to avoid a type error
        # This seems to be an oddity from using the same name Database
        # in mspass as pymongo
        if isinstance(db_or_df, pymongo.database.Database):
            self._db_load_normalization_cache(db_or_df, collection)
        elif isinstance(db_or_df, (type_ddd, type_pdd)):
            self._df_load_normalization_cache(db_or_df, collection)
        else:
            message = "{} constructor:  required arg0 must be a Database handle or panda Dataframe\n".format(
                self.__class__.__name__
            )
            message += "Actual type = {}".format(str(type(db_or_df)))
            raise TypeError(message)

    @abstractmethod
    def cache_id(self, mspass_object) -> str:
        """
        Concrete implementations must implement this method to define
        how a mspass data object, mspass_object, is to be used to
        construct the key to the cache dict container.   It is
        distinct from db_make_cache_id to allow differences in naming
        or even the algorithm used to construct the key from a datum
        relative to the database.  This complicates the interface but
        makes it more generic.

        :param mspass_object:  is expected to be a MsPASS object.
          Any type restrictions should be implemented in subclasses
          that implement the method.

        :return: should always return a valid string and never throw
          an exception.  If the algorithm fails the implementation should
          return a None.
        """
        pass

    @abstractmethod
    def db_make_cache_id(self, doc) -> str:
        """
        Concrete implementation must implement this method to define how
        the cache index is to be created from database documents passed
        through the arg doc, which pymongo always returns as a python dict.
        It is distinct from cache_id to allow differences in naming or
        the algorithm for loading the cache compared to accessing it
        using attributes of a data object.   If the id string cannot
        be created from doc an implementation should return None.
        The generic loaders in this class, db_load_normalization_cache
        and df_load_normalization_class, handle that situation cleanly
        but if a subclass overrides the load methods they should handle
        such errors.  "cleanly" in this case means they throw an
        exception which is appropriate since they are run during construction
        and any invalid key is not acceptable in that situation.
        """
        pass

    def find_one(self, mspass_object) -> tuple:
        """
        Implementation of find for generic cached method.   It uses the cache_id
        method to create the indexing string from mspass_object and then returns
        a match to the cache stored in self.  Only subclasses of this
        intermediate class can work because the cache_id method is
        defined as a pure virtual method in this intermediate class.
        That construct is used to simplify writing additional matcher
        classes.  All extensions need to do is define the cache_id
        and db_make_cache_id algorithms to build that index.

        :param mspass_object:  Any valid MsPASS data object.  That means
          TimeSeries, Seismogram, TimeSeriesEnsemble, or SeismogramEnsemble.
          This datum is passed to the (abstract) cache_id method to
          create an index string and the result is used to fetch the
          Metadata container matching that key.   What is required of the
          input is dependent on the subclass implementation of cache_id.

        :return:  2-component tuple following API specification in BasicMatcher.
          Only two possible results are possible from this implementation:

           None ErrorLog - failure with an error message that can be passed on
             if desired or printed
           Metadata None - all is good with no detected errors.  The Metadata
             container holds all attributes_to_load and any defined
             load_if_defined values.

        """
        find_output = self.find(mspass_object)

        if find_output[0] is None:
            return find_output
        elif len(find_output[0]) == 1:
            return [find_output[0][0], find_output[1]]
        else:
            # as with the database version we use require_unique_match
            # to define if we should be dogmatic or not
            if self.require_unique_match:
                message = "query does not yield a unique match and require_unique_match is set true"
                raise MsPASSError(
                    "DictionaryCacheMatcher.find:  " + message, ErrorSeverity.Fatal
                )
            else:
                message = "encountered a nonunique match calling find_one - returning contents of first matching document found"
                if find_output[1] is None:
                    elog = PyErrorLogger()
                else:
                    elog = find_output[1]
                elog.log_error(
                    "DictionaryCacheMatcher.find:  ", message, ErrorSeverity.Complaint
                )
                return [find_output[0][0], elog]

    def find(self, mspass_object) -> tuple:
        """
        Generic implementation of find method for cached tables/collections.

        This method is a generalization of the MongoDB database find method.
        It differs in two ways. First, it creates the "query" directly from
        a MsPASS data object (pymongo find requires a dict as input).
        Second, the result is return as a python list of Metadata containers
        containing what is (usually) a subset of the data stored in the
        original collection (table).   In contrast pymongo database find
        returns a database "Cursor" object which is their implementation of
        a large list that may exceed the size of memory.  A key point is
        the model here makes sense only if the original table itself is small
        enough to not cause a memory problem.  Further, find calls that
        yield long list may cause efficiency problems as subclasses that
        build on this usually will need to do a linear search through the
        list if they need to find a particular instance (e.g. call to find_one).

        :param mspass_object: data object to match against
          data in cache (i.e. query).
        :type mspass_object:  must be a valid MsPASS data object.
          currently that means TimeSeries, Seismogram, TimeSeriesEnsemble,
          or SeismogramEnsemble.   If it is anything else (e.g. None)
          this method will return a tuple [None, elog] with elog being
          a PyErrorLogger with a posted message.

        :return: tuple with two elements.  0 is either a list of valid Metadata
          container(s) or None and 1 is either None or an PyErrorLogger object.
          There are only two possible returns from this method:
              [None, elog] - find failed.  See/save elog for why it failed.
              [ [md1, md2, ..., mdn], None] - success with 0 a list of Metadata
                containing attributes_to_load and load_if_defined
                (if appropriate) in each component.
        """
        if not isinstance(mspass_object, Metadata):
            elog = PyErrorLogger()
            elog.log_error(
                "Received datum that was not a valid MsPASS data object",
                ErrorSeverity.Invalid,
            )
            return [None, elog]
        if hasattr(mspass_object, "dead"):
            if mspass_object.dead():
                return [None, None]
        thisid = self.cache_id(mspass_object)
        # this should perhaps generate two different messages as the
        # they imply slightly different things - the current message
        # is accurate though
        if (thisid == None) or (thisid not in self.normcache):
            error_message = "cache_id method found no match for this datum"
            elog = PyErrorLogger()
            elog.log_error(error_message, ErrorSeverity.Invalid)
            return [None, elog]
        else:
            return [self.normcache[thisid], None]

    def _db_load_normalization_cache(self, db, collection):
        """
        This private method abstracts the process of loading a cached
        version of a normalizing collection.  It creates a python
        dict stored internally with the name self.normcache.  The
        container is keyed by a string created by
        the virtual method cache_id.  The value associated with each
        dict key is a python list of Metadata containers.
        Each component is constructed from any document matching the
        algorithm defined by cache_id.   The list is constructed by
        essentially appending a new Metadata object whenever a
        matching cache_id is returned.

        The Metadata containers normally contain only a subset of
        the original attributes in the collection.  The list
        attributes_to_load is treated as required and this method
        will throw a MsPASSError exception if any of them are missing
        from any document parsed.  Use load_if_define for attributes
        that are not required for your workflow.

        This method will throw a MsPASS fatal error in two situations:
            1.  If the collection following the (optional) query is empty
            2.  If any attribute in the self.attributes_to_load is not
                defined in any document loaded.  In all BasicMatcher
                subclasses attributes_to_load are considered required.

        :param db:  MsPASS Database class MongoDB database handle.  Note
          it can be the subclass of the base class MongooDB handle
          as extensions for MsPASS to the handle are not used in this method.
        :param collection:  string defining the MongoDB collection that is
          to be loaded and indexed - the normalizing collection target.

        """
        dbhandle = db[collection]
        self.collection_size = dbhandle.count_documents(self.query)
        if self.collection_size <= 0:
            message = "Query={} of collection {} yielded no documents\n".format(
                str(self.query), collection
            )
            message += "Cannot construct a zero length object"
            raise MsPASSError(
                "DictionaryCacheMatcher._load_normalization_cache:  " + message,
                ErrorSeverity.Fatal,
            )
        cursor = dbhandle.find(self.query)
        self.normcache = dict()
        count = 0
        for doc in cursor:
            cache_key = self.db_make_cache_id(doc)
            # This error trap may not be necessary but the api requires us
            # to handle a None return
            if cache_key == None:
                raise MsPASSError(
                    "DictionaryCacheMatcher._load_normalization_cache:  "
                    + "db_make_cache_id failed - coding problem or major problem with collection="
                    + collection,
                    ErrorSeverity.Fatal,
                )
            try:
                md = _extractData2Metadata(
                    doc,
                    self.attributes_to_load,
                    self.aliases,
                    self.prepend_collection_name,
                    collection,
                    self.load_if_defined,
                )
                if cache_key in self.normcache:
                    self.normcache[cache_key].append(md)
                else:
                    self.normcache[cache_key] = [md]
                count += 1
            except MsPASSError as e:
                raise MsPASSError(
                    e.message
                    + " in document number {n} of collection {col}".format(
                        n=count, col=collection
                    ),
                    e.severity,
                )

    def _df_load_normalization_cache(self, df, collection):
        """
        This function does the same thing as _db_load_normalization_cache, the
        only difference is that this current function takes one argument, which
        is a dataframe.

        :param df: a pandas/dask dataframe where we load data from
        """
        query_result = df
        if self.query is not None and len(self.query) > 0:
            #   Create a query
            #   There are multiple ways of querying in a dataframe, according to
            #   the experiments in https://stackoverflow.com/a/46165056/11138718
            #   We pick the following approach:
            sub_conds = [df[key].values == val for key, val in self.query.items()]
            cond = np.logical_and.reduce(sub_conds)
            query_result = df[cond]

        if len(query_result.index) <= 0:
            message = (
                "Query={query_str} of dataframe={dataframe_str}"
                " yielded 0 documents - cannot construct this object".format(
                    query_str=str(self.query), dataframe_str=str(df)
                )
            )
            raise MsPASSError(
                "DictionaryCacheMatcher._load_normalization_cache:  " + message,
                ErrorSeverity.Fatal,
            )
        self.normcache = dict()
        count = 0
        for index, doc in query_result.iterrows():
            cache_key = index
            if not self.use_dataframe_index_as_cache_id:
                cache_key = self.db_make_cache_id(doc)
            # This error trap may not be necessary but the api requires us
            # to handle a None return
            if cache_key == None:
                raise MsPASSError(
                    "DictionaryCacheMatcher._load_normalization_cache:  "
                    + "db_make_cache_id failed - coding problem or major problem with collection="
                    + collection,
                    ErrorSeverity.Fatal,
                )
            try:
                md = _extractData2Metadata(
                    doc,
                    self.attributes_to_load,
                    self.aliases,
                    self.prepend_collection_name,
                    collection,
                    self.load_if_defined,
                )
                if cache_key in self.normcache:
                    self.normcache[cache_key].append(md)
                else:
                    self.normcache[cache_key] = [md]
                count += 1
            except MsPASSError as e:
                raise MsPASSError(
                    e.message
                    + " in document number {n} of collection {col}".format(
                        n=count, col=collection
                    ),
                    e.severity,
                )


class DataFrameCacheMatcher(BasicMatcher):
    """
    Matcher implementing a caching method based on a Pandas DataFrame

    This is an intermediate class for instances where the database collection
    to be matched is small enough that the in-memory model is appropriate.
    It should be used when the matching algorithm is readily cast into the
    subsetting api of a pandas DataFrame.

    The constructor of this intermediate class first calls the BasicMatcher
    (base class) constructor to initialize some common attribute including
    the critical lists of attributes to be loaded.   This constructor then
    creates the internal DataFrame cache by one of two methods.
    If arg0 is a MongoDB database handle it loads the data in the
    named collection to a DataFrame created during construction.  If the
    input is a DataFrame already it is simply copied selecting only
    columns defined by the attributes_to_load and load_if_defined lists.
    There is also an optional parameter, custom_null_values, that is a
    python dictionary defining values in a field that should be treated
    as a definition of a Null for that field.  The constuctor converts
    such values to a standard pandas null field value.

    This class implements generic find and find_one methods.
    Subclasses of this class must implement a "subset" method to be
    concrete.  A subset method is the abstract algorithm that defines
    a match for that instance expressed as a pandas subset operation.
    (For most algorithms there are multiple ways to skin that cat or is
     it a panda?)  See concrete subclasses for examples.

    This class cannot be instantiated because it is not concrete
    (has abstract - virtual - methods that must be defined by subclasses)
    See implementations for constructor argument definitions.
    """

    def __init__(
        self,
        db_or_df,
        collection=None,
        attributes_to_load=None,
        load_if_defined=None,
        aliases=None,
        require_unique_match=False,
        prepend_collection_name=False,
        custom_null_values=None,
    ):
        """
        Constructor for this intermediate class.  It should not be
        used except by subclasses as this intermediate class
        is not concrete.
        """
        self.prepend_collection_name = prepend_collection_name
        self.require_unique_match = require_unique_match
        self.custom_null_values = custom_null_values
        # this is a necessary sanity check
        if collection is None:
            raise TypeError(
                "DataFrameCacheMatcher constructor:  collection name must be defined when prepend_collection_name is set True"
            )
        elif isinstance(collection, str):
            self.collection = collection

        else:
            raise TypeError(
                "DataFrameCacheMatcher constructor:   collection argument must be a string type"
            )
        # We have to reference the base class pymongo.database.Database here because
        # the MsPASS subclass name is also Database.  That make this
        # conditional fail in some uses.   Using the base class is totally
        # appropriate here anyway as no MsPASS extension methods are used
        if not isinstance(db_or_df, (type_pdd, type_ddd, pymongo.database.Database)):
            raise TypeError(
                "DataFrameCacheMatcher constructor:  required arg0 must be either a pandas, dask Dataframe, or database handle"
            )
        if attributes_to_load is None:
            if load_if_defined is None:
                raise MsPASSError(
                    "DataFrameCacheMatcher constructor:  usage error.  Cannot use default of attributes_to_load (triggers loading all columns) and define a list of names for argument load_if_defined"
                )
            aload = list()
            for key in db_or_df.columns:
                aload.append(key)
        else:
            aload = attributes_to_load

        super().__init__(
            attributes_to_load=aload,
            load_if_defined=load_if_defined,
            aliases=aliases,
        )

        df = (
            db_or_df
            if isinstance(db_or_df, (type_pdd, type_ddd))
            else pd.DataFrame(list(db_or_df[self.collection].find({})))
        )
        self._load_dataframe_cache(df)

    def find(self, mspass_object) -> tuple:
        """
        DataFrame generic implementation of find method.

        This method uses content in any part of the mspass_object
        (data object) to subset the internal DataFrame cache to
        return subset of tuples matching some condition defined
        computed through the abstract (virtual) methdod subset.
        It then copies entries in attributes_to_load and when not
        null load_if_defined into one Metadata container for each
        row of the returned DataFrame.
        """
        if not isinstance(mspass_object, Metadata):
            elog = PyErrorLogger(
                "DataFrameCacheMatcher.find",
                "Received datum that was not a valid MsPASS data object",
                ErrorSeverity.Invalid,
            )
            return [None, elog]

        subset_df = self.subset(mspass_object)
        # assume all implementations will return a 0 length dataframe
        # if the subset failed.
        if len(subset_df) <= 0:
            error_message = "subset method found no match for this datum"
            elog = PyErrorLogger()
            elog.log_error(error_message, ErrorSeverity.Invalid)
            return [None, elog]
        else:
            # This loop cautiously fills one or more Metadata
            # containers with each row of the DataFrame generating
            # one Metadata container.
            mdlist = list()
            elog = None
            for index, row in subset_df.iterrows():
                md = Metadata()
                notnulltest = row.notnull()
                for k in self.attributes_to_load:
                    if notnulltest[k]:
                        if k in self.aliases:
                            key = self.aliases[k]
                        else:
                            key = k
                        if self.prepend_collection_name:
                            if key == "_id":
                                mdkey = self.collection + key
                            else:
                                mdkey = self.collection + "_" + key
                        else:
                            mdkey = key
                        md[mdkey] = row[key]
                    else:
                        if elog is None:
                            elog = PyErrorLogger()
                        error_message = "Encountered Null value for required attribute {key} - repairs of the input DataFrame are required".format(
                            key=k
                        )
                        elog.log_error(
                            error_message,
                            ErrorSeverity.Invalid,
                        )
                        return [None, elog]
                for k in self.load_if_defined:
                    if notnulltest[k]:
                        if k in self.aliases:
                            key = self.aliases[k]
                        else:
                            key = k
                        if self.prepend_collection_name:
                            if key == "_id":
                                mdkey = self.collection + key
                            else:
                                mdkey = self.collection + "_" + key
                        else:
                            mdkey = key
                        md[mdkey] = row[key]
                mdlist.append(md)
            return [mdlist, None]

    def find_one(self, mspass_object) -> tuple:
        """
        DataFrame implementation of the find_one method.

        This method is mostly a wrapper around the find method.
        It calls the find method and then does one of two thing s
        depending upon the value of self.require_unique_match.
        When that boolean is True if the match is not unique it
        creates an PyErrorLogger object, posts a message to the log,
        and then returns a [Null,elog] pair.  If self.require_unique_match
        is False and the match is not ambiguous, it again creates an
        PyErrorLogger and posts a message, but it also takes the first
        container in the list returned by find and returns in as
        component 0 of the pair.
        """
        findreturn = self.find(mspass_object)
        mdlist = findreturn[0]
        if mdlist is None:
            return findreturn
        elif len(mdlist) == 1:
            return [mdlist[0], findreturn[1]]
        elif len(mdlist) > 1:
            if self.require_unique_match:
                raise MsPASSError(
                    "DataFrameCacheMatcher.find_one:  found {n} matches when require_unique_match was set true".format(
                        n=len(mdlist)
                    ),
                    ErrorSeverity.Invalid,
                )
            if findreturn[1] is None:
                elog = PyErrorLogger()
            else:
                elog = findreturn[1]
            # This maybe should be only posted with a verbose option????
            error_message = "found {n} matches.  Returned first one found.  You should use find instead of find_one if the match is not unique".format(
                n=len(mdlist)
            )
            elog.log_error(error_message, ErrorSeverity.Complaint)
            return [mdlist[0], elog]
        else:
            # This is a safety purely for code maintenance.
            # currently find either returns None or a list with data in it
            # we enter this safety ONLY if find returns a zero length list
            # we raise an exception if that happens because it is
            # not expeted.
            raise MsPASSError(
                "DataFrameCacheMatchter.find_one:   find returned an empty list.  Can only happen if custom matcher has overridden find.  Find should return None if the match fails",
                ErrorSeverity.Fatal,
            )

    @abstractmethod
    def subset(self, mspass_object) -> pd.DataFrame:
        """
        Required method defining how the internal DataFrame cache is
        to be subsetted using the contents of the data object
        mspass_object.   Concrete implementation must implement this class.
        The point of this abstract method is that the way one defines
        how to get the information needed to define a match with the
        cache is application dependent.  An implementation can use Metadata
        attributes, data object attributes (e.g. TimeSeries t0 attribute),
        or even sample data to compute a value to use in DataFrame
        subset condition.   This simplifies writing a custom matcher to
        implementing only this method as find and find_one use it.

        Implementations should return a zero length DataFrame if the
        subset condition yields a null result.  i.e. the test
        len(return_result) should work and return 0 if the subset
        produced no rows.
        """
        pass

    def _load_dataframe_cache(self, df):
        # This is a bit error prone.  It assumes the BasicMatcher
        # constructor initializes a None default to an empty list
        fulllist = self.attributes_to_load + self.load_if_defined
        self.cache = df.reindex(columns=fulllist)[fulllist]
        if self.custom_null_values is not None:
            for col, nul_val in self.custom_null_values.items():
                self.cache[col] = self.cache[col].replace(nul_val, np.nan)


class ObjectIdDBMatcher(DatabaseMatcher):
    """
    Implementation of DatabaseMatcher for ObjectIds.  In this class the virtual
    method query_generator uses the mspass convention of using the
    collection name and the magic string "_id" as the data object
    key (e.g. channel_id) but runs the query using the "_id" magic
    string used in MongoDB for the  ObjectId of each document.

    Users should only utilize the find_one method of this class as find,
    by definition, will always return only one record or None.
    The find method, in fact, is overloaded and attempts to use it will
    result in raising a MsPASSError exception.

    :param db:  MongoDB database handle  (positional - no default)
    :type db: normally a MsPASS Database class but with this algorithm
      it can be the superclass from which Database is derived.

    :param collection:  Name of MongoDB collection that is to be queried
       (default "channel").
    :type collection: string

    :param attributes_to_load:  list of keys of required attributes that will
      be returned in the output of the find method.   The keys listed
      must ALL have defined values for all documents in the collection or
      some calls to find_one will fail.
      Default ["_id","lat","lon","elev","hang","vang"].
    :type attributes_to_load:  list of string defining keys in collection
      documents

    :param load_if_defined: list of keys of optional attributes to be
      extracted by find method.  Any data attached to these keys will only
      be posted in the find return if they are defined in the database
      document retrieved in the query.
    :param type:  list of strings defining collection keys

    :param aliases:  python dictionary defining alias names to apply
     when fetching from a data object's Metadata container.   The key sense
     of the mapping is important to keep straight.  The key of this
     dictionary should match one  of the attributes in attributes_to_load
     or load_if_defined.  The value the key defines should be the alias
     used to fetch the comparable attribute from the data.
    :type aliaes:  python dictionary

    :param prepend_collection_name:  when True attributes returned in
      Metadata containers by the find and find_one method will all have the
      collection name prepended with a (fixed) separator.  For example, if
      the collection name is "channel" the "lat" attribute in the channel
      document would be returned as "channel_lat".
    :type prepend_collection_name:  boolean
    """

    def __init__(
        self,
        db,
        collection="channel",
        attributes_to_load=["_id", "lat", "lon", "elev", "hang", "vang"],
        load_if_defined=None,
        aliases=None,
        prepend_collection_name=True,
    ):
        """
        Class Constructor.  Just calls the superclass constructor
        directly with no additions.  Sets unique match, however, to
        be dogmatically enforce uniqueness.
        """
        super().__init__(
            db,
            collection,
            attributes_to_load=attributes_to_load,
            load_if_defined=load_if_defined,
            aliases=aliases,
            require_unique_match=True,
            prepend_collection_name=prepend_collection_name,
        )

    def query_generator(self, mspass_object) -> dict:
        data_object_key = self.collection + "_id"
        if mspass_object.is_defined(data_object_key):
            query_id = mspass_object[data_object_key]
            # This is a bombproof way to create an ObjectId
            # works the same if query_id is a string representation of
            # the id or an actual ObjectId. In the later case it calls
            # the copy constructor.
            testid = ObjectId(query_id)
            return {"_id": testid}
        else:
            return None


class ObjectIdMatcher(DictionaryCacheMatcher):
    """
    Implement an ObjectId match with caching.  Most of the code for
    this class is derived from the superclass DictionaryCacheMatcher.
    It adds only a concrete implementation of the cache_id method
    used to construct a key for the cache defined by a python dict
    (self.normcache).  In this case the cache key is simply the
    string representation of the ObjectId of each document in
    the collection defined in construction.   The cache is
    then created by the superclass generic method _load_normalization_cache.

    :param db:  MongoDB database handle  (positional - no default)
    :type db: normally a MsPASS Database class but with this algorithm
      it can be the superclass from which Database is derived.

    :param collection:  Name of MongoDB collection that is to be loaded
       and cached to memory inside this object (default "channel")
    :type collection: string

    :param query:  optional query to apply to collection before loading
       document attributes into the cache.   A typical example would be
       a time range limit for the channel or site collection to
       avoid loading instruments not operational during the time span
       of a data set.  Default is None which causes the entire collection
       to be parsed.
    :type query:  python dict with content that defines a valid query
       when be passed to MongoDB the MongoDB find method.  If query is
       a type other than a None type or dict the constructor will
       throw a TypeError.

    :param attributes_to_load:  list of keys of required attributes that will
      be returned in the output of the find method.   The keys listed
      must ALL have defined values for all documents in the collection or
      some calls to find will fail.
      Default ["_id","lat","lon","elev","hang","vang"]
    :type attributes_to_load:  list of string defining keys in collection
      documents

    :param load_if_defined: list of keys of optional attributes to be
      extracted by find method.  Any data attached to these keys will only
      be posted in the find return if they are defined in the database
      document retrieved in the query.
    :param type:  list of strings defining collection keys

    :param aliases:  python dictionary defining alias names to apply
     when fetching from a data object's Metadata container.   The key sense
     of the mapping is important to keep straight.  The key of this
     dictionary should match one  of the attributes in attributes_to_load
     or load_if_defined.  The value the key defines should be the alias
     used to fetch the comparable attribute from the data.
    :type aliaes:  python dictionary

    :param prepend_collection_name:  when set true all attributes loaded
       from the normalizing collection will have the channel name prepended.
       That is essential if the collection contains generic names like "lat"
       or "depth" that would produce ambiguous keys if used directly.
       (e.g. lat is used for source, channel, and site collections in the
        default schema.)
    :type prepend_collection_name:  boolean
    """

    def __init__(
        self,
        db,
        collection="channel",
        query=None,
        attributes_to_load=["_id", "lat", "lon", "elev", "hang", "vang"],
        load_if_defined=None,
        aliases=None,
        prepend_collection_name=True,
    ):
        """
        Class Constructor.  Just calls the superclass constructor
        directly with no additions.  It does, however, set the
        require_unique_match boolean True which cause the find_one
        method to be dogmatic in enforcing unique matches.
        """
        # require_unique_match is alwatys set True here as that is
        # pretty much by definition.
        super().__init__(
            db,
            collection,
            query=query,
            attributes_to_load=attributes_to_load,
            load_if_defined=load_if_defined,
            aliases=aliases,
            require_unique_match=True,
            prepend_collection_name=prepend_collection_name,
        )
        if query is None:
            self.query = dict()
        elif isinstance(query, dict):
            self.query = query
        else:
            raise TypeError(
                "ObjectIdMatcher constructor:  optional query argument must be either None or a python dict"
            )

    def cache_id(self, mspass_object) -> str:
        """
        Implementation of virtual method with this name for this matcher.
        It implements the MsPASS approach of defining the key for a
        normalizing collection as the collection name and the magic
        string "_id"  (e.g. channel_id or site_id).   It assumes
        the collection name is define as self.collection by the
        constructor of the class when it is instantiated.  It attempts
        to extract the expanded _id name (e.g. channel_id) from the
        input mspass_object.  If successful it returns the string
        representation of the resulting (assumed) ObjectId.  If the
        key is not defined it returns None as specified by the
        superclass api.

        It is important to note the class attribute,
        self.prepend_collection_name, is indendent of the definition of the
        cache_id.  i.e. what we attempt to extract as the id ALWAYS
        used the collection name as a prefix (channel_id and not "_id").
        The internal boolean controls if the attributes returned by find_one
        will have the collection name prepended.

        :param mspass_object:   key-value pair container containing an id that is to
          be extracted.
        :type mspass_object:  Normally this is expected to be a mspass
          data object (TimeSeries, Seismogram, or ensembles of same) but
          it can be as simple as a python dict or Metadata with the required
          key defined.

        :return:  string representation of an ObjectId to be used
          to matching the cache index stored internally - find method.
        """

        testid = self.collection + "_id"
        if testid in mspass_object:
            return str(mspass_object[testid])
        else:
            return None

    def db_make_cache_id(self, doc) -> str:
        """
        Implementation of virtual methods with this name for this matcher.
        It does nothing more than extract the magic "_id" value from doc
        and returns its string representation.  With MongoDB that means
        the string representation of the ObjectId of each collection
        document is used as the key for the cache.

        :param doc:  python dict defining a document return by MongoDB.
          Only the "_id" value is used.
        :type doc:  python dict container returned by pymongo - usually a
          cursor component.
        """
        if "_id" in doc:
            return str(doc["_id"])
        else:
            return None


class MiniseedDBMatcher(DatabaseMatcher):
    """
    Database implementation of matcher for miniseed data using
    SEED keys net:sta:chan(channel only):loc and a time interval test.
    Miniseed data uses the exessively complex key that combines four unique
    string names (net, sta, chan, and loc) and a time interval
    of operation to define a unique set of station metadata for each
    channel.  In mspass we also create the site collection without the
    chan attribute.  This implementation works for both channel and
    site under control of the collection argument.

    This case is the complete opposite of something like the ObjectId
    matcher above as the match query we need to generate is
    excessively long requiring up to 6 fields.

    The default collection name is channel which is the only
    correct use if applied to data created through readers applied to
    wf_miniseed.   The implementation can also work on Seismogram
    data if and only if the channel argument is then set to "site".
    The difference is that for Seismogram data "chan" is a
    undefined concept.  In both cases the keys and content assume the mspass
    schema for the channel or site collections.  The constructor will
    throw a MsPASSError exception if the collection argument is
    anything but "channel" or "site" to enforce this limitation.
    If you use a schema other than the mspass schema the methods in
    this class will fail if you change any of the following keys:
        net, sta, chan, loc, starttime, endtime, hang, vang

    Users should only call
    the find_one method for this application.   The find_one algorithm
    first queries for any matches of net:sta:chan(channel only):loc(optional) and
    data t0 within the startime and endtime of the channel document
    attributes (an interval match).  That combination should yield
    either 1 or no matches if the channel collection is clean.
    However, there are known issues with station metadata that can
    cause multiple matches in unusual cases (Most notably overlapping
    time intervals defined for the same channel.)  The find_one method
    will handle that case returning the first one found and posting an
    error message that should be handled by the caller.

    Instantiation of this class is a call to the superclass
    constructor with specialized defaults and wrapper code to
    automatically handle potential mismatches between site and channel.
    The arguments for the constructor follow:

    :param db:  MongoDB database handle  (positional - no default)
    :type db: normally a MsPASS Database class but with this algorithm
      it can be the superclass from which Database is derived.

    :param collection:  Name of MongoDB collection that is to be queried
       The default is "channel".  Use "site" for Seismogram data.
       Use anything else at your own risk
       as the algorithm depends heavily on mspass schema definition
       and properties guaranteed by using the converter from obspy
       Inventory class loaded through web services or stationml files.
    :type collection: string

    :param attributes_to_load:  list of keys of required attributes that will
      be returned in the output of the find method.   The keys listed
      must ALL have defined values for all documents in the collection or
      some calls to find will fail.
      Default ["_id","lat","lon","elev","hang","vang"]
               "hang","vang","starttime","endtime"]
      when collection is set as channel.  Smaller list of
      ["_id","lat","lon","elev"] is
       default when collection is set as "site".
    :type attributes_to_load:  list of string defining keys in collection
      documents

    :param load_if_defined: list of keys of optional attributes to be
      extracted by find method.  Any data attached to these keys will only
      be posted in the find return if they are defined in the database
      document retrieved in the query.  Default is ["loc"].   A common
      addition here may be response data (see schema definition for keys)
    :type load_if_defined:  list of strings defining collection keys

    :param aliases:  python dictionary defining alias names to apply
     when fetching from a data object's Metadata container.   The key sense
     of the mapping is important to keep straight.  The key of this
     dictionary should match one  of the attributes in attributes_to_load
     or load_if_defined.  The value the key defines should be the alias
     used to fetch the comparable attribute from the data.
    :type aliaes:  python dictionary

    :param prepend_collection_name:  when True attributes returned in
      Metadata containers by the find and find_one method will all have the
      collection name prepended with a (fixed) separator.  For example, if
      the collection name is "channel" the "lat" attribute in the channel
      document would be returned as "channel_lat".
    :type prepend_collection_name:  boolean

    """

    def __init__(
        self,
        db,
        collection="channel",
        attributes_to_load=["starttime", "endtime", "lat", "lon", "elev", "_id"],
        load_if_defined=None,
        aliases=None,
        prepend_collection_name=True,
    ):
        aload_tmp = attributes_to_load
        if collection == "channel":
            if "hang" not in aload_tmp:
                aload_tmp.append("hang")
            if "vang" not in aload_tmp:
                aload_tmp.append("vang")
        elif collection != "site":
            raise MsPASSError(
                "MiniseedDBMatcher:  "
                + "Illegal collection argument="
                + collection
                + " Must be either channel or site",
                ErrorSeverity.Fatal,
            )

        super().__init__(
            db,
            collection,
            attributes_to_load=aload_tmp,
            load_if_defined=load_if_defined,
            aliases=aliases,
            require_unique_match=False,
            prepend_collection_name=prepend_collection_name,
        )

    def query_generator(self, mspass_object):
        """
        Concrete implementation of (required) abstract method defined
        in superclass DatabaseMatcher.  It generates the complex query
        for matching net, sta, chan, and optional loc along with the
        time interval match of data start time between the channel
        defined "starttime" and "endtime" attributes.

        This method provides one safety for a common data problem.
        A common current issue is that if miniseed data are saved
        to wf_TimeSeries and then read back in a later workflow
        the default schema will alter the keys for net, sta, chan,
        and loc to add the prefix "READONLY_" (e.g. READONLY_net).
        The query automatically tries to recover any of the
        station name keys using that recipe.

        :param mspass_object:   assumed to be a TimeSeries object
          with net, sta, chan, and (optional) loc defined.
          The time for the time interval test is translation to
          MongoDB syntax of:
           channel["starttime"] <= mspass_object.to <= channel["endtime"]
           This algorithm will abort if the statement mspass_object.t0
           does not resolve, which means the caller should assure
           the input is a TimeSeries object.

        :return:  normal return is a string defining the query.
           If any required station name keys are not defined the
           method will silently return a None.  Caller should handle
           a None condition.
        """
        query = dict()
        net = _get_with_readonly_recovery(mspass_object, "net")
        if net is None:
            return None
        query["net"] = net
        sta = _get_with_readonly_recovery(mspass_object, "sta")
        if sta is None:
            return None
        query["sta"] = sta
        if self.collection == "channel":
            chan = _get_with_readonly_recovery(mspass_object, "chan")
            if chan is None:
                return None
            query["chan"] = chan
        loc = _get_with_readonly_recovery(mspass_object, "loc")
        # loc being null is not unusual so if is is null we just add it to query
        if loc != None:
            query["loc"] = loc

        # We don't verify mspass_object is a valid TimeSeries here
        # assuming it was done prior to calling this method.
        # fetching t0 could cause an abort if mspass_object were not
        # previously validated.  An alternative would be to test
        # here and return None if mspass_object was not a valid TimeSeries
        # done this way because other bad things would happen if find
        # if that assumption was invalid

        query["starttime"] = {"$lte": mspass_object.t0}
        query["endtime"] = {"$gte": mspass_object.endtime()}
        return query

    def find_one(self, mspass_object):
        """
        We overload find_one to provide the unique match needed.
        Most of the work is done by the query_generator method in
        this case.  This method is little more than a wrapper to
        run the find method and translating the output into the
        slightly different form required by find_one.   More important
        is the fact that the wrapper implements two safties to make the
        code more robust:
            1.  It immediately tests that mspass_object is a valid TimeSeries
                or Seismogram object. It will raise a TypeError exception
                if that is not true.  That is enforced because find_one
                in this context make sense only for atomic objects.
            2.  It handles dead data cleanly logging a message complaining
                that the data was already marked dead.

        In addition note the find method this calls is assumed to handle
        the case of failures in the query_generator function if any of the
        required net, sta, chan keys are missing from mspass_object.
        """
        # Be dogmatic and demand mspass_object is a TimeSeries or Seismogram (atomic)
        if _input_is_atomic(mspass_object):
            if mspass_object.live:
                # trap this unlikely but possible condition as this
                # condition could produce mysterious behavior
                if mspass_object.time_is_relative():
                    elog = PyErrorLogger()
                    message = "Usage error:  input has a relative time standard but miniseed matching requires a UTC time stamp"
                    elog.log_error(message, ErrorSeverity.Invalid)
                    return [None, elog]
                find_output = self.find(mspass_object)
                if find_output[0] is None:
                    return [None, find_output[0]]
                number_matches = len(find_output[0])
                if number_matches == 1:
                    return [find_output[0][0], find_output[1]]
                else:
                    if find_output[1] is None:
                        elog = PyErrorLogger()
                    else:
                        elog = find_output[1]
                    message = "{n} channel recorded match net:sta:chan:loc:time_interval query for this datume\n".format(
                        n=number_matches
                    )
                    message += "Using first document in list returned by find method"
                    elog.log_error(message, ErrorSeverity.Complaint)
                    return [find_output[0][0], elog]

            else:
                # logged as complaint because by definition if it was
                # already killed it is Invalid
                elog = PyErrorLogger()
                elog.log_error(
                    "Received a datum marked dead - will not attempt match",
                    ErrorSeverity.Complaint,
                )
                return [None, elog]
        else:
            raise TypeError(
                "MiniseedDBMatcher.find_one:  this class method can only be applied to TimeSeries or Seismogram objects"
            )


class MiniseedMatcher(DictionaryCacheMatcher):
    """
    Cached version of matcher for miniseed station/channel Metadata.

    Miniseed data require 6 keys to uniquely define a single channel
    of data (5 at the Seismogram level where the channels are merged).
    A further complication for using the DictionaryCacheMatcher interface is
    that part of the definition is a UTC time interval defining when the
    metadata is valid.  We handle that in this implementation by
    implementing a two stage search algorithm for the find_one method.
    First, the cache index is defined by a unique string created from
    the four string keys of miniseed that the MsPASS default schema
    refers to with the keywords net, sta, chan, and loc.
    At this point in time we know of no examples of a seismic instrument
    where the number of distinct time intervals with different Metadata
    are huge so the secondary search is a simple linear search through
    the python list return by the generic find method using only the
    net, sta, chan, and loc keys as the index.

    The default collection name is channel which is the only
    correct use if applied to data created through readers applied to
    wf_miniseed.   The implementation can also work on Seismogram
    data if and only if the channel argument is then set to "site".
    The difference is that for Seismogram data "chan" is a
    undefined concept.  In both cases the keys and content assume the mspass
    schema for the channel or site collections.  The constructor will
    throw a MsPASSError exception if the collection argument is
    anything but "channel" or "site" to enforce this limitation.
    If you use a schema other than the mspass schema the methods in
    this class will fail if you change any of the following keys:
        net, sta, chan, loc, starttime, endtime, hang, vang

    Users should only call the find_one method for this application.
    The find_one method here overrides the generic find_one in the
    superclass DictionaryCacheMatcher.  It implements the linear search for a
    matching time interval test as noted above.  Note also this
    class does not support Ensembles directly.   Matching instrument
    data is by definition what we call atomic. If you are processing
    ensembles you will need to write a small wrapper function that
    would run find_one and handle the out looping over each member of
    the ensemble.

    :param db:  MongoDB database handle containing collection to be loaded.
    :type db: mspass Database handle(mspasspy.db.database.Database).

    :param collection:  Name of MongoDB collection that is to be queried
       The default is "channel".  Use "site" for Seismogram data.
       Use anything else at your own risk
       as the algorithm depends heavily on the mspass schema definition
       and properties guaranteed by using the converter from obspy
       Inventory class loaded through web services or stationml files.
    :type collection: string

    :param query:  optional query to apply to collection before loading
      data from the database.  This parameter is ignored if the input
      is a DataFrame.  A common use would be to reduce the size of the
      cache by using a time range limit on station metadata to only
      load records relevant to the dataset being processed.
    :type query:  python dictionary.

    :param attributes_to_load:  list of keys of required attributes that will
      be returned in the output of the find method.   The keys listed
      must ALL have defined values for all documents in the collection or
      some calls to find will fail.
      Default ["_id","lat","lon","elev","hang","vang"]
               "hang","vang","starttime","endtime"]
      when collection is set as channel.  Smaller list of
      ["_id","lat","lon","elev"] is
      default when collection is set as "site".  In either case the
      list MUST contain "starttime" and "endtime".  The reason is the
      linear search step will always use those two fields in the linear
      search for a time interval match. Be careful in how endtime is defined
      that resolves to an epoch time in the distant future and not some
      null database attribute; a possible scenario with DataFrame
      input but not a concern if using mspass loaders from StationML data.

      Note there is also an implicit assumption that the keys "net" and
      "sta" are always defined.  "chan" must also be defined if the
      collection name is "channel".  "loc" is handled as optional for
      database input but required if the input is via a Dataframe
      because we use the same cache id generator for all cases.
    :type attributes_to_load:  list of string defining keys in collection
      documents

    :param load_if_defined: list of keys of optional attributes to be
      extracted by find method.  Any data attached to these keys will only
      be posted in the find return if they are defined in the database
      document retrieved in the query.  Default is ["loc"].   A common
      addition here may be response data (see schema definition for keys)
    :type load_if_defined:  list of strings defining collection keys

    :param aliases:  python dictionary defining alias names to apply
     when fetching from a data object's Metadata container.   The key sense
     of the mapping is important to keep straight.  The key of this
     dictionary should match one  of the attributes in attributes_to_load
     or load_if_defined.  The value the key defines should be the alias
     used to fetch the comparable attribute from the data.
    :type aliaes:  python dictionary

    :param prepend_collection_name:  when True attributes returned in
      Metadata containers by the find and find_one method will all have the
      collection name prepended with a (fixed) separator.  For example, if
      the collection name is "channel" the "lat" attribute in the channel
      document would be returned as "channel_lat".
    :type prepend_collection_name:  boolean
    """

    def __init__(
        self,
        db,
        collection="channel",
        query=None,
        attributes_to_load=["starttime", "endtime", "lat", "lon", "elev", "_id"],
        load_if_defined=None,
        aliases=None,
        prepend_collection_name=True,
    ):
        aload_tmp = attributes_to_load
        if collection == "channel":
            # forcing this may be a bit too dogmatic but hang and vang
            # are pretty essential metadata for any channel
            if "hang" not in aload_tmp:
                aload_tmp.append("hang")
            if "vang" not in aload_tmp:
                aload_tmp.append("vang")

        elif collection != "site":
            raise MsPASSError(
                "MiniseedMatcher:  "
                + "Illegal collection argument="
                + collection
                + " Must be either channel or site",
                ErrorSeverity.Fatal,
            )

        if "starttime" not in aload_tmp or "endtime" not in aload_tmp:
            raise MsPASSError(
                "MiniseedMatcher:  "
                + "Error in attribute_to_load list - List must contain starttime and endtime keys",
                ErrorSeverity.Fatal,
            )

        super().__init__(
            db,
            collection,
            query=query,
            attributes_to_load=aload_tmp,
            load_if_defined=load_if_defined,
            aliases=aliases,
            require_unique_match=True,
            prepend_collection_name=prepend_collection_name,
        )

    def cache_id(self, mspass_object) -> str:
        """
        Concrete implementations of this required method.  The cache_id
        in this algorithm is a composite key made from net, sta, chan, and
        loc with a fixed separator string of "_".  A typical example
        is IU_AAK_BHZ_00_.

        An added feature to mesh with MsPASS conventions is a safety
        for attributes that are automatically renamed when saved
        that are marked readonly in the schema.   Such attributes
        have a prepended tag, (at this time "READONLYERROR_").  If
        one of the required keys for the index is missing (e.g. "net")
        the function tries to then fetch the modified name
        (e.g. "READONLYERROR_net").  If that also fails it returns
        a None as specified by the API.

        :param mspass_object:  mspass object to be matched with cache.
          Must contain net, sta fpr site matching and net, sta, and
          chan for the channel collection. If loc is not defined for
          any case an emtpy string in defined an the key has two
          trailing separator characters (e.g. IU_AAK_BHZ__)

        :return: normal return is a string that can be used as an index string.
          If any of the required keys is missing it will return a None
        """
        # we make this a const
        SEPARATOR = "_"
        net = _get_with_readonly_recovery(mspass_object, "net")
        if net is None:
            return None
        sta = _get_with_readonly_recovery(mspass_object, "sta")
        if sta is None:
            return None
        if self.collection == "channel":
            chan = _get_with_readonly_recovery(mspass_object, "chan")
            if chan is None:
                return None
        loc = _get_with_readonly_recovery(mspass_object, "loc")
        if loc == None:
            loc = ""
        idstring = net + SEPARATOR + sta + SEPARATOR + loc + SEPARATOR
        # a bit nonstandard to put chan at end but this isn't for human
        # consumption anyway
        if self.collection == "channel":
            idstring += chan + SEPARATOR
        return idstring

    def db_make_cache_id(self, doc) -> str:
        """
        Concrete implementations of this required method.  The cache_id
        in this algorithm is a composite key made from net, sta, chan, and
        loc with a fixed separator string of "_".  A typical example
        is IU_AAK_BHZ_00_.  This method creates this string from
        a MongoDB document assumed passed through a python dictionary
        as argument doc.  Unlike the cache_id method this function
        does not have a safety for readonly errors.  The reason is that
        it is designed to be used only while loading the cache from
        site or channel documents.

        :param doc:  python dict containing a site or channel document.
          Must contain net, sta fpr site matching and net, sta, and
          chan for the channel collection. If loc is not defined for
          any case an emtpy string in defined an the key has two
          trailing separator characters (e.g. IU_AAK_BHZ__)

        :return: normal return is a string that can be used as an index string.
          If any of the required keys is missing it will return a None
        """
        # we make this a const
        SEPARATOR = "_"
        # superclass must handle None returns for invalid document
        if "net" in doc:
            net = doc["net"]
        else:
            return None
        if "sta" in doc:
            sta = doc["sta"]
        else:
            return None
        if self.collection == "channel":
            if "chan" in doc:
                chan = doc["chan"]
            else:
                return None
        if "loc" in doc:
            loc = doc["loc"]
        else:
            loc = ""
        idstring = net + SEPARATOR + sta + SEPARATOR + loc + SEPARATOR
        # a bit nonstandard to put chan at end but this isn't for human
        # consumption anyway
        if self.collection == "channel":
            idstring += chan + SEPARATOR
        return idstring

    def find_one(self, mspass_object):
        """
        We overload find_one to provide the unique match needed.
        The algorithm does a linear search for the first time interval
        for which the start time of mspass_object is within the
        startime <= t0 <= endtime range of a record stored in
        the cache.  This works only if starttime and endtime are
        defined in the set of attributes loaded so the constructor
        of this class enforces that restriction.   The times in
        starttime and endtime are assumed to be defined as epoch
        times as the algorithm uses a simple numeric test of the
        data start time with the two times.  An issue to watch out
        for is endtime not being set to a valid distant time but some
        null field that resolves to something that doesn't work in
        a numerical test for < endtime.

        :param mspass_object:   data to be used for matching against the
        cache.  It must contain the required keys or the matching will
        fail.  If the datum is marked dead the algorithm will return
        immediately with a None response and an error message that
        would usually be dropped by the call in that situation.
        :type mspass_object:  must be one of the atomic data types of
          mspass (currently TimeSeries and Seismogram) with t0
          defined as an epoch time computed from a UTC time stamp.
        """
        # Be dogmatic and demand mspass_object is a TimeSeries
        if isinstance(mspass_object, (TimeSeries, Seismogram)):
            if mspass_object.live:
                # trap this unlikely but possible condition as this
                # condition could produce mysterious behavior
                if mspass_object.time_is_relative():
                    elog = PyErrorLogger()
                    message = "Usage error:  input has a relative time standard but miniseed matching requires a UTC time stamp"
                    elog.log_error(message, ErrorSeverity.Invalid)
                    return [None, elog]
                find_output = self.find(mspass_object)
                if find_output[0] is None:
                    # Current implementation posts a elog message that is
                    # returned in file_output[1].  Could consider adding to
                    # that log message here to clarify it was the miniseed
                    # instance
                    return [None, find_output[1]]
                else:
                    for md in find_output[0]:
                        stime_key = (
                            "starttime"
                            if not self.prepend_collection_name
                            else self.collection + "_starttime"
                        )
                        etime_key = (
                            "endtime"
                            if not self.prepend_collection_name
                            else self.collection + "_endtime"
                        )
                        stime = md[stime_key]
                        etime = md[etime_key]
                        t0 = mspass_object.t0
                        if t0 >= stime and t0 <= etime:
                            return [md, find_output[1]]

                    # we land here if the linear search failed and no
                    # t0 is within the ranges defined
                    if find_output[1] is None:
                        elog = PyErrorLogger()
                    else:
                        elog = find_output[1]
                    message = (
                        "No matching records found in "
                        + self.collection
                        + " collection for:\n"
                    )
                    message += "net=" + mspass_object["net"]
                    message += ", sta=" + mspass_object["sta"]
                    message += ", chan=" + mspass_object["chan"]
                    if mspass_object.is_defined("loc"):
                        message += ", loc=" + mspass_object["loc"]
                    message += "time=" + str(UTCDateTime(mspass_object.t0))
                    message += "\nFound a match for station codes but no channel time range contains that time "
                    elog.log_error(message, ErrorSeverity.Invalid)
                    return [None, elog]
            else:
                elog = PyErrorLogger()
                elog.log_error(
                    "Received a datum marked dead - will not attempt match",
                    ErrorSeverity.Invalid,
                )
                return [None, elog]
        else:
            raise TypeError(
                "MiniseedMatcher.find_one:  this class method can only be applied to TimeSeries or Seismogram objects"
            )

    def find_doc(self, doc, wfdoc_starttime_key="starttime"):
        """
        Function to support application to bulk_normalize to
        set channel_id or site_id.  Acts like find_one but without support
        for readonly recovery.   The bigger difference is that this
        method accepts a python dict retrieved in a cursor loop for
        bulk_normalize.   Returns the Metadata container that is matched
        from the cache.   This uses the same algorithm as the overloaded
        find_one above where a linear search is used to handle the time
        interval matching.  Here, however, the time field is extracted
        from doc with the key defined by starttime.

        This method overrides the generic version in BasicMatcher due
        to some special peculiarities of miniseed.

        :param doc:  document (pretty much assumed to be from wf_miniseed)
          to be matched with channel or site.
        :type doc:  python dictionary - document from MongoDB

        :param wfdoc_starttime_key:  optional parameter to change the
         key used to fetch the start time of waveform data from doc.
         Default is "starttime"/
        :type wfdoc_starttime_key: string

        :return:  matching Metadata container if successful. None if
          matching fails for any reason.
        """
        # always abort if the starttime key is not defined.  For
        # current implementation wf_miniseed documents always have that
        # key defined so throwing an exception marked Fatal is appropriate.
        if wfdoc_starttime_key not in doc:
            raise MsPASSError(
                "MiniseedMatcher:  "
                + "Required key defining waveform start time="
                + wfdoc_starttime_key
                + " is missing from document received",
                ErrorSeverity.Fatal,
            )
        # we use this version of the method to generate the cache key as
        # it works with the dict, BUT it doesn't allow the READONLYERROR
        # recovery
        testid = self.db_make_cache_id(doc)
        if testid is None:
            return None
        if testid in self.normcache:
            matches = self.normcache[testid]
        else:
            return None

        # linear search similar to that in find_one above
        for md in matches:
            if self.prepend_collection_name:
                stkey = self.collection + "_" + "starttime"
                etkey = self.collection + "_" + "endtime"
            else:
                stkey = "starttime"
                etkey = "endtime"
            # let this throw an exception if fetch fails. Constructor
            # should guarantee these two attributes are loaded
            stime = md[stkey]
            etime = md[etkey]
            dt0 = doc[wfdoc_starttime_key]
            if dt0 >= stime and dt0 <= etime:
                return md

        return None


class EqualityMatcher(DataFrameCacheMatcher):
    """
    Match with an equality test for the values of one or more keys
    with possible aliasing between data keys and database keys.

    This class can be used for matching a set of keys that together
    provide a unique matching capability.   Note the keys are
    applied sequentially to reduce the size of internal DataFrame
    cache in stages.  If the DataFrame is large it may improve performance
    if the most unique key in a series appears first.

    A special feature of the implementation is that we allow
    what is best thought of as reverse aliasing for the keys to
    be used for matching.   That is, the base class of this family
    has an attribute self.aliases that allow mapping from collection names
    to data object names.  The match_keys parameter here is done in
    the reverse order.  That is, the key of the match_keys dictionary
    is the data object key while the value associated with that key
    is the DataFrame column name to match.  The constructor of the
    class does a sanity check to verify the two are consistent.
    The constructor will throw an exception if the two dictionaries
    are inconstent.  Note that means if you use an actual alias through
    match_keys (i.e. the key and value are different) you must define
    the aliases dictionary with the same combination reversed.
    (e.g.  matchkeys={"KSTA":"sta"} requires aliases={"sta":"KSTA"})

    :param db_or_df:  MongoDB database handle or a pandas DataFrame.
      Most users will use the database handle version.   In that case
      the collection argument is used to determine what collection is
      loaded into the cache.   If using a DataFrame is used the
      collection name is only a tag defined by the user.   For a
      DataFrame a column index is required that contains at least
      the attributes defined in attribute_to_load.
    :type db_or_df: MongoDB database handle or pandas DataFrame.

    :param collection:  When using database input this is expected to be
      a string defining a valid MongoDB collection with documents that are
      to be scanned and loaded into the internal cache.  With DataFrame input
      this string is only a tag.  It is relevant then only if the
      prepend_collection_name boolean is set True.  There is no default
      for this parameter so it must be specified as arg 1.
    :type collection: string

    :param match_keys:  python dict of keys that are to be used for
      the equality match.  The dict is used as an alias mechanism allowing
      different keys to be used for the Metadata container in data to
      be tested relative to the keys used in the database for the same
      attribute.  (a typical common example would be something like
      "source_lat" in the data matching "lat" in the source collection).
      The key for each entry in this dict is taken as the key for the
      data side (mspass_object) and the value assigned to that key
      in this input is taken as the mongoDB/DataFrame key.
    :type match_keys:  python dictionary

    :param attributes_to_load:  list of keys of required attributes that will
      be returned in the output of the find method.   The keys listed
      must ALL have defined values for all documents in the collection or
      some calls to find will fail.   There is currently no default for
      this parameter and it must be defined as arg 3.
    :type attributes_to_load:  list of string defining keys in collection
      documents

    :param query:  optional query to apply to collection before loading
      data from the database.  This parameter is ignored if the input
      is a DataFrame.  A common use would be to reduce the size of the
      cache by using a time range limit on station metadata to only
      load records relevant to the dataset being processed.  This
      parameter is currently ignored for DataFrame input as we assume
      pandas subsetting would be used for the same functionality
      in the workflow prior to calling the class constructor for this object.
    :type query:  python dictionary.

    :param load_if_defined: list of keys of optional attributes to be
      extracted by find method.  Any data attached to these keys will only
      be posted in the find return if they are defined in the database
      document retrieved in the query.  Default resolves to an empty list.
      Note this parameter is ignored for DataFrame input.
    :type load_if_defined:  list of strings defining collection keys

    :param aliases:  python dictionary defining alias names to apply
     when fetching from a data object's Metadata container.   The key sense
     of the mapping is important to keep straight.  The key of this
     dictionary should match one  of the attributes in attributes_to_load
     or load_if_defined.  The value the key defines should be the alias
     used to fetch the comparable attribute from the data.
    :type aliaes:  python dictionary

    :param prepend_collection_name:  when True attributes returned in
      Metadata containers by the find and find_one method will all have the
      collection name prepended with a (fixed) separator.  Default is
      False.
    :type prepend_collection_name:  boolean

    :param require_unique_match:  boolean handling of ambiguous matches.
      When True find_one will throw an error if an entry is tries to match
      is not unique.  When False find_one returns the first document
      found and logs a complaint message.  (default is True)
    :type require_unique_match:  boolean
    """

    def __init__(
        self,
        db_or_df,
        collection,
        match_keys,
        attributes_to_load,
        query=None,
        load_if_defined=None,
        aliases=None,
        require_unique_match=True,
        prepend_collection_name=False,
        custom_null_values=None,
    ):
        super().__init__(
            db_or_df,
            collection,
            attributes_to_load=attributes_to_load,
            load_if_defined=load_if_defined,
            aliases=aliases,
            require_unique_match=require_unique_match,
            prepend_collection_name=prepend_collection_name,
            custom_null_values=custom_null_values,
        )
        if isinstance(match_keys, dict):
            self.match_keys = match_keys
            for key in match_keys:
                testkey = match_keys[key]
                # this means no aliasing for key
                if testkey == key:
                    continue
                if testkey in self.aliases:
                    backtestkey = self.aliases[testkey]
                    if backtestkey != key:
                        error_message = (
                            "EqualityMatcher constructor:  "
                            + "match_keys and aliases are inconsistent.\n"
                            + "match_keys="
                            + str(match_keys)
                            + "  aliases="
                            + str(self.aliases)
                        )
                        raise MsPASSError(error_message, ErrorSeverity.Fatal)
                else:
                    error_message = (
                        "EqualityMatcher constructor:  "
                        + "match_keys and aliases are inconsistent.\n"
                        + "match_key defines key="
                        + key
                        + " to have alias name="
                        + testkey
                        + " but alias name is not defined by aliases parameter"
                    )
                    raise MsPASSError(error_message, ErrorSeverity.Fatal)
        else:
            raise TypeError(
                "EqualityMatcher Constructor:  required argument 2 (matchkeys) must be a python dictionary"
            )

    def subset(self, mspass_object) -> pd.DataFrame:
        """
        Concrete implementation of this virtual method of DataFrameMatcher
        for this class.

        The subset is done sequentially driven by the order key order
        of the self.match_keys dictionary.   i.e. the algorithm uses
        the row reduction operation of a dataframe one key at a time.
        An implementation detail is that there may be a more clever way
        instead create a single conditional clause to pass to the
        DataFrame operator [] combining the key matches with "and".
        That would likely improve performance, particulary on large tables.
        Note the alias is applied using the self.match_keys.  i.e. one
        can have different keys on the left (mspass_data side is the
        match_keys dictionary key) than the right (dataframe column name).

        :param mspass_object:  Any valid mspass data object with a
        Metadata container.  The container must contain all the
        required match keys or the function will return an error condition
        (see below)
        :type mspass_object:  TimeSeries, Seismogram, TimeSeriesEnsemble
          or SeismogramEnsemble object

        :return:  DataFrame containing all data satisying the match
           series of match conditions defined on construction.  Silently
           returns a zero length DataFrame if is no match.   Be warned
           two other situations can cause the return to have no data:
               (1) dead input, and (2) match keys missing from mspass_object.
        """
        if hasattr(mspass_object, "dead"):
            if mspass_object.dead():
                return pd.DataFrame()
        # I don't think this can cause a memory problem as in python
        # this make dfret a temporary alias for self.cache
        # In the loop it is replaced by subset dataframes
        dfret = self.cache
        for key in self.match_keys.keys():
            if mspass_object.is_defined(key):
                testval = mspass_object[key]
                # this allows an alias between data and dataframe keys
                dfret = dfret[dfret[self.match_keys[key]] == testval]
            else:
                return pd.DataFrame()
        return dfret


class EqualityDBMatcher(DatabaseMatcher):
    """
    Database equivalent of EqualityMatcher.

    param db:  MongoDB database handle  (positional - no default)
    :type db: normally a MsPASS Database class but with this algorithm
      it can be the superclass from which Database is derived.

    :param collection:  Name of MongoDB collection that is to be queried.
      This arg is required by the constructor and has not default.
    :type collection: string

    :param match_keys:  python dict of keys that are to be used for
      the equality match.  The dict is used as an alias mechanism allowing
      different keys to be used for the Metadata container in data to
      be tested relative to the keys used in the database for the same
      attribute.  (a typical common example would be something like
      "source_lat" in the data matching "lat" in the source collection).
      The key for each entry in this dict is taken as the key for the
      data side (mspass_object) and the value assigned to that key
      in this input is taken as the mongoDB/DataFrame key.
    :type match_keys:  python dictionary

    :param attributes_to_load:  list of keys of required attributes that will
      be returned in the output of the find method.   The keys listed
      must ALL have defined values for all documents in the collection or
      some calls to find will fail.  There is no default for this class
      and the list must be defined as arg3.
    :type attributes_to_load:  list of string defining keys in collection
      documents.

    :param load_if_defined: list of keys of optional attributes to be
      extracted by find method.  Any data attached to these keys will only
      be posted in the find return if they are defined in the database
      document retrieved in the query.  Default is to add load no
      optional data.
    :param type:  list of strings defining collection keys

    :param aliases:  python dictionary defining alias names to apply
     when fetching from a data object's Metadata container.   The key sense
     of the mapping is important to keep straight.  The key of this
     dictionary should match one  of the attributes in attributes_to_load
     or load_if_defined.  The value the key defines should be the alias
     used to fetch the comparable attribute from the data.
    :type aliaes:  python dictionary

    :param prepend_collection_name:  when True attributes returned in
      Metadata containers by find and find_one method will all have the
      collection name prepended with a (fixed) separator.  For example, if
      the collection name is "channel" the "lat" attribute in the channel
      document would be returned as "channel_lat".  Default is False.
    :type prepend_collection_name:  boolean

    :param require_unique_match:  boolean handling of ambiguous matches.
      When True find_one will throw an error if an entry is tries to match
      is not unique.  When False find_one returns the first document
      found and logs a complaint message.  (default is False)
    :type require_unique_match:  boolean

    """

    def __init__(
        self,
        db,
        collection,
        match_keys,
        attributes_to_load,
        load_if_defined=None,
        aliases=None,
        require_unique_match=False,
        prepend_collection_name=False,
    ):
        super().__init__(
            db,
            collection,
            attributes_to_load=attributes_to_load,
            load_if_defined=load_if_defined,
            aliases=aliases,
            require_unique_match=require_unique_match,
            prepend_collection_name=prepend_collection_name,
        )
        if isinstance(match_keys, dict):
            self.match_keys = match_keys
        else:
            raise TypeError(
                "{} constructor:  required arg2 must be a python dictionary - received invalid type.  See docstring".format(
                    self.__class__.__name__
                )
            )

    def query_generator(self, mspass_object) -> dict:
        """
        Implementation of required method for this class.  It simply
        applies an equality test for all the keys defined by the values
        in the self.match_keys dict.
        """
        query = dict()
        for dkey in self.match_keys:
            if dkey in mspass_object:
                value = mspass_object[dkey]
                query[self.match_keys[dkey]] = value
            else:
                # API says return none if generator fails
                return None
        return query


class OriginTimeDBMatcher(DatabaseMatcher):
    """
    Generic class to match data by comparing a time defined in data to an
    origin time using a database query algorithm.

    The default behavior of this matcher class is to match data to
    source documents based on origin time with an optional time offset.
    Conceptually the data model for this matching is identical to conventional
    multichannel shot gathers where the start time is usually the origin time.
    It is also a common model for downloaded source oriented waveform
    segments from FDSN web services with obspy.  Obspy has an example
    in their documentation for how to download data defined exactly this way.
    In that mode we match each source document that matches a projected
    origin time within a specified tolerance. Specifically, let t0
    be the start time extracted from the data.  We then compute the
    projected, test origin time as test_otime = t0 - t0offset.
    Note the sign convention that a positive offset means the time t0
    is after the event origin time.   We then select all source
    records for which the time field satisifies:
        source.time - tolerance <= test_time <= source.time + tolerance

    The test_time value for matching from a datum can come through
    one of two methods driven by the constructor argument "time_key".
    When time_key is a None (default) the algorithm assumes all input
    are mspass atomic data objects that have the start time defined by
    the attribute "t0" (mspass_object.t0).  If time_key is a string
    it is assumed to be a Metadata key used to fetch an epoch time
    to use for the test.   The most likely use of that feature would be
    for ensemble processing where test_time is set as a field in the
    ensemble Metadata.  Note that form of associating source data to
    ensembles that are common source gathers can be much faster than
    the atomic version because only one query is needed per ensemble.

    :param db:  MongoDB database handle  (positional - no default)
    :type db: normally a MsPASS Database class but with this algorithm
      it can be the superclass from which Database is derived.

    :param collection:  Name of MongoDB collection that is to be queried
       (default "source").
    :type collection: string

    :param t0offset: constant offset from data start time that is expected
      as origin time.  A positive t0offset means the origin time is before
      the data start time.  Units are always assumed to be seconds.
    :type t0offset:  float

    :param tolerance:   time tolerance to test for match of origin time.
      (see formula above for exact use)
      If the source estimates are exactly the same as the ones used to
      define data start time this number can be a few samples.
      Otherwise a few seconds is safter for teleseismic data and
      less for local/regional events.  i.e. the choice depends up on
      how the source estimates relate to the data.
    :type tolerance:  float


    :param attributes_to_load:  list of keys of required attributes that will
      be returned in the output of the find method.   The keys listed
      must ALL have defined values for all documents in the collection or
      some calls to find_one will fail.   Default is
      ["lat","lon","depth","time"]
    :type attributes_to_load:  list of string defining keys in collection
      documents

    :param load_if_defined: list of keys of optional attributes to be
      extracted by find method.  Any data attached to these keys will only
      be posted in the find return if they are defined in the database
      document retrieved in the query.  Default is ["magnitude"]
    :param type:  list of strings defining collection keys

    :param aliases:  python dictionary defining alias names to apply
       when fetching from a data object's Metadata container.   The key sense
       of the mapping is important to keep straight.  The key of this
       dictionary should match one  of the attributes in attributes_to_load
       or load_if_defined.  The value the key defines should be the alias
       used to fetch the comparable attribute from the data.
    :type aliaes:  python dictionary

    :param prepend_collection_name:  when True attributes returned in
      Metadata containers by the find and find_one method will all have the
      collection name prepended with a (fixed) separator.  For example, if
      the collection name is "channel" the "lat" attribute in the channel
      document would be returned as "channel_lat".
    :type prepend_collection_name:  boolean

    :param require_unique_match:  boolean handling of ambiguous matches.
      When True find_one will throw an error if an entry is tries to match
      is not unique.  When False find_one returns the first document
      found and logs a complaint message.  (default is False)
    :type require_unique_match:  boolean
    """

    def __init__(
        self,
        db,
        collection="source",
        t0offset=0.0,
        tolerance=4.0,
        query=None,
        attributes_to_load=["_id", "lat", "lon", "depth", "time"],
        load_if_defined=["magnitude"],
        aliases=None,
        require_unique_match=False,
        prepend_collection_name=True,
        data_time_key=None,
        source_time_key=None,
    ):
        super().__init__(
            db,
            collection,
            attributes_to_load=attributes_to_load,
            load_if_defined=load_if_defined,
            aliases=aliases,
            require_unique_match=require_unique_match,
            prepend_collection_name=prepend_collection_name,
        )
        self.t0offset = t0offset
        self.tolerance = tolerance
        self.data_time_key = data_time_key
        self.source_time_key = source_time_key
        if query is None:
            query = {}
        if isinstance(query, dict):
            self.query = query
        else:
            raise TypeError(
                "{} constructor:  query argument must define a python dictionary or a None: received invalid type.  See docstring".format(
                    self.__class__.__name__
                )
            )

    def query_generator(self, mspass_object) -> dict:
        """
        Concrete implementation of this required method for a subclass of
        DatabaseMatcher.

        This algorithm implements the time test described in detail in
        docstring for this class.  Note the fundamental change in how the
        test time is computed that depends on the internal (self)
        attribute time_key.  When None we use the data's t0 attribute.
        Otherwise self.time_key is assumed to be a string key to
        fetch the test time from the object's Metadata container.

        :param mspass_object:   MsPASS defined data object that contains
          data to be used for this match (t0 attribute or content of
          self.time_key).
        :type mspass_object:  Any valid MsPASS data object.

        :return:  query python dictionary on sucess.  Return None if
          a query could not be constructed.  That happens two ways here.
          (1) If the input is not a valid mspass data object or marked dead.
          (2) if the time_key algorithm is used and time_key isn't defined
              in the input datum.
        """
        # This could generate mysterious results if a user messes up
        # badly, but  it makes the code more stable - otherwise
        # a parallel job could, for example, abort if one of the
        # components in a bag/rdd got set to None
        if not isinstance(mspass_object, Metadata):
            return None
        if hasattr(mspass_object, "dead"):
            if mspass_object.dead():
                return None

        if self.data_time_key is None:
            # this maybe should have a test to assure UTC time standard
            # but will defer for now
            test_time = mspass_object.t0
        else:
            if mspass_object.is_defined(self.data_time_key):
                test_time = mspass_object[self.data_time_key]
            else:
                return None
        test_time -= self.t0offset

        # depends upon self.query being initialized by constructor
        # as python dictionary
        query = copy.deepcopy(self.query)

        query["time"] = {
            "$gte": test_time - self.tolerance,
            "$lte": test_time + self.tolerance,
        }

        return query


class OriginTimeMatcher(DataFrameCacheMatcher):
    """
    Generic class to match data by comparing a time defined in data to
    an origin time using a cached DataFrame.

    The default behavior of this matcher class is to match data to
    source documents based on origin time with an optional time offset.
    Conceptually the data model for this matching is identical to conventional
    multichannel shot gathers where the start time is usually the origin time.
    It is also a common model for downloaded source oriented waveform
    segments from FDSN web services with obspy.  Obspy has an example
    in their documentation for how to download data defined exactly this way.
    In that mode we match each source document that matches a projected
    origin time within a specified tolerance. Specifically, let t0
    be the start time extracted from the data.  We then compute the
    projected, test origin time as test_otime = t0 - t0offset.
    Note the sign convention that a positive offset means the time t0
    is after the event origin time.   We then select all source
    records for which the time field satisifies:
        source.time - tolerance <= test_time <= source.time + tolerance

    The test_time value for matching from a datum can come through
    one of two methods driven by the constructor argument "time_key".
    When time_key is a None (default) the algorithm assumes all input
    are mspass atomic data objects that have the start time defined by
    the attribute "t0" (mspass_object.t0).  If time_key is a string
    it is assumed to be a Metadata key used to fetch an epoch time
    to use for the test.   The most likely use of that feature would be
    for ensemble processing where test_time is set as a field in the
    ensemble Metadata.  Note that form of associating source data to
    ensembles that are common source gathers can be much faster than
    the atomic version because only one query is needed per ensemble.

    This implentation should be used only if the catalog of events
    is reasonably small.  If the catalog is huge the database version
    may be more appropriate.

    :param db:  MongoDB database handle  (positional - no default)
    :type db: normally a MsPASS Database class but with this algorithm
      it can be the superclass from which Database is derived.

    :param collection:  Name of MongoDB collection that is to be queried
       (default "source").
    :type collection: string

    :param t0offset: constant offset from data start time that is expected
      as origin time.  A positive t0offset means the origin time is before
      the data start time.  Units are always assumed to be seconds.
    :type t0offset:  float

    :param tolerance:   time tolerance to test for match of origin time.
      (see formula above for exact use)
      If the source estimates are exactly the same as the ones used to
      define data start time this number can be a few samples.
      Otherwise a few seconds is safter for teleseismic data and
      less for local/regional events.  i.e. the choice depends up on
      how the source estimates relate to the data.
    :type tolerance:  float


    :param attributes_to_load:  list of keys of required attributes that will
      be returned in the output of the find method.   The keys listed
      must ALL have defined values for all documents in the collection or
      some calls to find_one will fail.   Default is
      ["_id","lat","lon","depth","time"].  Note if constructing from a
      DataFrame created from something like a Datascope origin table
      this list will need to be changed to remove _id as it in that context
      no ObjectID would normally be defined.  Be warned, however, that if
      used with a normalize function the _id may be required to match a
      "source_id" cross reference in a seismic data object.  Also note
      that the list must contain the key defined by the related
      argument "source_time_key" as that is used to match times in
      the source data with data start times.
    :type attributes_to_load:  list of string defining keys in collection
      documents

    :param load_if_defined: list of keys of optional attributes to be
      extracted by find method.  Any data attached to these keys will only
      be posted in the find return if they are defined in the database
      document retrieved in the query.  Default is ["magnitude"]
    :param type:  list of strings defining collection keys

    :param aliases:  python dictionary defining alias names to apply
     when fetching from a data object's Metadata container.   The key sense
     of the mapping is important to keep straight.  The key of this
     dictionary should match one  of the attributes in attributes_to_load
     or load_if_defined.  The value the key defines should be the alias
     used to fetch the comparable attribute from the data.
    :type aliaes:  python dictionary

    :param prepend_collection_name:  when True attributes returned in
      Metadata containers by the find and find_one method will all have the
      collection name prepended with a (fixed) separator.  For example, if
      the collection name is "channel" the "lat" attribute in the channel
      document would be returned as "channel_lat".
    :type prepend_collection_name:  boolean

    :param require_unique_match:  boolean handling of ambiguous matches.
      When True find_one will throw an error if an entry is tries to match
      is not unique.  When False find_one returns the first document
      found and logs a complaint message.  (default is False)
    :type require_unique_match:  boolean

    :param data_time_key:  data object Metadata key used to fetch
      time for testing as alternative to data start time.  If set None
      (default) the test will use the start time of an atomic data object
      for the time test.  If nonzero it is assumed to be a string used
      to fetch a time from the data's Metadata container.  That is the
      best way to run this matcher on Ensembles.
    :type data_time_key:  string

    :param source_time_key:  dataframe column name to use as source
      origin time field.  Default is "time".   This key must match
      a key in the attributes_to_load list or the constructor will
      throw an exception.  Note this should match the key definingn
      origin time in the collection not the common actual value
      stored with data.  I.e. normal usage is "time" not "source_time"
    :type source_time_key:  string  Can also be a None type which
      is causes the internal value to be set to "time"
    """

    def __init__(
        self,
        db_or_df,
        collection="source",
        t0offset=0.0,
        tolerance=4.0,
        attributes_to_load=["_id", "lat", "lon", "depth", "time"],
        load_if_defined=["magnitude"],
        aliases=None,
        require_unique_match=False,
        prepend_collection_name=True,
        data_time_key=None,
        source_time_key="time",
        custom_null_values=None,
    ):
        super().__init__(
            db_or_df,
            collection,
            attributes_to_load=attributes_to_load,
            load_if_defined=load_if_defined,
            aliases=aliases,
            require_unique_match=require_unique_match,
            prepend_collection_name=prepend_collection_name,
            custom_null_values=custom_null_values,
        )
        self.t0offset = t0offset
        self.tolerance = tolerance
        self.data_time_key = data_time_key
        if source_time_key is None:
            self.source_time_key = "time"
        else:
            self.source_time_key = source_time_key
        if self.source_time_key not in attributes_to_load:
            message = "OriginTimeMatcher constructor:  "
            message += "key for fetching origin time=" + self.source_time_key
            message += " is not in attributes_to_load list\n"
            message += "Required for matching with waveform start times"
            raise MsPASSError(message, ErrorSeverity.Fatal)

    def subset(self, mspass_object) -> pd.DataFrame:
        """
        Implementation of subset method requried by inheritance from
        DataframeCacheMatcher.   Returns a subset of the cache
        Dataframe with source origin times matching the definition
        of this object.  i.e. a time interval relative to the
        start time defined by mspass_object.   Note that if a key is
        given the time will be extrated from the Metadata container of
        mspass_object.  If no key is defined (self.data_time_key == None)
        the t0 attribute of mspass_object will be used.
        """
        if not isinstance(mspass_object, Metadata):
            return pd.DataFrame()
        if hasattr(mspass_object, "dead"):
            if mspass_object.dead():
                return pd.DataFrame()

        if self.data_time_key is None:
            # this maybe should have a test to assure UTC time standard
            # but will defer for now
            test_time = mspass_object.t0
        else:
            if mspass_object.is_defined(self.data_time_key):
                test_time = mspass_object[self.data_time_key]
            else:
                return pd.DataFrame()
        test_time -= self.t0offset

        tmin = test_time - self.tolerance
        tmax = test_time + self.tolerance
        # For this matcher we dogmatically use <= equivalent in the between
        # construct here - inclusive=True.  In this context seems appropriate
        inclusive = '"both"'
        dfquery = (
            self.source_time_key
            + ".between({tmin},{tmax},inclusive={inclusive})".format(
                tmin=tmin, tmax=tmax, inclusive=inclusive
            )
        )
        dfret = self.cache.query(dfquery)

        return dfret

    def find_one(self, mspass_object) -> tuple:
        """
        Override of find_one method of DataframeMatcher.  The override is
        necessary to handle the ambiguity of a timer interval match for
        source origin times.   That is, there is a finite probability
        that tow earthquakes can occur with the interval of this matcher
        defined by the time projected from the waveform start time
        (starttime - self.t0offset) + or - self.tolerance.   When
        multiple matches are found this method handles that ambiguity by
        finding the source where the origin time is closest to the
        waveform start time corrected by self.t0offset.

        Note this method normally expects input to be an atomic
        seismic object.  It also, however, accepts any object that
        is a subclass of Metadata.   The most important example of that
        is `TimeSeriesEnsemble` and `SeismogramEnsemble` objects.
        For that to work, however, you MUST define a key to use to
        fetch a reference time in the constructor to this object
        via the `data_time_key` argument.  If you then load the
        appropriate reference time in the ensemble's Metadata container
        you can normalize a common source gather's ensemble container
        with a workflow.   Here is a code fragment illustrating the idea:

        ```
        source_matcher = OriginTimeMatcher(db,data_time_key="origin_time")
        e = db.read_data(cursor, ... read args...)  # read ensemle e
        # assume we got ths time (otime)vsome other way above
        e['origin_time'] = otime
        e = normalize(e,source_matcher)
        ```
        If the match suceeds the attributes defined in te Dataframe
        cache will be loaded into the Metadata contaienr of e.
        That is the defiition of a common source gather.

        :param mspass_object:  atomic seismic data object to be matched.
           The match is normally made against the datum's t0 value so
           there is an implict assumption the datum is a UTC epoch time.
           If a data set is passed through this operator and the data
           are relative time all will fail.   The function intentionaly
           avoids that test for efficiency.   A plain Metadata container
           can be passed through mspass_object if and only if it
           contains a value associated with the key defined by the
           starttime_key attibute.

        :return: a tuple consistent with the BasicMatcher API definition.
          (i.e. pair [Metadata,ErrorLogger])
        """
        findreturn = self.find(mspass_object)
        mdlist = findreturn[0]
        if mdlist is None:
            return findreturn
        elif len(mdlist) == 1:
            md2use = mdlist[0]
        elif len(mdlist) > 1:
            md2use = self._nearest_time_source(mspass_object, mdlist)
        return [md2use, findreturn[1]]

    def _nearest_time_source(self, mspass_object, mdlist):
        """
        Private method to define the algorithm used to resolve
        an ambiguity when multipe sources are returned by find.
        This returns the Metadata container for the source
        most whose offset origin time most closely matches te
        content defined my mspass_object.
        """
        if self.prepend_collection_name:
            # the find method returns modified names if
            # prepend_collection_names is True.  Note the
            # actual DAtaframe uses the names without thae prepend string
            # This is needed to handle that property of find
            time_key = self.collection + "_" + self.source_time_key
        else:
            time_key = self.source_time_key
        N_matches = len(mdlist)
        # find component of list with the minimum projected time offset
        dt = np.zeros(N_matches)
        i = 0
        for md in mdlist:
            dt[i] = md[time_key]
            i += 1
        # always use t0 if possile.
        # this logic, however, allows mspass_object to be a
        # plain Metadata container or a python dictionary
        # intentinally let this throw an exception for Metadata if the
        # required key is missing.  If t0 is not defined it tries to
        # use self.data_time_key (normaly "startttme")
        if hasattr(mspass_object, "t0"):
            test_time = mspass_object.t0
        else:
            test_time = mspass_object[self.data_time_key]
        test_time -= self.t0offset
        dt -= test_time
        dt = np.abs(dt)
        component_to_use = np.argmin(dt)
        return mdlist[component_to_use]

    def find_doc(self, doc, starttime_key="starttime") -> dict:
        """
        Override of the find_doc method of BasicMatcher.   This method
        acts lke find_one but the inputs and outputs are different.
        The input to this method is a python dictionary that is
        expected to normally be a MongoDB document.   The output is
        also a python dictionary without (normally) a reduced set of
        attributes defined by self.attributes_to_load and
        self.load_if_defined.  We need to override the base class
        version of ths method because the base class version by
        default requires an atomic seismic data object
        (TimeSEries or Seismogram).  The algorithm used is a variant of
        that in the subset method of this class.

        This method also differs from find_one it that it has no
        mechanism to log errors.   find_one returns a Metadata container
        and an ErrorLogger container used to post messages.  This method
        will return a None if there are errors that cause it to fail.
        That can be ambiguous because a None return also is used to
        indicate failure to match anything.  The primary use of this
        method is normalizing an entire data set with the ObjetIds of
        source documnts with the `bulk_normaize` function.   In that case
        additional forensic work is possible with MongoDB to uncover why
        a given document match failed.

        Because the interval match relative to a waveform start time can
        be ambiguous from global events (Although rare earthquakes can
        easily occur with + or - self.tolerance time) when multiple
        rows of the dataframe match the interval test the one returned
        is the one for which the time projected from the waveform
        start time (uses self.t0offset value) is defined as the match
        that is returned.

        :param doc:  wf document (i.e. a document used to construct
          an atomic datum) to be matched with content of this object
          (assued the source collection or a variant that contains
          source origin times).
        :type doc:  python dictionary
        :param starttime_key:  key that can be used fetch the waveform
          segment start time that is to be used to match against
          origin times loaded in the object's cache.  '
        :type starttime_key:  str (default "starttime")
        :return:  python dictionary of the best match or None if there is
          no match or in nonfatal error conditions.
        """
        if starttime_key in doc:
            test_time = doc[starttime_key]
            test_time -= self.t0offset
            # copied from subset method
            tmin = test_time - self.tolerance
            tmax = test_time + self.tolerance
            # For this matcher we dogmatically use <= equivalent in the between
            # construct here - inclusive=True.  In this context seems appropriate
            inclusive = '"both"'
            dfquery = (
                self.source_time_key
                + ".between({tmin},{tmax},inclusive={inclusive})".format(
                    tmin=tmin, tmax=tmax, inclusive=inclusive
                )
            )
            subset_df = self.cache.query(dfquery)
            N_matches = len(subset_df)
            if N_matches <= 0:
                # no match return
                return None
            elif N_matches > 1:
                # first find the row with source origin time most closely
                # matching the doc starrtime value
                dt = np.zeros(N_matches)
                i = 0
                for index, row in subset_df.iterrows():
                    # this key has to exist or we wouldn't get here
                    dt[i] = row[self.source_time_key]
                dt -= test_time
                dt = np.abs(dt)
                row_index_to_use = np.argmin(dt)
            else:
                row_index_to_use = 0
            row = subset_df.iloc[row_index_to_use]
            doc_out = dict()
            notnulltest = row.notnull()
            for k in self.attributes_to_load:
                if notnulltest[k]:
                    if k in self.aliases:
                        key = self.aliases[k]
                    else:
                        key = k
                    if self.prepend_collection_name:
                        if key == "_id":
                            mdkey = self.collection + key
                        else:
                            mdkey = self.collection + "_" + key
                    else:
                        mdkey = key
                    doc_out[mdkey] = row[key]
                else:
                    # land here if a required attribute is missing
                    # from the dataframe cache.  find logs
                    # an error but all we can do here is flag
                    # failure returning None.   There is a rare
                    # possibilit of this failing with multiple
                    # source documents where one is bad and the other
                    # is not
                    return None

                for k in self.load_if_defined:
                    if notnulltest[k]:
                        if k in self.aliases:
                            key = self.aliases[k]
                        else:
                            key = k
                        if self.prepend_collection_name:
                            if key == "_id":
                                mdkey = self.collection + key
                            else:
                                mdkey = self.collection + "_" + key
                        else:
                            mdkey = key
                    doc_out[mdkey] = row[key]
            return doc_out
        else:
            return None


class ArrivalDBMatcher(DatabaseMatcher):
    """
    This is a class for matching a table of arrival times to input
    waveform data objects.  Use this version if the table of arrivals
    is huge and database query delays will not create a bottleneck
    in your workflow.

    Phase arrival time matching is a common need when waveform segments
    are downloaded.  When data are assembled as miniseed files or
    url downloads of miniseed data, the format has no way to hold
    arrival time data.  This matcher can prove useful for matching
    waveform segments with an origin as miniseed.

    The algorithm it uses for matching is a logic and of two tests:
        1.  We first match all arrival times falling between the
            sample range of an input MsPASS data object, d.  That is,
            first component of the query is to find all arrival times,
            t_a, that obey the relation:  d.t0 <= t_a <= d.endtime().
        2.  Match only data for which the (fixed) name "sta"
            in arrival and the data match.   A secondary key match using
            the "net" attribute is used only if "net" is defined with
            the data.  That is done to streamline processing of css3.0
            data where "net"  is not defined.

    Note the concept of an arrival time is also mixed as in
    some contexts it means a time computed from an earth model and other
    time a measured time that is "picked" by a human or computer algorithm.
    This class does not distinguish model-based from measured times.  It
    simply uses the time and station tag information with the algorithm
    noted above to attempt a match.

    :param db:  MongoDB database handle  (positional - no default)
    :type db: normally a MsPASS Database class but with this algorithm
      it can be the superclass from which Database is derived.

    :param collection:  Name of MongoDB collection that is to be queried
       (default "arrival", which is not currently part of the stock
        mspass schema.   Note it isn't required to be in the schema
        and illustrates flexibility').
    :type collection: string

    :param attributes_to_load:  list of keys of required attributes that will
      be returned in the output of the find method.   The keys listed
      must ALL have defined values for all documents in the collection or
      some calls to find_one will fail.
      Default ["phase","time"].
    :type attributes_to_load:  list of string defining keys in collection
      documents

    :param load_if_defined: list of keys of optional attributes to be
      extracted by find method.  Any data attached to these keys will only
      be posted in the find return if they are defined in the database
      document retrieved in the query.  Default is None
    :param type:  list of strings defining collection keys

    :param aliases:  python dictionary defining alias names to apply
     when fetching from a data object's Metadata container.   The key sense
     of the mapping is important to keep straight.  The key of this
     dictionary should match one  of the attributes in attributes_to_load
     or load_if_defined.  The value the key defines should be the alias
     used to fetch the comparable attribute from the data.
    :type aliaes:  python dictionary

    :param prepend_collection_name:  when True attributes returned in
      Metadata containers by the find and find_one method will all have the
      collection name prepended with a (fixed) separator.  For example, if
      the collection name is "channel" the "lat" attribute in the channel
      document would be returned as "channel_lat".
    :type prepend_collection_name:  boolean

    :param require_unique_match:  boolean handling of ambiguous matches.
      When True find_one will throw an error if an entry is tries to match
      is not unique.  When False find_one returns the first document
      found and logs a complaint message.  (default is False)
    :type require_unique_match:  boolean

    :param query:   optional query predicate.  That is, if set the
      interval query is appended to this query to build a more specific
      query.   An example might be station code keys to match a
      specific pick for a specific station like {"sta":"AAK"}.
      Another would be to limit arrivals to a specific phase name
      like {"phase" : "ScS"}.  Default is None which reverts to no
      query predicate.
    :type query:  python dictionary or None.  None is equivalewnt to
      passing an empty dictionary.  A TypeError will be thrown if this
      argument is not None or a dict.
    """

    def __init__(
        self,
        db,
        collection="arrival",
        attributes_to_load=["phase", "time"],
        load_if_defined=None,
        aliases=None,
        require_unique_match=False,
        prepend_collection_name=True,
        query=None,
    ):
        super().__init__(
            db,
            collection,
            attributes_to_load=attributes_to_load,
            load_if_defined=load_if_defined,
            aliases=aliases,
            require_unique_match=require_unique_match,
            prepend_collection_name=prepend_collection_name,
        )

        if query is None:
            self.query = dict()
        elif isinstance(query, dict):
            self.query = query
        else:
            raise TypeError(
                "ArrivalDBMatcher constructor:  query arg must define a python dictionary"
            )

    def query_generator(self, mspass_object) -> dict:
        """
        Concrete implementation of method required by superclass
        DatabaseMatcher.

        This generator implements the switching algorithm noted in the
        class docstring.  That is, for atomic data the time span for
        the interval query is determined from the range of the waveform
        data received through mspass_object.   For ensembles the
        algorithm fetches fields defined by self.startime_key and
        self.endtime_key to define the time interval.

        The interval test is overlaid on the self.query input.  i.e.
        the query dict components derived are added to the self.query.
        """

        if _input_is_atomic(mspass_object):
            if mspass_object.live:
                query = copy.deepcopy(self.query)
                stime = mspass_object.t0
                etime = mspass_object.endtime()
                query["time"] = {"$gte": stime, "$lte": etime}
                # these names are frozen
                sta = _get_with_readonly_recovery(mspass_object, "sta")
                net = _get_with_readonly_recovery(mspass_object, "net")
                if net is not None:
                    query["net"] = net
                if sta is not None:
                    query["sta"] = sta
                # intentionally ignore loc as option
                return query
        else:
            return None


class ArrivalMatcher(DataFrameCacheMatcher):
    """
    This is a class for matching a table of arrival times to input
    waveform data objects.  Use this version if the table of arrivals
    is not huge enough to cause a memory problem.

    Phase arrival time matching is a common need when waveform segments
    are downloaded.  When data are assembled as miniseed files or
    url downloads of miniseed data, the format has no way to hold
    arrival time data.  This matcher can prove useful for matching
    waveform segments with an origin as miniseed.

    The algorithm it uses for matching is a logic and of two tests:
        1.  We first match all arrival times falling between the
            sample range of an input MsPASS data object, d.  That is,
            first component of the query is to find all arrival times,
            t_a, that obey the relation:  d.t0 <= t_a <= d.endtime().
        2.  Match only data for which the (fixed) name "sta"
            in arrival and the data match.   A secondary key match using
            the "net" attribute is used only if "net" is defined with
            the data.  That is done to streamline processing of css3.0
            data where "net"  is not defined.

    Note the concept of an arrival time is also mixed as in
    some contexts it means a time computed from an earth model and other
    time a measured time that is "picked" by a human or computer algorithm.
    This class does not distinguish model-based from measured times.  It
    simply uses the time and station tag information with the algorithm
    noted above to attempt a match.

    This implementation caches the table of attributes desired to an
    internal pandas DataFrame.   It is thus most appropriate for
    arrival tables that are not huge.  Note it may be possible to
    do appropriate preprocessing to manage the arrival table size.
    e.g. the table can be grouped by station or in time blocks and
    then processed in a loop updating waveform database records
    in multiple passes.  The alternative for large arrival tables
    is to use the DB version of this matcher.

    :param db:  MongoDB database handle  (positional - no default)
    :type db: normally a MsPASS Database class but with this algorithm
      it can be the superclass from which Database is derived.

    :param collection:  Name of MongoDB collection that is to be queried
       (default "arrival", which is not currently part of the stock
        mspass schema.   Note it isn't required to be in the schema
        and illustrates flexibility').
    :type collection: string

    :param attributes_to_load:  list of keys of required attributes that will
      be returned in the output of the find method.   The keys listed
      must ALL have defined values for all documents in the collection or
      some calls to find_one will fail.
      Default ["phase","time"].
    :type attributes_to_load:  list of string defining keys in collection
      documents

    :param load_if_defined: list of keys of optional attributes to be
      extracted by find method.  Any data attached to these keys will only
      be posted in the find return if they are defined in the database
      document retrieved in the query.  Default is None
    :param type:  list of strings defining collection keyes

    :param aliases:  python dictionary defining alias names to apply
       when fetching from a data object's Metadata container.   The key sense
       of the mapping is important to keep straight.  The key of this
       dictionary should match one  of the attributes in attributes_to_load
       or load_if_defined.  The value the key defines should be the alias
       used to fetch the comparable attribute from the data.
    :type aliaes:  python dictionary

    :param prepend_collection_name:  when True attributes returned in
      Metadata containers by the find and find_one method will all have the
      collection name prepended with a (fixed) separator.  For example, if
      the collection name is "channel" the "lat" attribute in the channel
      document would be returned as "channel_lat".
    :type prepend_collection_name:  boolean

    :param require_unique_match:  boolean handling of ambiguous matches.
      When True find_one will throw an error if an entry is tries to match
      is not unique.  When False find_one returns the first document
      found and logs a complaint message.  (default is False)
    :type require_unique_match:  boolean

    :param ensemble_starttime_key:  defines the key used to fetch a
       start time for the interval test when processing with ensemble data.
       Default is "starttime".
    :type ensemble_starttime_key:  string

    :param ensemble_endtime_key:  defines the key used to fetch a
       end time for the interval test when processing with ensemble data.
       Default is "endtime".
    :type ensemble_endtime_key:  string

    :param query:   optional query predicate.  That is, if set the
      interval query is appended to this query to build a more specific
      query.   An example might be station code keys to match a
      specific pick for a specific station like {"sta":"AAK"}.
      Default is None.
    :type query:  python dictionary or None.  None is equivalewnt to
      passing an empty dictionary.  A TypeError will be thrown if this
      argument is not None or a dict.
    """

    def __init__(
        self,
        db_or_df,
        collection="arrival",
        attributes_to_load=["phase", "time"],
        load_if_defined=None,
        aliases=None,
        require_unique_match=False,
        prepend_collection_name=True,
        ensemble_starttime_key="starttime",
        ensemble_endtime_key="endtime",
        arrival_time_key=None,
        custom_null_values=None,
    ):
        super().__init__(
            db_or_df,
            collection,
            attributes_to_load=attributes_to_load,
            load_if_defined=load_if_defined,
            aliases=aliases,
            require_unique_match=require_unique_match,
            prepend_collection_name=prepend_collection_name,
            custom_null_values=custom_null_values,
        )
        # maybe a bit confusing to shorten the names here but the
        # argument names are a bit much
        self.starttime_key = ensemble_starttime_key
        self.endtime_key = ensemble_endtime_key
        if arrival_time_key is None:
            self.arrival_time_key = collection + "_time"
        elif isinstance(arrival_time_key, str):
            self.arrival_time_key = arrival_time_key
        else:
            raise TypeError(
                "ArrivalDBMatcher constructor: arrival_time_key argument must define a string"
            )

    def subset(self, mspass_object) -> pd.DataFrame:
        """
        Concrete implementation of method required by superclass
        DataFramematcher
        """

        if isinstance(mspass_object, Metadata):
            if mspass_object.live:
                if _input_is_atomic(mspass_object):
                    stime = mspass_object.t0
                    etime = mspass_object.endtime()
                else:
                    if mspass_object.is_defined(
                        self.starttime_key
                    ) and mspass_object.is_defined(self.endtimekey):
                        stime = mspass_object[self.starttime_key]
                        etime = mspass_object[self.endtime_key]
                    else:
                        return pd.DataFrame()
                sta = _get_with_readonly_recovery(mspass_object, "sta")
                if sta is not None:
                    dfret = self.cache[
                        ("sta" == sta)
                        & (self.arrival_time_key >= stime)
                        & (self.arrival_time_key <= etime)
                    ]
                    if len(dfret) > 1:
                        net = _get_with_readonly_recovery(mspass_object, "net")
                        if net is not None:
                            dfret = dfret["net" == net]
                    return dfret
            else:
                return pd.DataFrame()
        else:
            return pd.DataFrame()


@mspass_func_wrapper
def normalize(
    mspass_object,
    matcher,
    *args,
    kill_on_failure=True,
    handles_ensembles=True,
    checks_arg0_type=False,
    handles_dead_data=True,
    **kwargs,
):
    """
    Generic function to do in line normalization with dask/spark map operator.

    In MsPASS we use the normalized data model for receiver and source
    metadata.  The normalization can be done during any reads if the
    data have cross-referencing ids defined as described in the User's Manual.
    This function provides a generic interface to link to a normalizing
    collection within a workflow using a map operator applied to a dask
    bag or spark rdd containing a dataset of MsPASS data objects.
    The algorithm is made generic through the matcher argument that must
    point a concrete implementation of the abstract base class
    defined in this module as BasicMatcher.

    For example, suppose we create a concrete implementation of the
    MiniseedMatcher using all defaults from a database handle db as
    follows:
        matcher = MiniseedMatcher()
    Suppose we then load data from wf_miniseed with read_distributed_data
    into the dask bag we will call dataset.  We can normalize
    that data within a workflow as follows:
        dataset = dataset.map(normalize,matcher)

    An important warning about ensembles. If the function receives an
    ensemble by default it will enter a loop to apply the normalization
    to all "member" objects of the ensemble.   If you need to insert
    the normalizatio parameters into the ensemble's Metadata container
    instead of all the members set `handles_ensembles=True`.

    :param mspass_object:  data to be normalized
    :type mspass_object:  For all mspass matchers this
      must be one of the mspass data types of TimeSeries, Seismogram,
      TimeSeriesEnsemble, or SeismogramEnsemble.  Many matchers have
      further restrictions.  e.g. the normal use of the MiniseedMatcher
      using the defaults like the example insists the data received are
      either TimeSeries or Seismogram objects.   Read the docstring
      carefully for your matcher choice for any limitations.

    :param matcher:  a generic matching function that is a subclass of
      BasicMatcher.   This function only calls the find_one method.
    :type matcher:  must be a concrete subclass of BasicMatcher

    :param kill_on_failure:  when True if the call to the find_one
      method of matcher fails the datum returned will be marked dead.
    :type kill_on_failure:  boolean

    :param handles_ensembles:  boolean controlling how the function is
      applied with ensembles - see above.

    :return:  copy of mspass_object.  dead data are returned immediately.
      if kill_on_failure is true the result may be killed on return.
    """
    if hasattr(mspass_object, "dead"):
        if mspass_object.dead():
            return mspass_object
    find_output = matcher.find_one(mspass_object)
    # api of BasicMatcher specified a pair return we handle here
    if find_output[0] is None:
        mspass_object.kill()
    else:
        # this could be done with operator+= in C++ with appropriate
        # casting but I think this is the only solution here
        # we are just copying the return Metadata contents to the data
        for k in find_output[0]:
            mspass_object[k] = find_output[0][k]

    # append any error log returns to the data elog
    # operator += is bound to python by pybind11 so this works
    if find_output[1] is not None:
        mspass_object.elog += find_output[1]
    return mspass_object


def bulk_normalize(
    db,
    wfquery=None,
    wf_col="wf_miniseed",
    blocksize=1000,
    matcher_list=None,
):
    """
    This function iterates through the collection specified by db and wf_col,
    and runs a chain of normalization funtions in serial on each document
    defined by the cursor returned by wfquery.  It speeds updates by
    using the bulk methods of MongoDB.  The chain also speeds updates
    as the all matchers in matcher_list append to the update string
    for the same wf_col document.   A typical example would be to
    run this function on wf_miniseed data running a matcher to set
    channel_id, site_id, and source_id.

    :param db: should be a MsPASS database handle containing the wf_col
      and the collections defined by the matcher_list list.
    :param wf_col: The collection that need to be normalized, default is
      wf_miniseed
    :param blockssize:   To speed up updates this function uses the
      bulk writer/updater methods of MongoDB that can be orders of
      magnitude faster than one-at-a-time updates. A user should not normally
      need to alter this parameter.
    :param wfquery: is an optional query to apply to wf_col.  The output of this
      query defines the list of documents that the algorithm will attempt
      to normalize as described above.  The default (None) will process the entire
      collection (query set to an emtpy dict).
    :param matcher_list: a list of instances of one or more subclasses of BasicMather.
      In addition to the required classes all instances passed to through
      this interface must contain two required attributes:  (1) collection
      which defines the collection name, and (2) prepend_collection_name is
      a boolean that determines if the attributes loaded should have
      the collection name prepended (e.g. channel_id).  In addition,
      all instances must define the find_doc method which is not required
      by the BasicMatcher interface.   (find_doc is comparable to find_one
      but uses a python dictionary as the container instead of referencing
      a mspass data object.  find_one is the core method for inline normalization)


    :return: a list with a length of len(matcher_list)+1.  0 is the number of documents
      processed in the collection (output of query), The rest are the numbers of
      success normalizations for the corresponding NMF instances, they are mapped
      one on one (matcher_list[x] -> ret[x+1]).
    """

    if wfquery is None:
        wfquery = {}

    if matcher_list is None:
        #   The default value for matcher_list is for wf_miniseed with channel
        # Assume the defaults are sufficient but we limit the required
        # attribute list to save memory
        channel_matcher = MiniseedMatcher(
            db, attributes_to_load=["starttime", "endtime", "_id"]
        )
        matcher_list = [channel_matcher]

    for matcher in matcher_list:
        if not isinstance(matcher, BasicMatcher):
            raise MsPASSError(
                "bulk_normalize: the component in the matcher list={} is not a subclass of BasicMatcher".format(
                    str(matcher)
                ),
                ErrorSeverity.Fatal,
            )
        # check that matcher has the find_doc method  implemented - it is
        # not defined in the BasicMatcher interface and is required
        if not (
            hasattr(matcher, "find_doc") and callable(getattr(matcher, "find_doc"))
        ):
            message = "matcher_list contains class={classname} that does not have an implementation of the find_doc method required by this function - try using it in a map operator".format(
                classname=type(matcher).__name__
            )
            raise MsPASSError("bulk_normalize:  " + message, ErrorSeverity.Fatal)
        # these two attributes are also required and best checked here
        # for a minor cost
        if not hasattr(matcher, "prepend_collection_name"):
            message = "matcher list class={classname} does not define required attribute prepend_collection_name - trying using it in a map operator".format(
                classname=type(matcher).__name__
            )
            raise MsPASSError("bulk_normalize:  " + message, ErrorSeverity.Fatal)
        if not hasattr(matcher, "collection"):
            message = "matcher list class={classname} does not define required attribute collection - trying using it in a map operator".format(
                classname=type(matcher).__name__
            )
            raise MsPASSError("bulk_normalize:  " + message, ErrorSeverity.Fatal)
    ndocs = db[wf_col].count_documents(wfquery)
    if ndocs == 0:
        raise MsPASSError(
            "bulk_normalize: "
            + "query={wfquery} of collection={wf_col} yielded 0 documents\nNothing to process".format(
                wfquery=wfquery, wf_col=wf_col
            ),
            ErrorSeverity.Fatal,
        )

    # this incantation initializes cnt_list as a list with number of
    # components set as the size of matcher_list and initialized to 0
    cnt_list = [0] * len(matcher_list)
    counter = 0

    cursor = db[wf_col].find(wfquery)
    bulk = []
    for doc in cursor:
        wf_id = doc["_id"]
        need_update = False
        update_doc = {}
        for ind, matcher in enumerate(matcher_list):
            norm_doc = matcher.find_doc(doc)
            if norm_doc is None:
                # not this silently ignores failures
                # may want this to count failures for each matcher
                continue
            for key in matcher.attributes_to_load:
                new_key = key
                if matcher.prepend_collection_name:
                    if key == "_id":
                        new_key = matcher.collection + key
                    else:
                        new_key = matcher.collection + "_" + key
                update_doc[new_key] = norm_doc[new_key]

            cnt_list[ind] += 1
            need_update = True

        if need_update:
            bulk.append(pymongo.UpdateOne({"_id": wf_id}, {"$set": update_doc}))
            counter += 1
        # Tests for counter and len(bulk) are needed because the logic here
        # allows this block to be entered the first pass and if the pass
        # after the previous call to bulk_write did not yield a match
        # either will cause bulk_write to throw an error when it gets an
        # an empty list.   Should consider a logic  change here
        # to make this less obscure
        if counter % blocksize == 0 and counter != 0 and len(bulk) > 0:
            db[wf_col].bulk_write(bulk)
            bulk = []

    if counter % blocksize != 0:
        db[wf_col].bulk_write(bulk)

    return [ndocs] + cnt_list


def normalize_mseed(
    db,
    wfquery=None,
    blocksize=1000,
    normalize_channel=True,
    normalize_site=True,
):
    """
    In MsPASS the standard support for station information is stored in
    two collections called "channel" and "site".   When normalized
    with channel collection data a miniseed record can be associated with
    station metadata downloaded by FDSN web services and stored previously
    with MsPASS database methods.   The default behavior tries to associate
    each wf_miniseed document with an entry in "site".  In MsPASS site is a
    smaller collection intended for use only with data already assembled
    into three component bundles we call Seismogram objects.


    For both channel and site the association algorithm used assumes
    the SEED convention wherein the strings stored with the keys
    "net","sta","chan", and (optionally) "loc" define a unique channel
    of data registered globally through the FDSN.   The algorithm then
    need only query for a match of these keys and a time interval
    match with the start time of the waveform defined by each wf_miniseed
    document.   The only distinction in the algorithm between site and
    channel is that "chan" is not used in site since by definition site
    data refer to common attributes of one seismic observatory (commonly
    also called a "station").

    :param db: should be a MsPASS database handle containing at least
      wf_miniseed and the collections defined by the norm_collection list.
    :param blockssize:   To speed up updates this function uses the
      bulk writer/updater methods of MongoDB that can be orders of
      magnitude faster than one-at-a-time updates for setting
      channel_id and site_id.  A user should not normally need to alter this
      parameter.
    :param wfquery: is a query to apply to wf_miniseed.  The output of this
      query defines the list of documents that the algorithm will attempt
      to normalize as described above.  The default will process the entire
      wf_miniseed collection (query set to an emtpy dict).
    :param normalize_channel:  boolean for handling channel collection.
      When True (default) matches will be attempted with the channel collection
      and when matches are found the associated channel document id will be
      set in the associated wf_miniseed document as channel_id.
    :param normalize_site:  boolean for handling site collection.
      When True (default) matches will be attempted with the site collection
      and when matches are found the associated site document id will
      be set wf_miniseed document as site_id. Note at least one of
      the two booleans normalize_channel and normalize_site must be set True
      or the function will immediately abort.


    :return: list with three integers.  0 is the number of documents processed in
      wf_miniseed (output of query), 1 is the number with channel ids set,
      and 2 contains the number of site documents set.  1 or 2 should
      contain 0 if normalization for that collection was set false.
    """

    if wfquery is None:
        wfquery = {}

    if not (normalize_channel or normalize_site):
        raise MsPASSError(
            "normalize_mseed:  usage error.  normalize_channel and normalize_site cannot both be set False",
            ErrorSeverity.Fatal,
        )

    matcher_function_list = []
    # in the calls to the constructors below starttime and endtime
    # must be included for the miniseed matcher to work
    if normalize_channel:
        matcher = MiniseedMatcher(
            db,
            collection="channel",
            attributes_to_load=["_id", "starttime", "endtime"],
            prepend_collection_name=True,
        )
        matcher_function_list.append(matcher)

    if normalize_site:
        sitematcher = MiniseedMatcher(
            db,
            collection="site",
            attributes_to_load=["_id", "starttime", "endtime"],
            prepend_collection_name=True,
        )
        matcher_function_list.append(sitematcher)

    bulk_ret = bulk_normalize(
        db,
        wfquery=wfquery,
        wf_col="wf_miniseed",
        blocksize=blocksize,
        matcher_list=matcher_function_list,
    )

    ret = [bulk_ret[0]]
    if normalize_channel:
        ret.append(bulk_ret[1])
        if normalize_site:
            ret.append(bulk_ret[2])
        else:
            ret.append(0)
    else:
        ret.append(0)
        if normalize_site:
            ret.append(bulk_ret[1])
        else:
            ret.append(0)
    return ret


def _get_test_time(d, time):
    """
    A helper function to get the test time used for searching.
    If the time is given, we simply use that as the test time.
    Otherwise (the time is None), we first try to get the start
    time from d. If start time is not defined in d, None is
    return to indicate the time field should be ignored.

        :param d: Data object with a Metadata container to extract the field
        :param time: the start time used for matching
        :return: the test_time extracted
    """
    if time == None:
        if isinstance(d, (TimeSeries, Seismogram)):
            test_time = d.t0
        else:
            if d.is_defined("starttime"):
                test_time = d["starttime"]
            else:
                # Use None for test_time as a signal to ignore time field
                test_time = None
    else:
        test_time = time
    return test_time


def _get_with_readonly_recovery(d, key):
    """
    Private method for repetitious handling of trying to use the
    readonly tag to recover if one of net, sta, chan, or loc have
    been botched with readonly tag.  d is the datum from with key is
    to be extracted.   Returns a None if recovery failed.
    """
    ROTAG = "READONLY_"
    if d.is_defined(key):
        return d[key]
    else:
        testkey = ROTAG + key
        if d.is_defined(testkey):
            return d[testkey]
        else:
            return None


def _input_is_atomic(d):
    """
    A variant of input_is_valid is to test if a data object is atomic
    by the mspass definition.  In this case, that means a datum is not
    an ensemble.  This is necessary, for example, to assume something
    like asking for d.t0 doesn't generate an exception.
    """
    return isinstance(d, (TimeSeries, Seismogram))


def _load_as_df(db, collection, query, attributes_to_load, load_if_defined):
    """
    Internal helper function used to translate all or part of a
    collection to a DataFrame.  This algorithm should only be used
    for small collections as it makes an intermediate copy of the
    collection as a dictionary before calling the DataFrame.from_dict
    method to create the working dataframe.

    :param db:  Database handle assumed to contain collection
    :param collection:  collection from which the data are to be extracted
    :param query:  python dict defining a query to apply to the collection.
      If you want the entire collection specify None or an empty dictionary.
    :type query:  python dict defining a pymongo query or None.
    :param attributes_to_load: list of keys to extract of required attributes
      to load from collection.   This function will abort if any document
      does not contain one of these attributes.
    :param load_if_defined:  attributes loaded more cautiously.  If the
      attributes for any of the keys in this list are not found in a document
      the output dataframe has a Null defined for that cell.
    """
    if query is None:
        query = dict()
    ntuples = db[collection].count_documents(query)
    if ntuples == 0:
        return pd.DataFrame()
    # create dictionary with empty array values to initialize
    dict_tmp = dict()
    for k in attributes_to_load:
        dict_tmp[k] = []
    for k in load_if_defined:
        dict_tmp[k] = []
    cursor = db[collection].find(query)
    for doc in cursor:
        # attributes_to_load list are required.  For now let this
        # thow an exception if that is not true - may need a handler
        for k in attributes_to_load:
            dict_tmp[k].append(doc[k])
        for k in load_if_defined:
            if k in doc:
                dict_tmp[k].append(doc[k])
            else:
                dict_tmp[k].append(None)
    return pd.DataFrame.from_dict(dict_tmp)


def _extractData2Metadata(
    doc,
    attributes_to_load,
    aliases,
    prepend_collection_name,
    collection,
    load_if_defined,
):
    md = Metadata()
    for k in attributes_to_load:
        if k in doc:
            if k in aliases:
                key = aliases[k]
            else:
                key = k
            if prepend_collection_name:
                if key == "_id":
                    mdkey = collection + key
                else:
                    mdkey = collection + "_" + key
            else:
                mdkey = key
            md[mdkey] = doc[key]
        else:
            message = "Required attribute {key} was not found".format(
                key=k,
            )
            raise MsPASSError(
                "DictionaryCacheMatcher._load_normalization_cache:  " + message,
                ErrorSeverity.Fatal,
            )
    for k in load_if_defined:
        if k in doc:
            if k in aliases:
                key = aliases[k]
            else:
                key = k
            if prepend_collection_name:
                if key == "_id":
                    mdkey = collection + key
                else:
                    mdkey = collection + "_" + key
            else:
                mdkey = key
            md[mdkey] = doc[key]
    return md
