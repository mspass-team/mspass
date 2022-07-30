from abc import ABC, abstractmethod
import sys
from mspasspy.ccore.utility import MsPASSError, ErrorSeverity, Metadata
from mspasspy.ccore.seismic import (
    TimeSeries,
    Seismogram,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
)

from obspy import UTCDateTime
import pymongo
import inspect
import copy


def _input_is_valid(d):
    """
    This internal function standardizes the test to certify the
    input datum, d, is or is not a valid MsPASS data object.   Putting it
    in one place makes extending the code base for other data types much
    easier.  It uses an isinstance tests of d to standardize the test that
    the input is valid data.  It returns True if d is one a valid data
    object known to mspass.  Returns false it not.  Caller must decide
    what to do if the function returns false.
    """
    return isinstance(
        d, (TimeSeries, Seismogram, TimeSeriesEnsemble, SeismogramEnsemble)
    )


# We need this for matchers that only work for atomic data (e.g. mseed matching)
def _input_is_atomic(d):
    return isinstance(d, (TimeSeries, Seismogram))


class NMF(ABC):
    """
    Base class for a family of Normalization Match Functions (NMF).
    This family of object are used in MsPASS to standize the api for
    generic mongodb match operation for "normalizing" a collection.
    Normalization is comparable to a relational database join.
    With MongoDB normalization is most sensible for the case when the
    collection to be normalized is much smaller than collection that is
    to be joined (normalized)  i.e. the normalization operation is many
    to one with many links from the documents in the collection to be
    normalized to each normalizing document.  The stock normalizing collections
    in MsPASS are channel, site, and source.

    The api defines two basic operations any concrete instance of the
    class must implement:  (1)  a method to fetch the entire document
    defining a match and (2) a method to fetch and copy specified
    key-value pairs to a valid MsPASS data object.
    """

    def __init__(
        self,
        db,
        collection,
        attributes_to_load=None,
        load_if_defined=None,
        query=None,
        prepend_collection_name=True,
        kill_on_failure=True,
        verbose=False,
        cache_normalization_data=None,
    ):
        """
        Base class constructor. The implementation requires defaulted parameters
        that most subclasses can find useful.

        :param db:  MongoDB Database handle
        :param collection:  string defining the collection this object
          should use for normalization. If this argument is not a valid
          string the constructor will abort with a TypeError exception.
        :param attributes_to_load:  is a list of keys (strings) that are to
          be loaded with data by the normalize method. Default is None here,
          subclass should set their own default values.
        :param load_if_defined:   is a secondary list of keys (strings) that
          should be loaded only if they are defined. Default is None here,
          subclass should set their own default values.
        :param query:  optional query to apply to collection before loading.
          The default is load all.  If your data set is time limited and
          the collection has a time attribute (true of the standard channel,
          site, and source collections) you can reduce the memory footprint
          by using a time range query (python dict) for this argument.
        :param prepend_collection_name:  boolean controlling a standard
          renaming option.   When True (default)   all normalizing data
          keys get a collection name prepended to the key to give it a
          unique key.  e.g. if loading data from "channel" the "lat"
          (latitude of the instrument's location) field will be changed on
           posting to d to "channel_lat".   Setting this false should be
           a rare or never used option and should be done only if you deeply
           understand the consequences.
        :param kill_on_failure:  when set true (Default) match errors
          will cause data passed to the normalize method to be killed.
        :param verbose:  most subclasses will want a verbose option
          to control what is posted to elog messages or printed
          (most useful for serial jobs)
        :param cache_normalization_data:  When set True the specified
          collection is preloaded in an internal cache on construction and
          used for all subsequent matching.  This mode is highly recommended
          as it has been found to speed normalization by an order of magnitude
          or more relative to a database transaction for each call to normalize,
          which is what happens when this parameter is set False. Subclasses might
          not support the caching feature, so the default value is None.
        """
        if not isinstance(collection, str):
            raise TypeError(
                "{} constructor:  arg0 must be a collection name - received invalid type".format(
                    self.__class__.__name__
                )
            )
        self.collection = collection
        self.dbhandle = db[collection]

        if attributes_to_load is None:
            attributes_to_load = []
        if load_if_defined is None:
            load_if_defined = []
        if query is None:
            query = {}
        self.query = query

        self.prepend_collection_name = prepend_collection_name
        self.kill_on_failure = kill_on_failure
        self.verbose = verbose

        # These two lists are always needed for normalize methods.
        # Subclasses need to specify the default value in their
        # own init methods before calling super.init
        self.attributes_to_load = attributes_to_load
        self.load_if_defined = load_if_defined

        # Derived classes need to specify the cache_normalization_data, some of them
        # might don't have a caching feature
        self.cache_normalization_data = cache_normalization_data
        if self.cache_normalization_data == True:
            self.cache = self._load_normalization_cache()

    def __call__(self, d, *args, **kwargs):
        """
        This convenience method allows a concrete instance to be
        called with the simpler syntax with the (implied) principle
        method "normalize".   e.g. to normalize d with the
        channel collection using id_matcher you can use
        d = id_matcher(d)  instead of d = id_matcher.normalize(d)
        """
        return self.normalize(d, *args, **kwargs)

    def get_document(self, d, *args, **kwargs):
        """
        This is a method to fetch the document that matches. The
        document, in this case, is actually a MsPASS Metadata container.
        Only attributes defined by the attribute_to_load and load_if_defined
        lists will be returned in the result.
        This method works as an entry to two different implementations:
        1. If caching was enabled, _cached_get_document will be invoked, and
        data will be returned from the internal cache.
        2. If caching was turned off, _db_get_document will be invoked, a db
        query will then be invoked for each call to this method.
        The subclasses can call this method and extend it with extra arguments
        (*args, **kwargs), a typical extra argument is time (see composite_key_matcher)
        Any failures will cause d to be marked dead if kill_on_failure
        was set in the constructor (the default).

        :param d:  Data object with a Metadata container to be tested.
        That means this can be any valid MsPASS data object or even
        a raw Metadata container.  Only the class defined id key is
        accessed by d.  That id drives the algorithm as described above.
        :return:  Metadata container with the matching data. Returns None
        if there is not match AND posts a message to elog of d.
        """
        if (
            self.cache_normalization_data is None
            or self.cache_normalization_data == False
        ):
            return self._db_get_document(d, *args, **kwargs)
        else:
            return self._cached_get_document(d, *args, **kwargs)

    @abstractmethod
    def _cached_get_document(self, d, *args, **kwargs):
        """
        This method looks for the qualified document in the cache constructed
        before. Subclasses are required to implement this method.
        """
        pass

    @abstractmethod
    def _db_get_document(self, d, *args, **kwargs):
        """
        This method looks for the qualified document by sending a new query to
        the database. Subclasses are required to implement this method.
        """
        pass

    def _load_doc(self, doc, d=None):
        """
        This is a helper function that takes a dict input, and return a Metadata
        that only loads attribues defined in attribute_to_load and load_if_defined.
        An optional mspass object d can be useful to log the error.  The default is None,
        meaning that there is no data object related with the document to load, so a
        MsPassError will be raised when an error occurs.
        If d is not None, meaning one single data object d is related, a error message will
        be stored to the elog of d.

        :param doc:  A dict object from the mongodb
        :param d: The MsPass data object that invokes this method, default is None, meaning
        that no data object is related.
        :return: Metadata container with the matching data. Return None if some keys in
        attribues_to_load are not defined in the doc.
        """
        #   d is not given when used in caching method
        result = Metadata()
        for key in self.attributes_to_load:
            if key in doc:
                result[key] = doc[key]
            else:
                message = (
                    "Required key={} not found in normalization collection = {}".format(
                        key, self.collection
                    )
                )
                if d is None or not _input_is_valid(
                    d
                ):  #   Don't have an object to save error log
                    raise MsPASSError(message, ErrorSeverity.Invalid)
                self.log_error(d, message, ErrorSeverity.Invalid)
                return None
        for key in self.load_if_defined:
            if key in doc:
                result[key] = doc[key]
        return result

    def _load_normalization_cache(self):
        """
        This is a function internal to the matcher module used to standardize
        the loading of normalization data from MongoDB. It returns a python
        dict with keys defined by the string representation of each document
        found in the normalizing collection.   The value associated with each
        key is a Metadata container of a (usually) reduced set of data that
        is to be merged with the Metadata of a set of mspass data objects
        to produce the "normalization".
        This method use the db[collection] in the class to define the
        database collection to be indexed to the cache.
        self.required_attributes is used to indicate list of keys for attributes
        the function will dogmatically try to extract from each document. If any
        of these are missing in any document the function will abort with a
        MsPASSError exception (throwed in _load_doc)
        self.optional_attributes is used to indicate list of keys, which are
        are silently ignored if they are missing.
        self.query is applied before loading the normalizing collection defined by
        the collection argument.  By default the entire collection is loaded and
        returned. This can be useful with large collection to reduce memory bloat.
        e.g. if you have a large collection of channel data but your data set only
        spans a 1 year period you might set a query to only load data for stations
        running during that time period.
        """
        cursor = self.dbhandle.find(self.query)
        normcache = dict()
        for doc in cursor:
            mdresult = self._load_doc(doc)
            cache_key = str(doc["_id"])  # always defined for a MongoDB doc
            normcache[cache_key] = mdresult
        return normcache

    def normalize(self, d, *args, **kwargs):
        """
        This is a method to fetch and copy specified key-value pairs to a valid
        MsPASS data object.
        This method first tests if the input is a valid MsPASS data object.  It will
        silently do nothing if the data are not valid returning a None.
        It then tests if the datum is marked live. If it is found marked
        dead it silently returns d with no changes. For live data it calls the get_document
        method. If that succeeds it extracts the (constructor defined) list of
        desired attributes and posts them to the data's Metadata container.
        If get_document fails a message is posted to elog and if the
        constructor defined "kill_on_failure" parameter is set True the
        returned datum will be killed.
        Note that for most of the subclasses, the functionalities of normalize are
        the same. So in most cases, we don't need to override this method.

        :param d:  data to be normalized.  This must be a MsPASS data object.
          For this function that means TimeSeries, Seismogram, TimeSeriesEnsemble,
          or SeismogramEnsemble.  Not for ensembles the normalizing data will
          be posted to the ensemble metadata container not the members.
          This can be used, for example, to normalize a parallel container
          (rdd or bad) of common source gathers more efficiently than
          at the atomic level.
        :return: The normalized document
        """
        if not _input_is_valid(d):
            raise TypeError("ID_matcher.normalize:  received invalid data type")
        if d.dead():
            return d

        doc = self.get_document(d, *args, **kwargs)
        if doc == None:
            message = "No matching _id found for in collection={}".format(
                self.collection
            )
            self.log_error(d, message, ErrorSeverity.Invalid)
        else:
            # In this implementation the contents of doc have been prefiltered
            # to contain only those in the attributes_to_load or load_if_defined lists
            # Hence we copy all.
            for key in doc:
                if self.prepend_collection_name:
                    # Handle "_id" specially or we get double _ for
                    # common constructs like channel_id
                    if key == "_id":
                        newkey = self.collection + key
                    else:
                        newkey = self.collection + "_" + key
                    d[newkey] = doc[key]
                else:
                    d[key] = doc[key]
        return d

    def log_error(self, d, message, severity=ErrorSeverity.Informational, kill=None):
        """
        This base class method is used to standardize the error logging
        functionality of all NMF objects.   It writes a standardized
        message to simplify writing of subclasses - they only need to
        format a specific message to be posted.  The caller may optionally
        kill the datum and specify an alternative severity level to
        the default warning.

        Note most subclasses may want to include a verbose option in the constructor
        (or the reciprocal silent) that provide an option of only writing log messages when
        verbose is set true. There are possible cases with large data sets where
        verbose messages can cause bottlenecks and bloated elog collections. If verbose is
        set true, the datum will still be killed, but the message won't be written.

        :param d:  MsPASS data object to which elog message is to be
          written.
        :param message:  specialized message to post - this string is added
        to an internal generic message.
        :param severity:  ErrorSeverity to assign to elog message
        (See ErrorLogger docstring).  Default is Informational
        :param kill:  boolean controlling if the message should cause the
        datum to be killed. Default None meaning the kill_on_failure boolean of
        the class will be used. If kill is set, it will be overwritten temporarily.
        """
        if not _input_is_valid(d):
            #   If we can't log error to the object, simply return
            return

        if kill is None:
            kill = self.kill_on_failure

        if kill:
            d.kill()
            message += "\nDatum was killed"

        #   Add class name and caller function name for better locating the error
        class_name = self.__class__.__name__
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        caller_name = calframe[1][3]
        matchername = class_name + "." + caller_name

        if not hasattr(self, "verbose") or self.verbose is True:
            d.elog.log_error(matchername, message, severity)


class single_key_matcher(NMF):
    """
    An intermediate class. It extends the NMF by adding a variable: mdkey.
    mdkey is the field defined for matching a document.
    The matching logic is as follows:
        d[mdkey] == DOC_TO_MATCH['_id']
    This class is useful for implementing the ID_matcher (see below). Users
    may also find it useful for implementing other match classes.
    """

    def __init__(
        self,
        db,
        collection,
        mdkey,
        attributes_to_load=None,
        load_if_defined=None,
        query=None,
        prepend_collection_name=True,
        kill_on_failure=True,
        verbose=False,
        cache_normalization_data=None,
    ):
        """
        The constructor method for this class, basically it calls the
        constructor of the base class (NMF), and set the mdkey.
        :param db:  MongoDB Database handle
        :param collection:  string defining the collection this object
          should use for normalization.
        :param mdkey: the field for matching, see class description
        :param attributes_to_load:  is a list of keys (strings) that are to
          be loaded with data by the normalize method. Default is None here,
          subclass should set their own default values.
        :param load_if_defined:   is a secondary list of keys (strings) that
          should be loaded only if they are defined. Default is None here,
          subclass should set their own default values.
        :param query:  optional query to apply to collection before loading.
          The default is load all.
        :param prepend_collection_name:  boolean controlling a standard
          renaming option.   When True (default)   all normalizing data
          keys get a collection name prepended to the key to give it a
          unique key.
        :param kill_on_failure:  when set true (Default) match errors
          will cause data passed to the normalize method to be killed.
        :param verbose:  most subclasses will want a verbose option
          to control what is posted to elog messages or printed
          (most useful for serial jobs)
        :param cache_normalization_data:  When set True the specified
          collection is preloaded in an internal cache on construction and
          used for all subsequent matching.
        """
        NMF.__init__(
            self,
            db,
            collection,
            attributes_to_load,
            load_if_defined,
            query,
            prepend_collection_name,
            kill_on_failure,
            verbose,
            cache_normalization_data,
        )
        self.mdkey = mdkey

    def _get_key_id(self, d):
        """
        A helper function to get the value of corresponding mdkey of the
        matcher. If the mdkey is not defined, an error will be stored to the
        elog.
        :param d: A MsPass Object
        :return: None if the mdkey is not defined, otherwise just return the
        value
        """
        if d.is_defined(self.mdkey):
            testid = d[self.mdkey]
        else:
            message = "Normalizing ID with key={} is not defined in this object".format(
                self.mdkey
            )
            self.log_error(d, message, ErrorSeverity.Invalid)
            return None
        return testid

    def _cached_get_document(self, d):
        testid = self._get_key_id(d)
        if testid is None:
            return None
        try:
            result = self.cache[str(testid)]
            return result
        except KeyError:
            message = "Key [{}] not defined in cache".format(str(testid))
            self.log_error(d, message, ErrorSeverity.Invalid)
            return None

    def _db_get_document(self, d):
        query = copy.deepcopy(self.query)

        testid = self._get_key_id(d)
        if testid is None:
            return None
        query[self.mdkey] = testid

        doc = self.dbhandle.find_one(query)
        if doc is None:
            message = "Key [{}] not defined in normalization collection = {}".format(
                str(testid), self.collection
            )
            self.log_error(d, message, ErrorSeverity.Invalid)
            return None

        result = self._load_doc(doc, d)
        return result


class ID_matcher(single_key_matcher):
    """
    This class is used to match a data object to a normalizing collection
    using a MongoDB ObjectId and the key naming convention of MsPASS.
    That is, if the normalizing collection is channel it will look
    in the data object's Metadata for an attribute with the key "channel_id".
    If that attribute is found it will try to load the document for which
    the "_id" of that collection == the key constructed (channel_id for the example).

    By default this class will cache a (usually) reduced image of the
    normalizing collertion data.  This can improve performance significantly with large
    data sets at the cost of needing to store and, when the scheduler finds
    it necessary, to move a copy of the contents to a worker node.  Turn
    the caching off (set cache_normalization_data False in the constructor)
    if normalizing collection is large and could cause a memory problem.
    Note, the size can be computed as the size of the expected return of
    the (attribute_to_load list + objectid string size)*Ncol where Ncol is
    the number of documents in the normalizing collection.
    """

    def __init__(
        self,
        db,
        collection="channel",
        attributes_to_load=None,
        load_if_defined=None,
        query=None,
        prepend_collection_name=True,
        kill_on_failure=True,
        verbose=True,
        cache_normalization_data=True,
    ):
        """
        Constructor for this class.

        :param db:  MongoDB Database handle
        :param collection:   string defining the collection this object
          should use for normalization.   If this argument is not a valid
          string the constructor will abort with a TypeError exception.
          Default is "channel"
        :param attributes_to_load:  is a list of keys (strings) that are to
          be loaded with data by the normalize method.
          Note that the Default value is None, but it will then be initialized
          to a list of common channel attributes:  "lat", "lon", "elev", "hang",
          and "vang". This is necessary because we need to cope with the mutable
          default argument (see https://stackoverflow.com/questions/1132941/least
          -astonishment-and-the-mutable-default-argument)
        :param load_if_defined:   is a secondary list of keys (strings) that
          should be loaded only if they are defined.  A type example is the
          seed "loc" code that isn't always used.  Default is None (an empty list).
        :param query:  optional query to apply to collection before loading.
          The default is load all.
        :param prepend_collection_name:  boolean controlling a standard
          renaming option.
        :param kill_on_failure:  When True (the default) any data passed
          processed by the normalize method will be kill if there is no
          match to the id key requested or if the data lack an id key to
          do the match.
        :param verbose: a boolean value to control what is posted to elog messages
        or printed
        :param cache_normalization_data:  When set True (the default)
          the specified collection is preloaded in an internal cache
          on construction and used for all subsequent matching.
        """

        if attributes_to_load is None:
            attributes_to_load = ["lat", "lon", "elev", "hang", "vang"]
        if load_if_defined is None:
            load_if_defined = []

        single_key_matcher.__init__(
            self,
            db,
            collection,
            collection + "_id",
            attributes_to_load,
            load_if_defined,
            query,
            prepend_collection_name,
            kill_on_failure,
            verbose,
            cache_normalization_data,
        )

    def _db_get_document(self, d):
        query = copy.deepcopy(self.query)

        testid = self._get_key_id(d)
        if testid is None:
            return None
        query["_id"] = testid

        doc = self.dbhandle.find_one(query)
        if doc is None:
            message = "Key [{}] not defined in normalization collection = {}".format(
                str(testid), self.collection
            )
            self.log_error(d, message, ErrorSeverity.Invalid)
            return None

        result = self._load_doc(doc, d)
        return result


class composite_key_matcher(NMF):
    """
    An intermediate class that implements the shared functionalities of
    mseed_channel_matcher and mseed_site_matcher. Users may also extend this class to
    define their own matcher.

    A composite_key_matcher uses a composite key to match the documents, it is created
    by concatenating all required keys and their values. For example: mseed_channel_matcher's
    required keys are ["net", "sta", "chan"]. So the composite key will be something like:
    "cmp_id_net=TA_sta=034A_chan=BHE". This composite key can identify all the documents
    that have the same value for ["net", "sta", "chan"]. In addition, user can define a list
    of optional_keys that are not always present in a document. For example, "doc" in
    the mseed_channel_matcher.

    It can also be used to normalize data saved in wf_TimeSeries where the mseed tags
    are often altered by MsPASS to change fields like "net" to "READONLYERROR_net".
    There is an automatic fallback for each of the tags where if the proper name is not
    found we alway try to use the READONLYERROR_ version before giving up.
    """

    def __init__(
        self,
        db,
        collection,
        keys,
        optional_keys=None,
        attributes_to_load=None,
        load_if_defined=None,
        query=None,
        prepend_collection_name=True,
        kill_on_failure=True,
        verbose=True,
        cache_normalization_data=True,
        readonly_tag="READONLYERROR_",
    ):
        """
        Constructor for this class. Most of the arguments are inherited from the base
        class NMF.

        :param db:  MongoDB Database handle
        :param collection:  string defining the collection this object
          should use for normalization. If this argument is not a valid
          string the constructor will abort with a TypeError exception.
        :param keys: a list of fields that are used to create a composite key,
        these fields are always cached if caching is enabled
        :param optional_keys: a list of fields, very similar to keys, the only
        difference is that optional_keys can be missing in a document.
        :param attributes_to_load:  is a list of keys (strings) that are to
          be loaded with data by the normalize method. Default is None here,
          sublasses should set their own default values.
        :param load_if_defined:   is a secondary list of keys (strings) that
          should be loaded only if they are defined. Default is None here,
          subclasses should set their own default values.
        :param query:  optional query to apply to collection before loading.
          The default is load all.  If your data set is time limited and
          the collection has a time attribute (true of the standard channel,
          site, and source collections) you can reduce the memory footprint
          by using a time range query (python dict) for this argument.
        :param prepend_collection_name:  boolean controlling a standard
          renaming option.   When True (default)   all normalizing data
          keys get a collection name prepended to the key to give it a
          unique key.  e.g. if loading data from "channel" the "lat"
          (latitude of the instrument's location) field will be changed on
           posting to d to "channel_lat".   Setting this false should be
           a rare or never used option and should be done only if you deeply
           understand the consequences.
        :param kill_on_failure:  when set true (Default) match errors
          will cause data passed to the normalize method to be killed.
        :param verbose:  most subclasses will want a verbose option
          to control what is posted to elog messages or printed
          (most useful for serial jobs)
        :param cache_normalization_data:  When set True (default) the specified
          collection is preloaded in an internal cache on construction and
          used for all subsequent matching.
        :param readonly_tag:  As noted in the class docstring attributes
          marked read only in the schema can sometimes be saved with a
          prefix.  The get_document and normalize methods have an auto
          recover to look try to match read only parameters.  This
          argument defines the prefix used to define such attributes.
          The default is "READONLYERROR_" which is what is used by
          default in MsPASS.  Few if any users will likely need to
          ever set this parameter.
        """
        self.keys = keys
        self.optional_keys = optional_keys
        self.readonly_tag = readonly_tag
        super().__init__(
            db,
            collection,
            attributes_to_load,
            load_if_defined,
            query,
            prepend_collection_name,
            kill_on_failure,
            verbose,
            cache_normalization_data,
        )

    def _get_readonly_field(self, d, field, error_logging_enabled=True):
        """
        This function is used to get some fields from a document and retry by adding
        a readonly prefix to the field before giving up.
        :param d: Data object with a Metadata container to extract the field
        :param field: The name of the field
        :param error_logging_enabled: a boolean value that indicates whether the error
        should be logged to the elog. Default is True, user can ignore the error
        by setting it to False
        :return: the value of the field in d, if the field is not defined, an error
        is logged, and None is return.
        """
        error_logging_allowed = isinstance(d, (TimeSeries, Seismogram))
        if d.is_defined(field):
            return d[field]
        elif d.is_defined(self.readonly_tag + field):
            return d[self.readonly_tag + field]
        else:
            if error_logging_allowed and error_logging_enabled:
                self.log_error(
                    d,
                    "Required match key={key} or {tag}{key} are not defined for this datum".format(
                        key=field, tag=self.readonly_tag
                    ),
                    ErrorSeverity.Invalid,
                )
            return None

    def _create_composite_key(self, d, separator="_"):
        """
        Create a composite key by concatenating the fields in self.keys and
        their value.
        A typical composite key is in the following format:
        "cmp_id_{FIELD1}={VALUE1}__{FIELD2}={VALUE2}_{FIELD3}={VALUE4}...".
        :param d: Data object with a Metadata container to extract the field
        :param separator: Optional, can be used to use a different separator
        character.
        """
        composite_key = "cmp_id"
        for key in self.keys:
            val = self._get_readonly_field(d, key)
            if val is None or len(val) == 0:
                return None
            else:
                composite_key += "{}{}={}".format(separator, key, val)
        for key in self.optional_keys:
            val = self._get_readonly_field(d, key, False)
            if val is None or len(val) == 0:
                continue
            else:
                composite_key += "{}{}={}".format(separator, key, val)
        return composite_key

    def _load_normalization_cache(self):
        """
        This method is overriden to add/modify 2 features:
        (1) Instead of using "_id" field, a composite key is created and
            used as the index for the document
        (2) Always caching keys and optional_keys, in order to
            Make sure (1) can work when attributes_to_load doesn't
            contain certain keys needed for a composite key
        (3) Always caching ["starttime", "endtime"] because these two
        fields are essential for the query in get_document
        """
        cursor = self.dbhandle.find(self.query)
        normcache = dict()
        for doc in cursor:
            #   we can't just simply call _load_result because we need to load
            #   keys and optional_keys
            mdresult = self._load_doc(doc)
            for k in self.keys + ["starttime", "endtime"]:
                if k in (self.attributes_to_load + self.load_if_defined):
                    continue
                if k not in doc:
                    message = "Required key={} not found in normalization collection = {}".format(
                        k, self.collection
                    )
                    raise MsPASSError(message, ErrorSeverity.Invalid)
                else:
                    mdresult[k] = doc[k]
            for k in self.optional_keys:
                if k in (self.attributes_to_load + self.load_if_defined):
                    continue
                if k in doc:
                    mdresult[k] = doc[k]
            composite_key = self._create_composite_key(mdresult)
            if composite_key is None:
                raise MsPASSError(
                    "_build_xref: can't create composite key for {} because some keys are missing".format(
                        str(mdresult)
                    ),
                    ErrorSeverity.Fatal,
                )
            if composite_key in normcache:
                normcache[composite_key].append(mdresult)
            else:
                normcache[composite_key] = [mdresult]  # initializes array of id strings
        return normcache

    def get_document(self, d, time=None):
        if not isinstance(d, (TimeSeries, Seismogram, Metadata, dict)):
            raise TypeError(
                "mseed_channel_matcher.get_document:  data received as arg0 is not an atomic MsPASS data object"
            )
        # We need to convert a dict to Metadata to match the api for
        # data objects.  We need support for dict for interacting
        # directly with mongodb query results
        if isinstance(d, dict):
            d_to_use = Metadata(d)
        else:
            d_to_use = d
        if self.cache_normalization_data:
            doc = self._cached_get_document(d_to_use, time)
        else:
            doc = self._db_get_document(d_to_use, time)
        return doc

    def _get_test_time(self, d, time):
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

    def _cached_get_document(self, d, time=None):
        """
        Private method to do the work of the get_document method when channel
        data have been previously cached.
        """
        # do this test once to avoid repetitious calls later - minimal cost
        error_logging_allowed = isinstance(d, (TimeSeries, Seismogram))

        comp_key = self._create_composite_key(d)
        test_time = self._get_test_time(d, time)

        if comp_key in self.cache:
            doclist = self.cache[comp_key]
            # avoid a test and assume this is a match if there is only
            # one entry in the list
            if len(doclist) == 1:
                return doclist[0]

            if len(doclist) > 1:
                # When time is not defined (None) just return first entry
                # but post a warning
                if test_time == None:
                    # We might never enter this branch, since mspass objects always have a test_time = d.t0
                    if error_logging_allowed:
                        message = "Warning - no time specified for match and data has no starttime field defined.  Using first match found in channel collection"
                        self.log_error(d, message, ErrorSeverity.Suspect, False)
                    return doclist[0]

                cnt = 0
                ret = None
                for doc in doclist:
                    stime = doc["starttime"]
                    etime = doc["endtime"]
                    if test_time >= stime and test_time <= etime:
                        cnt += 1  #   We need to calculate the count of matchs, to decide if we need to log an warning
                        if ret is None:
                            ret = doc
                if cnt > 1:
                    self.log_error(
                        d,
                        "Multiple channel docs match net:sta:chan:loc:time for this datum - using first one found",
                        ErrorSeverity.Complaint,
                        False,
                    )
                if ret is not None:
                    return ret

            # If there is no qualified doc
            if error_logging_allowed:
                message = "No match for composite key={} and time={}".format(
                    comp_key, str(UTCDateTime(test_time))
                )
                self.log_error(
                    d,
                    message,
                    ErrorSeverity.Invalid,
                )
            return None
        else:
            if error_logging_allowed:
                message = (
                    "No entries are present in channel collection for net_sta_chan_loc = "
                    + comp_key
                )
                self.log_error(
                    d,
                    message,
                    ErrorSeverity.Invalid,
                )
            return None

    def _db_get_document(self, d, time=None):
        """
        Private method that does the work of get_document when caching is
        turned off.   This method does one database transaction per call.
        """
        # do this test once to avoid repetitious calls later - minimal cost
        error_logging_allowed = isinstance(d, (TimeSeries, Seismogram))

        query = copy.deepcopy(self.query)
        for key in self.keys:
            val = self._get_readonly_field(d, key)
            if val is None:
                return None
            else:
                query[key] = val
        for key in self.keys:
            val = self._get_readonly_field(d, key, False)
            if val is not None:
                query[key] = val

        querytime = self._get_test_time(d, time)
        if querytime is not None:
            query["starttime"] = {"$lt": querytime}
            query["endtime"] = {"$gt": querytime}
        elif error_logging_allowed:
            message = "Warning - no time specified for match and data has no starttime field defined.  Using first match found in channel collection"
            self.log_error(d, message, ErrorSeverity.Suspect, False)

        matchsize = self.dbhandle.count_documents(query)
        if matchsize == 0:
            if error_logging_allowed:
                message = "No match for query = " + str(query)
                self.log_error(
                    d,
                    message,
                    ErrorSeverity.Invalid,
                )
            return None

        if matchsize > 1 and self.verbose and error_logging_allowed:
            self.log_error(
                d,
                "Multiple channel docs match net:sta:chan:loc:time for this datum - using first one found",
                ErrorSeverity.Complaint,
                False,
            )

        match_doc = self.dbhandle.find_one(query)
        return self._load_doc(match_doc, d)

    def normalize(self, d, time=None):
        """
        Implementation of the normalize method for this class.

        :param d:  input data object to be normalized.  Must be a TimeSeries
          or Seismogram object.  If d is anything else the function will
          raise a TypeError.
        """
        if not _input_is_atomic(d):
            raise TypeError(
                "mseed_channel_matcher.normalize:  received invalid data type"
            )

        return super().normalize(d, time)


class mseed_channel_matcher(composite_key_matcher):
    """
    This class is used to match wf_miniseed to the channel collection using
    the mseed standard channel string tags net, sta, chan, and (optionally) loc.
    It can also be used to normalize data saved in wf_TimeSeries where the mseed tags
    are often altered by MsPASS to change fields like "net" to "READONLYERROR_net".
    There is an automatic fallback for each of the tags where if the proper
    name is not found we alway try to use the READONLYERROR_ version before
    giving up.
    An issue with this matcher is that it is very common to have redundant
    entries in the channel collection for the same channel of data.  That
    can happen for a variety of reasons that are harmless.  When that happens
    the method of this object will normally post an elog message warning of the
    potential issue.  Those warnings can be silenced by setting verbose
    in the constructor to False.
    The class also has a cache option that can dramatically improve
    performance for large data sets.  When using the database option
    (caching turned off) the normalize method issues a database query
    at each call.  If applied to a data set with a large number of
    waveforms that can add up.  We have found the cache algorithm is
    an order of magnitude or more faster than the database algorithm
    for typical channel collections assembled from FDSN web services.
    It is recommended unless the memory foot print is excessive.
    That too can usually be avoided by using a query to weed out unnecessary
    channel documents or by editing the channel document to reduce the
    debris from extraneous data.
    """

    def __init__(
        self,
        db,
        collection="channel",
        attributes_to_load=None,
        load_if_defined=None,
        query=None,
        prepend_collection_name=True,
        kill_on_failure=True,
        verbose=True,
        cache_normalization_data=True,
        readonly_tag="READONLYERROR_",
    ):
        """
        Constructor for this class.
        It instantiates a composite_key_matcher with default values for collection,
        attributes_to_load, load_if_defiend, keys, optional_keys.
        Note that although the default value of attributes_to_load, load_if_defined
        is None, they are actually initialized in the function body.

        :param db:  MongoDB Database handle
        :param collection:  string defining the collection this object
          should use for normalization. Default is "channel"
        :param keys: a list of fields that are used to create a composite key,
        these fields are always cached if caching is enabled. Default is None, but is
        initialized with ["net", "sta", "chan"].
        :param optional_keys: a list of fields, very similar to keys, the only
        difference is that optional_keys can be missing in a document. Default is None,
        but is initialized with ["loc"].
        :param attributes_to_load:  is a list of keys (strings) that are to
          be loaded with data by the normalize method.
          Note that the Default value is None, but it will then be initialized
          to a list of common channel attributes: "_id","net","sta","chan","lat",
          "lon","elev","hang","vang","starttime" and "endtime". This is necessary
          because we need to cope with the mutable default argument (see https://
          stackoverflow.com/questions/1132941/least-astonishment-and-the-mutable-default-argument)
        :param load_if_defined:   is a secondary list of keys (strings) that
          should be loaded only if they are defined. Default is None, but is initialized
          with ["loc"].
        :param query:  optional query to apply to collection before loading.
          The default is load all.  If your data set is time limited and
          the collection has a time attribute (true of the standard channel,
          site, and source collections) you can reduce the memory footprint
          by using a time range query (python dict) for this argument.
        :param prepend_collection_name:  boolean controlling a standard
          renaming option.   When True (default)   all normalizing data
          keys get a collection name prepended to the key to give it a
          unique key.
        :param kill_on_failure:  when set true (Default) match errors
          will cause data passed to the normalize method to be killed.
        :param verbose:  most subclasses will want a verbose option
          to control what is posted to elog messages or printed
          (most useful for serial jobs)
        :param cache_normalization_data:  When set True (default) the specified
          collection is preloaded in an internal cache on construction and
          used for all subsequent matching.
        :param readonly_tag: The get_document and normalize methods have an auto
          recover to try to match read only parameters.  This
          argument defines the prefix used to define such attributes.
          The default is "READONLYERROR_" which is what is used by
          default in MsPASS.  Few if any users will likely need to
          ever set this parameter.
        """
        if attributes_to_load is None:
            attributes_to_load = [
                "_id",
                "net",
                "sta",
                "chan",
                "lat",
                "lon",
                "elev",
                "hang",
                "vang",
                "starttime",
                "endtime",
            ]
        if load_if_defined is None:
            load_if_defined = ["loc"]

        keys = ["net", "sta", "chan"]
        optional_keys = ["loc"]

        composite_key_matcher.__init__(
            self,
            db,
            collection,
            keys,
            optional_keys,
            attributes_to_load,
            load_if_defined,
            query,
            prepend_collection_name,
            kill_on_failure,
            verbose,
            cache_normalization_data,
            readonly_tag,
        )


class mseed_site_matcher(composite_key_matcher):
    """
    This class is used to match derived from seed data to the site collection using
    the mseed standard site string tags net, sta, and (optionally) loc.
    It can also be used to data saved in wf_TimeSeries or wf_Seismogram where the mseed tags
    are often altered by MsPASS to change fields like "net" to "READONLYERROR_net".
    There is an automatic fallback for each of the tags where if the proper
    name is not found we alway try to use the READONLYERROR_ version before
    giving up.

    An issue with this matcher is that it is very common to have redundant
    entries in the site collection for the same site of data.  That
    can happen for a variety of reasons that are harmless.  When that happens
    the method of this object will normally post an elog message warning of the
    potential issue.  Those warnings can be silenced by setting verbose
    in the constructor to False.

    The class also has a cache option that can dramatically improve
    performance for large data sets.  When using the database option
    (caching turned off) the normalize method issues a database query
    at each call.  If applied to a data set with a large number of
    waveforms that can add up.  We have found the cache algorithm is
    an order of magnitude or more faster than the database algorithm
    for typical channel collections assembled from FDSN web services.
    It is recommended unless the memory foot print is excessive.
    That too can usually be avoided by using a query to weed out unnecessary
    channel documents or by editing the channel document to reduce the
    debris from extraneous data.
    """

    def __init__(
        self,
        db,
        collection="site",
        attributes_to_load=None,
        load_if_defined=None,
        query=None,
        prepend_collection_name=True,
        kill_on_failure=True,
        verbose=True,
        cache_normalization_data=True,
        readonly_tag="READONLYERROR_",
    ):
        """
        Constructor for this class.
        It instantiates a composite_key_matcher with default values for collection,
        attributes_to_load, load_if_defiend, keys, optional_keys.
        Note that although the default value of attributes_to_load, load_if_defined
        is None, they are actually initialized in the function body.

        :param db:  MongoDB Database handle
        :param collection:  string defining the collection this object
          should use for normalization. Default is "site"
        :param keys: a list of fields that are used to create a composite key,
        these fields are always cached if caching is enabled. Default is None, but is
        initialized with ["net", "sta"].
        :param optional_keys: a list of fields, very similar to keys, the only
        difference is that optional_keys can be missing in a document. Default is None,
        but is initialized with ["loc"].
        :param attributes_to_load:  is a list of keys (strings) that are to
          be loaded with data by the normalize method.
          Note that the Default value is None, but it will then be initialized
          to a list of common channel attributes: "_id","net","sta","lat","lon","elev",
          "starttime" and "endtime". This is necessary because we need to cope with the
          mutable default argument (see https://stackoverflow.com/questions/1132941/
          least-astonishment-and-the-mutable-default-argument)
        :param load_if_defined:   is a secondary list of keys (strings) that
          should be loaded only if they are defined. Default is None, but is initialized
          with ["loc"].
        :param query:  optional query to apply to collection before loading.
          The default is load all.  If your data set is time limited and
          the collection has a time attribute (true of the standard channel,
          site, and source collections) you can reduce the memory footprint
          by using a time range query (python dict) for this argument.
        :param prepend_collection_name:  boolean controlling a standard
          renaming option.   When True (default)   all normalizing data
          keys get a collection name prepended to the key to give it a
          unique key.
        :param kill_on_failure:  when set true (Default) match errors
          will cause data passed to the normalize method to be killed.
        :param verbose:  most subclasses will want a verbose option
          to control what is posted to elog messages or printed
          (most useful for serial jobs)
        :param cache_normalization_data:  When set True (default) the specified
          collection is preloaded in an internal cache on construction and
          used for all subsequent matching.
        :param readonly_tag: The get_document and normalize methods have an auto
          recover to try to match read only parameters.  This
          argument defines the prefix used to define such attributes.
          The default is "READONLYERROR_" which is what is used by
          default in MsPASS.  Few if any users will likely need to
          ever set this parameter.
        """
        if attributes_to_load is None:
            attributes_to_load = [
                "_id",
                "net",
                "sta",
                "lat",
                "lon",
                "elev",
                "starttime",
                "endtime",
            ]
        if load_if_defined is None:
            load_if_defined = ["loc"]

        keys = ["net", "sta"]
        optional_keys = ["loc"]

        composite_key_matcher.__init__(
            self,
            db,
            collection,
            keys,
            optional_keys,
            attributes_to_load,
            load_if_defined,
            query,
            prepend_collection_name,
            kill_on_failure,
            verbose,
            cache_normalization_data,
            readonly_tag,
        )


class origin_time_source_matcher(NMF):
    """
    One common scheme for fetching seismic data from an FDSN data center
    is event based with fixed time windows being selected relative to the
    origin time of each event in a data set.   Standard miniseed data
    obtained via that mechanism does not keep source data so such data need
    to be linked to the source collection for any event-based processing.
    This matcher can be used to do that.

    The algorithm used here is very simple.   It looks for data with
    start times in an interval defined by two parameters set in the
    constructor:  t0offset and tolerance.   If we define t0 as the
    start time of a given waveform and t_origin as a test origin
    time, the algorithm looks does a database query to find all
    events matching this inequality relationship:
        t_origin + t0offset - tolerance <= t0 <= t_origin + t0offset + tolerance

    This class uses database queries to find matching source collection
    documents satisfying the above relation.  It can be slow for
    large source collection, especially if the source collection time
    field is not indexed.  A development agenda for MsPASS in the future
    would be to provide an option to cache the source collection
    like some of the other implementations of the NMF base class in this
    module.  Community contributions to implement that are welcome.
    """

    def __init__(
        self,
        db,
        collection="source",
        t0offset=0.0,
        tolerance=4.0,
        time_key=None,
        attributes_to_load=None,
        load_if_defined=None,
        query=None,
        prepend_collection_name=True,
        kill_on_failure=True,
        verbose=True,
        cache_normalization_data=True,
    ):
        """
        Constructor for this class. Includes the important boolean
        that enables or disable caching.

        :param db:  MongoDB Database handle
        :param collection: the string that represents the name of the source
          collection, default value is "source"
        :param t0offset: the offset between t0 and the test origin time, it
        will be used in the query (see class description)
        :param tolerance: the tolerance used in the query to form a time
        range (see class description)
        :param time_key: the key defining t0.
        :param attributes_to_load:  list of keys that will always be loaded
          from each document in the normalization collection satisfying the
          query. Default is None but is initialized in the function body as
          ["lat", "lon", "depth", "time"].
        :param load_if_defined: is like attributes_to_load (a list of
          key strings) but the key-value pairs are not required. Default is
          None (an empty list).
        :param cache_normalization_data:  when True (default) all documents
          satisfying the query parameter in the channel collection will
          be loaded into memory in an internal cache.  When False each
          call to get_document or normalize will invoke a database query
          (find).  (see class description)
        :param kill_on_failure:  When True (the default) any data passed
          processed by the normalize method will be kill if there is no
          match to the id key requested or if the data lack an id key to
          do the match.
        :param query:  (optional) query to pass to find to prefilter the
          data loaded when cache_normalization_data is True.  This argument
          is ignore if cache_normalization_data is False.
        :param prepend_collection_name:   When set True (the default)
          all data pulled from channel will have the prefix "channel_"
          added to the key before they are posted to a data object by
          in the normalize method.  (e.g. "sta" will be posted as "channel_sta").
          That is the standard convention used in MsPASS to tag datat that
          come from normalization like this class does.  Set False only for
          the special case of wanting to load a set of attributes that will
          be renamed downstream and saved in some other schema.
        :param verbose:   when set True (default) the normalize method will
          post informational warnings about duplicate matches. For large
          data sets with a lot of duplicate channel records (e.g. from
          loading errors) consider setting this false to reduce bloat in the
          elog collection.   Normal use should leave it True.
        """
        if attributes_to_load is None:
            attributes_to_load = ["lat", "lon", "depth", "time"]
        if load_if_defined is None:
            load_if_defined = list()

        NMF.__init__(
            self,
            db,
            collection,
            attributes_to_load,
            load_if_defined,
            query,
            prepend_collection_name,
            kill_on_failure,
            verbose,
            cache_normalization_data,
        )

        self.t0offset = t0offset
        self.tolerance = tolerance
        self.time_key = time_key

    def get_document(self, d, time=None):
        if not isinstance(
            d,
            (
                TimeSeries,
                Seismogram,
                TimeSeriesEnsemble,
                SeismogramEnsemble,
                Metadata,
                dict,
            ),
        ):
            raise TypeError(
                "origin_time_source_matcher.get_document:  data received as arg0 is not an atomic MsPASS data object"
            )
        if isinstance(d, dict):
            d_to_use = Metadata(d)
        else:
            d_to_use = d
        return NMF.get_document(self, d_to_use, time)

    def _get_test_time(self, d, time):
        if time == None:
            if self.time_key:
                if d.is_defined(self.time_key):
                    test_time = d[self.time_key] - self.t0offset
                else:
                    return None  # matches logic but may not be right approach here
            else:
                if isinstance(d, (TimeSeries, Seismogram)):
                    test_time = d.t0 - self.t0offset
                else:
                    return None
        else:
            test_time = time - self.t0offset
        return test_time

    def _cached_get_document(self, d, time=None):
        test_time = self._get_test_time(d, time)
        if test_time is None:
            return None

        for _id, doc in self.cache.items():
            time = doc["time"]
            if (
                time >= test_time - self.tolerance
                and time <= test_time + self.tolerance
            ):
                return doc

        if isinstance(d, (TimeSeries, Seismogram)):
            message = "No match for time between {} and {}".format(
                str(UTCDateTime(test_time - self.tolerance)),
                str(UTCDateTime(test_time + self.tolerance)),
            )
            self.log_error(
                d,
                message,
                ErrorSeverity.Invalid,
            )

    def _db_get_document(self, d, time=None):
        # this logic allows setting ensemble metadata using a specific
        # time but if time is not defined we default to using data start time (t0)
        test_time = self._get_test_time(d, time)
        if test_time is None:
            return None

        query = copy.deepcopy(self.query)
        query["time"] = {
            "$gte": test_time - self.tolerance,
            "$lte": test_time + self.tolerance,
        }

        matchsize = self.dbhandle.count_documents(query)
        if matchsize == 0:
            if isinstance(d, (TimeSeries, Seismogram)):
                message = "No match for query = " + str(query)
                self.log_error(
                    d,
                    message,
                    ErrorSeverity.Invalid,
                )
            return None
        elif matchsize > 1 and self.verbose and isinstance(d, (TimeSeries, Seismogram)):
            self.log_error(
                d,
                "multiple source documents match the origin time computed from time received - using first found",
                ErrorSeverity.Complaint,
            )

        match_doc = self.dbhandle.find_one(query)
        return self._load_doc(match_doc, d)


class css30_arrival_interval_matcher(NMF):
    """
    This matcher is used to match phase picks stored in the database
    (default is arrival collection) to waveforms.  The basic algorithm
    is an interval match.  That is, an arrival with a time between
    starttime and endtime is considered a match.   If multiple matches
    are found for same phase name the algorithm uses a time offset test
    of starttime relative to the phase time.  The data for the arrival
    doc with time most closely matched to starttime+time_offset is
    selected.

    The main use of this class is to match a collection of raw data
    with arrival time picks made by another source
    (e.g. the Array Network Facilty of Earthscope css3.0 arrival picks)
    """

    def __init__(
        self,
        db,
        collection="arrival",
        time_offset=60.0,
        phasename="P",
        phasename_key="phase",
        attributes_to_load=None,
        load_if_defined=None,
        query=None,
        prepend_collection_name=True,
        kill_on_failure=False,
        verbose=True,
        cache_normalization_data=None,
    ):
        """
        Constructor for this class.

        :param db:  MongoDB Database handle
        :param collection: the string that represents the name of the source
          collection, default value is "arrival"
        :param time_offset: the offset between t0 and the test origin time, it
        will be used in the query (see class description)
        :param phasename: the phasename used in the query, default is "P".
        :param phasename_key: the name of the phasename's key in the collection,
        default is "phase".
        :param attributes_to_load:  list of keys that will always be loaded
          from each document in the normalization collection satisfying the
          query. Default is None but is initialized in the function body as
          ["time"].
        :param load_if_defined: is like attributes_to_load (a list of
          key strings) but the key-value pairs are not required. Default is
          None but is initialized in the function body as ["evid", "iphase", "seaz",
          "esaz", "deltim", "timeres"].
        :param kill_on_failure:  When True (the default) any data passed
          processed by the normalize method will be kill if there is no
          match to the id key requested or if the data lack an id key to
          do the match.
        :param query:  (optional) query to pass to find to prefilter the
          data loaded when cache_normalization_data is True.  This argument
          is ignore if cache_normalization_data is False.
        :param prepend_collection_name:   When set True (the default)
          all data pulled from channel will have the prefix "channel_"
          added to the key before they are posted to a data object by
          in the normalize method.  (e.g. "sta" will be posted as "channel_sta").
          That is the standard convention used in MsPASS to tag datat that
          come from normalization like this class does.  Set False only for
          the special case of wanting to load a set of attributes that will
          be renamed downstream and saved in some other schema.
        :param verbose:   when set True (default) the normalize method will
          post informational warnings about duplicate matches. For large
          data sets with a lot of duplicate channel records (e.g. from
          loading errors) consider setting this false to reduce bloat in the
          elog collection.   Normal use should leave it True.
        :param cache_normalization_data:  The argument required by NMF to turn on/off
          the caching. Currently the caching for this class is not implemented for
          this class, so the default value is None.
        """
        if attributes_to_load is None:
            attributes_to_load = ["time"]
        if load_if_defined is None:
            load_if_defined = ["evid", "iphase", "seaz", "esaz", "deltim", "timeres"]

        NMF.__init__(
            self,
            db,
            collection,
            attributes_to_load,
            load_if_defined,
            query,
            prepend_collection_name,
            kill_on_failure,
            verbose,
            cache_normalization_data,
        )

        self.phasename = phasename
        self.phasename_key = phasename_key
        self.time_offset = time_offset

    def _get_doc_time(self, d):
        if isinstance(d, (TimeSeries, Seismogram)):
            stime = d.t0
            etime = d.endtime()
            return (stime, etime)
        else:
            if d.is_defined("starttime") and d.is_defined("endtime"):
                stime = d["starttime"]
                etime = d["endtime"]
                return (stime, etime)
            else:
                raise MsPASSError(
                    "css30_arrival_interval_matcher._get_doc_time: can't extract time from input",
                    ErrorSeverity.Fatal,
                )

    def _db_get_document(self, d):
        (stime, etime) = self._get_doc_time(d)

        query = copy.deepcopy(self.query)
        query[self.phasename_key] = self.phasename
        query["time"] = {"$gte": stime, "$lte": etime}

        n = self.dbhandle.count_documents(query)
        if n == 0:
            return None
        else:
            cursor = self.dbhandle.find(query)
            # the key here perhaps should be set in constructor
            # for now it is frozen as this constant
            min_doc = None
            min_abs_dt = sys.float_info.max
            for doc in cursor:
                # ignore any docs with the time attribute not set
                if "time" in doc:
                    abs_dt = abs(doc["time"] - self.time_offset)
                    if abs_dt < min_abs_dt:
                        min_abs_dt = abs_dt
                        min_doc = doc
            if min_doc is None:  # handle these special cases
                raise MsPASSError(
                    "css30_arrival_interval_matcher.get_document:  no arrival docs found with phasename set as"
                    + self.phasename
                    + " with a time attribute defined.  This should not happen and indicates a serious database inconsistence.  Aborting",
                    ErrorSeverity.Fatal,
                )
            return self._load_doc(doc)

    def _cached_get_document(self, d, *args, **kwargs):
        return super()._cached_get_document(d, *args, **kwargs)


def bulk_normalize(
    db,
    wfquery=None,
    wf_col="wf_miniseed",
    blocksize=1000,
    nmf_list=None,
    verbose=False,
):
    """
    This function iterates through the collection specified by db and wf_col,
    and run a chain of normalization funtions in serial on each document in one
    single pass.
    It will save the time of multiple db updating operations, by using the bulk
    methods of MongoDB.

    :param db: should be a MsPASS database handle containing the wf_col
    and the collections defined by the nmf_list list.
    :param wf_col: The collection that need to be normalized, default is
    wf_miniseed
    :param blockssize:   To speed up updates this function uses the
    bulk writer/updater methods of MongoDB that can be orders of
    magnitude faster than one-at-a-time updates. A user should not normally
    need to alter this parameter.
    :param wfquery: is a query to apply to the collection.  The output of this
    query defines the list of documents that the algorithm will attempt
    to normalize as described above.  The default will process the entire
    collection (query set to an emtpy dict).
    :param nmf_list: a list of NMF instances. These instances should at least
    contain a get_document function and a dbhandler. The default will be a simple
    mseed_channel_matcher.
    :param verbose: When set true the get_document and normalize functions will
    be run in verbose mode.  Those methods will print a diagnostic for all
    ambiguous matches.  Because this function is expected to be run on potentially
    large raw data sets of miniseed inputs the default is False to reduce the
    overhead of potentially large log messages created by the all to common
    duplicate metadata problem. Please note that this function will alter the
    verbose levels of all NMF instances in nmf_list.

    :return: a list with a length of len(nmf_list)+1.  0 is the number of documents
    processed in the collection (output of query), The rest are the numbers of
    success normalizations for the corresponding NMF instances, they are mapped
    one on one (nmf_list[x] -> ret[x+1]).
    """

    if wfquery is None:
        wfquery = {}

    if nmf_list is None:
        #   The default value for nmf_list is one default
        channel_matcher = mseed_channel_matcher(
            db,
            attributes_to_load=["_id", "net", "sta", "starttime", "endtime"],
            verbose=verbose,
        )
        nmf_list = [channel_matcher]

    for nmf in nmf_list:
        if not isinstance(nmf, NMF):
            raise MsPASSError(
                "bulk_normalize: the function {} is not a NMF function".format(
                    str(nmf)
                ),
                ErrorSeverity.Fatal,
            )
        nmf.verbose = verbose

    ndocs = db[wf_col].count_documents(wfquery)
    if ndocs == 0:
        raise MsPASSError(
            "bulk_normalize: "
            + "query of wf_miniseed yielded 0 documents\nNothing to process",
            ErrorSeverity.Fatal,
        )

    cnt_list = [0] * len(nmf_list)
    counter = 0

    cursor = db[wf_col].find(wfquery)
    bulk = []
    for doc in cursor:
        wf_id = doc["_id"]
        wf_stime = doc["starttime"]
        need_update = False
        update_doc = {}
        for ind, nmf in enumerate(nmf_list):
            try:
                norm_doc = nmf.get_document(doc, time=wf_stime)
                if norm_doc is None:
                    continue
                for key in nmf.attributes_to_load:
                    new_key = key
                    if nmf.prepend_collection_name:
                        #   We assume that every NMF should contain a dbhandler
                        # handle _id specially to avoid double _
                        if key == "_id":
                            new_key = nmf.dbhandle.name + key
                        else:
                            new_key = nmf.dbhandle.name + "_" + key
                    update_doc[new_key] = norm_doc[key]
            except TypeError:  # Some NMF dervied classes don't accept time argument
                norm_doc = nmf.get_document(doc)
                if norm_doc is None:
                    continue
                for key in nmf.attributes_to_load:
                    new_key = key
                    if nmf.prepend_collection_name:
                        # again handle _id specially to avoid double _
                        if key == "_id":
                            new_key = nmf.dbhandle.name + key
                        else:
                            new_key = nmf.dbhandle.name + "_" + key
            #   If we reach here, we've got a norm_doc return
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
    normalize_site=False,
    verbose=False,
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
    be set wf_miniseed document as site_id.
    :param verbose: When set true the database methods for matching the
    net:sta:chan:loc:time keys will be run in verbose mode.  Those database
    methods will print a diagnostic for all ambiguous matches.  Because
    this function is expected to be run on potentially large raw data sets of
    miniseed inputs the default is False to reduce the overhead of potentially
    large log messages created by the all to common duplicate metadata problem.
    Users are encouraged to verify the channel and site collections have
    no serious problems with ambiguous net:sta:loc(chan) that are truly
    inconsistent (i.e. have different attributes for the same keys)

    :return: list with three integers.  0 is the number of documents processed in
    wf_miniseed (output of query), 1 is the number with channel ids set,
    and 2 contains the number of site documents set.  1 or 2 should
    contain 0 if normalization for that collection was set false.
    """

    if wfquery is None:
        wfquery = {}

    nmf_function_list = []
    if normalize_channel:
        matcher = mseed_channel_matcher(
            db,
            attributes_to_load=[
                "_id"
            ],  #   We only want to add channel_id to the db collection
            verbose=False,
        )
        nmf_function_list.append(matcher)

    if normalize_site:
        sitematcher = mseed_site_matcher(
            db,
            attributes_to_load=["_id"],
            verbose=False,
        )
        nmf_function_list.append(sitematcher)

    bulk_nml_ret = bulk_normalize(
        db,
        wfquery=wfquery,
        wf_col="wf_miniseed",
        blocksize=blocksize,
        nmf_list=nmf_function_list,
        verbose=verbose,
    )

    ret = [bulk_nml_ret[0]]
    if normalize_channel:
        ret.append(bulk_nml_ret[1])
        if normalize_site:
            ret.append(bulk_nml_ret[2])
        else:
            ret.append(0)
    else:
        ret.append(0)
        if normalize_site:
            ret.append(bulk_nml_ret[1])
        else:
            ret.append(0)
    return ret
