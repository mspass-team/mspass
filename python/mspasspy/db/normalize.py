from abc import ABC, abstractmethod
from mspasspy.ccore.utility import MsPASSError, ErrorSeverity, Metadata
from mspasspy.ccore.seismic import (
    TimeSeries,
    Seismogram,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
)

from obspy import UTCDateTime
from pkg_resources import require
from pyrsistent import optional

# this is copied from edit.py.  easier to duplicate than load that entire
# module with this one but collaborators you may want to change that
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


def _load_normalization_cache(
    db, collection, required_attributes=None, optional_attributes=None, query={}
):
    """
    This is a function internal to the matcher module used to standardize
    the loading of normalization data from MongoDB. It returns a python
    dict with keys defined by the string representation of each document
    found in the normalizing collection.   The value associated with each
    key is a Metadata container of a (usually) reduced set of data that
    is to be merged with the Metadata of a set of mspass data objects
    to produce the "normalization".

    :param db: MongoDB database handle
    :param collection:  database collection to be indexed to define the cache
    :param required_attributes:  list of key for attributes the function will
      dogmatically try to extract from each document.   If any of these are
      missing in any collection the function will abort with a MsPASSError
      exception
    :param optional_attributes:  Treated like required_attributes but
       if these are missing they are silently ignored an not posted to the
       python dict containers associated with each id string.
    :param query:  optional query to apply before loading the normalizing
       collection defined by the collection argument.  By default the
       entire collection is loaded and returned.   This can be useful with
       large collection to reduce memory bloat.  e.g. if you have a large
       collection of channel data but your data set only spans a 1 year
       period you might set a query to only load data for stations
       running during that time period.

    """
    if required_attributes is None:
        required_attributes = []
    if optional_attributes is None:
        optional_attributes = []
    dbcol = db[collection]
    cursor = dbcol.find(query)
    normcache = dict()
    for doc in cursor:
        mdresult = Metadata()
        for key in required_attributes:
            if key in doc:
                mdresult[key] = doc[key]
            else:
                raise MsPASSError(
                    "_load_normalization_cache:   required attribute with key = "
                    + key
                    + " not found",
                    ErrorSeverity.Fatal,
                )
        for key in optional_attributes:
            if key in doc:
                mdresult[key] = doc[key]
        cache_key = str(doc["_id"])  # always defined for a MongoDB doc
        normcache[cache_key] = mdresult
    return normcache


class NMF(ABC):
    """
    Abstract base class for a family of Normalization Match Functions (NMF).
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

    def __init__(self, kill_on_failure=True, verbose=False):
        """
        Base class constructor.   The implementation requires two
        defaulted parameters that most subclasses can find useful.

        :param kill_on_failure:  when set true (Default) match errors
          will cause data passed to the normalize method to be killed.
        :param verbose:  most subclasses will want a verbose option
          to control what is posted to elog messages or printed
          (most useful for serial jobs)
        """
        self.kill_on_failure = kill_on_failure
        # this list is always needed for normalize method.  Here we just
        # initialize it to an empty list to avoid that step in all subclasses
        self.attributes_to_load = []
        self.verbose = verbose

    def __call__(self, d):
        """
        This convenience method allows a concrete instance to be
        called with the simpler syntax with the (implied) principle
        method "normalize".   e.g. to normalize d with the
        channel collection using id_matcher you can use
        d = id_matcher(d)  instead of d = id_matcher.normalize(d)
        """
        self.normalize(d)

    @abstractmethod
    def get_document(self, d):
        pass

    @abstractmethod
    def normalize(self, d):
        pass

    def log_error(
        self, d, matchername, message, kill=False, severity=ErrorSeverity.Informational
    ):
        """
        This base class method is used to standardize the error logging
        functionality of all NMF objects.   It writes a standardized
        message to simplify writing of subclasses - they need only
        define the matchername (normally the name of the subclass) and
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
        :param matchername: is the string assigned to the "algorithm" field
        of the message posted to d.elog.
        :param message:  specialized message to post - this string is added
        to an internal generic message.
        :param kill:  boolean controlling if the message should cause the
        datum to be killed.   Default False meaning only the message
        is posted.
        :param severity:  ErrorSeverity to assign to elog message
        (See ErrorLogger docstring).  Default is Informational
        """
        if _input_is_valid(d):
            fullmessage = message
            if kill:
                d.kill()
                fullmessage += "\nDatum was killed"
            if not hasattr(self, "verbose") or self.verbose is True:
                d.elog.log_error(matchername, fullmessage, severity)
        else:
            raise MsPASSError(
                "NMF.log_error method received invalid data;  arg 0 must be a MsPASS data object",
                ErrorSeverity.Fatal,
            )


class ID_matcher(NMF):
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
        kill_on_failure=True,
        cache_normalization_data=True,
        query={},
        verbose=True,
    ):
        """
        Constructor for this class.

        :param db:  MongoDB Database handle
        :param collection:   string defining the collection this object
          should use for normalization.   If this argument is not a valid
          string the constructor will abort with a TypeError exception.
          Default is "channel"
        :param attributes_to_load:  is a list of keys (strings) that are to
          be loaded with data by the normalize method.  =Default is a list of
          common channel attributes:  "lat", "lon", "elev", "hang", and "vang"
        :param load_if_defined:   is a secondary list of keys (strings) that
          should be loaded only if they are defined.  A type example is the
          seed "loc" code that isn't always used.  Default is an empty list.
        :param kill_on_failure:  When True (the default) any data passed
          processed by the normalize method will be kill if there is no
          match to the id key requested or if the data lack an id key to
          do the match.
        :param cache_normalization_data:  When set True (the default)
          the specified collection is preloaded in an internal cache
          on construction and used for all subsequent matching.  This mode
          is highly recommended as it has been found to speed normalization
          by an order of magnitude or more relative to a database
          transaction for each call to normalize, which is what happens when
          this parameter is set False.
        :param query:  optional query to apply to collection before loading.
          The default is load all.  If your data set is time limited and
          the collection has a time attribute (true of the standard channel,
          site, and source collections) you can reduce the memory footprint
          by using a time range query (python dict) for this argument.
        :param verbose:  most subclasses will want a verbose option
          to control what is posted to elog messages or printed
        """
        if isinstance(collection, str):
            super().__init__(kill_on_failure, verbose)
            self.collection = collection
            self.mdkey = collection + "_id"
            self.dbhandle = db[collection]
            # assume type errors will be thrown if attributes_to_load is not array like
            # this is attributes_to_load is initialized to an empty list in
            # super()
            if attributes_to_load is None:
                attributes_to_load = ["lat", "lon", "elev", "hang", "vang"]
            if load_if_defined is None:
                load_if_defined = []

            for x in attributes_to_load:
                self.attributes_to_load.append(x)
            self.load_if_defined = list()
            for x in load_if_defined:
                self.load_if_defined.append(x)
            self.cache_normalization_data = cache_normalization_data

            if self.cache_normalization_data:
                # We dogmatically require prepend_collection_name=True
                self.cache = _load_normalization_cache(
                    db,
                    collection,
                    required_attributes=self.attributes_to_load,
                    optional_attributes=self.load_if_defined,
                    query=query,
                )
            else:
                self.cache = dict()

        else:
            raise TypeError(
                "ID_matcher constructor:  arg0 must be a collection name - received invalid type"
            )

    def get_document(self, d):
        """
        Implementation of the get_document class for this class.  The
        document, in this case, is actually a MsPASS Metadata container.
        Only attributes defined by the attribute_to_load and load_if_defined
        lists will be returned in the result.  For standard use that
        data will be returned from the internal cache of that list of
        attributes.  If caching was turned off in construction a
        database query will be invoked for each call to this method.
        Note in that case the type of the return will be different; a
        python dict of the entire document contents instead of a Metadata
        container with only the attributes cached on construction.

        Any failures will cause d to be marked dead if kill_on_failure
        was set in the constructor (the default).  The only exception is
        attributes in the load_if_defined are not considered required so
        if they are missing it is not considered an error.

        :param d:  Data object with a Metadata container to be tested.
        That means this can be any valid MsPASS data object or even
        a raw Metadata container.  Only the class defined id key is
        accessed by d.  That id drives the algorithm as described above.

        :return:  Metadata container with the matching data when
        caching is enabled. A python dict of the entire matching
        document when caching is off. Returns None if there is not match
        AND posts a message to elog of d.
        """

        if d.is_defined(self.mdkey):
            testid = d[self.mdkey]
        else:
            message = (
                "Normalizing ID with key="
                + self.mdkey
                + " is not defined in this object"
            )
            self.log_error(
                d,
                "ID_matcher.get_document",
                message,
                self.kill_on_failure,
                ErrorSeverity.Invalid,
            )
            return None
        if self.cache_normalization_data:
            result = self.cache[testid]
        else:
            query = {"_id": d[testid]}
            doc = self.dbhandle.find_ond(query)
            # TODO: Do we only need to find one doc here?
            # For consistency we have to copy doc into a Metadata container
            # for this situation - doc is a MongoDB document container and
            # may contain other attributes so we do a selective copy for consistency
            result = Metadata()
            for key in self.attributes_to_load:
                if key in doc:
                    result[key] = doc[key]
                else:
                    message = (
                        "Required key="
                        + key
                        + " not found in normalization collection = "
                        + self.collection
                    )
                    self.log_error(
                        d,
                        "ID_matcher.get_document",
                        message,
                        self.kill_on_failure,
                        ErrorSeverity.Invalid,
                    )
            for key in self.load_if_defined:
                if key in doc:
                    result[key] = doc[key]
        return result

    def normalize(self, d, prepend_collection_name=True):
        """
        Implementation of the normalize method for this class.  This method
        first tests if the input is a valid MsPASS data object.  It will
        silently do nothing if the data are not valid returning a None.
        It then tests if the datum is marked live.  If it is found marked
        dead it silently returns d with no changes.   For live data it calls the get_document
        method.  If that succeeds it extracts the (constructor defined) list of
        desired attributes and posts them to the data's Metadata container.
        If get_document fails a message is posted to elog and if the
        constructor defined "kill_on_failure" parameter is set True the
        returned datum will be killed.

        :param d:  data to be normalized.  This must be a MsPASS data object.
          For this function that means TimeSeries, Seismogram, TimeSeriesEnsemble,
          or SeismogramEnsemble.  Not for ensembles the normalizing data will
          be posted to the ensemble metadata container not the members.
          This can be used, for example, to normalize a parallel container
          (rdd or bad) of common source gathers more efficiently than
          at the atomic level.
        :param prepend_collection_name:  boolean controlling a standard
          renaming option.   When True (default)   all normalizing data
          keys get a collection name prepended to the key to give it a
          unique key.  e.g. if loading data from "channel" the "lat"
          (latitude of the instrument's location) field will be changed on
           posting to d to "channel_lat".   Setting this false should be
           a rare or never used option and should be done only if you deeply
           understand the consequences.
        """
        if _input_is_valid(d):
            if d.dead():
                return d
            doc = self.get_document(d)
            if doc == None:
                message = (
                    "No matching _id found for "
                    + self.mdkey
                    + " in collection="
                    + self.collection
                )
                self.log_error(
                    d,
                    "ID_matcher",
                    message,
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )
            else:
                # In this implementation the contents of doc have been prefiltered
                # to contain only those in the attributes_to_load or load_if_defined lists
                # Hence we copy all.
                for key in doc:
                    if prepend_collection_name:
                        val = doc[key]
                        newkey = self.collection + "_" + key
                        d[newkey] = doc[key]
                    else:
                        d[key] = doc[key]

            return d
        else:
            # land here if d was not a valid datum.
            raise TypeError("ID_matcher.normalize:  received invalid data type")


def _channel_composite_key(net, sta, chan, loc, separator="_"):
    """
    Returns a composite key that is unique for a seed channel defined
    (by default) a net_sta_chan_loc.  Optional separator argument
    can be used to use a different separator character.
    Loc is often null so choose to not include a trailing separator
    when that is the case.  Similarly, when chan is a zero length
    string it is omitted.  That allows this same function to be used for
    site and channel
    """
    key = net + separator + sta
    if len(chan) > 0:
        key = key + separator + chan
    if len(loc) > 0:
        key = key + separator + loc
    return key


class mseed_channel_matcher(NMF):
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
        attributes_to_load=None,
        load_if_defined=None,
        cache_normalization_data=True,
        query={},
        readonly_tag="READONLYERROR_",
        prepend_collection_name=True,
        kill_on_failure=True,
        verbose=True,
    ):
        """
        Constructor for this class.  Includes the important boolean
        that enables or disables caching.

        :param db:  MongoDB Database handle
        :param attributes_to_load:  list of keys that will always be loaded
          from each document in the normalization collection satisfying the
          query.   Note the constructor will abort with a MsPASSError if
          any documents are missing one of these key-value pairs.
        :param load_if_defined: is like attributes_to_load (a list of
          key strings) but the key-value pairs are not required.
        :param cache_normalization_data:  when True (default) all documents
          satisfying the query parameter in the channel collection will
          be loaded into memory in an internal cache.  When False each
          call to get_document or normalize will invoke a database query
          (find).  (see class description)
        :param query:  (optional) query to pass to find to prefilter the
          data loaded when cache_normalization_data is True.  This argument
          is ignore if cache_normalization_data is False.
        :param readonly_tag:  As noted in the class docstring attributes
          marked read only in the schema can sometimes be saved with a
          prefix.  The get_document and normalize methods have an auto
          recover to look try to match read only parameters.  This
          argument defines the prefix used to define such attributes.
          The default is "READONLYERROR_" which is what is used by
          default in MsPASS.  Few if any users will likely need to
          ever set this parameter.
        :param prepend_collection_name:   When set True (the default)
          all data pulled from channel will have the prefix "channel_"
          added to the key before they are posted to a data object by
          in the normalize method.  (e.g. "sta" will be posted as "channel_sta").
          That is the standard convention used in MsPASS to tag datat that
          come from normalization like this class does.  Set False only for
          the special case of wanting to load a set of attributes that will
          be renamed downstream and saved in some other schema.
        :param kill_on_failure:  When True (the default) any data passed
          processed by the normalize method will be kill if there is no
          match to the id key requested or if the data lack an id key to
          do the match.
        :param verbose:   when set True (default) the normalize method will
          post informational warnings about duplicate matches. For large
          data sets with a lot of duplicate channel records (e.g. from
          loading errors) consider setting this false to reduce bloat in the
          elog collection.   Normal use should leave it True.

        """
        super().__init__(kill_on_failure, verbose)
        self.dbhandle = db["channel"]

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
        # assume type errors will be thrown if attributes_to_load is not array like
        self.attributes_to_load = list()
        for x in attributes_to_load:
            self.attributes_to_load.append(x)
        self.load_if_defined = list()
        for x in load_if_defined:
            self.load_if_defined.append(x)
        if cache_normalization_data:
            self.cache_normalization_data = cache_normalization_data
            # We dogmatically require prepend_collection_name=True
            self.cache = _load_normalization_cache(
                db,
                "channel",
                required_attributes=self.attributes_to_load,
                optional_attributes=self.load_if_defined,
                query=query,
            )
            self.xref = self._build_xref()
        else:
            self.cache = dict()
            self.xref = dict()

        self.readonly_tag = readonly_tag
        self.prepend_collection_name = prepend_collection_name

    def _build_xref(self):
        """
        Used by constructor to build mseed cross reference dict
        with mseed key and list of object_ids matching for each unique
        key
        """
        xref = dict()
        for id, md in self.cache.items():
            net = md["net"]
            sta = md["sta"]
            chan = md["chan"]
            if md.is_defined("loc"):
                loc = md["loc"]
            else:
                loc = ""
            key = _channel_composite_key(net, sta, chan, loc)
            if key in xref:
                xref[key].append(id)
            else:
                xref[key] = [id]  # initializes array of id strings
        return xref

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

    def _cached_get_document(self, d, time=None):
        """
        Private method to do the work of the get_document method when channel
        data have been previously cached.
        """
        # do this test once to avoid repetitious calls later - minimal cost
        error_logging_allowed = isinstance(d, (TimeSeries, Seismogram))

        if d.is_defined("net"):
            net = d["net"]
        elif d.is_defined(self.readonly_tag + "net"):
            net = d[self.readonly_tag + "net"]
        else:
            if error_logging_allowed:
                self.log_error(
                    d,
                    "mseed_channel_matcher",
                    "Required match key=net or "
                    + self.readonly_tag
                    + "net are not defined for this datum",
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )
            return None

        if d.is_defined("sta"):
            sta = d["sta"]
        elif d.is_defined(self.readonly_tag + "sta"):
            sta = d[self.readonly_tag + "sta"]
        else:
            if error_logging_allowed:
                self.log_error(
                    d,
                    "mseed_channel_matcher",
                    "Required match key=sta or "
                    + self.readonly_tag
                    + "sta are not defined for this datum",
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )
            return None

        if d.is_defined("chan"):
            chan = d["chan"]
        elif d.is_defined(self.readonly_tag + "chan"):
            chan = d[self.readonly_tag + "chan"]
        else:
            if error_logging_allowed:
                self.log_error(
                    d,
                    "mseed_channel_matcher",
                    "Required match key=chan or "
                    + self.readonly_tag
                    + "chan are not defined for this datum",
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )
                return None

        # loc has to be handled differently because it is often not defined
        # We just don't add loc to the query if it isn't defined
        if d.is_defined("loc"):
            loc = d["loc"]
        elif d.is_defined(self.readonly_tag + "loc"):
            loc = d[self.readonly_tag + "loc"]
        else:
            loc = ""  # this cas is assumed handled by _channel_composite_key

        key = _channel_composite_key(net, sta, chan, loc)
        # Try to recover if time is not explicitly passed as an arg
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
        if key in self.xref:
            doclist = self.xref[key]
            # avoid a test and assume this is a match if there is only
            # one entry in the list
            if len(doclist) == 1:
                idkey = doclist[0]
                return self.cache[idkey]
            # When time is not defined (None) just return first entry
            # but post a warning
            if test_time == None:
                if error_logging_allowed:
                    message = "Warning - no time specified for match and data has no starttime field defined.  Using first match found in channel collection"
                    self.log_error(
                        d,
                        "mseed_channel_matcher._cached_get_document",
                        message,
                        False,
                        ErrorSeverity.Suspect,
                    )
                idkey = doclist[0]
                return self.cache[idkey]
            for idkey, doc in doclist.items():
                stime = doc["starttime"]
                etime = doc["endtime"]
                if test_time >= stime and test_time <= etime:
                    return doc
            if error_logging_allowed:
                message = (
                    "No match for net_sta_chan_loc ="
                    + key
                    + " and time="
                    + str(UTCDateTime(test_time))
                )
                self.log_error(
                    d,
                    "mseed_channel_matcher._cached_get_document",
                    message,
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )
            return None
        else:
            if error_logging_allowed:
                message = (
                    "No entries are present in channel collection for net_sta_chan_loc = "
                    + key
                )
                self.log_error(
                    d,
                    "mseed_channel_matcher._cached_get_document",
                    message,
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )
            return None

    def _db_get_document(self, d, time=None):
        """
        Private method that does the work of get_document when caching is
        turned off.   This method does one database transaction per call.
        """
        # do this test once to avoid repetitious calls later - minimal cost
        error_logging_allowed = isinstance(TimeSeries, Seismogram)
        query_is_ok = True
        query = {}
        if d.is_defined("net"):
            query["net"] = d["net"]
        elif d.is_defined(self.readonly_tag + "net"):
            query["net"] = d[self.readonly_tag + "net"]
        else:
            query_is_ok = False
            if error_logging_allowed:
                self.log_error(
                    d,
                    "mseed_channel_matcher",
                    "Required match key=net or "
                    + self.readonly_tag
                    + "net are not defined for this datum",
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )
        # We repeat the above logic for sta and chan for debugging but it
        # could cause bloated elog messages if a user makes a dumb error
        # with a large data set.  that seems preferable to mysterious behavior
        # could make it a verbose option but for now we will always blunder on
        if d.is_defined("sta"):
            query["sta"] = d["sta"]
        elif d.is_defined(self.readonly_tag + "sta"):
            query["sta"] = d[self.readonly_tag + "sta"]
        else:
            query_is_ok = False
            if error_logging_allowed:
                self.log_error(
                    d,
                    "mseed_channel_matcher",
                    "Required match key=sta or "
                    + self.readonly_tag
                    + "sta are not defined for this datum",
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )

        if d.is_defined("chan"):
            query["chan"] = d["chan"]
        elif d.is_defined(self.readonly_tag + "chan"):
            query["chan"] = d[self.readonly_tag + "chan"]
        else:
            query_is_ok = False
            if error_logging_allowed:
                self.log_error(
                    d,
                    "mseed_channel_matcher",
                    "Required match key=chan or "
                    + self.readonly_tag
                    + "chan are not defined for this datum",
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )

        # loc has to be handled differently because it is often not defined
        # We just don't add loc to the query if it isn't defined
        if d.is_defined("loc"):
            query["loc"] = d["loc"]
        elif d.is_defined(self.readonly_tag + "loc"):
            query["loc"] = d[self.readonly_tag + "loc"]

        # return now if this datum has been marked dead
        if not query_is_ok:
            return None

        # default to data start time if time is not explicitly passed
        if time == None:
            if isinstance(d, (TimeSeries, Seismogram)):
                querytime = d.t0
            else:
                if d.is_defined("starttime"):
                    querytime = d["starttime"]
                else:
                    # Use None for test_time as a signal to ignore time field
                    querytime = None
        else:
            querytime = time

        if querytime is not None:
            query["starttime"] = {"$lt": querytime}
            query["endtime"] = {"$gt": querytime}

        matchsize = self.dbhandle.count_documents(query)
        if matchsize == 0:
            return None
        if matchsize > 1 and self.verbose and error_logging_allowed:
            self.log_error(
                d,
                "mseed_channel_matcher",
                "Multiple channel docs match net:sta:chan:loc:time for this datum - using first one found",
                False,
                ErrorSeverity.Complaint,
            )
        return self.dbhandle.find_one(query)

    def normalize(self, d, time=None):
        """
        Implementation of the normalize method for this class.

        :param d:  input data object to be normalized.  Must be a TimeSeries
          or Seismogram object.  If d is anything else the function will
          silently return None.
        """
        if d.dead():
            return d
        if _input_is_atomic(d):
            doc = self.get_document(d, time)
            if doc == None:
                message = "No matching document was found in channel collection for this datum"
                self.log_error(
                    d,
                    "mseed_channel_matcher",
                    message,
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )
            else:
                for key in self.attributes_to_load:
                    if key in doc:
                        if self.prepend_collection_name:
                            mdkey = "channel_" + key
                        else:
                            mdkey = key
                        d[mdkey] = doc[key]
                    else:
                        # We accumulate error messages to aid user debugging
                        # but it could create bloated elog collections
                        message = (
                            "No data for key="
                            + self.mdkey
                            + " in document returned from collection="
                            + self.collection
                        )
                        self.log_error(
                            d,
                            "mseed_channel_matcher",
                            message,
                            self.kill_on_failure,
                            ErrorSeverity.Invalid,
                        )
            # Notice logic that if no match is found we log the error
            # (and usually kill it) and land here. Allows application in a map
            # operation
            return d
        else:
            raise TypeError(
                "mseed_channel_matcher.normalize:  received invalid data type"
            )


# this class is modified form mseed_channel_matcher removing the chan
# key and replacing channel by site.
# collaborators:   there may be relic and inconsistencies with the channel
# version. That needs to be checked.  The docstrings for this class are
# also not completed - glp 4/12/2022 - remove these comments for release
class mseed_site_matcher(NMF):
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
    """

    def __init__(
        self,
        db,
        attributes_to_load=None,
        load_if_defined=None,
        cache_normalization_data=True,
        query={},
        readonly_tag="READONLYERROR_",
        prepend_collection_name=True,
        kill_on_failure=True,
        verbose=True,
    ):
        super().__init__(kill_on_failure, verbose)
        self.dbhandle = db["site"]

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
        # assume type errors will be thrown if attributes_to_load is not array like
        self.attributes_to_load = list()
        for x in attributes_to_load:
            self.attributes_to_load.append(x)
        self.load_if_defined = list()
        for x in load_if_defined:
            self.load_if_defined.append(x)
        if cache_normalization_data:
            self.cache_normalization_data = cache_normalization_data
            # We dogmatically require prepend_collection_name=True
            self.cache = _load_normalization_cache(
                db,
                "site",
                required_attributes=self.attributes_to_load,
                optional_attributes=self.load_if_defined,
                query=query,
            )
            self.xref = self._build_xref()
        else:
            self.cache = dict()
            self.xref = dict()

        self.readonly_tag = readonly_tag
        self.prepend_collection_name = prepend_collection_name

    def _build_xref(self):
        """
        Used by constructor to build mseed cross reference dict
        with mseed key and list of object_ids matching for each unique
        key
        """
        xref = dict()
        for id, md in self.cache.items():
            net = md["net"]
            sta = md["sta"]
            chan = ""
            if md.is_defined("loc"):
                loc = md["loc"]
            else:
                loc = ""
            key = _channel_composite_key(net, sta, chan, loc)
            if key in xref:
                xref[key].append(id)
            else:
                xref[key] = [id]  # initializes array of id strings
        return xref

    def get_document(self, d, time=None):
        if not isinstance(d, (TimeSeries, Seismogram, Metadata, dict)):
            raise TypeError(
                "mseed_site_matcher.get_document:  data received as arg0 is not an atomic MsPASS data object"
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

    def _cached_get_document(self, d, time=None):
        """
        Private method to do work of get_document method when site
        data have been previously cached.
        """
        # do this test once to avoid repetitious calls later - minimal cost
        error_logging_allowed = isinstance(d, (TimeSeries, Seismogram))

        if d.is_defined("net"):
            net = d["net"]
        elif d.is_defined(self.readonly_tag + "net"):
            net = d[self.readonly_tag + "net"]
        else:
            if error_logging_allowed:
                self.log_error(
                    d,
                    "mseed_site_matcher",
                    "Required match key=net or "
                    + self.readonly_tag
                    + "net are not defined for this datum",
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )
            return None

        if d.is_defined("sta"):
            sta = d["sta"]
        elif d.is_defined(self.readonly_tag + "sta"):
            sta = d[self.readonly_tag + "sta"]
        else:
            if error_logging_allowed:
                self.log_error(
                    d,
                    "mseed_site_matcher",
                    "Required match key=sta or "
                    + self.readonly_tag
                    + "sta are not defined for this datum",
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )
            return None

        # loc has to be handled differently because it is often not defined
        # We just don't add loc to the query if it isn't defined
        if d.is_defined("loc"):
            loc = d["loc"]
        elif d.is_defined(self.readonly_tag + "loc"):
            loc = d[self.readonly_tag + "loc"]
        else:
            loc = ""  # this cas is assumed handled by _channel_composite_key

        # always set to a zero length string to allow use of this common function
        chan = ""

        key = _channel_composite_key(net, sta, chan, loc)
        # Try to recover if time is not explicitly passed as an arg
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
        if key in self.xref:
            doclist = self.xref[key]
            # avoid a test and assume this is a match if there is only
            # one entry in the list
            if len(doclist) == 1:
                idkey = doclist[0]
                return self.cache[idkey]
            # When time is not defined (None) just return first entry
            # but post a warning
            if test_time == None:
                if error_logging_allowed:
                    message = "Warning - no time specified for match and data has no starttime field defined.  Using first match found in channel collection"
                    self.log_error(
                        d,
                        "mseed_site_matcher._cached_get_document",
                        message,
                        False,
                        ErrorSeverity.Suspect,
                    )
                idkey = doclist[0]
                return self.cache[idkey]
            for idkey, doc in doclist:
                stime = doc["starttime"]
                etime = doc["endtime"]
                if test_time >= stime and test_time <= etime:
                    return doc
            if error_logging_allowed:
                message = (
                    "No match for net_sta_chan_loc ="
                    + key
                    + " and time="
                    + str(UTCDateTime(test_time))
                )
                self.log_error(
                    d,
                    "mseed_site_matcher._cached_get_document",
                    message,
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )
            return None
        else:
            if error_logging_allowed:
                message = (
                    "No entries are present in channel collection for net_sta_chan_loc = "
                    + key
                )
                self.log_error(
                    d,
                    "mseed_site_matcher._cached_get_document",
                    message,
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )
            return None

    def _db_get_document(self, d, time=None):
        # do this test once to avoid repetitious calls later - minimal cost
        error_logging_allowed = isinstance(TimeSeries, Seismogram)
        query_is_ok = True
        query = {}
        if d.is_defined("net"):
            query["net"] = d["net"]
        elif d.is_defined(self.readonly_tag + "net"):
            query["net"] = d[self.readonly_tag + "net"]
        else:
            query_is_ok = False
            if error_logging_allowed:
                self.log_error(
                    d,
                    "mseed_site_matcher",
                    "Required match key=net or "
                    + self.readonly_tag
                    + "net are not defined for this datum",
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )
        # We repeat the above logic for sta and chan for debugging but it
        # could cause bloated elog messages if a user makes a dumb error
        # with a large data set.  that seems preferable to mysterious behavior
        # could make it a verbose option but for now we will always blunder on
        if d.is_defined("sta"):
            query["sta"] = d["sta"]
        elif d.is_defined(self.readonly_tag + "sta"):
            query["sta"] = d[self.readonly_tag + "sta"]
        else:
            query_is_ok = False
            if error_logging_allowed:
                self.log_error(
                    d,
                    "mseed_site_matcher",
                    "Required match key=sta or "
                    + self.readonly_tag
                    + "sta are not defined for this datum",
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )

        # loc has to be handled differently because it is often not defined
        # We just don't add loc to the query if it isn't defined
        if d.is_defined("loc"):
            query["loc"] = d["loc"]
        elif d.is_defined(self.readonly_tag + "loc"):
            query["loc"] = d[self.readonly_tag + "loc"]

        # return now if this datum has been marked dead
        if not query_is_ok:
            return None

        # default to data start time if time is not explicitly passed
        if time == None:
            if isinstance(d, (TimeSeries, Seismogram)):
                querytime = d.t0
            else:
                if d.is_defined("starttime"):
                    querytime = d["starttime"]
                else:
                    # Use None for test_time as a signal to ignore time field
                    querytime = None
        else:
            querytime = time

        if querytime is not None:
            query["starttime"] = {"$lt": querytime}
            query["endtime"] = {"$gt": querytime}

        matchsize = self.dbhandle.count_documents(query)
        if matchsize == 0:
            return None
        if matchsize > 1 and self.verbose and error_logging_allowed:
            self.log_error(
                d,
                "mseed_site_matcher",
                "Multiple channel docs match net:sta:loc:time for this datum - using first one found",
                False,
                ErrorSeverity.Complaint,
            )
        return self.dbhandle.find_one(query)

    def normalize(self, d, time=None):
        if d.dead():
            return d
        if _input_is_atomic(d):
            doc = self.get_document(d, time)
            if doc == None:
                message = (
                    "No matching document was found in site collection for this datum"
                )
                self.log_error(
                    d,
                    "mseed_site_matcher",
                    message,
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )
            else:
                for key in self.attributes_to_load:
                    if key in doc:
                        if self.prepend_collection_name:
                            mdkey = "site_" + key
                        else:
                            mdkey = key
                        d[mdkey] = doc[key]
                    else:
                        # We accumulate error messages to aid user debugging
                        # but it could create bloated elog collections
                        message = (
                            "No data for key="
                            + self.mdkey
                            + " in document returned from collection="
                            + self.collection
                        )
                        self.log_error(
                            d,
                            "mseed_site_matcher",
                            message,
                            self.kill_on_failure,
                            ErrorSeverity.Invalid,
                        )
            # Notice logic that if no match is found we log the error
            # (and usually kill it) and land here. Allows application in a map
            # operation
            return d
        else:
            raise TypeError("mseed_site_matcher.normalize:  received invalid data type")


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
        attributes_to_load=None,
        kill_on_failure=True,
        prepend_collection_name=True,
        verbose=True,
    ):
        """ """
        super().__init__(kill_on_failure, verbose)
        self.collection = collection
        self.dbhandle = db[collection]
        self.t0offset = t0offset
        self.tolerance = tolerance
        self.prepend_collection_name = prepend_collection_name

        if attributes_to_load is None:
            attributes_to_load = ["lat", "lon", "depth", "time"]
        for x in attributes_to_load:
            self.attributes_to_load.append(x)

    def get_document(self, d, time=None):
        """ """
        if _input_is_valid(d):
            # this logic allows setting ensemble metadata using a specific
            # time but if time is not defined we default to using data start time (t0)
            if time == None:
                test_time = d.t0 - self.t0offset
            else:
                test_time = time - self.t0offset
            query = {
                "time": {
                    "$ge": test_time - self.tolerance,
                    "$le": test_time + self.tolerance,
                }
            }

            matchsize = self.dbhandle.count_documents(query)
            if matchsize == 0:
                return None
            elif matchsize > 1 and self.verbose:
                self.log_error(
                    d,
                    "origin_time_source_matcher",
                    "multiple source documents match the origin time computed from time received - using first found",
                    ErrorSeverity.Complaint,
                )
            return self.dbhandle.find_one(query)

        else:
            return None

    def normalize(self, d, time=None):
        if d.dead():
            return d
        if _input_is_valid(d):
            doc = self.get_document(d, time)
            if doc == None:
                message = (
                    "No matching document was found in"
                    + self.collection
                    + " collection for this datum"
                )
                self.log_error(
                    d,
                    "origin_time_source_matcher",
                    message,
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )
            else:
                for key in self.attributes_to_load:
                    if key in doc:
                        if self.prepend_collection_name:
                            mdkey = self.collection + "_" + key
                        else:
                            mdkey = key
                        d[mdkey] = doc[key]
                    else:
                        # We accumulate error messages to aid user debugging
                        # but it could create bloated elog collections
                        message = (
                            "No data for key="
                            + self.mdkey
                            + " in document returned from collection="
                            + self.collection
                        )
                        self.log_error(
                            d,
                            "origin_time_source_matcher",
                            message,
                            self.kill_on_failure,
                            ErrorSeverity.Invalid,
                        )
            # Notice logic that if no match is found we log the error
            # (and usually kill it) and land here. Allows application in a map
            # operation
            return d
        else:
            raise TypeError(
                "origin_time_source_matcher.normalize:  received invalid data type"
            )


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
        startime_offset=60.0,
        phasename="P",
        phasename_key="phase",
        attributes_to_load=None,
        load_if_defined=None,
        kill_on_failure=False,
        prepend_collection_name=True,
        verbose=True,
        arrival_collection_name="arrival",
    ):
        """ """
        super().__init__(kill_on_failure, verbose)
        self.phasename = phasename
        self.phasename_key = phasename_key

        if attributes_to_load is None:
            attributes_to_load = ["time"]
        if load_if_defined is None:
            load_if_defined = ["evid", "iphase", "seaz", "esaz", "deltim", "timeres"]

        for x in attributes_to_load:
            self.attributes_to_load.append(x)
        self.load_if_defined = []
        for x in load_if_defined:
            self.load_if_defined.append(x)
        self.prepend_collection_name = prepend_collection_name
        self.dbhandle = db[arrival_collection_name]

    def get_document(self, d):
        stime = d.t0
        etime = d.endtime()
        query = {self.phasename_key: self.phasename}
        query["time"] = {"$ge": stime, "$le": etime}
        n = self.dbhandle.count_documents(query)
        if n == 0:
            return None
        elif n == 1:
            return self.dbhandle.find_one(query)
        else:
            cursor = self.dbhandle.find(query)
            matchlist = []
            # the key here perhaps should be set in constructor
            # for now it is frozen as this constant
            for doc in cursor:
                # ignore any docs with the time attribute not set
                if "time" in doc:
                    dt = doc["time"] - self.time_offset
                    matchlist.append([abs(dt), doc])
            # handle these special cases
            n_to_test = len(matchlist)
            if n_to_test == 0:
                raise MsPASSError(
                    "css30_arrival_interval_matcher.get_document:  no arrival docs found with phasename set as"
                    + self.phasename
                    + " with a time attribute defined.  This should not happen and indicates a serious database inconsistence.  Aborting",
                    ErrorSeverity.Fatal,
                )
            elif n_to_test == 1:
                # weird syntax but this returns to doc of the one and only
                # tuple getting through the above loop.  Execution of this
                # block should be very very rare
                return matchlist[0][1]
            else:
                dtmin = matchlist[0][0]
                imin = 0
                for i in range(len(matchlist)):
                    dt = matchlist[i][0]
                    # not dt values are stored as abs differences
                    if dt < dtmin:
                        imin = i
                        dtmin = dt
                return matchlist[imin][1]

    def normalize(self, d):
        if d.dead():
            return d
        if _input_is_atomic(d):
            doc = self.get_document(d)
            if doc == None:
                message = (
                    "No matching document was found in"
                    + self.arrival_collection_name
                    + " collection for this datum"
                )
                self.log_error(
                    d,
                    "css30_arrival_interval_matcher",
                    message,
                    self.kill_on_failure,
                    ErrorSeverity.Invalid,
                )
            else:
                for key in self.attributes_to_load:
                    if key in doc:
                        if self.prepend_collection_name:
                            mdkey = self.collection + "_" + key
                        else:
                            mdkey = key
                        d[mdkey] = doc[key]
                    else:
                        # We accumulate error messages to aid user debugging
                        # but it could create bloated elog collections
                        message = (
                            "No data for key="
                            + self.mdkey
                            + " in document returned from collection="
                            + self.collection
                        )
                        self.log_error(
                            d,
                            "css30_arrival_interval_matcher",
                            message,
                            self.kill_on_failure,
                            ErrorSeverity.Invalid,
                        )
                # similar for optional but don't log errors for missing
                # attributes unless verbose is set true
                for key in self.load_if_defined:
                    if key in doc:
                        if self.prepend_collection_name:
                            mdkey = self.collection + "_" + key
                        else:
                            mdkey = key
                        d[mdkey] = doc[key]
                    elif self.verbose:
                        self.log_error(
                            "css30_arrival_interval_matcher",
                            "No data found with optional load key=" + key,
                            ErrorSeverity.Informational,
                        )

            # Notice logic that if no match is found we log the error
            # (and usually kill it) and land here. Allows application in a map
            # operation
            return d
        else:
            raise TypeError(
                "css30_arrival_interval_matcher.normalize:  received invalid data type"
            )
