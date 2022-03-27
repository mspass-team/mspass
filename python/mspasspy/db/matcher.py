#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from mspasspy.ccore.utility import MsPASSError, ErrorSeverity
from mspasspy.ccore.seismic import (
    TimeSeries,
    Seismogram,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
)

# this is copied from edit.py.  easier to duplicate than load that entire 
# module with this one
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
    return isinstance(d,(TimeSeries,Seismogram))

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
    def __init__(self,kill_on_failure=True,verbose=False):
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
    def get_document(self,d):
        pass
    @abstractmethod
    def normalize(self,d):
        pass
    def log_error(self, d, matchername, message, 
                  kill=False,severity=ErrorSeverity.Informational):
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
        verbose is set true.  There are possible cases with large data sets where
        verbose messages can cause bottlenecks and bloated elog collections.

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
            d.elog.log_error(matchername, fullmessage, severity)
        else:
            raise MsPASSError(
                "NMF.log_error method received invalid data;  arg 0 must be a MsPASS data object",
                ErrorSeverity.Fatal,
            )
            
class ID_matcher(NMF):
    """
    """
    def __init__(self, db, collection, attributes_to_load, kill_on_failure=True):
        if isinstance(collection,str):
            super().__init__(kill_on_failure)
            self.collection = collection
            self.mdkey = collection + "_id"
            self.dbhandle = db[collection]
            # assume type errors will be thrown if attributes_to_load is not array like
            # this is attributes_to_load is initialized to an empty list in 
            # super()
            for x in attributes_to_load:
                self.attributes_to_load.append(x)
            
        else:
            raise TypeError("ID_matcher constructor:  arg0 must be a collection name - received invalid type")
    def get_document(self,d):
        if d.is_defined(self.mdkey):
            query = {"_id" : d[self.mdkey]}
            # Note this will return None if the query fails - callers should handle that condition
            return self.dbhandle.find_one(query)
        else:
            # this is actually redundant with log_error usage but better to be clear
            if self.kill_on_failure:
                d.kill()
            message = "Normalizing ID with key=" + self.mdkey + " is not defined in this object"
            self.log_error(d,"ID_matcher",message,True,ErrorSeverity.Invalid)
            return None
    def normalize(self,d):
        if _input_is_valid(d):
            if d.dead():
                return d
            doc = self.get_document(d)
            if doc == None:
                message = "No matching _id found for " + self.mdkey + " in collection=" + self.collection
                self.log_error(d,"ID_matcher",message,self.kill_on_failure,ErrorSeverity.Invalid)
            else:
                for key in self.attributes_to_load:
                    if key in doc:
                        d[key] = doc[key]
                    else:
                        # We accumulate error messages to aid user debugging
                        # but it could create bloated elog collections
                        message = "No data for key=" + self.mdkey + " in document returned from collection=" + self.collection
                        self.log_error(d,"ID_matcher",message,self.kill_on_failure,ErrorSeverity.Invalid)
            # Notice logic that if no match is found we log the error 
            # (and usually kill it) and land here. Allows application in a map 
            # operation
            return d
        
class mseed_channel_matcher(NMF):
    """
    This class is used to match wf_miniseed to the channel collection using
    the mseed standard channel string tags net, sta, chan, and (optionally) loc.
    It can also be used to data saved in wf_TimeSeries where the mseed tags 
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
    """
    def __init__(self, db, 
                 attributes_to_load=["lat","lon","elev","hang","vang"],
                 kill_on_failure=True, 
                 readonly_tag="READONLYERROR_", 
                 prepend_collection_name=True,
                 verbose=True):
        super().__init__(kill_on_failure,verbose)
        self.dbhandle = db["channel"]
        # assume type errors will be thrown if attributes_to_load is not array like
        for x in attributes_to_load:
            self.attributes_to_load.append(x)
        self.readonly_tag = readonly_tag
        self.prepend_collection_name = prepend_collection_name

    def get_document(self,d,time=None):
        if not _input_is_atomic(d):
            raise TypeError("mseed_channel_matcher.get_document:  data received as arg0 is not an atomic MsPASS data object")
        query_is_ok = True
        query = {}
        if d.is_defined("net"):
            query["net"] = d["net"]
        elif d.is_defined(self.readonly_tag + "net"):
            query["net"] = d[self.readonly_tag + "net"]
        else:
            self.log_error(d,"mseed_channel_matcher",
                           "Required match key=net or " + self.readonly_tag + "net are not defined for this datum",
                           self.kill_on_failure, ErrorSeverity.Invalid)
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
            self.log_error(d,"mseed_channel_matcher",
                           "Required match key=sta or " + self.readonly_tag + "sta are not defined for this datum",
                           self.kill_on_failure, ErrorSeverity.Invalid)
        
        if d.is_defined("chan"):
            query["chan"] = d["chan"]
        elif d.is_defined(self.readonly_tag + "chan"):
            query["chan"] = d[self.readonly_tag + "chan"]
        else:
            query_is_ok = False
            self.log_error(d,"mseed_channel_matcher",
                           "Required match key=chan or " + self.readonly_tag + "chan are not defined for this datum",
                           self.kill_on_failure, ErrorSeverity.Invalid)
            
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
        if time:
            querytime = time
        else:
            querytime = d.t0
            
        query["starttime"] = {"$lt": querytime}
        query["endtime"] = {"$gt": querytime} 

        matchsize = self.dbhandle.count_documents(query)
        if matchsize == 0:
            return None
        if matchsize > 1 and self.verbose:
            self.log_error(d,"mseed_channel_matcher",
                           "Multiple channel docs match net:sta:chan:loc:time for this datum - using first one found",
                           False,ErrorSeverity.Complaint)
        return self.dbhandle.find_one(query)
    
    def normalize(self,d,time=None):
        if d.dead():
            return d
        if _input_is_atomic(d):
            doc = self.get_document(d,time)
            if doc == None:
                message = "No matching document was found in channel collection for this datum" 
                self.log_error(d,"mseed_channel_matcher",message,self.kill_on_failure,ErrorSeverity.Invalid)
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
                        message = "No data for key=" + self.mdkey + " in document returned from collection=" + self.collection
                        self.log_error(d,"mseed_channel_matcher",message,self.kill_on_failure,ErrorSeverity.Invalid)
            # Notice logic that if no match is found we log the error 
            # (and usually kill it) and land here. Allows application in a map 
            # operation
            return d
    
        
# this class is modified form mseed_channel_matcher removing the chan 
# key and replacing channel by site  
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
    def __init__(self, db, 
                 attributes_to_load=["lat","lon","elev"],
                 kill_on_failure=True, 
                 readonly_tag="READONLYERROR_", 
                 prepend_collection_name=True,
                 verbose=True):
        super().__init__(kill_on_failure,verbose)
        self.dbhandle = db["site"]
        # assume type errors will be thrown if attributes_to_load is not array like
        for x in attributes_to_load:
            self.attributes_to_load.append(x)
        self.readonly_tag = readonly_tag
        self.prepend_collection_name = prepend_collection_name

    def get_document(self,d,time=None):
        if not _input_is_atomic(d):
            raise TypeError("mseed_site_matcher.get_document:  data received as arg0 is not an atomic MsPASS data object")
        query_is_ok = True
        query = {}
        if d.is_defined("net"):
            query["net"] = d["net"]
        elif d.is_defined(self.readonly_tag + "net"):
            query["net"] = d[self.readonly_tag + "net"]
        else:
            self.log_error(d,"mseed_site_matcher",
                           "Required match key=net or " + self.readonly_tag + "net are not defined for this datum",
                           self.kill_on_failure, ErrorSeverity.Invalid)
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
            self.log_error(d,"mseed_site_matcher",
                           "Required match key=sta or " + self.readonly_tag + "sta are not defined for this datum",
                           self.kill_on_failure, ErrorSeverity.Invalid)
    
            
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
        if time:
            querytime = time
        else:
            querytime = d.t0
            
        query["starttime"] = {"$lt": querytime}
        query["endtime"] = {"$gt": querytime} 

        matchsize = self.dbhandle.count_documents(query)
        if matchsize == 0:
            return None
        if matchsize > 1 and self.verbose:
            self.log_error(d,"mseed_site_matcher",
                           "Multiple site docs match net:sta:chan:loc:time for this datum - using first one found",
                           False,ErrorSeverity.Complaint)
        return self.dbhandle.find_one(query)
    
    def normalize(self,d,time=None):
        if d.dead():
            return d
        if _input_is_atomic(d):
            doc = self.get_document(d,time)
            if doc == None:
                message = "No matching document was found in site collection for this datum" 
                self.log_error(d,"mseed_site_matcher",message,self.kill_on_failure,ErrorSeverity.Invalid)
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
                        message = "No data for key=" + self.mdkey + " in document returned from collection=" + self.collection
                        self.log_error(d,"mseed_site_matcher",message,self.kill_on_failure,ErrorSeverity.Invalid)
            # Notice logic that if no match is found we log the error 
            # (and usually kill it) and land here. Allows application in a map 
            # operation
            return d
        else:
            raise TypeError("mseed_site_matcher.normalize:  received invalid data type")

class origin_time_source_matcher(NMF):
    """
    """
    def __init__(self, db, collection="source",
                   t0offset=0.0,
                   tolerance=4.0,
                   attributes_to_load=["lat","lon","depth","time"],
                   kill_on_failure=True, 
                   prepend_collection_name=True,
                   verbose=True):
        """
        """
        super().__init__(kill_on_failure,verbose)
        self.collection = collection
        self.dbhandle = db[collection]
        self.t0offset = t0offset
        self.tolerance = tolerance
        self.prepend_collection_name = prepend_collection_name

        for x in attributes_to_load:
            self.attributes_to_load.append(x)
    def get_document(self,d,time=None):
        """
        """
        if _input_is_valid(d):
            # this logic allows setting ensemble metadata using a specific 
            # time but if time is not defined we default to using data start time (t0)
            if time == None:
                test_time = d.t0 - self.t0offset
            else:
                test_time = time - self.t0offset
            query = { "time" : { "$ge" : test_time - self.tolerance,
                                "$le" : test_time + self.tolerance
                                }
                     }
            
            matchsize = self.dbhandle.count_documents(query)
            if matchsize == 0:
                return None
            elif matchsize > 1 and self.verbose:
                self.log_error(d,"origin_time_source_matcher",
                               "multiple source documents match the origin time computed from time received - using first found",
                               ErrorSeverity.Complaint)
            return self.dbhandle.find_one(query)
            
        else:
            return None
        
    def normalize(self,d,time=None):
        if d.dead():
            return d
        if _input_is_valid(d):
            doc = self.get_document(d,time)
            if doc == None:
                message = "No matching document was found in" + self.collection + " collection for this datum" 
                self.log_error(d,"origin_time_source_matcher",message,self.kill_on_failure,ErrorSeverity.Invalid)
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
                        message = "No data for key=" + self.mdkey + " in document returned from collection=" + self.collection
                        self.log_error(d,"origin_time_source_matcher",message,self.kill_on_failure,ErrorSeverity.Invalid)
            # Notice logic that if no match is found we log the error 
            # (and usually kill it) and land here. Allows application in a map 
            # operation
            return d
        else:
            raise TypeError("origin_time_source_matcher.normalize:  received invalid data type")
        
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
    def __init__(self,db,
                 startime_offset=60.0,
                   phasename="P",
                   phasename_key = "phase",
                   attributes_to_load=["time"],
                   load_if_defined=["evid","iphase","seaz","esaz","deltim","timeres"],
                   kill_on_failure=False, 
                   prepend_collection_name=True,
                   verbose=True,
                   arrival_collection_name="arrival"):
        """
        """
        super().__init__(kill_on_failure,verbose)
        self.phasename = phasename
        self.phasename_key = phasename_key
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
        query = {self.phasename_key : self.phasename}
        query["time"] = {"$ge" : stime, "$le" : etime}
        n = self.dbhandle.count_documents(query)
        if n==0:
            return None
        elif n==1:
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
                    matchlist.append( [abs(dt),doc] )
            # handle these special cases 
            n_to_test = len(matchlist)
            if n_to_test == 0:
                raise MsPASSError("css30_arrival_interval_matcher.get_document:  no arrival docs found with phasename set as" + self.phasename + " with a time attribute defined.  This should not happen and indicates a serious database inconsistence.  Aborting",
                                  ErrorSeverity.Fatal)
            elif n_to_test == 1:
                # weird syntax but this returns to doc of the one and only
                # tuple getting through the above loop.  Execution of this 
                # block should be very very rare
                return matchlist[0][1]
            else:
                dtmin = matchlist[0][0]
                imin = 0
                for i in range(len(matchlist) - 1):
                    ii = i + 1
                    dt = matchlist[ii][0]
                    # not dt values are stored as abs differences
                    if dt < dtmin:
                        imin = ii
                        dtmin = dt
                return matchlist[imin][1]
    def normalize(self,d):
        if d.dead():
            return d
        if _input_is_atomic(d):
            doc = self.get_document(d)
            if doc == None:
                message = "No matching document was found in" + self.arrival_collection_name + " collection for this datum" 
                self.log_error(d,"css30_arrival_interval_matcher",message,self.kill_on_failure,ErrorSeverity.Invalid)
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
                        message = "No data for key=" + self.mdkey + " in document returned from collection=" + self.collection
                        self.log_error(d,"css30_arrival_interval_matcher",message,self.kill_on_failure,ErrorSeverity.Invalid)
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
                        self.log_error("css30_arrival_interval_matcher",
                            "No data found with optional load key=" + key,
                            ErrorSeverity.Informational)
                        
            # Notice logic that if no match is found we log the error 
            # (and usually kill it) and land here. Allows application in a map 
            # operation
            return d
        else:
            raise TypeError("css30_arrival_interval_matcher.normalize:  received invalid data type")

