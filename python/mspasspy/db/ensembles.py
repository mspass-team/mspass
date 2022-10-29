from abc import ABC,abstractmethod
from mspasspy.ccore.utility import MsPASSError,ErrorSeverity,Metadata
from mspasspy.ccore.seismic import (TimeSeries,
                                    TimeSeriesEnsemble,
                                    TimeSeriesVector)
from mspasspy.db.normalize import BasicMatcher,ObjectIdDBMatcher
from mspasspy.algorithms.window import WindowData,merge
from mspasspy.util.decorators import mspass_func_wrapper
from obspy.core.stream  import Stream
from copy import deepcopy

def BasicEnsembleReader(ABC):
    """
    Abstract base class for suite of readers of common seismology 
    groupings to read ensembles.  Two main things are required to 
    assemble an ensemble:  (1) database query to define the atomic 
    data that should be assembled as the ensemble, and (2) the actual 
    read operation to load the sample data and construct the ensemble. 
    The first depends on what defines the ensemble and in the model used here 
    defines a concrete implementation.    Thus, abstract methods of 
    this base class require implementations to be made concrete to 
    create, for example, common-source or common-receiver gathers.   
    Reading and constructing the ensemble, on the other hand, is generic and 
    is defined in this base class.  That is done with two generic methods:
    (1) read_ensemble returns an assembled ensemble based on the 
    result of the query generator that is dependent on the ensemble 
    definition, and (2) a preprocessor called "ensemble_size" that 
    should be used to test a workflow on a dataset to verify the 
    dataset would not generate huge ensembles that would cause memory 
    problems.  
    
    The API was set up to allow concrete classes to be used in either 
    serial or parallel jobs.   Both should constuct an instance of a
    concrete class as an initialization step.   Serial jobs would 
    then just be a loop over whatever defines the ensemble packaging.  
    Parallel jobs would typically initialize a bag/rdd from a list of 
    items (e.g. source ids for a data set of common source gathers).  
    Parallel jobs could then load ensembles by enclosing calls to the 
    (generic) read_ensemble method in a map operator.  
    
    Another feature implemented in a generic way in this base class is 
    normalization.   Normalization in an ensemble is defined as setting 
    the ensemble Metadata with attributes that are common to all enemble 
    members.  e.g. for a source gather one would normally want to post 
    the source hypocenter data to the ensemble metadata container.   
    That is handled through the generic family of BasicMatcher 
    objects defined in the normalize module.   "self" of this base 
    class can contain an instance of a matcher that can be used to 
    normalize the ensemble.   If the matcher argument in construction 
    is anything but a None (the default for the base class constructor) 
    the ensembled will be normalized by calling the find_one method of 
    the matcher.  The result will be posted to the ensmble's Metadata 
    container.  
    
    A confusing detail is that there is another way to normalize the loaded 
    data.  That is, the read_ensemble method can accept arguments it 
    will pass to the database read_ensemble method.  That allows atomic 
    level normalization through the normal "normalize" argument to 
    the read_ensemble_data method.  Many workflows may want to use atomic 
    normalization and the matcher normalization.  e.g. for common source
    gathers (ensembled) the normal procedure would be to define the
    matcher to set ensemble metadata to source attributes and have the 
    atomic data normalized with receiver metadata by using the normalize 
    argument passed to read_ensemble_data.  
    
    
    The following parameters are used to construct the base class and 
    are used directly by all current subclasses.
    
    :param db:   MsPASS Database handle holding the dataset to be processed.
    :type db:  MsPASS Database class (subclass of MongoDB handle)
    
    :param collection:  waveform data collection holding index to the waveform 
      data to be loaded.  Currently must be one of:  wf_miniseed, wf_TimeSeries, 
      or wf_Seismogram.  
    :type collection:  string
    
    :param matcher:  concrete implementation of a BasicMatcher family of 
      normalization objects.  Default for the base class is a None which 
      means no normalization is attempted.   (see above for more about this 
      feature)
    :type matcher:  concrete implementation a BasicMatcher defined in 
      the normalize module.  
    
    """
    def __init__(self,
                 db,
                 collection,
                 matcher=None):
        """
        Base class constructor.  See class docstring for arguments.
        """
        if not isinstance(collection,str):
            raise TypeError("BasicEnsembleReader constructor:   collection (required arg1) must be string defining a MongoDB collection name")
        allowed_col=["wf_miniseed","wf_TimeSeries","wf_Seismogram"]
        if collection not in allowed_col:
            raise MsPASSError("BasicEnsembleReader constructor:  collection name (arg1) defined as "+collection+".  Must be one of: "+str(allowed_col),
                              ErrorSeverity.Fatal)
        # This should be expanded to do this more carefully with some 
        # type checking an verifying the result is correct
        self.db = db
        self.collection = collection
        self.dbwfcol = db[collection]
        if matcher is not None:
            if not isinstance(matcher,BasicMatcher):
                raise TypeError("CommonSourceGatherReader constructor:  matcher argument must define a child of BasicMatcher")
        self.matcher = matcher
        
        
        
    @abstractmethod
    def _generate_query(self, *args, **kwargs)->dict:
        """
        Concrete implementations should implement this method.  
        It needs to generate a query string that defines a group of 
        waveforms forming your definition of an enemble.  The args 
        are defined as generic allow options to be passed through 
        the read_enemble method.  An iportant restriction is that 
        you must not require a different use for kwarg keywords 
        required by the MsPASS Database class read_ensemble_data 
        method because the generic read_ensemble method of this class
        uses those same args.  
        """
        pass
    
    def read_ensemble(self,*args,**kwargs):
        """
        Generic ensemble reader.  The algorithm is simple but 
        a lot of complexity and flexibility is hidden by funcions calls
        that do the actual work.  The algorithm is three steps:
            1.  Call the class _generate_query method to define the 
                waveforms tha will form this ensemble an then run that 
                on the database handle to get a MongoDB cursor object.
            2.  Call the MsPASS Database read_ensemble_data method with the 
                cursor to construct the ensemble.
            3.  If matcher is defined (not None) the ensemble Metadata 
                is "normalized" using the matcher's find_one method
                and posting the result to the enemble.  
                
        
        """
        
        query = self._generate_query(*args,**kwargs)
        cursor = self.dbwfcol.find(query)
        
        ensemble = self.db.read_ensemble_data(cursor,**kwargs)
        if self.matcher is not None:
            md = self.matcher.find_one(ensemble)
            # This might work with += but would be very confusing to do so
            for key in md:
                val = md[key]
                ensemble[key] = val
        return ensemble
    
    def ensemble_size(self,*args,**kwargs)->tuple:
        """
        Generic method to return the size of an ensemble we would expect to 
        be returned by definition in *args and **kwargs.   Most workflows
        would want a dry run to get this data.  Returns a tuple with 
        number of members and estimated memory use to hold sample data 
        the ensemble would contain.  Note the memory size estimate
        is only lower bound on memory us.  The estimate only accounts for 
        the sample data.  Any actual ensemble will be larger when the 
        Metadata containers are populated.  
        
        This method can be used in a dask/spark map operator to run it 
        in parallel.  
        
        The arg list is generic and passed directly to the concrete 
        implementations _generate_query method.   A dry run should use 
        the same args passed in an operational workflow to the read_ensemble 
        method. 
        """
        # If python had a const this would be one
        # size is 8 because in mspass sample data are always stored as 
        # C double
        bytes_per_sample = 8
        # key for number of samples
        npts_key = "npts"
        query = self._generate_query(*args,**kwargs)
        count = self.dbwfcol.count_documents(query)
        if count == 0:
            memorysize = 0
        else:
            memorysize = 0
            cursor = self.dbwfcol.find(query)
            for doc in cursor:
                if npts_key in doc:
                    npts = doc[npts_key]
                    memorysize += npts*bytes_per_sample
                else:
                    raise MsPASSError("BasicEnsembleReader.ensemble_size:  retrieved the following document that was missing required attribute="+npts_key+str(doc),
                                      ErrorSeverity.Fatal)
        return tuple([count,memorysize])
    
def CommonSourceGatherReader(BasicEnsembleReader):
    """
    Concrete implementation for common source gathers.  Uses optional 
    BasicMatcher class to define what should be loaded as ensemble 
    metadata.   It loads the  data through the matchers find_one method.
    Normalization using args passed to the Database read_ensemble_data 
    is also possible by adding those args when callng the read_ensemble 
    method.  
    """
    
    def __init__(self,
                  db,
                  collection,
                  matcher=None,
                ):
        super().__init__(db,collection,matcher)
        # matcher here needs to default to a source_id matcher using the 
        # database matcher for ids
        # note is not needed here because if matcher is not null call to super sets it
        if matcher is None:
            self.matcher = ObjectIdDBMatcher(self.db,
                                        collection="source",
                                        attributes_to_load=["lat","lon","depth","time"],
                                        prepend_collection_name=True,
                                        )
        
        
    def _generate_query(self,source_id,id_key="source_id")->dict:
        """
        Implementation of required abstract method for this class.  Dogmatically 
        depends upon using the ObjectId with the key source_id to define 
        the grouping. This puts the job of defining how the grouping is done 
        to a preprocessing step and makes this implemntation more generic.
        
        :param source_id:   ObjectId defining the source that defines the 
          ensemble desired.   
          
        :type source_id:  MongoDB ObjectId
        :param id_key:  key that will be used to subset the waveform collection 
          using the input id through source_id argument.  Default is standard 
          mspass convention name of "source_id"
        :type id_key:  string
        
        """
        query = dict()
        query[id_key] = source_id
        return query
class CRGById(BasicEnsembleReader) :
    """
    This class is used to standardize the process of creating what 
    in reflection seismology is called a common receiver gather".  
    That means ensembles are assembled from a dataset by grouping 
    all data from a common sensor position into an ensemble.  This 
    class generalizes that a bit to handle the common situation in 
    global seismology where there are multiple sensors at approximately 
    the same location.   All GSN data consumers will recognize this as the 
    infamous "loc" (location) code of the SEED standard.  We handle that
    abstraction by depending on the normalization abstraction "matcher" 
    concept to define how data are grouped.  The default behavior 
    of the class is to group data by MongoDB id used in MsPASS for 
    this purpose called "channel_id" and works for either wf_miniseed 
    input or wf_TimeSeries input.   Other applications require 
    passing the constructor a concrete instances of a BasicMatcher 
    defined in the normalize module.  
    """
    def __init__(self,
                  db,
                  collection="wf_TimeSeries",
                  normalizing_collection="channel",
                  matcher=None,
                ):
        super().__init__(db,collection,matcher)
        # matcher here needs to default to a channel_id matcher using the 
        # database matcher for ids
        # note is not needed here because if matcher is not null call to super sets it
        if matcher is None:
            if normalizing_collection == "channel":
                self.matcher = ObjectIdDBMatcher(self.db,
                                        normalizing_collection="channel",
                                        attributes_to_load=["_id","lat","lon","elev","hang","vang"],
                                        prepend_collection_name=True,
                                        )
            elif collection == "site":
                self.matcher = ObjectIdDBMatcher(self.db,
                                        normalizing_collection="site",
                                        attributes_to_load=["_id","lat","lon","elev"],
                                        prepend_collection_name=True,
                                        )
            else:
                raise MsPASSError("Cannot handle collection="+normalizing_collection+" must be either site or channel",
                                  ErrorSeverity.Fatal)
    def _generate_query(self,id_value)->dict:
        """
        Builds query either as a id matc.   The former 
        uses the key name passed through te id_key argument.   The later depends on 
        a dict passed through the seed_codes argument.  The function with 
        abort with a MsPASSError exception unless one and only one of the 
        options defined by id_key and seed_codes is enabled (not None).
        
        
        NO NO - seed approach makes no sense.   If station metadata is 
        time variable we need to dogmatically require the gather to 
        be defined by a time stamp.  Further things in this method 
        call belong in the constructor.   Maybe query should have an option 
        to ignore time test.   Constructor would define the matcher to 
        preload the require ensemble Metadata.  
        
        :param id_key: key used to define an object id defining a unique 
          "receiver" to be selected.   (standard valus are "site_id" and 
          "channel_id" but the string is used verbatim.)   Default is None 
          which is means id matching is disabled.
        :type id_key:  string
        
        :param seed_codes:  python dictionary containing key value pairs to 
          use to match one or more seed station code 
        """
        
        if id_value is None:
            raise MsPASSError("CommonReceiverGather:  usage error.  Must set either id_value or seed_codes arguement",
                              ErrorSeverity.Fatal)
        else:
            id_key = self.collection + "_id"
            query = {id_key : id_value}
        return query
 
class seed_keys:
    def __init__(self,doc):
        if "net" in doc:
            self.net = doc["net"]
        else:
            self.net = None
        self.sta = doc["sta"]
        self.chan = doc["chan"]
        if "loc" in doc:
            self.loc = doc["loc"]
        else:
            self.loc = None
    def __eq__(self,other):
        if (other.net == self.net) and (other.sta == self.sta) \
                    and (other.chan == self.chan) and (other.loc == self.loc):
            return True
        else:
            return False
    def same_channel(self,other):
        if (other.chan == self.chan) and (other.loc == self.loc):
            return True
        else:
            return False
        
          
class CRGBySeedCode(BasicEnsembleReader) :
    def __init__(self,
                  db,
                  collection="wf_miniseed",
                  normalizing_collection="channel",
                  matcher=None,
                ):
        super().__init__(db,collection,matcher)
        # matcher here needs to default to a channel_id matcher using the 
        # database matcher for ids
        # note is not needed here because if matcher is not null call to super sets it
        if matcher is None:
            if normalizing_collection == "channel":
                self.matcher = ObjectIdDBMatcher(self.db,
                                        normalizing_collection="channel",
                                        attributes_to_load=["_id","lat","lon","elev","hang","vang"],
                                        prepend_collection_name=True,
                                        )
            elif collection == "site":
                self.matcher = ObjectIdDBMatcher(self.db,
                                        normalizing_collection="site",
                                        attributes_to_load=["_id","lat","lon","elev"],
                                        prepend_collection_name=True,
                                        )
            else:
                raise MsPASSError("Cannot handle collection="+normalizing_collection+" must be either site or channel",
                                  ErrorSeverity.Fatal) 
    def _generate_query(self,doc_or_md)->dict:
        """
        Creates a pymongo dict query from any dictionary like container 
        defined by arg doc_or_md.   That allows calling this method 
        from a doc returned from MongoDB or a MsPASS data object 
        inheriting Metadata.  The later can be useful if one already has 
        an ensemble template with the required attributes loaded or a 
        sample atomic datum containing the required seed station keys. 
        This function demands sta only for site and sta-chan for channel 
        be defined in the input.   That is net and loc are only added to 
        the query if they are defined in doc_or_md.
        
        :param doc_or_md: associative array container used to build query. 
          Need only support the "in" clause to test if a key is present. 
          i.e. a statement like "if loc in doc_or_md:" must resolve.
          
        :type doc_or_md:   dict, Metadata, or any container that 
          that works with an "in" test for a key value and operator[] with 
          the key.
        """
        query = dict()
        query["sta"] = doc_or_md["sta"]
        if "net" in doc_or_md:
            query["net"] = doc_or_md["net"]
        if self.collection == "channel":
            query["chan"] = doc_or_md["channel"]
            if "loc" in doc_or_md:
                query["loc"] = doc_or_md["loc"]
        return query           

@mspass_func_wrapper        
def TimeIntervalReader(db,starttime,endtime,
                           collection="wf_miniseed",
                           base_query=None,
                           fix_overlaps=False,
                           zero_gaps=False,
                           object_history=False,
                           alg_name="TimeIntervalReader",
                           alg_id=None,
                           dryrun=False,               
                       )->list:
    """
    A common form of gather when handling continous data with UTC timing 
    is to carve out fixed time window.   Two common but different uses 
    are (1) carving out an interval of time based on an event origin time, 
    and (2) extracting a series of windows in a loop for block processing 
    like noise correlation or spectorgrams.   The later demand a lot of 
    efficiency in this process as that kind of process can repeat an 
    algorithm like this thousands of times.   Reading continuous data 
    always has to handle two properties of read data that this function 
    hides behind the implementation.  (1) in all but trivially short 
    experiments the length of recording for any given instrument is 
    far too large to fit in memory.  Data are thus always broken into 
    fixed chunks.  In most cases that has evolved to mean day-long 
    chunks.  (2) A large fraction of data have recording gaps for a 
    long list of reasons.  The second folds into the first in a less 
    obvious way because of another practical data issue;  timing problems.
    When an instrument's timing system fails the internal clock will 
    drift away from the external reference time.  When the instrument 
    again receives a timing signal the next packet written 
    (note all modern data are collected in digital packets like miniseed)
    may have a "time tear".  That means the time stamp is off by more than 
    1/2 sample interval from the time computed from the last valid time 
    stamp and the nominal sample interval.   When reading continuous 
    data that kind of anomaly will appear like one of two things: (1) 
    a data gap if the time tear is a forward jump in time, or (2) a data 
    overlap if the time tear is a backward jump in time.   In MsPASS 
    our miniseed indexer detects time tears and data gaps 
    (The algorithm just checks for greater than 1/2 sample time mismatches.)
    and writes a wf_miniseed document for each segment.  i.e. even a 
    single channel, day file from a continuous data set may have multiple
    segments defined because of gaps and time tears.  A huge complication of 
    this reader is it attempts to handle all gap and overlap related issues 
    and fix them when possible.   It cannot handle all situations, however, 
    and takes the approach that if it cannot make a reasonable repair 
    with some simple assumption it will kill the TimeSeries datum is is 
    attempting to assemble and post an error message that can be used 
    for post-mortum analysis of a workflow.  All the complexity of this 
    processing is hidden under two layers of code this reader utilizes:
        1.  The python function mspasspy.algorithms.window.merge 
            is a wrapper that handles some translations from 
        2.  The bottom layer, which, is a set of C++ code that does 99% of the 
            work.  The base layer was done in C++ for efficiency because,
            as noted above, use of this algorithm can be very time intensive
            and requires efficiency.   The current implementation uses 
            to C++ functions bound to python in the module 
            mspasspy.ccore.algorithms.basic with symbols "splice_segments" 
            and "repair_overlaps".  
    
    Users of this function will want to also read the docstring for 
    mspasspy.algorithms.window.merge as it contains more implementation 
    details on how the merge algorithm works and the assumptions it makes. 
    
    This reader is a high level reader to simplify reading time windows of 
    data from a continuous data set.  The basic model is you specify a 
    time window from which you want some subset or all the data 
    with data recorded during that interval.  We further assume that
    data has been indexed and is (by default and only thing sure to work) 
    defined by set of wf_miniseed documents. You can specify a subset 
    using the "base_query" argument (see below).  The algorithm then 
    queries MongoDB to find all waveform segments that span all or part 
    of the requested time interval.  It then reads and reorganizes the 
    data into a list of TimeSeriesEnsemble objects.   There will be 
    one such ensemble for each unique channel/location code found 
    after applying the base query.   Hence, for example, if you used a 
    query to only accept "B" channels with null location (loc attribute) 
    codes you could expect to get a list of at least 3 ensembles:   BHZ,
    BHE, and BHN (you might also get something like BH1 and BH2 - a detail).
    All members of all ensembles will either have a complete vector within 
    the specified range and be marked live or marked dead.  The default 
    for gap handling is any gap or nonrepairable overlap will cause a dead
    datum to be loaded with a elog entry defining the reason for the kill. 
    If the boolean argument zero_gaps is set true (default is False) 
    gaps will be zero filled and marked live.  The only evidence of that 
    kind of problem in that mode is that a boolean Metadata attribute 
    "has_gaps" will be set True (clean data will not have that attribute 
    defined) and the gap time windows defined in a document with the 
    key "gaps" with subdocuments defining the gap time intervals. 
    
    Note:  start times of all returned data will have a variation of + to - 
      1 sample interval. If you need synchronous timing to a greater 
      precision than one sample you will need to resample the outputs to 
      a fixed time comb.
    
    :param db: MongoDB database handle 
    :type db:  Usually a MsPASS Database class but could be the superclass 
        MongoDB handle.
        
    :param starttime:   starting time of time window of data to be extracted.
    :type starttime:  double epoch time
    
    :param endtime: end time of time window of data to be extracted.
    :type starttime:  double epoch time
    
    :param base_query: A dictionary defining a higher level subset to 
      limit what data are to be retrieved.  The most common would be 
      limits on sample interval or channel naming.  Must be a valid 
      pymongo find definition with a python dictionary.
    :type base_query:  python dictionary defining a valid MongoDB query.
        
    :param collection:  waveform collection to be read.  Default is 
      wf_miniseed.  If you use any other collection name be aware the 
      documents retrieved must have the keys "net", "sta", and "chan".  
      ("loc" is treated as optional as in all of mspass) in the seed way AND 
      have the time range of each datum defined by the keys "starttime" and 
      "endtime".  This function is know to work only with wf_miniseed
      documents created one of the mspass index_miniseed_* methods of Database.
    :type collection:  string
    
    :param fix_overlaps: see docstring for mspasspy.algorithms.window.merge 
    :param zero_gaps: see docstring for mspasspy.algorithms.window.merge
    
    :return:  list of TimeSeriesEnsemble objects (see main description for
        how the data in these containers are grouped)
    """
    tstart = starttime
    tend = endtime
    query = dict(base_query)
    query["$or"] = [
                {"starttime" : {"$gte" : tstart, "$lte" : tend},
                "endtime" : {"$gte" : tstart, "$lte" : tend} }   
            ]
    sortlist = {"chan" : 1,"loc" : 1, "starttime" : 1, "net" : 1, "sta" : 1}
    cursor = db[collection].find(query).sort(sortlist)
    

    # We create an array of ensembles - one for each unique combination of 
    # chan and loc.
    count = 0
    ensemble_list = list()
    for doc in cursor:
        if count==0:
            current_keys = seed_keys(doc)
            current = _initialize_ensemble(doc, tstart, tend)
            last_datum = db.read_data(doc,collection=collection)
            continue
        else:
            test_keys = seed_keys(doc)
            if test_keys == current_keys:
                # If we land here it means we have multiple segments
                segments = TimeSeriesVector()
                segments.append(last_datum)
                while( test_keys == current_keys):
                    datum = db.read_data(doc,collection=collection)
                    segments.append(datum)
                    if cursor.isExhaused():
                        break
                    doc = cursor.next()
                    current_keys = deepcopy(test_keys)
                    test_keys = seed_keys((doc))
                datum = merge(segments,
                                  starttime=tstart,
                                  endtime=tend,
                                  fix_overlaps=fix_overlaps,
                                  zero_gaps=zero_gaps,
                                  object_history=object_history,
                                  alg_name=alg_name,
                                  alg_id=alg_id,
                                  dryrun=dryrun
                              )
                # Merge can kill data for a variety of reasons
                # When that happens clear any sample data before 
                # pushing the result to the ensemble.  We want to save
                # the elog for debugging data issues
                if datum.dead():
                    datum.set_npts(0)
                    windowed_data = datum   # shallow copy make this appropriate
            else:
                datum = db.read_data(doc,collection=collection)
            if datum.live():
                windowed_data = WindowData(datum,tstart,tend)
            last_datum=TimeSeries(datum)  # a deep copy using C API for speed
            if current_keys.same_channel(test_keys):
                current.member.append(windowed_data)
            else:
                ensemble_list.append(current)
                current = _initialize_ensemble(doc, tstart, tend)
                current.member.append(windowed_data)
                
    #cleanup - last emsemble need to be pushed to output list
    ensemble_list.append(current)
    return ensemble_list
        
def _initialize_ensemble(doc,tstart,tend)->TimeSeriesEnsemble:
    """
    Helper for above to do the repititious task of creating a skeleton 
    ensemble to be filled by atomic reads.  Creates a basic ensemble 
    and then loads an ensemble header appropriate for the application of 
    carving out a time window.
    
    :param doc: doc to use as pattern 
    :param tstart: start time (epoch time) of time window 
    :param tend: end time (epoch time) of time window
    """
    ens = TimeSeriesEnsemble()
    md = Metadata()
    md["starttime"] = tstart
    md["endtime"] = tend
    if "net" in doc:
        md["net"] = doc["net"]
    else:
        md["net"] = "Undefined"
    md["sta"] = doc["sta"]
    md["chan"] = doc["chan"]
    if "loc" in doc:
        md["loc"] = doc["loc"]
    else:
        md["loc"] = "Undefined"
    ens.update_metadata(md)
    return ens   # Not returned ensemble is marked dead in construction in this context
    
