import pymongo
from mspasspy.ccore.utility import Metadata
from mspasspy.ccore.seismic import TimeSeriesEnsemble, TimeSeriesVector
from mspasspy.algorithms.window import WindowData, merge


class seed_keys:
    """
    Small helper class used in this module.   It contains the four magic
    strings used by FDSN to define a unique channel of seismic data
    (net, sta, chan, and loc).   The main use is the implementation of
    the == operator for the class to compare if all keys are equal.
    The class constructor is desiged to be built from MongoDB documents
    and cleanly handle the common wart of null loc codes.
    """
    def __init__(self, doc):
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

    def __eq__(self, other):
        if (
            (other.net == self.net)
            and (other.sta == self.sta)
            and (other.chan == self.chan)
            and (other.loc == self.loc)
        ):
            return True
        else:
            return False

    def same_channel(self, other):
        if (other.chan == self.chan) and (other.loc == self.loc):
            return True
        else:
            return False


def TimeIntervalReader(
    db,
    starttime,
    endtime,
    collection="wf_miniseed",
    base_query=None,
    fix_overlaps=False,
    zero_gaps=False,
    save_zombies=False,
    object_history=False,
    alg_name="TimeIntervalReader",
    alg_id=None,
    dryrun=False,
) -> list:
    """
    A common form of gather when handling continous data with UTC timing
    is to carve out fixed time window.   Two common but different uses
    are (1) carving out an interval of time based on an event origin time,
    and (2) extracting a series of windows in a loop for block processing
    like noise correlation or spectorgrams.   The later demands a lot of
    efficiency in this process as that kind of process can repeat an
    algorithm like this thousands of times.   Reading continuous data
    always has to handle two properties of real data that this function
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
        1.  The python function :py:func:`mspasspy.algorithms.window.merge`
            is a wrapper that handles some translations from the
            C++ primitives to python.  That function is used to glue
            waveform segments together while handling gaps and overlaps
            consistently,  
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
    :param save_zombies:  boolean controlling how data marked dead by
      the merge function are handled.   If set True the dead will be stuffed
      in the member vector of the returned ensembles.   If False (default)
      they will be silently discarded.

    :return:  list of TimeSeriesEnsemble objects (see main description for
        how the data in these containers are grouped)
    """
    tstart = starttime
    tend = endtime
    if base_query:
        query = dict(base_query)
    else:
        query = dict()
    # query["$or"] = [
    #           {"starttime" : {"$gte" : tstart, "$lte" : tend},
    #          "endtime" : {"$gte" : tstart, "$lte" : tend} }
    #     ]
    query["$and"] = [{"starttime": {"$lte": tend}}, {"endtime": {"$gte": tstart}}]
    sortlist = [
        ("chan", pymongo.ASCENDING),
        ("loc", pymongo.ASCENDING),
        ("net", pymongo.ASCENDING),
        ("sta", pymongo.ASCENDING),
        ("starttime", pymongo.ASCENDING),
    ]
    cursor = db[collection].find(query).sort(sortlist)

    ndocs = db[collection].count_documents(query)

    # We create an array of ensembles - one for each unique combination of
    # chan and loc.
    count = 0
    ensemble_list = list()
    for doc in cursor:
        if count == 0:
            current_keys = seed_keys(doc)
            current = _initialize_ensemble(doc, tstart, tend)
            segments = TimeSeriesVector()
            datum = db.read_data(doc, collection=collection)
            if datum.dead():
                count += 1
                continue
            else:
                segments.append(datum)
        else:
            test_keys = seed_keys(doc)
            # Handle the last item specially.
            if count >= ndocs - 1:
                datum = db.read_data(doc, collection=collection)
                if test_keys == current_keys:
                    segments.append(datum)
                    if len(segments) == 1:
                        # I don't think this will ever be executed but
                        # it makes the logic more robust
                        datum = WindowData(segments[0], tstart, tend)
                    else:
                        datum = merge(
                            segments,
                            starttime=tstart,
                            endtime=tend,
                            fix_overlaps=fix_overlaps,
                            zero_gaps=zero_gaps,
                            object_history=object_history,
                            alg_name=alg_name,
                            alg_id=alg_id,
                            dryrun=dryrun,
                        )

                    if datum.live or save_zombies:
                        current.member.append(datum)
                else:
                    # This is special cleanup code to handle
                    # the case when the last doc defines a new segment
                    # May be confusing because here we break the
                    # loop while if we are gluing segments we
                    # continue through the next block.   Because of
                    # the ordering of the data this block will only
                    # be entered if the last doc is a new net:sta:chan:loc
                    # that would otherwise initiate creation and appending
                    # of a new segments vector.
                    datum = db.read_data(doc, collection=collection)
                    datum = WindowData(datum, tstart, tend)
                    if datum.live:  # do nothing further if dead
                        if current_keys.same_channel(test_keys):
                            current.member.append(datum)
                        else:
                            ensemble_list.append(current)
                            current = _initialize_ensemble(doc, tstart, tend)
                            current.member.append(datum)

                # All cases for this cleanup have to push the latest
                # ensemble to the output that is returned after exiting
                # the loop
                if len(current.member) > 0:
                    # if all the contents of current are zombies this
                    # method will refuse to set current as live
                    current.set_live()
                else:
                    # due to a feature of the C++ ensemble templates
                    # this is not currently required but better to
                    # be explicit in this kill
                    current.kill()
                ensemble_list.append(current)

            elif test_keys != current_keys:
                if len(segments) == 1:
                    datum = WindowData(segments[0], tstart, tend)
                else:
                    datum = merge(
                        segments,
                        starttime=tstart,
                        endtime=tend,
                        fix_overlaps=fix_overlaps,
                        zero_gaps=zero_gaps,
                        object_history=object_history,
                        alg_name=alg_name,
                        alg_id=alg_id,
                        dryrun=dryrun,
                    )
                # Merge can kill data for a variety of reasons
                # The cutsy boolean name controls if the dead are retained
                if datum.live or save_zombies:
                    current.member.append(datum)
                # Decide if we need to start a new ensemble.  That
                # happens here when the channel names do not match
                # If they match we assume one of the other codes changed
                # so we initialize the segments vector
                if current_keys.same_channel(test_keys):
                    current_keys = test_keys
                    segments = TimeSeriesVector()
                    datum = db.read_data(doc, collection=collection)
                    if datum.live:
                        segments.append(datum)
                else:
                    if len(current.member) > 0:
                        current.set_live()
                    else:
                        # due to a feature of the C++ ensemble templates
                        # this is not currently required but better to
                        # be explicit in this kill
                        current.kill()
                    # We always post current even if dead and empty
                    # caller needs to handle null returns.
                    ensemble_list.append(current)
                    if count < ndocs - 1:
                        current = _initialize_ensemble(doc, tstart, tend)
                        current_keys = test_keys
                        segments = TimeSeriesVector()
                        datum = db.read_data(doc, collection=collection)
                        if datum.live:
                            segments.append(datum)

            else:
                datum = db.read_data(doc, collection=collection)
                segments.append(datum)
                current_keys = test_keys

        count += 1

    return ensemble_list


def _initialize_ensemble(doc, tstart, tend) -> TimeSeriesEnsemble:
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
    return ens  # Not returned ensemble is marked dead in construction in this context
