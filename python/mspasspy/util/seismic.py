from mspasspy.ccore.seismic import (
    TimeSeries,
    Seismogram,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
)
from mspasspy.ccore.utility import Metadata, ErrorSeverity
from mspasspy.ccore.algorithms.basic import TimeWindow
from bson import json_util
import numpy as np
import pandas as pd


def print_metadata(d, indent=2):
    """
    Prints Metadata stored in the object passed as arg0 (d) as json
    format output with indentation defined by the optional
    indent argument.  Indent is always on and defaults to 2 characters.
    The purpose of this function is to standardize printing
    the Metadata contents of any MsPASS data object.   It has the
    beneficial side effect of producing the same print for
    documents (python dictionaries) retrieved directly from MongoDB.

    It is important to realize that when applied to a
    `TimeSeriesEnsemble` or `SeismogramEnsemble` only the ensemble
    Metadata container will be printed.

    :param d:  datum for which the Metadata is to be printed.
    :type d:  any subclass of `mspasspy.ccore.Metadata` or a
    python dictionary (the container used for documents returned
    by pymongo).  If d is anything else a TypeError exception
    will be thrown.
    :param indent:   indentation argument for json printing
    :type indent:  integer (default 2)
    """
    if isinstance(d, Metadata):
        doc = dict(d)
    elif isinstance(d, dict):
        doc = d
    else:
        message = (
            "print_metadata:   arg0 must be a dictionary or a subclass of Metadata\n"
        )
        message += "type of arg0={}".format(type(d))
        raise TypeError(message)
    print(json_util.dumps(doc, indent=indent))


def number_live(ensemble) -> int:
    """
    Scans an ensemble and returns the number of live members.  If the
    ensemble is marked dead it immediately return 0.  Otherwise it loops
    through the members countinng the number live.
    :param ensemble:  ensemble to be scanned
    :type ensemble:  Must be a `TimeSeriesEnsemble` or `SeismogramEnsemble` or
    it will throw a TypeError exception.
    """
    if not isinstance(ensemble, (TimeSeriesEnsemble, SeismogramEnsemble)):
        message = "number_live:   illegal type for arg0={}\n".format(type(ensemble))
        message += "Must be a TimeSeriesEnsemble or SeismogramEnsemble\n"
        raise TypeError(message)
    if ensemble.dead():
        return 0
    nlive = 0
    for d in ensemble.member:
        if d.live:
            nlive += 1
    return nlive


def regularize_sampling(ensemble, dt_expected, Nsamp=10000, abort_on_error=False):
    """
    This is a utility function that can be used to validate that all the members
    of an ensemble have a sample interval that is indistinguishable from a specified
    constant (dt_expected)   The test for constant is soft to allow handling
    data created by some older digitizers that skewed the recorded sample interval to force
    time computed from N*dt to match time stamps on successive data packets.
    The formula used is the datum dt is declared constant if the difference from
    the expected dt is less than or equal to dt_expected/2*(Nsamp-1).  That means
    the computed endtime difference from that using dt_expected is less than
    or equal to dt/2.

    The function by default will kill members that have mismatched sample
    intervals and log an informational message to the datum's elog container.
    In this mode the entire ensemble can end up marked dead if all the members
    are killed (That situation can easily happen if the entire data set has the wrong dt.);
    If the argument `abort_on_errors` is set True a ValueError exception will be
    thrown if ANY member of the input ensemble.

    An important alternative to this function is to pass a data set
    through the MsPASS `resample` function found in mspasspy.algorithms.resample.
    That function will guarantee all live data have the same sample interval
    and not kill them like this function will.   Use this function for
    algorithms that can't be certain the input data will have been resampled
    and need to be robust for what might otherwise be considered a user error.

    :param ensemble:  ensemble container of data to be scanned for irregular
    sampling.
    :type ensemble:  `TimeSeriesEnsemble` or `SeismogramEnsemble`.   The
    function does not test for type and will abort with an undefined method
    error if sent anything else.
    :param dt_expected:  constant data sample interval expected.
    :type dt_expected:  float
    :param Nsamp:   Nominal number of samples expected in the ensemble members.
    This number is used to compute the soft test to allow for slippery clocks
    discussed above.   (Default 10000) e = regularize_sampling(e,dt)
    assert e.live
    :type Nsamp:  integer
    :param abort_on_error:  Controls what the function does if it
    encountered a datum with a sample interval different that dt_expected.
    When True the function aborts with a ValueError exception if ANY
    ensemble member does not have a matching sample interval.  When
    False (the default) the function uses the MsPASS error logging
    to hold a message an kills any member datum with a problem.
    :type abort_on_error:  boolean

    """
    alg = "regularize_sampling"
    if ensemble.dead():
        return ensemble
    # this formula will flag any ensemble member for which the sample
    # rate yields a computed end time that differs from the beam
    # by more than one half sample
    delta_dt_cutoff = dt_expected / (2.0 * (Nsamp - 1))
    for i in range(len(ensemble.member)):
        d = ensemble.member[i]
        if d.live:
            if not np.isclose(dt_expected, d.dt):
                if abs(dt_expected - d.dt) > delta_dt_cutoff:
                    message = str()
                    message = "Member {} of input ensemble has different sample rate={} than expected dt={}".format(
                        i, d.dt, dt_expected
                    )
                    if abort_on_error:
                        raise ValueError(message)
                    else:
                        ensemble.member[i].elog.log_error(
                            alg, message, ErrorSeverity.Invalid
                        )
                        ensemble.member[i].kill()
    if not abort_on_error:
        nlive = number_live(ensemble)
        if nlive <= 0:
            message = "All members of this ensemble were killed.\n"
            message += "expected dt may be wrong or you need to run the resample function on this data set"
            ensemble.elog.log_error(alg, message, ErrorSeverity.Invalid)
            ensemble.kill()
    return ensemble


def ensemble_time_range(ensemble, metric="inner") -> TimeWindow:
    """
    Scans a Seismic data ensemble returning a measure of the
    time span of members.   The metric returned ban be either
    smallest time range containing all the data, the range
    defined by the minimum start time and maximum endtime,
    or an average defined by either the median or the
    arithmetic mean of the vector of startime and endtime
    values.

    :param ensemble:  ensemble container to be scanned for
    time range.
    :type ensemble:  `TimeSeriesEnsemble` or `SeismogramEnsemble`.
    :param metric:   measure to use to define the time range.
    Accepted values are:
      "inner" - (default) return range defined by largest
          start time to smallest end time.
      "outer" - return range defined by minimum start time and
          largest end time (maximum time span of data)
      "median" - return range as the median of the extracted
          vectors of start time and end time values.
      "mean" - return range as arithmetic average of
          start and end time vectors
    :return:  `TimeWindow` object with start and end times.  If the
    ensemble has all dead member the default constructed TimeWindow
    object will be returned which has zero length.
    """
    if not isinstance(ensemble, (TimeSeriesEnsemble, SeismogramEnsemble)):
        message = "ensemble_time_range:   illegal type for arg0={}\n".format(
            type(ensemble)
        )
        message += "Must be a TimeSeriesEnsemble or SeismogramEnsemble\n"
        raise TypeError(message)
    if metric not in ["inner", "outer", "median", "mean"]:
        message = "ensemble_time_range:  illegal value for metric={}\n".format(metric)
        message += "Must be one of:  inner, outer, median, or mean"
        raise ValueError(message)
    stvector = []
    etvector = []
    for d in ensemble.member:
        if d.live:
            stvector.append(d.t0)
            etvector.append(d.endtime())
    if len(stvector) == 0:
        return TimeWindow()
    if metric == "inner":
        stime = max(stvector)
        etime = min(etvector)
    elif metric == "outer":
        stime = min(stvector)
        etime = max(etvector)
    elif metric == "median":
        stime = np.median(stvector)
        etime = np.median(etvector)
    elif metric == "mean":
        # note numpy's mean an average are different with average being
        # more generic - intentionally use mean which is also consistent with
        # the keyword used
        stime = np.mean(stvector)
        etime = np.mean(etvector)
    # Intentionally not using else to allow an easier extension
    return TimeWindow(stime, etime)

def sort_ensemble(ensemble,key,nullvalue=0.0,ascending=True,drop_dead=True):
    """
    Sorts members of an ensemble by a single Metadata key value.
    
    For graphical QC one often needs to sort an ensemble by a metadata 
    attribute to appraise how the attribute relates to a graphical 
    display of that data.   This function does that with a memory 
    intensive algorithm the makes a copy of the input that is returned.  
    
    :param ensemble:  input to be sorted 
    :type ensemble: `TimeSeriesEnsemble` or `SeismogramEnsemble`
    :param key: key of Metadata attribute whose value is to be used for sorting
    :type key: string
    :param nullvaue:   value assigned for sort for any ensemble member for 
        which a value is not defined for the sort key.
    :type nullvalue:  should match expected type of values associate with key. 
        (default is a float 0.0)
    :param ascending:  boolean defining direction of sort.  When True 
        sort is in ascending order.  False returns data sorted in descending order.
    :param drop_dead:  when True (default) any ensemble member marked dead will 
        be not appear in the output.  When False dead data will get an implicit 
        value defined by "nullvalue".   Where the dead appear will depend upon 
        what that value is relative to the valid values. 
    """
    alg = "sort_ensemble"
    if not isinstance(ensemble,(TimeSeriesEnsemble,SeismogramEnsemble)):
        message = "arg0 must be a TimeSeriesEnsemble or SeismogramEnsemble object\n"
        message += "Actual type={}".type(ensemble)
        raise MsPASSError(alg,message,ErrorSeverity.Fatal)
    vallist=list()
    indexlist=list()
    for i in range(len(ensemble.member)):
        d = ensemble.member[i]
        if d.live:
            if d.is_defined(key):
                vallist.append(d[key])
            else:
                vallist.append(nullvalue)
            indexlist.append(i)
        elif not drop_dead:
            vallist.append(nullvalue)
            indexlist.append(i)
    dfdict = {key : vallist, "member_number" : indexlist}
    df = pd.DataFrame(dfdict)
    del dfdict
    df.sort_values(key,ascending=ascending)
    N = len(df)
    if isinstance(ensemble,TimeSeriesEnsemble):
        ensout = TimeSeriesEnsemble(Metadata(ensemble),N)
    else:
        ensout = SeismogramEnsemble(Metadata(ensemble),N)
    for index, row in df.iterrows():
        i = row["member_number"].astype(int)
        ensout.member.append(ensemble.member[i])
    if N>0:
        ensout.set_live()
    return ensout

