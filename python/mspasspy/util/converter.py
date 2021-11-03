"""
Functions for converting to and from MsPASS data types.
"""
from typing import Collection
import numpy as np
import obspy.core
import collections

from mspasspy.ccore.utility import Metadata, AntelopePf, MsPASSError
from mspasspy.ccore.seismic import (
    _CoreSeismogram,
    Seismogram,
    TimeSeries,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
    Keywords,
)
from mspasspy.ccore.algorithms.basic import ExtractComponent


def dict2Metadata(dic):
    """
    Function to convert Python dict data to Metadata.

    pymongo returns a Python dict container from find queries to any collection.
    Simple type in returned documents can be converted to Metadata
    that are used as headers in the C++ components of mspass.

    :param dict: Python dict to convert
    :type dict: dict
    :return: Metadata object translated from d
    :rtype: :class:`~mspasspy.ccore.Metadata`
    """
    return Metadata(dic)


# dict.toMetadata = dict2Metadata


def Metadata2dict(md):
    """
    Converts a Metadata object to a Python dict.

    This is the inverse of dict2Metadata.  It converts a Metadata object to
    a Python dict. Note that Metadata behavies like dict, so this conversion
    is usually not necessay.

    :param md: Metadata object to convert.
    :type md: :class:`~mspasspy.ccore.Metadata`
    :return: Python dict equivalent to md.
    :rtype: dict
    """
    return dict(md)


Metadata.todict = Metadata2dict


def AntelopePf2dict(pf):
    """
    Converts a AntelopePf object to a Python dict.
    This converts a AntelopePf object to a Python dict by recursively
    decoding the tbls.
    :param pf: AntelopePf object to convert.
    :type md: :class:`~mspasspy.ccore.AntelopePf`
    :return: Python dict equivalent to md.
    :rtype: dict
    """

    keys = pf.keys()
    tbl_keys = pf.tbl_keys()
    arr_keys = pf.arr_keys()

    data = collections.OrderedDict()
    for key in keys:
        val = pf.get(key)
        data[key] = val
    for key in tbl_keys:
        val = pf.get_tbl(key)
        data[key] = val
    for key in arr_keys:
        pf_branch = pf.get_branch(key)
        branch_dict = AntelopePf2dict(pf_branch)
        data[key] = branch_dict
    return data


AntelopePf.todict = AntelopePf2dict


def TimeSeries2Trace(ts):
    """
    Converts a TimeSeries object to an obspy Trace object.

    MsPASS can handle scalar data either as an obspy Trace object or
    as with the mspass TimeSeries object.  The capture nearly the same
    concepts.  The main difference is that TimeSeries support the
    error logging and history features of mspass while obspy, which is
    a separate package, does not.  Obspy has a number of useful
    algorithms that operate on scalar data, however, so it is frequently
    useful to switch between Trace and TimeSeries formats.  The user is
    warned, however, that converting a TimeSeries to a Trace object
    with this function will result in the loss of any error log information.
    For production runs unless the data set is huge, we recommend saving
    the intermediate result AFTER calling this function if there is any
    possibility there are errors posted on any data.  We say after because
    some warning errors from this function may be posted in elog.  Since
    python uses call by reference d may thus be altered.

    :param ts: is the TimeSeries object to be converted
    :type ts: :class:`~mspasspy.ccore.TimeSeries`
    :return: an obspy Trace object from conversion of d.  An empty Trace
        object will be returned if d was marked dead
    :rtype: :class:`~obspy.core.trace.Trace`b
    """
    dresult = obspy.core.Trace()
    dresult.dead_mspass = True
    # Silently return an empty trace object if the data are marked dead now
    if not ts.live:
        return dresult
    # We first deal with attributes in BasicTimeSeries that have to
    # be translated into an obspy stats dictionary like object
    dresult.dead_mspass = False
    dresult.stats["delta"] = ts.dt
    dresult.stats["npts"] = ts.npts
    dresult.stats["starttime"] = obspy.core.UTCDateTime(ts.t0)
    # It appears obspy computes endtime - this throws an AttributeError if
    # included. Ratained for reference to keep someone from putting this back
    # dresult.stats['endtime']=obspy.core.UTCDateTime(ts.endtime())
    # todo relative time attribute
    # These are required by obspy but optional in mspass.  Hence, we have
    # to extract them with caution.  Note defaults are identical to
    # Trace constructor
    if ts.is_defined(Keywords.net):
        dresult.stats["network"] = ts.get_string(Keywords.net)
    else:
        dresult.stats["network"] = ""
    if ts.is_defined(Keywords.sta):
        dresult.stats["station"] = ts.get_string(Keywords.sta)
    else:
        dresult.stats["station"] = ""
    if ts.is_defined(Keywords.chan):
        dresult.stats["channel"] = ts.get_string(Keywords.chan)
    else:
        dresult.stats["channel"] = ""
    if ts.is_defined(Keywords.loc):
        dresult.stats["location"] = ts.get_string(Keywords.loc)
    else:
        dresult.stats["location"] = ""
    if ts.is_defined("calib"):
        dresult.stats["calib"] = ts.get_double("calib")
    else:
        dresult.stats["calib"] = 1.0

    # We have to copy other metadata to preserve them too.  That is
    # complicated by the fact that some (notably endtime) are read only
    # and will abort the program if we just naively copy them.
    # The list below are the keys to exclude either because they
    # are computed by Trace (i.e. endtime) or are already set above
    do_not_copy = [
        "delta",
        "npts",
        "starttime",
        "endtime",
        "network",
        "station",
        "channel",
        "location",
        "calib",
    ]
    for k in ts.keys():
        if not (k in do_not_copy):
            dresult.stats[k] = ts[k]
    # dresult.data = np.ndarray(ts.npts)
    # for i in range(ts.npts):
    # dresult.data[i] = ts.data[i]
    dresult.data = np.array(ts.data)
    return dresult


TimeSeries.toTrace = TimeSeries2Trace


def Seismogram2Stream(
    sg, chanmap=["E", "N", "Z"], hang=[90.0, 0.0, 0.0], vang=[90.0, 90.0, 0.0]
):
    # fixme hang and vang parameters
    """
    Convert a mspass::Seismogram object to an obspy::Stream with 3 components split apart.

    mspass and obspy have completely incompatible approaches to handling three
    component data.  obspy uses a Stream object that is a wrapper around and
    a list of Trace objects.  mspass stores 3C data bundled into a matrix
    container.   This function takes the matrix container apart and produces
    the three Trace objects obspy want to define 3C data.   The caller is
    responsible for how they handle bundling the output.

    A very dark side of this function is any error log entries in the part
    mspass Seismogram object will be lost in this conversion as obspy
    does not implement that concept.  If you need to save the error log
    you will need to save the input of this function to MongoDB to preserve
    the errorlog it may contain.

    :param sg: is the Seismogram object to be converted
    :type sg: :class:`~mspasspy.ccore.Seismogram`
    :param chanmap:  3 element list of channel names to be assigned components
    :type chanmap: list
    :param hang:  3 element list of horizontal angle attributes (azimuth in degrees)
      to be set in Stats array of output for each component.  (default is
      for cardinal directions)
    :type hang: list
    :param vang:  3 element list of vertical angle (theta of spherical coordinates)
      to be set in Stats array of output for each component.  (default is
      for cardinal directions)
    :type vang: list
    :return: obspy Stream object containing a list of 3 Trace objects in
       mspass component order. Presently the data are ALWAYS returned to
       cardinal directions (see above). It will be empty if sg was marked dead
    :rtype: :class:`obspy.core.stream.Stream`
    """
    dresult = obspy.core.Stream()
    dresult.dead_mspass = True
    # Note this logic will silently return an empty Stream object if the
    # data are marked dead
    if sg.live:
        dresult.dead_mspass = False
        uuids = sg.id()
        logstuff = sg.elog
        for i in range(3):
            ts = ExtractComponent(sg, i)
            ts.put_string(Keywords.chan, chanmap[i])
            ts.put_double(Keywords.channel_hang, hang[i])
            ts.put_double(Keywords.channel_vang, vang[i])
            # ts is a CoreTimeSeries but we need to add a few things to
            # make it mesh with TimeSeries2Trace
            tsex = TimeSeries(ts, uuids)
            tsex.elog = logstuff
            dobspy = TimeSeries2Trace(tsex)
            dresult.append(dobspy)
    else:
        for i in range(3):
            tc = obspy.core.Trace()
            tc.dead_mspass = True
            dresult.append(tc)
    return dresult


Seismogram.toStream = Seismogram2Stream


def Trace2TimeSeries(trace, history=None):
    """
    Convert an obspy Trace object to a TimeSeries object.

    An obspy Trace object mostly maps directly into the mspass TimeSeries
    object with the stats of Trace mapping (almost) directly to the TimeSeries
    Metadata object that is a base class to TimeSeries.  A deep copy of the
    data vector in the original Trace is made to the result. That copy is
    done in C++ for speed (we found a 100+ fold speedup using that mechanism
    instead of a simple python loop)  There is one important type collision
    in copying obspy starttime and endtime stats fields.  obspy uses their
    UTCDateTime object to hold time but TimeSeries only supports an epoch
    time (UTCDateTime.timestamp) so the code here has to convert from the
    UTCDateTime to epoch time in the TimeSeries.  Note in a TimeSeries
    starttime is the t0 attribute.

    The biggest mismatch in Trace and TimeSeries is that Trace has no concept
    of object level history as used in mspass.   That history must be maintained
    outside obspy.  To maintain full history the user must pass the
    history maintained externally through the optional history parameter.
    The contents of history will be loaded directly into the result with
    no sanity checks.

    :param trace: obspy trace object to convert
    :type trace: :class:`~obspy.core.trace.Trace`
    :param history:  mspass ProcessingHistory object to post to result.
    :return: TimeSeries object derived from obpsy input Trace object
    :rtype: :class:`~mspasspy.ccore.TimeSeries`
    """
    # The obspy trace object stats attribute only acts like a dictionary
    # we can't use it directly but this trick simplifies the copy to
    # mesh with py::dict for pybind11 - needed in TimeSeries constructor below
    h = dict(trace.stats)
    # These tests are excessively paranoid since starttime and endtime
    # are required attributes in Trace, but better save in case
    # someone creates one outside obspy
    if Keywords.starttime in trace.stats:
        t = h[Keywords.starttime]
        h[Keywords.starttime] = t.timestamp
    else:
        # We have to set this to something if it isn't set or
        # the TimeSeries constructor may abort
        h[Keywords.starttime] = 0.0
    # we don't require endtime in TimeSeries so ignore if it is not set
    if "endtime" in trace.stats:
        t = h["endtime"]
    h["endtime"] = t.timestamp
    #
    # these define a map of aliases to apply when we convert to mspass
    # metadata from trace - we redefined these names but others could
    # surface as obspy evolves independently from mspass
    mspass_aliases = dict()
    mspass_aliases["station"] = Keywords.sta
    mspass_aliases["network"] = Keywords.net
    mspass_aliases["location"] = Keywords.loc
    mspass_aliases["channel"] = Keywords.chan
    for k in mspass_aliases:
        if k in h:
            x = h.pop(k)
            alias_key = mspass_aliases[k]
            h[alias_key] = x
    dout = TimeSeries(h, trace.data)
    if history != None:
        dout.load_history(history)
    dout.set_live()
    # The following dead_mspass attribute is used by our decorator API
    # to determine whether an object was dead before the conversion.
    try:
        if trace.dead_mspass:
            dout.live = False
    except AttributeError:
        pass
    return dout


obspy.core.Trace.toTimeSeries = Trace2TimeSeries


def Stream2Seismogram(st, master=0, cardinal=False, azimuth="azimuth", dip="dip"):
    """
    Convert obspy Stream to a Seismogram.

    Convert an obspy Stream object with 3 components to a mspass::Seismogram
    (three-component data) object.  This implementation actually converts
    each component first to a TimeSeries and then calls a C++ function to
    assemble the complete Seismogram.   This has some inefficiencies, but
    the assumption is this function is called early on in a processing chain
    to build a raw data set.

    :param st: input obspy Stream object.  The object MUST have exactly 3 components
        or the function will throw a AssertionError exception.  The program is
        less dogmatic about start times and number of samples as these are
        handled by the C++ function this python script calls.  Be warned,
        however, that the C++ function can throw a MsPASSrror exception that
        should be handled separately.
    :param master: a Seismogram is an assembly of three channels composed created from
        three TimeSeries/Trace objects.   Each component may have different
        metadata (e.g. orientation data) and common metadata (e.g. station
        coordinates).   To assemble a Seismogram a decision has to be made on
        which component has the definitive common metadata.   We use a simple
        algorithm and clone the data from one component defined by this index.
        Must be 0,1, or 2 or the function wil throw a RuntimeError.  Default is 0.
    :param cardinal: boolean used to define one of two algorithms used to assemble the
        bundle.  When true the three input components are assumed to be in
        cardinal directions (x1=positive east, x2=positive north, and x3=positive up)
        AND in a fixed order of E,N,Z. Otherwise the Metadata fetched with
        the azimuth and dip keys are used for orientation.
    :param azimuth: defines the Metadata key used to fetch the azimuth angle
       used to define the orientation of each component Trace object.
       Default is 'azimuth' used by obspy.   Note azimuth=hang in css3.0.
       Cannot be aliased - must be present in obspy Stats unless cardinal is true
    :param dip:  defines the Metadata key used to fetch the vertical angle orientation
        of each data component.  Vertical angle (vang in css3.0) is exactly
        the same as theta in spherical coordinates.  Default is obspy 'dip'
        key. Cannot be aliased - must be defined in obspy Stats unless
        cardinal is true

    :raise: Can throw either an AssertionError or MsPASSrror(currently defaulted to
    pybind11's default RuntimeError.  Error message can be obtained by
    calling the what method of RuntimeError).
    """
    # First make sure we have exactly 3 components
    assert len(st) == 3, "Stream length must be EXACTLY 3 for 3-components"
    assert 0 <= master < 3, "master argument must be 0, 1, or 2"

    # if all traces are dead in a stream, it should be converted to a dead seismogram
    try:
        size = len(st)
        for i in range(len(st)):
            if st[i].dead_mspass:
                size -= 1
        if size == 0:
            res = Seismogram()
            res.live = False
            return res
    except AttributeError:
        pass

    # Complicated logic here, but the point is to make sure the azimuth
    # attribute is set. The cardinal part is to override the test if
    # we can assume he components are ENZ
    if not cardinal:
        if (
            azimuth not in st[0].stats
            or azimuth not in st[1].stats
            or azimuth not in st[2].stats
        ):
            raise RuntimeError(
                "Stream2Seismogram:  Required attribute "
                + azimuth
                + " must be in mdother list"
            )
    if not cardinal:
        if dip not in st[0].stats or dip not in st[1].stats or dip not in st[2].stats:
            raise RuntimeError(
                "Stream2Seismogram:  Required attribute "
                + dip
                + " must be in mdother list"
            )
    # Outer exception handler to handle range of possible errors in
    # converting each component.  Note we pass an empty list for mdother
    # and aliases except the master
    bundle = []
    for i in range(3):
        bundle.append(Trace2TimeSeries(st[i]))
    # The constructor we use below has frozen names hang for azimuth and
    # vang for what obspy calls dip.   Copy to those names - should work
    # even if the hang and vang are the names although with some inefficiency
    # assume that would not be normal so avoid unnecessary code
    if cardinal:
        bundle[0].put(Keywords.channel_hang, 90.0)
        bundle[1].put(Keywords.channel_hang, 0.0)
        bundle[2].put(Keywords.channel_hang, 0.0)
        bundle[0].put(Keywords.channel_vang, 90.0)
        bundle[1].put(Keywords.channel_vang, 90.0)
        bundle[2].put(Keywords.channel_vang, 0.0)
    else:
        for i in range(3):
            hang = bundle[i].get_double(azimuth)
            bundle[i].put(Keywords.channel_hang, hang)
            vang = bundle[i].get_double(dip)
            bundle[i].put(Keywords.channel_vang, vang)
    # Assume now bundle contains all the pieces we need.   This constructor
    # for _CoreSeismogram should then do the job
    # This may throw an exception, but we require the caller to handle it
    # All errors returned by this constructor currenlty leave the data INVALID
    # so handler should discard anything with an error
    dout = _CoreSeismogram(bundle, master)
    res = Seismogram(dout, "INVALID")
    res.live = True
    return res


obspy.core.Stream.toSeismogram = Stream2Seismogram


def TimeSeriesEnsemble2Stream(tse):
    """
    Convert a timeseries ensemble to stream.  Always copies
    all ensemble Metadata to tse members before conversion.   That is
    necessary to avoid loss of data in the case where the only copy is
    stored in the ensemble's metadata.

    :param tse: timeseries ensemble
    :return: converted stream
    """
    res = obspy.core.Stream()
    # Save the set of keys in ensemble metadata and post the list
    # to ensmeble md with a special key.  The inverse function
    # looks for that special list and uses it to restore the
    # ensemble's metadata.  That handling is similar to how we handle
    # dead on atomic data.  Note _get_ensemble_md is not a C function but
    # defined only in the pybind11 wrappers
    md = tse._get_ensemble_md()
    # Having this as const in is a maitenanc issue.  Shouldn't be a problem
    # provided no other file uses this key.  For now it is self contained in
    # this module.
    tse["CONVERTER_ENSEMBLE_KEYS"] = md.keys()
    # This pushes all conents of ensemble md to all members. That includes
    # the scratch list we just posted. inverse needs to clear the temp
    tse.sync_metadata()
    # Remover the temporary from the ensemble metadata or the return
    # will be inconsistent with the input
    tse.erase("CONVERTER_ENSEMBLE_KEYS")
    for ts in tse.member:
        res.append(TimeSeries2Trace(ts))
    return res


TimeSeriesEnsemble.toStream = TimeSeriesEnsemble2Stream


def _converter_get_ensemble_keys(ens):
    """
    Small helper for converting from Stream to either of the
    mspass ensemble objects.   Returns a list of keys from the first
    live member using the internal key CONVERTER_ENSEMBLE_KEYS.
    Normally should return a list of any ensemble keys.  If the
    special key is not found it returns an empty list.
    """
    for d in ens.member:
        if d.live:
            if d.is_defined("CONVERTER_ENSEMBLE_KEYS"):
                return d["CONVERTER_ENSEMBLE_KEYS"]
            else:
                return list()


def Stream2TimeSeriesEnsemble(stream):
    """
    Convert a stream to timeseries ensemble.
    :param stream: stream input
    :return: converted timeseries ensemble
    """
    size = len(stream)
    tse = TimeSeriesEnsemble()
    for i in range(size):
        tse.member.append(Trace2TimeSeries(stream[i]))
        # potential dead loss problem is resolved by saving the info in converted objects

    # Handle the ensemble metadata.   The little helper we call here
    # get the list set with CONVERTER_ENSEMBLE_KEYS.
    enskeys = _converter_get_ensemble_keys(tse)
    if len(enskeys) > 0:
        post_ensemble_metadata(tse, enskeys)
        # By default the above leaves copies of the ensemble md in each member
        # Treat that a ok, but we do need to clear the temporary we posted
        for d in tse.member:
            # Depend on the temp being set in all members - watch out in
            # maintenance if any of the related code changes that may be wrong
            d.erase("CONVERTER_ENSEMBLE_KEYS")

    return tse


obspy.core.Stream.toTimeSeriesEnsemble = Stream2TimeSeriesEnsemble


def SeismogramEnsemble2Stream(sge):
    """
    Convert a seismogram ensemble to stream
    :param sge: seismogram ensemble input
    :return: stream
    """
    # This uses the same approach as TimeSeriesEnsemblet2Stream to handle
    # ensemble metadata.  See comments there for potential maintenanc issues
    md = sge._get_ensemble_md()
    sge["CONVERTER_ENSEMBLE_KEYS"] = md.keys()
    sge.sync_metadata()
    # as above remove this temporary from sge or it alters the
    # input - python gives us a pointer to this thing so it is mutable
    sge.erase("CONVERTER_ENSEMBLE_KEYS")
    res = obspy.core.Stream()
    for sg in sge.member:
        res += Seismogram2Stream(sg)
    return res


SeismogramEnsemble.toStream = SeismogramEnsemble2Stream


def Stream2SeismogramEnsemble(stream):
    """
    Convert a stream to seismogram ensemble.
    :param stream: stream input
    :return: converted seismogram ensemble
    """
    size = len(stream)
    res = SeismogramEnsemble()
    for i in range(int(size / 3)):
        res.member.append(Stream2Seismogram(stream[i * 3 : i * 3 + 3], cardinal=True))
        # fixme cardinal
    # Handle the ensemble metadata.   The little helper we call here
    # get the list set with CONVERTER_ENSEMBLE_KEYS.  The
    # code here is identical to that for TimeSeriesEnsemble version
    # because the ensemble containers have parallel symbols and the atomic
    # members are close relatives
    enskeys = _converter_get_ensemble_keys(res)
    if len(enskeys) > 0:
        post_ensemble_metadata(res, enskeys)
        # By default the above leaves copies of the ensemble md in each member
        # Treat that a ok, but we do need to clear the temporary we posted
        for d in res.member:
            # Depend on the temp being set in all members - watch out in
            # maintenance if any of the related code changes that may be wrong
            d.erase("CONVERTER_ENSEMBLE_KEYS")
    return res


obspy.core.Stream.toSeismogramEnsemble = Stream2SeismogramEnsemble


def _all_members_match(ens, key):
    """
    This is a helper function for below.  I scans ens to assure all members
    of the ensemble have the same value for the requested key. It uses
    the python operator == for testing.  That can fail for a variety of
    reasons the "match" may be overly restrictive for some types of data
    linked to key.

    :param ens:  ensemble data to scan.  Function will throw a MsPASS error if
      the data this symbol is associated with is not a mspass ensemble object.
    :param key:  key whose values are to be tested for all members of ens.
    :return:  True of all members match, false if there are any differences.
      Note if a key is not defined in a live member the result will be false.
      Dead data are ignored.
    """
    if isinstance(ens, TimeSeriesEnsemble) or isinstance(ens, SeismogramEnsemble):
        nlive = 0
        for d in ens.member:
            if d.live:
                if nlive == 0:
                    val0 = d[key]
                    nlive += 1
                else:
                    if not key in d:
                        return False
                    val = d[key]
                    if val0 != val:
                        return False
                    nlive += 1
        return True
    else:
        raise MsPASSError(
            "_all_members_match:  input is not a mspass ensemble object", "Invalid"
        )


def post_ensemble_metadata(ens, keys=[], check_all_members=False, clean_members=False):
    """
    It may be necessary to call this function after conversion from
    an obspy Stream to one of the mspass Ensemble classes.  This function
    is necessary because a mspass Ensemble has a concept not part of the
    obspy Stream object.  That is, mspass ensembles have a global
    Metadata container.  That container is expected to contain Metadata
    common to all members of the ensemble.  For example, for data from
    a single earthquake it would be sensible to post the source location
    information in the ensemble metadata container rather than
    having duplicates in each member.

    Two different approaches can be used to do this copy.  The faster,
    but least reliable method is to simply copy the values from the first
    member of the ensemble.  That approach is enabled by default.
    It is completely reliable when used after a conversion from an obspy
    Stream but ONLY if the data began life as a mspass ensemble with
    exactly the same keys set as global.   The type example of that
    is after an obspy algorithm is applied to a mspass ensemble
    via the mspass decorators.

    A more cautious algorithm can be enabled by setting check_all_members
    True. In that mode the list of keys received is tested with a
    not equal test for against each member.  Note we do not do anything
    fancy with floating point data to allow for finite precision.
    The reason is Metadata float values are normally expected to be
    constant data.  In that case an != test will yield false when the
    comparison is between two copies.  The not equal test may fail, however,
    if used with computed floating point numbers.   An example where
    that is possible would be spatial gathers like PP data assembled by
    midpoint coordinates.  If you need to build gathers in such a context
    we recommend you use an integer image point tied to a specialized
    document collection in MongoDB that defines the geometry of that
    point.  There may be other examples, but the point is don't trust
    computed floating point values to work.  It will also not work if
    the values of a key-value pair don't support an != comparison.
    That could be common if the value request for copy was a python object.

    :param ens:  ensemble data to be processed.  The function will throw
      a MsPASSError exception of ens is not either a TimeSeriesEnsemble or a
      SeismogramEnsemble.
    :param keys:  is expected to be a list of metadata keys (required to be
      strings) that are to be copied from member metadata to ensemble
      metadata.
    :param check_all_members:  switch controlling method used to extract
      metadata that is to be copied (see above for details). Default is False
    :param clean_members:  when true data copied to ensemble metadata
      will be removed from all members.  This option is only allowed
      if check_all_members is set True.  It will be silently ignored if
      check_all_members is False.
    """
    alg = "post_ensemble_metadata"

    if isinstance(ens, TimeSeriesEnsemble) or isinstance(ens, SeismogramEnsemble):
        md = Metadata()
        for d in ens.member:
            if d.live:
                for k in keys:
                    if not k in d:
                        raise MsPASSError(
                            alg
                            + ":  no data matching requested key="
                            + k
                            + " Cannot post to ensemble",
                            "Invalid",
                        )
                    md[k] = d[k]
        if check_all_members:
            for d in ens.member:
                for k in keys:
                    if not _all_members_match(ens, k):
                        raise MsPASSError(
                            alg
                            + ":  Data mismatch data members with key="
                            + k
                            + "\n  In check_all_members mode all values associated with this key must match",
                            "Invalid",
                        )
            if clean_members:
                for d in ens.member:
                    for k in keys:
                        d.erase(k)
        ens.update_metadata(md)

    else:
        raise MsPASSError(
            alg
            + ":  Illegal data received.  This function runs only on mspass ensemble objects",
            "Invalid",
        )
