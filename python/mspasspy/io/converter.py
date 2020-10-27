"""
Functions for converting to and from MsPASS data types.
"""
import numpy as np
import obspy.core

from mspasspy.ccore.utility import (ErrorSeverity, Metadata)
from mspasspy.ccore.seismic import (CoreSeismogram,
                                    CoreTimeSeries,
                                    Seismogram,
                                    TimeReferenceType,
                                    TimeSeries,
                                    TimeSeriesEnsemble,
                                    SeismogramEnsemble)
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
    :rtype: :class:`~obspy.core.trace.Trace`
    """
    dresult = obspy.core.Trace()
    dresult.dead_mspass = True
    # Silently return an empty trace object if the data are marked dead now
    if not ts.live:
        return dresult
    # We first deal with attributes in BasicTimeSeries that have to
    # be translated into an obspy stats dictionary like object
    dresult.dead_mspass = False
    dresult.stats['delta'] = ts.dt
    dresult.stats['npts'] = ts.npts
    dresult.stats['starttime'] = obspy.core.UTCDateTime(ts.t0)
    # todo relative time attribute
    # These are required by obspy but optional in mspass.  Hence, we have
    # to extract them with caution.  Note defaults are identical to
    # Trace constructor
    if ts.is_defined('net'):
        dresult.stats['network'] = ts.get_string('net')
    else:
        dresult.stats['network'] = ''
    if ts.is_defined('sta'):
        dresult.stats['station'] = ts.get_string('sta')
    else:
        dresult.stats['station'] = ''
    if ts.is_defined('chan'):
        dresult.stats['channel'] = ts.get_string('chan')
    else:
        dresult.stats['channel'] = ''
    if ts.is_defined('loc'):
        dresult.stats['location'] = ts.get_string('loc')
    else:
        dresult.stats['location'] = ''
    if ts.is_defined('calib'):
        dresult.stats['calib'] = ts.get_double('calib')
    else:
        dresult.stats['calib'] = 1.0

    # It might be possible to replace the following loop with
    # calling an (nonexistent) function called md2dict that would
    # generalize this loop.   May need that for serialization and it
    # may be written in C.  If so, we might want to replace the loop
    # below with a call to the function for speed.  On the other hand,
    # creating a dict would require a copy loop or perhaps the
    # function could be simplified with a call to Trace(dict) where
    # dict is the translation of md - as I read Trace i think that might
    # work as an alternative algorithm.  Make it work before you make
    # it fast!
    for k in ts.keys():
        dresult.stats[k] = ts[k]
    dresult.data = np.ndarray(ts.npts)
    for i in range(ts.npts):
        dresult.data[i] = ts.data[i]
    return dresult


TimeSeries.toTrace = TimeSeries2Trace


def Seismogram2Stream(sg, chanmap=['E', 'N', 'Z'], hang=[90.0, 0.0, 0.0], vang=[90.0, 90.0, 0.0]):
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
            ts.put_string('chan', chanmap[i])
            ts.put_double('hang', hang[i])
            ts.put_double('vang', vang[i])
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


def Trace2TimeSeries(trace):
    """
    Convert an obspy Trace object to a TimeSeries object.

    :param trace: obspy trace object to convert
    :type trace: :class:`~obspy.core.trace.Trace`
    :return: TimeSeries object derived from obpsy input Trace object
    :rtype: :class:`~mspasspy.ccore.TimeSeries`
    """
    # First get the data size to know use allocating constructor for TimeSeries
    ns = trace.stats.npts
    dout = CoreTimeSeries(ns)
    # an api change is needed here.  Also TimeSeries needs to handle invalid objectid
    dout = TimeSeries(dout, "INVALID")
    dout.npts = ns
    # Now extract TimeSeries attributes equivalent in d.stats and write to output
    dout.dt = trace.stats.delta
    # obspy only understands UTC time as a standard so we just set it
    dout.tref = TimeReferenceType.UTC
    dout.live = True
    try:
        if trace.dead_mspass:
            dout.live = False
    except AttributeError:
        pass
    t0utc = trace.stats.starttime
    dout.t0 = t0utc.timestamp
    for k in trace.stats:
        if k not in ('npts', 'delta', 'starttime', 'sampling_rate', 'endtime'):
            dout[k] = trace.stats[k]
    # Here, we do not use append, which is equivalent to the C++ method
    # push_back, for efficiency.
    for i in range(ns):
        dout.data[i] = trace.data[i]
    return dout


obspy.core.Trace.toTimeSeries = Trace2TimeSeries


def Stream2Seismogram(st, master=0, cardinal=False, azimuth='azimuth', dip='dip'):
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
        if azimuth not in st[0].stats or azimuth not in st[1].stats or azimuth not in st[2].stats:
            raise RuntimeError("Stream2Seismogram:  Required attribute " +
                               azimuth + " must be in mdother list")
    if not cardinal:
        if dip not in st[0].stats or dip not in st[1].stats or dip not in st[2].stats:
            raise RuntimeError("Stream2Seismogram:  Required attribute " +
                               dip + " must be in mdother list")
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
        bundle[0].put("hang", 90.0)
        bundle[1].put("hang", 0.0)
        bundle[2].put("hang", 0.0)
        bundle[0].put("vang", 90.0)
        bundle[1].put("vang", 90.0)
        bundle[2].put("vang", 0.0)
    else:
        for i in range(3):
            hang = bundle[i].get_double(azimuth)
            bundle[i].put('hang', hang)
            vang = bundle[i].get_double(dip)
            bundle[i].put('vang', vang)
    # Assume now bundle contains all the pieces we need.   This constructor
    # for CoreSeismogram should then do the job
    # This may throw an exception, but we require the caller to handle it
    # All errors returned by this constructor currenlty leave the data INVALID
    # so handler should discard anything with an error
    dout = CoreSeismogram(bundle, master)
    res = Seismogram(dout, 'INVALID')
    res.live = True
    return res


obspy.core.Stream.toSeismogram = Stream2Seismogram


def TimeSeriesEnsemble2Stream(tse):
    res = obspy.core.Stream()
    for ts in tse.member:
        res.append(TimeSeries2Trace(ts))
    return res


TimeSeriesEnsemble.toStream = TimeSeriesEnsemble2Stream


def Stream2TimeSeriesEnsemble(stream):
    size = len(stream)
    tse = TimeSeriesEnsemble()
    for i in range(size):
        tse.member.append(Trace2TimeSeries(stream[i]))
        # potential dead loss problem is resolved by saving the info in converted objects
    return tse


obspy.core.Stream.toTimeSeriesEnsemble = Stream2TimeSeriesEnsemble


def SeismogramEnsemble2Stream(sge):
    res = obspy.core.Stream()
    for sg in sge.member:
        res += Seismogram2Stream(sg)
    return res


SeismogramEnsemble.toStream = SeismogramEnsemble2Stream


def Stream2SeismogramEnsemble(stream):
    size = len(stream)
    res = SeismogramEnsemble()
    for i in range(int(size / 3)):
        res.member.append(Stream2Seismogram(stream[i * 3:i * 3 + 3], cardinal=True))
        # fixme cardinal
    return res


obspy.core.Stream.toSeismogramEnsemble = Stream2SeismogramEnsemble
