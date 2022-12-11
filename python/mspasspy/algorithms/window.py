from mspasspy.ccore.utility import (
    MsPASSError,
    AtomicType,
    ErrorSeverity,
    ProcessingStatus,
)
from mspasspy.ccore.seismic import (
    Seismogram,
    TimeSeries,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
    TimeSeriesVector
)
from mspasspy.ccore.algorithms.basic import (
    TimeWindow,
    _TopMute,
    _WindowData,
    _WindowData3C,
    repair_overlaps,
    splice_segments
)
from mspasspy.ccore.algorithms.amplitudes import (
    _scale,
    _scale_ensemble,
    _scale_ensemble_members,
    ScalingMethod,
)
from mspasspy.util.decorators import mspass_func_wrapper


def ensemble_error_post(d, alg, message, severity):
    """
    This is a small helper function useful for error handlers in except
    blocks for ensemble objects.  If a function is called on an ensemble
    object that throws an exception this function will post the message
    posted to all ensemble members.  It silently does nothing if the
    ensemble is empty.
    :param d: is the ensemble data to be handled.  It print and error message
      and returns doing nothing if d is not one of the known ensemble
      objects.
    :param alg: is the algorithm name posted to elog on each member
    :param message: is the string posted to all members
    :param severity: is the error severity level
    """
    if isinstance(d, TimeSeriesEnsemble) or isinstance(d, SeismogramEnsemble):
        n = len(d.member)
        if n <= 0:
            return
        for i in range(n):
            d.member[i].elog.log_error(alg, str(message), severity)
    else:
        print(
            "Coding error - ensemble_error_post was passed an unexpected data type of",
            type(d),
        )
        print("Not treated as fatal but a bug fix is needed")


def _post_amplitude(d, method, amp):
    """
    Internal small helper function for repeated tests of method - posts the
    computed amplitudes to metadata with a different key for each method
    used to compute amplitude.
    """
    if method == "rms" or method == "RMS":
        d["rms_amplitude"] = amp
    elif method == "perc":
        d["perc_amplitude"] = amp
    elif method == "MAD" or method == "mad":
        d["mad_amplitude"] = amp
    else:
        d["amplitude"] = amp


@mspass_func_wrapper
# inplace_return was intentionally ommitted from thie arg list here because
# False should always be enforced.  If the default changed that would
# break this function.
def scale(
    d,
    compute_from_window=False,
    window=None,
    method="peak",
    level=1.0,
    scale_by_section=False,
    use_mean=False,
    object_history=False,
    alg_name="scale",
    alg_id=None,
    dryrun=False,
    function_return_key=None,
):
    """
    Top level function interface to data scaling methods.

    This function can be used to scale seismic data contained in any of
    the four seismic data objects defined in mspass:  TimeSeries, Seismogram,
    TimeSeriesEnsemble, and SeismogramEnsemble.   An extensive set of
    amplitude estimation metrics are available by selecting one of the
    allowed values for the method parameter.   Ensembles can be scaled
    at the individual seismogram level or as a whole (scale_by_section=True).

    Note all methods preserve the original amplitude by updating the
    Metadata parameter calib to include the scaling.  i.e. as always the
    amplitude of the data in physical units can be restored by multiplying
    the data samples by calib.

    :param d:  is input data object.  If not one of the four mspass seismic
      data types noted above the function will throw a RuntimeError exception.
    :param compute_from_window: boolean used to compute amplitude and scale
      based on a windowed section of the input waveform.   By default (this
      boolan False) the amplitude for scaling is computed from the full
      waveform.  When True the window argument must contain a valid TimeWindow
      that spans a time range smaller than the data range.  In that situation
      if the window is inconsistent with the data range the return will be
      marked dead and messages will be found posted in elog.  For ensembles
      all or only  portion of the group will be killed if this happens.
      Note this parameter is also ignored when scale_by_section is true.
    :param window:  mspass TimeWindow object defining the time range
      over which the amplitude for scaling is to be computed.  (see the
      compute_from_window parameter description)
    :param method: string defining the gain method to use.  Currently supported
      method values are:  peak, RMS (rms accepted), perc, and MAD
      (also accepts mad or Mad).  Peak uses the largest amplitude for
      scaling.  For 3C data that means vector amplitude while for scalar data
      it is the largest absolute value. rms is the standard rms measure,
      although for 3C data is is rms vector amplitudes so scaling is by the
      number of vectors not the total number of samples (3 times number of
      vectors).  perc uses a sorted percentage clip level metric as used in
      seismic unix.  mad is a variant where the value returned is the
      median absolute deviation (mad) that is actual the same as perc=1/2.
      Default is peak.  WARNING:  if an invalid value for method is passed the
      data will be returned unaltered with a complaint message issue for
      very datum (indivually or in ensembles) run that way.
    :param level:   For all but perc this defines the scale to which the data
      are scaled.  For perc it is used to set the percent clip level.
      If the value passed is illegal (0 or negative for most methods while
      perc must also be positive but less or equal 1) a complain message will
      be posted to elog and the level adjusted to 1.0.
    :param scale_by_section:  is a boolean that controls the scaling
      behavior on ensembles only (It is silently ignored for atomic
      TimeSeries and Seismogram data).  When true a single gain factor is
      applied to all members of an ensemble.  When false each member is
      gained individually as if this function were applied in a loop to
      each member.
    :param use_mean:  boolean used only for ensembles and when scale_by_section is
      True.   The algorithm used in that case has an option to use the mean
      log amplitude for scaling the section instead of the default median
      amplitude.

    :return: Data scaled to specified level.  Note the scaling always preserves
      absolute amplitude by adjusting the value of the calib attribute of the
      return so calib*data is the same value before and after the scaling.
    :rtype: same as input

    """
    if isinstance(d, TimeSeries) or isinstance(d, Seismogram):
        if d.dead():
            return d
    # First validate arguments
    # The logic here would be much cleaner if ensembles had an elog attribute
    # may happen as group discussions have proposed that change.  this should
    # change to be cleaner of elog is added to ensmeble objects
    if (
        method != "peak"
        and method != "RMS"
        and method != "rms"
        and method != "perc"
        and method != "MAD"
        and method != "mad"
    ):
        message = (
            "method parameter passed = "
            + method
            + " is not valid.  Should be peak, rms, perc, or mad\nContinuing with no change to data"
        )
        if isinstance(d, TimeSeriesEnsemble) or isinstance(d, SeismogramEnsemble):
            ensemble_error_post(d, alg_name, message, ErrorSeverity.Complaint)
        else:
            # This could cause an abort if the input is not one of the four stock data types
            # but that is ok as that is an obvious usage error and should be a fatal error
            d.elog.log_error(
                alg_name,
                "method parameter passed = "
                + method
                + " is not valid.  "
                + "Should be peak, rms, perc, or mad",
                ErrorSeverity.Complaint,
            )
        return d
    if method == "perc":
        if level <= 0 or level > 1.0:
            message = "perc scaling method given illegal value={plevel}\nDefaulted to 1.0".format(
                plevel=level
            )
            if isinstance(d, TimeSeriesEnsemble) or isinstance(d, SeismogramEnsemble):
                ensemble_error_post(d, alg_name, message, ErrorSeverity.Complaint)
            else:
                d.elog.log_error(alg_name, message, ErrorSeverity.Complaint)
                level = 1.0
    else:
        if level <= 0.0:
            message = "{meth} scaling method given illegal value={slevel}\nDefaulted to 1.0".format(
                meth=method, slevel=level
            )
            if isinstance(d, TimeSeriesEnsemble) or isinstance(d, SeismogramEnsemble):
                ensemble_error_post(d, alg_name, message, ErrorSeverity.Complaint)
            else:
                d.elog.log_error(alg_name, message, ErrorSeverity.Complaint)
            level = 1.0
    if compute_from_window:
        if isinstance(window, TimeWindow):
            ampwin = window
        else:
            message = "optional window parameter set but value is not a TimeWindow object\nReverting to unwindowed estimate"
            if isinstance(d, TimeSeriesEnsemble) or isinstance(d, SeismogramEnsemble):
                ensemble_error_post(d, alg_name, message, ErrorSeverity.Complaint)
            else:
                d.elog.log_error(alg_name, message, ErrorSeverity.Complaint)
            # this is an invalid window because start>end is used as a signal
            # in WindowData to use the entire waveform.  It switches automatically
            # without logging an error (currently).  Definitely a little weird
            # but this hides that detail for python users
            ampwin = TimeWindow(0.0, -1.0)
    else:
        ampwin = TimeWindow(0.0, -1.0)
    # The pybind11 and C++ way of defining an enum class creates an
    # obnoxiously ugly syntax. We insulate the user from this oddity
    # by using a string arg to define this enum passed to _scale
    method_to_use = ScalingMethod.Peak
    if method == "rms" or method == "RMS":
        method_to_use = ScalingMethod.RMS
    elif method == "perc":
        method_to_use = ScalingMethod.perc
    elif method == "MAD" or method == "mad":
        method_to_use = ScalingMethod.MAD
    try:
        # Note this logic depends on an oddity of the C++ api in the
        # functions called with the _scale binding name.   When the TimeWindow
        # is invalid the entire data range is silently used - not viewed as
        # an error.  Hence, the following works only when the logic above to
        # handle the definition of the window parameter is set.
        if isinstance(d, TimeSeries) or isinstance(d, Seismogram):
            amp = _scale(d, method_to_use, level, ampwin)
            _post_amplitude(d, method_to_use, amp)
        elif isinstance(d, TimeSeriesEnsemble) or isinstance(d, SeismogramEnsemble):
            if len(d.member) <= 0:  # Silently return nothing if the ensemble is empy
                return d
            if scale_by_section:
                amp = _scale_ensemble(d, method_to_use, level, use_mean)
                # We post the amplitude the ensembe's metadata in this case
                _post_amplitude(d, method_to_use, amp)
            else:
                ampvec = _scale_ensemble_members(d, method_to_use, level, ampwin)
                i = 0
                for x in d.member:
                    if x.live:
                        _post_amplitude(x, method_to_use, ampvec[i])
                    i += 1
        else:
            raise MsPASSError(
                "scale: input data is not a supported mspass seismic data type", "Fatal"
            )
        return d
    except MsPASSError as err:
        if isinstance(d, Seismogram) or isinstance(d, TimeSeries):
            d.elog.log_error(alg_name, str(err), ErrorSeverity.Invalid)
            d.kill()
        # avoid further isinstance at the expense of a maintenance issue.
        # if we add any other supported data objects we could have a
        # problem here.  This assumes what lands here is an ensemble
        else:
            ensemble_error_post(d, alg_name, err)
            for x in d.member:
                x.kill()
        return d
    # this is needed to handle an oddity recommended on this
    # web site:  http://effbot.org/zone/stupid-exceptions-keyboardinterrupt.htm
    except KeyboardInterrupt:
        raise
    except:
        message = "Something threw an unexpected exception\nThat is a bug that needs to be fixed - contact authors"
        if isinstance(d, Seismogram) or isinstance(d, TimeSeries):
            d.elog.log_error(alg_name, message, ErrorSeverity.Invalid)
        else:
            ensemble_error_post(d, alg_name, message, ErrorSeverity.Invalid)


@mspass_func_wrapper
def WindowData(
    d,
    win_start,
    win_end,
    t0shift=None,
    object_history=False,
    alg_name="scale",
    alg_id=None,
    dryrun=False,
):
    """
    Cut data defined by a TimeWindow object.

    Cutting a smaller waveform segment from a larger waveform segment
    is a very common seismic data processing task.   The function is
    a python wrapper to adapt C++ code that accomplishes that task
    to the MsPASS framework.

    Note this function uses one model for being bombproof with a map
    operation.  Any exception that makes the result invalid will cause
    an error message to be posted to the elog attribute of the input
    datum AND the data will be marked dead (killed).

    :param d: is the input data.  d must be either a :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
      object or the function will log an error to d and return a None.
    :param twin_start: defines the start of timeWindow to be cut
    :type twin_start: :class:`float`
    :param twin_end: defines the end of timeWindow to be cut
    :type twin_end: :class:`float`
    :param t0shift: is an optional time shift to apply to the time window.
      This parameter is convenient to avoid conversions to relative time.
      A typical example would be to set t0shift to an arrival time and let
      the window define time relative to that arrival time.  Default is None
      which cause the function to assume twin is to be used directly.
    :param object_history: boolean to enable or disable saving object
      level history.  Default is False.  Note this functionality is
      implemented via the mspass_func_wrapper decorator.
    :param alg_name:   When history is enabled this is the algorithm name
      assigned to the stamp for applying this algorithm.
      Default ("WindowData") should normally be just used.
      Note this functionality is implemented via the mspass_func_wrapper decorator.
    :param ald_id:  algorithm id to assign to history record (used only if
      object_history is set True.)
      Note this functionality is implemented via the mspass_func_wrapper decorator.
    :param dryrun:
      Note this functionality is implemented via the mspass_func_wrapper decorator.
    :param dryrun:
      Note this functionality is implemented via the mspass_func_wrapper decorator.

    :return: copy of d with sample range reduced to twin range.  Returns
      an empty version of the parent data type (default constructor) if
      the input is marked dead
    """
    if d.dead():
        return d
    twcut = TimeWindow(win_start, win_end)
    if t0shift:
        twcut.shift(t0shift)
    try:
        # This handler duplicates an error test in the WindowData C code but
        # it will be more efficient to handle it here.
        if twcut.start < d.t0 or twcut.end > d.endtime():
            detailline = "Window range: {wst},{wet}  Data range:  {dst},{det}".format(
                wst=twcut.start, wet=twcut.end, dst=d.t0, det=d.endtime()
            )
            d.elog.log_error(
                "WindowData",
                "Data range is smaller than window range\n" + detailline,
                ErrorSeverity.Invalid,
            )
            d.kill()
            return d
        if isinstance(d, TimeSeries):
            dcut = _WindowData(d, twcut)
            return dcut
        elif isinstance(d, Seismogram):
            dcut = _WindowData3C(d, twcut)
            return dcut
        else:
            raise RuntimeError(
                "WindowData:  Invalid input data type received=" + str(type(d))
            )
    except MsPASSError as err:
        d.log_error("WindowData", str(err), ErrorSeverity.Invalid)
        d.kill()
        return d


@mspass_func_wrapper
def WindowData_with_duration(
    d,
    duration,
    t0shift=None,
    object_history=False,
    alg_name="scale",
    alg_id=None,
    dryrun=False,
):
    if duration < 0:
        detailline = "Window duration: {dur}  Data range:  {dst},{det}".format(
            dur=duration, dst=d.t0, det=d.endtime()
        )
        d.elog.log_error(
            "WindowData",
            "Duration is a negative number.\n" + detailline,
            ErrorSeverity.Invalid,
        )
        d.kill()
        return d
    win_start = d.t0 + 1
    win_end = win_start + duration
    if d.dead():
        return d
    twcut = TimeWindow(win_start, win_end)
    if t0shift:
        twcut.shift(t0shift)
    try:
        # This handler duplicates an error test in the WindowData C code but
        # it will be more efficient to handle it here.
        if win_start < d.t0 or win_end > d.endtime():
            detailline = "Window range: {wst},{wet}  Data range:  {dst},{det}".format(
                wst=win_start, wet=win_end, dst=d.t0, det=d.endtime()
            )
            d.elog.log_error(
                "WindowData",
                "Data range is smaller than window range\n" + detailline,
                ErrorSeverity.Invalid,
            )
            d.kill()
            return d
        if isinstance(d, TimeSeries):
            dcut = _WindowData(d, twcut)
            return dcut
        elif isinstance(d, Seismogram):
            dcut = _WindowData3C(d, twcut)
            return dcut
        else:
            raise RuntimeError(
                "WindowData:  Invalid input data type received=" + str(type(d))
            )
    except MsPASSError as err:
        d.log_error("WindowData", str(err), ErrorSeverity.Invalid)
        d.kill()
        return d
#@mspass_func_wrapper
def merge(
    tsvector,
    starttime=None,
    endtime=None,
    fix_overlaps=False,
    zero_gaps=False,
    object_history=False,
    alg_name="merge",
    alg_id=None,
    dryrun=False,
    )->TimeSeries:
    """
    Splices a vector of TimeSeries objects together and optionally carves 
    out a specified time window.  It acts a bit like an obspy function 
    with the same name, but has completely different options and works 
    with native MsPASS TimeSeries objects.  
    
    This function is a workhorse for handling continuous data that are 
    universally stored today as a series of files.   The input to this 
    function is an array of TimeSeries objects that are assummed to be 
    created from a set of data stored in such files.   The data are assumed 
    to be from a common stream of a single channel and sorted so the 
    array index defines a time order.  The algorithm attempts to glue the 
    segments together into a single time series that is returned.   
    The algorithm by default assumes the input is "clean" which means 
    the endtime of each input TimeSeries is 1 sample ahead of the start time
    of the next segment (i.e. (segment[i+1].t0()-segment[i].endtime()) == dt).
    The actual test is that the time difference is less than dt/2.  
    
    This algorithm treats two conditions as a fatal error and will throw 
    a MsPASSError when the condition occurs:
        1.   It checks that the input array of TimeSeries data are in 
             time order.  
        2.   It checks that the inputs all have the same sample rate.  
        3.   If fix_overlaps is False if an overlap is found it 
             is considered an exception.  
    Either of these conditions will cause the function to throw an 
    exception.  The assumption is that either is a user error created 
    by failing to reading the directions that emphasize this requirement 
    for the input.  
    
    Other conditions can cause the output to be marked dead with an 
    error message posted to the output's elog attribute.  These are:
        1.  This algorithm aims to produce an output with data stored in 
            a continuous vector with a length defined by the total time 
            span of the input.  Naive use can create enormously long 
            vectors that would mostly be empty.   (e.g. two random 
            day files with a 5 year difference in start time)  The 
            algorithm refuses to try to merge data when the span exceeds
            an internal threshold of 10^8 samples.   
        2.  If fix_overlaps is false any overlap of successive endtime to 
            the next starttime of more than 0.5 samples will cause the 
            output to be killed.   The assumption is such data have a 
            serious timing problem retained even after any cleaning. 
            
    The above illustrates that this function behaves differently and makes
    different assumption if the argument check_overlaps is True or False.
    False is faster because it bypasses the algorithm used to fix overlaps, 
    but is safer if your data is not perfectly clean in the sense of 
    lacking any timing issues or problem with duplicates.   If the 
    fix_overlaps boolean is set True, the mspass overlap handler is 
    involked that is a C++ function with the name "repair_overlaps".
    The function was designed only to handle the following common 
    situations.  How they are handled is different for each situation. 
        1.  Duplicate waveform segments spanning a common time interval 
            can exist in raw data and accidentally by indexing two copies 
            the same data.   If the samples in the overlapping section 
            match the algorithm will attempt to remove the overlapping 
            section.   The algorithm is known to work only for a pair of 
            pure duplicates and a smaller segment with a start time after 
            a more complete segment.   It may fail if there are more than 
            three or more copies of the same waveform in the input or 
            one of the waveforms spans a smaller time range than the other 
            but has the same start time.   The first is a gross user error. 
            The second should be rare an is conceivable only with raw 
            data where a packet or two was randomly saved twice - something 
            that shouldn't happen but could with flakey hardware. 
        2.  If an overlap is detected AND the sample data in the overlap 
            are different the algorithm assumes the data have a timing 
            problem that created this situation.   Our experience is this
            situation only happens when an instrument has a timing problem. 
            All continuous data generating digitizers we know of use a 
            timing system slaved to an external reference (usually GPS time).
            If the external signal is lost the clock drifts.  When the 
            signal is restored if the digitizer detects a large time 
            jump it may reset (time jerk) to tag the next packet of data 
            with the updated time based on the standard.  If that time 
            jump is forward it will leave an apparent gap in the data, which 
            we discuss below, but if the jump is backward it will leave an 
            apparent overlap with inconsistent samples.   The 
            repair_overlaps function assumes that is the cause of all overlaps 
            it detects.  
            
    The final common situation this function needs to handle is gaps.  A 
    gap is defined in this algorithm as any section where the endtime of 
    one segment is followed by a start time of the next segment that is 
    more than 1 sample in duration.  Specifically when
       (segment[i+1].t0()-segment[i].endtime()) > 1.5*dt
    The result depends on values of the (optional) windowing arguments 
    starttime and endtime and the boolean "zero_gaps" argument.  
    If windowing is enabled (done by changing 
    default None values of starttime and endtime) any gap will be harmless 
    unless it is present inside the specified time time range.  In all 
    cases what happens to the output depends upon the boolean zero_gaps.  
    When zero_gaps is False any gaps detected within the output range of 
    the result (windowed range if specified but the full time of the input otherwise)
    When set True gap sections will be zeroed in the output data vector.  
    All outputs with gaps that have been zeroed will have the boolean 
    Metadata attribute "has_gaps" set True and undefined otherwise.   
    When the has_data attribute is set true the tap windows will be 
    stored as a list of "TimeWindow" objects with the Metadata key "gaps". 
    
    :param tsvector:  array of TimeSeries data that are to be spliced 
      together to create a single output.   The contents must all have
      the same sample rate and be sorted by starttime.   
    :type tsvector:  expected to be a TimeSeriesVector, which is the name 
      we give in the pybind11 code to a C++ std:vector<TimeSeries> container.  
      Any iterable container of TimeSeries data will be accepted, however, 
      with a loss of efficiency.  e.g. it could be a list of TimeSeries 
      data but if so another copy of the created internally and passed 
      to the ccore function that does the work.  We recommend custom 
      applications use the TimeSeriesVector container directly but 
      many may find it more convenient to bundle data into a TimeSeriesEnsemble
      and use the member attribute of the ensemble as the input.  
      i.e. if ens is a TimeSeriesEnsemble use something like this:
           outdata = merge(ens.member)
    :param starttime: (optional) start time to apply for windowing the 
      output.  Default is None which means the output will be created 
      as the merge of all the inputs.  When set WindowData is applied 
      with this start time.   Note if endtime is defined but starttime 
      is None windowing is enabled with starttime = earliest segment start time.
    :type starttime:  double - assumed to be a UTC time expressed as a unix 
      epoch time. 
    :param endtime: (optional) end time to apply for windowing the 
      output.  Default is None which means the output will be created 
      as the merge of all the inputs.  When set WindowData is applied 
      with this end time.   Note if starttime is defined but endtime 
      is None windowing is enabled with endtime = latest end time of 
      all segments. 
    :type endtime:  double - assumed to be a UTC time expressed as a unix 
      epoch time. 
    :param fix_overlaps:  when set True (default is False) if an overlap 
      is detected the algorithm will attempt to repair the overlap 
      to yield a continuous time series vectgor if it determined to have 
      matching data.  See description above for more details. 
    :type fix_overlaps:  boolean
    :param zero_gaps:  When set False, which is the default, any gaps 
      detected in the output window will cause the return to be marked 
      dead.  When set True, gaps will be zeroed and with a record of 
      gap positions posted to the Metadata of the output.  See above 
      for details. 
    :param zero_gaps:  boolean
    :param object_history: boolean to enable or disable saving object
      level history.  Default is False.  Note this functionality is
      implemented via the mspass_func_wrapper decorator.
    :param alg_name:   When history is enabled this is the algorithm name
      assigned to the stamp for applying this algorithm.
      Default ("WindowData") should normally be just used.
      Note this functionality is implemented via the mspass_func_wrapper decorator.
    :param ald_id:  algorithm id to assign to history record (used only if
      object_history is set True.)
      Note this functionality is implemented via the mspass_func_wrapper decorator.
    :param dryrun:
      Note this functionality is implemented via the mspass_func_wrapper decorator.
    :param dryrun:
      Note this functionality is implemented via the mspass_func_wrapper decorator.
      
    :return: TimeSeries in the range defined by the time span of the input 
      vector of segments or if starttime or endtime are specified a reduced 
      time range.  The result may be marked dead for a variety of reasons 
      with error messages explaining why in the return elog attribute. 
    """
    if not isinstance(tsvector,TimeSeriesVector):
        # We assume this will throw an exception if tsvector is not iterable
        # or doesn't contain TimeSeries objects
        dvector = TimeSeriesVector()
        for d in tsvector:
            dvector.append(d)
    else:
        dvector = tsvector
    if fix_overlaps:
        dvector = repair_overlaps(dvector)
    spliced_data = splice_segments(dvector,object_history)
    if spliced_data.dead():
        # the contents of spliced_data could be huge so best to 
        # do this to effectively clear the data vector
        spliced_data.set_npts(0)
        return(TimeSeries(spliced_data))
    window_data = False
    output_window = TimeWindow()
    if starttime is None:
        output_window.start = spliced_data.t0()
    else:
        output_window.start = starttime
        window_data = True
    if endtime is None:
        output_window.end = spliced_data.endtime()
    else:
        output_window.end = endtime
        window_data = True
    if window_data:
        if spliced_data.has_gap(output_window):
            if zero_gaps:
                spliced_data.zero_gaps()
                spliced_data = _post_gap_data(spliced_data)
            else:
                spliced_data.kill()
                spliced_data.elog.log_error("merge",
                            "Data have gaps in output range; will be killed",
                            ErrorSeverity.Invalid)
        return WindowData(spliced_data,
            output_window.start, output_window.end, object_history=object_history)
    else:
        if spliced_data.has_gaps():
            if zero_gaps:
                spliced_data.zero_gaps()
                spliced_data = _post_gap_data(spliced_data)
            else:
                spliced_data.kill()
                spliced_data.elog.log_error("merge",
                            "merged data have gaps; output will be killed",
                            ErrorSeverity.Invalid)
        return TimeSeries(spliced_data)
       
def _post_gap_data(d):
    """
    Private function used by merge immediately above.   Takes an input 
    d that is assumed (no testing is done here) to be a TimeSeriesWGaps 
    object.  It pulls gap data from d and posts the gap data to d. 
    It then returns d.  It silently does nothing if d has not gaps defined.
    """
    if d.has_gap():
        d["has_gaps"] = True
        twlist = d.get_gaps()
        # to allow the result to more cleanly stored to MongoDB we 
        # convert the window data to list of python dictionaries which 
        # mongo will use to create subdocuments 
        gaps = []
        for tw in twlist:
            g = {"starttime" : tw.start, "endtime" : tw.end}
            gaps.append(g)
        d["gaps"] = gaps
    return d
            
class TopMute:
    """
    A top mute is a form of taper applied to the "front" of a signal.
    Front in standard jargon means that with the time axis running from
    left to right (normal unless your native language is Arabic) the time
    period on the left side of a plot.   Data tagged at times less than what we
    call the zero time here are always zeroed.  The mute defines a ramp function
    running from the zero time to the "one" time.  In the ramp zone the
    ramp function multiplies the sample data so it is a form of "taper" as
    used in Fourier analysis.  This class should normally only be used on data with the time
    reference type set Relative.  It can be applied to UTC time standard data but
    with such data one of these objects would often need to be created for
    each atomic data object, which would be horribly inefficient.  In most
    cases conversion to relative time is an essential step before using
    this algorithm.


    This implementation uses an "apply" method for processing data.
    That means for a parallel construct instead of the symbol for a function
    you use the apply method as a function call.  (e.g. if tm is an is an
    instance of this class and "d" is a TimeSeries or Seismogram object to be
    muted the function call that applies the mute would be ts.apply(d))
    """

    def __init__(self, t0=0.0, t1=0.5, type="cosine"):
        """
        Creates a TopMute object for application to MsPASS data objects.

        The constructor is a pythonic front end to the C++ version of this
        class (it the same name in C++ but the binding code maps the C++
        name to _TopMute).  The args are the same but this wrapper allows
        keywords and removes positional requirements as usual in python.

        :param t0:  time of end of zeroing period of top Mute
        :param t1:  time of end of taper zone when the multiplier goes to 1
        :param type: form of ramp (currently must be either "linear" or "cosine")
        """
        # This call will throw a MsPASSError exception if the parameters
        # are mangled but we let that happen in this context assuming a
        # constructor like this is created outside any procesisng loop
        self.processor = _TopMute(t0, t1, type)
        self.t0 = t0

    def apply(self, d, object_history=False, instance=None):
        """
        Use thie method to apply the defined top mute to one of the MsPASS
        atomic data objects. The method does a sanity check on the input
        data range.  If the starttime of the data is greater than t0 for
        the mute the datum is killed and an error posted to elog.  The
        reason is in that situation the data would be completely zeroed
        anyway and it is better to define it dead and leave an error message
        than to completely null data.
        :param d:  input atomic MsPASS data object (TimeSeries or Seismogram)
        :object_history:  It set true the function will add define this
          step as an map operation to preserve object level history.
          (default is False)
        :param instance:   string defining the "instance" of this algorithm.
          This parameter is needed only if object_history is set True.  It
          is used to define which instance of this algrithm is being applied.
          (In the C++ api this is what is called the algorithm id).  I can
          come from the global history manager or be set manually.
        """
        if not isinstance(d, TimeSeries) and not isinstance(d, Seismogram):
            raise MsPASSError(
                "TopMute.apply:  usage error.  Input data must be a TimeSeries or Seismogram object",
                ErrorSeverity.Invalid,
            )
        if d.dead():
            return d
        if d.t0 > self.t0:
            d.elog.log_error(
                "TopMute.apply",
                "Data start time is later than time of mute zero zone\n"
                + "Datum killed as this would produce a null signal",
                ErrorSeverity.Invalid,
            )
            d.kill()
        else:
            self.processor.apply(d)
            if object_history:
                if instance == None:
                    d.elog(
                        "TopMute.apply",
                        "Undefined instance argument - cannot save history data",
                        ErrorSeverity.Complaint,
                    )
                elif d.is_empty():
                    d.elog(
                        "TopMute.apply",
                        "Error log is empty.  Cannot be extended without a top level entry",
                        ErrorSeverity.Complaint,
                    )
                else:
                    if isinstance(d, Seismogram):
                        d.new_map(
                            "TopMute",
                            instance,
                            AtomicType.SEISMOGRAM,
                            ProcessingStatus.VOLATILE,
                        )
                    else:
                        d.new_map(
                            "TopMute",
                            instance,
                            AtomicType.TIMESERIES,
                            ProcessingStatus.VOLATILE,
                        )
