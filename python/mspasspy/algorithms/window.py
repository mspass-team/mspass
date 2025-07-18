from mspasspy.ccore.utility import (
    Metadata,
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
    TimeSeriesVector,
)
from mspasspy.ccore.algorithms.basic import (
    TimeWindow,
    _TopMute,
    _WindowData,
    _WindowData3C,
    repair_overlaps,
    splice_segments,
)
from mspasspy.ccore.algorithms.amplitudes import (
    _scale,
    _scale_ensemble,
    _scale_ensemble_members,
    ScalingMethod,
)
from mspasspy.util.decorators import mspass_func_wrapper, mspass_method_wrapper


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
    *args,
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
    handles_ensembles=True,
    checks_arg0_type=True,
    handles_dead_data=True,
    **kwargs,
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
    else:
        message = "scale:  received invalid data type={} for arg0".format(str(type(d)))
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


# not decorated for reasons given in docstring below
def WindowDataAtomic(
    d,
    win_start,
    win_end,
    t0shift=None,
    short_segment_handling="kill",
    log_recoverable_errors=True,
):
    """
    Cut atomic data to a shorter time segment defined by a time range.

    Cutting a smaller waveform segment from a larger waveform segment
    is a very common seismic data processing task.  Handling that low
    level operation needs to be done efficiently but with a reasonable
    number of options.   This function is very fast if the inputs
    match the expected model where the requested time segment is
    inside the range of the datum passed via arg0.   In that mode the
    function is a thin wrapper on a C++ function that does most of
    the work.   When the window is inconsistent with the data,
    which is defined here as a "short segment", more overhead
    is involved if you want to recover something.   To be specific
    a "short segment" means the requested time span for a the
    window operation form win_start to win_end is not completely
    inside (inclusive of the endpoints) the range of the data.

    Handling of "short segments" has these elements:

    1.  If the window range is completely outside the range of
        the data the result is always killed and returned
        as a dead datum with no sample data. (d.npts=0)
    2.  Behavior if there is the window range overlaps but
        has boundaries outside the data range depends on the setting
        of the argument `short_segment_handling` and the
        boolean argument `log_recoverable_errors`.
        If `log_recoverable_errors` is set True (default) and
        the result is returned live (i.e. the error was recoverable
        but the data are flawed) a complaint message describing
        what was done will be posted to the elog container
        of the return.   If False recoveries will be done
        silently.   That can be useful in some algorithms
        (e.g. cross-correlation) where partial segments can be
        handled.  What defines possible recovery is set by
        the `short_segment_handling` string.  It must be
        one of only three possible values or the function
        will abort with a ValueError exception:

        -  "kill" (default) does not recovery attempt and will kill any data with time inconsistencies.
        -  "truncate" - this truncates to the output to the time range max(d.t0,win.starttime) to min(d.endtime(),win.endtime).
        -  "pad" - will cause the function to have data in the span define dby win_start to win_end but the sections where the data are undefined will be set to zeros.

    Users should also be aware that this function preserves subsample timing
    in modern earthquake data.   All seismic reflection processing
    systems treat timing as synchronous on all channels.   That assumption
    is not true for data acquired by independent digitizers with
    external timing systems (today always GPS timing).   In MsPASS
    that is handled through the `t0` attribute of atomic data objects and
    the (internal) time shift from GMT used when the time standard is
    shifted to "relative".   A detail of this function is it preserves
    t0 subsample timing so if you carefully examine the t0 value
    on the return of this function it will match the `twin_start`
    value only to the nearest sample.   That way time computed with the
    time method (endtime is a special case for sample npts-1)
    will have subsample accuracy.

    Note:  This function should not normally be used.  WindowData
    calls it directy for atomic inputs and adds only a tiny overhead.
    It can still be used as long as you don't need object-level history.

    :param d: is the input data.  d must be either
      a :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
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
      It can be specified one of two ways:  (1) as a number where it
      is assumed to be a time shift to apply in seconds, or (2) as a
      a string.  In the later case the string is assumed to be a valid
      key for fetching a time from a datum's Metadata container.
      Note the name t0shift is a bit inconsistent with this usage but
      was retained as the original api did not have the string
      option.
    :type t0shift:  real number (float) or a string that defines a
      Metadata container key.
    :param short_segment_handling: Defines method for handling data where
      the requested interval is shorter than the requested window but does
      have some overlap. `segment_handling_methods` must be one of the following:

      .. code-block:: text

      `kill` - in this mode any issues cause the return to be marked dead
               (this is the default)
      'pad' -  in this mode return will have short segments will be padded
               with zeros to define data of the required length.
      'truncate' - in this mode short segments will be truncated on left
                  and/or right to match actual data range.

      Note that if the input time range does not overlap the requested
      interval "kill" is always the behavior as by definition the result
      is null.
    :type short_segment_handle:  string (from list above)  Default is "kill".
      Throws a ValueError exception of not one of the accepted values.
    :param log_recoverable_errors:  When True (default) any recoverable
      windowing error (meaning at least a partial overlap in time range)
      will cause a log message to be posted to the elog container of the
      output.  When False recovery will be done silently.  Note when
      `short_segment_handling` is set to "kill" logging is not optional and
      kill will always create an error log entry.

    :return: copy of d with sample range reduced to twin range.  Returns
      an empty version of the parent data type (default constructor) if
      the input is marked dead
    """
    alg = "WindowDataAtomic"
    segment_handling_methods = ["kill", "pad", "truncate"]
    if short_segment_handling not in segment_handling_methods:
        message = (
            "WindowData:   illegal option given for segment_handling_method={}".format(
                str(segment_handling_methods)
            )
        )
        raise ValueError(message)
    if not isinstance(d, (TimeSeries, Seismogram)):
        message = "WindowData:   illegal type for arg0={}\nMust be either TimeSeries or Seismogram".format(
            str(type(d))
        )
        raise TypeError(message)
    if d.dead():
        return d

    def window_message(this_d, tw):
        """
        File scope function standardizes error message for problems.
        """
        message = (
            "Window range: {wst} < t < {wet}  Data range:  {dst} < t < {det}\n".format(
                wst=tw.start, wet=tw.end, dst=this_d.t0, det=this_d.endtime()
            )
        )
        return message

    # This block defines the TimeWindow object twcut to implement the options
    # for handling the possible issues with inconsistencies of the time range requested and data
    # time span.   First define it as the full range and dither the time window if appropriate
    twcut0 = TimeWindow(win_start, win_end)
    twcut = TimeWindow(twcut0)  # the start and end time of this copy may be changed
    if t0shift:
        if isinstance(t0shift, str):
            if d.is_defined(t0shift):
                t0shift = d[t0shift]
            else:
                message = "t0shift argument passed as string value={}\n".format(t0shift)
                message += "Implies use as key to fetch Metadata from datum, but the requested key is not defined\n"
                message += "Setting shift to 0.0 - this is likely to cause later handling to kill this datum"
                d.elog.log_error(alg, message, ErrorSeverity.Complaint)
                t0shift = 0.0
        twcut = twcut.shift(t0shift)
        twcut0 = twcut.shift(t0shift)
    if twcut.end < d.t0 or twcut.start > d.endtime():
        # always kill and return a zero length datum if there is no overlap
        message = "Data time range is outside the time range window time range\n"
        message += window_message(d, twcut)
        d.elog.log_error(alg, message, ErrorSeverity.Invalid)
        d.set_npts(0)
        d.kill()
        return d
    message = str()
    # If either of these get set true we set padding_required True
    cut_on_left = False
    cut_on_right = False
    padding_required = False
    if short_segment_handling != "kill":
        # earthquake data start times are not on a synchronous time mesh so we
        # have to use a rounding algorithm in this block to set windows
        # relative to the sample grid for each datum.
        if twcut.start < (d.t0 - d.dt / 2.0):
            if log_recoverable_errors:
                message += "Window start time is less than data start time\n"
                message += window_message(d, twcut)
                message += "Setting window start time to data start time={}\n".format(
                    d.t0
                )
                d.elog.log_error(alg, message, ErrorSeverity.Complaint)
            twcut.start = d.t0
            cut_on_left = True
        if twcut.end > (d.endtime() + d.dt / 2.0):
            if log_recoverable_errors:
                message += "Window end time is after data end time\n"
                message += window_message(d, twcut)
                message += "Setting window end time to data end time={}\n".format(
                    d.endtime()
                )
                d.elog.log_error(alg, message, ErrorSeverity.Complaint)
            twcut.end = d.endtime()
            cut_on_right = True
        if len(message) > 0:
            if short_segment_handling == "truncate":
                message += "This data segment will be shorter than requested"
            elif short_segment_handling == "pad":
                message += "Datum returned will be zero padded in undefined time range"
        if short_segment_handling == "pad":
            if cut_on_right or cut_on_left:
                padding_required = True
    try:
        if isinstance(d, TimeSeries):
            dcut = _WindowData(d, twcut)
        else:
            # not with current logic this alway means we are handling a Seismogram here
            dcut = _WindowData3C(d, twcut)
        if padding_required:
            if isinstance(d, TimeSeries):
                dpadded = TimeSeries(dcut)
            else:
                dpadded = Seismogram(dcut)
            # preserve subsample timing of t0 from parent
            istart = dcut.sample_number(twcut0.start)
            # this has to be computed from window duration NOT the computed
            # start time using the t0 + i*dt formula of the time method of
            # BasicTimeSeries.   The reason is a subtle rounding issue with
            # subsample timing that can cause a factor of 1 ambiguity from rounding.
            # C++ code uses this same formula so we also need to be consistent
            dpadded.t0 = dcut.time(istart)
            npts = round((twcut0.end - dpadded.t0) / d.dt) + 1
            dpadded.set_npts(npts)  # assume this initializes arrays to zeros
            # a bit more obscure with the : notation here but much faster
            # than using python loops
            istart = dpadded.sample_number(dcut.t0)
            iend = dpadded.sample_number(dcut.endtime()) + 1
            # subsample rounding can cause iend to be one sample to large.
            # this would be easier to handle with a python loop but it
            # would be much slower
            if iend > dpadded.npts:
                di = iend - dpadded.npts
                if di == 1:
                    iend = dpadded.npts
                    icend = dcut.npts - 1
                    if isinstance(d, TimeSeries):
                        dpadded.data[istart:iend] = dcut.data[0:icend]
                    else:
                        dpadded.data[:, istart:iend] = dcut.data[:, 0:icend]
                else:
                    message = "Unexpected return from C++ WindowData function in pad option section\n"
                    message += "Computed sample number for padded sample number start={} and end={}\n".format(
                        istart, iend
                    )
                    message += "Allowed index range = 0 to {}\n".format(dpadded.npts)
                    message += (
                        "This should not happen and is a bug that should be reported"
                    )
                    raise MsPASSError(alg, message, ErrorSeverity.Fatal)
            else:
                if isinstance(d, TimeSeries):
                    dpadded.data[istart:iend] = dcut.data
                else:
                    dpadded.data[:, istart:iend] = dcut.data
            return dpadded
        else:
            return dcut

    except MsPASSError as err:
        # This handler is needed in case the C++ functions WindowData or WindowData3C
        # throw an exception.  With the current logic that should not happen but
        # this makes the code base more robust in the event changes occur
        d.log_error(alg, str(err), ErrorSeverity.Invalid)
        d.kill()
        return d


@mspass_func_wrapper
def WindowData(
    mspass_object,
    win_start,
    win_end,
    t0shift=None,
    short_segment_handling="kill",
    log_recoverable_errors=True,
    overwrite_members=False,
    retain_dead_members=True,
    *args,
    object_history=False,
    alg_name="WindowData",
    alg_id=None,
    dryrun=False,
    handles_ensembles=True,
    checks_arg0_type=True,
    handles_dead_data=True,
    **kwargs,
):
    """
    Apply a window operation to cut out data within a specified time range
    to any MsPASS seismic data object.

    Cutting a smaller waveform segment from a larger waveform segment
    is a very common seismic data processing task.  Handling that low
    level operation needs to be done efficiently but with a reasonable
    number of options.   This function is very fast if the inputs
    match the expected model where the requested time segment is
    inside the range of the datum passed via arg0.   In that mode the
    function is a thin wrapper on a C++ function that does most of
    the work.   When the window is inconsistent with the data,
    which is defined here as a "short segment", more overhead
    is involved if you want to recover something.   To be specific
    a "short segment" means the requested time span for a the
    window operation form win_start to win_end is not completely
    inside (inclusive of the endpoints) the range of the data.

    Handling of "short segments" has these elements:

    1.  If the window range is completely outside the range of
        the data the result is always killed and returned
        as a dead datum with no sample data. (d.npts=0)
    2.  Behavior if there is the window range overlaps but
        has boundaries outside the data range depends on the setting
        of the argument `short_segment_handling` and the
        boolean argument `log_recoverable_errors`.
        If `log_recoverable_errors` is set True (default) and
        the result is returned live (i.e. the error was recoverable
        but the data are flawed) a complaint message describing
        what was done will be posted to the elog container
        of the return.   If False recoveries will be done
        silently.   That can be useful in some algorithms
        (e.g. cross-correlation) where partial segments can be
        handled.  What defines possible recovery is set by
        the `short_segment_handling` string.  It must be
        one of only three possible values or the function
        will abort with a ValueError exception:

        .. code-block:: text

        "kill" - (default) does not recovery attempt and will
                 kill any data with time inconsistencies.
        "truncate" - this truncates to the output to the
                time range max(d.t0,win.starttime) to
                min(d.endtime(),win.endtime).
        "pad" - will cause the function to have data in
                the span define dby win_start to win_end but
                the sections where the data are undefined will
                be set to zeros.

    This function handles input that is any valid MsPASS data
    object.  For atomic data it is a very thin wrapper for the
    related function `WindowDataAtomic`.   For atomic data this
    is nothing more than a convenience function that allows you
    to omit the "Atomic" qualifier.  For ensemble data
    this function is more-or-less a loop over all ensemble
    members running `WindowDataAtomic` on each member datum.
    In all cases the return is the same type as the input but
    either shortened to the specified range or killed.
    A special case is ensembles where only some members
    may be killed.

    A special option for ensembles only can be triggered by
    setting the optional argument `overwrite_members` to True.
    Default behavior returns an independent ensemble created
    from the input cut to the requested window interval.
    When `overwrite_members` is set True the windowing of the
    members will be done in place ovewriting the original
    ensemble contents.  i.e. in tha output is a reference to
    the same object as the input.  The primary use of the
    `overwrite_members == False` option is for use in map
    operators on large ensembles as it can significantly
    reduce the memory footprint.

    Note the description of subsample time handling in the
    related docstring for `WindowDataAtomic`.   For ensembles
    each member output preserves subsample timing.

    Finally, how the function handles data marked dead is \
    important.  For atomic data the is no complexity.
    dead is dead and the function just immediately returns
    a reference to the input.  For ensembles some members
    can be dead or the entire ensemble can be marked dead.
    If the ensemble is marked dead the function immediately
    returns a reference to the input.  If any members are
    dead the result will depend on the boolean argument
    "retain_data_members".  When True (the default) dead
    members will be copied verbatim to the output.
    If False the dead members will be SILENTLY deleted.
    The False option is only recommended if the windowing is
    internal to a function and the windowed output will be
    discarded during processing.  Otherwise the error log of why
    data were killed will be lost.   If you need to save
    memory by clearing dead bodies use the `Undertaker`
    class to bury the dead and retain the error log data.

    :param d: is the input data.  d must be either a
      :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
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
      It can be specified one of two ways:  (1) as a number where it
      is assumed to be a time shift to apply in seconds, or (2) as a
      a string.  In the later case the string is assumed to be a valid
      key for fetching a time from a datum's Metadata container.
      Note the name t0shift is a bit inconsistent with this usage but
      was retained as the original api did not have the string
      option.
    :type t0shift:  real number (float) or a string that defines a
      Metadata container key.
    :param short_segment_handling: Defines method for handling data where
      the requested interval is shorter than the requested window but does
      have some overlap. `segment_handling_methods` must be one of the following:

      .. code-block:: text

        `kill` - in this mode any issues cause the return to be marked dead
                 (this is the default)
        'pad' -  in this mode return will have short segments will be padded
                 with zeros to define data of the required length.
        'truncate' - in this mode short segments will be truncated on left
                    and/or right to match actual data range.

      Note that if the input time range does not overlap the requested
      interval "kiLl" is always the behavior as by definition the result
      is null.
    :type short_segment_handle:  string (from list above)  Default is "kill".
      Throws a ValueError exception of not one of the accepted values.
    :param log_recoverable_errors:  When True (default) any recoverable
      windowing error (meaning at least a partial overlap in time range)
      will cause a log message to be posted to the elog container of the
      output.  When False recovery will be done silently.  Note when
      `short_segment_handling` is set to "kill" logging is not optional and
      kill will always create an error log entry.
    :param overwrite_members:   controls handling of the member vector of
      ensembles as described above.  When True the member atomic data will
      be overwritten by the windowed version and the ensmble returned will
      be a reference to the same container as the input.  When False
      (the default) a new container is created and returned.  Note in that
      mode dead data are copied to the same slot as the input unaltered.
      This argument will be silently ignored if the input is an atomic
      MsPASS seismic object.
    :type overwrite_members:  boolean
    :param retain_dead_members:   Controls how dead data are handled with
      ensembles.  When True (default) dead ensemble members are copied verbatim
      to the output.  When False they are silently deleted. (see above for
      a more complete description).  This argument is ignored for Atomic data.
    :type retain_dead_members: boolean
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
    if isinstance(mspass_object, (TimeSeries, Seismogram)):
        if mspass_object.dead():
            return mspass_object
        else:
            return WindowDataAtomic(
                mspass_object,
                win_start,
                win_end,
                t0shift=t0shift,
                short_segment_handling=short_segment_handling,
                log_recoverable_errors=log_recoverable_errors,
            )
    elif isinstance(mspass_object, (TimeSeriesEnsemble, SeismogramEnsemble)):
        if mspass_object.dead():
            return mspass_object
        else:
            nlive = 0
            if overwrite_members:
                for i in range(len(mspass_object.member)):
                    if mspass_object.member[i].live:
                        mspass_object.member[i] = WindowDataAtomic(
                            mspass_object.member[i],
                            win_start,
                            win_end,
                            t0shift=t0shift,
                            short_segment_handling=short_segment_handling,
                            log_recoverable_errors=log_recoverable_errors,
                        )
                        if mspass_object.member[i].live:
                            nlive += 1
                        # when returning the original reference the
                        # retain_dead_members option  is always True

                # In this case this just creates a duplicate reference
                ensout = mspass_object
            else:
                Nmembers = len(mspass_object.member)
                if isinstance(mspass_object, TimeSeriesEnsemble):
                    ensout = TimeSeriesEnsemble(Metadata(mspass_object), Nmembers)
                else:
                    ensout = SeismogramEnsemble(Metadata(mspass_object), Nmembers)
                for i in range(len(mspass_object.member)):
                    if mspass_object.member[i].live:
                        d = WindowDataAtomic(
                            mspass_object.member[i],
                            win_start,
                            win_end,
                            t0shift=t0shift,
                            short_segment_handling=short_segment_handling,
                            log_recoverable_errors=log_recoverable_errors,
                        )
                        ensout.member.append(d)
                        if d.live:
                            nlive += 1
                    elif retain_dead_members:
                        ensout.member.append(mspass_object.member[i])
                # always set live and let the next line kill it if nlive is 0
                ensout.live = True
            if nlive == 0:
                message = "All members of this ensemble were killed by WindowDataAtomic;  ensemble returned will be marked dead"
                ensout.elog.log_error("WindowData", message, ErrorSeverity.Invalid)
                ensout.kill()
            return ensout
    else:
        message = (
            "Illegal type for arg0={}.  Must be a MsPASS seismic data object".format(
                str(type(mspass_object))
            )
        )
        raise TypeError(message)


@mspass_func_wrapper
def WindowData_autopad(
    d,
    stime,
    etime,
    pad_fraction_cutoff=0.05,
    *args,
    object_history=False,
    alg_name="WindowData_autopad",
    alg_id=None,
    dryrun=False,
    handles_ensembles=False,
    checks_arg0_type=True,
    handles_dead_data=False,
    **kwargs,
):
    """
    Windows an atomic data object with automatic padding if the
    undefined section is not too large.

    When using numpy or MsPASS data arrays the : notation can be used
    to drastically speed performance over using a python loop.
    This function is most useful for contexts where the size of the
    output of a window must exactly match what is expected from
    the time range.   A type example is a multichannel algorithm
    where you need to use an API that loads a set of signals into
    a matrix that is used as the workspace for processing.  That is
    the univeral model, for example, in seismic reflection processing.
    This algorithm will silently zero pad any undefined samples
    at the end of the window IF the fraction of undefined data relative to
    the number of samples expected from the time range defined by
    etime-stime is less than the "pad_fraction_cutoff".  If
    the input time span shorter than the computed mismatch limit
    the result will be returned marked dead with an elog entry
    highlighting the problem.

    :param d:  atomic MsPASS seismic data object to be windowed.
    :type d:  works with either `TimeSeries` or `Seismogram`
      objects.  Will raise a TypeError exception if d is not one of
      the two atomic data types.
    :param stime:  start time of window range
    :type stime:  float
    :param etime:  end time of window range
    :type etime:  float
    :param pad_fraction_cutoff:  limit for the
      fraction of data with undefined values before the datum is
      killed.   (see above)  If set to 0.0 this is an expensive way
      to behave the same as WindowData
    :return:  object of the same type as d.   Will be marked dead
      if the fraction of undefined data exceeds pad_fraction_cutoff.
      Otherwise will return a data vector of constant size that may
      be padded.
    """
    if not isinstance(d, (TimeSeries, Seismogram)):
        message = "WindowData_autopad:  arg0 must be either a TimeSeries or Seismogram object.  Actual type={}".format(
            type(d)
        )
        raise TypeError(message)
    N_expected = round((etime - stime) / d.dt) + 1
    dw = WindowData(d, stime, etime)
    if dw.dead():
        # assumes default kills if stime and etime are not within data bounds
        # in that situation the first call to WindowData will kill and we don't get here
        dw = WindowData(d, stime, etime, short_segment_handling="truncate")
        pad_fraction = abs(N_expected - dw.npts) / N_expected
        if pad_fraction < pad_fraction_cutoff:
            dw = WindowData(d, stime, etime, short_segment_handling="pad")
        else:
            message = "time span of data is too short for cutoff fraction={}\n".format(
                pad_fraction_cutoff
            )
            message += "padded_time_range/(etime-stime)={} is below cutoff".format(
                pad_fraction
            )
            dw.elog.log_error("WindowData_autopad", message, ErrorSeverity.Invalid)
            dw.kill()
    return dw


# TODO:   this function does not support history mechanism because the
# standard decorator is does not support a bound std::vector<TimeSeries>
# container.  I requires one of the four MsPASS data objects.
def merge(
    tsvector,
    starttime=None,
    endtime=None,
    fix_overlaps=False,
    zero_gaps=False,
    object_history=False,
) -> TimeSeries:
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

    .. code-block:: text

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
    Metadata attribute "has_gap" set True and undefined otherwise.
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

      .. code-block:: text

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
    :param zero_gaps:  boolean controlling how gaps are to be handled.
      See above for details of the algorithm.
    :type zero_gaps:  boolean (default False)
    :param object_history: boolean to enable or disable saving object
      level history.  Default is False.  Note this functionality is
      implemented via the mspass_func_wrapper decorator.
    :return: TimeSeries in the range defined by the time span of the input
      vector of segments or if starttime or endtime are specified a reduced
      time range.  The result may be marked dead for a variety of reasons
      with error messages explaining why in the return elog attribute.
    """
    if not isinstance(tsvector, TimeSeriesVector):
        # We assume this will throw an exception if tsvector is not iterable
        # or doesn't contain TimeSeries objects
        dvector = TimeSeriesVector()
        for d in tsvector:
            dvector.append(d)
    else:
        dvector = tsvector
    if fix_overlaps:
        dvector = repair_overlaps(dvector)
    spliced_data = splice_segments(dvector, object_history)
    if spliced_data.dead():
        # the contents of spliced_data could be huge so best to
        # do this to effectively clear the data vector
        spliced_data.set_npts(0)
        return TimeSeries(spliced_data)
    window_data = False
    output_window = TimeWindow()
    if starttime is None:
        output_window.start = spliced_data.t0
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
                spliced_data.elog.log_error(
                    "merge",
                    "Data have gaps in output range; will be killed",
                    ErrorSeverity.Invalid,
                )
        return WindowData(
            spliced_data,
            output_window.start,
            output_window.end,
            object_history=object_history,
        )
    else:
        if spliced_data.has_gap():
            if zero_gaps:
                spliced_data.zero_gaps()
                spliced_data = _post_gap_data(spliced_data)
            else:
                spliced_data.kill()
                spliced_data.elog.log_error(
                    "merge",
                    "merged data have gaps; output will be killed",
                    ErrorSeverity.Invalid,
                )
        return TimeSeries(spliced_data)


def _post_gap_data(d):
    """
    Private function used by merge immediately above.   Takes an input
    d that is assumed (no testing is done here) to be a TimeSeriesWGaps
    object.  It pulls gap data from d and posts the gap data to d.
    It then returns d.  It silently does nothing if d has not gaps defined.
    """
    if d.has_gap():
        d["has_gap"] = True
        twlist = d.get_gaps()
        # to allow the result to more cleanly stored to MongoDB we
        # convert the window data to list of python dictionaries which
        # mongo will use to create subdocuments
        gaps = []
        for tw in twlist:
            g = {"starttime": tw.start, "endtime": tw.end}
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

    @mspass_method_wrapper
    def apply(
        self,
        d,
        *args,
        object_history=False,
        instance=None,
        checks_arg0_type=True,
        handles_ensembles=False,
        handles_dead_data=True,
        **kwargs,
    ):
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
        """
        if not isinstance(d, TimeSeries) and not isinstance(d, Seismogram):
            message = "TopMute.apply:  usage error.  Input data must be a TimeSeries or Seismogram object"
            message += "Actual type of arg={}".format(type(d))
            raise TypeError(message)
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
        return d
