#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This file contains a set of wrappers for the revised version of the
colored noise greulagrization (CNR) algorithms.  The prototype was
called CNR3CDecon.   It will be deprecated.  This one uses a set of
revised C++ classes for the engine that allow the functions used here
to satify the dask definition of "pure".


Created on Tue May 28 19:22:10 2024

@author: pavlis
"""
import numpy as np

from mspasspy.util.decorators import mspass_func_wrapper
from mspasspy.ccore.seismic import TimeSeries, Seismogram, SeismogramEnsemble
from mspasspy.ccore.algorithms.basic import _WindowData3C, TimeWindow
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError
from mspasspy.ccore.algorithms.deconvolution import CNRDeconEngine
from mspasspy.algorithms.window import WindowData
from mspasspy.algorithms.basic import ExtractComponent


def fetch_bandwidth_data(md, keys) -> tuple:
    """
    Small convenience function extracts high and low frequency
    bandwidth data from dictionary like container md.   Assumes keys is
    assumed a pair of strings passed as the content of the bandwidth_keys
    argument of CNRRFDecon or CNRArrayDecon.   It is one of those
    small functions used to avoid repetitions code.
    :param md:  dictionary like container from which values are to be fetched.
    :type md:  dictionary like container - needs string subscripting and
    in test ("x" in md).
    :param keys:  pair of strings that will be used to extract values from md.
    :type keys:  list/array/tuple of two strings
    :return tuple:  returns a tuple with two values.  With success two floating
    point numbers that are the values returned by each of the keys passed via
    the keys argument.   The decon operator assume that 0 is the low frequency
    edge of the working band and 1 is the high frequency band edge.
    Returns a -1.0 value in any field that was not defined.   Note that makes
    sense since although a negative frequency is meaningful it is not in  this
    context.
    """
    k = keys[0]
    if k in md:
        flow = md[k]
    else:
        flow = -1.0
    k = keys[1]
    if k in md:
        fhigh = md[k]
    else:
        fhigh = -1.0
    return tuple([flow, fhigh])


def validate_bandwidth_data(d, bd, bdkeys):
    """
    Companion to fetch_bandwidth_data above to regularize handing of
    errors from fetching bandwidth data.  A negative frequency is used as a
    signal b fetch_bnadwidth_data of a metadata error in fetching a particular
    bandwidth value.   This function posts a standard message to d, kills it,
    and returns the body with the appended error message.  Another helper
    done to reduce duplicate code.

    :param d:  seismic data object being processed
    :type d:  normally a `Seismogram` object but there is no type checking.
    Uses kill and error log concepts.
    :param bd:  pair of floats returned by fetch_bandwidth_data.  -1.0 is
    used by that function to flag undefined.
    :type bd:  tuple/array/list with 2 floats
    :param bdkeys:   parallel list/array/tuple of keys that were used by
    fetch_bandwidth_data to fetch bandwidth data.  Required to post an
    error message when necessary that will be helpful.
    :return:  copy of the data if there are not problems.  If the bandwidth
    data is not defined or is invalide (flow>figh) the return will be marked
    dead and contain error messages describing the problem.
    """
    if d.live:
        alg = "validate_bandwidth_data"
        if bd[0] < 0.0 or bd[1] < 0.0:
            message = "Error parsing require bandwidth data\n"
            if bd[0] < 0.0:
                message += "Could not fetch data for low frequency corner using key {}\n".format(
                    bdkeys[0]
                )
            if bd[1] < 0.0:
                message += "Could not fetch data for high frequency corner using key {}\n".format(
                    bdkeys[1]
                )
            d.elog.log_error(alg, message, ErrorSeverity.Invalid)
            d.kill()
        elif bd[1] <= bd[0]:
            message = (
                "Invalid bandwidth data retrieved:  flow={} and fhigh={}\n".format(
                    bd[0], bd[1]
                )
            )
            message += "Require flow<fhigh"
            d.elog.log_error(alg, message, ErrorSeverity.Invalid)
            d.kill()
    return d


def fetch_and_validate_bandwidth_data(
    d,
    bdkeys,
    bandwidth_subdocument_key=None,
) -> tuple:
    """
    Top-level function to validate bandwidth data passed via a Metadata
    container in a seismic data object.   This uses both the fetch_bandwidth_data
    and the validate_bandwidth_data functions to standardize getting a valid
    range of frequencies to use for the inverse operator estimated by
    the CNRDeconEngine operator.  It will never raise an exception but
    can kill the datum it receives for testing if the keys are invalid
    or the values are not reasonble (i.e. high edge of the band is less than
    the low frequency edge).  This function is used in both the single
    station and array (ensemble) versions of the CNR Deconvolution
    method.   It like the things it uses avoids a bunch of otherwise
    redundant code.

    :param d:  input datum
    :type d:  atomic  mnpass seismic data object
    :param bdkeys:   required tuple/array of keys that define the
    keys that should be used top fetch the low (component 0) and
    high (component 1) band edges for the operator.
    :type:  subscriptable container with two strings - python array or tuple.
    :param bandwidth_subdocument_key:   When not None (the default) the
    function will first attempt to extract a subdocument (dictionary) from
    the input data using this key.   If successful it will attempt to
    extract data from that dictionary using the keys defined in bdkeys.
    If the key is not found the datum will be marked data, and returned
    with a descriptive error message.
    :type bandwith_subdocument_key:  None or string
    :return: type with 3 components
      0 = copy of data which may be marked dead if the process failed
      1 - flow
      2 - fhigh
    """
    alg = "fetch_and_validate_bandwidth_data"
    if bandwidth_subdocument_key:
        if d.is_defined(bandwidth_subdocument_key):
            bd = d[bandwidth_subdocument_key]
            bdvals = fetch_bandwidth_data(bd, bdkeys)
            # this function may kill
            d = validate_bandwidth_data(d, bdvals, bdkeys)
        else:
            message = "Required bandwidth_subdocument_key={} ".format(
                bandwidth_subdocument_key
            )
            message += "is not defined for this datum"
            d.elog.log_error(alg, message, ErrorSeverity.Invalid)
            d.kill()
    else:
        bdvals = fetch_bandwidth_data(d, bdkeys)
        d = validate_bandwidth_data(d, bdvals, bdkeys)
    return [d, bdvals[0], bdvals[1]]


def prediction_error(engine, wavelet) -> float:
    """
    Computes prediction error of deconvolution operator defined as
    norm(ao-io)/norm(io) where ao is the return from the
    actual_output method of engine and io is the return from
    the ideal_output of engine.  The computed norm is L2
    :param engine:   assumed valid instance of a CNRDeconEngine
       class
    :param wavelet:   wavelet TimeSeries object used in deconvolution.
       For this operator it is not necessarily constant this
       required for actual_output method.
    """

    # with internal use can assume ao and io are the same length
    # and time aligned - caution
    ao = engine.actual_output(wavelet)
    io = engine.ideal_output()
    err = ao - io
    return np.linalg.norm(err.data) / np.linalg.norm(io.data)


@mspass_func_wrapper
def CNRRFDecon(
    seis,
    engine,
    *args,
    component=2,
    noise_spectrum=None,
    signal_window=None,
    noise_window=None,
    use_3C_noise=True,
    bandwidth_subdocument_key=None,
    bandwidth_keys=["low_f_band_edge", "high_f_band_edge"],
    QCdata_key="CNRFDecon_properties",
    return_wavelet=False,
    window_output=True,
    object_history=False,
    alg_name="CNRRFDecon",
    alg_id=None,
    dryrun=False,
    inplace_return=False,
    handles_ensembles=False,
    function_return_key=None,
    checks_arg0_type=True,
    handles_dead_data=True,
    **kwargs,
) -> tuple:
    """
    Uses the CNRRFDeconEngine instance passed as `engine` to
    produce a "receiver function" type of deconvolution from an
    input `Seismogram` object passed through arg0.   I define a
    "receiver function" as the output of a deconvolution
    from a single seismogram input.   That differs from an array
    deconvolution where multiple seismograms are used to compute
    the convolution operator.   The `CNRDeconEngine` C++ class
    used by this function has a sibling called `CNRArrayDecon` that
    uses the same engine but is an array method.

    As a "receiver function" algorithm this function demands some
    restrictions on the input that are checked before it will run.
    1.   The input through arg0 must be a `Seismgram`.
    2.   An instance of a `CNRDeconEngine` is required and must
         be entered via arg1.
    3.   The algorithm requires a segment of the input Seismogram
         data it should treat as signal and a power spectrum
         estimate of the noise to use for regularization.

    For flexibility the way you can satisify item 3 has some
    complexity.   There are two combinations of arguments that
    can be used to satisfy requirement 3
    1.  Define BOTH signal_window and noise_window (both args are None
        by default).   When that constraint is true the input
        Seismogram (seis == arg0) is cut into two segments defined
        by the noise_window and signal_window `TimeWindow` objects.
        The segment defined by "signal_window" will be deconvolved in
        the frequency domain using the component defined by the
        `component` argument as the wavelet estimate.   Since this
        algorithm is designed for P wave data that is expected to
        be some estimate of the longitudinal component (Z of RTZ, L of LQT,
        of P of the free surface transformation).  This algorithm
        then computes a noise spectrum estimate internally from the
        waveform segment extracted with using the noise_window time range.
        If use_3C_noise is set True the average of all three components is
        used for the spectrum estimate.   When False the spectrum is
        computed form the longitudinal component defined with the
        "component" argument.  NOTE:   this approach should be viewed
        as the standard usage of this function.
    2.  An alternative approach is triggered by passing a valid
        `PowerSpectrum` object via the "noise_data" argument.   When
        that is the case the spectrum estimate in the object received is
        used for the inverse operator regularization.   In this
        case the data are assumed already windowed if signal_window is
        None and noise_window is ignored.  When signal_window is defined
        the data for deconvolution is windowed to that time range
        before running the deconvolution operator.

    An alternative way to look at this is the function uses the following
    logic to sort out valid inputs.  It is written in python pseudocode

       if noise_data:
           if signal_window:
               seis = WindowData(seis,signal_window.start, signal_window.end)
           deconvolve seis
       elif signal_window and noise_window:
           seis = WindowData(seis,signal_window.start, signal_window.end)
           noise = WindowData(seis,noise_window.start,noise_window.end)
           noise_spectrum = engine.compute_spectrum(noise)
           deconvolve seis using noise_spectrum
       else:
           raise exception

    The algorithm applied by this function has two novel features:
    1.  The bandwidth of the deconvolution operator is adjusted based on
        estimated bandwith from the spectrum of the signal and noise.
        The bandwwidth properties are extracted from Metadata
        using keys with defaults defined by values stored in a
        subdocument (python dict) accessed by the key defined by
        the "bandwidth_keys" argument.  The defaults are those
        set by the mspass function broadband_snr_QC.
    2.  The inverse is regularized by by a frequency dependent damping
        using the noise spectrum.

    Examples found in tutorials demonstrate how this algorithm
    produce usable output in marginal snr conditions that would cause
    other common RF decon operators to produce junk.


    The main work of this function is done inside a C++ function
    bound to python of type `CNRDeconEngine` passed as the arg1
    (symbol `engine`).   The engine is passed as an argument because
    it has a fairly high instantiation cost.   Note that object
    is always instantiated an AntelopePf file that defines its
    properties.  See the comments in the default file and the
    documentation of the C++ class for guidance on setting the
    engine parameters.



    Function arguments:

    :param seis:   `Seismogram` object containing data to be deconvolved.
      As noted above it should normally be the section of data that is to be
      deconvolved or a larger window that will contain data spanning the time
      range defined by the signal_window and noise_window arguments.
    :type seis:  Must be a `Seismogram` object.
    :param engine:   Instance of the CNRDeconEngine object noted above.
        Most of the behavior of the operator is defined by this engine object.
    :type engine:  `CNRDeconEngine`
    :param component:   component of seis to use as the wavelet estimate.
        As noted above this function is designed for P wave data so ths
        should defined the component number of seis to assume is an
        estimate of the source wavelet.
    :type component: integer (default 2)
    :param wavelet:   Alternative way to specify wavelet to use for deconvolution, f
    :param noise_spectrum:   Alternative way to define data to be used
        to regularize the deconvolution operator.  See above for
        usage restrictions.
    :type noise_spectrum:  `PowerSpectrum` object or None.   If None (default)
        both the `signal_window` and `noise_window` must be defines
        (see above for the full logic).
    :param signal_window:   time span to extract as the signal to be
        deconvolved.  If defined it is always used.   Only allowed to
        be None if `noise_spectrum` is defined.
    :type signal_window:  mspasspy `TimeWindow` object.
    :param noise_window:  similar to signal_window but for data section that
        is to be defined as noise.  Ignored if `noise_spectrum` is defined.
        Otherwise it is required and data in the specified time range is
        used to regularize the inverse operator in the frequency domain.
    :type noise_window:  `TimeSeries` object or None (default)  Note
        can be None only if `noise_spectum` is defined.
    :param use_3C_noise:  When True the power spectrum used for regularizing
        the inverse for deconvolution will be computed from the average power
        spectrum on all three components.   When False (default) the spectrum
        is derived by extracting a TimeSeries from that defined by the
        component argument.  Ignored if `noise_spectrum` is defined
    :param bandwidth_subdocument_key:  When set (default is None)
        bandwidth data will be extracted from a subdocument retrieved with
        this key.   e.g. the broadband_snr_QC function, which would be the normal
        input, has a default of `Parrival` into which snr data are posted including
        bandwidth estimates.  When set the function first retrieves the
        subdocument dict and then the keys defined by `bandwidth_keys` are
        extracted from that dict container.   When this arg is None (default)
        the `bandwidth_keys` are assumed to be defined directly in the Metadata
        container of seis.
    :type bandwidth_subdocument_key:  string
    :param bandwidth_keys:  must define a tuple with two strings that are
        keys defining the bandwidth defined for the inverse operator.   Those
        values should normally be computed by using the MsPASS function
        `broadband_srr_QC`.  The 0 component of the tuple is assumed to be
        a key to use to extract the low frequency corner estimate for the data
        bandwidth.   Component 1 is the key used to extract the high frequency
        corner.  Note the relationship of this parameter to
        `bandwidth_subdocument_key` described immediately above.
    :type bandwidth_keys:  tuple with two strings defining value keys.
        Defaults are keys used by `broadband_snr_QC` so this parameter
        will not need to be changed unless a different algorithm is used
        to estimate te bandwidth parameters.
    :param QCdata_key:   The engine has a QCMetrics method that
        posts some useful data for evaluating results.  This function
        adds others.  The combination is posted to a python dict
        that is then pushed to the return datum's Metadata container
        with this key.  The result when stored in MongoDB is a
        "subdocument" with this key.
    :type QCdata_key:  string (default "CNRFDecon_properties" )
    :param return_wavelet:  When False (default) only the estimated
        receiver function is returned.  When True the return is a tuple
        with 0 containing the RF estimate, 1 containing the actual_output
        of the operator and 1 containing the ideal outpu wavelet.
    :param window_output:  boolean that when True (default) causes the
        output to be windowed in the range defined by signal_window.
        When False the output will normally be longer with the number of
        points being the fft size used internally.   That is, the fft
        is normally zero padded so some signal bleeds to samples between
        npts an the fft size.   False is largely reserved for debugging.
    :return:  `Seismogram` when return_wavelet is False and a tuple as
        described above if True.

    """
    # required arg type checks
    alg = "CNRRFDecon"
    if not isinstance(seis, Seismogram):
        message = alg
        message += ":  illegal type={} for arg0\n".format(str(type(seis)))
        message += "arg0 must be a Seismogram object"
        raise TypeError(message)
    if seis.dead():
        if return_wavelet:
            return [seis, None, None]
        else:
            return seis
    if not isinstance(engine, CNRDeconEngine):
        message = alg
        message += ":  required arg1 (engine) is invalid type={}\n".format(
            str(type(engine))
        )
        message += "Must be an instance of a CNRDeconEngine"
        raise TypeError(message)
    if component not in [0, 1, 2]:
        message = (
            "Illegal value received with component={}.  must be 0, 1, or 2".format(
                component
            )
        )
        raise ValueError(message)

    if signal_window:
        # using the C++ bound function for efficiency here
        # the pybind11 code will throw a bit of an obscure but decipherable
        # message if signal_window is an invalid type
        d = _WindowData3C(seis, signal_window)
    else:
        # important to make  copy here as error conditions could clobber
        # origial
        d = Seismogram(seis)
        # need to define this for window_output
        if window_output:
            signal_window = TimeWindow(seis.t0, seis.endtime())

    if noise_spectrum:
        if noise_spectrum.dead():
            message = "Received noise_spectrum PowerSpectrum object marked dead - cannot process this datum"
            d.elog.log_error(alg, message, ErrorSeverity.Invalid)
            d.kill()
            if return_wavelet:
                return [d, None, None]
            else:
                return d
        psnoise = noise_spectrum
    else:
        if noise_window:
            n = _WindowData3C(seis, noise_window)
            if use_3C_noise:
                psnoise = engine.compute_noise_spectrum_3C(n)
            else:
                n = ExtractComponent(n, component)
                psnoise = engine.compute_noise_spectrum(n)
            if psnoise.dead():
                # this appends elog contents to data elog before returning it dead
                d.elog += psnoise.elog
                message = "Failure in engine.compute_noise_spectrum;  see previous elog message for reason"
                d.elog.log_error("CNRDecon", message, ErrorSeverity.Invalid)
                d.kill()
                # default constructed Seismogram is already marked dead so no need for kill call
                if return_wavelet:
                    return [d, None, None]
                else:
                    return d
        else:
            message = alg
            message += ":  illegal argument combination\n"
            message += "When noise_spectrum is not defined BOTH signal_window and noise_window arguments must be defined"
            raise ValueError(message)

    odt = engine.get_operator_dt()
    if d.dt != odt:
        message = "datum dt = {} does not match fixed operator dt={}\n".format(
            d.dt, odt
        )
        message += "Cannot deconvolve this datum; killing output"
        d.kill()
        d.set_npts(0)
        d.elog.log_error(alg, message, ErrorSeverity.Invalid)
        if return_wavelet:
            return [d, None, None]
        else:
            return d

    [d, flow, fhigh] = fetch_and_validate_bandwidth_data(
        d,
        bandwidth_keys,
        bandwidth_subdocument_key,
    )

    # A datume can be killed in the above test so return immediately if
    # that happened
    if d.dead():
        if return_wavelet:
            return [d, None, None]
        else:
            return d
    # for an RF a data component is used as a wavelet
    w = ExtractComponent(d, component)
    # the engine can throw an exception we need to handle
    try:
        engine.initialize_inverse_operator(w, psnoise)
        d = engine.process(d, psnoise, flow, fhigh)
    except MsPASSError as err:
        d.elog.log_error(err)
        d.kill()
    except Exception as generr:
        message = "Unexpected exception:\n"
        message += str(generr)
        d.elog.log_error(alg, message, ErrorSeverity.Invalid)
        d.kill()

    # note d can be killed and returned or marked dead by the error handlers
    # in both cases we can't continue
    if d.dead():
        if return_wavelet:
            return [d, None, None]
        else:
            return d
    else:
        # some data problems cause NaN outputs for reasons not quite clear
        # Hypothesis is caused by large data spikes from a mass recenter but
        # may not be the only cause.  This is a safety valve since NaN output
        # is always invalid
        bad_values_mask = np.isnan(d.data) | np.isinf(d.data)
        if np.count_nonzero(bad_values_mask) > 0:
            message = "numeric problems - decon output vector has NaN or Inf values\n"
            message += "Known problem for certain types of bad data\n"
            message += "If this occurs a lot check decon parameters"
            d.elog.log_error(alg, message, ErrorSeverity.Invalid)
            d.kill()
            return [d, None, None]

        QCmd = engine.QCMetrics()
        # convert to a dict for posting
        QCmd = dict(QCmd)
        QCmd["algorithm"] = alg
        # this is not currently computed in the C++ operator
        # but is a critical metric that is easily computed with numpy
        # note RFdeconProcessor has this same algorithm which is
        # a maintenance issue to assure they are consistent
        pe = prediction_error(engine, w)
        QCmd["prediction_error"] = pe
        d[QCdata_key] = QCmd
    if window_output:
        d = _WindowData3C(d, signal_window)
    if return_wavelet:
        retval = []
        retval.append(d)
        ao = engine.actual_output(w)
        retval.append(ao)
        ideal_out = engine.ideal_output()
        retval.append(ideal_out)
        return retval
    else:
        return d


@mspass_func_wrapper
def CNRArrayDecon(
    ensemble,
    beam,
    engine,
    *args,
    noise_window=None,
    signal_window=None,
    noise_spectrum=None,
    use_3C_noise=False,
    beam_component=2,  # used only if beam is Seismogram
    use_wavelet_bandwidth=True,
    bandwidth_subdocument_key=None,
    bandwidth_keys=["low_f_band_edge", "high_f_band_edge"],
    return_wavelet=False,
    handles_ensembles=True,
    **kwargs,
) -> tuple:
    """
    Notes:  delete when writing final docstring
    - assume ensemble members are aligned
    - allow beam to be scalar or 3c, use component only if 3c
    - get noise either by windowing beam or via noise_spectrum arg.  cross checks required to be one or the other

    This function applies the Colored Noise Regularization (CNR)
    deconvolution algorithm to data from an array held in a
    `SeismogramEnsemble` object passed as the `ensemble` argument.
    It is important to understand that the algorithm assumes the ensemble
    data all have a common source wavelet that is to be deconvolved with
    the signal defined by the `wavelet` argument.  The only case where that is
    known to be true is for P or S phases at teleseismic distances
    where high frequencies are automatically removed by attentuation.
    The approach may also work for smaller aperture arrays with regional phases
    but the spatial scale of aperture limits is more ambiguous.   It is known
    that teleseismic body waves produce coherent stacks with arrays having
    an aperture of thousands of kiloments (e.g. the Earthscope TA).

    The deconvolution is driven by the single signal stored in the
    TimeSeries object passed via the `wavelet` argument.   The inverse of that
    wavelet is computed by the CNR algorithm using the spectrum of the noise
    computed or passed to the funtion via the `noise_spectrum` argument. That is, we
    use the same method as the multitaper method where damping*noise(f) is
    added to the wavelet spectrum w(f) at each frequency in computing the
    inverse operator in the frequency domain.

    To allow some flexibility in the input, at the cost of a small amount of
    complexity in argument checking, the function is expected to be run in
    one of two ways.  The two modes are driven by the content of
    three arguments:   noise_window, signal_window, and noise_spectrum.
    Your inputs should match the assumptions for one of these two modes:
        1.  If `noise_spectrum` is set (default is a None type),
            the function assumes beam and the ensemble members have all
            been previously windowed down to contain only the signal that
            is to be deconvolved.   In this case, the noise_spectrum is
            used to regularized the inverse to be computed from
            the wavelet TimeSeries input.
        2.  If `noise_spectrum` is undefined (i.e. None type default)
            the two arguments `signal_window` and `noise_window` must both
            be defined as valid mspass `TimeWindow` objects.  In this mode
            beam and all the members of the ensemble are assumed to be in
            relative time and of sufficient duration that we can use
            `WindowData` to extract required data.  Note the time range
            of the `noise_window` is only relevant to the beam
            `TimeSeries` input.   i.e. what happens in this mode is that
            `WindowData` is applied to the `beam` signal and the spectrum of
            the noise is computed from that windowed segment.   If the
            `beam` data does not contain that time range the entire
            ensemble will be marked dead and the function will return
            immediately.   Hence, if running in this mode be sure the
            computed stack (beam) has data in the time range defined by
            noise_window.   In this mode the `signal_window` is applied to
            both the beam and all ensemble members and the time span
            will be the same as `signal_window` for all data.  The
            cleanest model to assure the function works in this mode is
            to create the input with these restrictions:  (a) all enemble
            members are in relative time, (b) the beam is in the same
            relative time base based on a phase arrival time,
            and (c) the beam is a stack of the ensemble members with
            a range large enough to span the input `noise_window` range.
    These two modes exist because at the time of this writing I could see
    two very different ways to estimate the spectrum used to regularize
    the inversion:  (a) the method of mode 2 derived from stack, and
    (b) some average of the noise spectra of the ensemble members.
    It is not at all clear which are preferred or even if it matters.

    The other key issue in a solution produced by the algorithm used in
    CNRDeconEngine is the bandwidth used to shape the output.   The engine's'
    deconvolution algorithm has arguments for the low and high frequency corners
    that should be used to set the shaping wavelet for the output.
    With an ensemble the choice of what that bandwidth should be is
    not clear.   The default extracts the required data form the
    `beam` TimeSeries object using the keys defined by the recipe
    described in the next paragraph.  That is enabled by setting the
    boolan arg `use_wavelet_bandwidth` True (the default).  If that
    arg is set False, every member of the output ensemble that is
    successfully deconvolved will have a different shaping wavelet
    set by the bandwidth attributes defined for that datum.   It is
    unknown at this writing which of these modes will be more useful
    in what settings.

    The bandwidth of the output for either of the approaches described in the
    previous paragraph are controlled by two arguments:  `bandwidth_subdocument_key`
    and `bandwidth_keys`.   If `bandwidth_subdocument_key` is set
    (default is a None type) the function will attempt to extract a dictionary
    with the key that is defined.  It will treat that as a "subdocument" and
    attempt to low and high frequency band edges from that sudocument.  If
    that arg is not set (None type defaullt) the function fetches the
    low and high frequency band edges directly from the relevant data
    object's Metadata container. The keys used to fetch the low and
    high frequency band edges always come from a 2-element tuple that
    is assumed to be the content of the `bandwidth_keys` argument.
    (the 0 component is the low frequency edge and 1 the high edge)
    Where that data is extracted depends on the `use_wavelet_bandwidth` boolean.
    When True the function tries to extract the required attributes from
    the `beam` TimeSeries object.  If that fails the entire ensemble
    will be returned marked dead.  If False, the required values will
    be extracted from each member and only members lacking the required
    attributes would be killed.
    """

    prog = "CNRArrayDecon"
    if not isinstance(ensemble, SeismogramEnsemble):
        message = "Illegal type for arg0 = "
        message += str(type(ensemble))
        message += "\nMust be a SeismogramEnsemble"
        raise TypeError(message)
    if not isinstance(beam, (TimeSeries, Seismogram)):
        message = "Illegal type for required arg1 = "
        message += str(type(beam))
        message += "\nMust be a TimeSeries or Seismogram"
        raise TypeError(message)
    if ensemble.dead() or beam.dead():
        if beam.dead():
            message = "Received beam input marked dead - cannot proceed"
            ensemble.elog.log_error(prog, message, ErrorSeverity.Invalid)
            ensemble.kill()
        if return_wavelet:
            return [ensemble, None, None]
        return ensemble

    # if noise_spectrum is defined noise_windowing is ignored
    if noise_spectrum:
        psnoise = noise_spectrum
    elif signal_window and noise_window:
        # logic means both have to be defined
        # will not check type as TimeWindow objects as the following
        # will abort with reasonable error messages if they aren't
        # could use C++ raw function but this will give a more informative
        # error message if the type of noise_window is wrong
        n2use = WindowData(beam, noise_window.start, noise_window.end)
        if n2use.dead():
            message = (
                "Windowing failed when trying to extract noise data from beam signal\n"
            )
            message += "Killing entire ensemble because we cannot deconvolve these data with this algorithm without noise data"
            ensemble.elog.log_error(prog, message, ErrorSeverity.Invalid)
            ensemble.kill()
            if return_wavelet:
                return [ensemble, None, None]
            else:
                return ensemble
        if isinstance(n2use, TimeSeries) and use_3C_noise:
            message = (
                "beam is a TimeSeries but requested 3C noise with windowing enabled\n"
            )
            message += "Using windowed scalar beam data to compute noise spectrum"
            ensemble.elog.log_error(prog, message, ErrorSeverity.Complaint)
        if isinstance(beam, Seismogram):
            if not use_3C_noise:
                n2use = ExtractComponent(n2use, beam_component)
            # noise is extracted, now we need to extract the beam component
            # as the wavelet
            beam = ExtractComponent(beam, beam_component)
        if use_3C_noise:
            psnoise = engine.compute_noise_spectrum3C(n2use)
        else:
            psnoise = engine.compute_noise_spectrum(n2use)
        # we cannot proceed if the noise spectrum estimation failed
        if psnoise.dead():
            ensemble.elog += psnoise.elog
            ensemble.log_error(
                prog,
                "compute_noise_spectrum failed - cannot process this ensemble",
                ErrorSeverity.Invalid,
            )
            ensemble.kill()
            if return_wavelet:
                return [ensemble, None, None]
            else:
                return ensemble
        beam = WindowData(beam, signal_window.start, signal_window.end)
    else:
        message = "Illegal argument combination.\n"
        message += "When noise_spectrum is not set both noise_window and signal_windows are required\n"
        message += "See docstring for this function"
        raise ValueError(message)
    if signal_window:
        ensout = WindowData(ensemble, signal_window.start, signal_window.end)
    else:
        ensout = ensemble

    # this is a key step that computes the inverse operator in the
    # frequency domain.  In this algorithm that inverse is applied to all
    # ensemble members.
    try:
        engine.initialize_inverse_operator(beam, psnoise)
    except MsPASSError as err:
        ensout.elog.log_error(err)
        ensout.kill()
    except Exception as generr:
        ensout.elog.log_error(
            "CNRArrayDecon", "Unexpected exception", ErrorSeverity.Invalid
        )
        ensout.elog.log_error("CNRArrayDecon", str(generr), ErrorSeverity.Invalid)
        ensout.kill()
    if ensout.dead():
        if return_wavelet:
            return [ensout, None, None]
        else:
            return ensout

    if use_wavelet_bandwidth:
        [beam, flow, fhigh] = fetch_and_validate_bandwidth_data(
            beam, bandwidth_keys, bandwidth_subdocument_key
        )
        # the above can kill - no choice in this case but to kill the entire
        # ensemble.  This is so bad it maybe should be an exception
        # or allow an option to abort on this error
        if beam.dead():
            message = "Invalid bandwidth data encountered when trying to fetch from beam datum\n"
            message += "Details in earlier log message - killing entire ensemble"
            ensout.elog = beam.elog
            ensout.elog.log_error(prog, message, ErrorSeverity.Invalid)
            ensout.kill()
            if return_wavelet:
                return [ensout, None, None]
            else:
                return ensout
    for i in range(len(ensout.member)):
        if ensout.member[i].live:
            d = ensout.member[i]  # only an alias in this context
            if not use_wavelet_bandwidth:
                [d, flow, fhigh] = fetch_and_validate_bandwidth_data(
                    d, bandwidth_keys, bandwidth_subdocument_key
                )
            # We don't need to test for psnoise being live as we assume
            # that error was trapped above - caution if code is altered
            if d.live:
                # There are other ways this can throw an exception so we
                # need this error handler
                try:
                    ensout.member[i] = engine.process(d, psnoise, flow, fhigh)
                except MsPASSError as err:
                    ensout.member[i].elog.log_error(err)
                    ensout.member[i].kill()
    if return_wavelet:
        retval = []
        retval.append(ensout)
        ao = engine.actual_output(beam)
        retval.append(ao)
        ideal_out = engine.ideal_output()
        retval.append(ideal_out)
    else:
        retval = ensout

    return retval
