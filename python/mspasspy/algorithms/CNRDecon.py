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
from mspasspy.ccore.seismic import TimeSeries,Seismogram,SeismogramEnsemble
from mspasspy.ccore.utility import MsPASSError,ErrorSeverity,Metadata
from mspasspy.algorithms.window import WindowData
from mspasspy.algorithms.basic import ExtractComponent
def fetch_bandwidth_data(md,keys)->tuple:
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
    return tuple([flow,fhigh])

def validate_bandwidth_data(d,bd,bdkeys):
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
        if bd[0]<0.0 or bd[1]<0.0:
            message = "Error parsing require bandwidth data\n"
            if bd[0]<0.0:
                message += "Could not fetch data for low frequency corner using key {}\n",format(bdkeys[0])
            if bd[1]<0.0:
                message += "Could not fetch data for high frequency corner using key {}\n",format(bdkeys[1])
            d.elog.log_error(alg,message,ErrorSeverity.Invalid)
            d.kill()
        elif bd[1] <= bd[0]:
            message = "Invalid bandwidth data retrieved:  flow={} and fhigh={}\n".format(bd[0],bd[1])
            message += "Require flow<fhigh"
            d.elog.log_error(alg,message,ErrorSeverity.Invalid)
            d.kill()
    return d

def fetch_and_validate_bandwidth_data(d,
                                      bdkeys,
                                      bandwidth_subdocument_key=None,
                                      )->tuple:
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
            bdvals = fetch_bandwidth_data(bd,bdkeys)
            # this function may kill
            d = validate_bandwidth_data(d, bdvals, bdkeys)
        else:
            message = "Required bandwidth_subdocument_key={} ".format(bandwidth_subdocument_key)
            message += "is not defined for this datum"
            d.elog.log_error(alg,message,ErrorSeverity.Invalid)
            d.kill()
    else:
        bdvals = fetch_bandwidth_data(d,bdkeys)
        d = validate_bandwidth_data(d, bdvals, bdkeys) 
    return [d,bdvals[0],bdvals[1]]
            
    
def CNRRFDecon(seis,
               engine,
               component=2,
               wavelet=None,
               noise_data=None,
               signal_window=None,
               noise_window=None,
               use_3C_noise=False,
               bandwidth_subdocument_key=None,
               bandwidth_keys=['low_f_band_edge','high_f_band_edge'],
               return_wavelet=False,
               )->tuple:
    """
    Uses the CNRRFDeconEngine instance passed as `engine` to deconvolve 
    the content of the input `Seismogram` object passed via `seis` (arg0).  
    The "CNR" algorithm used a "colored noise regularization" algorithm 
    that has two novel features:
    1.  The bandwidth of the deconvolution operator is adjusted based on 
        estimated bandwith from the spectrum of the signal and noise.
        The bandwwidth properties by are extracted from Metadata 
        using keys with defaults defined by values stored in a 
        subdocument (python dict) accessed by the key defined by 
        the "bandwidth_keys" argument.  The defaults are those 
        set by the mspass function broadband_snr_QC.  
    2.  The inverse is regularized by by a frequency dependent damping 
        using the noise spectrum.
        
    There are some complexities in the use of this function due to 
    issue in how the operator is initialized.   That is, the engine 
    is generic and made to work for this single station and the 
    related array algorithm found in this same module.   A key point is 
    that the default will not work so some combination of kwarg 
    value is required.
    1.  For standard P wave receiver function estimates the simplest 
        model is to require `seis` to be a Seismogram object rotated so 
        that one component can be used as a wavelet.  (ZNR, LQT, or FST)
        In that case `component` should specify the component to extract 
        that should be treated as the source wavelet (Z,L, or P component).
        In that case normal use would set the signal_window as a time 
        window relative to the P time of the data section to be deconvolved. 
        Normal use for this context would set the `noise_window` to 
        a `TimeWindow` of data before P that should be treated as the noise. 
        In that situation we use WindowData to carve out both windows 
        from the input `seis` Seismogram data.  i.e. the algorithm 
        assumes the input `seis` spans a time range that incluces 
        the range of `signal_window` and `noise_window`.  
    2.  An alternative is to window the signal and noise data 
        before calling this function.   If that is the case the algorithm 
        expects `signal_window` and `noise_window` to be the default 
        None type and the `seis` input defines the signal_window and 
        `noise_data` are already windowed versions of the data to use 
        to compute the operator.   
    3.  There are nonstandard variations possible with 2 that depend on 
        the boolean argument `use_3C_noise`.  When True in this context 
        `noise_data` is assumed to be a `Seismogram` object.  When False 
        it is required to be a `TimeSeries.   The difference is that when 
        True the average power of the noise on all three components is used
        for the damping of the inverse.  When false the spectrum used 
        is expected to be computed from a scalar signal.   Note that means 
        for option 1 if `use_3C_model` is False the spectrum is computed 
        from the same component used to define the wavelet. 
    4.  To compute so called S-wave receiver functions all three of the 
        above options can work BUT you must change the parameters 
        appropriately.  In particular, when windows are used they 
        must be very different from P.  The different options allow some 
        variation in choices.   In all case, however, a key difference is 
        that for S data the estimate is actually precursors to S so 
        the signal of interest precedes S, which would normally be time 0.  
        The function currently has no procedure to reverse the time as is 
        common practice for handlign S-RF data.
        
    Any other usage may not work or produce invalid results.  
        
    The main work of this function is done inside a C++ function 
    bound to python of type `CNRDeconEngine` passed as the arg1 
    (symbol `engine`).   The engine is passed as an argument because 
    it has a fairly high instantiation cost.   The standard 
    key-value pairs:
        `algorithm` - "generalized_water_level" or "color_noise_damping"
        `damping_factor`
        `noise_floor`
        `target_sample_interval`
        `deconvolution_window_start`
        `deconvolution_window_end`
        `operator_nfft`  - optrional overide of default nfft size computed by power of 2
        TODO:   easier to do from mod of CNR3CDecon.pf
        
    Function arguments:
        
    :param seis:   `Seismogram` object containing data to be deconvolved. 
    As noted above it should normally be the section of data that is to be
    deconvolved or a larger window that will contain data defined by 
    the signal_window and noise_window sections,.   
    :type seis:  Must be a `Seismogram` object.
    :param engine:   Instance of the CNRDeconEngine object noted above.   
    Most of the behavior of the operator is defined by this engine object. 
    :type engine:  `CNRDeconEngine`
    :param component:   component of seis to use as the wavelet estimate.
    This parameter is only used if the wavelet argument is None (the default).
    If signal_window is define the data are first windowed to that range 
    before the wavelet is extracted from the specified component.  Note that is 
    the classic "rexceiver function" concept where some P component is extracted
    as the wavelet estimate.
    :type component: integer (default 2)
    :param wavelet:   Alternative way to specify wavelet to use for deconvolution, 
    When defined (default is None meaning undefined) the signal it contains 
    will be used to estimate a deconvolution operator using the colored noise 
    method. 
    :type wavelet:  `TimeSeries`
    :param noise_data:   Alternative way to define data to be used to compute 
    noise spectrum for wavelet regularization.   When defined the spectrum of the signal 
    contained in this atomic seismic object is used to regularize the inverse 
    operator for the deconvolution.
    :type noise_data:  must be a `TimeSeries` object unless the `use_3C_noise`
    boolean (see below) is set True.  When that is True and this arg is not 
    None it the contents must be a `Seismogram` object.
    :param signal_window:   time span to extract as the signal to be deconvolved.
    :type signal_window:  mspasspy `TimeWindow` object.   
    :param noise_window:  similar to signal_window but for data section that 
    is to be defined as noise.  This arg is ignored even if set if the 
    `noise_data` argument contains a valid seismic datum.   
    :param use_3C_noise:  When True the power spectrum used for regularizing 
    the inverse for deconvolution will be computed from the average power 
    spectrum on all three components.   When False (default) the spectrum 
    is derived by extracting a TimeSeries from that defined by the 
    component argument.  The function tests for consistency of possiblel 
    combinations.  The function will thrown an exception if use_3C_noise is
    set True and `noise_data` defined but not a `Seismogram` object.  If 
    False and `noise_data` is a `Seismogram` the noise spectrum will be 
    computed from the data defined by the `component` index (0, 1, or 2)
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
    :param return_wavelet:  When False (default) only the estimated 
    receiver function is returned.  When True the return is a tuple 
    with 0 containing the RF estimate, 1 containing the actual_output 
    of the operator and 1 containing the ideal outpu wavelet.  
    :return:  `Seismogram` when return_wavelet is False and a tuple as 
    described above if True.
  
    """
    if component not in [0,1,2]:
        message = "Illegal value received with component={}.  must be 0, 1, or 2".format(component)
        raise ValueError(message)
    alg = "CNRRFDecon"
    if wavelet:
        w = wavelet
    else:
        if signal_window:
            d2use = WindowData(seis,signal_window.start,signal_window.end)
        else:
            d2use = seis
        w = ExtractComponent(d2use,component)
    if noise_data:
        n2use = noise_data
    else:
        if noise_window:
            n2use = WindowData(seis,noise_window.start,noise_window.end)
        else:
            message = "Illegal argument combination.   noise_data argument "
            message += "is None but noise_window argument was also undefined\n"
            raise ValueError(message)
    if use_3C_noise and isinstance(n2use,TimeSeries):
        message = "Illegal argument combination.   "
        message += "received a TimeSeries as noise_data but use_3C_noise was set True"
        raise ValueError(message)
    if not use_3C_noise and isinstance(n2use,Seismogram):
        n2use = ExtractComponent(n2use,component)
    odt = engine.get_operator_dt()
    if w.dt != odt:
        message = "wavelet dt = {} does not match fixed operator dt={}\n".format(w.dt,odt)
        message += "Cannot deconvolve this datum; killing output"
        d2use.kill()
        d2use.set_npts(0)
        d2use.elog.log_error(alg,message,ErrorSeverity.Invalid)
        return d2use
    if d2use.dt != odt:
        message = "data dt = {} does not match fixed operator dt={}\n".format(d2use.dt,odt)
        message += "Cannot deconvolve this datum; killing output"
        d2use.kill()
        d2use.set_npts(0)
        d2use.elog.log_error(alg,message,ErrorSeverity.Invalid)
        return d2use
    # have to test noise separately because of option to load noise data 
    # as a separate Seismogram object
    if n2use.dt != odt:
        message = "noise data dt = {} does not match fixed operator dt={}\n".format(d2use.dt,odt)
        message += "Cannot deconvolve this datum; killing output"
        d2use.kill()
        d2use.set_npts(0)
        d2use.elog.log_error(alg,message,ErrorSeverity.Invalid)
        return d2use
    if use_3C_noise:
        if isinstance(n2use,Seismogram):
            psnoise = engine.compute_noise_spectrum(n2use)
        else:
            message = alg
            message += ":  use_3C_noise is set True but noise data received is not a Seismogram object"
            raise TypeError(message)
    else:
        if isinstance(n2use,TimeSeries):
            psnoise = engine.compute_noise_spectrum(n2use)
        else:
            message = alg
            message += ":  use_3C_noise is set False bot noise data received is not a TimeSeries object"
            raise TypeError(message)
    if psnoise.dead():
        dout = Seismogram()
        dout.elog += psnoise.elog
        message = "Failure in engine.compute_noise_spectrum;  see previous elog message for reason"
        dout.elog.log_error("CNRDecon",message,ErrorSeverity.Invalid)
        # default constructed Seismogram is already marked dead so no need for kill call
        if return_wavelet:
            return [dout,None,None] 
        else:
            return dout
    [d2use,flow,fhigh] = fetch_and_validate_bandwidth_data(d2use,
                                                           bandwidth_keys,
                                                           bandwidth_subdocument_key,
                                                           )        
    
    # A datume can be killed in the above test so return immediately if 
    # that happened
    if d2use.dead():
        return d2use
    
    engine.initialize_inverse_operator(w,psnoise)
    dout = engine.process(d2use,psnoise,flow,fhigh)
    QCmd = engine.QCMetrics()
    dout["CNRDecon_QCmetrics"] = QCmd
    # These will be in the subdocument QCmd but duplicating them 
    # at at the document level will make queries on easier
    dout["CNRDecon_f_low"] = flow
    dout["CNRDecon_f_high"] = fhigh
    return_value = []
    return_value.append(dout)
    if return_wavelet:
        return_value=[]
        return_value.append(dout)
        ao = engine.actual_output(wavelet)
        return_value.append[ao]
        ideal_out = engine.ideal_output()
        return_value.append(ideal_out)
    else:
        return dout


def CNRArrayDecon(ensemble,
                  beam,
                  engine,
                  noise_window=None,
                  signal_window=None,
                  noise_spectrum=None,
                  use_3C_noise=False,
                  beam_component=2,   # used only if beam is Seismogram
                  use_wavelet_bandwidth=True,
                  bandwidth_subdocument_key=None,
                  bandwidth_keys=['low_f_band_edge','high_f_band_edge'],
                  return_wavelet=False,
                  )->tuple:
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
            beam and all the members of ensemble are assumed to be in 
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
    CNRDeconEngine is the bandwidth used to shape the output.   The engine
    `deconvolve` method has arguments for the low and high frequency corners
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
    if not isinstance(ensemble,SeismogramEnsemble):
        message = "Illegal type for arg0 = "
        message += str(type(ensemble))
        message += "\nMust be a TimeSeriesEnsemble"
        raise TypeError(message)
    if ensemble.dead():
        return [ensemble]
    if not isinstance(beam,[TimeSeries,Seismogram]):
        message = "Illegal type for required arg1 = "
        message += str(type(beam))
        message += "\nMust be a TimeSeries or Seismogram"
        raise TypeError(message)

        
    # if noise_spectrum is defined noise_windowing is ignored    
    if noise_spectrum:
        psnoise = noise_spectrum
    elif signal_window and noise_window:
        # logic means both have to be defined 
        # will not check type as TimeWindow objects as the following 
        # will abort with reasonable error messages if they aren't
        # could use C++ raw function but this will give a more informative
        # error message if the type of noise_window is wrong
        n2use = WindowData(beam,noise_window.start,noise_window.end)
        if n2use.dead():
            message = "Windowing failed when trying to extract noise data from beam signal\n"
            message += "Killing entire ensemble because we cannot deconvolve these data with this algorithm without noise data"
            ensemble.elog.log_error(prog,message,ErrorSeverity.Invalid)
            ensemble.kill()
            return [ensemble]
        if isinstance(n2use,TimeSeries) and use_3C_noise:
            message = "beam is a TimeSeries but requested 3C noise with windowing enabled\n"
            message += "Using windowed scalar beam data to compute noise spectrum"
            ensemble.elog.log_error(prog,message,ErrorSeverity.Complaint)
        if isinstance(beam,Seismogram):
            if not use_3C_noise:
                n2use = ExtractComponent(n2use,beam_component)
            # noise is extracted, now we need to extract the beam component 
            # as the wavelet
            beam = ExtractComponent(beam,beam_component)
        # this method is overloaded.  When input is Seismogram 
        # it will automatically compute average spectrum of all 
        # three components
        psnoise = engine.compute_noise_spectrum(n2use)
        beam = WindowData(beam,signal_window.start,signal_window.end)
    else:
        message = "Illegal argument combination.\n"
        message += "When noise_spectrum is not set both noise_window and signal_windows are required\n"
        message += "See docstring for this function"
        raise ValueError(message)
    if signal_window:
        ensout = WindowData(ensemble,signal_window.start,signal_window.end)
    else:
        ensout = ensemble
        

    # this is a key step that computes the inverse operator in the 
    # frequency domain.  In this algorithm that inverse is applied to all 
    # ensemble members.
    engine.initialize_inverse_operator(beam,psnoise)

    if use_wavelet_bandwidth:
        [beam,flow,fhigh] = fetch_and_validate_bandwidth_data(beam, 
                                                bandwidth_keys,
                                                bandwidth_subdocument_key)
        # the above can kill - no choice in this case but to kill the entire 
        # ensemble.  This is so bad it maybe should be an exception 
        # or allow an option to abort on this error
        if beam.dead():
            message = "Invalid bandwidth data encountered when trying to fetch from beam datum\n"
            message += "Details in earlier log message - killing entire ensemble"
            ensout.elog=beam.elog
            ensout.elog.log_error(prog,message,ErrorSeverity.Invalid)
            ensout.kill()
            return [ensout]
    for i in range(len(ensout.member)):
        if ensout.member[i].live:
            d = ensout.member[i]   # only an alias in this context
            if not use_wavelet_bandwidth:
                [d,flow,fhigh] = fetch_and_validate_bandwidth_data(d, 
                                                        bandwidth_keys,
                                                        bandwidth_subdocument_key)
            # this conditional isn't really necessary as the C++ algorithm 
            # returns immediately if the datum is marked dead, but this 
            # makes the logic clear and the code base more robust for a near zero cost
            if d.live:
                ensout.member[i] = engine.process(d,flow,fhigh)
            
    return_value=[]
    return_value.append(ensout)
    if return_wavelet:
        return_value = []
        ao = engine.actual_output()
        return_value.append[ao]
        ideal_out = engine.ideal_output()
        return_value.append(ideal_out)
        
    return return_value


    