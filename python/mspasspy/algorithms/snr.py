import numpy as np
from scipy.signal import hilbert
from obspy.geodetics import locations2degrees
from mspasspy.ccore.utility import (MsPASSError,
                                    ErrorSeverity)
from mspasspy.ccore.seismic import TimeSeries,Seismogram
from mspasspy.ccore.algorithms.deconvolution import MTPowerSpectrumEngine
from mspasspy.ccore.algorithms.amplitudes import (RMSAmplitude,
                                                     PercAmplitude,
                                                     MADAmplitude,
                                                     PeakAmplitude,
                                                     EstimateBandwidth,
                                                     BandwidthStatistics)
from mspasspy.ccore.algorithms.basic import (TimeWindow,
                                             Butterworth,
                                             ExtractComponent)
from mspasspy.algorithms.window import WindowData

def _window_invalid(d,win):
    """
    Small helper used internally in this module.  Tests if TimeWidow defined
    by win has a span inside the range of the data object d.  Return True
    if the range is invalid - somewhat reversed logic is used because of
    the name choice that make the code conditioals clearer.
    """
    if d.t0 < win.start and d.endtime() > win.end:
        return True
    else:
        return False
    
def _safe_snr_calculation(s,n):
    """
    Helper used in this module for all snr calculations.   snr is 
    always defined as a ratio of signal amplitude divided by noise amplitude. 
    An issue is that with simulation data it is very common to have a noise 
    window that is pure zeros.   If computed naively snr would then be 
    normally be returned as NaN.  NaNs can cause a lot of mysterious errors 
    so we handle that differently here.  When noise amplitude is 0 we 
    then test the signal amplitude.  If it is nonzero we return large 
    number defined inside this function as 999999.9 (just under 1 million).
    If both amplitudes are zero we return -1.0 which can be properly treated 
    as an error or data with low snr.  
    """
    if n == 0.0:
        if s > 0.0:
            return 999999.9
        else:
            return -1.0
    else:
        return s/n
        
def snr(data_object,noise_window=TimeWindow(-130.0,-5.0),
  signal_window=TimeWindow(-5.0,120.0),noise_metric='mad',signal_metric='mad',
  perc=95.0):
  """
  Compute time-domain based signal-to-noise ratio with a specified metric. 
  
  Signal-to-noise ratio is a fundamental measurement in all forms of 
  seismic data processing.   There is, however, not a single unified metric 
  that ideal for all types of signals one may want to analyze.  One class 
  of metrics used time-domain metrics to use some measure of amplitude in 
  a signal and noise window cut from a single waveform segment.  A type 
  example is snr of some particular "seismic phase" (P, S, PP, ScS, etc)
  relative to some measure of background noise.  e.g. for P phases it is 
  nearly universal to try to estimate snr from some window defined by the 
  arrival time of P and a noise window before the time P arrives (pre-event noise).
  
  This function provides a generic api to measure a large range of metrics 
  using one of four choices for measuring the norm of the data in the 
  signal and noise windows:
      1.  rms - L2 norm
      2.  mad - median absolute difference, which is essentially the median amplitude in this context
      3.  perc - percentage norm ala seismic unix.  perc is defined at as the 
          amplitude level were perc percentage of the data have an amplitude 
          smaller than this value.  It is computed by ranking (sorting) the 
          data, computing the count of that perctage relative to the number of 
          amplitude samples, and returning the amplitude of the nearest value
          to that position in the ranked data.  
      4.  peak - is the peak value which in linear algebra is the L infinity norm
      
  Note the user can specify a different norm for the signal and noise windows.  
  The perc metric requires specifying what percentage level to use.
  
  This function will throw a MsPASSError exception if the window parameters 
  do not define a time period inside the range of the data_object. You will 
  need a custom function if the model of windows insider a larger waveform 
  segment does not match your data. 
  
  There is one final detail about an snr calculation that we handle carefully.
  With simulation data it is very common to have error free simulations where 
  the "noise" window one would use with real data is all zeros.  An snr calculated
  with this function in that situation would either return inf or NaN depending 
  on some picky details.  Neither is good as either can cause downstream 
  problems.  For that reason we trap any condition where the noise amplitude 
  measure is computed as zero.  If the signal amplitude is also zero we return 
  a -1.0.  Otherwise we return a large, constant, positive number.  Neither 
  condition will cause an exception to be thrown as that condition is considered 
  somewhat to be anticipated. 
  
  :param data_object:  MsPASS atomic data object (TimeSeries or Seismogram) 
    to use for computing the snr.  Note that for Seismogram objects the 
    metrix always use L2 measures of amplitude of each sample (i.e. vector amplitudes)
    If snr for components of a Seismogram are desired use ExtractComponent and 
    apply this function to each component separately. 
  :param noise_window: TimeWindow objects defining the time range to extract 
    from data_object to define the part of the signal considered noise.  
    Times can be absolute or relative.  Default the range -5 to 120 which 
    is makes sense only as time relative to some phase arrival time.  
  :param signal_window:  TimeWindow object defining the time range to 
    extract from data_object to define the part of the signal defines as 
    signal to use for the required amplitude measure.  Default of -130 to 
    -5 is consistent with the default noise window (in terms of length) and 
    is assumes a time relative to a phase arrival time.  For absolute times 
    each call to this function may need its own time window.
  :param noise_metric:  string defining one of the four metrics defined above
    ('mad','peak','perc' or 'rms') to use for noise window measurement.
  :param signal_metric:  string defining one of the four metrics defined above
    ('mad','peak','perc' or 'rms') to use for signal window measurement.
  :return: estimated signal-to-noise ratio as a single float.  Note the 
    special returns noted above for any situation where the noise window 
    amplitude is 0
  """
  if _window_invalid(data_object,noise_window):
    raise MsPASSError("snr:  noise_window []{wstart} - {wend}] is outside input data range".format(wstart=noise_window.start,wend=noise_window.end),
      ErrorSeverity.Invalid)
  if _window_invalid(data_object,signal_window):
    raise MsPASSError("snr:  noise_window []{wstart} - {wend}] is outside input data range".format(wstart=noise_window.start,wend=noise_window.end),
      ErrorSeverity.Invalid)
  n = WindowData(data_object,noise_window.start,noise_window.end)
  s = WindowData(data_object,signal_window.start,signal_window.end)
  if noise_metric == 'rms':
    namp=RMSAmplitude(n)
  elif noise_metric == 'mad':
    namp=MADAmplitude(n)
  elif noise_metric == 'peak':
    namp=PeakAmplitude(n)
  elif noise_metric == 'perc':
    namp=PercAmplitude(n,perc)
  else:
    raise MsPASSError("snr:  Illegal noise_metric argument = "+noise_metric,ErrorSeverity.Invalid)

  if signal_metric == 'rms':
    samp=RMSAmplitude(s)
  elif signal_metric == 'mad':
    samp=MADAmplitude(s)
  elif signal_metric == 'peak':
    samp=PeakAmplitude(s)
  elif signal_metric == 'perc':
    samp=PercAmplitude(s,perc)
  else:
    raise MsPASSError("snr:  Illegal signal_metric argument = "+signal_metric,ErrorSeverity.Invalid)

  return _safe_snr_calculation(samp, namp)

def FD_snr_estimator(data_object,
  noise_window=TimeWindow(-130.0,-5.0),noise_spectrum_engine=None,
    signal_window=TimeWindow(-5.0,120.0), signal_spectrum_engine=None,
      band_cutoff_snr=2.0,
      # check these are reasonable - don't remember the formula when writing this
        tbp=5.0,ntapers=10,
         high_frequency_search_start=5.0,
          poles=3,
            perc=95.0,
              optional_metrics=None,
                return_as_dict=False,
                  store_as_subdocument=False,
                    subdocument_key='snr_data'):
        #optional_metrics=['snr_stats','filtered_envelope','filtered_L2','filtered_Linf','filtered_MAD','filtered_perc']):
    """
    Estimates one or more metrics of signal-to-noise from a TimeSeries object.
    An implicit assumption is that the analysis is centered on a timeable "phase"
    like P, PP, etc.

    This is a python function that can be used to compute one or several
    signal-to-noise ratio estimates based on an estimated bandwidth using
    the C++ function EstimateBandwidth.  The function has a fair number of
    options, but the core metric computed are the bandwidth estimates
    computed by that function.  It uses a fairly simple search algorithm
    that functions well for most earthquake sources.  For the low end the
    algorithm searches from the first frequency indistinguishable from DC to
    find the lowest frequency for which the snr exceeds a threshold specified
    by the input parameter 'band_cutoff_snr'.   It does a similar search
    from the high end from a point 80% of Nyquist - a good choice for all
    modern digital data that use FIR antialias filters.   Both searches are
    not just defined with just the first frequency to satisfy the snr
    threshold criteria.  Only when a group of frequencies more than 2 times
    the time-bandwidth product exceed the threshold is the band edge
    defined.   The actual band edge is then defined as the first frequency
    exceeding the threshold.  That more elaborate algorithm was used to
    prevent pure lines in either the signal or noise spectrum from
    corrupting the estimates.

    A set of optional metrics can be computed.  All optional metrics use
    the bandwidth estimates in one way or another.   Optional metrics are
    defined by the following keywords passed through a list (actually
    any iterable container will work) of strings defining one or more
    of the keywords. The metrics and a brief description of each follow:

    *snr_stats* computes what are commonly plotted in box plots for the
    snr estimates within the estimated bandwidth:  minimum, maximum,
    0.25 (1/4) point, 0.75 (3/4) point, and the median.   These are set
    with following dict keys:   'snr_band_maximum','snr_band_minimum',
    'snr_band_1/4', 'srn_band_3/4', and 'snr_band_median' respectively.

    *filtered_envelope*, *filtered_L2*, *filtered_Linf*, *filtered_perc*, and *filtered_MAD*:
    All of these optional metrics first copy the data_object and then
    filter the copy with a Butterworth bandpass filter with the number of
    poles specified by the npoles argument and corners at the estimated
    band edge by the EstimateBandwidth function.   The metrics computed
    are time domain snr estimates computed with he filtered data.  They are
    actually computed from functions in this same module that can be
    used independently and have their own docstring description. The
    functions called have the following names in order of the keyword
    list above:  *snr_envelope*, *snr_L2*, *snr_Linv*, and *snr_MAD*.
    When the computed they are set in the output dictionary with the
    following (again in order) keys:  'snr_envelope','snr_L2', 'srn_Linf',
    and 'snr_MAD'.

    :param data_object:  TimeSeries object to be processed. For Seismogram
    objects the assumption is algorithm would be used for a single
    component (e.g longitudinal or vertical for a P phase)

    :param noise_window: defines the time window to use for computing the
    spectrum considered noise.
    
    :param signal_window: defines the time window to use that defines what
    you consider "the signal".
    
    :param noise_spectrum_engine: is expected to either by a None type
    or an instance of a ccore object called an MTPowerSpectralEngine.
    When None an instance of MTPowerSpectralEngine is computed for
    each call to this function.   That is a convenience for small
    jobs or when called with data from mixed sample rates and/or variable
    length time windows.   It is very inefficient to use the default
    approach for processing large data sets and really for any use in a
    map operation with dask or spark.  Normal use should be for the user to
    predefine an MtPowerSpectralEngine from the expected window size
    for a given data sample rate and include it in the function call.
    
    :param signal_spectrum_engine:  is the comparable MTPowerSpectralEngine
    to use to compute the signal power spectrum.   Default is None with the
    same caveat as above for the noise_spectrum_engine.
    
    :param tbp:  time-bandwidth product to use for computing the set of
    Slepian functions used for the multitaper estimator.  This parameter is
    used only if the noise_spectrum_engine or signal_spectrum_engine
    arguments are set as None.
    
    :param ntapers:  is the number of Slepia functions (tapers) to compute
    for the multitaper estimators. Like tbp it is referenced only if
    noise_spectrum_engine or signal_spectrum_engine are set to None.
    Note the function will throw an exception if the ntaper parameter is
    not consistent with the time-bandwidth product.  (ADD THE FORMULA HERE)
    
    :param high_frequency_search_start: Used to specify the upper frequency 
      used to start the search for the upper end of the bandwidth by 
      the function EstimateBandwidth.  Default is 4.0 which reasonable for 
      teleseismic P wave data.  Should be change for usage other than 
      analysis of teleseimic P phases or you the bandwidth may be 
      grossly underestimated.
      
    :param npoles:   defines number of poles to us for the Butterworth
    bandpass applied for the "filtered" metrics (see above).  Default is 3.
    
    :param perc:   used only if 'filtered_perc' is in the optional metrics list.
    Specifies the perc parameter as used in seismic unix.  Uses the percentage
    point specified of the sorted abs of all amplitudes.  (Not perc=50.0 is
    identical to MAD)  Default is 95.0 which is 2 sigma for Gaussian noise.
    
    :param optional_metrics: is an iterable container containing one or more
    of the optional snr metrics discussed above.
    
    :param return_as_dict:  All the quantities computed in this function 
    are best abstacted as Metadata.   For the expected normal use of this 
    function in a map call of dask os spark it is most appropriate to 
    post the results to the Metadata container in the input data_object 
    and return the modified data_object.  When this argument is set False, 
    which is the default, that is the behavior.  When this argument is 
    set to True, only the python dict container used internally to store 
    the (variable) outputs is returned.  The True case is most useful 
    if this function is being run interactively or in a serial workflow.
    We also use it in a set of thin wrappers for more specialized 
    functions elsewhere in this module.
    
    :param store_as_subdocument:  This parameter is included for 
    flexibility but should not normally be changed by the user.  As noted 
    earlier the outputs of this function are best abstracted as Metadata. 
    When this parameter is False the Metadata members are all posted with 
    directly to data_object's Metadata container.  If set True the 
    internally generated python dict is copied and stored with a key 
    defined through the subdocument_key argument.  See use below in 
    function arrival_snr.  
        
    :param subdocument_key:  key for storing results as a subdocument. 
    This parameter is ignored unless store_as_subdocument is True.  
    Default is "snr_data"
    
    :return:  python dict with computed metrics associated with keys defined above.
    :rtype: python dict containing measurements with keys noted above.  If there 
    are error (e.g. window errors) they will be posted in the data_object elog 
    and the return will be an empty dict
    """
    # For this algorithm we dogmatically demand the input be a TimeSeries
    if not isinstance(data_object,TimeSeries):
        raise MsPASSError("FD_snr_estimator:  Received invalid data object - arg0 data must be a TimeSeries",
            ErrorSeverity.Invalid)
    if data_object.dead():
        if return_as_dict:
            return dict()
        else:
            return data_object
    # We enclose all the main code here in a try block and cat any MsPASSErrors
    # they will be posted as log message. Others will not be handled 
    # intentionally letting python's error mechanism handle them as 
    # unexpected exceptions - MsPASSError can be anticipated for data problems
    snrdata=dict()
    try:
        # First extract the required windows and compute the power spectra
        n = WindowData(data_object,noise_window.start,noise_window.end)
        s = WindowData(data_object,signal_window.start,signal_window.end)
        if noise_spectrum_engine:
            nengine=noise_spectrum_engine
        else:
            nengine = MTPowerSpectrumEngine(n.npts,tbp,ntapers)
        if signal_spectrum_engine:
            sengine = signal_spectrum_engine
        else:
            sengine = MTPowerSpectrumEngine(n.npts,tbp,ntapers)
        N=nengine.apply(n)
        S=sengine.apply(s)
        bwd = EstimateBandwidth(S.df,S,N,
                band_cutoff_snr,tbp,high_frequency_search_start)
        # These estimates are always computed and posted
        snrdata['low_f_band_edge'] = bwd.low_edge_f
        snrdata['high_f_band_edge'] = bwd.high_edge_f
        snrdata['low_f_band_edge_snr'] = bwd.low_edge_snr
        snrdata['high_f_band_edge_snr'] = bwd.high_edge_snr
        snrdata['spectrum_frequency_range'] = bwd.f_range
        snrdata['bandwidth_fraction'] = bwd.bandwidth_fraction()
        snrdata['bandwidth'] = bwd.bandwidth()
        #For current implementation all the optional metrics require 
        #computed a filtered version of the data.  If a new option is
        #desired that does not require filtering the data the logic 
        #here will need to be changed to create a more exclusive test
        if len(optional_metrics) > 0:
            # use the mspass butterworth filter for speed - obspy 
            # version requires a conversion to Trace objects
            BWfilt = Butterworth(False,True,True,
                                 poles,bwd.low_edge_f,
                                 poles,bwd.high_edge_f,
                                 data_object.dt)
            filtered_data = TimeSeries(data_object)
            BWfilt.apply(filtered_data)
            nfilt = WindowData(filtered_data,noise_window.start,noise_window.end)
            sfilt = WindowData(filtered_data,signal_window.start,signal_window.end)
            # In this implementation we don't need this any longer so we 
            # delete it here.  If options are added beware
            del filtered_data
            # Some minor efficiency would be possible if we avoided 
            # duplication of computations when multiple optional metrics 
            # are requested, but the fragility that adds to maintenance 
            # is not justified
            if 'snr_stats' in optional_metrics:
                stats = BandwidthStatistics(S,N,bwd)
                # stats is a Metadata container - copy to snrdata
                for k in stats.keys():
                    snrdata[k] = stats[k]         
            if 'filtered_envelope' in optional_metrics:
                analytic_nfilt=hilbert(nfilt.data)
                analytic_sfilt=hilbert(sfilt.data)
                nampvector=np.abs(analytic_nfilt)
                sampvector=np.abs(analytic_sfilt)
                namp = np.median(nampvector)
                samp = np.max(sampvector)
                snrdata['snr_envelope_Linf_over_L1'] = _safe_snr_calculation(samp,namp)           
            if 'filtered_L2' in optional_metrics:
                namp=RMSAmplitude(nfilt)
                samp=RMSAmplitude(sfilt)
                snrvalue=_safe_snr_calculation(samp, namp)
                snrdata['snr_L2'] = snrvalue
            if 'filtered_MAD' in optional_metrics:
                namp=MADAmplitude(nfilt)
                samp=MADAmplitude(sfilt)
                snrvalue=_safe_snr_calculation(samp, namp)
                snrdata['snr_MAD'] = snrvalue
            if 'filtered_Linf' in optional_metrics:
                # the C function expects a fraction - for users a percentage
                # is clearer
                namp=PercAmplitude(nfilt,perc/100.0)
                samp=PeakAmplitude(sfilt)
                snrvalue=_safe_snr_calculation(samp, namp)
                snrdata['snr_Linf'] = snrvalue
            if 'filtered_perc' in optional_metrics:
                namp=MADAmplitude(nfilt)
                samp=PercAmplitude(sfilt,perc/100.0)
                snrvalue=_safe_snr_calculation(samp, namp)
                snrdata['snr_perc'] = snrvalue

    except MsPASSError as err:
        data_object.elog.log_error(err)
    if return_as_dict:
        return snrdata
    else:
        if store_as_subdocument:
            data_object[subdocument_key] = snrdata
        else:
            for k in snrdata:
                data_object[k] = snrdata[k]
        return data_object

 
def arrival_snr(data_object,
  noise_window=TimeWindow(-130.0,-5.0),noise_spectrum_engine=None,
    signal_window=TimeWindow(-5.0,120.0), signal_spectrum_engine=None,
      band_cutoff_snr=2.0,
      # check these are reasonable - don't remember the formula when writing this
        tbp=5.0,ntapers=10,
         high_frequency_search_start=5.0,
          poles=3,
            perc=95.0,
              phase_name='P',
                metadata_key='Parrival',
                  optional_metrics=['snr_stats','filtered_envelope','filtered_L2','filtered_Linf','filtered_MAD','filtered_perc']):
    

    """
    Specialization of FD_snr_estimator.   A common situation where snr 
    data is a critical thing to estimate is data windowed around a given 
    seismic phase.   FD_snr_estimator is a bit more generic.  This function 
    removes some of the options from the more generic function and 
    has a frozen structure appropriate for measuring snr of a particular phase.
    In particular it always stores the results as a subdocument (python dict) 
    keyed by the name defined in the metadata_key argument.   The idea of 
    that sturcture is the contents of the subdocument are readily extracted 
    and saved to a MongoDB "arrival" collection with a normalization key.
    Arrival driven workflows can then use queries to reduce the number of 
    data actually retrieved for final processing.  i.e. the arrival 
    collection data should be viewed as a useful initial quality control 
    feature. 
    
    Most parameters for this function are described in detail in the 
    docstring for FD_snr_estimator.  The user is referred there to 
    see the usage.   The following are added for this specialization:
        
    :param phase_name:  Name tag for the seismic phase being analyzed.  
    This string is saved to the output subdocument with the key "phase".
    The default is "P"
    
    :param metadata_key:  is a string used as a key under which the 
    subdocument (python dict) created internally is stored.  Default is 
    "Parrival".   The idea is if multiple phases are being analyzed 
    each phase should have a different key set by this argument 
    (e.g. if PP were also being analyzed in the same workflow you 
     might use a key like "PParrival").
    
    :return:  a copy of data_object with the the results stored under 
    the key defined by the metadata_key argument.
    """
    if data_object.dead():
        return data_object
    snrdata=FD_snr_estimator(data_object,noise_window,noise_spectrum_engine,
            signal_window,signal_spectrum_engine,band_cutoff_snr,tbp,ntapers,
              high_frequency_search_start,poles,perc,optional_metrics,
                return_as_dict=True,store_as_subdocument=False)
    snrdata['phase'] = phase_name
    data_object[metadata_key] = snrdata
    
def arrival_snr_QC(data_object,
  noise_window=TimeWindow(-130.0,-5.0),noise_spectrum_engine=None,
    signal_window=TimeWindow(-5.0,120.0), signal_spectrum_engine=None,
      band_cutoff_snr=2.0,
      # check these are reasonable - don't remember the formula when writing this
        tbp=5.0,ntapers=10,
         high_frequency_search_start=5.0,
          poles=3,
            perc=95.0,
              phase_name='P',
                metadata_key='Parrival',
                  optional_metrics=['snr_stats','filtered_envelope','filtered_L2','filtered_Linf','filtered_MAD','filtered_perc'],
                    db=None,
                      collection='arrival',
                        use_measured_arrival_time=False,
                          measured_arrival_time_key='Ptime',
                            taup_model=None,
                              update_mode=False,
                                component=2,
                                  source_collection='source',
                                    receiver_collection=None):
    """
    Compute a series of metrics that can be used for quality control 
    filtering of seismic phase data.  
    
    This is the highest level function in this module for computing 
    signal-to-noise ratio metrics for processing signals that can be 
    defined by a computable or measurable "phase".  Features this 
    function adds over lower level functions in this module are:
        1.  An option to save computed metrics to a MongoDB collection 
            (defaults as "arrival").  If the update_mode argument is 
            set True (default is False) the function expects the data_object 
            to contain the attribute "arrival_id" that references the 
            ObjectID of an existing entry in the the collection where the
            data this function computes is to be saved (default is"arrival").
        2.  Adds an option to use a computed or measured arrival as the 
            time reference for all windowing.   The lower level snr 
            functions in this module require the user do what this 
            function does prior to calling the function.  Note one or the other is required 
            (i.e. either computed or measured time will be define t0 of the 
             processing)
            
    The input of arg 0 (data_object) can be either a TimeSeries or 
    a Seismogram object.  If a Seismogram object is passed the "component"
    argument is used to extract the specified single channel from the Seismogram 
    object and than component is used for processing.  That is necessary 
    because all the algorithms used are single channel algorithms.  To 
    use this function on all components use a loop over components BUT 
    make sure you use a unique value for the argument "metadata_key" for 
    each component.  Note this will also produce multiple documents per 
    input datum.  
    
    The type of the data_object also has a more subtle implication the 
    user must be aware of.  That is, in the MsPASS schema we store receiver coordinates 
    in one of two different collections:  "channel" for TimeSeries data and 
    "site" for Seismogram data.  When such data are loaded the generic keys 
    like lat are always converted to names like channel_lat or site_lat 
    for TimeSeries and Seismogram data respectively.   This function uses 
    the data type to set that naming.  i.e. if the input is TimeSeries 
    it tries to fetch the latitude data as channel_lat while if it the input 
    is a Seismogram it tries to fetch site_lat.   That is true of all coordinate 
    data loaded by normalization from a source and receiver collection.
            
    The following args are passed directly to the function arrival_snr:
    noise_window, signal_window, band_cutoff_snr, tbp, ntapers, poles, 
    perc, phase_name, metadata_key, and optional_metrics.  See the docstring 
    for arrival_snr and FD_snr_estimator for descriptions of how these 
    arguments should be used.  This top level function adds arguments 
    decribed below.

    :param db:  mspass Database object that is used as a handle for to MongoDB.
    Default is None, which the function takes to mean you don't want to 
    save the computed values to MongoDB.   In this mode the computed 
    metrics will all be posted to a python dict that can be found under the 
    key defined by the "metadata_key" argument.   When db is defined the 
    contents of that same python dict will save to MongoDB is the 
    collection defined by the "collection" argument.  If db is run as 
    the default None the user is responsible for saving and managing the 
    computed snr data.   Be aware a simple later call to db.save_data
    will not produce the same normalized data with the (default) arrival 
    collection.  
    
    :param collection:  MongoDB collection name where the results of this 
    function will be saved.  If the "update_mode" argument is also set 
    True the update section will reference this collection. Default is "arrival". 
    
    :param use_measured_arrival_time:  boolean defining the method used to 
    define the time reference for windowing used for snr calculations.  
    When True the function will attempt to fetch a phase arrival time with
    the key defined by the "measured_arrival_time_key" argument.  In that 
    mode if the fetch fails the data_object will be killed and an error 
    posted to elog.   That somewhat brutal choice was intentional as the 
    expectation is if you want to use measured arrival times you don't 
    want data where there are no picks.   The default is True to make 
    the defaults consistent.  The reason is that the tau-p calculator 
    handle is passed to the function when using model-based travel times.  
    There is no way to default that so it defaults to None.   
    
    :param measured_arrival_time_key: is the key used to fetch a 
    measured arrival time.   This parameter is ignored if use_measured_arrival_time 
    is False.
    
    :param taup_model: when use_measured_arrival_time is False this argument 
    is required.  It defaults as None because there is now way the author 
    knows to initialize it to anything valid.  If set it MUST be an instance 
    of the obspy class TauPyModel (https://docs.obspy.org/packages/autogen/obspy.taup.tau.TauPyModel.html#obspy.taup.tau.TauPyModel)
    Mistakes in use of this argument can cause a MsPASSError exception to 
    be thrown (not logged thrown as a fatal error) in one of two ways: 
    (1)  If use_measured_arrival_time is False this argument must be defined, 
    and (2) if it is defined it MUST be an instance of TauPyModel. 
    
    :param update_mode:   When True the function will attempt to extract 
    a MongoDB ObjectID from data_object's Metadata using the (currently fixed) 
    key "arrival_id".   If found it will add the computed data to an existing 
    document in the collection defined by the collection argument.  Otherwise 
    it will simply add a new entry and post the ObjectID of the new document 
    with the (same fixed) key arrival_id.  When False no attempt to fetch 
    the arrival id is made and we simply add a record.  This parameter is 
    completely ignored unless the db argument defines a valid Database class.
    
    :param component: integer (0, 1, or 2) defining which component of a 
    Seismogram object to use to compute the requested snr metrics.   This 
    parameter is ignored if the input is a TimeSeries.
    
    :param source_collection:  normalization collection for source data. 
    The default is the MsPASS name "source" which means the function will 
    try to load the source hypocenter coordinates (when required) as 
    source_lat, source_lon, source_depth, and source_time.  
    
    :param receiver_collection:  when set this name will override the 
    automatic setting of the expected normalization collection naming 
    for receiver functions (see above).  The default is None which causes 
    the automatic switching to be involked.  If it is any other string 
    the automatic naming will be overridden. 
    
    :return:  the data_object modified by insertion of the snr QC data
    in the object's Metadata
    """
    if data_object.dead():
        return data_object
    if isinstance(data_object,TimeSeries):
        # We need to make a copy of a TimeSeries object to assure the only 
        # thing we change is the Metadata we add to the return
        data_to_process = TimeSeries(data_object)
        if receiver_collection:
            rcol = receiver_collection
        else:
            rcol = 'channel'
    elif isinstance(data_object,Seismogram):
        if component < 0 or component > 2:
            raise MsPASSError("arrival_snr_QC:  usage error.  "
                + "component parameter passed with illegal value={n}\n".format(n=component)
                + "Must be 0, 1, or 2")
        data_to_process = ExtractComponent(data_object,component)
        if receiver_collection:
            rcol = receiver_collection
        else:
            rcol = 'site'
    else:
        raise MsPASSError("arrival_snr_QC:   received invalid input data\n"
                + "Input must be either TimeSeries or a Seismogram object",
                ErrorSeverity.Fatal)
    if use_measured_arrival_time:
        arrival_time = data_object[measured_arrival_time_key]
    else:
        # This test is essential or python will throw a more obscure,
        # generic exception
        if taup_model is None:
            raise MsPASSError("arrival_snr_QC:  usage error.  "
                    + "taup_model parameter is set None but use_measured_arrival_time is False\n"
                    + "This gives no way to define processing windows.  See docstring",
                    ErrorSeverity.Fatal)
        source_lat = data_object[source_collection + '_lat']
        source_lon = data_object[source_collection + '_lon']
        source_depth = data_object[source_collection + '_depth']
        source_time = data_object[source_collection + '_time']
        receiver_lat = data_object[rcol + '_lat']
        receiver_lon = data_object[rcol + '_lon']
        delta = locations2degrees(source_lat,source_lon,receiver_lat,receiver_lon)
        arrival = taup_model.get_travel_times(source_depth_in_km=source_depth,
                        distance_in_degree=delta,phase_list=[phase_name])
        arrival_time = source_time + arrival[0].time
        taup_arrival_phase = arrival[0].phase.name
        # not sure if this will happen but worth trapping it as a warning if 
        # it does
        if phase_name != taup_arrival_phase:
            data_object.elog.log_error("arrival_snr_QC",
                "Requested phase name=" + phase_name +
                " does not match phase name tag returned by obpsy taup calculator="+taup_arrival_phase,
                "Complaint")
    if data_to_process.time_is_UTC():
        data_to_process.ator(arrival_time)
    snrdata=FD_snr_estimator(data_to_process,noise_window,noise_spectrum_engine,
            signal_window,signal_spectrum_engine,band_cutoff_snr,tbp,ntapers,
              high_frequency_search_start,poles,perc,optional_metrics,
                return_as_dict=True,store_as_subdocument=False)
    snrdata['phase'] = phase_name  
    # Note we add this result to data_object NOT data_to_process because that 
    # is not always the same thing - for a TimeSeries input it is a copy of 
    # the original but it may have been altered while for a Seismogram it is 
    # an extracted component
    data_object[metadata_key] = snrdata
    if db:
        arrival_id_key = collection + "_id"
        dbcol = db[collection]
        if update_mode:   
            if data_object.is_defined(arrival_id_key):
                arrival_id = data_object[arrival_id_key]
                filt = {'_id' : arrival_id}
                update_clause = {'$set' : snrdata}
                dbcol.update_one(filt,update_clause)
            else:
                data_object.elog.log_error("arrival_snr_QC",
                    "Running in update mode but arrival id key="+arrival_id_key+" is not defined\n"
                    + "Inserting computed snr data as a new document in collection="
                    + collection,"Complaint")
                arrival_id = dbcol.insert_one(snrdata).inserted_id
                data_object[arrival_id_key] = arrival_id
        else:
            arrival_id = dbcol.insert_one(snrdata).inserted_id
            data_object[arrival_id_key] = arrival_id
    return data_object


