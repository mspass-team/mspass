#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module implementing a similar algorithm to dbxcor in python.  

Created on Tue Jul  9 05:37:40 2024

@author: pavlis
"""
from mspasspy.ccore.utility import ErrorLogger,ErrorSeverity,Metadata
from mspasspy.ccore.seismic import (TimeSeries,
                                    TimeSeriesEnsemble,
                                    TimeReferenceType,
                                    DoubleVector)
from mspasspy.ccore.algorithms.basic import TimeWindow
from mspasspy.algorithms.window import WindowData
import numpy as np
from scipy import signal

def dbxcor_weights(ensemble,stack):
    """
    Computes the robust weights used originally in dbxcor for each 
    member of ensemble.   Result is returned in a parallel numpy 
    array (i.e. return[i] is computed weight for ensemble.member[i])
    
    This function is made for speed and has no safeties.  It assumes 
    all the members of ensemble are the same and it is the same length 
    as stack.  It will throw an exception if that is violated so callers 
    should guarantee that happens.  
    
    Returns a numpy vector of weights.  Any dead data will have a weight of 
    -1.0 (test for negative is sufficient).   
    
    :param ensemble:  `TimeSeriesEnsemble` of data from which weights are to 
    be computed.   
    :type ensemble:  Assumed to be a `TimeSeriesEnsemble`.  No type checking
    is done so if input is wrong an exception will occur but what is thown 
    will depend on what ensemble actually is.
    :param stack:  TimeSeries containing stack to be used to compute weights.
    The method returns robust weights relative to the vector of data in 
    this object.
    :type stack:  `TimeSeries`.
    
    """
    norm_floor = np.finfo(np.float32).eps * stack.npts
    N = len(ensemble.member)
    wts = np.zeros(N)
    r = np.zeros(stack.npts)
    # Scale the scack vector to be a unit vector
    s_unit = np.array(stack.data)
    nrm_s = np.linalg.norm(stack.data)
    s_unit /= nrm_s  
    for i in range(N):
        if ensemble.member[i].dead():
            wts[i] = -1.0
        else:
            # debug
            print("dbxcor_weights:  datum.npts=",ensemble.member[i].npts,
                  "stack vector size=",len(s_unit))
            d_dot_stack = np.dot(ensemble.member[i].data,s_unit)
            r = ensemble.member[i].data - d_dot_stack*s_unit
            nrm_r = np.linalg.norm(r)
            nrm_d = np.linalg.norm(ensemble.member[i].data)
            if nrm_r < norm_floor:
                nrm_r = norm_floor
            if nrm_d < norm_floor:
                # this shouldn't happen but better to do this
                # than get a nan for a wt value which we could 
                # otherwise
                nrm_d = norm_floor

            wts[i] = abs(d_dot_stack)/(nrm_r*nrm_d)
 
    # rescale weights so largest is 1 - easier to understand
    maxwt = np.max(wts)
    wts /= maxwt
    return wts

def validate_ensemble(ensemble):
    """
    The stacking algorithm used in the robust_stack function below 
    requires all members to be the same sample interval.   This function 
    checks that all LIVE data have a common sample interval.   In the same 
    scan it computes the time range spanned by all members.   It returns 
    a tuple with the following 4 components:
        0 - "ok" boolean.  That is, if the scan showed now issues returns 
            True.  If False component 1 will have error log messages.
        1 - ErrorLogger.   When 0 is False the log will contain one 
            or more error messages.  When true it will be empty and 
            can be ignored.
        2 - start time of time interval spanned by all data
        3 - end time of time interval spanned by all data.
        

    """
    alg = "validate_ensemble"
    # empty ensembles are by definition not ok
    elog = ErrorLogger()
    if len(ensemble.member) <= 0:
        message = "Ensemble is empty - no data to process"
        elog.log_error(alg,message,ErrorSeverity.Invalid)
        return [False,elog,0.0,0.0]
    t0values=[]
    endvalues=[]
    dtvalues=[]
    for i in range(len(ensemble.member)):
        d = ensemble.member[i]  # used as a shorthand symbol 
        if d.live:
            t0values.append(d.t0)
            endvalues.append(d.endtime())
            dtvalues.append(d.dt)
    starttime = np.max(t0values)
    endtime = np.min(endvalues)
    dt_test = np.median(dtvalues)
    # isclose warns tests against number less than one 
    # should not use default atol.  The solution we use 
    # her is to test dt values scaled to 1.0 dividing by median dt
    aok=True
    nlive=0
    for i in range(len(ensemble.member)):
        d = ensemble.member[i]
        if d.live:
            nlive += 1
            dt_scaled = d.dt/dt_test
            if not np.isclose(dt_scaled,1.0):
                message = "Irregular sample interval in ensemble member {}\n".format(i)
                message = "Median sample interval={} but this datum has dt={}\n",format(dt_test,d.dt)
                # a single problem is a complaint, caller should decide if we need to kill
                elog.log_error(alg,message,ErrorSeverity.Complaint)
                aok = False
    if nlive == 0:
        message = "Ensemble has no live data - all members are marked dead"
        elog.log_error(alg,message,ErrorSeverity.Invalid)
        aok = False
    else:
        nbad=0
        for d in ensemble.member:
            if d.live and  d.time_is_UTC():
                nbad += 1
        if nbad>0:
            aok=False
            message = "{} of {} ensemble members are on UTC time base\n".format(nbad,len(ensemble.member))
            message += "Use ator to convert the ensemble to relative time base required by this algorithm"
            elog.log_error(alg,message,ErrorSeverity.Invalid)
    return [aok,elog,starttime,endtime]

def regularize_ensemble(ensemble,starttime,endtime)->TimeSeriesEnsemble:
    """
    Secondary function to regularize an ensemble for input to robust 
    stacking algorithm.  ASsumes all data have the same sample rate.  
    Uses WindowData to assumre all data are inside the common 
    range startime:endtime.  Silently drops dead data.
    """
    ensout = TimeSeriesEnsemble(Metadata(ensemble),len(ensemble.member))
    if ensemble.elog.size()>0:
        ensout.elog = ensemble.elog
    for i in range(len(ensemble.member)):
        d = ensemble.member[i]
        if d.live:
            if np.fabs((d.t0-starttime)) > d.dt or np.fabs(d.endtime()-endtime) > d.dt:
                d = WindowData(d,starttime,endtime)
            ensout.member.append(d)
        else:
            message = "Dropped member number {} that was marked dead".format(i)
            ensout.elog.log_error("regularize_ensemble",message,ErrorSeverity.Complaint)
    if len(ensout.member)>0:
        ensout.set_live()
    return ensout
    
def robust_stack(ensemble,
                 method='dbxcor',
                 stack0=None,
                 stack_md=None,
                 )->TimeSeries:
    """
    Generic function for robust stacking live members of a `TimeSeriesEnsemble`.
    An optional initial stack estimate can be used via tha stack0 argument. 
    The function currently supports two methods:  "median" for a median 
    stack and "dbxcor" to implement the robust loss function 
    used in the dbxcor program defined in Pavlis and Vernon (2010).
    
    :param ensemble:   input data to be stacked.   Should all be in 
    relative time with all members having the same relative time span.
    :type ensemble:  TimeSeriesEnsemble
    :param method: Defines a name string of the method to be used to 
    compute the stack.  
    :type method:  string.  Currently must be one of two values or the 
    function will abort:   "median" or "dbxcor".  As the names imply 
    "median" will cause the function to return the median of the sample 
    vectors while "dbxcor" applies the dbxcor method.  
    :param stack0:  optional initial estimate for stack.  Estimators 
    other than median I know of use a loss function for downweighting 
    members of the stack that do not match the stack as defined by 
    some misfit metric.   This argument can be used to input an optional
    starting estimate of the stack for the dbxcor method.  By default it 
    uses the median as the starting point, but this can be used to 
    input something else.   Note the function will silently ignore this 
    argument if method == "median".   
    :type stack0:  TimeSeries.   Note the time span of this optional input 
    must be the same or wider than the ensemble member range defined by 
    the (internal to this module) validate_ensemble function or the 
    return will be return as a copy of this TimeSeries marked dead.
    Default for this argument is None which means the median will be 
    used for the initial stack for dbxcor
    :param stack_md:   optional Metadata container to define the 
    content of the stack output.  By default the output will have only
    Metadata that duplicate required internal attributes (e.g. t0 and npts).  
    An exception is if stack0 is used the Metadata of that container will 
    be copied and this argument will be ignored.  
    :type stack_md:  Metadata container or None.  When None (default) 
    the output will have only minimal Metadata content.   Most applications 
    will need to override the default as the output will be completely 
    ambiguous. 
    """
    # if other values for method are added they need to be added here
    if method not in ["median","dbxcor"]:
        message = "Illegal value for argument method={}\n".format(method)
        message += "Currently must be either median or dbxcor"
        raise ValueError(message)
    # don't test type - if we get illegal type let it thro0w an exception
    if ensemble.dead():
        return ensemble
    [aok,elog,starttime,endtime] = validate_ensemble(ensemble)
    # dogmatically refuse to continue aok is not returned True 
    # by the above function.  Alterantive is to have an optional 
    # expeced sample interval and kill everything that doesn't match
    # here we assume user should use the resample function before calling 
    # this function
    if not aok:
        ensemble.elog += elog
        ensemble.kill()
        return ensemble
    # This function should guarantee all data are within 
    # one sample of the startime:endtime range.   It should 
    # also discard any dead data to simplify indexing
    # intitionally overwriting input for memory management
    ensemble = regularize_ensemble(ensemble,starttime,endtime)
    M = len(ensemble.member)
    # can now assume they are all the same length and don't need to worry about empty ensembles
    N = ensemble.member[0].npts

    if stack0:
        stack = WindowData(stack0,starttime,endtime)
        if stack.dead():
            message = "Received an initial stack estimate with time range smaller than stack window\n"
            message = "Recovery not implemented - stack returned is invalid"
            
            stack.elog.log_error("robust_stack",message,ErrorSeverity.Invalid)
            return stack
    else:
        # bit of a weird logic here - needed because we need option for 
        # dbxcor method to use median stack as starting point or use 
        # the input via stack0.  This does that 
        #
        # Also this is a bit of a weird trick using inheritance to construct a 
        # TimeSeries object for stack using an uncommon constructor.
        # The actual constructor wants a BasicTimeSeries and Metadata as 
        # arg0 and arg1.   A TimeSeries is a sublass of BasicTimeSeries so this 
        # resolves.  Note the conditional is needed as None default for 
        # stack_md would abort
        stack = TimeSeries(N)
        if stack_md:
            stack = TimeSeries(stack,stack_md)
            stack.t0 = starttime
            # this works because we can assume ensemble is not empty and clean
            stack.dt = ensemble.member[0].dt
        # constructor defaults time base to relative so need to handle GMT 
        # should not be the norm but we need to handle this
        # This is a pretty obscure part of the ccore api
        if ensemble.member[0].time_is_UTC:
            stack.tref = TimeReferenceType.UTC
    
        # Always compute the median stack as a starting point 
        # that was the algorithm of dbxcor and there are good reasons for it
        data_matrix=np.zeros(shape=[M,N])
        for i in range(M):
            data_matrix[i,:] = ensemble.member[i].data
            stack_vector = np.median(data_matrix,axis=0)
        stack.data=DoubleVector(stack_vector)
        stack.set_live()
    if method == "dbxcor":
        stack = _dbxcor_stacker(ensemble,stack)

    return stack
        
    
def _dbxcor_stacker(ensemble,stack0,eps=0.0001,maxiterations=20)->tuple:
    """
    Runs the dbxcor robust stacking algorithm using initial stack estimate 
    stack0.   Returns a clone of stack0 but with the sample data replaced
    by the output of the robust algorithm using the dbxcor weighting function.
    Returns a tuple with the stack as component 0 and a numpy vector 
    of the final robust weights as component 1.   
    
    This function is intended to be used only internally in this module 
    as it has no safties and assumes ensemble and stack0 are what it expects.
    
    :param ensemble:  TimeSeriesEnsemble assumed to have constant data 
    range and sample interval and not contain any dead data.
    :param stack0:  TimeSeries of initial stack estimate - assumed to have
    same data vector length as all ensemble members.
    :param eps:  relative norm convergence criteria.  Stop iteration when 
    norm(delta stack data)/norm(stack.data)<eps.
    :param maxiterations:  maximum number of iterations (default 20)
    """
    stack=TimeSeries(stack0)
    # useful shorthands
    N=stack0.npts
    M=len(ensemble.member)
    for i in range(maxiterations):
        wts = dbxcor_weights(ensemble,stack)
        newstack = np.zeros(N)
        sumwts = 0.0
        for j in range(M):
            # dbxcor_weights returns -1 for dead data but we don't 
            # use that feature here as we assume all are live
            newstack += wts[j]*ensemble.member[i].data
            sumwts += wts[j]
        newstack /= sumwts   # standard formula or weighted stack 
        # order may matter herre.  In this case delta becomes a numpy 
        # array which is cleaner in this context
        delta = newstack - stack.data
        relative_delta = np.linalg.norm(delta)/np.linalg.norm(stack.data)
        # update stack here so if we break loop we don't have to repeat
        # this copying
        # newstack is a numpy vector so this cast is necesary
        stack.data = DoubleVector(newstack)
        if relative_delta<eps:
            break
    return stack
        
def _xcor_shift(ts,beam):
    """
    Internal function with no safeties to compute a time shift in 
    seconds for beam correlation.   The shift is computed by 
    using the scipy correlate function.  The computed shift is the 
    time shift that would need to be applied to ts to align with 
    that of beam.   Note that number can be enormous if using UTC 
    times and it will still work.   The expected use, however, 
    in this function is with data prealigned by an phase arrival 
    time (predicted from a mdoel of an estimate).  
    :param ts:  TimeSeries datum to correlate with beam
    :param beam:  TimeSeries defining the common signal (beam) for
      correlation - the b argument to correlation.  
    """
    xcor = signal.correlate(ts.data,beam.data,mode='same')
    lags=signal.correlation_lags(ts.npts,beam.npts,mode='same')
    # numpy/scipy treat sample 0 as time 0 
    # with TimeSeries we have to correct with the t0 values to get timing right
    lag_of_max_in_samples = lags[np.argmax(xcor)]
    lagtime = ts.dt*lag_of_max_in_samples + ts.t0 - beam.t0
    return lagtime

def beam_align(ensemble,beam,time_shift_limit=10.0):
    """
    Aligns ensemble members using signal defined by beam (arg1) argument. 
    Computes cross correlation between each ensemble member and the beam. 
    All live ensemble members are shifted to align with time base of the 
    beam.   Note that can be a huge shift if the beam is relative and 
    the ensemble members are absolute time.  It should work in that context 
    but the original context was aligning common-source gathers for 
    teleseismic phase alignment where the expectation is all the ensemble 
    members and the beam are in relative time with 0 defined by some 
    estimate of the phase arrival time.   Will correctly handle irregular 
    window sizes between ensemble members and beam signal.   
    
    :param ensemble:  ensemble of data to be correlated with 
    beam data.  
    :type ensemble:  assume to be a TimeSeriesEnsemble
    :param beam:  common signal to correlate with ensemble members.  
    :type beam:  assumed to be a TimeSeries object
    :param time_shift_limit:  ceiling on allowed time shift for 
    ensemble members.   Any computed shift with absolute value 
    larger than this value will be reset to this value with the 
    sign of the shift preserved.   (i.e. a negative lag will 
    be set to the negative of this number).  The default is 10.0 
    which is large for most data more or less making this an optional 
    parameter.
    :type time_shift_limit: float (may abort if you use an int 
    because the value can to sent to a C++ method that it type 
    sensitive)
    :return:   copy of ensemble with the members time shifted to align with 
    the time base of beam.   
    
    """
    # this may not be necessary for internal use but if used 
    # externally it is necessary to avoid mysterious results
    # we don't test ensemble or beam because exceptions are 
    # guaranteed in that case that should allow problem solving
    if time_shift_limit<0.0:
        message = "beam_align:  illegal value time_shift_limit={}\n".format(time_shift_limit)
        message += "value must be positive"
        raise ValueError(message)
    for i in range(len(ensemble.member)):
        d = ensemble.member[i]
        # in this context not needed but tiny cost for robustness
        if d.live:
            timelag = _xcor_shift(d,beam)
            # apply a ceiling/floor to allowed time shift via 
            # the time_shift_limit arg 
            if timelag>time_shift_limit:
                timelag = time_shift_limit
            elif timelag < (-time_shift_limit):
                timelag = -time_shift_limit
            # We MUST use this method instead of dithering t0 to keep 
            # absolute time right.  This will fail if the inputs were 
            # not shifted from UTC times
            # also note a +lag requires a - shift
            ensemble.member[i].shift(timelag)
    return ensemble

                    
    
def align_and_stack(ensemble,
                    beam,
                    correlation_window=None,
                    correlation_window_keys=['correlation_window_start','correlation_window_end'],
                    window_beam=False,
                    robust_stack_window=None,
                    robust_stack_window_keys=['robust_window_start','robust_window_end'],
                    robust_stack_method="dbxcor",
                    output_stack_window=None,  # None means use ensemble range maxt0 to minendtime
                    ensemble_index_key="i0",  # interal - should not change but warn in docstring
                    robust_weight_key='robust_stack_weight',
                    time_shift_key='arrival_time_correction',
                    time_shift_limit=2.0,    
                    convergence=0.001,
                    )->tuple:
    """
    This function uses an initial estimate of the array stack passed as
    the `beam` argument as a seed to a robust algorithm that will 
    align all the data in the input ensemble by cross-correlation with 
    the beam, apply a robust stack to the aligned signals, update the 
    beam with the robust stack, and repeat until the changes to the 
    beam signal are small.   Returns a copy of the ensemble time 
    shifted to be aligned with beam time base and an updated beam 
    estimate crated by the robust stack.  The shifts and the weights
    of each input signal are store in the Metadata of each live ensemble 
    member returned with keys defined by `robust_weight_key` and 
    `time_shift_key`.  
    
    This function is a python implementation of the same basic 
    algorithm used in the dbxcor program described by 
    Pavlis and Vernon(2010) Array Processing of teleseismic body waves 
    with the USArray, Computers and Geosciences,15, 910-920.
    It has additional options made possible by the python interface 
    and integration into MsPASS.  In particular, the original algorithm 
    was designed to work as part of a GUI where the user had to pick 
    a set of required parameters for the algorithm.   In this function 
    those are supplied through Metadata key-value pairs and/or arguments.
    This function uses a pythonic approach aimed to allow this program 
    to be run in batch without user intervention.  The original dbxcor 
    algorithm required four interactive picks to set the input.   
    The way we set them for this automated algorithm is described in the 
    following four itemize paragraphs:
  
        1.  The "correlation window", which is the waveform segment 
            used to compute cross-correlations between the beam and all
            ensmebled members, is set one of three ways.  The default 
            uses the time window defined by the starttime and endtime of 
            the `beam` signal as the cross-correlation window.   
            Alternative, this window can be specified either by 
            fetching values from the `Metadata` container of beam 
            or via the `correlation_window` argument.   The algorithm 
            first tests if `correlation_window` is set an is an 
            instance of a `TimeWindow` object.   If the type of 
            the argument is not a `TimeWindow` object an error is logged 
            and the program reverts to using the span of the beam 
            signal as the correlation window.   If `correlation_window`
            is a None (default) the algorithm then checks for a valid 
            input via the `correlation_window_keys` argument.  If 
            defined that argument is assumed to contain a pair of strings 
            that can be used as keys to fetch start (component 0)
            and end times (component 1) from the Metadata container of 
            the TimeSeries objct passed via beam. For example, 
            ```
               correlation_window_keys = ['correlation_start','correlation_end']
            ```
            would cause the function to attempt to fetch the 
            start time with "correlation_start" and end time with 
            "correlation_end".  In the default both `correlation_window` 
            and `correlation_window_keys` are None which cause the 
            function to silently use the window defined as 
            [beam.t0, beam.endtime()] as the correlation winow.  
            If the optional boolean `window_beam` argument is set True 
            the function will attempt to window the beam using a range 
            input via either of the optional methods of setting the 
            correlation window.  An error is logged and nothing happens if 
            `window_beam` is set True and the default use of the beam 
            window is being used.   
        2.  The "robust window" is a concept used in dbxcor to 
            implement a special robust stacking algorithm that is a novel 
            feature of the dbxcor algorithm.   It uses a very aggresssive 
            weighting scheme to downweight signals that do not match the 
            beam.  The Pavlis and Vernon paper shows examples of how this 
            algorithm can cleanly handle ensembles with a mix of high 
            signal-to-noise data with pure junk and produce a clean
            stack that is defined 
        3.  dbxcor required the user to pick a seed signal to use as 
            the initial beam estimate.  That approach is NOT used here 
            but idea is to have some estimate passed to the algorithm 
            via the beam (arg1) argument.  In MsPASS the working model 
            is to apply the  broadband_snr_QC function to the data before 
            running this function and select the initial seed (beam) from 
            one or more of the computed snr metrics.   In addition, 
            with this approach I envision a two-stage computation where 
            the some initial seed is used for a first pass.   The 
            return is then used to revise the correlation window by 
            examining stack coherence metrics and then rerunning the 
            algorithm.   The point is it is a research problem for 
            different types of data to know how to best handle the 
            align and stack problem.  
        4.  dbxcor had a final stage that require picking the arrival time 
            to use as the reference from the computed beam trace.  That is 
            actually necessary if absolute times are needed because the 
            method used will be biased by the time shift of the beam 
            relative to the reference time.   See the Pavlis and Vernon 
            paper for more on this topic.   The idea here is that if 
            absolute times are needed some secondary processing will be used 
            to manually or automatically pick an arrival time from the 
            beam output.  
            
    This function does some consistency checking of arguments to handle 
    the different modes for handling the correlation and robust windows
    noted above.  It also applies a series of validation tests on the 
    input ensemble before attempting to run.  Any of the following will 
    cause the return to be a dead ensemble with an explanation in the 
    elog container of the ensemble (in these situation the stack is an 
    empty `TimeSeries` container):
        1.  Irreglar sample intervals of live data.
        2.  Any live data with the time reference set to UTC 
        3.  Inconsistency of the time range of the data and the 
            time windows parsed for the correlation and robust windows.  
            That is, it checks the time span of all member functions and 
            if the time range of all members (min of start time and maximum end times).
            is not outside (inclusive) the range of the correlation and robust 
            windows it is viewed as invalid.
        4.  What we called the "robust window" in the dbxcor paper is
            required to be inside (inclusive of endpoints) the cross-correlation window 
            time window.   That could be relaxed but is a useful constraint because 
            in my (glp) experience the most coherent part of phase arrivals is the 
            first few cycles of the phase that is also the part cross-correlation 
            needs to contain if it is to be stable.   The "robust window" should 
            be set to focus on the section of the signal that will have the most 
            coherent stack.  
    There is a further complexity in the iteration sequence used by this algorithm 
    for any robust stack method.  That is, time shifts computed by cross-correlation 
    can potentially move the correlation window outside the bounds of the 
    data received as input.   To reduce the impact of that potential problem 
    the function has an optional argument called `time_shift_limit` 
    that is validated against other inputs.   In particular, the function 
    computes the average start and end time (keep in mind the assumption is the 
    time base is time relative to the arrival time a particular phase) 
    of the input ensemble.   If the difference between the average start time 
    and the correlation window start time is less than `time_shift_limit` 
    the input is viewed as problematic.   How that is handled depends on how 
    the correlation window is set.  If it is received as constant 
    (`correlation_window` argument) an exception will be thrown to abort 
    the entire job.   It that window is extracted from the beam TimeSeries 
    Metadata container a complaint is logged to the outputs.  An 
    endtime inconsistency is treated the same way.  i.e. it is treated as 
    a problem if the average ensemble endtime - the correlation window 
    endtime is less than the `time_shift_limit`.  
    
                    
    Note the output stack normally spans a different time range than 
    either the correlation or robust windows.   That property is defined 
    by the `output_stack_window` argument.  See below for details.   
    
    :param ensemble:   ensemble of data to be aligned and stacked.  
    This function requires all data to be on a relative time base.  
    It will throw a MsPASSError exception if any live datum has a UTC time base.
    The assumption is all data have a time span that have the correlation 
    and robust windows inside the data time range.   Both windows are 
    carved from the inputs using the WindowData function which will kill 
    any members that do not satisfy this requirement.  
    :type ensemble:  `TimeSeriesEnsemble` with some fairly rigid requirements. 
    (see above)
    :param beam:  Estimate of stack (may be just one representative member)
    used as the seed for initial alignment and stacking.   
    :type beam:  `TimeSeries`.  Must have a length consistent with window 
    parameters. 
    :param correlation_window: Used to specify the time window for 
    computing cross-correlations with the beam signal.   Closely linked to 
    `correlation_window_keys` as described above.
    :type correlation_window:  `TimeWindow` to define explicitly.  If None
    (default) uses the recipe driven by `correlation_window_keys` (see above)
    :param correlation_window_keys:   optional pair of Metadata keys used to 
    extract cross-correlation window attributes from beam Metadata container. 
    If defined component 0 is taken as the key for the start time of the window and 
    component 1 the key for the end time.  
    :type correlation_window_key:  iterable list containing two strings.  
    Default is None which is taken to mean the span of the beam signal defines 
    the cross-correlation window.
    :param window_beam:  if True the parsed cross-correlation window attributes
    are applied to the beam signal as well as the data before starting 
    processing.   Default is False which means the beam signal is used 
    directly in all cross-correlations. 
    :param robust_stack_window: Provide an explicity `TimeWindow` used for 
    extracting the robust window for this algorithm.   Interacts with the 
    robust_stack_window_keys argument as described above. 
    :type robust_stack_window:  If defined must be a `TimeWindow` object.  
    If a None type (default) use the logic defined above to set this time window.
    :param robust_stack_window_keys: specifies a pair of strings to be used 
    as keys to extract the strart time (component 0) and end time (component 1)
    of the robust time window to use from the beam `TimeSeries.
    :type robust_stack_window_keys: iterable list of two strings 
    :param output_stack_window:  optional `TimeWindow` to apply to the 
    computed robust stack output.   Default returns a stack spanning the 
    inner range of all live members of the ensemble. 
    :type output_stack_window:  `TimeWindow` object.  If None (default) the range 
    is derived from the ensemble member time ranges.
    :param robust_weight_key:  The robust weight used for each member to 
    compute the robust stack output is posted to the Metadata container of 
    each live member with this key.  
    :type robust_weight_key:  string
    :param robust_stack_method:   keyword defining the method to use for 
    computing the robust stack.  Currently accepted value are:
    "dbxcor" (default) and "median".  
    :type robust_stack_method:  string  - must be one of options listed above.
    :param time_shift_key:  the time shift applied relative to the starting 
    point is posted to each live member with this key.  It is 
    IMPORTANT to realize this is the time for this pass.  If this functions 
    is applied more than once and you reuse this key the shift from the 
    previous run will be overwritten.  If you need to accumulate shifts 
    it needs to be handled outside this function. 
    :type time_shift_key:  string  (default "arrival_time_correction")
    :param convergence:   fractional change in robust stack estimates in 
    iterative loop to define convergence.  This should not be changed 
    unless you deeply understand the algorithm.
    :type convergence:  real number (default 0.001)
    :return: tuple with 0 containing the original ensemble but time 
    shifted by cross-correlation.   Failed/discarded signals for the 
    stack are not killed but should be detected by not having the 
    time shift Metadata value set.   component 1 is the computed 
    stack windowed to the range defined by `stack_time_window`.
    """
    alg="align_and_stack"
    # maximum iterations.  could be passed as an argument but I have 
    # never seen this algorithm not converge in 20 interation
    MAXITERATION=20
    # Enformce types of ensemble and beam
    if not isinstance(ensemble,TimeSeriesEnsemble):
        message = alg + ":  illegal type for arg0 (ensemble) = {}\n".format(str(type(ensemble)))
        message += "Must be a TimeSeriesEnsemble"
        raise ValueError
    if not isinstance(beam,TimeSeries):
        message = alg + ":  illegal type for arg1 (beam) = {}\n".format(str(type(beam)))
        message += "Must be a TimeSeries"
        raise ValueError
    if ensemble.dead():
        return
    # we need to make sure this is part of a valid set of algorithms 
    if robust_stack_method not in ["dbxcor","median"]:
        message = "Invalid value for robust_stack_method={}.  See docstring".format(robust_stack_method)
        raise ValueError(message)
    # This section implements the somewhat complex chain of options for 
    # setting the correlation window
    xcor_window_is_defined = False  # needed for parsing logic below
    # when this value is True window constraint errors cause the beam returned to be killed 
    # with error messages.   If set in parsers to False an exception is thrown 
    # as in that situation both windows would be set as arguments and the function would 
    # always fail
    windows_extracted_from_metadata = True  
    if correlation_window:
        if isinstance(correlation_window,TimeWindow):
            xcorwin = correlation_window
            xcor_window_is_defined=True
            windows_extracted_from_metadata = False
        else:
            message = "Illegal type for correlation_window={}\m".format(str(type(correlation_window)))
            message = "For this option must be a TimeWindow object"
            raise ValueError(message)
    elif correlation_window_keys:
        # this is a bit dogmatic - I know there is a less restrictive 
        # test than this
        if isinstance(correlation_window_keys,list):
            skey=correlation_window_keys[0]
            ekey=correlation_window_keys[1]
            if beam.is_defined(skey) and beam.is_defined(ekey):
                stime=beam[skey]
                etime=beam[ekey]
            else:
                message = "missing one or both of correlation_window_keys\n"
                if beam.is_defined(skey):
                    stime = beam[skey]
                else:
                    message += 'start time key={} is not set in beam signal\n'.format(skey)
                    message += 'reverting to beam signal start time'
                    ensemble.elog.log_error(alg,message,ErrorSeverity.Complaint)
                    stime = beam.t0
                if beam.is_defined(ekey):
                    etime = beam[ekey]
                else:
                    message += 'emd time key={} is not set in beam signal\n'.format(ekey)
                    message += 'reverting to beam signal endtime() method output'
                    ensemble.elog.log_error(alg,message,ErrorSeverity.Complaint)
                    etime = beam.endtime()
            xcorwin = TimeWindow(stime,etime)
            xcor_window_is_defined = True
        else:
            message = "Illegal type={} for correlation_window_keys argument\n".format(str(type(correlation_window_keys)))
            message += "If defined must be a list with 2 component string used as keys"
            raise ValueError(message)
    else:
        # it isn't considered an error to land here as this is actually the default 
        # note it is important in the logic that xcor_window_is_defined be 
        # left false
        xcorwin = TimeWindow(beam.t0,beam.endtime())
        windows_extracted_from_metadata = False
    if xcor_window_is_defined and window_beam:
        beam = WindowData(beam,xcorwin.start,xcorwin.end)
    # now a simpler logic to handle robust window 
    if robust_stack_window:
        if isinstance(robust_stack_window,TimeWindow):
            rwin = robust_stack_window
            windows_extracted_from_metadata = False
            
        else:
            message = "Illegal type for robust_stack_window={}\m".format(str(type(robust_stack_window)))
            message = "For this option must be a TimeWindow object"
            raise ValueError(message)
        
    elif robust_stack_window_keys:
        # this is a bit dogmatic - I know there is a less restrictive 
        # test than this
        if isinstance(robust_stack_window_keys,list):
            skey=robust_stack_window_keys[0]
            ekey=robust_stack_window_keys[1]
            if beam.is_defined(skey) and beam.is_defined(ekey):
                stime=beam[skey]
                etime=beam[ekey]
            else:
                message = "missing one or both of robust_stack_window_keys\n"
                if beam.is_defined(skey):
                    stime = beam[skey]
                else:
                    message += 'start time key={} is not set in beam signal\n'.format(skey)
                    message += 'reverting to beam signal start time'
                    ensemble.elog.log_error(alg,message,ErrorSeverity.Complaint)
                    stime = beam.t0
                if beam.is_define(ekey):
                    etime = beam[ekey]
                else:
                    message += 'emd time key={} is not set in beam signal\n'.format(ekey)
                    message += 'reverting to beam signal endtime() method output'
                    ensemble.elog.log_error(alg,message,ErrorSeverity.Complaint)
                    etime = beam.endtime()
            rwin = TimeWindow(stime,etime)
        else:
            message = "Illegal type={} for robust_stack_window_keys argument\n".format(str(type(robust_stack_window_keys)))
            message += "If defined must be a list with 2 component string used as keys"
            raise ValueError(message)
    else:
        message = "Must specify either a value for robust_stack_window or robust_stack_window_keys - both were None"
        raise ValueError(message)
    
    # Validate the ensemble
    #  First verify the robust window is inside the correlation window (inclusive of edges)
    if not ( rwin.start >= xcorwin.start and rwin.end <= xcorwin.end):
        message = "Cross correlation window and robust window intervals are not consistent\n"
        message += "Cross-correlation window:  {}->{}.   Robust window:  {}->{}\n".format(correlation_window.start,correlation_window.end,robust_stack_window.start,robust_stack_window.end)
        message += "Robust window interval must be within bounds of correlation window"
        if windows_extracted_from_metadata:
            beam.elog.log_error(alg,message,ErrorSeverity.Invalid)
            beam.kill()
            return [ensemble,beam]
        else:
            raise ValueError(alg + ":  " + message)
    # this function does all other ensemble validation and returns an estimate of the time span
    # of the data
    [aok,elog,stime,etime] = validate_ensemble(ensemble)
    if aok:
        if stime > xcorwin.start or etime < xcorwin.end:
            message = "Correlation window defined is not consistent with input ensemble\n"
            message += "Estimated ensemble time span is {} to {}\n".format(stime,etime)
            message += "Correlation window time span is {} to {}\n".format(xcorwin.start,xcorwin.end)
            message += "Correlation window range must be inside the data range"
            # we don't use the windows_extracted_from_metadata boolean and never throw 
            # an exception in this case because data range depends upon each ensemble 
            # so there is not always fail case
            beam.elog.log_error(alg,message,ErrorSeverity.Invalid)
            beam.kill()
            return [ensemble,beam]
    else:
        beam.elog += elog
        beam.kill()
        return [ensemble,beam]
    # need this repeatedly so set it
    N_members = len(ensemble.member)
    output_stack = TimeSeries(beam)
    if output_stack_window:
        output_stack.set_t0(output_stack_window.start)
        npts = int((output_stack_window.start-output_stack_window.end)/beam.dt) + 1
        # this algorithm depends on the data array be initialized to zeros 
        # with this step
        output_stack.set_npts(npts)
    else:
        t0values=[]
        etimevalues=[]
        for i in range(N_members):
            d = ensemble.member[i]
            t0values.append(d.t0)
            etimevalues.append(d.endtime())
        stime = np.max(t0values)
        etime = np.min(etimevalues)
        output_stack_window = TimeWindow(stime,etime)
        output_stack.set_t0(stime)
        npts = int((etime-stime)/beam.dt) + 1
        output_stack.set_npts(npts)
    # need to build this internal reference as algorithm will
    # discard dead data in processing for efficiency
    for i in range(N_members):
        if ensemble.member[i].live:
            ensemble.member[i][ensemble_index_key] = i
    # this could be updated on each iteration but the assumption 
    # here is that the shifts are a small fraction of the window length 
    # used for correlation
    xcorens = WindowData(ensemble, xcorwin.start, xcorwin.end)

    initial_starttimes = np.zeros(N_members)
    for i in range(N_members):
        if xcorens.member[i].live:
            # assume algorithm below does not do resurrection of the dead
            initial_starttimes[i] = xcorens.member[i].t0
    rbeam0=WindowData(beam,rwin.start,rwin.end)
    nrm_rbeam = np.linalg.norm(rbeam0.data)
    for i in range(MAXITERATION):
        xcorens = beam_align(xcorens,beam,time_shift_limit=time_shift_limit)
        print("shifts computed iteration ",i)
        for j in range(len(initial_starttimes)):
            print(j,xcorens.member[j].t0,initial_starttimes[j],
                    xcorens.member[j].t0-initial_starttimes[j],
                  )
        # pad option is essential here to avoid killing data 
        # hitting window edges - large padding will likely strongly 
        # penalize that datum
        rens = WindowData(xcorens,
                          rwin.start, 
                          rwin.end,
                          short_segment_handling='pad')
        # this clones the Metadata of beam for the output
        rbeam = robust_stack(rens,stack0=rbeam0,method=robust_stack_method)
        delta_rbeam = rbeam - rbeam0
        nrm_delta = np.linalg.norm(delta_rbeam.data)
        if nrm_delta/nrm_rbeam < convergence:
            break
        rbeam0 = rbeam
    if i>=MAXITERATION:
        output_stack=rbeam
        rbeam.kill()
        message = "robust_stack iterative loop did not converge"
        rbeam.elog.log_error(alg,message,ErrorSeverity.Invalid)
        return [ensemble,rbeam]
    
    # apply time shifts to original ensemble that we will return 
    # and set the value for the attribute defined by "time_shift_key" 
    # argument.   This has to be done here so we can properly cut the 
    # window to be stacked
    for i in range(len(rens.member)):
        d = rens.member[i]
        if d.live:
            j = d[ensemble_index_key]
            tshift = xcorens.member[i].t0 - initial_starttimes[i]
            #tshift =  initial_starttimes[i] - xcorens.member[i].t0
            # this shift maybe should be optional
            # a positive lag from xcor requires a negative shift 
            # for a TimeSeries object  
            ensemble.member[i].shift(-tshift)
            # not this posts the lag not the origin shift which is 
            # the minus of the lag
            ensemble.member[i].put_double(time_shift_key,tshift)
    
    if robust_stack_method=="dbxcor":
        # We need to post the final weights to all live members
        wts = dbxcor_weights(rens,rbeam)
        # these need to be normalized so sum is 1 to simplify array stack below
        sum_live_wts=0.0
        for w in wts:
            # dbxcor_weights will flag dead data with a negative weight
            if w>0.0:
                sum_live_wts += w
        for i in range(len(wts)):
            # WindowData may kill with large shifts so 
            # we need the live test here
            if wts[i]>0.0 and rens.member[i].live:
                wts[i] /= sum_live_wts
        for i in range(len(rens.member)):
            # use shorthand since we aren't altering rens in this loop
            d = rens.member[i]
            if d.live:
                j = d[ensemble_index_key]
                ensemble.member[j].put_double(robust_weight_key,wts[i])
                d2stack=WindowData(ensemble.member[i],
                                   output_stack_window.start,
                                   output_stack_window.end,
                                   short_segment_handling="truncate",
                                   log_recoverable_errors=False,
                                   )
                # important assumption is that the weights are normalized 
                # so sum of wts is 1
                d2stack.data *= wts[i]
                # TimeSeries opertor+= handles irregular windows treating them 
                # like zero padding and truncating anything outside range of lhs
                output_stack += d2stack
            else:
                # need to set this because we don't kill 
                # the input member but the one detected in stacking
                ensemble.member[j].put_double(robust_weight_key,0.0)
        output_stack.set_live()
    elif robust_stack_method == "median":
        # recompute the median stack from the aligned (live) data cut to the output_stack_window range
        nlive = 0
        # note sizes passed to shape are set above when validating inputs and are assumed to not have changed
        Npts = output_stack.npts
        gather_matrix = np.zeros(shape=[N_members,Npts])
        for d in ensemble.member:
            # always use the zero padding method of WindowData 
            # run silently for median stack.  Maybe should allow 
            # options for this case
            dcut = WindowData(d,
                              output_stack_window.start,
                              output_stack_window.end,
                              short_segment_handling="pad",
                              log_recoverable_errors=False,
                              )
            if dcut.live:
                # rounding effects with a window time range 
                # iteractions with t0 can cause the size of 
                # dcut to different from output_stack.npts
                # logic handles that
                if dcut.npts == Npts:
                    gather_matrix[nlive,:] = np.array(dcut.data)
                elif dcut.npts < Npts:
                    gather_matrix[nlive,0:dcut.npts] = np.array(dcut.data)
                else:
                    gather_matrix[nlive,:] = np.array(dcut.data[0:Npts])    
                nlive += 1
        stack_vector = np.median(gather_matrix[0:nlive,:],axis=0)
        # this matrix could be huge so we release it as quickly as possible
        del gather_matrix
        output_stack.data = DoubleVector(stack_vector)
        output_stack.set_live()

    else:
        raise RuntimeError("robust_stack_method illegal value altered during run - this should not happen and is a bug")
    # TODO:  may want to always or optionally window ensemble output to output_stack_window
    return [ensemble,output_stack]
        
