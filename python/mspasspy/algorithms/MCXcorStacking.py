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
                                    SeismogramEnsemble,
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

# TODO:   this  functions following belong somewhere else in some utility 
# module
def number_live(ensemble)->int:
    """
    Scans an ensemble and returns the number of live members.  If the 
    ensemble is marked dead it immediately return 0.  Otherwise it loops 
    through the members countinng the number live.
    :param ensemble:  ensemble to be scanned
    :type ensemble:  Must be a `TimeSeriesEnsemble` or `SeismogramEnsemble` or 
    it will throw a TypeError exception. 
    """
    if not isinstance(ensemble,(TimeSeriesEnsemble,SeismogramEnsemble)):
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
    
def regularize_sampling(ensemble,dt_expected,Nsamp=10000,abort_on_error=False):
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
    discussed above.   (Default 10000) 
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
    delta_dt_cutoff = dt_expected/(2.0*(Nsamp - 1))
    for i in range(len(ensemble.member)):
        d = ensemble.member[i]
        if d.live:
            if not np.isclose(dt_expected,d.dt):
                if abs(dt_expected-d.dt) > delta_dt_cutoff:
                    message = "Member {} of input ensemble has different sample rate than expected constant dt\n",format(i)
                    message += "Expected dt={} but this datum has dt={}\n".format(expected_dt,d.dt)
                    if abort_on_error:
                        raise ValueError(message)
                    else:
                        ensemble.member[i].elog.log_error(alg,message,ErrorSeverity.Invalid)
                        ensemble.member[i].kill()
    if not abort_on_error:
        nlive = number_live(ensemble)
        if nlive <= 0:
            message = "All members of this ensemble were killed.\n"
            messagge += "expected dt may be wrong or you need to run the resample function on this data set"
            ensemble.kill()
    return ensemble

def ensemble_time_range(ensemble,metric="inner")->TimeWindow:
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
    :return:  `TimeWindow` object with start and end times.
    """
    if not isinstance(ensemble,(TimeSeriesEnsemble,SeismogramEnsemble)):
        message = "ensemble_time_range:   illegal type for arg0={}\n".format(type(ensemble))
        message += "Must be a TimeSeriesEnsemble or SeismogramEnsemble\n"
        raise TypeError(message)
    if metric not in ["inner","outer","median","mean"]:
        message = "ensemble_time_range:  illegal value for metric={}\n".format(metric)
        message += "Must be one of:  inner, outer, median, or mean"
        raise ValueError(message)
    stvector=[]
    etvector=[]
    for d in ensemble.member:
        if d.live:
            stvector.append(d.t0)
            etvector.append(d.endtime())
    if metric=="inner":
        stime = max(stvector)
        etime = min(etvector)
    elif metric == "outer":
        stime =  max(stvector)
        etime = min(etvector)
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
    return TimeWindow(stime,etime)

def regularize_ensemble(ensemble,starttime,endtime,fractional_mistmatch_limit)->TimeSeriesEnsemble:
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
                d = WindowWithCutoff(d,
                               starttime,
                               endtime,
                               fractional_mismatch_limit=fractional_mistmatch_limit,
                               )
                if d.live:
                    message = "Dropped member number {} because undefined data range exceeded limit of {}\n".format(i,fractional_mistmatch_limit)
                    ensout.member.append(d)
            else:
                ensout.member.append(d)
        else:
            message = "Dropped member number {} that was marked dead on input".format(i)
            ensout.elog.log_error("regularize_ensemble",message,ErrorSeverity.Complaint)
    if len(ensout.member)>0:
        ensout.set_live()
    return ensout

# TODO;  this should go in window.py
def WindowWithCutoff(d,stime,etime,fractional_mismatch_limit=0.05):
    """
    Windows an atomic data object with automatic padding if the 
    undefined section is not too large.  
    
    When using numpy or MsPASS data arrays the : notation can be used 
    to drastically speed performance over using a python loop.  
    This function is most useful for contexts here the size of the 
    output of a window must exactly match what is expected from 
    the time range.   The algorithm will silently zero pad any 
    undefined samples IF the fraction of undefined data relative to 
    the number of samples expected from the time range defined by 
    etime-stime is less than the "fractional_mismatch_limit".  
    
    :param d:  atomic MsPASS seismic data object to be windowed. 
    :type d:  works with either `TimeSeries` or `Seismogram` 
    objects.
    :param stime:  start time of window range 
    :type stime:  float
    :param etime:  end time of window range
    :type etime:  float
    :param fractional_mismatch_limit:  limit for the 
    fraction of data with undefined values before the datum is
    killed.   (see above)  If set to 0.0 this is an expensive way 
    to behave the same as WindowData
    :return:  object of the same type as d.   Will be marked dead 
    if the fraction of undefined data exceeds fractional_mismatch_limit.
    Otherwise will return a data vector of constant size that may 
    be padded.  This function never leaves an error log message. 
    """
    N_expected = round((etime-stime)/d.dt) + 1
    dw = WindowData(d,stime,etime)
    if dw.dead():
        # assumes default kills if stime and etime are not with data bounds
        dw = WindowData(d,stime,etime,short_segment_handling='truncate')
        if dw.npts/N_expected < fractional_mismatch_limit:
            dw = WindowData(d,stime,etime,short_segment_handling="pad")
        else:
            dw.kill()
    return dw
    
    
def robust_stack(ensemble,
                 method='dbxcor',
                 stack0=None,
                 stack_md=None,
                 timespan_method="ensemble_inner",
                 fractional_mismatch_limit=0.05,
                 )->TimeSeries:
    """
    Generic function for robust stacking live members of a `TimeSeriesEnsemble`.
    An optional initial stack estimate can be used via tha stack0 argument. 
    The function currently supports two methods:  "median" for a median 
    stack and "dbxcor" to implement the robust loss function 
    used in the dbxcor program defined in Pavlis and Vernon (2010).
    Other algorithms could easily be implemented via this same api 
    by adding an option for the "method" argument.
    
    All robust estimators I am aware of that use some form of penalty 
    function (e.g. m-estimators or the dbxcor penalty function) require 
    an initial estimator for the stack.  They do that because the 
    penalty function is defined from a metric of residuals relative to 
    the current estimate of center.   The median, however, does not 
    require an initial estimator which complicates the API for this function. 
    For the current options understand that stack0 is required for 
    the dbxcor algorithm but will be ignored if median is requested.  
    
    The other complication of this function is handling of potential 
    irregular time ranges of the ensemble input and how to set the 
    time range for the output.   The problem is further complicated by 
    use in an algorithm like `align_and_stack` in this module where 
    the data can get shifted to have undefined data within the 
    time range the data aims to utilize.   The behavior of the 
    algorithm for this issue is controlled by the kwarg values 
    with the keys "timespan_method" and "fractional_mismatch_limit".  
    As the name imply "timespan_method" defines how the time span 
    for the stack should be defined.   The following options 
    are supported:
    
    "stack0" - sets the time span to that of the input 
    `TimeSeries` passed as stack0.  i.e. the range is set to 
    stack0.dt to stack0.endtime().
    
    "ensemble_inner" - (default) use the range defined by the "inner" method 
    for computing the range with the function `ensemble_time_range`. 
    (see `ensemble_time_range` docstring for the definition).
    
    "ensemble_outer" - use the range defined by the "outer" method 
    for computing the range with the function `ensemble_time_range`. 
    (see `ensemble_time_range` docstring for the definition).
    
    "ensemble_median" -  use the range defined by the "median" method 
    for computing the range with the function `ensemble_time_range`. 
    (see `ensemble_time_range` docstring for the definition).
    
    These interact with the value passed via "fractional_mismatch_level". 
    When the time range computed is larger than the range of a particular 
    member of the input ensemble this parameter determines whether or not 
    the member will be used in the stack.  If the fraction of 
    undefined samples (i.e. N_undefined/Nsamp) is greater than this cutoff 
    that datum will be ignored.   Otherwise if there are undefined 
    values they will be zero padded.   
    
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
    :type stack_md:  Metadata container or None.  If stack0 is defined 
    this argument is ignored.   Otherwise it should be used to add 
    whatever Metadata is required to provide a tag that can be used to 
    identify the output.  If not specified the stack Metadata 
    will be only those produce from default construction of a 
    TimeSeries.  That is almost never what you want.   Reiterate, 
    however, that if stack0 is defined the output stack will be a 
    clone of stack0 with possible modifications of time and data 
    range attributes and anything the stack algorithm posts.
    """
    alg = "robust_stack"
    # if other values for method are added they need to be added here
    if method not in ["median","dbxcor"]:
        message = alg + ":  Illegal value for argument method={}\n".format(method)
        message += "Currently must be either median or dbxcor"
        raise ValueError(message)
    # don't test type - if we get illegal type let it thro0w an exception
    if ensemble.dead():
        return ensemble
    if timespan_method == "stack0":
        if stack0:
            # intentionally don't test type of stack0 
            # if not a TimeSeries this will throw an exception
            timespan = TimeWindow(stack0.t0,stack0.endtime())
        else:
            message = alg + ":  usage error\n"
            message += "timespan_method was set to stack0 but the stack0 argument is None\n"
            message += "stack0 must be a TimeSeries to use this option"
            raise ValueError(message)
    elif timespan_method == "ensemble_inner":
        timespan = ensemble_time_range(ensemble,metric="inner")
    elif timespan_method == "ensemble_outer":
        timespan = ensemble_time_range(ensemble,metric="outer")
    elif timespan_method == "ensemble_median":
        timespan = ensemble_time_range(ensemble,metric="median")
    else:
        message = alg + ":  illegal value for argument ensemble_metric={}".format(ensemble_metric)
        raise ValueError(message)
    
    ensemble = regularize_ensemble(ensemble,timespan.start,timespan.end,fractional_mismatch_limit)
    # the above can remove some members 
    M = len(ensemble.member)
    # can now assume they are all the same length and don't need to worry about empty ensembles
    N = ensemble.member[0].npts

    if stack0:
        stack = WindowWithCutoff(stack0,
                                 timespan.start,
                                 timespan.end,
                                 fractional_mismatch_limit=fractional_mismatch_limit,
                                 )
        if stack.dead():
            message = "Received an initial stack estimate with time range inconsistent with data\n"
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
                    output_stack_window=None, 
                    robust_weight_key='robust_stack_weight',
                    time_shift_key='arrival_time_correction',
                    time_shift_limit=2.0,
                    abort_irregular_sampling=False,    
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
    This function uses a pythonic approach aimed to allow this function
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
            first tests if `correlation_window` is set and is an 
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
        4.  dbxcor had a final stage that required picking the arrival time 
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
        1.  Irregular sample intervals of live data.
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
    
    :param time_shift_limit:
    :param  abort_irregular_sampling: boolean that controls error 
    handling of data with irregular sample rates.  This function uses 
    a generous test for sample rate mismatch.  A mismatch is 
    detected only if the computed time skew over the time span of 
    the input beam signal is more than 1/2 of the beam sample interval 
    (beam.dt).  When set true the function will 
    abort with a ValueError exception if any ensemble member fails the 
    sample interval test.  If False (the default) offending ensemble
    members are killed and a message is posted.  Note the actual 
    ensemble is modified so the return may have fewer data live 
    than the input when this mode is enabled. 
    :type abort_irregular_sampling:  boolean
    
    :return: tuple with 0 containing the original ensemble but time 
    shifted by cross-correlation.   Failed/discarded signals for the 
    stack are not killed but should be detected by not having the 
    time shift Metadata value set.   component 1 is the computed 
    stack windowed to the range defined by `stack_time_window`.
    """
    alg="align_and_stack"
    # xcor ensemble has the initial start time posted to each 
    # member using this key - that content goes away because 
    # xcorens has function scope
    it0_key = "_initial_t0_value_"
    ensemble_index_key="_ensemble_i0_"
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
    ensemble = regularize_sampling(ensemble,beam.dt,Nsamp=beam.npts)
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
    ensemble_timespan = ensemble_time_range(ensemble,metric="median") 
    if ensemble_timespan.start > xcorwin.start or ensemble_timespan.end < xcorwin.end:
        message = "Correlation window defined is not consistent with input ensemble\n"
        message += "Estimated ensemble time span is {} to {}\n".format(ensemble_timespan.start,ensemble_timespan.end)
        message += "Correlation window time span is {} to {}\n".format(xcorwin.start,xcorwin.end)
        message += "Correlation window range must be inside the data range"
        # we don't use the windows_extracted_from_metadata boolean and never throw 
        # an exception in this case because data range depends upon each ensemble 
        # so there is not always fail case
        beam.elog.log_error(alg,message,ErrorSeverity.Invalid)
        beam.kill()
        return [ensemble,beam]
    # need this repeatedly so set it
    N_members = len(ensemble.member)
    
    if output_stack_window:
        # this will clone the beam trace metadata automatically
        # using pad option assures t0 will be output_stack_window.start
        # and npts is consistent with window requested
        output_stack = WindowData(beam,output_stack_window.start,output_stack_window.end,short_segment_handling="pad")
        # this is an obscure but fast way to initialize the data vector to all 0s
        output_stack.set_npts(output_stack.npts)
    else:
        # also clones beam metadata but in this case we get the size from the ensemble time span
        output_stack = TimeSeries(beam)
        output_stack_window = TimeWindow(ensemble_timespan)
        output_stack.set_t0(output_stack_window.start)
        npts = int((output_stack_window.end - output_stack_window.start)/beam.dt) + 1
        output_stack.set_npts(npts)
    # the loop below builds cross-referencing index positions 
    # stored in the windowed ensemble's metadata container 
    # with the key defined by ensemble_index_key
    # note that baggage is used to unscramble xcorens later but 
    # does not appear in the output ensemble derived form the 
    # content of the "ensemble" symbol   
    xcorens = TimeSeriesEnsemble(Metadata(ensemble),N_members)
    for i in range(N_members):
        if ensemble.member[i].live:
            d = WindowData(ensemble.member[i],
                           xcorwin.start,
                           xcorwin.end,
                           )
            if d.live:
                d[ensemble_index_key] = i
                xcorens.member.append(d)
    xcorens.set_live()
    nlive = number_live(xcorens)
    if nlive==0:
        message = "WindowData with range {} to {} killed all members\n".format(xcorwin.start,xcorwin.end)
        message += "All members have time ranges inconsistent with that cross-correlation window"
        ensemble.elog.log_error(alg,message,ErrorSeverity.Invalid)
        ensemble.kill()
        return [ensemble,beam]
    # We need this Metadata posted to sort out total time
    # shifts needed for arrival time estimates
    for i in range(len(xcorens.member)):
        xcorens.member[i].put_double(it0_key,
                                     xcorens.member[i].t0)

    # above guarantees this cannot return a dead datum
    rbeam0=WindowData(beam,rwin.start,rwin.end)
    nrm_rbeam = np.linalg.norm(rbeam0.data)
    for i in range(MAXITERATION):
        xcorens = beam_align(xcorens,beam,time_shift_limit=time_shift_limit)
        rens = WindowData(xcorens,
                          rwin.start,rwin.end,
                          short_segment_handling='pad')
        # this clones the Metadata of beam for the output
        rbeam = robust_stack(rens,
                             stack0=rbeam0,
                             method=robust_stack_method,
                             timespan_method="stack0",
                             )
        delta_rbeam = rbeam - rbeam0
        nrm_delta = np.linalg.norm(delta_rbeam.data)
        if nrm_delta/nrm_rbeam < convergence:
            break
        rbeam0 = rbeam
        nrm_rbeam = np.linalg.norm(rbeam0.data)
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
    for i in range(len(xcorens.member)):
        d = xcorens.member[i]
        # this test is needed in case the processing above 
        # killed one of the members of xcorens.  That can 
        # happen a number of ways.  Note the index cross reference 
        # in xcorens may not match that in enemble
        if d.live:
            initial_starttime = xcorens.member[i][it0_key]
            tshift = d.t0 - initial_starttime
            j = d[ensemble_index_key]
            # this shift maybe should be optional
            # a positive lag from xcor requires a negative shift 
            # for a TimeSeries object  
            ensemble.member[j].shift(-tshift)
            # not this posts the lag not the origin shift which is 
            # the minus of the lag
            ensemble.member[j].put_double(time_shift_key,tshift)
    
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
        
