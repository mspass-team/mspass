import numpy as np
import mspasspy.ccore as mspass
def ensemble_error_post(d,alg,message):
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
    (Note due to a current flaw in the api we don't have access to the 
    severity attribute.  For now this always set it Invalid)
    """
    if(isinstance(d,mspass.TimeSeriesEnsemble) 
           or isinstance(d,mspass.SeismogramEnsemble)):
        n=len(d.member)
        if(n<=0):
            return
        for i in range(n):
            d.member[i].elog.log_error(alg,str(message),mspass.ErrorSeverity.Invalid)
    else:
        print('Coding error - ensemble_error_post was passed an unexpected data type of',
              type(d))
        print('Not treated as fatal but a bug fix is needed')
def scale(d,method='peak',level=1.0,window=None,scale_by_section=False,use_mean=False,
          preserve_history=False,instance=None,dryrun=False):
    """
    Top level function interface to data scaling methods.
    
    This function can be used to scale seismic data contained in any of
    the four seismic data objects defined in mspass:  TimeSeries, Seismogram,
    TimeSeriesEnsemble, and SeismogramEnsemble.   An extensive set of 
    amplitude estimation metrics are available by selecting one of the 
    allowed values for the method parameter.   Ensembles can be scaled 
    at the individual seismogram level or as a whole (scale_by_section=True).
    Object level history can be enabled by setting preserve_history true.  
    See parameter descriptions for additional features for history preservation.
    
    Note all methods preserve the original amplitude by updating the 
    Metadata parameter calib to include the scaling.  i.e. as always the 
    amplitude of the data in physical units can be restored by multiplying 
    the data samples by calib. 
    
    :param d:  is input data object.  If not one of the four mspass seismic
      data types noted above the function will throw a RuntimeError exception.
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
      Default is peak.
    :param level:   For all but perc this defines the scale to which the data
      are scaled.  For perc it is used to set the percent clip level.  
      A RuntimeError exception is thrown if method is perc and level is larger 
      that one. Default is 1.0
    :param window: is an optional mspass.TimeWindow applied to compute amplitude 
      for scaling.  When not a null (python None) a windowing algorithm will 
      be applied before computing the amplitude metric and the amplitude 
      computed in that window will be used for scaling.   Default is None 
      meaning this option is turned off and the entire waveform segment is 
      scanned for the amplitude estimation.
    :param scale_by_section:  is a boolean that controls the scaling 
      behavior on ensembles only (It is silently ignored for atomic 
      TimeSeries and Seismogram data).  When true a single gain factor is 
      applied to all members of an ensemble.  When false each member is 
      gained individually as if this function were applied in a loop to 
      each member.
    :use_mean:  boolean used only for ensembles and when scale_by_section is 
      True.   The algorithm used in that case has an option to use the mean 
      log amplitude for scaling the section instead of the default median
      amplitude.
    :param preserve_history:
    :param instance:  preserve_history and instance are intimately related 
      and control how object level history is handled.  Object level history 
      is disabled by default for efficiency.  If preserve_history is set True 
      and the string passed as instance is defined (not None which is the default)
      each Seismogram or TimeSeries object will attempt to save the history 
      through a new_map operation.   If the history chain is empty this will
      silently generate an error posted to error log on each object. 
    :param dryrun: is a boolean used for preprocessing to validate 
      arguments.  When true the algorithm is not run, but the function
      only checks the argument list for invalid combinations.  This is 
      useful for prerun checks of a large job to validate a workflow.  
      Errors generate exceptions but the function returns before 
      attemping any calculations.  Default is false
     
    :return: amplitude(s) in a python array.   Array has only one element 
      for all returns except ensembles when scale_by_section is False.
    :rtype: For atomic objects returns the computed amplitude for the 
      data received through d and sets that value as array element 0.  
      For ensembles returns a pybind11 wrapped 
      std::vector of amplitudes for the ensemble members when scale_by_section 
      is false (member by member scaling) but only a single number in element 0
      for section scaling.   An empty return is possible without an exception 
      being thrown if a single atomic data type is marked dead or if the 
      ensemble received has no data.  
    
    """
    algname="scale"
    # First validate arguments 
    if( not ( method=='peak' or method=='RMS' or method=='rms' or method=='perc'
             or method=='MAD' or method=='mad')):
        raise RuntimeError(algname+":  method parameter passed = ",method," is not valid"
            + "Should be peak, rms, perc, or mad")
    if(method=='perc'):
        if(level<0 or level>1.0):
            raise RuntimeError(algname+":  level parameter passed=",
                level," is illegal.  Must be between 0 and 1")
    else:
        if(level<=0.0):
            raise RuntimeError(algname+":  level argument received has illegal value=",
                level," level must be a positive number")
    if(window!=None):
        if(not isinstance(window,mspass.TimeWindow)):
            raise RuntimeError(algname
              +":  optional window parameter set but data passed is not a mspass.TimeWindow object")
    if(preserve_history):
        if(instance==None):
            raise RuntimeError(algname
              + ":  preserve_history was set true but instance parameter was not defined")
    if(dryrun):
        return 'ok'
    # The pybind11 and C++ way of defining an enum class creates an 
    # obnoxiously ugly syntax. We insulate the user from this oddity 
    # by using a string arg to define this enum passed to _scale
    method_to_use=mspass.ScalingMethod.Peak
    if(method=='rms' or method=='RMS'):
        method_to_use=mspass.ScalingMethod.RMS
    elif(method=='perc'):
        method_to_use=mspass.ScalingMethod.perc
    elif(method=='MAD' or method=='mad'):
        method_to_use=mspass.ScalingMethod.MAD
    # else not needed due to tests above 
    # Note the large block from here on may need an error handler to 
    # avoid global aborts.   Maybe only the caller needs a handler
    ampvec=[]   # needed to allow ampvec to be the return
    try:
        if(window==None):
            if(isinstance(d,mspass.TimeSeries)):
            # Silently return 0 if marked dead
                if(d.dead()):
                    return ampvec
                else:
                    amp=mspass._scale(d,method_to_use,level)
                    ampvec.append(amp)
                if(preserve_history):
                    if(d.is_empty()):
                        d.elog.log_error(algname
                         +": cannot preserve history because container was empty\nMust at least contain an origin record")
                    else:
                        d.new_map(algname,instance,mspass.AtomicType.TIMESERIES,
                              mspass.ProcessingStatus.VOLATILE)
            elif(isinstance(d,mspass.Seismogram)):
            # Silently return 0 if marked dead
                if(d.dead()):
                    return ampvec
                else:
                    amp=mspass._scale(d,method_to_use,level)
                    ampvec.append(amp)
                if(preserve_history):
                    if(d.is_empty()):
                        d.elog.log_error(algname,
                         "cannot preserve history because container was empty\nMust at least contain an origin record",
                         mspass.ErrorSeverity.Complaint)
                    else:
                        d.new_map(algname,instance,mspass.AtomicType.SEISMOGRAM,
                              mspass.ProcessingStatus.VOLATILE)
            elif(isinstance(d,mspass.TimeSeriesEnsemble) 
                or isinstance(d,mspass.SeismogramEnsemble)):
                if(len(d.member)<=0):  # Silently return nothing if the ensemble is empy
                    return ampvec
                if(scale_by_section):
                    amp=mspass._scale_ensemble(d,method_to_use,level,use_mean)
                    ampvec.append(amp)
                else:
                    ampvec=mspass._scale_ensemble_members(d,method_to_use,level)
                if(preserve_history):
                    n=len(d.member)
                    for i in range(n):
                    # Silently do nothing if the data are marked dead
                        if(d.member[i].live):
                            if(d.member[i].is_empty()):
                                d.member[i].elog.log_error(algname,
                                  "cannot preserve history because container was empty\nMust at least contain an origin record",
                                  mspass.ErrorSeverity.Complaint)
                            else:
                                if(isinstance(d.member[i],mspass.Seismogram)):
                                    d.member[i].new_map(algname,instance,mspass.AtomicType.SEISMOGRAM,
                                            mspass.ProcessingStatus.VOLATILE)
                                else:
                                    d.member[i].new_map(algname,instance,mspass.AtomicType.TIMESERIES,
                                            mspass.ProcessingStatus.VOLATILE)
            else:
                raise RuntimeError("scale:  input data is not a supported mspass seismic data type")
        else:
            print("scale function:  Windowed scaling option not yet supported")
        # A python shortcoming is it is far from obvious from indents that 
        # all nonerror states land here and return this value
        return ampvec
    except RuntimeError as err:
        if( isinstance(d,mspass.Seismogram) 
                    or isinstance(d,mspass.TimeSeries) ):
            # This shows an api problem.  MsPaSSErrors are cast to 
            # RuntimeErrors and lose access to the ErrorSeverity attribute
            # We need to implement a custom exception for MsPASSError 
            # as described in pybind11 documentation. This line
            # should be fixed when that is done
            d.elog.log_error(algname,str(err),mspass.ErrorSeverity.Invalid)
        # avoid further isinstance at the expense of a maintenance issue.
        # if we add any other supported data objects we could have a 
        # problem here.  This assumes what lands here is an ensemble
        else:
            ensemble_error_post(d,algname,err)
    # this is needed to handle an oddity recommended on this
    # web site:  http://effbot.org/zone/stupid-exceptions-keyboardinterrupt.htm
    except KeyboardInterrupt:
        raise
    except:
        message="Something threw an unexpected exception\nThat is a bug that needs to be fixed - contact authors"
        if( isinstance(d,mspass.Seismogram) or isinstance(d,mspass.TimeSeries) ):
          d.elog.log_error(algname,message,mspass.ErrorSeverity.Invalid)
        else:
          ensemble_error_post(d,algname,message,mspass.ErrorSeverity.Invalid) 
        
def WindowData(d,twin,t0shift=None,preserve_history=False,instance=None):
    """
    Cut data defined by a TimeWindow object.
    
    Cutting a smaller waveform segment from a larger waveform segment 
    is a very common seismic data processing task.   The function is 
    a python wrapper to adapt C++ code that accomplishes that task 
    to the MsPASS framework. 
    
    Note this function uses one model for being bombproof with a map 
    operation.  Any exception that makes the result invalid will cause
    an error message to be posted to the elog attribute of the input 
    datum AND the function returns a None.   Callers should test for 
    validity of output before trying to use the result or something else
    will almost certainly abort the job downstream. 
    
    :param d: is the input data.  d must be either a TimeSeries or Seismogram 
      object or the function will log an error to d and return a None.
    :param twin: is a mspass.TimeWindow defining window to be cut
    :param t0shift: is an optional time shift to apply to the time window. 
      This parameter is convenient to avoid conversions to relative time. 
      A typical example would be to set t0shift to an arrival time and let 
      the window define time relative to that arrival time.  Default is None
      which cause the function to assume twin is to be used directly.  
    :param preserve_history: boolean to enable or disable saving object 
      level history.  Default is False.   Note if this feature is enabled
      and the input data history chain is empty the function will log an 
      error to the returned data and not update the history chain.
    :param instance: is instance key to pass to processing history chain.
      If None (the default) and preserve_history is true the processing
      history chain will not be updated and a complaint error posted to 
      d.elog.   
     
    :return: copy of d with sample range reduced to twin range.  Returns 
      an empty version of the parent data type (default constructor) if 
      the input is marked dead
    """
    twcut=mspass.TimeWindow(twin)
    if(t0shift!=None):
        twcut.shift(t0shift)
    try:
        if(isinstance(d,mspass.TimeSeries)):
            if(d.dead()):
                return mspass.TimeSeries()
            dcut=mspass._WindowData(d,twcut)
            if(preserve_history):
                if(instance==None):
                    dcut.elog.log_error("WindowData",
                      "Undefined instance argument - cannot save history data",
                      mspass.ErrorSeverity.Complaint)
                elif(dcut.is_empty()):
                    dcut.elog.log_error("WindowData",
                     "Error log is empty.  Cannot be extended without a top level entry",
                     mspass.ErrorSeverity.Complaint)
                else:
                    dcut.new_map("WindowData",instance,mspass.AtomicType.TIMESERIES,
                              mspass.ProcessingStatus.VOLATILE)
            return dcut
        elif(isinstance(d,mspass.Seismogram)):
            if(d.dead()):
                return mspass.Seismogram()
            dcut=mspass._WindowData3C(d,twcut)
            if(preserve_history):
                if(instance==None):
                    d.elog("WindowData",
                      "Undefined instance argument - cannot save history data",
                      mspass.ErrorSeverity.Complaint)
                elif(dcut.is_empty()):
                    dcut.elog("WindowData",
                     "Error log is empty.  Cannot be extended without a top level entry",
                     mspass.ErrorSeverity.Complaint)
                else:
                    dcut.new_map("WindowData",instance,mspass.AtomicType.SEISMOGRAM,
                              mspass.ProcessingStatus.VOLATILE)
            return dcut
        else:
            raise RuntimeError("WindowData:  Invalid input data type received")
    except RuntimeError as err:
        d.log_error("WindowData",str(err),mspass.ErrorSeverity.Invalid)
        return None

