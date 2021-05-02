import numpy as np

from mspasspy.ccore.utility import MsPASSError, AtomicType, ErrorSeverity, ProcessingStatus
from mspasspy.ccore.seismic import Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble, TimeWindow,_TopMute
from mspasspy.ccore.algorithms.basic import _WindowData, _WindowData3C
from mspasspy.ccore.algorithms.amplitudes import _scale, _scale_ensemble ,_scale_ensemble_members, ScalingMethod

def ensemble_error_post(d,alg,message, severity):
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
    if(isinstance(d,TimeSeriesEnsemble)
           or isinstance(d,SeismogramEnsemble)):
        n=len(d.member)
        if(n<=0):
            return
        for i in range(n):
            d.member[i].elog.log_error(alg,str(message), severity)
    else:
        print('Coding error - ensemble_error_post was passed an unexpected data type of',
              type(d))
        print('Not treated as fatal but a bug fix is needed')
def scale(d,method='peak',level=1.0,window=None,scale_by_section=False,use_mean=False,
          object_history=False,instance=None,dryrun=False):
    """
    Top level function interface to data scaling methods.

    This function can be used to scale seismic data contained in any of
    the four seismic data objects defined in mspass:  TimeSeries, Seismogram,
    TimeSeriesEnsemble, and SeismogramEnsemble.   An extensive set of
    amplitude estimation metrics are available by selecting one of the
    allowed values for the method parameter.   Ensembles can be scaled
    at the individual seismogram level or as a whole (scale_by_section=True).
    Object level history can be enabled by setting object_history true.
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
    :param window: is an optional TimeWindow applied to compute amplitude
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
    :param use_mean:  boolean used only for ensembles and when scale_by_section is
      True.   The algorithm used in that case has an option to use the mean
      log amplitude for scaling the section instead of the default median
      amplitude.
    :param object_history:
    :param instance:  object_history and instance are intimately related
      and control how object level history is handled.  Object level history
      is disabled by default for efficiency.  If object_history is set True
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
        if(not isinstance(window,TimeWindow)):
            raise RuntimeError(algname
              +":  optional window parameter set but data passed is not a TimeWindow object")
    if(object_history):
        if(instance==None):
            raise RuntimeError(algname
              + ":  object_history was set true but instance parameter was not defined")
    if(dryrun):
        return 'ok'
    # The pybind11 and C++ way of defining an enum class creates an
    # obnoxiously ugly syntax. We insulate the user from this oddity
    # by using a string arg to define this enum passed to _scale
    method_to_use=ScalingMethod.Peak
    if(method=='rms' or method=='RMS'):
        method_to_use=ScalingMethod.RMS
    elif(method=='perc'):
        method_to_use=ScalingMethod.perc
    elif(method=='MAD' or method=='mad'):
        method_to_use=ScalingMethod.MAD
    # else not needed due to tests above
    # Note the large block from here on may need an error handler to
    # avoid global aborts.   Maybe only the caller needs a handler
    ampvec=[]   # needed to allow ampvec to be the return
    try:
        if(window==None):
            if(isinstance(d,TimeSeries)):
            # Silently return 0 if marked dead
                if(d.dead()):
                    return ampvec
                else:
                    amp=_scale(d,method_to_use,level)
                    ampvec.append(amp)
                if(object_history):
                    if(d.is_empty()):
                        d.elog.log_error(algname
                         +": cannot preserve history because container was empty\nMust at least contain an origin record")
                    else:
                        d.new_map(algname,instance,AtomicType.TIMESERIES,
                              ProcessingStatus.VOLATILE)
            elif(isinstance(d,Seismogram)):
            # Silently return 0 if marked dead
                if(d.dead()):
                    return ampvec
                else:
                    amp=_scale(d,method_to_use,level)
                    ampvec.append(amp)
                if(object_history):
                    if(d.is_empty()):
                        d.elog.log_error(algname,
                         "cannot preserve history because container was empty\nMust at least contain an origin record",
                         ErrorSeverity.Complaint)
                    else:
                        d.new_map(algname,instance,AtomicType.SEISMOGRAM,
                              ProcessingStatus.VOLATILE)
            elif(isinstance(d,TimeSeriesEnsemble)
                or isinstance(d,SeismogramEnsemble)):
                if(len(d.member)<=0):  # Silently return nothing if the ensemble is empy
                    return ampvec
                if(scale_by_section):
                    amp=_scale_ensemble(d,method_to_use,level,use_mean)
                    ampvec.append(amp)
                else:
                    ampvec=_scale_ensemble_members(d,method_to_use,level)
                if(object_history):
                    n=len(d.member)
                    for i in range(n):
                    # Silently do nothing if the data are marked dead
                        if(d.member[i].live):
                            if(d.member[i].is_empty()):
                                d.member[i].elog.log_error(algname,
                                  "cannot preserve history because container was empty\nMust at least contain an origin record",
                                  ErrorSeverity.Complaint)
                            else:
                                if(isinstance(d.member[i],Seismogram)):
                                    d.member[i].new_map(algname,instance,AtomicType.SEISMOGRAM,
                                            ProcessingStatus.VOLATILE)
                                else:
                                    d.member[i].new_map(algname,instance,AtomicType.TIMESERIES,
                                            ProcessingStatus.VOLATILE)
            else:
                raise RuntimeError("scale:  input data is not a supported mspass seismic data type")
        else:
            print("scale function:  Windowed scaling option not yet supported")
        # A python shortcoming is it is far from obvious from indents that
        # all nonerror states land here and return this value
        return ampvec
    except RuntimeError as err:
        if( isinstance(d,Seismogram)
                    or isinstance(d,TimeSeries) ):
            # This shows an api problem.  MsPaSSErrors are cast to
            # RuntimeErrors and lose access to the ErrorSeverity attribute
            # We need to implement a custom exception for MsPASSError
            # as described in pybind11 documentation. This line
            # should be fixed when that is done
            d.elog.log_error(algname,str(err),ErrorSeverity.Invalid)
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
        if( isinstance(d,Seismogram) or isinstance(d,TimeSeries) ):
          d.elog.log_error(algname,message,ErrorSeverity.Invalid)
        else:
          ensemble_error_post(d,algname,message,ErrorSeverity.Invalid)

def WindowData(d,twin,t0shift=None,object_history=False,instance=None):
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
    :param twin: is a TimeWindow defining window to be cut
    :param t0shift: is an optional time shift to apply to the time window.
      This parameter is convenient to avoid conversions to relative time.
      A typical example would be to set t0shift to an arrival time and let
      the window define time relative to that arrival time.  Default is None
      which cause the function to assume twin is to be used directly.
    :param object_history: boolean to enable or disable saving object
      level history.  Default is False.   Note if this feature is enabled
      and the input data history chain is empty the function will log an
      error to the returned data and not update the history chain.
    :param instance: is instance key to pass to processing history chain.
      If None (the default) and object_history is true the processing
      history chain will not be updated and a complaint error posted to
      d.elog.

    :return: copy of d with sample range reduced to twin range.  Returns
      an empty version of the parent data type (default constructor) if
      the input is marked dead
    """
    twcut=TimeWindow(twin)
    if(t0shift!=None):
        twcut.shift(t0shift)
    try:
        if(isinstance(d,TimeSeries)):
            if(d.dead()):
                # Functions like this should return a copy of the original
                # if it was marked dead - this allows preservation of
                # history data to allow recording why data were killed
                return d
            dcut=_WindowData(d,twcut)
            if(object_history):
                if(instance==None):
                    dcut.elog.log_error("WindowData",
                      "Undefined instance argument - cannot save history data",
                      ErrorSeverity.Complaint)
                elif(dcut.is_empty()):
                    dcut.elog.log_error("WindowData",
                     "Error log is empty.  Cannot be extended without a top level entry",
                     ErrorSeverity.Complaint)
                else:
                    dcut.new_map("WindowData",instance,AtomicType.TIMESERIES,
                              ProcessingStatus.VOLATILE)
            return dcut
        elif(isinstance(d,Seismogram)):
            if(d.dead()):
                # As above return original if dead
                return d
            dcut=_WindowData3C(d,twcut)
            if(object_history):
                if(instance==None):
                    dcut.elog("WindowData",
                      "Undefined instance argument - cannot save history data",
                      ErrorSeverity.Complaint)
                elif(dcut.is_empty()):
                    dcut.elog("WindowData",
                     "Error log is empty.  Cannot be extended without a top level entry",
                     ErrorSeverity.Complaint)
                else:
                    dcut.new_map("WindowData",instance,AtomicType.SEISMOGRAM,
                              ProcessingStatus.VOLATILE)
            return dcut
        else:
            raise RuntimeError("WindowData:  Invalid input data type received")
    except RuntimeError as err:
        d.log_error("WindowData",str(err),ErrorSeverity.Invalid)
        return None

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
    def __init__(self,t0=0.0,t1=0.5,type='cosine'):
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
        self.processor=_TopMute(t0,t1,type)
        self.t0=t0
    def apply(self,d,object_history=False,instance=None):
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
        if not (isinstance(d,TimeSeries) or isinstance(d,Seismogram)):
            raise MsPASSError("TopMute.apply:  usage error.  Input data must be a TimeSeries or Seismogram object",
                ErrorSeverity.Invalid)
        if d.dead():
            return
        if d.t0>self.t0:
            d.elog.log_error("TopMute.apply","Data start time is later than time of mute zero zone\n"
                + "Datum killed as this would produce a null signal",ErrorSeverity.Invalid)
            d.kill()
        else:
            self.processor.apply(d)
            if(object_history):
                if(instance==None):
                    d.elog("TopMute.apply",
                      "Undefined instance argument - cannot save history data",
                      ErrorSeverity.Complaint)
                elif(d.is_empty()):
                    d.elog("TopMute.apply",
                     "Error log is empty.  Cannot be extended without a top level entry",
                     ErrorSeverity.Complaint)
                else:
                    if isinstance(d,Seismogram):
                        d.new_map("TopMute",instance,AtomicType.SEISMOGRAM,
                              ProcessingStatus.VOLATILE)
                    else:
                        d.new_map("TopMute",instance,AtomicType.TIMESERIES,
                              ProcessingStatus.VOLATILE)
