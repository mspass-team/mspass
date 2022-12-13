from abc import ABC, abstractmethod
from mspasspy.ccore.utility import MsPASSError,ErrorSeverity
from mspasspy.ccore.seismic import (
    TimeSeries,
    Seismogram,
    TimeSeriesEnsemble,
    SeismogramEnsemble)
from mspasspy.util.converter import (
    Trace2TimeSeries,
    TimeSeries2Trace,
    Seismogram2Stream,
    Stream2Seismogram,
    SeismogramEnsemble2Stream,
    Stream2SeismogramEnsemble,
    )

import numpy as np

class BasicResampler(ABC):
    """
    Base class for family of resampling operators.   All it really does is 
    define the interface and standardize the target of the operator.  
    A key concept of this family of operator is they are intended to 
    be used in a map operator to regularize the sample rate to a 
    constant.   Hence, the base class defines that constant output 
    sample rate (alternatively the sample interval, dt).  
    
    ALL implementations must recognize a couple fundamental concepts:
        1.  This is intended to ONLY be used on waveform segments.   
            The problem of resampling continuous data requires different 
            algorithms.  The reason is boundary effects.  All 
            implementations are subject to edge transients.  How the 
            implementation does or does not handle that issue is 
            viewed as an implementation detail, 
        2.  Downsampling ALWAYS requires some method to avoid aliasing of 
            the output.  How that is done is considered an implementation 
            detail. 
            
    This is a sketch of an algorithm is pseudopython code showing how 
    a typical instance of this class (in the sample ObspyResampler) 
    would be used in  parallel workflow:
        
    .. rubric:: Example
    
    ro = ObspyResampler(10.0)   # target sample rate of 10 sps
    cursor = db.TimeSeries.find({})
    bag = read_distributed_data(cursor,collection="wf_TimeSeries")
    bag = bag.map(ro.resample)
    bag.map(db.save_data())
    bag.compute()
    
    
    """
    def __init__(self,dt=None,sampling_rate=None):
        if dt and sampling_rate:
            raise MsPASSError(
                  "BasicResample:  usage error.  Specify either dt or sampling_rate.  You defined both",
                  ErrorSeverity.Fatal
                )
        if dt:
            self.dt = dt
            self.samprate = sampling_rate
        elif sampling_rate:
            self.dt = 1.0/sampling_rate
            self.samprate = sampling_rate
    @abstractmethod
    def resample(self,mspass_object):
        """
        Main operator a concrete class must implement.  It should accept 
        any mspass data object and return a clone that has been resampled 
        to the sample interface defined by this base class. 
        
        """
        pass
    
class ObspyResampler(BasicResampler):
    """
    This class is a wrapper for obspy's resample algorithm they implement 
    as a method of their Trace and Stream objects.  Note the obpsy methods are 
    themselves only a wrapper for ascipy function with the same name. 
    The algorithm and its limitations are described in the obpsy and scipy 
    documentation you can easily find with a web search.   A key point 
    about this algorithm is that unlike decimate it allows resampling to 
    something not an integer multiple or division from the input.  
    A type example where that is essential is some old OBS data from 
    Scripps instruments that had a sample rate that was a multiple of 
    one of the more standard rates like 20 or 100.   Anti-aiasing is handled 
    automatically by default, but can be imposed externally and overriden 
    using the no_filter option.  
    
    I (glp) have no practical experience with this implementation in obspy.
    There are hints that it is subject to a somewhat universal tradeoff 
    between flexibility and reliability.  From all I've read I would 
    suggest you use this algorithm only for upsampling and to resample 
    oddball data like that from Scripps OBS instruments.  Most data 
    are probably best downsampled to a common sample rate with a 
    decimate algorithm whenever possible.   We have implemented a 
    compable wrapper to this one for decimate called ObspyDecimator. 
    
    
    All the arguments to the constructor are identical (in name an concept)
    to the resample methods of Trace/Stream.  See the obspy documentation 
    for detailed description and limitations.  
    
    The primary method of this class is a concrete implementation of the 
    resample method.  
    
    """
    def __init__(self,sampling_rate, window="hann",no_filter=True, strict_length=False):
        """
        """
        super().__init__(sampling_rate=sampling_rate)
        self.window=window
        self.no_filter=no_filter
        self.strict_length=strict_length
        
    def resample(self,mspass_object):
        if isinstance(mspass_object,TimeSeries):
            tr = TimeSeries2Trace(mspass_object)
            tr.resample(self.samprate,
                        window=self.window,
                        no_filter=self.no_filter,
                        strict_length=self.strict_length,
                        )
            return Trace2TimeSeries(tr)
        elif isinstance(mspass_object,Seismogram):
            strm = Seismogram2Stream(mspass_object)
            strm.resample(self.samprate,
                        window=self.window,
                        no_filter=self.no_filter,
                        strict_length=self.strict_length,
                        )
            return Stream2Seismogram(strm)
        elif isinstance(mspass_object,TimeSeriesEnsemble):
            for i in len(mspass_object.member):
                d = mspass_object.member[i]
                tr = TimeSeries2Trace(d)
                tr.resample(self.samprate,
                        window=self.window,
                        no_filter=self.no_filter,
                        strict_length=self.strict_length,
                        )
                mspass_object.member[i] = Trace2TimeSeries(tr)
            return mspass_object
        elif isinstance(mspass_object,SeismogramEnsemble):
            strm = SeismogramEnsemble2Stream(mspass_object)
            strm.resample(self.samprate,
                        window=self.window,
                        no_filter=self.no_filter,
                        strict_length=self.strict_length,
                        )
            return Stream2SeismogramEnsemble(strm)
        else:
            message = "ObspyResampler.resample: received unsupported data type="+str(type(mspass_object))
            raise TypeError(message)
            
class ObspyDecimator(BasicResampler):
    """
    Wrapper to utilize obspy's decimate operator (actually scipy's decimate)
    to automatically resample a set set through a map operator following 
    the api concepts of BasicResampler.  The algorithm works only on 
    a data set with sample intervals that work by the decimation concept.  
    That means the data are only downsampled (or left alone) and the 
    target sample interval is an integer multiple of the input data's 
    sample interval.
    
    Most seismology data today are defined by a finite set of common sample 
    rates that differ by integer division.   The fundamental reason is that 
    all modern digitizers use internal downsampling with DSP chips from a
    base sample rate orders of magnitude larger than the output. The intrinsic
    smoothing in the FIR filters used for antialiasing reduces amplitude 
    errors by roughly the square root of the number of samples averaged.  
    The result is that the most common digitizers for the past 30 years 
    have had these, most common selectable sample rates:  1 sps, 5 sps, 
    10 sps, 20 sps, 40 sps, 50 sps, 100 sps, and 250 sps.   These all have 
    integer relationships.   e.g. 20 sps is 100 sps divided by 5 and 
    50 sps is 100 divided by 2.  The algorithm for this implementation
    will ONLY WORK if the target sample rate is ALWAYS constructable from 
    a simple integer division = integer multiple of sample interval.  
    e.g. to decimate 100 sps data (dt=0.01) we divide the sample rate by 
    5 or multiple the sample interval by 5 (0.05 s in this case).  
    If any datum cannot be resampled that way it will be killed.   
    For example, it is not possible to construct 20 sps data from 50 sps
    data with this algorithm.  
    
    Be aware decimators also have intrinsic edge effects.   The anitialias
    filter that has to be applied (you can get garbage otherwise) will always
    produce an edge transient.  A key to success with any downsampling 
    operator is to always have a pad zone if possible.  That is, you start 
    with a longer time window than you will need for final processing and 
    discard the pad zone when you enter the final stage.   Note that is 
    true of ALL filtering actually.   
    
    The constructor has exactly the same argument signature as obspy's 
    decimate function described in the docstring currently found here:
        https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.decimate.html
    for Stream.  The docstring for Trace is nearly identical.
    """
    def __init__(self,sampling_rate,no_filter=True, strict_length=False):
        """
        """
        super().__init__(sampling_rate=sampling_rate)
        self.no_filter=no_filter
        self.strict_length=strict_length
    def _dec_factor(self,d):
        """
        Returns decimation factor to use for atomic data d.  
        d can be a mspass atomic type or an obspy Trace. 
        Uses np.isclose to establish if the sample interval is feasible.
        If so it returns the decimation factor as an integer that should be 
        used on d.   Returns -1 if the sample rate is irregular and 
        is not close enough to be an integer multiple.  Return 0 if 
        the data dt would require upsampling
        """
        # internal use guarantees this can only be TimeSeries or Seismogram
        # so this resolves
        d_dt = d.dt
        float_dfac=self.dt/d_dt
        int_dfac=int(float_dfac)
        # This perhaps should use a softer constraint than default
        if np.isclose(float_dfac,float(int_dfac)):
            return int_dfac
        elif int_dfac==0:
            return 0
        else:
            return -1
    def _make_illegal_decimator_message(self,error_code,data_dt):
        """
        Private method to format a common message if the data's sample
        interval, data_dt, is not feasible to produce by decimation. 
        The error message is the return
        """
        if error_code == 0:
            message = "Data sample interval={ddt} is smaller than target dt={sdt}.  This operator can only downsample".format(ddt=data_dt,sdt=self.dt)
        else:
            message = "Data sample interval={ddt} is not an integer multiple of {sdt}".format(ddt=data_dt,sdt=self.dt)
        return message
    def resample(self,mspass_object):
        """
        Implementation of required abstract method for this operator.   
        The only argument is mspass_object.   The operator will downsample 
        the contents of the sample data container for any valid input.  
        If the input is not a mspass data object (i.e. atomic TimeSeries 
        or Seismogram) or one of the enemble objects it will throw a 
        TypeError exception.   
        """
        # We do this test at the top to avoid having returns testing for 
        # a dead datum in each of the if conditional blocks below
        if isinstance(mspass_object,(TimeSeries,Seismogram,TimeSeriesEnsemble,SeismogramEnsemble)):
            if mspass_object.dead():
                return mspass_object
        else:
            message = "ObspyDecimator.resample: received unsupported data type="+str(type(mspass_object))
            raise TypeError(message)
        if isinstance(mspass_object,TimeSeries):
            decfac=self._dec_factor(mspass_object)
            if decfac<=0:
                mspass_object.kill()
                message = self._make_illegal_decimator_message(decfac,mspass_object.dt)
                mspass_object.elog.log_error(
                        "ObspyDecimator.resample",
                        message,
                        ErrorSeverity.Invalid
                        )
            else:    
                tr = TimeSeries2Trace(mspass_object)
                tr.decimate(decfac,
                        no_filter=self.no_filter,
                        strict_length=self.strict_length,
                        )
                mspass_object = Trace2TimeSeries(tr)
        elif isinstance(mspass_object,Seismogram):
            decfac=self._dec_factor(mspass_object)
            if decfac<=0:
                mspass_object.kill()
                message = self._make_illegal_decimator_message(decfac,mspass_object.dt)
                mspass_object.elog.log_error(
                        "ObspyDecimator.resample",
                        message,
                        ErrorSeverity.Invalid
                        )
            else:
                strm = Seismogram2Stream(mspass_object)
                for tr in strm:
                    tr.decimate(decfac,
                        no_filter=self.no_filter,
                        strict_length=self.strict_length,
                     )
                mspass_object = Stream2Seismogram(strm)
        elif isinstance(mspass_object,TimeSeriesEnsemble):
            for i in len(mspass_object.member):
                d = mspass_object.member[i]
                decfac = self._dec_factor(d)
                if decfac<=0:
                    message = self._make_illegal_decimator_message(decfac,d.dt)
                    mspass_object.member[i].elog.log_error(
                        "ObspyDecimator.resample",
                        message,
                        ErrorSeverity.Invalid
                        )
                    mspass_object.member[i].kill()
                else:
                    tr = TimeSeries2Trace(mspass_object)
                    tr.decimate(decfac,
                                    no_filter=self.no_filter,
                                    strict_length=self.strict_length,
                                )
                    mspass_object.member[i] = Trace2TimeSeries(tr)
        else:
        # This block is assumed equivalent to 
        # elif isinstance(mspass_object,SeismogramEnsemble):
        # because of isinstance test at the top.  Careful if the supported 
        # list of data types changes
            for i in len(mspass_object.member):
                d = mspass_object.member[i]
                decfac = self._dec_factor(d)
                if decfac<=0:
                    message = self._make_illegal_decimator_message(decfac,d.dt)
                    mspass_object.member[i].elog.log_error(
                        "ObspyDecimator.resample",
                        message,
                        ErrorSeverity.Invalid
                        )
                    mspass_object.member[i].kill()
                else:
                    strm = Seismogram2Stream(mspass_object)
                    for tr in strm:
                        decfac = self._dec_factor(tr)
                        tr.decimate(decfac,
                                    no_filter=self.no_filter,
                                    strict_length=self.strict_length,
                                )
                    d = Stream2Seismogram(strm)
                    mspass_object.member[i] = d

        return mspass_object