#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
from multitaper.mtspec import MTSpec
from mspasspy.ccore.utility import MsPASSError,ErrorSeverity,Metadata
from mspasspy.ccore.seismic import TimeSeries,DoubleVector,PowerSpectrum
class MTPowerSpectrumEngine():
    """
    Python plug in replacement for ccore class with the same name.
    
    Earlier version of MsPASS had only a C++ implementation of multitaper 
    spectra found in mspasspy.ccore.algorithms.deconvolution.MTPowerSpectrumEngine.
    This class is a plug in replacement for the C++ class but the actual
    computational engine uses German Prieto's multitaper module available 
    from github.  His package is easily installed independently using 
    "pip install multitaper" or the conda equivalent.
    
    To mesh with Preito's library required an anomaly in behavior that 
    user's must be aware of.  That is, Prieto's MTSpec class does not 
    use a model where a class instances is constructed from an input 
    time series (literally a numpy array).   That does not mesh with the 
    "apply" model of MTPowerSpectrum engine that was designed as 
    construct once and then pass as an argument through a map operator.   
    The apply model meshes better with map/reduce and is more efficient for 
    this particular problem as it avoids the (expensive) operation of 
    computing the slepian tapers for each data vector it processes.  
    That feature is simulated here by storing an instance of the 
    MTSpec class inside this wrapper class.  BUT be aware the MTSpec 
    class (MTSpec_instance) is not actually instantiated until the 
    first call to the apply method.  For normal use in map reduce that 
    detail should be invisible except each worker will incur the overhead
    of initialization when the first apply method is called.   
    
    This class adds some features in Prieto's module not found in the
    much cruder use in the C++ version.   See the constructor docstring 
    below for details.   The available class methods are identical to 
    the C++ version.   Additional features of Prieto's library 
    can be accessed in a bit if a strange way through the class
    attribute name.MTSpect_instance where "name" is the symbol used to 
    define a concrete instance of the class.   MTSpec_instance is a 
    instance of the MTSpec class in Prieto's library.  See the 
    docstring for that class found at the following for details of 
    what is available through that mechanism:
        https://multitaper.readthedocs.io/en/latest/mtspec.html#mtspec.MTSpec
        
    
    The following code fragment illustrates the use of other features in 
    Prieto's library not available with the C++ version.   This example 
    computes the spectrum of the waveform segment in the TimeSeries 
    object d (assumed defined earlier) with time-bandwidth product 5.0 
    and 8 tapers.  It then computes the multitaper jackknife errors 
    using that method of MTSpec.
    
        npts=d.npts
        engine=MTPowerSpectrumEngine(npts,5.0,8)
        powspec = engine.apply(d)
        jackknife_errors = engine.MTSpec_instance.jackspec()
        

    :param winsize:   expected number of samples per apply call
    :param tbp:  time-bandwidth product of multitapers to compute (float)
    :param number_tapers:  number of tapers to use for spectral estimator. 
    Integer must be < 2*tbp.  Unlike MTSpec there is no default.
    :param nfft:  number of points to use in fft.  Default uses the winsize. 
    Be aware of the standard issues about fft algorithms and array sizes.  
    :param idapt:   MTSpec argument that sets the weighting scheme.  
    Must be an integer with one of three values:
        0 - use "adaptive" weighting scheme (default)
        1 - weight each taper by 1.0 ("unity" weights in matlab equivalent)
        2 - weight by eigenvalues of each taper. 
    (Note:  matlab's help files have a good, practical description of how 
     these choices compare)
    """
    def __init__(self,winsize,tbp,number_tapers,
                 nfft=0,
                 iadapt=0,
                 ):
        
        self.winsize=winsize
        self.tbp=tbp   #nw in multitaper
        self.number_tapers=number_tapers   #kspec in multitaper
        self.vn=None  # constructor for MTSpec will set this when passed None
        self.nfft=nfft
        self.idapt=iadapt
        self.lamb=None # constructor for MTSpec set this
        self.MTSpec_instance=None
    def apply(self,d,dt=1.0)->PowerSpectrum:
        """
        Compute the spectrum of the data contained in d using the 
        properties defined on construction.  Result is returned as a 
        MsPASS PowerSpectrum object.  The metadata of the return is 
        minimal for raw array input but for TimeSeries input the 
        parent data is cloned for the output. 
        
        :param d:  input data containing an array to use to compute the 
        desired PowerSpectrum.   
        :type d:  Must be one of the following:
            TimeSeries - When the input is a MsPASS TimeSeries object the 
            entire data vector is passed to the engine.  Use WindowData if 
            you need to use a subset of an input data either in a map 
            call preceding this or within the argument list 
            (e.g. instance.apply(WindowData(d,winstart,wineend)))
        
           numpy array - expects a 1d vector of length == what the engine expects
           DoubleVector - a C++ std::vector bound with pybind11 of length 
           that expected by the engine.   
           
        :param dt:  optional sample interval of input data. Ignored for 
        TimeSeries input but essential if passing a plain numpy or DoubleVector
        if you need the actual frequency values.  Default is 1.0.  
        
        :return:  returns a MsPSS PowerSpectrum object.  The symbol spectrum 
        contains the computed power spectrum estimate.   The PowerSpectrum 
        object has a Metadata container.  When the input is TimeSeries 
        the return will contain a copy of the Metadata form the input.  
        Be warned that means there may be attributes at odds with the 
        actual PowerSpectrum contents.  This method posts the following 
        additional metadata:  npts, delta, MTSpec_dt, MTSpec_df, MTSpec_nw,
        MTSpec_kspec, MTSpec_nfft, MTSpec_npts

        """
        # All inputs end up filling a numpy array passed to MTSpec below
        if isinstance(d,TimeSeries):
            y=np.array(d.data)
            dt = d.dt
            md=Metadata(d)
        elif isinstance(d,DoubleVector):
            # pybind11 bindings defined std::vector as a DoubleVector
            # This is exactly like the use for TimeSeries where data
            # is a DoubleVector
            y=np.array(d)
            md=Metadata()
            # These are mspass schema standard names - could be 
            # a future maintenance issue
            md['npts']=len(d)
            md['delta']=dt
        elif isinstance(d,np.ndarray):
            y=d
            md=Metadata()
            # These are mspass schema standard names - could be 
            # a future maintenance issue
            md['npts']=len(d)
            md['delta']=dt
        else:
            raise TypeError("MTPowerSpectrumEngine.apply:  arg0 has invalid type - must be TimeSeries, DoubleVector, or numpy array")
        self.MTSpec_instance = MTSpec(
                       y,
                       nw=self.tbp,
                       kspec=self.number_tapers,
                       dt=dt,
                       nfft=self.nfft,
                       iadapt=self.idapt,
                       vn=self.vn,
                       lamb=self.lamb)
        # Set these so they will be automatically reused on a 
        # successive call.   May need to trap size mismatch above 
        # if these were previously set - this needs some testing
        #  REMOVE ME WHEN VERIFIED
        self.vn = self.MTSpec_instance.vn
        self.lamb = self.MTSpec_instance.lamb
        # We copy these attributes to metadata for optional downstream use
        md["MTSpec_dt"] = self.MTSpec_instance.dt
        md["MTSpec_df"] = self.MTSpec_instance.df
        md["MTSpec_nw"] = self.MTSpec_instance.nw
        md["MTSpec_kspec"] = self.MTSpec_instance.kspec
        md["MTSpec_nfft"] = self.MTSpec_instance.nfft
        md["MTSpec_npts"] = self.MTSpec_instance.npts

        # these could be set as aliases but we set the explicitly for now 
        # even if it is redundamt
        md["time_bandwith_product"] = self.MTSpec_instance.nw
        md["number_tapers"] = self.MTSpec_instance.kspec
        # this is an obnoxious collision with the C++ api DoubleVector 
        npts=self.MTSpec_instance.npts
        work=DoubleVector()
        for i in range(npts):
            work.append(self.MTSpec_instance.spec[i])
            
        result = PowerSpectrum(md,
                                work,
                                self.MTSpec_instance.df,
                               "multitaper.MTSpec")
        return result
        
        
    def frequencies(self):
        """
        Return a numpy array of the frequencies associated with each component 
        of the spectrum vector. 
        """
        if self.MTSpec_instance:
            return self.MTSpec_instance.freq
        else:
            raise MsPASSError(
                "MTPowerSpectrumEngine.frequencies:   engine has not been initialized.  Method undefined until apply method called",
                ErrorSeverity.Fatal
                )
    def time_bandwidth_product(self)->float:
        """
        Return time bandwidth product for this operator.
        """
        if self.MTSpec_instance:
            return self.MTSpec_instance.nw
        else:
            raise MsPASSError(
                "MTPowerSpectrumEngine.time_bandwidth_product:   engine has not been initialized.  Method undefined until apply method called",
                ErrorSeverity.Fatal
                )
    def number_tapers(self)->int:
        """
        Return the number of tapers defined for this operator.
        """
        if self.MTSpec_instance:
            return self.MTSpec_instance.kspec
        else:
            raise MsPASSError(
                "MTPowerSpectrumEngine.number_tapers:   engine has not been initialized.  Method undefined until apply method called",
                ErrorSeverity.Fatal
                )
        
    def set_df(self,df):
        """
        Explicit setter for frequency bin interval.  Needed when 
        changing sample interval for input data. Rarely of use but 
        included for compatibility with the C++ class with the 
        same name.
        
        This maybe should just be pass or throw an error. 
        """
        self.MTSpec_instance.df=df
        