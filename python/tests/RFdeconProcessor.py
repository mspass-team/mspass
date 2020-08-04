#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This file contains a class definition for a wrapper for 
the suite of scalar deconvolution methods supported by mspass.
It demonstrates the concept of a processing object created 
by wrapping C code.

Created on Fri Jul 31 06:24:10 2020

@author: Gary Pavlis
"""
import mspasspy.ccore as mspass
class RFdeconProcessor:
    """
    This class is a wrapper for the suite of receiver function deconvolution 
    methods we call scalar methods.  That is, the operation is reducable to 
    two time series:   wavelet signal and the data (TimeSeries) signal.  
    That is in contrast to three component methods that always treat the 
    data as vector samples.   The class should be created as a global 
    processor object to be used in a spark job.  The design assumes the 
    processor object will be passed as an argument to the RFdecon 
    function that should appear as a function in a spark map call.  
    """
    def __init__(self,alg="LeastSquares",pf="RFdeconProcessor.pf"):
        self.algorithm=alg
        pfhandle=mspass.AntelopePf(pf)
        if(self.algorithm=="LeastSquares"):
            self.md=pfhandle.get_branch('LeastSquare')
            self.processor=mspass.LeastSquareDecon(self.md)
            self.__uses_noise=False
        elif(alg=="WaterLevel"):
            self.md=pfhandle.get_branch('WaterLevel')
            self.processor=mspass.WaterLevelDecon(self.md)
            self.__uses_noise=False
        elif(alg=="MultiTaperXcor"):
            self.md=pfhandle.get_branch('MultiTaperXcor')
            self.processor=mspass.MultiTaperXcorDecon(self.md)
            self.__uses_noise=True
        elif(alg=="MultiTaperSpecDiv"):
            self.md=pfhandle.get_branch('MultiTaperSpecDiv')
            self.processor=mspass.MultiTaperSpecDivDecon(self.md)
            self.__uses_noise=True
        elif(alg=="GeneralizedIterative"):
            raise RuntimeError("Generalized Iterative method not yet supported")
        else:
            raise RuntimeError("Illegal value for alg="+alg)
        # although loaddata and loadwavelet methods don't always require 
        # it we cache the analysis time window for efficiency
        tws=self.md.get_double("deconvolution_data_window_start")
        twe=self.md.get_double("deconvolution_data_window_end")
        self.__dwin=mspass.TimeWindow(tws,twe)
        if(self.__uses_noise):
            tws=self.md.get_double("noise_window_start")
            twe=self.md.get_double("noise_window_end")
            self.__nwin=mspass.TimeWindow(tws,twe)
        else:
            self.__nwin=mspass.TimeWindow  # always initialize even if not used 
    def loaddata(self,d,dtype="Seismogram",component=0,window=False):
        """
        Loads data for processing.  When window is set true 
        use the internal pf definition of data time window 
        and window the data.  The dtype parameter changes the 
        behavior of this algorithm significantly depending on 
        the setting.   It can be one of the following:  
        Seismogram, TimeSeries, or raw_vector.   For the first 
        two the data to process will be extracted in a 
        pf specfied window if window is True.  If window is 
        False TimeSeries data will be passed directly and 
        Seismogram data will have the data defined by the 
        component parameter copied to the internal data 
        vector workspace.   If dtype is set to raw_vector 
        d is assumed to be a raw numpy vector of doubles or 
        an the aliased std::vector used in ccore, for example, 
        in the TimeSeries object s vector.  Setting dtype 
        to raw_vector and window True will result in this 
        method throwing a RuntimeError exception as the 
        combination is not possible since raw_vector data 
        have no time base.   
        
        :param d: input data (contents expected depend upon 
        value of dtype parameter).
        :param dtype: string defining the form d is expected 
          to be (see details above)
        :param component: component of Seismogram data to 
          load as data vector.  Ignored if dtype is raw_vector 
          or TimeSeries.  
        :param window: boolean controlling internally 
          defined windowing.  (see details above)
        
        :return:  Nothing (not None nothing) is returned
        """
        # First basic sanity checks
        if(dtype=="raw_vector" and window):
            raise RuntimeError("RFdeconProcessor.loaddata:  "
             + "Illegal argument combination\nwindow cannot be true with raw_vector input")
        if( not (dtype=="Seismogram" or dtype=="TimeSeries" or dtype=="raw_vector")):
            raise RuntimeError("RFdeconProcessor.loaddata:  "
             +" Illegal dtype parameter="+dtype)
        dvector=[]
        if(window):
            if(dtype=="Seismogram"):
                ts=mspass.ExtractComponent(d,component)
                ts=mspass.WindowData(ts,self.dwin)
                dvector=ts.s
            elif(dtype=="TimeSeries"):
                ts=mspass.WindowData(d,self.dwin)
                dvector=ts.s
            else:
                dvector=d
        else:
            if(dtype=="Seismogram"):
                ts=mspass.ExtractComponent(d,component)
                dvector=ts.s
            elif(dtype=="TimeSeries"):
                dvector=ts.s
            else:
                dvector=d 
        self.processor.loaddata(dvector)        
    
    def loadwavelet(self,w,dtype="Seismogram",component=2,window=False):
        # This code is painfully similar to loaddata. To reduce errors 
        # only the names have been changed to protect the innocent
        if(dtype=="raw_vector" and window):
            raise RuntimeError("RFdeconProcessor.loadwavelet:  "
             + "Illegal argument combination\nwindow cannot be true with raw_vector input")
        if( not (dtype=="Seismogram" or dtype=="TimeSeries" or dtype=="raw_vector")):
            raise RuntimeError("RFdeconProcessor.loadwavelet:  "
             +" Illegal dtype parameter="+dtype)
        wvector=[]
        if(window):
            if(dtype=="Seismogram"):
                ts=mspass.ExtractComponent(w,component)
                ts=mspass.WindowData(ts,self.dwin)
                wvector=ts.s
            elif(dtype=="TimeSeries"):
                ts=mspass.WindowData(d,self.dwin)
                wvector=ts.s
            else:
                wvector=w
        else:
            if(dtype=="Seismogram"):
                ts=mspass.ExtractComponent(w,component)
                wvector=ts.s
            elif(dtype=="TimeSeries"):
                wvector=ts.s
            else:
                wvector=w 
        self.processor.loadwavelet(wvector)        
    
    def loadnoise(self,n,dtype="Seismogram",component=2,window=False):
        # First basic sanity checks
        # Return immediately for methods that ignore noise.  
        # Note we do this silenetly assuming the function wrapper below 
        # will post an error to elog for the output to handle this nonfatal error
        if(self.algorthm=="LeastSquares" or self.algorithm=="WaterLevel"):
            return
        if(dtype=="raw_vector" and window):
            raise RuntimeError("RFdeconProcessor.loadnoise:  "
             + "Illegal argument combination\nwindow cannot be true with raw_vector input")
        if( not (dtype=="Seismogram" or dtype=="TimeSeries" or dtype=="raw_vector")):
            raise RuntimeError("RFdeconProcessor.loadnoise:  "
             +" Illegal dtype parameter="+dtype)
        nvector=[]
        #IMPORTANT  these two parameters are not required by the 
        # ScalarDecon C code but need to be inserted in pf for any algorithm
        # that requires noise data (i.e. multitaper) and the window 
        # options is desired
        if(window):
            tws=self.md.get_double("noise_data_window_start")
            twe=self.md.get_double("noise_data_window_start")
            win=mspass.TimeWindow(tws,twe)
            if(dtype=="Seismogram"):
                ts=mspass.ExtractComponent(n,component)
                ts=mspass.WindowData(ts,win)
                nvector=ts.s
            elif(dtype=="TimeSeries"):
                ts=mspass.WindowData(n,win)
                nvector=ts.s
            else:
                nvector=n
        else:
            if(dtype=="Seismogram"):
                ts=mspass.ExtractComponent(n,component)
                nvector=ts.s
            elif(dtype=="TimeSeries"):
                nvector=ts.s
            else:
                nvector=d 
        self.processor.loadnoise(nvector) 

    def process(self):
        """
        Compute the RF estimate using the algorithm defined internally.
    
       :return: vector of data that are the RF estimate computed from previously loaded data.
       """
        self.processor.process()
        return self.processor.getresult()

    def actual_output(self):
        """
        The actual output of a decon operator is the inverse filter applied to 
        the wavelet.  By design it is an approximation of the shaping wavelet
        defined for this operator.  
    
        :return: Actual output of the operator as a ccore.TimeSeries object.  
        The Metadata of the return is bare bones.  The most important factor 
        about this result is that because actual output waveforms are normally 
        a zero phase wavelet of some kind the result is time shifted to be 
        centered (i.e. t0 is rounded n/2 where n is the length of the vector 
                  returned).
        """
        result=self.processor.actual_output()
        return result
    def ideal_output(self):
        """
        The ideal output of a decon operator is the same thing we call a
        shaping wavelet.  This method returns the ideal output=shaping wavelet
        as a TimeSeries object.   Like the actual output method the return 
        function is circular shifted so the function peaks at 0 time located 
        at n/2 samples from the start sample.  Graphic displays will then show 
        the wavelet centered and peaked at time 0.   The prediction error 
        can be computed as the difference between the actual_output and 
        ideal_output TimeSeries objects.   The norm of the prediction error 
        is a helpful metric to display the stability and accuracy of the 
        inverse.
        """
        result=self.processor.ideal_output()
        return result
    def inverse_filter(self):
        """
        This method returns the actual inverse filter that if convolved with 
        he original data will produce the RF estimate.  Note the filter is 
        meaningful only if the source wavelet is minimum phase.  A standard 
        theorem from time series analysis shows that the inverse of mixed 
        phase wavelet is usually unstable and a maximum phase wavelet is always
        unstable.   Fourier-based methods can still compute a stable solution 
        even with a mixed phase wavelet because of the implied circular 
        convolution.  
    
        The result is returned as  TimeSeries object.  
        """
        result=self.processor.inverse_filter()
        return result
    def QCMetrics(self):
        """
        All decon algorithms compute a set of algorithm dependent quality 
        control metrics.  This method returns the metrics as a set of fixed
        name:value pairs in a mspass.Metadata object.   The details are 
        algorithm dependent.  See related documentation for metrics computed 
        by different algorithms.
        """
        result=self.processor.QCMetrics()
        return result
    def change_parameters(self,md):
        """
        Use this method to change the internal parameter setting of the 
        processor.  It can be used, for example, to switch from the damped
        least square method to the water level method.  Note the input 
        must be a complete definition for a parameter set defining a 
        particular algorithm.  i.e. this is not an update method but 
        t reinitializes the processor. 
    
        :param md: is a mspass.Metadata object containing required parameters
        for the alternative algorithm.
        """
        self.processor.change_parameters(md)
        
    @property
    def uses_noise(self):
        return self.__uses_noise
    
    @property
    def dwin(self):
        return self.__dwin
    
    @property
    def nwin(self):
        return self.__nwin

    
        
def RFdecon(processor,d,wavelet=None,noisedata=None,wcomp=2,ncomp=2,
            save_history=False,algid='0'):
    """
    Use this function to compute conventional receiver functions 
    from a single three component seismogram.  The type of 
    processor is defined by the processor argument that is expected 
    to be an instance of the wrapper class RFdeconProcessor. In 
    mspass the processor class should be created and stored as a 
    global variable and passed to this function within a 
    spark map operator.  That allows the processing to proceed in 
    parallel without the overhead of creating the processor 
    definition for each seismogram.  
    
    Default assumes d contains all data sections required to do 
    the deconvolution with the wavelet in component 2 (3 for matlab
    and FORTRAN people).  By default the data and noise 
    (if required by the algorithm) sections will be extracted from 
    the (assumed larger) data section using time windows defined 
    internally in the processor pf definition.   For variations (e.g. 
    adding tapering to one or more of the time series inputs) 
    use the d, wavelet, and (if required) noise arguments to load 
    each component separately.  Note d is dogmatically required 
    to be three component data while optional wavelet and noisedata
    series are passed as plain numpy vectors (i.e. without the 
    decoration of a TimeSeries).   
    
    To make use of the extended outputs from RFdeconProcessor 
    algorithms (e.g. actual output of the computed operator) 
    call those methods after this function returns successfully 
    with a three-component seismogram output.  That is possible 
    because the processor object caches the most recent wavelet 
    and inverse used for the deconvolution.   An exception is 
    that all algorithms call their QCmetrics method of processor 
    and push them to the headers of the deconvolved output.  
    QCmetric attributes are algorithm dependent. 
    
    The ProcessingHistory feature can optionally be enabled by 
    setting the save_history argument to True.   When enable one should 
    normally set a unique id for the algid argument.  
    
    :param processor:  RFdeconProcessor object defining algorithm
     to be applied (see related documentation for details).
    :param d:  Seismogram input data.  See notes above about 
     time span of these data.
    :param wavelet:   vector of doubles (numpy array or the 
     std::vector container internal to TimeSeries object) defining
     the wavelet to use to compute deconvolution operator.  
     Default is None which assumes processor was set up to use 
     a component of d as the wavelet estimate.
    :param noisedata:  vector of doubles (numpy array or the 
     std::vector container internal to TimeSeries object) defining
     noise data to use for computing regularization.  Not all RF
     estimation algorithms use noise estimators so this parameter 
     is optional.   It can also be extracted from d depending on 
     parameter file options. 
    :param wcomp:  When defined from Seismogram d the wavelet 
     estimate in conventional RFs is one of the components that 
     are most P wave dominated. That is always one of three 
     things:  Z, L of LQT, or the L component from the output of 
     Kennett's free surface transformation operator.  The 
     default is 2, which for ccore.Seismogram is always one of 
     the above.   This parameter would be changed only if the 
     data has undergone some novel transformation not yet invented
     and the best wavelet estimate was on in 2 (3 with FORTRAN 
     and matlab numbering).  
     :param ncomp: component number to use to compute noise.  This is used 
     only if the algorithm in processor requires a noise estimate. 
     Normally it should be the same as wcomp and is by default (2).
     :param save_history:  When true the ProcessingHistory mechanism 
     is enabled.
     :param algid: Define the algorithm id for the ProcessingHistory chain.
     This parameter is ignored unless save_history is true.   Default is 
     the string "0" but it should normally be set to some unique id 
     defined for that instance of this algorithm.  The algorithm is 
     always set to RFdecon.  
     
    :return:  Seismogram object containing the RF estimates.  
     The orientations are always the same as the input.   
    """
    try:
        if(wavelet != None):
            processor.loadwavelet(wavelet,dtype='raw_vector')
        else:
            #processor.loadwavelet(d,dtype='Seismogram',window=True,component=wcomp)
            processor.loadwavelet(d,window=True)
        if(processor.uses_noise):
            if(noisedata!=None):
                processor.loadnoise(noisedata,dtype='raw_vector')
            else:
                processor.loadnoise(d,window=True,component=ncomp)
    except RuntimeError as err:
        d.kill()
        d.elog.log_error('WindowData',err.repr(err),mspass.ErrorSeverity.Invalid)
        return d
    # We window data before computing RF estimates for efficiency
    # Otherwise we would call the window operator 3 times below
    result=mspass.Seismogram
    try:
        dw=mspass.WindowData3C(d,processor.dwin)
        result=mspass.Seismogram(dw)
        result.load_history(d)
    except RuntimeError as err:
        d.kill()
        d.elog.log_error('WindowData',err.repr(err),mspass.ErrorSeverity.Invalid)
        return d
    npts=result.npts
    try:
        for k in range(3):
            processor.loaddata(d,component=k)
            x=processor.process()
            # overwrite this component's data in the result Seismogram
            # Use some caution handling any size mismatch
            nx=len(x)
            if(nx>=npts):
                for i in range(npts):
                    result.u[k,i]=x[i]
            else:
                #this may not be the fastest way to do this but it is simple and clean
                # matters little since this is an error condition and should be rare
                for i in range(npts):
                    if(i<nx):
                        result.u[k,i]=x[i]
                    else:
                        result.u[k,i]=0.0
                # This is actually an error condition so we log it
                message='Windowing size mismatch.\nData window length = %d which is less than operator length= %d' % (nx,npts)
                result.elog.log_error("RFdecon",message,mspass.ErrorSeverity.Complaint)
    except RuntimeError as err:
        result.kill()
        result.elog.log_error('RFdecon',err.repr(err),mspass.ErrorSeverity.Invalid)
    finally:
        if(save_history):
            result.new_map('RFdecon',algid,mspass.AtomicType.Seismogram)
        return result