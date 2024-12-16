#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This file contains a class definition for a wrapper for
the suite of scalar deconvolution methods supported by mspass.
It demonstrates the concept of a processing object created
by wrapping C code.  It also contains a top-level function
that is a pythonic interface that meshes with MsPASS schedulers
for parallel processing called `RFdecon`.   `RFdecon` is a
wrapper for all single-station methods.   It cannot be used for
array methods.

Created on Fri Jul 31 06:24:10 2020

@author: Gary Pavlis
"""
import numpy as np

from mspasspy.ccore.seismic import DoubleVector,Seismogram
from mspasspy.ccore.utility import AntelopePf, Metadata, MsPASSError, ErrorSeverity
from mspasspy.util.converter import Metadata2dict
from mspasspy.algorithms.window import WindowData
from mspasspy.ccore.algorithms.basic import TimeWindow, _ExtractComponent
from mspasspy.ccore.algorithms.deconvolution import (
    LeastSquareDecon,
    WaterLevelDecon,
    MultiTaperXcorDecon,
    MultiTaperSpecDivDecon,
)
from mspasspy.util.decorators import mspass_func_wrapper


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

    def __repr__(self) -> str:
        repr_str = "{type}(alg='{alg}', md='{md}')".format(
            type=str(self.__class__), alg=self.algorithm, md=self.md
        )
        return repr_str

    def __str__(self) -> str:
        md_str = str(Metadata2dict(self.md))
        processor_str = "{type}(alg='{alg}', md='{md}')".format(
            type=str(self.__class__), alg=self.algorithm, md=self.md
        )
        return processor_str

    def __init__(self, alg="LeastSquares", pf="RFdeconProcessor.pf"):
        self.algorithm = alg
        # use a copy in what is more or less a switch-case block
        # to be robust - I don't think any of the constructors below
        # alter pfhandle but the cost is tiny for this stability
        pfhandle = AntelopePf(pf)
        if self.algorithm == "LeastSquares":
            # In this and elif blocks below we convert
            # return of get_branch to a Metadata container
            # that is necessary because get_branch retuns the
            # AntelopePf subclass and we want this to be a clean
            # Metdata object.   Further, at present a pf will not
            # serialize
            self.md = Metadata(pfhandle.get_branch("LeastSquare"))
            self.processor = LeastSquareDecon(self.md)
            self.__uses_noise = False
        elif alg == "WaterLevel":
            self.md = Metadata(pfhandle.get_branch("WaterLevel"))
            self.processor = WaterLevelDecon(self.md)
            self.__uses_noise = False
        elif alg == "MultiTaperXcor":
            self.md = Metadata(pfhandle.get_branch("MultiTaperXcor"))
            self.processor = MultiTaperXcorDecon(self.md)
            self.__uses_noise = True
        elif alg == "MultiTaperSpecDiv":
            self.md = Metadata(pfhandle.get_branch("MultiTaperSpecDiv"))
            self.processor = MultiTaperSpecDivDecon(self.md)
            self.__uses_noise = True
        elif alg == "GeneralizedIterative":
            raise RuntimeError("Generalized Iterative method not yet supported")
        else:
            raise RuntimeError("Illegal value for alg=" + alg)

    def loaddata(self, d, dtype="Seismogram", component=0, window=False):
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
        if dtype == "raw_vector" and window:
            raise RuntimeError(
                "RFdeconProcessor.loaddata:  "
                + "Illegal argument combination\nwindow cannot be true with raw_vector input"
            )
        if not (
            dtype == "Seismogram" or dtype == "TimeSeries" or dtype == "raw_vector"
        ):
            raise RuntimeError(
                "RFdeconProcessor.loaddata:  " + " Illegal dtype parameter=" + dtype
            )
        dvector = []
        if window:
            if dtype == "Seismogram":
                ts = _ExtractComponent(d, component)
                ts = WindowData(ts, self.dwin.start, self.dwin.end)
                dvector = ts.data
            elif dtype == "TimeSeries":
                ts = WindowData(d, self.dwin.start, self.dwin.end)
                dvector = ts.data
            else:
                dvector = d
        else:
            if dtype == "Seismogram":
                ts = _ExtractComponent(d, component)
                dvector = ts.data
            elif dtype == "TimeSeries":
                dvector = ts.data
            else:
                dvector = d
        # Have to explicitly convert to ndarray because DoubleVector cannot be serialized.
        self.dvector = np.array(dvector)

    def loadwavelet(self, w, dtype="Seismogram", component=2, window=False):
        # This code is painfully similar to loaddata. To reduce errors
        # only the names have been changed to protect the innocent
        if dtype == "raw_vector" and window:
            raise RuntimeError(
                "RFdeconProcessor.loadwavelet:  "
                + "Illegal argument combination\nwindow cannot be true with raw_vector input"
            )
        if not (
            dtype == "Seismogram" or dtype == "TimeSeries" or dtype == "raw_vector"
        ):
            raise RuntimeError(
                "RFdeconProcessor.loadwavelet:  " + " Illegal dtype parameter=" + dtype
            )
        wvector = []
        if window:
            if dtype == "Seismogram":
                ts = _ExtractComponent(w, component)
                ts = WindowData(ts, self.dwin.start, self.dwin.end)
                wvector = ts.data
            elif dtype == "TimeSeries":
                ts = WindowData(w, self.dwin.start, self.dwin.end)
                wvector = ts.data
            else:
                wvector = w
        else:
            if dtype == "Seismogram":
                ts = _ExtractComponent(w, component)
                wvector = ts.data
            elif dtype == "TimeSeries":
                wvector = ts.data
            else:
                wvector = w
        # Have to explicitly convert to ndarray because DoubleVector cannot be serialized.
        self.wvector = np.array(wvector)

    def loadnoise(self, n, dtype="Seismogram", component=2, window=False):
        # First basic sanity checks
        # Return immediately for methods that ignore noise.
        # Note we do this silently assuming the function wrapper below
        if not self.__uses_noise:
            return
        if dtype == "raw_vector" and window:
            raise RuntimeError(
                "RFdeconProcessor.loadnoise:  "
                + "Illegal argument combination\nwindow cannot be true with raw_vector input"
            )
        if not (
            dtype == "Seismogram" or dtype == "TimeSeries" or dtype == "raw_vector"
        ):
            raise RuntimeError(
                "RFdeconProcessor.loadnoise:  " + " Illegal dtype parameter=" + dtype
            )
        nvector = []
        # IMPORTANT  these two parameters are not required by the
        # ScalarDecon C code but need to be inserted in pf for any algorithm
        # that requires noise data (i.e. multitaper) and the window
        # options is desired
        if window:
            tws = self.md.get_double("noise_window_start")
            twe = self.md.get_double("noise_window_end")
            if dtype == "Seismogram":
                ts = _ExtractComponent(n, component)
                ts = WindowData(ts, tws, twe)
                nvector = ts.data
            elif dtype == "TimeSeries":
                ts = WindowData(n, tws, twe)
                nvector = ts.data
            else:
                nvector = n
        else:
            if dtype == "Seismogram":
                ts = _ExtractComponent(n, component)
                nvector = ts.data
            elif dtype == "TimeSeries":
                nvector = ts.data
            else:
                nvector = n
        # Have to explicitly convert to ndarray because DoubleVector cannot be serialized.
        self.nvector = np.array(nvector)

    def apply(self):
        """
        Compute the RF estimate using the algorithm defined internally.

        :return: vector of data that are the RF estimate computed from previously loaded data.
        """
        if hasattr(self, "dvector"):
            self.processor.loaddata(DoubleVector(self.dvector))
        if hasattr(self, "wvector"):
            self.processor.loadwavelet(DoubleVector(self.wvector))
        if self.__uses_noise and hasattr(self, "nvector"):
            self.processor.loadnoise(DoubleVector(self.nvector))
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
        if hasattr(self, "dvector"):
            self.processor.loaddata(DoubleVector(self.dvector))
        if hasattr(self, "wvector"):
            self.processor.loadwavelet(DoubleVector(self.wvector))
        if self.__uses_noise and hasattr(self, "nvector"):
            self.processor.loadnoise(DoubleVector(self.nvector))
        self.processor.process()
        return self.processor.actual_output()

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
        if hasattr(self, "dvector"):
            self.processor.loaddata(DoubleVector(self.dvector))
        if hasattr(self, "wvector"):
            self.processor.loadwavelet(DoubleVector(self.wvector))
        if self.__uses_noise and hasattr(self, "nvector"):
            self.processor.loadnoise(DoubleVector(self.nvector))
        self.processor.process()
        return self.processor.ideal_output()

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

        if hasattr(self, "dvector"):
            self.processor.loaddata(DoubleVector(self.dvector))
        if hasattr(self, "wvector"):
            self.processor.loadwavelet(DoubleVector(self.wvector))
        if self.__uses_noise and hasattr(self, "nvector"):
            self.processor.loadnoise(DoubleVector(self.nvector))
        self.processor.process()
        return self.processor.inverse_filter()


    def QCMetrics(self,prediction_error_key="prediction_error")->dict:
        """
        All decon algorithms compute a set of algorithm dependent quality
        control metrics.  The return is a Metadata container.
        All this wrapper really does is translate that return into
        a python dictionary that can be used as the base of a subdocument
        posting to outputs.  This method MUST ONLY BE CALLED after
        calling the process method of the C++ engine.
        """
        # the base of what is returned is an echo of the input parameter set
        qcmd = dict(self.md)
        # merge in an output of the implementations QCMetrics method
        qcmeth_output = dict(self.processor.QCMetrics())
        # warning this merge operator was only implemented in python 3.9
        qcmd = qcmd | qcmeth_output
        # always compute the prediction error
        perr = self._prediction_error()
        qcmd[prediction_error_key] = perr
        return dict(qcmd)

    def change_parameters(self, md):
        """
        Use this method to change the internal parameter setting of the
        processor.  It can only change the parameters for a particular
        algorithm.   A new instance of this class needs to be created if
        you need to switch to a different algorithm.   It does little
        more than call the read_metadata of the already loaded processor.
        All the scalar decon methods implement that method.

        :param md: is a mspass.Metadata object containing required parameters
        for the alternative algorithm.
        """
        self.md = Metadata(md)
        self.processor.read_metadata(self.md)

    @property
    def uses_noise(self):
        return self.__uses_noise

    @property
    def dwin(self):
        tws = self.md.get_double("deconvolution_data_window_start")
        twe = self.md.get_double("deconvolution_data_window_end")
        return TimeWindow(tws, twe)

    @property
    def nwin(self):
        if self.__uses_noise:
            tws = self.md.get_double("noise_window_start")
            twe = self.md.get_double("noise_window_end")
            return TimeWindow(tws, twe)
        else:
            return TimeWindow  # always initialize even if not used

    def _prediction_error(self)->float:
        """
        Small internal function used to compute prediction error of
        deconvolution operator defined as norm(ao-io)/norm(io) where
        norm is L2.
        """
        ao = self.actual_output()
        io = self.ideal_output()
        # with internal use can assume ao and io are the same length
        err = ao - io
        return np.linalg.norm(err.data)/np.linalg.norm(io.data)


@mspass_func_wrapper
def RFdecon(
    d,
    engine=None,
    alg="LeastSquares",
    pf="RFdeconProcessor.pf",
    wavelet=None,
    noisedata=None,
    wcomp=2,
    ncomp=2,
    QCdocument_key="RFdecon_properties",
    object_history=False,
    alg_name="RFdecon",
    alg_id=None,
    dryrun=False,
):
    """
    Use this function to compute conventional receiver functions
    from a single three component seismogram. In this function,
    an instance of wrapper class RFdeconProcessor will be built and
    initialized with alg and pf.

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

    :param d:  Seismogram input data.  See notes above about
      time span of these data.
    :type d:  Must be a `Seismogram` object or the function will throw
      a TypeError exceptionl
    :param engine:   optional instance of a RFdeconProcessor
      object.   By default the function instantiates an instance of
      a processor for each call to the function.   For algorithms
      like the multitaper based algorithms with a high initialization
      cost performance will improve by sending an instance to the
      function via this argument.
    :type engine:  None or an instance of `RFdeconProcessor`.
      When None (default) an instance of an `RFdeconProcessor` is
      created on entry based on the keyword defined by the `alg`
      argument.   The algorithm built into the instance of
      `RFdeconProcessor` is used if engine is not null.
    :param alg: The algorithm to be applied, used for initializing
       a RFdeconProcessor object.  Ignored if `engine` is used.
    :param pf: The pf file to be parsed, used for inititalizing a
       RFdeconProcessor.  Ignored if `engine` is used.
    :type pf:  string defining an absolute path for the file name
       or a path relative to a directory defined by PFPATH.
    :param wavelet:   vector of doubles (numpy array or the
       std::vector container internal to TimeSeries object) defining
       the wavelet to use to compute deconvolution operator.
       Default is None which assumes processor was set up to use
       a component of d as the wavelet estimate.
    :type wavelet:  None or an iterable vector container
       (in MsPASS that means a python array, a numpy array, or a DoubleVector)
    :param noisedata:  vector of doubles (numpy array or the
       std::vector container internal to TimeSeries object) defining
       noise data to use for computing regularization.  Not all RF
       estimation algorithms use noise estimators so this parameter
       is optional.   It can also be extracted from d depending on
       parameter file options.
    :type noisedata:  None or an iterable vector container
       (in MsPASS that means a python array, a numpy array, or a DoubleVector)
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
     :type wcomp:  int (must 0, 1, or 2)
     :param ncomp: component number to use to compute noise.  This is used
       only if the algorithm in processor requires a noise estimate.
       Normally it should be the same as wcomp and is by default (2).
     :type ncomp:  int (must be 0, 1, or 2)
     :param QCdocument_key:   A summary of the parameters defining the
        deconvolution operator (really a dump of the pf content used for
        creating the engine) and computed QC attributes are posted to a
        python dictionary.   That content is posted to the outputs
        Metadata container with the key defined by this argument.
        In MongoDB lingo that means when saved to the database the
        dictionary content associated with this key becomes a "subdocument".
     :type QCdocument_key:  string (default is "RFdecon_properties")
     :param object_history: boolean to enable or disable saving object
           level history.  Default is False.  Note this functionality is
           implemented via the mspass_func_wrapper decorator.
     :param alg_name:   When history is enabled this is the algorithm name
           assigned to the stamp for applying this algorithm.
           Default ("WindowData") should normally be just used.
           Note this functionality is implemented via the mspass_func_wrapper decorator.
     :param ald_id:  algorithm id to assign to history record (used only if
           object_history is set True.)
           Note this functionality is implemented via the mspass_func_wrapper decorator.
     :param dryrun:  When true only the arguments are checked for validity.
           When true nothing is calculated and the original data are returned.
           Note this functionality is implemented via the mspass_func_wrapper decorator.

    :return:  Normally returns Seismogram object containing the RF estimates.
     The orientations are always the same as the input.  If `return-wavelets` is set
     True returns a tuple with three components:  0 - `Seismogram` returned as with
     default, 1 - ideal output wavelet `TimeSeries`, 2 - actual output wavelet
     stored as a `TimeSeries` object.
    """

    if not isinstance(d,Seismogram):
        message = "RFdecon:  arg0 is of type={}.  Must be a Seismogram object".format(str(type(d)))
        raise TypeError(message)
    if d.dead():
        return d
    if engine:
        if isinstance(engine, RFdeconProcessor):
            processor = engine
        else:
            message = (
                "RFdecon:   illegal type for define by engine argment = {}\n".format(
                    type(engine)
                )
            )
            message += "If defined must be an instance of RFdeconProcessor"
            raise TypeError(message)
    else:
        processor = RFdeconProcessor(alg, pf)

    try:
        if wavelet is not None:
            processor.loadwavelet(wavelet, dtype="raw_vector")
        else:
            # processor.loadwavelet(d,dtype='Seismogram',window=True,component=wcomp)
            processor.loadwavelet(d, window=True, component=wcomp)
        if processor.uses_noise:
            if noisedata != None:
                processor.loadnoise(noisedata, dtype="raw_vector")
            else:
                processor.loadnoise(d, window=True, component=ncomp)
    except MsPASSError as err:
        d.kill()
        d.elog.log_error(err)
        return d
    # We window data before computing RF estimates for efficiency
    # Otherwise we would call the window operator 3 times below
    # WindowData does will kill the output if the window doesn't match
    # which is reason for the test immediately after this call
    result = WindowData(d, processor.dwin.start, processor.dwin.end)
    if result.dead():
        return result
    npts = result.npts
    try:
        for k in range(3):
            processor.loaddata(result, component=k)
            x = processor.apply()
            # overwrite this component's data in the result Seismogram
            # Use some caution handling any size mismatch
            nx = len(x)
            if nx >= npts:
                for i in range(npts):
                    result.data[k, i] = x[i]
            else:
                # this may not be the fastest way to do this but it is simple and clean
                # matters little since this is an error condition and should be rare
                for i in range(npts):
                    if i < nx:
                        result.data[k, i] = x[i]
                    else:
                        result.data[k, i] = 0.0
                # This is actually an error condition so we log it
                message = (
                    "Windowing size mismatch.\nData window length = %d which is less than operator length= %d"
                    % (nx, npts)
                )
                result.elog.log_error("RFdecon", message, ErrorSeverity.Complaint)
    except MsPASSError as err:
        result.kill()
        result.elog.log_error(err)
    except:
        print(
            "RFDecon:  something threw an unexpected exception - this is a bug and needs to be fixed.\nKilling result from RFdecon."
        )
        result.kill()
        result.elog.log_error(
            "RFdecon", "Unexpected exception caught", ErrorSeverity.Invalid
        )
    finally:
        # assume this method creates the dictionary we use as base for the
        # QC subdocument.  Note that always includes the prediction error
        subdoc = processor.QCMetrics()
        result[QCdocument_key] = subdoc
        return result
