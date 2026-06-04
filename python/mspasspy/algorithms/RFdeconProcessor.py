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
import warnings

from mspasspy.ccore.seismic import DoubleVector, PowerSpectrum, Seismogram, TimeSeries
from mspasspy.ccore.utility import AntelopePf, Metadata, MsPASSError, ErrorSeverity
from mspasspy.util.converter import Metadata2dict
from mspasspy.algorithms.window import WindowData
from mspasspy.ccore.algorithms.basic import TimeWindow, _ExtractComponent
from mspasspy.ccore.algorithms.deconvolution import (
    LeastSquareDecon,
    TimeDomainLeastSquareDecon,
    WaterLevelDecon,
    MultiTaperXcorDecon,
    MultiTaperSpecDivDecon,
    TimeDomainGIDDecon,
    FrequencyDomainGIDDecon,
)
from mspasspy.util.decorators import mspass_func_wrapper


def _as_gid_timeseries(x, dt, t0, argname):
    if isinstance(x, TimeSeries):
        return x
    if isinstance(x, DoubleVector):
        values = np.asarray(x, dtype=float)
    else:
        try:
            values = np.asarray(x, dtype=float)
        except Exception as err:
            raise TypeError(
                "RFdecon: for GID algorithms, {} must be a TimeSeries "
                "or one-dimensional numeric vector".format(argname)
            ) from err
    if values.ndim != 1:
        raise TypeError(
            "RFdecon: for GID algorithms, {} must be a TimeSeries "
            "or one-dimensional numeric vector".format(argname)
        )
    if values.size <= 0:
        raise ValueError(
            "RFdecon: for GID algorithms, {} must not be empty".format(argname)
        )
    ts = TimeSeries(len(values))
    ts.set_t0(t0)
    ts.set_dt(dt)
    ts.set_live()
    for i, value in enumerate(values):
        ts.data[i] = float(value)
    return ts


def _get_pf_branch_with_legacy_fallback(pfhandle, preferred, legacy):
    if preferred in pfhandle.arr_keys():
        return Metadata(pfhandle.get_branch(preferred))
    if preferred in pfhandle.tbl_keys() or pfhandle.is_defined(preferred):
        raise MsPASSError(
            f"{preferred} is defined but is not an &Arr parameter branch",
            ErrorSeverity.Invalid,
        )
    return Metadata(pfhandle.get_branch(legacy))


class RFdeconProcessor:
    """
    This class is a wrapper for the suite of receiver function deconvolution
    methods we call scalar methods.  That is, the operation is reducible to
    two time series:   wavelet signal and the data (TimeSeries) signal.
    That is in contrast to three component methods that always treat the
    data as vector samples.  The class should be created as a global
    processor object to be used in a spark job.  The design assumes the
    processor object will be passed as an argument to the RFdecon
    function that should appear as a function in a spark map call.

    Supported algorithm names are ``LeastSquares``, ``WaterLevel``,
    ``MultiTaperPowerXcor``, ``MultiTaperPowerSpecDiv``,
    ``TimeDomainLeastSquares``, ``GeneralizedIterative``/``TimeDomainGID``,
    and ``FrequencyDomainGID``.  ``MultiTaperXcor`` and
    ``MultiTaperSpecDiv`` are retained as deprecated compatibility aliases for
    the power-stabilized multitaper operators.
    The default scalar operators solve linear convolution problems and return
    the configured lag window rather than a wrapped circular-convolution
    result.  Frequency-domain operators use padding before FFT processing;
    ``TimeDomainLeastSquares`` builds a Toeplitz linear-convolution matrix.
    The GID variants iterate on a sparse impulse response and return the shaped
    receiver function for the configured output window.
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
        self.__is_3c_engine = False
        if pf == "RFdeconProcessor.pf":
            if alg in ("GeneralizedIterative", "TimeDomainGID"):
                pf = "TimeDomainGIDDecon.pf"
            elif alg == "FrequencyDomainGID":
                pf = "FrequencyDomainGIDDecon.pf"
        # use a copy in what is more or less a switch-case block
        # to be robust - I don't think any of the constructors below
        # alter pfhandle but the cost is tiny for this stability
        pfhandle = AntelopePf(pf)
        if self.algorithm == "LeastSquares":
            # In this and elif blocks below we convert
            # return of get_branch to a Metadata container
            # that is necessary because get_branch returns the
            # AntelopePf subclass and we want this to be a clean
            # Metadata object.  Further, at present a pf will not
            # serialize
            self.md = Metadata(pfhandle.get_branch("LeastSquare"))
            self.processor = LeastSquareDecon(self.md)
            self.__uses_noise = False
        elif self.algorithm == "TimeDomainLeastSquares":
            self.md = Metadata(pfhandle.get_branch("TimeDomainLeastSquare"))
            self.processor = TimeDomainLeastSquareDecon(self.md)
            self.__uses_noise = False
        elif alg == "WaterLevel":
            self.md = Metadata(pfhandle.get_branch("WaterLevel"))
            self.processor = WaterLevelDecon(self.md)
            self.__uses_noise = False
        elif alg in ("MultiTaperPowerXcor", "MultiTaperXcor"):
            if alg == "MultiTaperPowerXcor":
                self.md = _get_pf_branch_with_legacy_fallback(
                    pfhandle, "MultiTaperPowerXcor", "MultiTaperXcor"
                )
            else:
                self.md = Metadata(pfhandle.get_branch("MultiTaperXcor"))
            if alg == "MultiTaperXcor":
                warnings.warn(
                    "MultiTaperXcor is a deprecated compatibility name for "
                    "the multitaper power-stabilized untapered-phase operator. "
                    "Use MultiTaperPowerXcor for new code.",
                    DeprecationWarning,
                    stacklevel=2,
                )
            self.processor = MultiTaperXcorDecon(self.md)
            self.__uses_noise = True
        elif alg in ("MultiTaperPowerSpecDiv", "MultiTaperSpecDiv"):
            if alg == "MultiTaperPowerSpecDiv":
                self.md = _get_pf_branch_with_legacy_fallback(
                    pfhandle, "MultiTaperPowerSpecDiv", "MultiTaperSpecDiv"
                )
            else:
                self.md = Metadata(pfhandle.get_branch("MultiTaperSpecDiv"))
            if alg == "MultiTaperSpecDiv":
                warnings.warn(
                    "MultiTaperSpecDiv is a deprecated compatibility name for "
                    "the multitaper power-stabilized untapered-phase operator. "
                    "Use MultiTaperPowerSpecDiv for new code.",
                    DeprecationWarning,
                    stacklevel=2,
                )
            self.processor = MultiTaperSpecDivDecon(self.md)
            self.__uses_noise = True
        elif alg in ("GeneralizedIterative", "TimeDomainGID"):
            mdtop = pfhandle.get_branch("deconvolution_operator_type")
            self.md = Metadata(mdtop.get_branch("time_domain_gid_deconvolution"))
            self.processor = TimeDomainGIDDecon(pfhandle)
            self.__uses_noise = True
            self.__is_3c_engine = True
        elif alg == "FrequencyDomainGID":
            mdtop = pfhandle.get_branch("deconvolution_operator_type")
            self.md = Metadata(mdtop.get_branch("frequency_domain_gid_deconvolution"))
            self.processor = FrequencyDomainGIDDecon(pfhandle)
            self.__uses_noise = True
            self.__is_3c_engine = True
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
                wvector = w.data
            else:
                wvector = w
        # Have to explicitly convert to ndarray because DoubleVector cannot be serialized.
        self.wvector = np.array(wvector)
        if self.__is_3c_engine:
            if dtype == "TimeSeries" and not window:
                self.wtimeseries = TimeSeries(w)
            elif hasattr(self, "wtimeseries"):
                del self.wtimeseries

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
        if self.__is_3c_engine:
            if dtype == "TimeSeries" and not window:
                self.ntimeseries = TimeSeries(n)
            elif hasattr(self, "ntimeseries"):
                del self.ntimeseries

    def apply(self):
        """
        Compute the RF estimate using the algorithm defined internally.

        :return: vector of data that are the RF estimate computed from previously loaded data.
        """
        if self.__is_3c_engine:
            raise RuntimeError(
                "RFdeconProcessor.apply cannot process a three-component "
                "iterative engine component by component; use apply_3c instead"
            )
        if hasattr(self, "dvector"):
            self.processor.loaddata(DoubleVector(self.dvector))
        if hasattr(self, "wvector"):
            self.processor.loadwavelet(DoubleVector(self.wvector))
        if self.__uses_noise and hasattr(self, "nvector"):
            self.processor.loadnoise(DoubleVector(self.nvector))
        self.processor.process()
        return self.processor.getresult()

    def apply_3c(self, d):
        """
        Compute a three-component generalized iterative deconvolution result.

        The GID engines operate on a full `Seismogram` because the iteration
        picks vector spikes from all three components at once.  This method is
        intentionally separate from `apply`, which remains the scalar
        component-wise interface for conventional RF methods.
        """
        if not self.__is_3c_engine:
            raise RuntimeError("apply_3c is only valid for GID algorithms")
        target_dt = self.md.get_double("target_sample_interval")
        if hasattr(self, "wvector"):
            if hasattr(self, "wtimeseries"):
                self.processor.loadwavelet(self.wtimeseries)
            else:
                self.processor.loadwavelet(
                    _as_gid_timeseries(
                        self.wvector, target_dt, self.dwin.start, "wavelet"
                    )
                )
        if self.__uses_noise and hasattr(self, "nvector"):
            if hasattr(self, "ntimeseries"):
                self.processor.loadnoise(self.ntimeseries)
            else:
                self.processor.loadnoise(
                    _as_gid_timeseries(
                        self.nvector, target_dt, self.nwin.start, "noisedata"
                    )
                )
        try:
            load_status = self.processor.load(d, self.full_dwin, self.nwin)
        except MsPASSError as err:
            raise MsPASSError(
                "RFdeconProcessor.apply_3c: configured signal/noise windows "
                "could not be loaded from input data: {}".format(str(err)),
                ErrorSeverity.Invalid,
            )
        if load_status:
            raise MsPASSError(
                "RFdeconProcessor.apply_3c: configured signal/noise windows "
                "could not be loaded from input data",
                ErrorSeverity.Invalid,
            )
        self.processor.process()
        return Seismogram(self.processor.getresult())

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
            if self.__is_3c_engine:
                raise RuntimeError(
                    "actual_output for GID engines is available after apply_3c"
                )
            self.processor.loaddata(DoubleVector(self.dvector))
        if hasattr(self, "wvector"):
            self.processor.loadwavelet(DoubleVector(self.wvector))
        if self.__uses_noise and hasattr(self, "nvector"):
            self.processor.loadnoise(DoubleVector(self.nvector))
        self.processor.process()
        return self.processor.actual_output()

    def output_shaping_wavelet(self):
        """
        Return the output shaping wavelet, ws(t) in Wang and Pavlis (2016).

        For GID this is the configured wavelet used to convolve the sparse
        impulse response to form the finite-bandwidth receiver function.  For
        scalar operators it is the optional post-deconvolution
        shaping/bandlimiting wavelet.
        """
        if hasattr(self, "dvector"):
            if self.__is_3c_engine:
                raise RuntimeError(
                    "output_shaping_wavelet for GID engines is available "
                    "after apply_3c"
                )
            self.processor.loaddata(DoubleVector(self.dvector))
        if hasattr(self, "wvector"):
            self.processor.loadwavelet(DoubleVector(self.wvector))
        if self.__uses_noise and hasattr(self, "nvector"):
            self.processor.loadnoise(DoubleVector(self.nvector))
        self.processor.process()
        return self.processor.output_shaping_wavelet()

    def ideal_output(self):
        """
        Legacy alias for output_shaping_wavelet.

        New code should use output_shaping_wavelet to avoid confusing this
        diagnostic with the actual output/resolution kernel.
        """
        return self.output_shaping_wavelet()

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
            if self.__is_3c_engine:
                raise RuntimeError(
                    "inverse_filter for GID engines is available from the "
                    "underlying inverse_wavelet method after apply_3c"
                )
            self.processor.loaddata(DoubleVector(self.dvector))
        if hasattr(self, "wvector"):
            self.processor.loadwavelet(DoubleVector(self.wvector))
        if self.__uses_noise and hasattr(self, "nvector"):
            self.processor.loadnoise(DoubleVector(self.nvector))
        self.processor.process()
        return self.processor.inverse_wavelet()

    def QCMetrics(self, prediction_error_key="prediction_error") -> dict:
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
        qcmd.update(qcmeth_output)
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
    def is_3c_engine(self):
        return self.__is_3c_engine

    @property
    def dwin(self):
        tws = self.md.get_double("deconvolution_data_window_start")
        twe = self.md.get_double("deconvolution_data_window_end")
        return TimeWindow(tws, twe)

    @property
    def full_dwin(self):
        if self.md.is_defined("full_data_window_start"):
            tws = self.md.get_double("full_data_window_start")
            twe = self.md.get_double("full_data_window_end")
            return TimeWindow(tws, twe)
        return self.dwin

    @property
    def nwin(self):
        if self.__uses_noise:
            tws = self.md.get_double("noise_window_start")
            twe = self.md.get_double("noise_window_end")
            return TimeWindow(tws, twe)
        else:
            return TimeWindow  # always initialize even if not used

    def _prediction_error(self) -> float:
        """
        Small internal function used to compute prediction error of
        deconvolution operator defined as norm(ao-io)/norm(io) where
        norm is L2.
        """
        ao = self.actual_output()
        io = self.output_shaping_wavelet()
        # with internal use can assume ao and io are the same length
        err = ao - io
        return np.linalg.norm(err.data) / np.linalg.norm(io.data)


@mspass_func_wrapper
def RFdecon(
    d,
    *args,
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
    handles_ensembles=False,
    checks_arg0_type=True,
    handles_dead_data=True,
    **kwargs,
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
    to be three component data.  Conventional scalar methods accept
    optional wavelet and noisedata as plain numeric vectors.  GID methods
    accept prepared TimeSeries inputs or one-dimensional numeric vectors;
    raw vectors are converted to TimeSeries using the processor target
    sample interval and the configured deconvolution/noise window start.
    If any configured data, wavelet, or noise window cannot be extracted, the
    function returns a killed datum and does not attach the QC subdocument.

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
    setting the save_history argument to True.   When enabled one should
    normally set a unique id for the algid argument.

    :param d:  Seismogram input data.See notes above about time span of thesedata.
    :type d:  Must be a Seismogram object or the function will throw a TypeError exception.
    :param engine:   optional instance of a RFdeconProcessor
        object.   By default the function instantiates an instance of
        a processor for each call to the function.   For algorithms
        like the multitaper based algorithms with a high initialization
        cost performance will improve by sending an instance to the
        function via this argument.
    :type engine:  None or an instance of RFdeconProcessor.
        When None (default) an instance of an RFdeconProcessor is
        created on entry based on the keyword defined by the alg
        argument.   The algorithm built into the instance of
        RFdeconProcessor is used if engine is not null.
    :param alg: The algorithm to be applied, used for initializing
        a RFdeconProcessor object.  Ignored if engine is used.  Conventional
        scalar methods are applied component by component.  Generalized
        iterative methods (`GeneralizedIterative`, `TimeDomainGID`, and
        `FrequencyDomainGID`) operate on the full three-component seismogram
        because spike selection is vector-valued.  By default they preserve the
        receiver-function convention of deriving the source wavelet from
        component 2, but callers may pass a prepared TimeSeries wavelet or a
        one-dimensional numeric vector to use the same external wavelet for all
        components.
    :param pf: The pf file to be parsed, used for inititalizing a
        RFdeconProcessor.  Ignored if engine is used.
    :type pf:  string defining an absolute path for the file name
        or a path relative to a directory defined by PFPATH.
    :param wavelet:   vector of doubles (numpy array or the
        std::vector container internal to TimeSeries object) or TimeSeries
        defining the wavelet to use to compute deconvolution operator.
        Default is None which assumes processor was set up to use
        a component of d as the wavelet estimate.
        For GID algorithms, raw vectors are converted to a TimeSeries using
        target_sample_interval and deconvolution_data_window_start.
    :type wavelet:  None, TimeSeries, or an iterable vector container
        (in MsPASS that means a python array, a numpy array, or a DoubleVector)
    :param noisedata:  vector of doubles (numpy array or the
        std::vector container internal to TimeSeries object), TimeSeries, or
        PowerSpectrum defining noise data to use for computing regularization.
        Not all RF
        estimation algorithms use noise estimators so this parameter
        is optional.   It can also be extracted from d depending on
        parameter file options.
        For GID algorithms, raw vectors are converted to a TimeSeries using
        target_sample_interval and noise_window_start.
    :type noisedata:  None, TimeSeries, PowerSpectrum, or an iterable vector container
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
    :param alg_id:  algorithm id to assign to history record (used only if
        object_history is set True.)
        Note this functionality is implemented via the mspass_func_wrapper decorator.
    :param dryrun:  When true only the arguments are checked for validity.
        When true nothing is calculated and the original data are returned.
        Note this functionality is implemented via the mspass_func_wrapper decorator.

    :return:  Returns a Seismogram object containing the RF estimates.
        The orientations are always the same as the input.
    """

    if not isinstance(d, Seismogram):
        message = "RFdecon:  arg0 is of type={}.  Must be a Seismogram object".format(
            str(type(d))
        )
        raise TypeError(message)
    if d.dead():
        return d
    if engine:
        if isinstance(engine, RFdeconProcessor):
            processor = engine
        else:
            message = (
                "RFdecon:   illegal type for defined engine argument = {}\n".format(
                    type(engine)
                )
            )
            message += "If defined must be an instance of RFdeconProcessor"
            raise TypeError(message)
    else:
        processor = RFdeconProcessor(alg, pf)

    if processor.is_3c_engine:
        try:
            target_dt = processor.md.get_double("target_sample_interval")
            if wavelet is not None:
                wts = _as_gid_timeseries(
                    wavelet,
                    target_dt,
                    processor.dwin.start,
                    "wavelet",
                )
                processor.processor.loadwavelet(wts)
            else:
                processor.processor.clear_external_wavelet()
                for attr in ("wvector", "wtimeseries"):
                    if hasattr(processor, attr):
                        delattr(processor, attr)
            if noisedata is not None:
                if isinstance(noisedata, PowerSpectrum):
                    processor.processor.loadnoise(noisedata)
                else:
                    nts = _as_gid_timeseries(
                        noisedata,
                        target_dt,
                        processor.nwin.start,
                        "noisedata",
                    )
                    processor.processor.loadnoise(nts)
            else:
                processor.processor.clear_external_noise()
                for attr in ("nvector", "ntimeseries"):
                    if hasattr(processor, attr):
                        delattr(processor, attr)
            result = processor.apply_3c(d)
            subdoc = dict(processor.processor.QCMetrics())
            subdoc["algorithm"] = processor.algorithm
            result[QCdocument_key] = subdoc
            return result
        except MsPASSError as err:
            d.kill()
            d.elog.log_error(err)
            return d
        except Exception as err:
            d.kill()
            d.elog.log_error("RFdecon", str(err), ErrorSeverity.Invalid)
            return d

    try:
        if wavelet is not None:
            processor.loadwavelet(wavelet, dtype="raw_vector")
        else:
            # processor.loadwavelet(d,dtype='Seismogram',window=True,component=wcomp)
            processor.loadwavelet(d, window=True, component=wcomp)
        if processor.uses_noise:
            if noisedata is not None:
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
    except Exception as err:
        result.kill()
        result.elog.log_error(
            "RFdecon",
            "Unexpected exception caught: {}".format(str(err)),
            ErrorSeverity.Invalid,
        )
    finally:
        if result.dead():
            return result
        # assume this method creates the dictionary we use as base for the
        # QC subdocument.  Note that always includes the prediction error
        subdoc = processor.QCMetrics()
        result[QCdocument_key] = subdoc
        return result
