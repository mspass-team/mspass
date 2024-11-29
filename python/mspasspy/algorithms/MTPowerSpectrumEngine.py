#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import numpy as np
from multitaper.mtspec import MTSpec
from mspasspy.ccore.utility import MsPASSError, ErrorSeverity, Metadata
from mspasspy.ccore.seismic import TimeSeries, DoubleVector, PowerSpectrum


class MTPowerSpectrumEngine:
    """
    Wrapper class to use German Prieto's multitaper package as a plug
    in replacement for the MsPASS class of the same name.   The MsPASS
    version is in C++ and there are some collisions in concept.  It should
    be used only in a python script where it might be useful to use
    features of Prieto's library not available in the cruder implementation
    in MsPASS.   (e.g. adaptive weights in multitaper power spectrum estimation).

    The biggest collision in concept is that the MsPASS version with this class
    name was designed to be created and moved around to avoid the overhead of
    recreating the Slepian tapers on each use.  Prieto's class creates these
    in the constructor but caches a number of things when first created that
    allows secondary calls to run faster.  The implementation attempts
    to overcome this collision by caching an instance of Prieto's mstspec class
    on the first call to the apply method.   Secondary calls to apply test
    for consistency with the previous call and only recreate the mtspec
    instance if something that invalidates the previous call has changed.

    :param winsize:   Number of samples expected for data window.  Note this
      argument defines the size of the eigentapers so it defines the size of
      input time series the object expect to process.
    :param tbp:  multitaper time-bandwidth product
    :param number_tapers:  number of tapers to use for estimators (See Prieto's)
      documentation for details)
    :param nfft:  optional size of the fft work arrays to use.  nfft should be
      greater than or equal winsize.  When greater zero padding is used in the
      usual way.  Default is 0 which sets nfft to 2*winsize.  If nfft<winsize
      a diagnostic message will be printed and nfft will be set equal to winsize.
    :param iadapt:  integer argument passed to Prieto's MTspec that sets the
      eigentaper weighting scheme.  See Prieto's MTspect class documentation
      for options.  Default is 0 which enables the adaptive weighting scheme.
    """

    def __init__(
        self,
        winsize,
        tbp,
        number_tapers,
        nfft=0,
        iadapt=0,
    ):
        self.winsize = winsize
        self.tbp = tbp  # nw in multitaper
        self.number_tapers = number_tapers  # kspec in multitaper
        self.vn = None  # constructor for MTSpec will set this when passed None
        self.nfft = nfft
        self.idapt = iadapt
        self.lamb = None  # constructor for MTSpec set this
        self.MTSpec_instance = None

    def apply(self, d, dt=1.0):
        """
        need to support vector input or a TimeSeries - returns a PowerSpectrum

        """
        # All inputs end up filling a numpy array passed to MTSpec below
        if isinstance(d, TimeSeries):
            y = np.array(d.data)
            dt = d.dt
            md = Metadata(d)
        elif isinstance(d, DoubleVector):
            # pybind11 bindings defined std::vector as a DoubleVector
            # This is exactly like the use for TimeSeries where data
            # is a DoubleVector
            y = np.array(d)
            md = Metadata()
            # These are mspass schema standard names - could be
            # a future maintenance issue
            md["npts"] = len(d)
            md["delta"] = dt
        elif isinstance(d, np.ndarray):
            y = d
            md = Metadata()
            # These are mspass schema standard names - could be
            # a future maintenance issue
            md["npts"] = len(d)
            md["delta"] = dt
        else:
            raise TypeError(
                "MTPowerSpectrumEngine.apply:  arg0 has invalid type - must be TimeSeries, DoubleVector, or numpy array"
            )
        self.MTSpec_instance = MTSpec(
            y,
            nw=self.tbp,
            kspec=self.number_tapers,
            dt=dt,
            nfft=self.nfft,
            iadapt=self.idapt,
            vn=self.vn,
            lamb=self.lamb,
        )
        # Set these so they will be automatically reused on a
        # successive call.   May need to trap size mismatch above
        # if these were previously set - this needs some testing
        #  REMOVE ME WHEN VERIFIED
        self.vn = self.MTSpec_instance.vn
        self.lamb = self.MTSpec_instance.lamb
        self.nfft = self.MTSpec_instance.nfft
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
        # This method returns only the positive frequencies and spectra values
        f, spec = self.MTSpec_instance.rspec()
        # this is an obnoxious collision with the C++ api DoubleVector
        npts = self.MTSpec_instance.npts
        work = DoubleVector()
        for i in range(len(spec)):
            work.append(spec[i])

        result = PowerSpectrum(
            md,
            work,
            self.MTSpec_instance.df,
            "multitaper.MTSpec",
            0.0,
            dt,
            md["npts"],
        )
        return result

    def frequencies(self):
        """
        Return a numpy array of the frequencies
        """
        if self.MTSpec_instance:
            return self.MTSpec_instance.freq
        else:
            raise MsPASSError(
                "MTPowerSpectrumEngine.frequencies:   engine has not been initialized.  Method undefined until apply method called",
                ErrorSeverity.Fatal,
            )

    def time_bandwidth_product(self) -> float:
        """
        Return time bandwidth product for this operator.
        """
        if self.MTSpec_instance:
            return self.MTSpec_instance.nw
        else:
            raise MsPASSError(
                "MTPowerSpectrumEngine.time_bandwidth_product:   engine has not been initialized.  Method undefined until apply method called",
                ErrorSeverity.Fatal,
            )

    def number_tapers(self) -> int:
        """
        Return the number of tapers defined for this operator.
        """
        if self.MTSpec_instance:
            return self.MTSpec_instance.kspec
        else:
            raise MsPASSError(
                "MTPowerSpectrumEngine.number_tapers:   engine has not been initialized.  Method undefined until apply method called",
                ErrorSeverity.Fatal,
            )

    def set_df(self, df):
        """
        Explicit setter for frequency bin interval.  Needed when
        changing sample interval for input data. Rarely of use but
        included for compatibility with the C++ class with the
        same name.

        This maybe should just be pass or throw an error.
        """
        self.MTSpec_instance.df = df
