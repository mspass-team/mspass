from abc import ABC, abstractmethod
from mspasspy.ccore.utility import MsPASSError, ErrorSeverity, dmatrix
from mspasspy.ccore.seismic import (
    TimeSeries,
    Seismogram,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
    DoubleVector,
)
from mspasspy.util.converter import (
    Trace2TimeSeries,
    TimeSeries2Trace,
    Seismogram2Stream,
    Stream2Seismogram,
    SeismogramEnsemble2Stream,
    Stream2SeismogramEnsemble,
)

import numpy as np
from scipy import signal


class BasicResampler(ABC):
    """
    Base class for family of resampling operators.   All this class really does is
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
    a typical instance of this class (in the example ScipyResampler)
    would be used in  parallel workflow sketch:

    .. rubric:: Example

    resamp_op = ScipyResampler(10.0)   # target sample rate of 10 sps
    cursor = db.TimeSeries.find({})
    bag = read_distributed_data(cursor,collection="wf_TimeSeries")
    bag = bag.map(resamp_op.resample)
    bag.map(db.save_data())
    bag.compute()

    A point to emphasize is the model is to generate the operator
    through a constructor (ScipResampler in the example) that defines
    the target sample rate.  All data passed through that operator
    through the map operator call will be returned to create a bag/rdd
    with a uniform sample rate.   All implementations should also
    follow the MsPASS rule for parallel algorithms to kill data that
    cannot be handled and not throw exceptions unless the whole usage is
    wrong.
    """

    def __init__(self, dt=None, sampling_rate=None):
        if dt and sampling_rate:
            raise MsPASSError(
                "BasicResample:  usage error.  Specify either dt or sampling_rate.  You defined both",
                ErrorSeverity.Fatal,
            )
        if dt:
            self.dt = dt
            self.samprate = sampling_rate
        elif sampling_rate:
            self.dt = 1.0 / sampling_rate
            self.samprate = sampling_rate

    def target_samprate(self):
        return self.samprate

    def target_dt(self):
        return self.dt

    def dec_factor(self, d):
        """
        All implementations of decimation algorithms should use this
        method to test if resampling by decimation is feasible.
        A decimation operator only works for downsampling with
        the restriction that the ouptut sample interval is an integer
        multiple of the input sample interval.

        The function returns a decimation factor to use for atomic data d
        being tested.  The algorithm uses the numpy "isclose"
        function to establish if the sample interval is feasible.
        If so it returns the decimation factor as an integer that should be
        used on d.  Not implementations should handle a return of 1
        specially for efficiency.  A return of 1 means no resampling
        is needed.  A return of 0 or -1 is used for two slightly different
        cases that indicate a decimation operation is not feasible.
        A return of 0 should be taken as a signal that the data requires
        upsampling to match the target sampling rate (interval).
        A return of -1 means the data require downsampling but the
        a decimator operator is not feasible.  (e.g. needing to create
        10 sps data from 25 sps input.)


        :param d:   input mspass data object to be tested.   All that is
        actually required is d have a "dt" attribute (i.e. d.dt is defined)
        that is the sample interval for that datum.
        :type d:   assumed to be a MsPASS atomic data object for which the
        dt attribute is defined.  This method has no safeties to test
        input type.  It will throw an exception if d.dt does not resolve.
        """
        # internal use guarantees this can only be TimeSeries or Seismogram
        # so this resolves
        d_dt = d.dt
        float_dfac = self.dt / d_dt
        int_dfac = int(float_dfac)
        # This perhaps should use a softer constraint than default
        if np.isclose(float_dfac, float(int_dfac)):
            return int_dfac
        elif int_dfac == 0:
            return 0
        else:
            return -1

    @abstractmethod
    def resample(self, mspass_object):
        """
        Main operator a concrete class must implement.  It should accept
        any mspass data object and return a clone that has been resampled
        to the sample interface defined by this base class.

        """
        pass


class ScipyResampler(BasicResampler):
    """
    This class is a wrapper for the scipy resample algorithm.  Obspy users
    should note that the Trace and Stream method called "resample"
    is only a light wrapper to apply the scipy resample function.

    The algorithm and its limitations are described in the scipy
    documentation you can easily find with a web search.   A key point
    about this algorithm is that unlike decimate it allows resampling to
    something not an integer multiple or division from the input OR
    if you need to upsample data to match the rest of the data set
    (Note that is not usually a good idea unless the upsampling is followed
     by a decimator to get all data to a lower, uniform sample rate.)
    A type example where that is essential is some old OBS data from
    Scripps instruments that had a sample rate that was a multiple of
    one of the more standard rates like 20 or 100.  Such data can be
    downsampled immediately too something like 10 sps with this operator
    or upsampled to something like 50 and then downsampled to something
    like 10 with a factor of 5 decimator.

    We emphasize a nice feature of the scipy implementation is that
    it automatically applies a rational antialiasing filter when downsampling,
    If, however, you need to do something like regularize a data set with
    irregular sample rates but preserve a common upper frequency response
    controlled at the high frequency end by digizer antialias filters
    (e.g. LH channels from Q330 data) you will need to crack the
    scipy documentation on setting up a custom antialias filter using
    FIR filters defined through the window argument.  All common digitizer
    FIR filter coefficients can be found in appropriate response files.
    That should, in principle, be feasible but mspass developers have
    not tested that hypothesis.

    The concept of the window argument in this constructor is idential
    to that described in the documentation for scipy.signal.resample.
    The value passed, in fact, is used as the argument whenever the scipy
    function is called.   Note the other optional arguments to scipy
    resample are always defaulted because the current default apply to
    all cases we handle.  Be careful if resample changes.

    The primary method of this class is a concrete implementation of the
    resample method.

    """

    def __init__(self, sampling_rate, window="hann"):
        """ """
        super().__init__(sampling_rate=sampling_rate)
        self.window = window

    def resample(self, mspass_object):
        """
        Applies the scipy.signal.resample function to all data held in
        a mspass container passed through arg0 (mspass_object).
        This method will accept all supported MsPASS datat objects:
        TimeSeries, Seismogram, TimeSeriesEnsemble, and SeismogramEnsemble.
        For Ensembles the method is called recursively on each of the
        members.

        The method returns mspass_object with the sample data altered
        by the operator defined by a particular instance, which is defined
        exclusively by the target sample rate for the output.  All metadata
        will be clone without checking.  If the metadata have attributes
        linked to the sample interval the metadata of the result may not
        match the data.

        If the input is marked dead it will be returned immediately with
        no change.
        """
        # We do this test at the top to avoid having returns testing for
        # a dead datum in each of the if conditional blocks below
        if isinstance(
            mspass_object,
            (TimeSeries, Seismogram, TimeSeriesEnsemble, SeismogramEnsemble),
        ):
            if mspass_object.dead():
                return mspass_object
        else:
            message = "ScipyResampler.resample: received unsupported data type=" + str(
                type(mspass_object)
            )
            raise TypeError(message)

        if isinstance(mspass_object, TimeSeries):
            data_time_span = (
                mspass_object.endtime() - mspass_object.t0 + mspass_object.dt
            )
            n_resampled = int(data_time_span * self.samprate)
            rsdata = signal.resample(
                mspass_object.data, n_resampled, window=self.window
            )
            mspass_object.set_npts(n_resampled)
            mspass_object.dt = self.dt
            # We have to go through this conversion to avoid TypeError exceptions
            # i.e we can't just copy the entire vector rsdata to the data vector
            dv = DoubleVector(rsdata)
            mspass_object.data = dv
        elif isinstance(mspass_object, Seismogram):
            data_time_span = (
                mspass_object.endtime() - mspass_object.t0 + mspass_object.dt
            )
            n_resampled = int(data_time_span * self.samprate)
            rsdata = signal.resample(
                mspass_object.data, n_resampled, window=self.window, axis=1
            )
            mspass_object.set_npts(n_resampled)
            mspass_object.dt = self.dt
            # We have to go through this conversion to avoid TypeError exceptions
            # i.e we can't just copy the entire vector rsdata to the data vector
            dm = dmatrix(rsdata)
            mspass_object.data = dm
        else:
            # The else above is equivalent to the following:
            # elif isinstance(mspass_object,(TimeSeriesEnsemble,SeismogramEnsemble)):
            # Change if additional data object support is added
            for d in mspass_object.member:
                self.resample(d)

        return mspass_object


class ScipyDecimator(BasicResampler):
    """
    This class defines a generic operator where a decimator can be used
    to downsample any input data to a common sample rate.  A decimator
    requires the ratio of the input to output sample rate to be an
    integer (equivalently the ratio of the output sample interval to the
    input sample interval).  The operator will fail on any data it
    receives that are irregular in that sense.   For example, 10 sps
    data can be created by downsampling 40 sps data by a factor of 4.
    In constract, 10 sps can not be created by decimation of 25 sps
    data because the ratio is 2.5 (not an integer).

    This operator satisfies the concept defined in BasicResampler.
    That is, a particular concrete instance once constructed will
    define an operator that will resample any input data to a common sample
    rate/interval.  Because of the requirement of this algorithm that
    the sample intervals/rates are related by integers the operator
    has to be able to handle irregular sample rate data.  The algorithm
    is brutal and will kill any datum for which the integer test fails
    and post an elog message.

    This operator is really little more than a wrapper around a
    scipy function with a similar same name (scipy.signal.decimate).
    The resample method handles any supported MsPASS data object type
    but will fail with a TypeError if it receives any other data type.

    Be aware decimators all have unavoidable edge effects.   The anitialias
    filter that has to be applied (you can get garbage otherwise) will always
    produce an edge transient.  A key to success with any downsampling
    operator is to always have a pad zone if possible.  That is, you start
    with a longer time window than you will need for final processing and
    discard the pad zone when you enter the final stage.   Note that is
    actually true of ALL filtering.

    The constructor has almost the same arguments defined in the documentation
    page for scipy.signal.decimate.   The only exception is the axis
    argument.  It needs to be defined internally.
    For scalar data we pass 0 while for three component data we sent it 1 which is
    means the decimator is applied per channel.
    """

    def __init__(self, sampling_rate, n=None, ftype="iir", zero_phase=True):
        """ """
        super().__init__(sampling_rate=sampling_rate)
        self.ftype = ftype
        self.zero_phase = zero_phase
        self.order = n

    def _make_illegal_decimator_message(self, error_code, data_dt):
        """
        Private method to format a common message if the data's sample
        interval, data_dt, is not feasible to produce by decimation.
        The error message is the return
        """
        if error_code == 0:
            message = "Data sample interval={ddt} is smaller than target dt={sdt}.  This operator can only downsample".format(
                ddt=data_dt, sdt=self.dt
            )
        else:
            message = (
                "Data sample interval={ddt} is not an integer multiple of {sdt}".format(
                    ddt=data_dt, sdt=self.dt
                )
            )
        return message

    def resample(self, mspass_object):
        """
        Implementation of required abstract method for this operator.
        The only argument is mspass_object.   The operator will downsample
        the contents of the sample data container for any valid input.
        If the input is not a mspass data object (i.e. atomic TimeSeries
        or Seismogram) or one of the enemble objects it will throw a
        TypeError exception.

        Note for ensembles the algorithm simply applies this method in
        a loop over all the members of the ensemble.  Be aware that any
        members of the ensemble cannot be resampled to the target sampling
        frequency (interval) they will be killed.  For example, if you
        are downsampling to 20 sps and you have 25 sps data in the ensemble
        the 25 sps data will be killed on output with an elog message
        posted.

        Returns an edited clone of the input with revised sample data but
        no changes to any Metadata.
        """
        # We do this test at the top to avoid having returns testing for
        # a dead datum in each of the if conditional blocks below
        if isinstance(
            mspass_object,
            (TimeSeries, Seismogram, TimeSeriesEnsemble, SeismogramEnsemble),
        ):
            if mspass_object.dead():
                return mspass_object
        else:
            message = "ScipyDecimator.resample: received unsupported data type=" + str(
                type(mspass_object)
            )
            raise TypeError(message)

        if isinstance(mspass_object, TimeSeries):
            decfac = self.dec_factor(mspass_object)
            if decfac <= 0:
                mspass_object.kill()
                message = self._make_illegal_decimator_message(decfac, mspass_object.dt)
                mspass_object.elog.log_error(
                    "ScipyDecimator.resample", message, ErrorSeverity.Invalid
                )
            else:
                dsdata = signal.decimate(
                    mspass_object.data,
                    decfac,
                    n=self.order,
                    ftype=self.ftype,
                    zero_phase=self.zero_phase,
                )
                dsdata_npts = len(dsdata)
                mspass_object.set_npts(dsdata_npts)
                mspass_object.dt = self.dt
                # We have to go through this conversion to avoid TypeError exceptions
                # i.e we can't just copy the entire vector rsdata to the data vector
                mspass_object.data = DoubleVector(dsdata)

        elif isinstance(mspass_object, Seismogram):
            decfac = self.dec_factor(mspass_object)
            if decfac <= 0:
                mspass_object.kill()
                message = self._make_illegal_decimator_message(decfac, mspass_object.dt)
                mspass_object.elog.log_error(
                    "ScipyDecimator.resample", message, ErrorSeverity.Invalid
                )
            else:
                # note axis=1 means apply the decimator along the column
                # index - that means by channel.
                dsdata = signal.decimate(
                    mspass_object.data,
                    decfac,
                    axis=1,
                    n=self.order,
                    ftype=self.ftype,
                    zero_phase=self.zero_phase,
                )
                # Seismogram stores data as a 3xnpts matrix.  numpy
                # uses the shape attribute to hold rowsxcolumns
                msize = dsdata.shape
                dsdata_npts = msize[1]
                mspass_object.set_npts(dsdata_npts)
                mspass_object.dt = self.dt
                # We have to go through this conversion to avoid TypeError exceptions
                # i.e we can't just copy the entire vector rsdata to the data vector
                mspass_object.data = dmatrix(dsdata)

        else:
            # else here is equivalent to this:
            # elif isinstance(mspass_object,(TimeSeriesEnsemble,SeismogramEnsemble)):
            # Change if we add support for additional data objects like gather
            # version of ensemble currently under construction
            for d in mspass_object.member:
                d = self.resample(d)

        return mspass_object


def resample(mspass_object, decimator, resampler, verify_operators=True):
    """
    Resample any valid data object to a common sample rate (sample interval).

    This function is a wrapper that automates handling of resampling.
    Its main use is in a dask/spark map operator where the input can
    be a set of irregularly sampled data and the output is required to be
    at a common sample rate (interval).   The problem has some complexity
    because decimation is normally the preferred method of resampling
    when possible due to speed and more predictable behavior.
    The problem is that downsampling by decimation is only possible if
    the output sampling interval is an integer multiple of the input
    sample interval.   With modern seismology data that is usually
    possible, but there are some common exceptions.  For example,
    10 sps cannot be created from 25 sps by decimation.   The algorithm
    tests the data sample rate and if decimation is possible it
    applies a decimation operator passed as the argument "decimator".
    If not, it calls the operator "resampler" that is assumed to be
    capable of handling any sample rate change.   The two operators
    must have been constructed with the same output target sampling
    frequency (interval).  Both must also be a subclass of BasicResampler
    to match the api requirements.

    :param mspass_object:   mspass datum to be resampled
    :type mspass_object:  Must a TimeSeries, Seismogram, TimeSeriesEnsemble,
      or SeismogramEnsemble object.
    :param decimator:   decimation operator.
    :type decimator:  Must be a subclass of BasicResampler
    :param resampler:  resampling operator
    :type resampler:  Must be a subclass of BasicResampler
    :param verify_operators: boolean controlling whether safety checks
      are applied to inputs.  When True (default) the contents of
      decimator and resampler are verified as subclasses of BasicResampler
      and the function tests if the target output sampling frequency (interval)
      of both operators are the same.  The function will throw an exception if
      any of the verify tests fail.   Standard practice should be to verify
      the operators and valid before running a large workflow and running
      production with this arg set False for efficiency.  That should
      acceptable in any case I can conceive as once the operators are
      defined in a parallel workflow they should be invariant for the
      entire application in a map operator.

    """
    if verify_operators:
        if not isinstance(decimator, BasicResampler):
            raise TypeError(
                "resample:  decimator operator (arg1) must be subclass of BasicResampler"
            )
        if not isinstance(resampler, BasicResampler):
            raise TypeError(
                "resample:  resampler operator (arg2) must be subclass of BasicResampler"
            )
        if not np.isclose(decimator.target_dt(), resampler.target_dt()):
            raise MsPASSError(
                "resample:  decimator and resampler must have the same target sampling rate",
                ErrorSeverity.Fatal,
            )
    if isinstance(mspass_object, (TimeSeries, Seismogram)):
        # This method returns -1 if decimation is not possible because
        # sampling frequency is not an integer multiple of the target
        # df defined in decimator.  Use that as switch to enable
        # resampling
        decfac = decimator.dec_factor(mspass_object)
        if decfac == 1:
            return mspass_object
        elif decfac > 0:
            return decimator.resample(mspass_object)
        else:
            return resampler.resample(mspass_object)
    elif isinstance(mspass_object, (TimeSeriesEnsemble, SeismogramEnsemble)):
        if mspass_object.dead():
            return mspass_object
        # I tried to do this loop with recursion but spyder kept
        # flagging it as an error - I'm not sure it would be.
        # The logic for atomic data is simple anyway and this
        # might actually be faster avoiding the function calls
        nmembers = len(mspass_object.member)
        for i in range(nmembers):
            d = mspass_object.member[i]
            decfac = decimator.dec_factor(d)
            # Note when decfac is 1 the current member is not altered
            if decfac > 1:
                mspass_object.member[i] = decimator.resample(d)
            elif decfac <= 0:
                mspass_object.member[i] = resampler.resample(d)
        return mspass_object

    elif mspass_object is None:
        # Handle this automatically for robustness.  Just silently return
        return mspass_object
    else:
        #  It should be an exception if we get anything else
        raise TypeError("resample:   arg0 must be a MsPASS data object")
