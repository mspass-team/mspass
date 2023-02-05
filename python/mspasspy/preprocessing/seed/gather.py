from abc import ABC, abstractmethod
import xarray as xr
import dask.array as da
import dask
import zarr
import numpy as np
import pandas as pd
from mspasspy.ccore.seismic import (
    TimeSeriesEnsemble,
    SeismogramEnsemble,
    TimeSeries,
    Seismogram,
    DoubleVector,
)

from mspasspy.ccore.utility import (
    Metadata,
    MsPASSError,
    AtomicType,
    ErrorSeverity,
    dmatrix,
    ProcessingHistory,
)

from mspasspy.ccore.utility import MsPASSError, ErrorSeverity, ErrorLogger
from mspasspy.algorithms.window import WindowData


def isOldEnsembleObject(obj):
    supported_ensemble_types = (TimeSeriesEnsemble, SeismogramEnsemble)
    return isinstance(obj, supported_ensemble_types)


def isMsPassObject(obj):
    return isinstance(obj, (TimeSeries, Seismogram))


def extractDataFromMsPassObject(mspass_object):
    """
    mspass_object is either a seismogram or a timeseries, mspass_object.data
    is a DoubleVector/dmatrix, we want to convert it extract the data from it and
    convert it into a numpy array
    The return type is np.ndarray

    :param mspass_object:   Seismogram or TimeSeries to extract data from
    """
    if not isMsPassObject(mspass_object):
        raise TypeError("Can't extract data from the object, not a mspass object")
    return np.array(mspass_object.data)


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
    if not isinstance(mspass_object, (TimeSeriesEnsemble, SeismogramEnsemble)):
        raise TypeError("resample:   arg0 must be a MsPASS data object")

    resampled_obj = mspass_object
    if not mspass_object.dead():
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
                resampled_obj.member[i] = decimator.resample(d)
            elif decfac <= 0:
                resampled_obj.member[i] = resampler.resample(d)
        starttime = min([mspass_object.member[i].t0 for i in range(nmembers)])
        endtime = max([mspass_object.member[i].endtime() for i in range(nmembers)])
        resampled_obj = WindowData(resampled_obj, starttime, endtime)
    return resampled_obj


def extractDataFromOldEnsemble(ensemble_object, decimator, resampler):
    """
    A wrapper function to extract data data from an old ensemble object, works by
    iterating the member in the ensemble and calling extractDataFromMsPassObject
    on each of them.
    Note that we want to make sure that all member data have the same starttime
    and sample rate, so resample is called on the ensemble object before doing the
    actual data handling.

    :param ensemble_object: TimeSeriesEnsemble or SeismogramEnsemble
    :param decimator:   decimation operator.
    :type decimator:  Must be a subclass of BasicResampler
    :param resampler:  resampling operator
    :type resampler:  Must be a subclass of BasicResampler
    """

    if not isOldEnsembleObject(ensemble_object):
        raise TypeError(
            "Can't extract data from the object, not an old ensemble object"
        )
    if len(ensemble_object.member) <= 0:
        raise TypeError("The input object doesn't have any member data")
    shape = [len(ensemble_object.member), 1, ensemble_object[0].npts]
    if isinstance(ensemble_object, SeismogramEnsemble):
        shape = [len(ensemble_object.member), 3, ensemble_object[0].npts]
    ret = np.empty(shape)
    for i in range(len(ensemble_object.member)):
        item_data = extractDataFromMsPassObject(ensemble_object[i])
        if isinstance(ensemble_object, TimeSeriesEnsemble):
            item_data = item_data.reshape(1, ensemble_object[0].npts)
        ret[i] = item_data
    resample(ret, decimator, resampler)
    return ret


def extractMemberMetadataFromOldEnsemble(ensemble_object):
    """
    A helper function to extract the member metadata from the old ensemble object
    It iterates all the members, reads the metadata and concatenate them into one
    single dataframe.

    :param ensemble_object: TimeSeriesEnsemble or SeismogramEnsemble
    """
    if not isOldEnsembleObject(ensemble_object):
        raise TypeError(
            "Can't extract metadata from the object, not an old ensemble object"
        )
    if len(ensemble_object.member) <= 0:
        raise TypeError("The input object doesn't have any member data")

    metadata_list = [
        dict(ensemble_object.member[i]) for i in range(len(ensemble_object.member))
    ]
    member_metadata = pd.DataFrame(metadata_list)

    is_live = [
        ensemble_object.member[i].live for i in range(len(ensemble_object.member))
    ]

    member_metadata["is_live"] = is_live

    return member_metadata


class BasicGather(ABC):
    """
    Base class for new MsPASS ensemble class that utilizes array
    implementations to store the data.  The implementtion may or may not
    provide for arrays that are too large to fit in memory.
    That behavior should be controlled by parameters on constructin.

    The data object defined by this class and it's children needs to be
    understood as appropriate only for ensemble data that satisfy several
    restrictions that the current MsPASS ensemble objects
    (TimeSeriesEnsemble and SeismogramEnsemble) do not rqequire:
        1.  All the data have the same sample rate.
        2.  Each signal has the same number of samples.
        3.  The data cannot have gaps.   Data with gaps neeed to be discarded
            or patched (e.g. with a gap fill) before being loaded into
            one of these contaienrs.

    Methods for this base class are largely getters and setters for
    univeral parameters that match the above.  It must be understood that
    with this design the base class is an incomplete skeleton that
    is fleshed out by subclasses.   This clsss has some virtual methods
    (defined with the ABC @abstractmethod decorator) and a set of methods
    that would never work if only the base class were contructed
    (not allowed because of the virtual meethods).  In particular, the
    way the base class handle metadata is potnetially confusing because
    the base class constructor only defines stubs for the data used to
    hold member and ensemble metadata

    A design issue for discussionis is whether not a gather should implement
    the ator and rota functionality of BasicTimeSeries.   The cost is
    tiny up front and retrofitting after that fact might cause a lot of
    headaches.
    """

    def __str__(self):
        return """
    <
        capacity: {},
        size: {},
        npts: {},
        array_type: {},
        num_components: {},
        ensemble_metadata: {},
        member_metadata: {},
        is_parallel: {},
        is_compact: {},
        npartitions: {},
        elog: {},
        member_data: {},
        column_values: {}
    >""".format(
            self.capacity,
            self.size,
            self.npts,
            self.array_type,
            self.num_components,
            self.ensemble_metadata,
            self.member_metadata,
            self.is_parallel,
            self.is_compact,
            self.npartitions,
            self.elog,
            self.member_data,
            self.column_values,
        )

    def __default_constructor__(
        self,
        capacity,
        npts,
        num_components,
        array_type,
        is_compact,
        npartitions,
    ):
        """
        This constructor is used to create an empty object with the given
        capacity and size, the main idea of this constructor is to intialize
        the fields in the object, including the member data and the metadata.
        None of the arguments can be empty.

        :param capacity: Expected number of signals in the ensemble
          to be loaded.  That is, the number of columns in the array
          container.   The idea is that we standardize creation of the
          work array in this base class.   Concrete implementations are
          expected to estimte the size of array needed and then call
          this constuctor before filling the array with data based on
          the input to the subclass constructor.  e.g. for MsPASS
          ensembles that would be set by len(ensemble.member).
          The array implementtion is set by the related parameter
          array_size.  This parameter is required and there is no
          default due to the expected use only by subclasses
        :type capacity:  integer

        :param npts:  length in samples of all members.  This parameter
          is required and there is no default due to the expected use only
          by subclasses
        :type npts:  integer

        :param num_components:  number of components in one member, 1 for
          TimeSeries, and 3 for Seismogram
        :type num_components: int

        :param array_type:  sets the way large arrays are implemented.
          Default should by "numpy" which means the sample array will
          be defined as a nptsXnumber_members FORTRAN order numpy array.
          Other obvious choices are "dask" or "zarr"  List of acceptable
          options to be determined
         :type array_type:  string that must match keywords

        :param is_compact: if true, the member data are stored in row major
        order, which means they are stored in adjacent memory locations.
        otherwise, the data are stored in column major order.
        :type is_compact: boolean

        :param npartitions: The number of desired partitions for Dask or xarray.
        :type npartitions: :class:`int`
        """
        self.capacity = capacity
        self.npts = npts
        self.size = 0
        self.array_type = array_type
        self.num_components = num_components
        self.ensemble_metadata = None
        self.member_metadata = None
        self.member_data = None
        self.is_compact = is_compact
        self.npartitions = npartitions
        self.elog = ErrorLogger()
        self.column_values = None

        supported_array_types = [
            "numpy",
            "xarray",
            "dask",
        ]

        if self.array_type not in supported_array_types:
            raise TypeError(
                "The array type should be one of the follows: {}".format(
                    supported_array_types
                )
            )

        if (
            num_components == 1
        ):  # For the scalar data, we don't need to do rearrangement
            is_compact = False

        shape = (
            [capacity, num_components, npts]
            if is_compact
            else [capacity, npts, num_components]
        )

        if array_type == "xarray":
            self.member_data = xr.DataArray()
            chunksize = [shape[0] // npartitions, shape[1], shape[2]]
            self.member_data.data = da.empty(shape=shape, chunks=chunksize)
        elif array_type == "numpy":
            self.member_data = np.empty(shape)
        elif array_type == "dask":
            chunksize = [shape[0] // npartitions, shape[1], shape[2]]
            self.member_data = da.empty(shape=shape, chunks=chunksize)

    def __constructor_from_input_data(
        self,
        input_data,
        array_type,
        is_compact,
        npartitions,
    ):
        """
        This constructor is used for converting raw data into the new array based
        ensemble, we want to support the following types:
        dask array, numpy array
        the input data should have the same compact type as this new object

        :param input_data: the array that stored the member data of the ensemble
        most common usage is to pass the data extracted by the helper function.
        It supports both dask array and numpy array. We would convert the type if
        the input_data's type is different from the array_type
        :type input_data: da.Array or np.ndarray

        :param array_type:  sets the way large arrays are implemented.
          Default should by "numpy" which means the sample array will
          be defined as a nptsXnumber_members FORTRAN order numpy array.
          Other obvious choices are "dask" or "zarr"  List of acceptable
          options to be determined
         :type array_type:  string that must match keywords

        :param is_compact: if true, the member data are stored in row major
        order, which means they are stored in adjacent memory locations.
        otherwise, the data are stored in column major order.
        :type is_compact: boolean

        :param npartitions: The number of desired partitions for Dask or xarray.
        :type npartitions: :class:`int`
        """
        self.capacity = 0
        self.npts = 0
        self.size = 0
        self.array_type = array_type
        self.num_components = 0
        self.ensemble_metadata = None
        self.member_metadata = None
        self.member_data = None
        self.is_compact = is_compact
        self.npartitions = npartitions
        self.elog = ErrorLogger()

        supported_types = (da.Array, np.ndarray)
        if not isinstance(input_data, supported_types):
            raise TypeError(
                "The array type should be one of the follows: {}".format(
                    supported_types
                )
            )

        supported_array_types = [
            "numpy",
            "xarray",
            "dask",
        ]

        if self.array_type not in supported_array_types:
            raise TypeError(
                "The array type should be one of the follows: {}".format(
                    supported_array_types
                )
            )

        if array_type == "numpy":
            if isinstance(input_data, da.Array):
                self.member_data = input_data.compute()
            elif isinstance(input_data, np.ndarray):
                self.member_data = input_data
        elif array_type == "xarray":
            self.member_data = xr.DataArray()
            chunksize = [
                input_data.shape[0] // self.npartitions,
                input_data.shape[1],
                input_data.shape[2],
            ]
            if isinstance(input_data, da.Array):
                self.member_data.data = input_data.rechunk(chunks=chunksize)
            elif isinstance(input_data, np.ndarray):
                self.member_data.data = da.from_array(input_data, chunks=chunksize)
        elif array_type == "dask":
            chunksize = [
                input_data.shape[0] // self.npartitions,
                input_data.shape[1],
                input_data.shape[2],
            ]
            if isinstance(input_data, da.Array):
                self.member_data = input_data.rechunk(chunks=chunksize)
            elif isinstance(input_data, np.ndarray):
                self.member_data = da.from_array(input_data, chunks=chunksize)

        if self.is_compact:
            self.member_data.transpose((0, 2, 1))

        """
        if is_compact: [size, num_components, npts]
        else:          [size, npts, num_components]
        """
        self.size, self.num_components, self.npts = (
            self.member_data.shape[0],
            self.member_data.shape[1],
            self.member_data.shape[2],
        )
        self.capacity = self.size

    def __constructor_from_ensemble_obj__(
        self,
        input_obj,
        array_type,
        is_compact,
        npartitions,
    ):
        """
        This constructor is used for converting an old ensemble object to the new
        array based ensemble, basically just a wrapper for the construction from
        raw data.
        :param input_obj:   Seismogram or TimeSeries to extract data from
        :param array_type:  sets the way large arrays are implemented.
          Default should by "numpy" which means the sample array will
          be defined as a nptsXnumber_members FORTRAN order numpy array.
          Other obvious choices are "dask" or "zarr"  List of acceptable
          options to be determined
         :type array_type:  string that must match keywords

        :param is_compact: if true, the member data are stored in row major
        order, which means they are stored in adjacent memory locations.
        otherwise, the data are stored in column major order.
        :type is_compact: boolean

        :param npartitions: The number of desired partitions for Dask or xarray.
        :type npartitions: :class:`int`
        """
        supported_ensemble_types = (TimeSeriesEnsemble, SeismogramEnsemble)
        if not isinstance(input_obj, supported_ensemble_types):
            raise TypeError(
                "The input object should be one of the follows: {}".format(
                    supported_ensemble_types
                )
            )

        if isinstance(input_obj, TimeSeriesEnsemble):
            self.__constructor_from_input_data(
                extractDataFromOldEnsemble(input_obj),
                array_type,
                is_compact,
                npartitions,
            )

        elif isinstance(input_obj, SeismogramEnsemble):
            self.__constructor_from_input_data(
                extractDataFromOldEnsemble(input_obj),
                array_type,
                is_compact,
                npartitions,
            )

        # Add the ensemble's metadata
        self.ensemble_metadata = dict(input_obj)

        # Extract Member Metadata from old ensemble
        # Add the members' metadata
        self.member_metadata = extractMemberMetadataFromOldEnsemble(input_obj)

    def __init__(
        self,
        capacity,
        size,
        npts,
        num_components,
        npartitions,
        input_data=None,
        input_obj=None,
        member_metadata=None,
        ensemble_metadata=None,
        dt=None,
        array_type="dask",
        is_compact=True,
        is_parallel=True,
    ):
        """
        This base class constructor should take the view that the base
        class is the fraework with no data.  That is in line with
        BasicTimeSeries and an appropriate launching for subclasses that
        define scalar and 3C data as conrete implementations.
        A difference here, however, is that constuctor will create the
        work array but just not populate it.

        :param capacity: Expected number of signals in the ensemble
          to be loaded.  That is, the number of columns in the array
          container.   The idea is that we standardize creation of the
          work array in this base class.   Concrete implementations are
          expected to estimte the size of array needed and then call
          this constuctor before filling the array with data based on
          the input to the subclass constructor.  e.g. for MsPASS
          ensembles that would be set by len(ensemble.member).
          The array implementtion is set by the related parameter
          array_size.  This parameter is required and there is no
          default due to the expected use only by subclasses
        :type capacity:  integer

        :param npts:  length in samples of all members.  This parameter
          is required and there is no default due to the expected use only
          by subclasses
        :type npts:  integer

        :param dt:  sample interval of all data in ensemble.  Default is
          None which taken to mean it should be derived from the data.
        :type dt:  float

        :param array_type:  sets the way large arrays are implemented.
          Default should by "numpy" which means the sample array will
          be defined as a nptsXnumber_members FORTRAN order numpy array.
          Other obvious choices are "dask" or "zarr"  List of acceptable
          options to be determined
         :type array_type:  string that must match keywords

        """

        if input_obj is not None:
            self.__constructor_from_ensemble_obj__(
                input_obj=input_obj,
                array_type=array_type,
                is_compact=is_compact,
                npartitions=npartitions,
            )

        elif input_data is not None:
            self.__constructor_from_input_data(
                input_data=input_data,
                array_type=array_type,
                is_compact=is_compact,
                npartitions=npartitions,
            )
        else:
            if member_metadata is None:
                raise TypeError("member metadata should be given")

            self.__default_constructor__(
                capacity=capacity,
                npts=npts,
                num_components=num_components,
                array_type=array_type,
                is_compact=is_compact,
                npartitions=npartitions,
            )

        self.capacity = capacity
        self.npts = npts
        self.array_type = array_type
        self.num_components = num_components
        self.ensemble_metadata = ensemble_metadata
        if self.member_metadata is None:
            self.member_metadata = member_metadata
        self.is_parallel = is_parallel
        self.is_compact = is_compact
        self.npartitions = npartitions
        self.elog = ErrorLogger()

        if dt is None:
            dt = self.member_metadata["delta"][0]
        if self.ensemble_metadata is None:  # default setting for the metadata
            self.ensemble_metadata = {
                "is_live": False,
                "is_utc": False,
                "dt": dt,
            }

    def append(self, mspass_object):
        """
        Appends data in mspass_object to internal array object.
        For now, we don't handle resizing similar to std containers in C++
        where allocs (in this case a resize) is triggered by filling.
        Instead, we would just rely on the xarray/numpy's append.
        """
        if not isinstance(mspass_object, (TimeSeries, Seismogram)):
            raise TypeError("only TimeSeries and Seismogram are supported")
        if (isinstance(mspass_object, TimeSeries) and self.num_components != 1) or (
            isinstance(mspass_object, Seismogram) and self.num_components != 3
        ):
            raise TypeError("The components number doesn't match the input object")

        new_data = extractDataFromMsPassObject(mspass_object)  # get the numpy data
        if self.is_compact:
            new_data.transpose()

        if self.size < self.capacity:
            self.member_data[self.size] = new_data
        else:
            if self.array_type == "dask":
                if self.is_compact:
                    da.append(self.member_data, new_data, 2)
                else:
                    da.append(self.member_data, new_data, 1)
            elif self.array_type == "numpy":
                if self.is_compact:
                    np.append(self.member_data, new_data, 2)
                else:
                    np.append(self.member_data, new_data, 1)

        self.member_metadata.loc[
            len(self.member_metadata.index)
        ] = mspass_object.todict()
        self.size += 1

    def starttime(self) -> list:
        """
        Return a list (python array) of starttimes.  We should assume
        times are always stored as doubles.
        """
        return self.member_metadata["starttime"].tolist()

    def column_values(self) -> list:
        """
        Returns the values assigned to the column axis.  These should
        default to integers matching the column index values.   They
        should be allowed to be anything, however, to handle things
        like variable offset record sections.
        """
        if self.column_values is None:
            return list(range(self.size))
        else:
            return self.column_values

    def set_column_values(self, x):
        """
        Set the column values array to the values in x.
        :param x: a list of column names
        """
        # Sanity check: the input be a list
        if type(x) != list:
            raise TypeError("The column values should be a list")
        self.column_values = x

    def dt(self):
        """
        Return the sample rate of the ensemble
        """
        return self.ensemble_metadata["dt"]

    # The following names match BasicTimeSeries because the match in
    # concept.  starttime above doesn't really because in a single object
    # t0==starttime is constant.
    def time(self, i, j):
        """
        Return time of array position i,j (Note works the same for 3c and
        scalar data)

        :param i:   row index
        :type i: int
        :param j:   column index
        :type j: int
        """
        # from the definiation in timeseries:
        # time(i) = (mt0+mdt*i);
        return self.starttime()[i] + self.dt() * j

    def sample_number(self, time, x) -> np.ndarray:
        """
        Return a numpy array of ints of the index position of an ordered pair defined by a time
        (row) value and a column value that can be mapped to the
        column index.
        :param x: a list of column names
        :param time: the time (row) value
        """
        indexes = [self.column_values.index(i) for i in x]
        ans = [(time - self.starttime()[ind]) / self.dt() for ind in indexes]
        return np.asarray(ans)

    def time_is_UTC(self):
        """
        Returns True if the time standard for the data is set as UTC
        """
        return self.ensemble_metadata["is_utc"]

    def time_is_relative(self):
        """
        Returns True if the time standard for the data is set as relative
        """
        return not self.time_is_UTC()

    def live(self, j) -> bool:
        """
        Return true if jth member is set live.
        :param j: index number
        :type j: int
        """
        return self.member_metadata["is_live"][j]

    def dead(self, j) -> bool:
        """
        Return true if jth member is marked dead (opposite of live).
        :param j: index number
        :type j: int
        """
        return not self.live(j)

    # This next set are s in BasicTimeSeries but all
    def samprate(self):
        """
        Return the sample rate of the ensemble
        """
        return 1.0 / self.dt()

    def rtoa(self):
        """
        Convert the relative time to absolute time
        """
        if not self.time_is_relative():
            return

        if self.t0shift <= 100.0:
            raise MsPASSError(
                "time shift to return to UTC time is not defined", "Invalid"
            )

        # TODO: do we need to check if the items are live or dead here?
        self.member_metadata["starttime"].applymap(lambda x: (x + self.t0shift))

        self.t0shift = 0
        self.ensemble_metadata["is_utc"] = True

    def ator(self, shift):
        """
        Convert the absolut time to relative time
        """
        if not self.time_is_UTC():
            return
        self.t0shift = shift
        self.member_metadata["time"].applymap(lambda x: (x - self.t0shift))

    def shift(self, timeshift):
        """
        Time shift for all the member in the ensemble
        :param timeshift: the shift time range
        :type timeshift: float
        """
        old_t0shift = self.t0shift
        self.rtoa()
        self.ator(old_t0shift + timeshift)

    # The following methods in BasicTimeSeries are optional.  It isn't
    # clear to me if their use is required - at least initially.  With
    # python a ddingg methods is not as probematic as a Java or C++
    # They are;  time_reference, timetype, set_to, set_tref, and
    # set_npts.

    # An alternative design would be to put these in an intermediate
    # class but don't think that would be useful for a python implementation
    def get_metadata(self, j, return_type="dict"):
        """
        Return the Metadata components of member j.  return_type should
        allow optional return as dict or Metadata.  Not clear which should
        be default.
        :param j: index of member
        :type j: int
        :param return_type: return type, dict or metadata
        :type return_type: str
        """
        supported_return_types = ["dict", "metadata"]
        if return_type not in supported_return_types:
            raise TypeError(
                "The return type should be one of the follows: {}".format(
                    supported_return_types
                )
            )
        ret_metadata = self.member_metadata.loc[j]
        ret_dict = ret_metadata.to_dict()
        if return_type == "dict":
            return ret_dict
        elif return_type == "metadata":
            return Metadata(ret_dict)

    def set_metadata(self, j, md):
        """
        Setter for metadata of member j.   md is the new continer that would
        replace current conent.  Most useful for constructors.
        :param j: index of member
        :type j: int
        :param md: the metadata to set, dict or metadata
        :type md: dict or metadata
        """
        if not isinstance(md, (dict, Metadata)):
            raise TypeError("The metadata should be a dict-like object")
        if isinstance(md, Metadata):
            md = md.to_dict()
        md_df = pd.DataFrame.from_dict(md)
        self.member_metadata.loc[j] = md_df

    def edit_metadata(self, j, md):
        """
        Differs from set_metadata in that the contents of md are added to
        the current and do not fully repalce them.   Needed for updating
        metadata after costruction
        :param j: index of member
        :type j: int
        :param md: the metadata to edit, dict or metadata
        :type md: dict or metadata
        """
        if not isinstance(md, (dict, Metadata)):
            raise TypeError("The metadata should be a dict-like object")
        if isinstance(md, Metadata):
            md = md.to_dict()
        for key, val in md:
            # We assume that user shouldn't add new metadata
            if key not in self.member_metadata.columns:
                raise TypeError(
                    "key {} is not presented in the member metadata".format(key)
                )
            self.member_metadata[key][j] = val

    @abstractmethod
    def member(self, j):
        """
        Returns the data associated with member j.   Virtual method
        returns different type for scalar ersus 3c data.
        """
        pass

    @abstractmethod
    def subset(self):
        """
        Return a subset of the data defined by either a collection of
        members or a restricted sample range (a form of windowing).
        Concrete implementtions can define what this means.   Minimum
        usage would be something functionally equivalent to the f90/matlab
        synatx of ensemble.member[i:j].   We may also want to be less
        generic and require addition methods with other names like
        "extract_members" or "window" (time range).
        """
        pass

    def ensemble_metadata(self, return_type="dict"):
        """
        Like metadata method but returns the ensemble metadata.
        """
        supported_return_types = ["dict", "metadata"]
        if return_type not in supported_return_types:
            raise TypeError(
                "The return type should be one of the follows: {}".format(
                    supported_return_types
                )
            )
        if return_type == "dict":
            return self.ensemble_metadata
        elif return_type == "metadata":
            return Metadata(self.ensemble_metadata)

    def sync_metadata(self):
        """
        This would act like the one in Ensemble in C++ copying all the
        ensemble key-value pairs to the members.  It is debatable that
        this would be needed but would be trivial to implement.
        """
        for key, val in self.ensemble_metadata:
            self.member_metadata = self.member_metadata.assign(key=val)

    # not sure if we can make this member abstract.  It needs to
    # be in this design to allow different signatures for scalar and 3c data
    @abstractmethod
    def __getitem__(self):
        """
        Python version of operator[].   Not sure the declaration as
        abstractmethod will work or is even deirable.  I put it hat
        way for now as we do want to require thiat method.  For scalar
        data it should get two indices while for 3C data it needs 3.
        """
        pass

    @abstractmethod
    def __setitem__(self):
        """
        Setter by index.   Same issue that is probably isn't necesessy
        in he implementtion to use the abstractmethod eecorator.
        """
        pass


class Gather(BasicGather):
    """
    Concrete implementtion for a scalar gather.  This is the array
    equivalent of a TimeSeriesEnsemble appropriate when the input matches
    the concept of BasicGather.    It follows the OOP stndard pardigm that creation is initialization.  This puts a lot of features in the constructor for the class."""

    def __init__(
        self,
        capacity=0,
        size=0,
        npts=0,
        num_components=0,
        npartitions=0,
        input_data=None,
        input_obj=None,
        member_metadata=None,
        ensemble_metadata=None,
        dt=None,
        array_type="dask",
        is_compact=True,
    ):
        """
        Because in this design the base class only sets up the workspace
        most of the nitty gritty work of building this thig will go here.

        :param mspss_object:  Should contain the data to be loaded.
        :type mspass_object:  should accept TimeSeriesEnsemble or another
          Gather object.  Would advise a different class if we wanted to
          allow soemthig like a raw matrix as input.  We perhaps should
          accept a None and in that situation just create the workspace
          of a speciied size and assume the gather will be build by
          many calls to append.

        :param capacity :  expected number of signals the gather
          will eventually contain.  If None (default) the size is
          determined by the input ensemble.  If nonzero it should be
          a positive integer larger than the size of the ensemble passed
          as mspass_object.
        :type:  integer

        :param npts:  expected number of samples for all data in the gather
          If None, which is the default, it should be estimated from the
          input data.  Note mspass_object beng set None and this parameter
          or capacity set None should cause an exception to be
          raised.  Either that or it should be ignored and a warning
          message posted - probably a better idea.

        dt, array_type are as defined in the
        base clsss.  They are passed directly to the base class
        construtor
        """
        # Will directly call the constructor of the base class, but we need
        # to do some sanity check first.
        if input_obj is not None:
            if not isinstance(input_obj, TimeSeriesEnsemble):
                raise TypeError("The input object should be a TimeSeriesEnsemble")

        elif input_data is not None:
            input_num_comp = input_data.shape[1]
            if input_num_comp != 1:
                raise TypeError(
                    "The input data shape is not valid, should be scalar data"
                )

        super().__init__(
            capacity=capacity,
            size=size,
            npts=npts,
            num_components=num_components,
            npartitions=npartitions,
            input_data=input_data,
            input_obj=input_obj,
            member_metadata=member_metadata,
            ensemble_metadata=ensemble_metadata,
            dt=dt,
            array_type=array_type,
            is_compact=is_compact,
        )

    def member(self, j) -> TimeSeries:
        """
        Returns the data associated with member j.  Unlike above I suggest
        we not do the idea of "data_only".  Aways return a TimeSeries.
        Suggest data method for that purpose
        :param j: index
        :param j: int
        """
        if j >= self.size:
            raise MsPASSError(
                "The given index: {} is out of range.".format(j), "Invalid"
            )
        md = self.get_metadata(j, "metadata")
        obj = TimeSeries(md)

        for i, d in enumerate(self.data(j)):
            obj.data[i] = d
        obj.live = True
        if self.dead(j):
            obj.kill()
        return obj

    def data(self, j) -> np.ndarray:
        """
        Return the raw data vector associated with column j.   Defined here
        as an ndarray return but probably should be any iterable
        container that acts like a vector.
        :param j: index
        :param j: int
        """
        if j >= self.size:
            raise MsPASSError(
                "The given index: {} is out of range.".format(j), "Invalid"
            )
        col = self.member_data[j]
        if self.is_parallel:
            return col.compute().flatten()
        else:
            return col.flatten()

    def subset(self, start, end) -> "Gather":
        """
        Return a subset of the Gather with signals from start to end
        (like start:end in F90 or matlab).

        :param start:   the start index of the subset
        :param end: specifies the index where the subset ends (but
        excluding the value at this index, just like array's slicing)
        """
        new_input_data = self.member_data[start:end]
        new_member_metadata = self.member_metadata[start:end]
        new_npartitions = end - start
        if new_npartitions > self.npartitions:
            npartitions = self.npartitions

        new_gather = Gather(
            input_data=new_input_data,
            array_type=self.array_type,
            is_compact=self.is_compact,
            npartitions=new_npartitions,
            member_metadata=new_member_metadata,
        )
        return new_gather

    def __getitem__(self, pos):
        """
        Index operator to return data value at sample index pos = (i,j).
        :param pos: a tuple of index to data value
        :type pos: tuple
        """
        i, j = pos
        if self.is_compact:
            val = self.member_data[i][0][j]
        else:
            val = self.member_data[i][j][0]
        if self.is_parallel:
            return val.compute()
        else:
            return val

    def __setitem__(self, pos, newvalue):
        """
        Index setter - I think this is the right syntax for two indices.
        :param pos: a tuple of index to data value
        :type pos: tuple
        :param newvalue: the new value to set
        :type newvalue: any type
        """
        i, j = pos
        if self.is_compact:
            self.member_data[i, 0, j] = newvalue
        else:
            self.member_data[i, j, 0] = newvalue


class SeismogramGather(BasicGather):
    """
    Gather for three-component data.  Can only be created from inputs that
    satisfy the restrictions of BasicGather.  It is much like Gather
    but with a 3D instead of a 2D array holding sample data.
    """

    def __init__(
        self,
        capacity=0,
        size=0,
        npts=0,
        num_components=0,
        npartitions=0,
        input_data=None,
        input_obj=None,
        member_metadata=None,
        ensemble_metadata=None,
        dt=None,
        array_type="dask",
        is_compact=True,
    ):
        """
        Because in this design the base class only sets up the workspace
        most of the nitty gritty work of building this thig will go here.

        :param mspss_object:  Should contain the data to be loaded.
        :type mspass_object:  should accept TimeSeriesEnsemble or another
          Gather object.  Would advise a different class if we wanted to
          allow soemthig like a raw matrix as input.  We perhaps should
          accept a None and in that situation just create the workspace
          of a speciied size and assume the gather will be build by
          many calls to append.

        :param number_members:  expected number of signals the gather
          will eventually contain.  If None (default) the size is
          determined by the input ensemble.  If nonzero it should be
          a positive integer larger than the size of the ensemble passed
          as mspass_object.
        :type:  integer

        :param npts:  expected number of samples for all data in the gather
          If None, which is the default, it should be estimated from the
          input data.  Note mspass_object beng set None and this parameter
          or number_members set None should cause an exception to be
          raised.  Either that or it should be ignored and a warning
          message posted - probably a better idea.

        dt, array_type, and resize_blocksize are as defined in the
        base clsss.  They are passed directly to the base class
        construtor
        """
        # Will directly call the constructor of the base class, but we need
        # to do some sanity check first.
        if input_obj is not None:
            if not isinstance(input_obj, SeismogramEnsemble):
                raise TypeError("The input object should be a TimeSeriesEnsemble")

        elif input_data is not None:
            input_num_comp = input_data.shape[1]
            if input_num_comp != 3:
                raise TypeError(
                    "The input data shape is not valid, should be scalar data"
                )

        super().__init__(
            capacity=capacity,
            size=size,
            npts=npts,
            num_components=num_components,
            npartitions=npartitions,
            input_data=input_data,
            input_obj=input_obj,
            member_metadata=member_metadata,
            ensemble_metadata=ensemble_metadata,
            dt=dt,
            array_type=array_type,
            is_compact=is_compact,
        )

    def member(self, j) -> Seismogram:
        """
        Returns the data associated with member j.  Unlike above I suggest
        we not do the idea of "data_only".  Aways return a TimeSeries.
        Suggest data method for that purpose
        :param j: index
        :param j: int
        """
        if j >= self.size:
            raise MsPASSError(
                "The given index: {} is out of range.".format(j), "Invalid"
            )
        md = self.get_metadata(j, "metadata")
        obj = Seismogram(md, False)

        obj.data = dmatrix(self.data(j))
        obj.set_live()
        if self.dead(j):
            obj.kill()
        return obj

    def data(self, j) -> np.ndarray:
        """
        Return the raw data matrix associated with column j.   Defined here
        as an ndarray return but probably should be any iterable
        container that acts like a matrix.  Not clear what will be needed to
        support large matrix formats.
        :param j: index
        :param j: int
        """
        if j >= self.size:
            raise MsPASSError(
                "The given index: {} is out of range.".format(j), "Invalid"
            )
        col = self.member_data[j]
        if self.is_compact:
            col.transpose()
        if self.is_parallel:
            return col.compute()
        else:
            return col

    def subset(self, start, end) -> "SeismogramGather":
        """
        Return a subset of the Gather with signals from start to end
        (like start:end in F90 or matlab).

        :param start:   the start index of the subset
        :param end: specifies the index where the subset ends (but
        excluding the value at this index, just like array's slicing)
        """
        new_input_data = self.member_data[start:end]
        new_member_metadata = self.member_metadata[start:end]
        new_npartitions = end - start
        if new_npartitions > self.npartitions:
            npartitions = self.npartitions

        new_gather = SeismogramGather(
            input_data=new_input_data,
            array_type=self.array_type,
            is_compact=self.is_compact,
            npartitions=new_npartitions,
            member_metadata=new_member_metadata,
        )
        return new_gather

    def __getitem__(self, pos):
        """
        Index operator to fetch sample from time axis at i, member axis at j,
        and component number k.
        :param pos: a tuple of index to data value
        :type pos: tuple
        """
        i, j, k = pos
        if self.is_compact:
            val = self.member_data[i][k][j]
        else:
            val = self.member_data[i][j][k]
        if self.is_parallel:
            return val.compute()
        else:
            return val

    def __setitem__(self, pos, newvalue):
        """
        Indexing setter.   Not sure this is the right syntax but should show
        the idea.
        :param pos: a tuple of index to data value
        :type pos: tuple
        :param newvalue: the value to set
        """
        i, j, k = pos
        if self.is_compact:
            self.member_data[i, k, j] = newvalue
        else:
            self.member_data[i, j, k] = newvalue


def read_basic_array_ensemble(db, ensemble_object, object_id):
    """
    read the array from database and dump data to the ensemble object
    object_id: the MongoDB object id of the ensemble to be read from the disk. The object is unique
            and provides a one-to-one mapping to the ensemble's metadata and member data.
    the schema for ensemble doc:
        _id : unique id
        metadata : a dict that stores the ensemble's metadata
        member_metadata : a list of metadata for each member
        store_type = 'zarr' : we might support other storage?
        zarr_group_path : str, the path of the zarr group
        zarr_arr_name : str, the name of the zarr array
    """
    ensemble_col = db.ensemble
    doc = ensemble_col.find_one({"_id": object_id})
    df = pd.DataFrame(doc["member_metadata"])
    ddf = dask.dataframe.from_pandas(df)
    ensemble_object.member_metadata = ddf
    if ensemble_object.impl == "xarray":
        ensemble_object.member.attrs = doc[
            "metadata"
        ]  # we can store the metadata in .attrs
        if doc["store_type"] == "zarr":
            store = zarr.DirectoryStore(doc["zarr_group_path"])
            zarr_arr = zarr.group(store=store)[doc["zarr_arr_name"]]
            dask_arr = da.from_array(zarr_arr, chunks=zarr_arr.chunks)
            ensemble_object.member.data = dask_arr
    return ensemble_object


def write_basic_array_ensemble(db, ensemble_object, object_id):
    """
    write the data of ensemble_object to the storage, and save the metadata
    object_id: the MongoDB object id of the ensemble to be read from the disk. The object is unique
            and provides a one-to-one mapping to the ensemble's metadata and member data.
    the schema for ensemble doc:
        _id : unique id
        metadata : a dict that stores the ensemble's metadata
        member_metadata : a list of metadata for each member
        store_type = 'zarr' : we might support other storage?
        zarr_group_path : str, the path of the zarr group
        zarr_arr_name : str, the name of the zarr array
    """
    ensemble_col = db.ensemble
    doc = ensemble_col.find_one({"_id": object_id})
    df = ensemble_object.member_metadata.compute()
    key = {"_id": object_id}
    data = {"member_metadata": df.to_dict()}
    if ensemble_object.impl == "xarray":
        data["metadata"] = ensemble_object.member.attrs
        if doc["store_type"] == "zarr":
            store = zarr.DirectoryStore(doc["zarr_group_path"])
            group = zarr.group(store=store)
            zarr_arr = group.create_dataset(
                [doc["zarr_arr_name"]],
                shape=ensemble_object.member.shape,
                chunks=ensemble_object.member.data.chunks,
                dtype=ensemble_object.member.dtype,
                overwritebool=True,
            )
            dask.array.to_zarr(ensemble_object.member.data, zarr_arr, overwrite=True)
    ensemble_col.update_one(filter=key, update=data, upsert=True)
