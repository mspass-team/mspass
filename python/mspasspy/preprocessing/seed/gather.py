from abc import ABC, abstractmethod
import xarray as xr
import dask.array as da
import dask
import zarr
import numpy as np
from mspasspy.ccore.seismic import (
    TimeSeriesEnsemble,
    SeismogramEnsemble,
    TimeSeries,
    Seismogram,
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

# from mspasspy.algorithms.basic import rtoa


def isOldEnsembleObject(obj):
    old_ensemble_types = (TimeSeriesEnsemble, SeismogramEnsemble)
    return isinstance(obj, supported_ensemble_types)


def isMsPassObject(obj):
    return isinstance(obj, (TimeSeries, Seismogram))


def extractDataFromMsPassObject(mspass_object):
    """
    mspass_object is either a seismogram or a timeseries, mspass_object.data
    is a DoubleVector, we want to convert it extract the data from it and
    convert it into a numpy array
    The return type is np.ndarray
    """
    if not isMsPassObject(mspass_object):
        raise TypeError("Can't extract data from the object, not a mspass object")
    return np.array(mspass_object.data)


def extractDataFromOldEnsemble(ensemble_object):
    if not isOldEnsembleObject(ensemble_object):
        raise TypeError(
            "Can't extract data from the object, not an old ensemble object"
        )
    if len(ensemble_object.member) <= 0:
        raise TypeError("The input object doesn't have any member data")
    num_components = 3 if isinstance(ensemble_object, SeismogramEnsemble) else 1

    shape = [len(ensemble_object.member), ensemble_object[0].npts]
    ret = np.empty(shape)
    for i in range(len(ensemble_object.member)):
        ret[i] = extractDataFromMsPassObject(ensemble_object[i])
    return ret


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

    """
    Note:
    Fields in the class:
    1. size/capacity: number of members in the gather (x)
    2. npts: number of samples for all data in the gather (y)
    3. array_type: which array we are using? Dask/numpy
    4. number_components: 1 for timeseries, 3 for seismogram
    Actually, for now, we are not using capacity at all, instead we are depending on the dask/numpy
    array's append.
    5. Column_values: a list of values assigned to the column axis, default is a list

    What should the ensemble metadata have:
    1. is_utc  //  We assume that the data is either UTC or relative
    2. dt

    What should the member metadata have:
    1. starttime
    2. is_live
    3. t0_shift

    """

    def __default_constructor__(
        self,
        capacity,
        npts,
        number_components,
        array_type,
        is_compact,
        num_partition,
    ):
        """
        This constructor is used to create an empty object with the given
        capacity and size, the main idea of this constructor is to intialize
        the fields in the object, including the member data and the metadata
        All the argument can't be empty
        """
        self.capacity = capacity
        self.npts = npts
        self.size = 0
        self.dt = None
        self.array_type = array_type
        self.number_components = number_components
        self.ensemble_metadata = None
        self.member_metadata = None
        self.member_data = None
        self.is_compact = is_compact
        self.num_partition = num_partition
        self.elog = ErrorLogger()
        self.column_values = None

        supported_array_types = [
            "numpy",
            "xarray",
        ]

        if self.array_type not in supported_array_types:
            raise TypeError(
                "The array type should be one of the follows: {}".format(
                    supported_array_types
                )
            )

        if (
            number_components == 1
        ):  # For the scalar data, we don't need to do rearrangement
            is_compact = False

        shape = (
            [capacity, number_components, npts]
            if is_compact
            else [capacity, npts, number_components]
        )

        if array_type == "xarray":
            self.member_data = xr.DataArray()
            chunksize = [shape[0] // num_partition, shape[1], shape[2]]
            self.member_data.data = da.empty(shape=shape, chunks=chunksize)
        elif array_type == "numpy":
            self.member_data = np.empty(shape)

    def __constructor_from_input_data(
        self,
        input_data,
        array_type,
        is_compact,
        num_partition,
    ):
        """
        This constructor is used for converting raw data into the new array based
        ensemble, we want to support the following types:
        dask array, numpy array
        the input data should have the same compact type as this new object
        """
        self.capacity = 0
        self.npts = 0
        self.size = 0
        self.dt = None
        self.array_type = array_type
        self.number_components = 0
        self.ensemble_metadata = None
        self.member_metadata = None
        self.member_data = None
        self.is_compact = is_compact
        self.num_partition = num_partition
        self.elog = ErrorLogger()

        supported_types = (da.Array, np.array)
        if not isinstance(input_data, supported_types):
            raise TypeError(
                "The array type should be one of the follows: {}".format(
                    supported_types
                )
            )

        supported_array_types = [
            "numpy",
            "xarray",
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
            if isinstance(input_data, da.Array):
                chunksize = [
                    input_data.shape[0] // self.num_partition,
                    input_data.shape[1],
                    input_data.shape[2],
                ]
                self.member_data.data = input_data.rechunk(chunks=chunksize)
            elif isinstance(input_data, np.array):
                self.member_data.data = da.from_array(input_data, chunks=chunksize)
        if self.is_compact:
            self.size, self.number_components, self.npts = (
                self.member_data.shape[0],
                self.member_data.shape[1],
                self.member_data.shape[2],
            )
        else:
            self.size, self.number_components, self.npts = (
                self.member_data.shape[0],
                self.member_data.shape[2],
                self.member_data.shape[1],
            )

        self.capacity = self.size

    def __constructor_from_ensemble_obj__(
        self,
        input_obj,
        array_type,
        is_compact,
        num_partition,
    ):
        """
        This constructor is used for converting an old ensemble object to the new
        array based ensemble
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
                num_partition,
            )
            # Copy the data from TimeSeriesEnsemble to the new object

        elif isinstance(input_obj, SeismogramEnsemble):
            self.__constructor_from_input_data(
                extractDataFromOldEnsemble(input_obj),
                array_type,
                is_compact,
                num_partition,
            )

    def __init__(
        self,
        capacity,
        size,
        npts,
        number_components,
        num_partition,
        input_data=None,
        input_obj=None,
        member_metadata=None,
        ensemble_metadata=None,
        dt=None,
        array_type="xarray",
        is_compact=True,
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
         :type array_type:  string that must match keywords TBD

        """
        self.capacity = capacity
        self.npts = npts
        self.dt = dt
        self.array_type = array_type
        self.number_components = number_components
        self.ensemble_metadata = ensemble_metadata
        self.member_metadata = member_metadata
        self.is_parallel = is_parallel
        self.is_compact = is_compact
        self.num_partition = num_partition
        self.elog = ErrorLogger()

        if self.ensemble_metadata is None:  # default setting for the metadata
            self.ensemble_metadata = {
                "is_live": False,
                "is_utc": False,
                "dt": 0,
            }

        if input_obj is not None:
            self.__constructor_from_ensemble_obj__(
                input_obj=input_obj,
                array_type=array_type,
                is_compact=is_compact,
                num_partition=num_partition,
            )
            return

        if input_data is not None:
            self.__constructor_from_input_data(
                input_data=input_data,
                array_type=array_type,
                is_compact=is_compact,
                num_partition=num_partition,
            )
            return

        self.__default_constructor__(
            capacity=capacity,
            npts=npts,
            number_components=number_components,
            array_type=array_type,
            is_compact=is_compact,
            num_partition=num_partition,
        )

    def append(self, mspass_object):
        """
        Appends data in mspass_object to internal array object.
        For now, we don't handle resizing similar to std containers in C++
        where allocs (in this case a resize) is triggered by filling.
        Instead, we would just rely on the xarray/numpy's append.
        """
        if not isinstance(mspass_object, (TimeSeries, Seismogram)):
            raise TypeError("only TimeSeries and Seismogram are supported")
        if (isinstance(mspass_object, TimeSeries) and self.number_components != 1) or (
            isinstance(mspass_object, Seismogram) and self.number_components != 3
        ):
            raise TypeError("The components number doesn't match the input object")

        new_data = extractDataFromMsPassObject(mspass_object)  # get the numpy data
        if self.array_type == "dask":
            if is_compact:
                # Not very sure if it can work
                da.append(self.member_data, new_data, 2)
            else:
                da.append(self.member_data, new_data, 1)
        elif self.array_type == "numpy":
            if is_compact:
                np.append(self.member_data, new_data, 2)
            else:
                np.append(self.member_data, new_data, 1)

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
            return list(range(size))
        else:
            return self.column_values

    def set_column_values(self, x):
        """
        Set the column values array to the values in x.
        """
        # Sanity check: the input be a list
        if type(x) != list:
            raise TypeError("The column values should be a list")
        self.column_values = x

    def dt(self):
        return self.ensemble_metadata["dt"]

    # The following names match BasicTimeSeries because the match in
    # concept.  starttime above doesn't really because in a single object
    # t0==starttime is constant.
    def time(self, i, j):
        """
        Return time of array position i,j (Note works the same for 3c and
        scalar data)
        """
        # from the definiation in timeseries:
        # time(i) = (mt0+mdt*i);
        return self.starttime()[i] + self.dt() * j

    def sample_number(self, time, x) -> tuple:
        """
        Return a tuple of ints of the index position of an ordered pair defined by a time
        (row) value and a column value that can be mapped to the
        column index.
        """
        indexes = [self.column_values.index(i) for i in x]
        ans = [(time - self.starttime()[ind]) / self.dt() for ind in indexes]
        return tuple(ans)

    def time_is_UTC(self):
        """
        Returns True if the time standard for the data is set as
        UTC
        """
        return self.ensemble_metadata["is_utc"]

    def time_is_relative(self):
        """
        Returns True if the time standard for the data is set as
        relative
        """
        return not self.time_is_UTC()

    def live(self, j) -> bool:
        """
        Return true if jth member is set live.
        """
        return self.member_metadata["is_live"][j]

    def dead(self, j) -> bool:
        """
        Return true if jth member is marked dead (opposite of live).
        """
        return not self.dead(j)

    # This next set are s in BasicTimeSeries but all
    def samprate(self):
        return 1.0 / self.dt()

    def rtoa(self):
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
        if not self.time_is_UTC():
            return
        self.t0shift = shift
        self.member_metadata["time"].applymap(lambda x: (x - self.t0shift))

    def shift(self, timeshift):
        old_t0shift = t0shift
        self.rtoa()
        self.ator(old_t0shift + timeshift)

    # The following methods in BasicTimeSeries are optional.  It isn't
    # clear to me if their use is required - at least initially.  With
    # python a ddingg methods is not as probematic as a Java or C++
    # They are;  time_reference, timetype, set_to, set_tref, and
    # set_npts.

    # An alternative design would be to put these in an intermediate
    # class but don't think that would be useful for a python implementation
    def metadata(self, j, return_type="dict"):
        """
        Return the Metadata components of member j.  return_type should
        allow optional return as dict or Metadata.  Not clear which should
        be default.
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
        """
        if not isinstance(md, (dict, Metadata)):
            raise TypeError("The metadata should be a dict-like object")
        if isinstance(md, Metadata):
            md = md.to_dict()
        md_df = pd.Dataframe.from_dict(md)
        self.member_metadata.loc[j] = md_df

    def edit_metadata(self, j, md):
        """
        Differs from set_metadata in that the contents of md are added to
        the current and do not fully repalce them.   Needed for updating
        metadata after costruction
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
    def member(self, j, data_only=False):
        """
        Returns the data associated with member j.   Virtual method
        returns different type for scalar ersus 3c data.
        data_only in this context is only a hint to a concrete
        implementation to have an option to return just the data vector/matrix
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
    the concept of BasicGather.    It follows the OOP stndard
    pardigm that creation is initialization.  This puts a lot of features
    in the constructor for the class.
    """

    def __init__(
        self,
        capacity,
        size,
        npts,
        number_components,
        num_partition,
        input_data=None,
        input_obj=None,
        member_metadata=None,
        ensemble_metadata=None,
        dt=None,
        array_type="xarray",
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

    def member(self, j) -> TimeSeries:
        """
        Returns the data associated with member j.  Unlike above I suggest
        we not do the idea of "data_only".  Aways return a TimeSeries.
        Suggest data method for that purpose
        """
        if j >= self.size:
            raise MsPASSError(
                "The given index: {} is out of range.".format(j), "Invalid"
            )
        md = self.metadata(j, "metadata")
        obj = TimeSeries(md)

        obj.data = DoubleVector(self.data(j))
        obj.npts = len(mspass_object.data)
        if self.dead(j):
            obj.kill()
        return obj

    def data(self, j) -> np.ndarray:
        """
        Return the raw data vector associated with column j.   Defined here
        as an ndarray return but probably should be any iterable
        container that acts like a vector.  Not clear what will be needed to
        support large matrix formats.
        """
        if j >= self.size:
            raise MsPASSError(
                "The given index: {} is out of range.".format(j), "Invalid"
            )
        col = self.member_data[j].reshape()
        if self.is_parallel:
            return col.compute()
        else:
            return col

    def subset(self, start, end) -> "Gather":
        """
        Return a subset of the Gather with signals from start to end
        (like start:end in F90 or matlab).

        TBD is if subset should be overloaded to allow other selections
        """
        new_input_data = self.member_data[start:end]
        new_gather = Gather(
            input_data=new_input_data,
            array_type=self.array_type,
            is_compact=self.is_compact,
            num_partition=self.num_partition,
        )

    def __getitem__(self, i, j):
        """
        Index operator to return data value at sample index i,j.
        """
        val = self.member_data[i][j][0]
        if self.is_parallel:
            return val.compute()
        else:
            return val

    def __setitem__(self, i, j, newvalue):
        """
        Index setter - I think this is the right syntax for two indices.
        """
        self.member_data[i][j][0] = newvalue
