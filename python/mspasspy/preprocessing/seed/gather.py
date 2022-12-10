from abc import ABC, abstractmethod


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
    with this desig the base class is an incomplete skeleton that
    is fleshed out by subclasses.   This clsss has some virtual methods
    (defined with the ABC @abstractmethod decorator) and a set of methods
    that would never work if only the base class were contructed
    (not allowed because of the virtual meethods).  In particular, the
    way the base class handle metadata is potnetially confusing because
    the base class constructor only defines stubs for the data used to
    hold member and ensemble metadata

    A new design feature suggested here not in our earlier documents is the
    idea of dynamic, periodic resizes for the append method.  The append
    method and the description of resize_blocksize in the constructor.


    A design issue for discussionis is whether not a gather should implement
    the ator and rota functionality of BasicTimeSeries.   The cost is
    tiny up front and retrofitting after that fact might cause a lot of
    headaches.  REcommend we implement them as defined below.

    """

    def __init__(
        self,
        number_members,
        npts,
        number_components=1,
        dt=None,
        array_type="numpy",
        resize_blocksize=10,
    ):
        """
            This base class constructor should take the view that the base
            class is the fraework with no data.  That is in line with
            BasicTimeSeries and an appropriate launching for subclasses that
            define scalar and 3C data as conrete implementations.
            A difference here, however, is that constuctor will create the
            work array but just not populate it.
        .

            :param number_members: Expected number of signals in the ensemble
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
            :type number_members:  integer

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

            :param resize_blocksize:  Resizing a container like ths is always
              going to be an expensive operation.  The container needs to behave
              like C++ stdlib containers where there is a hidden blocksize
              that triggers periodic reallocs when the data buffer fills.
              This parameter is a proposed way to implement the same concept.
              It would mainly be used for the append method.   Whenever append
              hits the array wall it would resize the array adding resize_blocksize
              columns (signal slots), copy the current data, append the new,
              and destroy the old.  Obviously a potentially very expenseive
              operation but a very useful feature. This name sucks but the
              concept is what is important
            :type resize_blocksize:  integer

        """
        # these need additional code to handle autosetting but for now
        # define them like this to ake below clearer

        self.npts = npts
        self.dt = dt
        self.array_type == array_type
        # we need an error log
        self.elog = ErrorLogger()
        # this a global boolean used to mark the entire ensemble invalid
        # when false.
        self.ensemble_is_marked_live = False
        self.live = []  # boolean array for members
        self.t0 = []  # starttime array (maybe should be an ndarray)
        # This illustrates the idea of how number_members should be used
        # Not complete but illustrtes the idea
        if input_data is None:
            if number_members is None:
                raise RuntimeError("usage message")
            self.number_members = 0
            self.array_member_size = number_members
            self.data = _private_create_function(self.npts, self.array_member_size)
        elif isinstance(input_data, TimeSeriesEnsmeble):
            self.number_members = len(input_data.member)
            self.array_member_size = self.number_members
            self.data = _private_create_function(elf.npts, self.array_member_size)
        elif isinstance(input_data, SeismogramEnsemble):
            elf.number_members = len(input_data.member)
            self.array_member_size = self.number_members
            self.data = _private_create_function_for_3C(
                elf.npts, self.array_member_size
            )
        else:
            raise TypeError("Input is not a vaid type")

    def append(self, mspass_object, otherargs):
        """
        Appends data in mspass_object to internal array object.
        Needs to handle sizing similar to std containers in C++
        where allocs (in this case a resize) is triggered by filling.
        Constructors need to also handle this concept - see above.
        The resize increment is resize_blocksize.  When append filsl the
        current space (self.number_members == self.array_member_size)
        a resize will be triggered.
        """
        pass

    def starttime(self) -> list:
        """
        Return a list (pythona array) of starttimes.  We should assume
        times are always stored as doubles.
        """
        pass

    def column_values(self) -> list:
        """
        Returns the values assigned to the column axis.  These should
        default to integers matching the column index values.   They
        should be allowed to be anything, however, to handle things
        like variable offset record sections.
        """
        pass

    def set_column_values(self, x):
        """
        Set the column values array to the values in x.
        """
        pass

    # The following names match BasicTimeSeries because the match in
    # concept.  starttime above doesn't really because in a single object
    # t0==starttime is constant.
    def time(self, i, j):
        """
        Return time of array position i,j (Note works the same for 3c and
        scalar data)
        """
        pass

    def sample_number(self, time, x) -> tuple:
        """
        Return a tuple of ints of the index position of an ordered pair defined by a time
        (row) value and a column value that can be mapped to the
        column index.
        """
        pass

    def time_is_UTC(self):
        """
        Returns True if the time standard for the data is set as
        UTC
        """
        pass

    def time_is_relative(self):
        """
        Returns True if the time standard for the data is set as
        relative
        """
        pass

    def live(self, j) -> bool:
        """
        Return true if jth member is set live.
        """

    def dead(self, j) -> bool:
        """
        Return true if jth member is marked dead (opposite of live).
        """

    # This next set are s in BasicTimeSeries but all
    def samprate(self):
        return 1.0 / self.dt

    def rtoa(self):
        pass

    def ator(self, shift):
        pass

    def shift(self, timeshift):
        pass

    # The following methods in BasicTimeSeries are optional.  It isn't
    # clear to me if their use is required - at least initially.  With
    # pythona ddingg methods is not as probematic as a Java or C++
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
        pass

    def set_metadata(self, j, md):
        """
        Setter for metadata of member j.   md is the new continer that would
        replace current conent.  Most useful for constructors.
        """
        pass

    def edit_metadata(self, j, md):
        """
        Differs from set_metadata in that the contents of md are added to
        the current and do not fully repalce them.   Needed for updating
        metadata after costruction
        """
        pass

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
        pass

    def sync_metadata(self):
        """
        This would act like the one in Ensemble in C++ copying all the
        ensemble key-value pairs to the members.  It is debatable that
        this would be needed but would be trivial to implement.
        """
        pass

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


class Gather(BasciEnsembleArray):
    """
    Concrete implementtion for a scalar gather.  This is the array
    equivalent of a TimeSeriesEnsemble appropriate when the input matches
    the concept of BasicGather.    It follows the OOP stndard
    pardigm that creation is initialization.  This puts a lot of features
    in the constructor for the class.
    """

    def __init__(
        self,
        mspass_object,
        number_members=None,
        npts=None,
        dt=None,
        array_type="numpy",
        resize_blocksize=10,
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
        # this is a rough prototype showing how the base clsss constructor
        # should be used.
        if mspass_object is None:
            if number_members is None or npts is None:
                raise RuntimeError("illegal input ")
            super().__init__(
                number_members,
                npts,
                number_components=1,
                dt=dt,
                array_type=array_type,
                resize_blocksize=resize_blocksize,
            )
        elif isinstance(mspass_object, TimeSeriesEnsemble):
            enssize = len(mspass_object.member)
            if number_members is None:
                nmem_to_use = enssize
            else:
                if number_members < enssize:
                    nmem_to_use = enssize
                else:
                    nmem_to_use = enssize
            for d in mspass_object.member:
                if d.live:
                    npts_to_use = d.npts
                    break
            super().__init__(
                nmem_to_use,
                npts_to_use,
                number_components=1,
                dt=dt,
                array_type=array_type,
                resize_blocksize=resize_blocksize,
            )
            # elo is assumed created in base clsss consructor
            self.elog.log_error("npts and input disageement", ErrorSeverity.Warning)

            # Additional code to initialie here to load metadata and any
            # other stuff
        elif isinstance(mspass_object, Gather):
            # this maybe could do with deepcopy but suspect the disk
            # arrays might not work
            super().__init__(
                mspass_object.number_members,
                mspass_object.npts,
                number_components=1,
                dt=mspass_object.dt,
                array_type=mspass_object.array_type,
                resize_blocksize=mspass_object.resize_blocksize,
            )
            # additional code here to load other attributes to copy
        else:
            raise RuntimeError("illegal input error")

    def member(self, j) -> TimeSeries:
        """
        Returns the data associated with member j.  Unlike above I suggest
        we not do the idea of "data_only".  Aways return a TimeSeries.
        Suggest data method for that purpose
        """
        pass  # stub

    def data(self, j) -> ndarray:
        """
        Return the raw data vector associated with column j.   Defined here
        as an ndarray return but probably should be any iterable
        container that acts like a vector.  Not clear what will be needed to
        support large matrix formats.
        """
        pass

    def subset(self, start, end) -> Gather:
        """
        Return a subset of the Gather with signals from start to end
        (like start:end in F90 or matlab).

        TBD is if subset should be overloaded to allow other selections
        """
        pass

    def __getitem__(self, i, j):
        """
        Index operator to return data value at sample index i,j.
        """
        pass

    def __setitem__(self, i, j, newvalue):
        """
        Index setter - I think this is the right syntax for two indices.
        """
        pass
