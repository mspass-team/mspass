import pymongo


def get_jobid(db):
    """
    Ask MongoDB for the a valid jobid.

    All processing jobs should have a call to this function at the beginning
    of the job script.  It simply queries MongoDB for the largest current
    value of the key "jobid" in the history collection.   If the history
    collection is empty it returns 1 under a bias that a jobid of 0 is
    illogical.

    :param db: database handle
    :type db:  top level database handle returned by a call to MongoClient.database
    """
    hiscol = db.history
    hist_size = hiscol.find().count()
    if hist_size <= 0:
        return 1
    else:
        maxcur = hiscol.find().sort([("jobid", pymongo.DESCENDING)]).limit(1)
        maxcur.rewind()  # may not be necessary but near zero cost
        maxdoc = maxcur[0]
        return maxdoc["jobid"] + 1


def pfbranch_to_dict(pf, key):
    """
    Recursive function to convert a single branch in an AntelopePf to a python dict.

    This function utilizes recursion to follow a chain of arbitrary length of
    branches defined in an AntelopePf object.   Result is a dict with a chain of
    dicts of the same length.  i.e. if AntelopePf has 3 levels of branches
    the dict will have a 3 levels of associative arrays keyed by the same
    branch names as the Arr items in the original Pf file.  Note this should
    be called from the top level one branch at a time.  i.e. for the parent
    AntelopePf this function should be called once for each returned key by
    pf.arr_keys().

    Note that at each level Tbl& sections of the original pf are parsed to
    be converted to lists of strings with each line of the Tbl section being
    one string in the list.

    :param pf: is an AntelopePf.  Recursive calls use get_branch outputs
       that return one of these.
    :param key: key used to access the branch requested
    :type key: string
    :return:  python dict translation of AntelopePf branch structure
    :raise:  RunTime errors are possible from the ccore methods that are called.
    """
    brkeys = pf.arr_keys()
    if len(brkeys) > 0:
        # This loads simple parameters at this level
        allbrdata = pf.todict()
        # This loads any Tbl& data at this level of the hierarchy
        tblkeys = pf.tbl_keys()
        for k in tblkeys:
            tvals = pf.get_tbl(k)
            allbrdata[k] = tvals
        for k in brkeys:
            pfbranch = pf.get_branch(k)
            bk = pfbranch_to_dict(pfbranch, k)
            allbrdata[k] = bk
        return allbrdata
    else:
        brdata = pf.todict()
        return brdata


class basic_history_data:
    """
    This is a pure data object that if it were written in C could be
    defined as a struct.   It holds the data used to define the
    parameters for a given algorithm.
    """

    def __init__(self, job):
        self.jobid = job
        self.algorithm = "UNDEFINED"
        self.param_type = "NONE"
        self.params = {}  # dict with unspecified content dumped to collection

    def load_algorithm_args(self, alg, argdict):
        """
        Loads parameters defined to a set of function arguments.

        Simple algorithms without a lot of parameters often simply need a
        set of argument values.   Here we require this to be defined by
        a set of key:value pairs that map to dict.  We also consider this
        the lowest common denominator for a parameter definition so make
        it a part of the base class.

        :param alg: This should be a string defining the algorithm being registered.
        :param argdict:  This should be a dict of key:value pairs defining input
           parameters.  For algorithms defined at the top level by a python
           function this should match the names of parameters in the arg list.
           For C++ functions wrapped with pybind11 it should match the arg
           keys defined in the wrappers.   The key string will be used for
           key:value pair in BSON written to MongoDB.
        """
        self.algorithm = alg
        self.param_type = "dict"
        self.params = argdict


class pf_history_data(basic_history_data):
    """
    Loads history data container with data from an AntelopePf object.

    mspasspy.ccore.utility defines the AntelopePf object that is an option for parameter
    inputs.  The file structure is identical to the
    Antelope Pf file syntax.   The API to an AntelopePF is not, however,
    the same as the python bindings in Antelope as it handles Tbl
    and Arr sections completely differently more in line with alternatives
    like YAML.   This method converts the data in an AntelopePf to a python
    dict that can be dumped directly to MongoDB with pymongo's insert
    methods. Converting the MongoDB document back to a pf structure requires
    the inverse operator that does not exist, but should eventually be
    created if this approach sees extensive use.
    """

    def __init__(self, job, alg, pf):
        """
        Basic constructor for this subclass.

        This constructor applies the construction is initialization model
        of oop.  The AntelopePf pointed to by pf is parsed in this constuctor
        to file and set the params dict and other attributes.

        :param job: jobid (integer) normally should be preceded by call to get_jobid function.
        :param alg:  string defining a name assigned to the algorithm field
        :param pf:  AntelopePf object to be parsed and posted.
        """
        self.jobid = job
        self.algorithm = alg
        self.param_type = "AntelopePf"
        # This works because Metadata2dict calls pf.keys() which only returns
        # simple name:value pair parameters.  We use branch and tbl calls later
        self.params = pf.todict()
        # tbl's next - simpler than a Arr which requires recursion
        tblkeys = pf.tbl_keys()
        for k in tblkeys:
            tvals = pf.get_tbl(k)
            # testing suggests tvals is a regular python list so this
            # should work cleanly
            self.params[k] = tvals
        # Arr's are harder if we want to allow them to be arbitrarily deep.
        # Hence we use this recursive function defined above.
        arrkeys = pf.arr_keys()
        for k in arrkeys:
            branchval = {}
            branchval = pfbranch_to_dict(pf, k)
            self.params[k] = branchval


class HistoryLogger:
    """
    Base class for generic, global history/provenance preservation in MsPASS.
    The main concept of this object that a pymongo script to run a processing
    job would create this object or one of it's children to preserve the
    global run parameters for the a processing sequence.   We limit that to
    mean a sequence of processing algorithms that have a set of predefined
    parameters that control their behaviour.  The global parameters are
    preserved in a special collection in MongoDB we give the (fixed) name
    of "history".
    """

    def __init__(self, db, job=0):
        """
        Basic constructor.

        This is currently the only constructor for this object.  It creates
        a handle to MongoDB and sets a unique integer key called jobid.
        Calling this constructor will guaranetee the jobid will be unique.

        :param db:   is a top level handle to a MongoDB server created by
           calling the database method of a MongoClient instance.
        :param job: job can be used to manually set the jobid.  We use
           a simple high water mark comparable to lastid in the
           Antelope/Datascope database where the next valid id is lastid+1.
           Hence if the input value of job is less than the current high
           water mark in the history collection for jobid, the jobid is
           silently set to the +1 of the largest jobid found in history.
           (default is 0 which automatically uses the high water mark method)
           Users can get the actual value set from the jobid variable after
           successful creation of this object.
        """
        self.history_collection = db.history
        # Check the input job id for validity and use get_jobid if needed
        if job == 0:
            self.jobid = get_jobid(db)
        else:
            jobtmp = get_jobid(db)
            if job > jobtmp:
                self.jobid = job
            else:
                self.jobid = jobtmp
                print(
                    "HistoryLogger(Warning):  input jobid=",
                    job,
                    " was invalid.  Set jobid=",
                    jobtmp,
                )
        self.history_chain = []  # create empty container for history record

    def register(self, alg, partype, params):
        """
        Register an algorithm's signature to preserve processing history.
        Each algorithm in a processing chains should be registered by this
        mechanism before starting a mspass processing chain.  The
        register method should be called in the order in which the algorithms are
        applied.

        :param alg: is the name of the algorithm that will be run.  Assumed to be
               a string.
        :param partype: defines the format of the data defining input parameters
               to this algorithm (Must be either 'dict' or 'AntelopePf')
        :param params: is the actual input data. Actual type of this data
               this arg references will depend up partype.  partype defines
               the type of the object expect (dict in this case means a python
               dict object)
        :raise:  Throws a RuntimeError with a message if partype is not on the
               list of supported parameter types
        """
        if partype == "dict":
            bhd = basic_history_data(self.jobid)
            bhd.load_algorithm_args(alg, params)
            self.history_chain.append(bhd)
        elif partype == "AntelopePf":
            pfhis = pf_history_data(self.jobid, alg, params)
            self.history_chain.append(pfhis)
        else:
            raise RuntimeError(
                "HistoryLogger (Warning):  Unsupported parameter type=" + partype
            )

    def save(self):
        """
        Save the contents to the history collection.

        The doc created in a save is more or less an image of the
        structure of this object translated to a python dict
        """
        doc = {}
        doc["jobid"] = self.jobid
        for d in self.history_chain:
            subdoc = {}
            subdoc["algorithm"] = d.algorithm
            subdoc["param_type"] = d.param_type
            subdoc["params"] = d.params
            doc[d.algorithm] = subdoc
        self.history_collection.insert_one(doc)
