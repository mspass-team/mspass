import dask.distributed as ddist

from collections.abc import Mapping, Iterable


def sliding_window_pipeline(
    dlist,
    processing_function,
    dask_client,
    sliding_window_size="auto",
    task_per_worker=2,
    completion_function=None,
    accumulator=None,
    pfunc_args=None,
    pfunc_kwargs=None,
    cfunc_args=None,
    cfunc_kwargs=None,
    a_args=None,
    a_kwargs=None,
    verbose=False,
):
    """
    Run a processing function and optional completion function on an
    interable input using the sliding window of Futures algorithm.

    Embarrassingly parallel workflows can often be reduced to a single
    function that chains a sequence of algorithms together.
    In dask terminology that workflow defines a pipeline.
    Such a workflow can also be done with a sequence of dask/spark map
    operators chained together.  Experience has show, however, that a large
    fraction of workflows using MsPASS fail with memory faults in dask
    when implemented with the dask bag map method. The reason seems
    to be that the problematic workflows reduce to a sequence of three fundamentally
    different operations that originate with a database query, list of
    documents, or a DataFrame:  (1) read seismic data defined by the (small) inputs
    into the bag, (2) run a sequence of processing functions on that data, and
    (3) save results (usually involving a combination of file writes and
    database adds).  That causes the workflow to bloat by orders of magnitude
    inside the chain of map opeators.   The default configuration of dask
    does not handle this well and tends to queue too many tasks for
    workers leading to worker memory faults that are often mysterious
    leaving only indirect forensic evidence. This funtion was developed
    to handle pipeline processing (equivalent to a string of map operators)
    subject to memory faults. We know of two common situations where this
    function should be used instead of the standard map operators of the
    map-reduce paradigm:  (1) atomic processing of very large (10^5 or more)
    data sets, or (2) handling large ensembles that push worker memory
    limits.  Guidelines on how to estimate memory use can be found in the
    MsPASS User Manual.

    The way this algorithm controls memory use is by limiting the scheduler
    to only process data defined by the "sliding_window_size" argument.
    The function uses dask Futures (note Futures are supported only in dask
    so this function cannot be used with a pyspark workflow) and submits
    and runs only sliding_window_size instances of the task defined by
    the "completion_function" argument.  The default behavior runs
    approximately 2 instances of the function per worker.  The number is
    "approximate" because the scheduler algorithm often queues up more
    instances on startup and the load is unbalanced until all workers are
    assigned data to process.   That means for the default with
    tasks_per_worker=2, a rough measure of the minimum memory size per worker is
    c + a*D where c is an estimate of the base memory use by each worker
    process, D is the nominal maximum data size of data objects created and
    used in the processing function, and a is some multiplier larger than 2.
    In my (glp) experience a safe guess for a is about 2 or 3.   If running on
    a single node a=1 can still work provided (D+c)*N_workers is less than the
    memory size on the worker node available to workers.  That is, memory
    not needed by the operating system and other MsPASS systems that may or
    may not be running on the same node.  See the MsPASS User Manual for
    more guidance on this issue.

    The other element of this function is the optional "completion_function"
    argument.  That function is run on the output of every instance of
    the function returned by workers.  By default the output of the
    completion function replaces that of the processing function yielding
    a list of the same size as the input `dlist`.   For large numbers of
    inputs such a giant list can be problematic for a variety of reasons.
    For that reason this function has an optional `accumulator`
    argument.  As the name implies it should be an accumulation function
    that creates a summary of the output or some useful combination of
    the outputs (e.g. a stack of data).   That function must have
    at least two required arguments and they must match this
    somewhat rigid expectation:

      arg0 - must be the current value being accumulated.  ALSO the
        function must define arg0==None as a signal that the
        accumulated result is to be initialized.  That is required
        for the first time the function is entered.
      arg1 - is the current value to be merged (accumulated into)
        the value of arg0.
    Each call to accumulator should return a value that is the
    same type as arg0.   That and the None initialization allows the
    accumulator to be stateless.   Optional *args and **kwargs
    values can be passed by the optional a_args and
    a_kwargs parameters.

    The function supports two types of completion functions controlled
    by the boolean argument "completion_is_accumulator".  When True
    the completion function is expected to be an accumulator. In that
    mode the completion function should still return it's current
    result but the result will overwritten as each Future completes.
    The function then returns the final value.


    The function is made generic at the cost of having a rather complicated
    API.   That is summarized here, but the reader is advised to consult
    the MsPASS User Manual and online sources if you aren't intimately familiar
    with the way "*args" and "**kwargs" are used in python.

    :param dlist:  any iterable with components matching the requirements
      of arg0 of processing_function.  Commonly a list of queries or
      MongoDB documents.  Note although it should theoretically work we do not recommend
      using a MongoDB CommandCursor for this argument as it is subject to
      timeout errors if the processing function runs for a significant time.
    :param processing_function:   symbol defining the function object
      (usually just the function name) to run on each component of dlist.
      If the function has required arguments you must define the related
      argument pfunc_args.  If it requires kwarg parameters you must
      also define pfunc_kwargs.
    :param pfunc_args:  iterable container defining the list of values for
      required arguments to the function defined as processing_function.
    :type pfunc_args:   technically any iterable with a length at least as
      long as the required arg list by processing_function.   Usually a simple
      python list.  e.g. if signature signature for the processing function
      is
      ```
      def myprocessor(a,b,c):
      ```
      and you have b=2.0 and c=5 you should set `pfunc_args=[2.0,3]`.


        function that is to be applied to each component of dlist.  This function will
       be used with dask distributed submit using *args and **kwargs with this equivalent call:
       `processing_function(d,*args,**kwargs)
    :param dask_client:  instance of a dask.distributed.Client that defines
       the cluster to which the processing_function is to run upon.  In
       MsPASS normal use is to fetch this with the MsPASS client
       `get_scheduler` method.
    :param sliding_window_size:   size of the sliding window submit buffer.
    :type sliding_window_size:  must be either the unique string "auto" or
      an integer.  If set to "auto" (default) the sliding window size is set to
      the value of `tasks_per_worker` times the number of workers
      dask_client responds are running when this function is first called.
      Note that works perfectly on HPC systems but can fail in cloud
      environment if dask is runnign with dynamic scheduling.  This can
      also be a integer value that manually sets the sliding window size.
    :param task_per_worker: determines the size of the sliding window
      when `sliding_window_size="auto"`.  Ignored if the sliding window
      size is set manually with an integer value.
    :param completion_function:   if defined the result returned by each submit
       will be processed with this function.   When defined the function
       output will be the single output of this function.
    :param accumulator: optional accumulator function applied to the
       output of the completion function.   In most cases the large list
       returned by this function with or without a completion function is
       awkward to catastrophic to deal with.  Further there are algorithms
       where the completion function needs to act as a Reduce operator
       (an efficient way to do that for large data sets).  You can use
       this optional function to allow an accumulation operation.
       The function requires to arguments:  arg0 must be the previous
       value handle, and arg1 the current value being handled.  It must
       also emit the same type as arg0 it receives.   You should think of
       the operation as a generalization of `return arg0+=arg1`.
       It is ESSENTIAL this function handle the case with arg0 set to None
       and handle that as an initializer and return a valid initial
       value for the return.
    :type completion_function:  a function object or None.  If None (default)
       the outputs from each future are simply appended to a list.
       When defined each return will be passed to this function.
    :param cfunc_args: comparable to `pfunc_args` but for the completion
       function.  Set to None (default) if the completion function does not
       have any required arguments.
    :param cfunc_kwargs:  comparable to pfunc_kwargs but for the completion
       funcion.  Set to None (default) if the completion_function does not
       require kwargs.
    :param a_args: comparable to `pfunc_args` but for the accumulator
       function.  Set to None (default) if the accumulatgor function does not
       have any required arguments.
    :param a_kwargs:  comparable to pfunc_kwargs but for the accumulator
       function.  Set to None (default) if the accumulator does not
       require kwargs.
    :param verbose:  boolean controlling output.  When False (default)
       the function itself is totally silent.  When set True it
       prints a message for each submit and each return.  Not recommended for workflows
       that use a large number of submits as the output can easily get huge.

    :return:   When the completion_function is not defined (default)
       this function will return a list of return values from each
       component of dlist passed through the processing function.
       Be warned this list can get huge if the data set is large
       and anything but a tiny datum is returned by processing_function.
       The definitive use of this function in MsPASS is a function
       that runs `Database.save_data` returning the default output of that
       method.   That is easily handled as the default is the a list of
       boolean values.  If, however, the same function is run with
       `return_data=True` a memory overflow in the caller is likely
       unless the entire output of the processing fits in the memory
       space of the caller.  When a completion function is used the
       return is the return of the completion function.
       A complexity is that if the `accumulator` function is defined
       the return is NOT a list but the accumulated output of
       that function.
    """
    # check args for validity and set integer run_mode for combinations of
    # logic for processing_function and completion_function arguments
    alg = "sliding_window_pipeline"
    if not isinstance(dlist, Iterable):
        message = "{}:  Illegal value for arg0 - must be an interable container (usually a list)".format(
            alg
        )
        raise ValueError(message)
    if not callable(processing_function):
        message = "{}:  Illegal value for arg1 - must be the name of a processing function to be submitted".format(
            alg
        )
        raise ValueError(message)
    if pfunc_args is None:
        # this and the comparable logic with kwargs is needed or a type error will be thrown when we use it
        # this is how python accepts a null args
        pfunc_args = ()
    else:
        if not isinstance(pfunc_args, Iterable):
            message = "{}:  Illegal input for pfunc_args.   Must be an iterable (usually a list)".format(
                alg
            )
            raise ValueError(message)
    if pfunc_kwargs is None:
        pfunc_kwargs = {}
    else:
        if not isinstance(pfunc_kwargs, Mapping):
            message = "{}:  Illegal input for pfunc_kwargs.   Must be an dict like container (isinstance(x,Mapping) == True)".format(
                alg
            )
            raise ValueError(message)
    # handle cfunc slightly differently because it may be used at all
    # this conditional couod be used directly below but to me this
    # makes logic clearer
    if completion_function is None:
        run_completion_function = False
    else:
        run_completion_function = True
        if not callable(completion_function):
            message = "{}:  Illegal value for completion_function argument\n".format(
                alg
            )
            message += "Must be a variable defining a function to apply to outputs of completion_function (arg1)"
            raise ValueError(message)
        if cfunc_args is None:
            cfunc_args = ()
        else:
            if not isinstance(cfunc_args, Iterable):
                message = "{}:  Illegal input for cfunc_args.   Must be an iterable (usually a list)".format(
                    alg
                )
                raise ValueError(message)
        if cfunc_kwargs is None:
            cfunc_kwargs = {}
        else:
            if not isinstance(cfunc_kwargs, Mapping):
                message = "{}:  Illegal input for cfunc_kwargs.   Must be an dict like container (isinstance(x,Mapping) == True)".format(
                    alg
                )
                raise ValueError(message)
        if accumulator is not None:
            if not callable(accumulator):
                message = "{}:  Illegal value for accumulator argument\n".format(alg)
                message += "Must be a variable defining a function to apply to outputs of completion_function"
                raise ValueError(message)
            if a_args is None:
                a_args = ()
            else:
                if not isinstance(a_args, Iterable):
                    message = "{}:  Illegal input for a_args.   Must be an iterable (usually a list)".format(
                        alg
                    )
                    raise ValueError(message)
            if a_kwargs is None:
                a_kwargs = {}
            else:
                if not isinstance(a_kwargs, Mapping):
                    message = "{}:  Illegal input for a_kwargs.   Must be an dict like container (isinstance(x,Mapping) == True)".format(
                        alg
                    )
                    raise ValueError(message)

    swsize = 0  # initialization - set in block below
    if isinstance(dask_client, ddist.Client):
        active_workers = dask_client.nthreads()
        num_workers = len(active_workers)
        del active_workers
        if sliding_window_size == "auto":
            swsize = round(num_workers * task_per_worker)
            if swsize < num_workers:
                print(
                    f"{alg}:  WARNING set sliding window size to {swsize} which smaller than the number of workers={num_workers}"
                )
                print(
                    "This will not use the dask cluster if you are running this job on HPC"
                )
                print(
                    "This setting makes sense only if you are running on a cluster service being used by multiple applications"
                )
    else:

        message = "{}: Illegal value for arg2.  Must be an instance of dask.distributed.Client".format(
            alg
        )
        raise ValueError(message)
    futures_list = list()
    N = len(dlist)
    if verbose:
        print(f"Processing {N} items")
    if N < swsize:
        swsize = N
    i_d = 0
    for d in dlist:
        if verbose:
            print("Submitting item number ", i_d)
        f = dask_client.submit(processing_function, d, *pfunc_args, **pfunc_kwargs)
        futures_list.append(f)
        i_d += 1
        if i_d >= swsize:
            break
    # initialize both of these although one or the other is returned
    results = list()
    accumulated_output = None
    number_handled = 0
    seq = ddist.as_completed(futures_list)
    for f in seq:
        f_result = f.result()
        if verbose:
            print("Handling result number ", number_handled)
        if run_completion_function:
            f_result = completion_function(f_result, *cfunc_args, **cfunc_kwargs)
            if accumulator is None:
                results.append(f_result)
            else:
                # accumulator MUST handle None for arg0 - see docstring
                accumulated_output = accumulator(
                    accumulated_output, f_result, *a_args, **a_kwargs
                )
        # found to be necessary to assure dask doesn't hog memory held by f
        dask_client.cancel(f)
        del f
        if i_d < N:
            if verbose:
                print("Submitting item number ", i_d)
            f = dask_client.submit(
                processing_function, dlist[i_d], *pfunc_args, **pfunc_kwargs
            )
            seq.add(f)
            i_d += 1
    if accumulator is None:
        return results
    else:
        return accumulated_output
