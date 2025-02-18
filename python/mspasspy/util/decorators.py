from decorator import decorator

from mspasspy.util.converter import Stream2Seismogram, Trace2TimeSeries
from mspasspy.ccore.utility import MsPASSError, ErrorSeverity
from mspasspy.ccore.seismic import (
    Seismogram,
    TimeSeries,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
)
from mspasspy.util.seismic import number_live

# from mspasspy.global_history.manager import GlobalHistoryManager
from mspasspy.util import logging_helper


@decorator
def mspass_func_wrapper(
    func,
    data,
    *args,
    object_history=False,
    alg_id=None,
    alg_name=None,
    dryrun=False,
    inplace_return=False,
    function_return_key=None,
    handles_ensembles=True,
    checks_arg0_type=False,
    handles_dead_data=True,
    **kwargs,
):
    """ 
    Decorator wrapper to adapt a simple function to the mspass parallel processing framework.

    This decorator can be used to adapt a processing function to the 
    mspass framework.  It can be used to handle the following nonstandard 
    functionality that would not appear in a function outside of mspass:
        1.  This decorator can make a function dogmatic about the type of arg0.
            A processing function in mspass normally requires arg0 to be a 
            seismic data type.   If the function being decorated handles 
            arg0 propertly add `checks_arg0_type=True` to the functions 
            kwargs list.  That will assure the error handler used by the 
            function itself will be used instead of the generic wrapper code.
            When False (the default) this decorator will always throw a 
            TypeError with a generic message if arg0 is invalid.
        2.  MsPASS data objects all use the concept of the boolean "live"
            as a way to mark a datum as valid (dead means it is bad). 
            MsPASS functios should normally kill a problem datum and 
            not throw exceptions for anything but system level error 
            like a malloc error. A properly designed function 
            has a test for dead data immediately after verifying 
            arg0 is valid and testing of "data.live" is a valid thing to 
            do.  To assure that doesn't happen if checks_arg0_type is 
            false and the test for a valid type yields success
            (i.e. find data is a valid seismic object) a test for 
            live will data will be done and the functionw will immediately 
            return a datum marked dead.  If set True, the default, we let 
            the functions dead data handler do the task.
        3.  MsPASS data object have an internal mechanism to preserve 
            object-level history that records the sequence of algorithms 
            applied to a piece of data to get it to it's current stage  
            This decorator adds that functionality through three kwarg 
            values: `object_history`,`alg_id', and `alg_name'.  `object_history`
            is  boolean that enables (if True) or disables (False - default)
            that mechanism.   The other two are tags.   See the User 
            Manual for more details.  
        4.  MsPASS data are either "atomic" (TimeSeries or Seismogram) or 
            "ensembles" (TimeSeriesEnseble and SeismogramEnsemble).   
            "ensembles", however, are a conainer holding one or more 
            atomic objects.  Processing algorithms may require only atomic 
            or ensemble objects.  For algorithms that expect atomic data, 
            however, it is common to need to apply the same function to 
            all ensemble "members" (the set of atomic objects that define the
            ensemble).   Some processing functions are designed to handle 
            either but some aren't.  If a function is set up to hanlde 
            ensembles, the `handles_ensembles` boolean should be set True. 
            That is set True because a well designed algorithm for use with 
            MsPASS should do that.  Set `handles_ensembles=False` if the 
            function is set up to only handle atomic data.  In that situation 
            the decorator will handle ensembles by setting up a loop to 
            call the decorated function for each enemble member. 
        5.  The boolean `inplace_return` should be set True on a function 
            that it set up like a FORTRAN subroutine or a C void return.  
            i.e. it doesn't return anything but alters data internally. 
            In that situation the decorator returns arg0 that is assumed to 
            have been altered by the function.
        6.  Use `function_return_key` if a function doesn't return a seismic 
            data object at all but computes something returned as a value. 
            If used that capability allows a return value to be mapped into 
            the Metadata container of the input data object and set with 
            the specified key.  
            
    Finally, this decorator has a standardized behavior for handling 
    ensembles that is not optional.  If the input is an ensemble it is 
    not at all uncommon to have an algorithm kill all ensemble members.  
    The ensemble container has a global "live" attribute that this decorator 
    will set False (i.e. define as dead) if all the member object are marked
    dead.

    :param func: target function
    :param data: arg0 of function call being decorated.  This symbol is 
      expected to point to an input data that is expected to be a MsPASS
      data object i.e. TimeSeries, Seismogram, Ensemble.
    :param args: extra positional arguments for decorated function
    :param object_history:  boolean used to enable or disable 
       object level history for this function.  When False (default)
       object-level history is not enabld for this function.  When 
       set True the decorator will add object history history th e
       name of the function and the value of alg_id.  
       Object level history is disabled by default for efficiency.  
       If object_history is set True and the string passed
       as alg_id is defined (not None which is the default) each 
       Seismogram or TimeSeries object will attempt to
       save the history through a new_map operation.   
       If the history chain is empty this will silently generate
       an error posted to error log on each object.
    :param alg_id: alg_id is a unique id to record the usage of func 
      while preserving the history.
    :type alg_id: str (e.g. str of an ObjectId)
    :param alg_name: alternative name to assign the algorithm.  If not 
      defined (set as None) the name of the decorated function is used.  
    :type alg_name: :class:`str` or None (default)
    :param dryrun: True for dry-run, the algorithm is not run, but the arguments used in this wrapper will be checked.
      This is useful for pre-run checks of a large job to validate a workflow. Errors generate exceptions
      but the function returns before attempting any calculations.
    :param inplace_return: when func is an in-place function that doesn't return anything, but you want to
       return the origin data (for example, in map-reduce), set inplace_return as true.
    :param function_return_key:  Some functions one might want to wrap with this decorator
       return something that is appropriate to save as Metadata.  If so, use this argument to
       define the key used to set that field in the data that is returned.
       This feature should normally be considered as a way to wrap an existing
       algorithm that you do not wish to alter, but which returns something useful.
       In principle that return can be almost anything, but we recommend this feature
       be limited to only simple types (i.e. int, float, etc.).  The decorator makes
       o type checks so the caller is responsible for assuring what is posted will not cause
       downstream problems.  The default for this parameter is None, which
       is taken to mean any return of the wrapped function will be ignored.  Note
       that when function_return_key is anything but None, it is assumed the
       returned object is the (usually modified) data object.  NOTE:  this
       options is NOT supported for ensembles.   If you set this value for
       ensemble input this decorator will throw a ValueError exception.
    :param handles_ensembes:  set True if the function this is applied to
      can hangle ensemble objects directly.   When False, which is the default
      this decorator applies the atomic function to each ensemble member in
      a loop over membes.
    :param kwargs: extra kv arguments
    :return: modified seismic data object.  Can be a different type than 
       the input.   If function_return_key is used only the Metadata 
       container  should be altered.
    """
    # Add an error trap for arg0 if this function doesn't do that
    if not checks_arg0_type:
        if not isinstance(
                data, (Seismogram, TimeSeries, SeismogramEnsemble, TimeSeriesEnsemble)
            ):
            raise TypeError("mspass_func_wrapper only accepts mspass object as data input")
        else:
            if data.dead():
                return data
    if function_return_key and isinstance(
        data, (TimeSeriesEnsemble, SeismogramEnsemble)
    ):
        message = "Usage error:  "
        message += "function_return_key was defined as {} but input type is an ensemble.\n".format(
            function_return_key
        )
        message += "That options is not allowed for ensembles in any function"
        raise ValueError(message)
    if not alg_name:
        alg_name = func.__name__

    if object_history and alg_id is None:
        raise ValueError(alg_name + ": object_history was true but alg_id not defined")

    if dryrun:
        return "OK"
    # reverse logic a bit confusing but idea is to do nothing if 
    # handles_dead_data is true assuming the function will handle dead 
    # data propertly
    if not handles_dead_data:
        if data.dead():
            return data

    if isinstance(data, (Seismogram, TimeSeries)):
        run_only_once = True
    elif isinstance(data, (TimeSeriesEnsemble, SeismogramEnsemble)):
            if handles_ensembles:
                run_only_once = True
            else:
                run_only_once = False
    else:
        # we only get here if checks_arg0_type is True and the 
        # type is bad - with this logic func will be called and 
        # we assume it throws an exception
        run_only_once = True
    try:
        if run_only_once:
            res = func(data, *args, **kwargs)
            if object_history:
                logging_helper.info(data, alg_id, alg_name)
            if function_return_key is not None:
                if isinstance(function_return_key, str):
                    data[function_return_key] = res
                else:
                    data.elog.log_error(
                        alg_name,
                        "Illegal type received for function_return_key argument="
                        + str(type(function_return_key))
                        + "\nReturn value not saved in Metadata",
                        ErrorSeverity.Complaint,
                    )
                if not inplace_return:
                    data.elog.log_error(
                        alg_name,
                        "Inconsistent arguments; inplace_return was set False and function_return_key was not None.\nAssuming inplace_return == True is correct",
                        ErrorSeverity.Complaint,
                    )
                return data
            elif inplace_return:
                return data
            else:
                return res
        else:
            # this block is only for ensembles - the run_only_once boolean
            # means we enter here only if this is an ensmble and the
            # boolean handles_ensembles is false
            N = len(data.member)
            for i in range(N):
                # alias to make logic clearer
                d = data.member[i]
                d = func(d, *args, **kwargs)
                data.member[i] = d
                if object_history:
                    logging_helper.info(data.member[i], alg_id, alg_name)
                    
            # mark ensmeble killed by all applications of func as dead
            N_live = number_live(data)
            if N_live<=len(data.member):
                message = "function killed all ensemble members.  Marking ensemble dead"
                data.elog.log_error(alg_name,message,ErrorSeverity.Invalid)
                data.kill()
                
            # Note the in place return concept does not apply to
            # ensemles - all are in place by defintion if passed through
            # this wrapper
            return data
    except RuntimeError as err:
        if isinstance(data, (Seismogram, TimeSeries)):
            data.elog.log_error(alg_name, str(err), ErrorSeverity.Invalid)
        else:
            logging_helper.ensemble_error(data, alg_name, err, ErrorSeverity.Invalid)
        # some unexpected error happen, if inplace_return is true, we may want to return the original data
        if inplace_return:
            return data
    except MsPASSError as ex:
        if ex.severity == ErrorSeverity.Fatal:
            raise
        if isinstance(data, (Seismogram, TimeSeries)):
            data.elog.log_error(alg_name, ex.message, ex.severity)
        else:
            logging_helper.ensemble_error(data, alg_name, ex.message, ex.severity)
        # some unexpected error happen, if inplace_return is true, we may want to return the original data
        if inplace_return:
            return data


@decorator
def mspass_func_wrapper_multi(
    func,
    data1,
    data2,
    *args,
    object_history=False,
    alg_id=None,
    alg_name=None,
    dryrun=False,
    **kwargs,
):
    """
    This wrapper serves the same functionality as mspass_func_wrapper, but there are a few differences. The first is
    this wrapper accepts two mspasspy data objects as input data. The second is that inplace_return is not implemented
    here. The same processing history and error logs will be duplicated and stored in both of the input.

    :param func: target function
    :param data1: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param data2: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param args: extra arguments
    :param object_history: True to preserve this processing history in the data object, False not to. object_history
     and alg_id are intimately related and control how object level history is handled.
     Object level history is disabled by default for efficiency.  If object_history is set True and the string passed
     as alg_id is defined (not None which is the default) each Seismogram or TimeSeries object will attempt to
     save the history through a new_map operation.   If the history chain is empty this will silently generate
     an error posted to error log on each object.
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param dryrun: True for dry-run, the algorithm is not run, but the arguments used in this wrapper will be checked.
      This is useful for pre-run checks of a large job to validate a workflow. Errors generate exceptions
      but the function returns before attempting any calculations.
    :param kwargs: extra kv arguments
    :return: the output of func
    """
    if not isinstance(
        data1, (Seismogram, TimeSeries, SeismogramEnsemble, TimeSeriesEnsemble)
    ):
        raise TypeError(
            "mspass_func_wrapper_multi only accepts mspass object as data input"
        )

    if not isinstance(
        data2, (Seismogram, TimeSeries, SeismogramEnsemble, TimeSeriesEnsemble)
    ):
        raise TypeError(
            "mspass_func_wrapper_multi only accepts mspass object as data input"
        )

    if not alg_name:
        alg_name = func.__name__

    if object_history and alg_id is None:
        raise ValueError(alg_name + ": object_history was true but alg_id not defined")

    if dryrun:
        return "OK"

    if is_input_dead(data1, data2):
        return

    try:
        res = func(data1, data2, *args, **kwargs)
        if object_history:
            logging_helper.info(data1, alg_id, alg_name)
            logging_helper.info(data2, alg_id, alg_name)
        return res
    except RuntimeError as err:
        if isinstance(data1, (Seismogram, TimeSeries)):
            data1.elog.log_error(alg_name, str(err), ErrorSeverity.Invalid)
        else:
            logging_helper.ensemble_error(data1, alg_name, err, ErrorSeverity.Invalid)
        if isinstance(data2, (Seismogram, TimeSeries)):
            data2.elog.log_error(alg_name, str(err), ErrorSeverity.Invalid)
        else:
            logging_helper.ensemble_error(data2, alg_name, err, ErrorSeverity.Invalid)
    except MsPASSError as ex:
        if ex.severity == ErrorSeverity.Fatal:
            raise
        if isinstance(data1, (Seismogram, TimeSeries)):
            data1.elog.log_error(alg_name, ex.message, ex.severity)
        else:
            logging_helper.ensemble_error(data1, alg_name, ex.message, ex.severity)
        if isinstance(data2, (Seismogram, TimeSeries)):
            data2.elog.log_error(alg_name, ex.message, ex.severity)
        else:
            logging_helper.ensemble_error(data2, alg_name, ex.message, ex.severity)


@decorator
def mspass_method_wrapper(
    func,
    selfarg,
    data,
    *args,
    object_history=False,
    alg_id=None,
    alg_name=None,
    dryrun=False,
    inplace_return=False,
    function_return_key=None,
    handles_ensembles=True,
    checks_arg0_type=False,
    handles_dead_data=True,
    **kwargs,
):
    """
    Decorator wrapper to adapt a class method function to the mspass parallel processing framework.

    This function serves as a decorator wrapper, which is widely used in the mspasspy library.
    It is used to wrap class methods which implement a method that does something
    to a mspass data object that is assumed to be passed as arg0 of the method.
    This decorator executes the target method on the input data.  It is used
    mainly to reduce duplicate code to perserve history and error logs
    with mspass object.

    A large fraction of algorithms used in mspass are "atomic" meaning
    they only operator on TimeSeries and/or Seismogram objects.   In mspass
    an "ensemble" is a container with multiple atomic objects.  This decorator
    can then also be used to adapt an atomic algorithm to work automatically
    with ensembles.   When using the decorator if the funtion being decorated
    works only on atomic data set the boolean `handles_ensembles` should be
    set as False.   True, which is the default, should be used functions that 
    handle ensembles automatically. 


    :param func: target function
    :param selfarg:  the self pointer for the class with which this method is associated
    :param data: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param args: extra arguments
    :param object_history: True to preserve this processing history in the data object, False not to. object_history
       and alg_id are intimately related and control how object level history is handled.
       Object level history is disabled by default for efficiency.  If object_history is set True and the string passed
       as alg_id is defined (not None which is the default) each Seismogram or TimeSeries object will attempt to
       save the history through a new_map operation.   If the history chain is empty this will silently generate
       an error posted to error log on each object.
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param dryrun: True for dry-run, the algorithm is not run, but the arguments used in this wrapper will be checked.
      This is useful for pre-run checks of a large job to validate a workflow. Errors generate exceptions
      but the function returns before attempting any calculations.
    :param inplace_return: when func is an in-place function that doesn't return anything, but you want to
       return the origin data (for example, in map-reduce), set inplace_return as true.
    :param function_return_key:  Some functions one might want to wrap with this decorator
       return something that is appropriate to save as Metadata.  If so, use this argument to
       define the key used to set that field in the data that is returned.
       This feature should normally be considered as a way to wrap an existing
       algorithm that you do not wish to alter, but which returns something useful.
       In principle that return can be almost anything, but we recommend this feature
       be limited to only simple types (i.e. int, float, etc.).  The decorator makes
       no type checks so the caller is responsible for assuring what is posted will not cause
       downstream problems.  The default for this parameter is None, which
       is taken to mean any return of the wrapped function will be ignored.  Note
       that when function_return_key is anything but None, it is assumed the
       returned object is the (usually modified) data object.
     :param handles_ensembes:  set True (default) if the function this is applied to
       can hangle ensemble objects directly.   When False this decorator 
       applies the atomic function to each ensemble member in
       a loop over members.
    :param kwargs: extra kv arguments
    :return: origin data or the output of func
    """
    # Add an error trap for arg0 if this function doesn't do that
    if not checks_arg0_type:
        if not isinstance(
                data, (Seismogram, TimeSeries, SeismogramEnsemble, TimeSeriesEnsemble)
            ):
            raise TypeError("mspass_method_wrapper only accepts mspass object as data input")
        else:
            if data.dead():
                return data

    # if not defined
    if not alg_name:
        # This obscure construct sets arg_name to the class name.
        # str is needed as type alone returns a type class.  str makes it a name
        # although the name is a bit ugly.
        alg_name = str(type(selfarg))

    if object_history and alg_id is None:
        raise ValueError(alg_name + ": object_history was true but alg_id not defined")
    if function_return_key and isinstance(
        data, (TimeSeriesEnsemble, SeismogramEnsemble)
    ):
        message = "Usage error:  "
        message += "function_return_key was defined as {} but input type is an ensemble.\n".format(
            function_return_key
        )
        message += "That options is not allowed for ensembles in any class method"
        raise ValueError(message)
    if dryrun:
        return "OK"

    # reverse logic a bit confusing but idea is to do nothing if 
    # handles_dead_data is true assuming the function will handle dead 
    # data propertly
    if not handles_dead_data:
        if data.dead():
            return data

    if isinstance(data, (Seismogram, TimeSeries)):
        run_only_once = True
    elif isinstance(data, (TimeSeriesEnsemble, SeismogramEnsemble)):
            if handles_ensembles:
                run_only_once = True
            else:
                run_only_once = False
    else:
        # we only get here if checks_arg0_type is True and the 
        # type is bad - with this logic func will be called and 
        # we assume it throws an exception
        run_only_once = True

    try:
        if run_only_once:
            res = func(selfarg, data, *args, **kwargs)
            if object_history:
                logging_helper.info(data, alg_id, alg_name)
            if function_return_key is not None:
                if isinstance(function_return_key, str):
                    data[function_return_key] = res
                else:
                    data.elog.log_error(
                        alg_name,
                        "Illegal type received for function_return_key argument="
                        + str(type(function_return_key))
                        + "\nReturn value not saved in Metadata",
                        ErrorSeverity.Complaint,
                    )
                if not inplace_return:
                    data.elog.log_error(
                        alg_name,
                        "Inconsistent arguments; inplace_return was set False and function_return_key was not None.\nAssuming inplace_return == True is correct",
                        ErrorSeverity.Complaint,
                    )
                return data
            elif inplace_return:
                return data
            else:
                return res
        else:
            # this block is only for ensembles - the run_only_once boolean
            # means we enter here only if this is an ensmble and the
            # boolean handles_ensembles is false
            N = len(data.member)
            for i in range(N):
                # alias to make logic clearer
                d = data.member[i]
                d = func(selfarg, d, *args, **kwargs)
                data.member[i] = d
                if object_history:
                    logging_helper.info(data.member[i], alg_id, alg_name)

            # mark ensmeble killed by all applications of func as dead
            N_live = number_live(data)
            if N_live<=len(data.member):
                message = "function killed all ensemble members.  Marking ensemble dead"
                data.elog.log_error(alg_name,message,ErrorSeverity.Invalid)
                data.kill()
            # Note the in place return concept does not apply to
            # ensemles - all are in place by defintion if passed through
            # this wrapper
            return data
    except RuntimeError as err:
        if isinstance(data, (Seismogram, TimeSeries)):
            data.elog.log_error(alg_name, str(err), ErrorSeverity.Invalid)
        else:
            logging_helper.ensemble_error(data, alg_name, err, ErrorSeverity.Invalid)
        # some unexpected error happen, if inplace_return is true, we may want to return the original data
        if inplace_return:
            return data
    except MsPASSError as ex:
        if ex.severity == ErrorSeverity.Fatal:
            raise
        if isinstance(data, (Seismogram, TimeSeries)):
            data.elog.log_error(alg_name, ex.message, ex.severity)
        else:
            logging_helper.ensemble_error(data, alg_name, ex.message, ex.severity)
        # some unexpected error happen, if inplace_return is true, we may want to return the original data
        if inplace_return:
            return data


def is_input_dead(*args, **kwargs):
    """
    A helper method to see if any mspass objects in the input parameters are dead. If one is dead,
    we should keep silent, i.e. no longer perform any further operations on this dead mspass object.
    Note for an ensemble object, only if all the objects of it are dead, we mark them as dead,
    otherwise they are still alive.

    :param args: any parameters.
    :param kwargs: any key-word parameters.
    :return: True if there is a dead mspass object in the parameters, False if no mspass objects in the input parameters
     or all of them are still alive.
    """
    for arg in args:
        if isinstance(arg, TimeSeries) and arg.dead():
            return True
        if isinstance(arg, Seismogram) and arg.dead():
            return True
        if isinstance(arg, TimeSeriesEnsemble):
            for ts in arg.member:
                if not ts.dead():
                    return False
            return True
        if isinstance(arg, SeismogramEnsemble):
            for ts in arg.member:
                if not ts.dead():
                    return False
            return True
    for k in kwargs:
        if isinstance(kwargs[k], TimeSeries) and kwargs[k].dead():
            return True
        if isinstance(kwargs[k], Seismogram) and kwargs[k].dead():
            return True
        if isinstance(kwargs[k], TimeSeriesEnsemble):
            for ts in kwargs[k].member:
                if not ts.dead():
                    return False
            return True
        if isinstance(kwargs[k], SeismogramEnsemble):
            for ts in kwargs[k].member:
                if not ts.dead():
                    return False
            return True
    return False


def timeseries_copy_helper(ts1, ts2):
    """ """
    # keys in this list are ignored - for now only one but made a list
    # to simplify additions later
    metadata_to_ignore = ["CONVERTER_ENSEMBLE_KEYS"]
    ts1.npts = ts2.npts
    ts1.dt = ts2.dt
    ts1.tref = ts2.tref
    ts1.live = ts2.live
    ts1.t0 = ts2.t0
    for k in ts2.keys():  # other metadata copy
        if k not in metadata_to_ignore:
            # because this is used only in decorator we can assure
            # overrides are consistent
            ts1[k] = ts2[k]
    # We need to clear this/these too as if they were already there the
    # above logic preserves them
    for k in metadata_to_ignore:
        if ts1.is_defined(k):
            ts1.erase(k)
    ts1.data = ts2.data


def timeseries_ensemble_copy_helper(es1, es2):
    for i in range(len(es1.member)):
        timeseries_copy_helper(es1.member[i], es2.member[i])
    # fixme: not sure in what algorithm the length of es1 and es2 would be different
    # also, the following does not address uninitiated history issues in the extended elements
    # same problem applies to the seismogram_ensemble_copy_helper
    if len(es2.member) > len(es1.member):
        es1.member.extend(es2.member[len(es1.member) :])


@decorator
def timeseries_as_trace(func, *args, **kwargs):
    """
    This decorator converts all the mspasspy TimeSeries objects in user inputs (*args and **kargs)
    to trace objects (defined in Obspy), and execute the func with the converted user inputs. After the execution,
    the trace objects will be converted back by overriding the data and metadata of the origin mspasspy objects. This
    wrapper makes it easy to process mspasspy objects using Obspy methods.

    :param func: target func
    :param args: extra arguments
    :param kwargs: extra kv arguments
    :return: the output of func
    """
    if is_input_dead(*args, **kwargs):
        return
    converted_args = []
    converted_args_ids = []
    converted_kwargs = {}
    converted_kwargs_keys = []
    for i in range(len(args)):
        if isinstance(args[i], TimeSeries):
            converted_args.append(args[i].toTrace())
            converted_args_ids.append(i)
        else:
            converted_args.append(args[i])
    for k in kwargs:
        if isinstance(kwargs[k], TimeSeries):
            converted_kwargs[k] = kwargs[k].toTrace()
            converted_kwargs_keys.append(k)
        else:
            converted_kwargs[k] = kwargs[k]
    res = func(*converted_args, **converted_kwargs)
    for i in converted_args_ids:
        ts = Trace2TimeSeries(converted_args[i])
        timeseries_copy_helper(args[i], ts)
    for k in converted_kwargs_keys:
        ts = Trace2TimeSeries(converted_kwargs[k])
        timeseries_copy_helper(kwargs[k], ts)
    return res


def seismogram_copy_helper(seis1, seis2):
    # keys in this list are ignored - for now only one but made a list
    # to simplify additions later
    metadata_to_ignore = ["CONVERTER_ENSEMBLE_KEYS"]
    seis1.npts = seis2.npts
    seis1.dt = seis2.dt
    seis1.tref = seis2.tref
    seis1.live = seis2.live
    seis1.t0 = seis2.t0
    for k in seis2.keys():  # other metadata copy
        if k not in metadata_to_ignore:
            # because this is used only in decorator we can assure
            # overrides are consistent
            seis1[k] = seis2[k]
    # We need to clear this/these too as if they were already there the
    # above logic preserves them
    for k in metadata_to_ignore:
        if seis1.is_defined(k):
            seis1.erase(k)
    seis1.data = seis2.data


def seismogram_ensemble_copy_helper(es1, es2):
    for i in range(len(es1.member)):
        seismogram_copy_helper(es1.member[i], es2.member[i])
    if len(es2.member) > len(es1.member):
        es1.member.extend(es2.member[len(es1.member) :])


@decorator
def seismogram_as_stream(func, *args, **kwargs):
    """
    This decorator converts all the mspasspy Seismogram objects in user inputs (*args and **kargs)
    to stream objects (defined in Obspy), and execute the func with the converted user inputs. After the execution,
    the stream objects will be converted back by overriding the data and metadata of the origin mspasspy objects.

    :param func: target func
    :param args: extra arguments
    :param kwargs: extra kv arguments
    :return: the output of func
    """
    if is_input_dead(*args, **kwargs):
        return
    converted_args = []
    converted_kwargs = {}
    converted_args_ids = []
    converted_kwargs_keys = []
    converted = False
    for i in range(len(args)):
        if isinstance(args[i], Seismogram):
            converted_args.append(args[i].toStream())
            converted_args_ids.append(i)
            converted = True
        else:
            converted_args.append(args[i])
    for k in kwargs:
        if isinstance(kwargs[k], Seismogram):
            converted_kwargs[k] = kwargs[k].toStream()
            converted_kwargs_keys.append(k)
            converted = True
        else:
            converted_kwargs[k] = kwargs[k]
    res = func(*converted_args, **converted_kwargs)
    if converted:
        # todo save relative time attribute
        # fixme cardinal here
        for i in converted_args_ids:
            seis = Stream2Seismogram(converted_args[i], cardinal=True)
            args[i].data = seis.data
            # metadata copy
            for k in seis.keys():
                args[i][k] = seis[k]
        for k in converted_kwargs_keys:
            seis = Stream2Seismogram(converted_kwargs[k], cardinal=True)
            kwargs[k].data = seis.data
            # metadata copy
            for key in seis.keys():
                kwargs[k][key] = seis[key]
    return res


@decorator
def timeseries_ensemble_as_stream(func, *args, **kwargs):
    """
    This decorator converts all the mspasspy TimeSeries ensemble objects in user inputs (*args and **kargs)
    to stream objects (defined in Obspy), and execute the func with the converted user inputs. After the execution,
    the stream objects will be converted back by overriding the data and metadata of each member in the ensemble.

    :param func: target func
    :param args: extra arguments
    :param kwargs: extra kv arguments
    :return: the output of func
    """
    if is_input_dead(*args, **kwargs):
        return
    converted_args = []
    converted_kwargs = {}
    converted_args_ids = []
    converted_kwargs_keys = []
    converted = False
    for i in range(len(args)):
        if isinstance(args[i], TimeSeriesEnsemble):
            converted_args.append(args[i].toStream())
            converted_args_ids.append(i)
            converted = True
        else:
            converted_args.append(args[i])
    for k in kwargs:
        if isinstance(kwargs[k], TimeSeriesEnsemble):
            converted_kwargs[k] = kwargs[k].toStream()
            converted_kwargs_keys.append(k)
            converted = True
        else:
            converted_kwargs[k] = kwargs[k]
    res = func(*converted_args, **converted_kwargs)
    if converted:
        for i in converted_args_ids:
            tse = converted_args[i].toTimeSeriesEnsemble()
            timeseries_ensemble_copy_helper(args[i], tse)
        for k in converted_kwargs_keys:
            tse = converted_kwargs[k].toTimeSeriesEnsemble()
            timeseries_ensemble_copy_helper(kwargs[k], tse)
    return res


@decorator
def seismogram_ensemble_as_stream(func, *args, **kwargs):
    """
    This decorator converts all the mspasspy Seismogram ensemble objects in user inputs (*args and **kargs)
    to stream objects (defined in Obspy), and execute the func with the converted user inputs. After the execution,
    the stream objects will be converted back by overriding the data and metadata of each member in the ensemble.

    :param func: target func
    :param args: extra arguments
    :param kwargs: extra kv arguments
    :return: the output of func
    """
    if is_input_dead(*args, **kwargs):
        return
    converted_args = []
    converted_kwargs = {}
    converted_args_ids = []
    converted_kwargs_keys = []
    converted = False
    for i in range(len(args)):
        if isinstance(args[i], SeismogramEnsemble):
            converted_args.append(args[i].toStream())
            converted_args_ids.append(i)
            converted = True
        else:
            converted_args.append(args[i])
    for k in kwargs:
        if isinstance(kwargs[k], SeismogramEnsemble):
            converted_kwargs[k] = kwargs[k].toStream()
            converted_kwargs_keys.append(k)
            converted = True
        else:
            converted_kwargs[k] = kwargs[k]
    res = func(*converted_args, **converted_kwargs)
    if converted:
        for i in converted_args_ids:
            seis_e = converted_args[i].toSeismogramEnsemble()
            seismogram_ensemble_copy_helper(args[i], seis_e)
        for k in converted_kwargs_keys:
            seis_e = converted_kwargs[k].toSeismogramEnsemble()
            seismogram_ensemble_copy_helper(kwargs[k], seis_e)
    return res


@decorator
def mspass_reduce_func_wrapper(
    func,
    data1,
    data2,
    *args,
    object_history=False,
    alg_id=None,
    alg_name=None,
    dryrun=False,
    **kwargs,
):
    """
    This decorator is designed to wrap functions so that they can be used as reduce operator. It takes two inputs, data1
    and data2, both of them are mspasspy objects. The processing history and error logs will recorded in both data1
    and data2. Other functionalities are the same as mspass_func_wrapper.

    :param func: target function
    :param data1: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param data2: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param args: extra arguments
    :param object_history: True to preserve this processing history in the data object, False not to. object_history
     and alg_id are intimately related and control how object level history is handled.
     Object level history is disabled by default for efficiency.  If object_history is set True and the string passed
     as alg_id is defined (not None which is the default) each Seismogram or TimeSeries object will attempt to
     save the history through a new_reduce operation. If the history chain is empty this will silently generate
     an error posted to error log on each object.
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param dryrun: True for dry-run, the algorithm is not run, but the arguments used in this wrapper will be checked.
      This is useful for pre-run checks of a large job to validate a workflow. Errors generate exceptions
      but the function returns before attempting any calculations.
    :param kwargs: extra kv arguments
    :return: the output of func
    """
    if not alg_name:
        alg_name = func.__name__

    if isinstance(
        data1, (TimeSeries, Seismogram, TimeSeriesEnsemble, SeismogramEnsemble)
    ):
        if type(data1) != type(data2):
            raise TypeError("data2 has a different type as data1")
    else:
        raise TypeError("only mspass objects are supported in reduce wrapped methods")

    if object_history and alg_id is None:
        raise ValueError(alg_name + ": object_history was true but alg_id not defined")

    if dryrun:
        return "OK"

    try:
        res = func(data1, data2, *args, **kwargs)
        if object_history:
            logging_helper.reduce(data1, data2, alg_id, alg_name)
        return res
    except RuntimeError as err:
        if isinstance(data1, (Seismogram, TimeSeries)):
            data1.elog.log_error(alg_name, str(err), ErrorSeverity.Invalid)
            data2.elog.log_error(alg_name, str(err), ErrorSeverity.Invalid)
        else:
            logging_helper.ensemble_error(data1, alg_name, err, ErrorSeverity.Invalid)
            logging_helper.ensemble_error(data2, alg_name, err, ErrorSeverity.Invalid)
    except MsPASSError as ex:
        if (
            ex.severity != ErrorSeverity.Informational
            and ex.severity != ErrorSeverity.Debug
        ):
            raise
        if isinstance(data1, (Seismogram, TimeSeries)):
            data1.elog.log_error(alg_name, ex.message, ex.severity)
            data2.elog.log_error(alg_name, ex.message, ex.severity)
        else:
            logging_helper.ensemble_error(data1, alg_name, ex.message, ex.severity)
            logging_helper.ensemble_error(data2, alg_name, ex.message, ex.severity)


# @decorator
# def mspass_func_wrapper_global_history(func, *args, global_history=None, **kwargs):
#     """
#     This decorator helps the global history manager to log down the usage of any mspasspy function if global history manager provided,
#     and then execute the func with the user inputs.

#     :param func: target func
#     :param args: extra arguments
#     :param global_history: global history manager object passed to save the usage of this function to database
#     :type global_history: :class:`mspasspy.global_history.manager.GlobalHistoryManager`
#     :param kwargs: extra kv arguments
#     :return: the output of func
#     """
#     if global_history:
#         if not isinstance(global_history, GlobalHistoryManager):
#             raise TypeError("only an object with the type GlobalHistoryManager should be passes as an argument as global_history.")

#         alg_name = func.__name__
#         # get the whole map algorithm string
#         alg_string = getsource(func)
#         # extract parameters
#         args_str = ",".join(f"{value}" for value in args)
#         kwargs_str = ",".join(f"{key}={value}" for key, value in kwargs.items())
#         parameters = args_str
#         if kwargs_str:
#             parameters += "," + kwargs_str

#         # get the alg_id if exists, else create a new one
#         alg_id = global_history.get_alg_id(alg_name, parameters)
#         if not alg_id:
#             alg_id = ObjectId()
#         global_history.logging(alg_id, alg_name, parameters)

#     return func(*args, **kwargs)
