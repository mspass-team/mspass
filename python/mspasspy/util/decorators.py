from decorator import decorator

from mspasspy.util.converter import (Stream2Seismogram,
                                     Trace2TimeSeries)

from mspasspy.ccore.utility import MsPASSError, ErrorSeverity
from mspasspy.ccore.seismic import Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble
# from mspasspy.global_history.manager import GlobalHistoryManager
from mspasspy.util import logging_helper
from dill.source import getsource
from bson.objectid import ObjectId


@decorator
def mspass_func_wrapper(func, data, *args, object_history=False, alg_id=None, alg_name=None, dryrun=False,
                        inplace_return=False, function_return_key=None, **kwargs):
    """
    Decorator wrapper to adapt a simple function to the mspass parallel processing framework.

    This function serves as a decorator wrapper, which is widely used in mspasspy library. It executes the target
    function on input data. Data are restricted to be mspasspy objects. It also preserves the processing history and
    error logs into the mspasspy objects. By wrapping your function using this decorator, you can save some workload.
    Runtime error won't be raised in order to be efficient in map-reduce operations. MspassError with a severity Fatal
    will be raised, others won't be raised.

    :param func: target function
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
    :param kwargs: extra kv arguments
    :return: origin data or the output of func
    """
    if not isinstance(data, (Seismogram, TimeSeries, SeismogramEnsemble, TimeSeriesEnsemble)):
        raise TypeError("mspass_func_wrapper only accepts mspass object as data input")

    # if not defined
    if not alg_name:
        alg_name = func.__name__

    if object_history and alg_id is None:
        raise ValueError(alg_name + ": object_history was true but alg_id not defined")
    if dryrun:
        return "OK"

    try:
        res = func(data, *args, **kwargs)
        if object_history:
            logging_helper.info(data, alg_id, alg_name)
        if function_return_key is not None:
            if isinstance(function_return_key,str):
                data[function_return_key]=res
            else:
                data.elog.log_error(alg_name,
                 "Illegal type received for function_return_key argument="+str(type(function_return_key)+"\nReturn value not saved in Metadata"),
                 ErrorSeverity.Complaint)
            if not inplace_return:
                data.elog.log_error(alg_name,
                  "Inconsistent arguments; inplace_return was set False and function_return_key was not None.\nAssuming inplace_return == True is correct",
                  ErrorSeverity.Complaint)
            return data
        elif inplace_return:
            return data
        else:
            return res
    except RuntimeError as err:
        if isinstance(data, (Seismogram, TimeSeries)):
            data.elog.log_error(alg_name, str(err), ErrorSeverity.Invalid)
        else:
            logging_helper.ensemble_error(data, alg_name, err, ErrorSeverity.Invalid)
    except MsPASSError as ex:
        if ex.severity == ErrorSeverity.Fatal:
            raise
        if isinstance(data, (Seismogram, TimeSeries)):
            data.elog.log_error(alg_name, ex.message, ex.severity)
        else:
            logging_helper.ensemble_error(data, alg_name, ex.message, ex.severity)


@decorator
def mspass_func_wrapper_multi(func, data1, data2, *args, object_history=False, alg_id=None, alg_name=None, dryrun=False, **kwargs):
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
    if not isinstance(data1, (Seismogram, TimeSeries, SeismogramEnsemble, TimeSeriesEnsemble)):
        raise TypeError("mspass_func_wrapper_multi only accepts mspass object as data input")

    if not isinstance(data2, (Seismogram, TimeSeries, SeismogramEnsemble, TimeSeriesEnsemble)):
        raise TypeError("mspass_func_wrapper_multi only accepts mspass object as data input")

    if not alg_name:
        alg_name = func.__name__

    if object_history and alg_id is None:
        raise ValueError(alg_name + ": object_history was true but alg_id not defined")
    if dryrun:
        return "OK"

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
    ts1.npts = ts2.npts
    ts1.dt = ts2.dt
    ts1.tref = ts2.tref
    ts1.live = ts2.live
    ts1.t0 = ts2.t0
    for k in ts2.keys():  # other metadata copy
        ts1[k] = ts2[k]  # override previous metadata is ok, since they are consistent
    ts1.data = ts2.data


def timeseries_ensemble_copy_helper(es1, es2):
    for i in range(len(es1.member)):
        timeseries_copy_helper(es1.member[i], es2.member[i])
    # fixme: not sure in what algorithm the length of es1 and es2 would be different
    # also, the following does not address uninitiated history issues in the extended elements
    # same problem applies to the seismogram_ensemble_copy_helper
    if len(es2.member) > len(es1.member):
        es1.member.extend(es2.member[len(es1.member):])


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
    seis1.npts = seis2.npts
    seis1.dt = seis2.dt
    seis1.tref = seis2.tref
    seis1.live = seis2.live
    seis1.t0 = seis2.t0
    for k in seis2.keys():  # other metadata copy
        seis1[k] = seis2[k]  # override previous metadata is ok, since they are consistent
    seis1.data = seis2.data


def seismogram_ensemble_copy_helper(es1, es2):
    for i in range(len(es1.member)):
        seismogram_copy_helper(es1.member[i], es2.member[i])
    if len(es2.member) > len(es1.member):
        es1.member.extend(es2.member[len(es1.member):])


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
def mspass_reduce_func_wrapper(func, data1, data2, *args, object_history=False, alg_id=None, alg_name=None, dryrun=False, **kwargs):
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

    if isinstance(data1, (TimeSeries, Seismogram, TimeSeriesEnsemble, SeismogramEnsemble)):
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
        if ex.severity != ErrorSeverity.Informational and \
                ex.severity != ErrorSeverity.Debug:
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
