from decorator import decorator
import numpy as np

from obspy.core.stream import Stream
from obspy.core.trace import Trace, Stats

from mspasspy.io.converter import (TimeSeries2Trace,
                                   Seismogram2Stream,
                                   TimeSeriesEnsemble2Stream,
                                   SeismogramEnsemble2Stream,
                                   Stream2Seismogram,
                                   Trace2TimeSeries,
                                   Stream2TimeSeriesEnsemble,
                                   Stream2SeismogramEnsemble)

from mspasspy.ccore.utility import MsPASSError, ErrorSeverity
from mspasspy.ccore.seismic import Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble, TimeReferenceType
from mspasspy.util import logging_helper


@decorator
def mspass_func_wrapper(func, data, *args, preserve_history=False, instance=None, dryrun=False,
                        inplace_return=False, **kwargs):
    """
    This function serves as a decorator wrapper, which is widely used in mspasspy library. It executes the target
    function on input data. Data are restricted to be mspasspy objects. It also preserves the processing history and
    error logs into the mspasspy objects. By wrapping your function using this decorator, you can save some workload.
    Runtime error won't be raised in order to be efficient in map-reduce operations. MspassError with a severity Fatal
    will be raised, others won't be raised.

    :param func: target function
    :param data: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param args: extra arguments
    :param preserve_history: True to preserve this processing history in the data object, False not to. preserve_history
     and instance are intimately related and control how object level history is handled.
     Object level history is disabled by default for efficiency.  If preserve_history is set True and the string passed
     as instance is defined (not None which is the default) each Seismogram or TimeSeries object will attempt to
     save the history through a new_map operation.   If the history chain is empty this will silently generate
     an error posted to error log on each object.
    :param instance: instance is a unique id to record the usage of func while preserving the history.
    :type instance: str
    :param dryrun: True for dry-run, the algorithm is not run, but the arguments used in this wrapper will be checked.
      This is useful for pre-run checks of a large job to validate a workflow. Errors generate exceptions
      but the function returns before attempting any calculations.
    :param inplace_return: when func is an in-place function that doesn't return anything, but you want to
     return the origin data (for example, in map-reduce), set inplace_return as true.
    :param kwargs: extra kv arguments
    :return: origin data or the output of func
    """
    if not isinstance(data, (Seismogram, TimeSeries, SeismogramEnsemble, TimeSeriesEnsemble)):
        raise TypeError("mspass_func_wrapper only accepts mspass object as data input")

    algname = func.__name__

    if preserve_history and instance is None:
        raise ValueError(algname + ": preserve_history was true but instance not defined")
    if dryrun:
        return "OK"

    try:
        res = func(data, *args, **kwargs)
        if preserve_history:
            logging_helper.info(data, algname, instance)
        if res is None and inplace_return:
            return data
        return res
    except RuntimeError as err:
        if isinstance(data, (Seismogram, TimeSeries)):
            data.elog.log_error(algname, str(err), ErrorSeverity.Invalid)
        else:
            logging_helper.ensemble_error(data, algname, err, ErrorSeverity.Invalid)
    except MsPASSError as ex:
        if ex.severity == ErrorSeverity.Fatal:
            raise
        if isinstance(data, (Seismogram, TimeSeries)):
            data.elog.log_error(algname, ex.message, ex.severity)
        else:
            logging_helper.ensemble_error(data, algname, ex.message, ex.severity)


@decorator
def mspass_func_wrapper_multi(func, data1, data2, *args, preserve_history=False, instance=None, dryrun=False, **kwargs):
    """
    This wrapper serves the same functionality as mspass_func_wrapper, but there are a few differences. The first is
    this wrapper accepts two mspasspy data objects as input data. The second is that inplace_return is not implemented
    here. The same processing history and error logs will be duplicated and stored in both of the input.

    :param func: target function
    :param data1: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param data2: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param args: extra arguments
    :param preserve_history: True to preserve this processing history in the data object, False not to. preserve_history
     and instance are intimately related and control how object level history is handled.
     Object level history is disabled by default for efficiency.  If preserve_history is set True and the string passed
     as instance is defined (not None which is the default) each Seismogram or TimeSeries object will attempt to
     save the history through a new_map operation.   If the history chain is empty this will silently generate
     an error posted to error log on each object.
    :param instance: instance is a unique id to record the usage of func while preserving the history.
    :type instance: str
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

    algname = func.__name__

    if preserve_history and instance is None:
        raise ValueError(algname + ": preserve_history was true but instance not defined")
    if dryrun:
        return "OK"

    try:
        res = func(data1, data2, *args, **kwargs)
        if preserve_history:
            logging_helper.info(data1, algname, instance)
            logging_helper.info(data2, algname, instance)
        return res
    except RuntimeError as err:
        if isinstance(data1, (Seismogram, TimeSeries)):
            data1.elog.log_error(algname, str(err), ErrorSeverity.Invalid)
        else:
            logging_helper.ensemble_error(data1, algname, err, ErrorSeverity.Invalid)
        if isinstance(data2, (Seismogram, TimeSeries)):
            data2.elog.log_error(algname, str(err), ErrorSeverity.Invalid)
        else:
            logging_helper.ensemble_error(data2, algname, err, ErrorSeverity.Invalid)
    except MsPASSError as ex:
        if ex.severity == ErrorSeverity.Fatal:
            raise
        if isinstance(data1, (Seismogram, TimeSeries)):
            data1.elog.log_error(algname, ex.message, ex.severity)
        else:
            logging_helper.ensemble_error(data1, algname, ex.message, ex.severity)
        if isinstance(data2, (Seismogram, TimeSeries)):
            data2.elog.log_error(algname, ex.message, ex.severity)
        else:
            logging_helper.ensemble_error(data2, algname, ex.message, ex.severity)


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
            args[i].member = tse.member
        for k in converted_kwargs_keys:
            tse = converted_kwargs[k].toTimeSeriesEnsemble()
            kwargs[k].member = tse.member
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
            args[i].member = seis_e.member
        for k in converted_kwargs_keys:
            seis_e = converted_kwargs[k].toSeismogramEnsemble()
            kwargs[k].member = seis_e.member
    return res


@decorator
def mspass_reduce_func_wrapper(func, data1, data2, *args, preserve_history=False, instance=None, dryrun=False,
                               **kwargs):
    """
    This decorator is designed to wrap functions so that they can be used as reduce operator. It takes two inputs, data1
    and data2, both of them are mspasspy objects. The processing history and error logs will recorded in both data1
    and data2. Other functionalities are the same as mspass_func_wrapper.

    :param func: target function
    :param data1: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param data2: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param args: extra arguments
    :param preserve_history: True to preserve this processing history in the data object, False not to. preserve_history
     and instance are intimately related and control how object level history is handled.
     Object level history is disabled by default for efficiency.  If preserve_history is set True and the string passed
     as instance is defined (not None which is the default) each Seismogram or TimeSeries object will attempt to
     save the history through a new_reduce operation. If the history chain is empty this will silently generate
     an error posted to error log on each object.
    :param instance: instance is a unique id to record the usage of func while preserving the history.
    :type instance: str
    :param dryrun: True for dry-run, the algorithm is not run, but the arguments used in this wrapper will be checked.
      This is useful for pre-run checks of a large job to validate a workflow. Errors generate exceptions
      but the function returns before attempting any calculations.
    :param kwargs: extra kv arguments
    :return: the output of func
    """
    algname = func.__name__

    if isinstance(data1, (TimeSeries, Seismogram, TimeSeriesEnsemble, SeismogramEnsemble)):
        if type(data1) != type(data2):
            raise TypeError("data2 has a different type as data1")
    else:
        raise TypeError("only mspass objects are supported in reduce wrapped methods")

    if preserve_history and instance is None:
        raise ValueError(algname + ": preserve_history was true but instance not defined")

    if dryrun:
        return "OK"

    try:
        res = func(data1, data2, *args, **kwargs)
        if preserve_history:
            logging_helper.reduce(data1, data2, algname, instance)
        return res
    except RuntimeError as err:
        if isinstance(data1, (Seismogram, TimeSeries)):
            data1.elog.log_error(algname, str(err), ErrorSeverity.Invalid)
            data2.elog.log_error(algname, str(err), ErrorSeverity.Invalid)
        else:
            logging_helper.ensemble_error(data1, algname, err, ErrorSeverity.Invalid)
            logging_helper.ensemble_error(data2, algname, err, ErrorSeverity.Invalid)
    except MsPASSError as ex:
        if ex.severity != ErrorSeverity.Informational and \
                ex.severity != ErrorSeverity.Debug:
            raise
        if isinstance(data1, (Seismogram, TimeSeries)):
            data1.elog.log_error(algname, ex.message, ex.severity)
            data2.elog.log_error(algname, ex.message, ex.severity)
        else:
            logging_helper.ensemble_error(data1, algname, ex.message, ex.severity)
            logging_helper.ensemble_error(data2, algname, ex.message, ex.severity)
