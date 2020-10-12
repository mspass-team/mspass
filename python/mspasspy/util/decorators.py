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

from mspasspy.ccore import Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble, MsPASSError
import mspasspy.ccore as mspass
import mspasspy.util.logging_helper as logging_helper

@decorator
def mspass_func_wrapper_multi(func, data1, data2, *args, preserve_history=False, instance=None, dryrun=False, **kwargs):
    pass

@decorator
def mspass_func_wrapper(func, data, *args, preserve_history=False, instance=None, dryrun=False, **kwargs):

    if not isinstance(data, (Seismogram, TimeSeries, SeismogramEnsemble, TimeSeriesEnsemble)):
        raise RuntimeError("mspass_func_wrapper only accepts mspass object as data input")

    algname = func.__name__
    try:
        if preserve_history and instance is None:
            raise RuntimeError(algname + ": preserve_history was true but instance not defined")
        if dryrun:
            return "OK"
        res = func(data, *args, **kwargs)
        if preserve_history:
            logging_helper.info(data, algname, instance)
        return res
    except RuntimeError as err:
        if isinstance(data, (Seismogram, TimeSeries)):
            data.elog.log_error(algname, str(err), mspass.ErrorSeverity.Invalid)
        else:
            logging_helper.ensemble_error(data, algname, err, mspass.ErrorSeverity.Invalid)
    except MsPASSError as ex:
        message = "MsPass Error, severity: " + ex.severity + ", message: " + ex.message + \
            "\nIf it is a bug that needs to be fixed, please contact authors"
        if isinstance(data, (Seismogram,TimeSeries)):
            data.elog.log_error(algname, message, mspass.ErrorSeverity.Invalid)
        else:
            logging_helper.ensemble_error(data, algname, message, mspass.ErrorSeverity.Invalid)


def is_input_dead(*args, **kwargs):
    """
    A helper method to see if any mspass objects in the input parameters are dead. If one is dead,
    we should keep silent, i.e. no longer perform any further operations on this dead mspass object.
    Note for an ensemble object, only if all the objects of it are dead, we see them as dead,
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


@decorator
def timeseries_as_trace(func, *args, **kwargs):
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
        args[i].s = ts.s
        # metadata
        # refer to copy constructor
    for k in converted_kwargs_keys:
        ts = Trace2TimeSeries(converted_kwargs[k])
        kwargs[k].s = ts.s
    return res


@decorator
def seismogram_as_stream(func, *args, **kwargs):
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
            args[i].u = seis.u
        for k in converted_kwargs_keys:
            seis = Stream2Seismogram(converted_kwargs[k], cardinal=True)
            kwargs[k].u = seis.u
    return res


@decorator
def timeseries_ensemble_as_stream(func, *args, **kwargs):
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
    if is_input_dead(*args, **kwargs):
        return
    converted_args = []
    converted_kwargs= {}
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
