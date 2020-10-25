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
from mspasspy.util import logging_helper


@decorator
def mspass_func_wrapper(func, data, *args, preserve_history=False, instance=None, dryrun=False,
                        inplace_return=False, **kwargs):
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
        if res is None and inplace_return:
            return data
        return res
    except RuntimeError as err:
        if isinstance(data, (Seismogram, TimeSeries)):
            data.elog.log_error(algname, str(err), mspass.ErrorSeverity.Invalid)
        else:
            logging_helper.ensemble_error(data, algname, err, mspass.ErrorSeverity.Invalid)
    except MsPASSError as ex:
        if ex.severity == mspass.ErrorSeverity.Fatal:
            raise
        if isinstance(data, (Seismogram, TimeSeries)):
            data.elog.log_error(algname, ex.message, ex.severity)
        else:
            logging_helper.ensemble_error(data, algname, ex.message, ex.severity)


@decorator
def mspass_func_wrapper_multi(func, data1, data2, *args, preserve_history=False, instance=None, dryrun=False, **kwargs):
    if not isinstance(data1, (Seismogram, TimeSeries, SeismogramEnsemble, TimeSeriesEnsemble)):
        raise RuntimeError("mspass_func_wrapper only accepts mspass object as data input")

    if not isinstance(data2, (Seismogram, TimeSeries, SeismogramEnsemble, TimeSeriesEnsemble)):
        raise RuntimeError("mspass_func_wrapper only accepts mspass object as data input")

    algname = func.__name__
    try:
        if preserve_history and instance is None:
            raise RuntimeError(algname + ": preserve_history was true but instance not defined")
        if dryrun:
            return "OK"
        res = func(data1, data2, *args, **kwargs)
        if preserve_history:
            logging_helper.info(data1, algname, instance)
            logging_helper.info(data2, algname, instance)
        return res
    except RuntimeError as err:
        if isinstance(data1, (Seismogram, TimeSeries)):
            data1.elog.log_error(algname, str(err), mspass.ErrorSeverity.Invalid)
        else:
            logging_helper.ensemble_error(data1, algname, err, mspass.ErrorSeverity.Invalid)
        if isinstance(data2, (Seismogram, TimeSeries)):
            data2.elog.log_error(algname, str(err), mspass.ErrorSeverity.Invalid)
        else:
            logging_helper.ensemble_error(data2, algname, err, mspass.ErrorSeverity.Invalid)
    except MsPASSError as ex:
        if ex.severity == mspass.ErrorSeverity.Fatal:
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
        # metadata copy
        for k in ts.keys():
            args[i][k] = ts[k]
    for k in converted_kwargs_keys:
        ts = Trace2TimeSeries(converted_kwargs[k])
        kwargs[k].s = ts.s
        # metadata copy
        for key in ts.keys():
            kwargs[k][key] = ts[key]
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
            # metadata copy
            for k in seis.keys():
                args[i][k] = seis[k]
        for k in converted_kwargs_keys:
            seis = Stream2Seismogram(converted_kwargs[k], cardinal=True)
            kwargs[k].u = seis.u
            # metadata copy
            for key in seis.keys():
                kwargs[k][key] = seis[key]
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
            # for k in tse.keys():
            #     args[i][k] = tse[k]
        for k in converted_kwargs_keys:
            tse = converted_kwargs[k].toTimeSeriesEnsemble()
            kwargs[k].member = tse.member
            # for key in tse.keys():
            #     kwargs[k][key] = tse[key]
    return res


@decorator
def seismogram_ensemble_as_stream(func, *args, **kwargs):
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
            # for k in seis_e.keys():
            #     args[i][k] = seis_e[k]
        for k in converted_kwargs_keys:
            seis_e = converted_kwargs[k].toSeismogramEnsemble()
            kwargs[k].member = seis_e.member
            # for key in seis_e.keys():
            #     kwargs[k][key] = seis_e[key]
    return res


# wrapper is just an example, if a user wants some specific function, they can refer to the implementation here.
@decorator
def mspass_reduce_func_wrapper(func, data1, data2, *args, preserve_history=False, instance=None, dryrun=False,
                               **kwargs):
    algname = func.__name__
    if dryrun:
        return "OK"
    try:
        if isinstance(data1, (TimeSeries, Seismogram, TimeSeriesEnsemble, SeismogramEnsemble)):
            if type(data1) != type(data2):
                raise TypeError("data2 has a different type as data1")
        else:
            raise TypeError("only mspass objects are supported in reduce wrapped methods")
        res = func(data1, data2, *args, **kwargs)
        if preserve_history:
            logging_helper.reduce(data1, data2, algname, instance)
        return res
    except RuntimeError as err:
        if isinstance(data1, (Seismogram, TimeSeries)):
            data1.elog.log_error(algname, str(err), mspass.ErrorSeverity.Invalid)
            data2.elog.log_error(algname, str(err), mspass.ErrorSeverity.Invalid)
        else:
            logging_helper.ensemble_error(data1, algname, err, mspass.ErrorSeverity.Invalid)
            logging_helper.ensemble_error(data2, algname, err, mspass.ErrorSeverity.Invalid)
    except MsPASSError as ex:
        if ex.severity != mspass.ErrorSeverity.Informational or \
                ex.severity != mspass.ErrorSeverity.Debug:
            raise
        if isinstance(data1, (Seismogram, TimeSeries)):
            data1.elog.log_error(algname, ex.message, ex.severity)
            data2.elog.log_error(algname, ex.message, ex.severity)
        else:
            logging_helper.ensemble_error(data1, algname, ex.message, ex.severity)
            logging_helper.ensemble_error(data2, algname, ex.message, ex.severity)
