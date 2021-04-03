from mspasspy.util.decorators import mspass_reduce_func_wrapper
from mspasspy.ccore.seismic import Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble


@mspass_reduce_func_wrapper
def stack(data1, data2, preserve_history=False, alg_id=None, alg_name=None, dryrun=False):
    """
    This function sums the data field of two mspasspy objects, the result will be stored in data1.
    Note it is wrapped by mspass_reduce_func_wrapper, so the history and error logs can be preserved.

    :param data1: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param data2: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param preserve_history: True to preserve the history. For details, refer to
     :class:`~mspasspy.util.decorators.mspass_reduce_func_wrapper`.
    :param alg_id: alg_id is a unique id to record the usage of this function while preserving the history.
     Used in the mspass_reduce_func_wrapper.
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_reduce_func_wrapper.
    :return: data1 (modified).
    """
    if isinstance(data1, (TimeSeries,Seismogram)):
        data1 += data2
    elif isinstance(data1, (TimeSeriesEnsemble, SeismogramEnsemble)):
        if len(data1.member) != len(data2.member):
            raise IndexError("data1 and data2 have different sizes of member")
        for i in range(len(data1.member)):
            data1.member[i] += data2.member[i]
    return data1
