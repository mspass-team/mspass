from mspasspy.util.decorators import mspass_reduce_func_wrapper
from mspasspy.ccore.utility import MsPASSError
from mspasspy.ccore.seismic import Seismogram, TimeSeries, TimeSeriesEnsemble, SeismogramEnsemble


@mspass_reduce_func_wrapper
def stack(data1, data2, preserve_history=False, instance=None, dryrun=False):
    """
    :param data1:
    :param data2:
    :param preserve_history:
    :param instance:
    :param dryrun:
    :return:
    """
    if isinstance(data1, (TimeSeries,Seismogram)):
        data1 += data2
    elif isinstance(data1, (TimeSeriesEnsemble, SeismogramEnsemble)):
        if len(data1.member) != len(data2.member):
            raise IndexError("data1 and data2 have different sizes of member")
        for i in range(len(data1.member)):
            data1.member[i] += data2.member[i]
    return data1
