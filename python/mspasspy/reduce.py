
import pyspark
import dask.bag as daskbag

from mspasspy.util.decorators import mspass_reduce_func_wrapper
from mspasspy.util.converter import list2Ensemble
from mspasspy.ccore.seismic import (
    Seismogram,
    TimeSeries,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
)


@mspass_reduce_func_wrapper
def stack(data1, data2, object_history=False, alg_id=None, alg_name=None, dryrun=False):
    """
    This function sums the data field of two mspasspy objects, the result will be stored in data1.
    Note it is wrapped by mspass_reduce_func_wrapper, so the history and error logs can be preserved.

    :param data1: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param data2: input data, only mspasspy data objects are accepted, i.e. TimeSeries, Seismogram, Ensemble.
    :param object_history: True to preserve the history. For details, refer to
     :class:`~mspasspy.util.decorators.mspass_reduce_func_wrapper`.
    :param alg_id: alg_id is a unique id to record the usage of this function while preserving the history.
     Used in the mspass_reduce_func_wrapper.
    :param alg_name: alg_name is the name of the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_reduce_func_wrapper.
    :return: data1 (modified).
    """
    if isinstance(data1, (TimeSeries, Seismogram)):
        data1 += data2
    elif isinstance(data1, (TimeSeriesEnsemble, SeismogramEnsemble)):
        if len(data1.member) != len(data2.member):
            raise IndexError("data1 and data2 have different sizes of member")
        for i in range(len(data1.member)):
            data1.member[i] += data2.member[i]
    return data1


def mspass_spark_foldby(self, key="site_id"):
    return self.map(lambda x: (x[key], x)).foldByKey([],
                      lambda x, y: (x if isinstance(x, list) else [x]) + 
                                   (y if isinstance(y, list) else [y]),
                     
                     ).map(lambda x: list2Ensemble(x[1]))


def mspass_dask_foldby(self, key="site_id"):
    return self.foldby(lambda x: x[key], 
                      lambda x, y: (x if isinstance(x, list) else [x]) + 
                                   (y if isinstance(y, list) else [y])
                     ).map(lambda x: list2Ensemble(x[1]))


pyspark.RDD.mspass_foldby = mspass_spark_foldby
daskbag.Bag.mspass_foldby = mspass_dask_foldby
