try:
    import dask.bag as daskbag
except ImportError:
    pass
try:
    import pyspark
except ImportError:
    pass
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
    """
    This function implements a convenient foldby method for a spark RDD to generate ensembles from atomic data.  
    The concept is to assemble ensembles of :class:`mspasspy.ccore.seismic.TimeSeries` or 
    :class:`mspasspy.ccore.seismic.Seismogram` objects with a common Metadata attribute using 
    a single key.  The output is the ensemble objects we call class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` 
    and :class:`mspasspy.ccore.seismic.SeismogramEnsemble` respectively.  Note that "foldby" in this context
    acts a bit like a reduce operator BUT the data volume is not reduced;  we just bundle groups of 
    related data into ensembles.   The outputs are always larger due to the overhead of the ensemble 
    container.   That is important as be careful with this operator as it can easily create huge
    ensembles that could cause a memory fault in your workflow.  
    
    Note that because this is implemented as a method of the RDD class the usage is a different from 
    the map and reduce methods.  arg0 is "self" which means it must be defined by the input RDD.  
    
    Example::
    
      # preceded by set of map-reduce lines to create RDD mydata
      mydata = mspass_spark_foldby(mydata,key="source_id")

    :param key: The key that defines the gather. By default, it will use "site_id" to produce a common station gather.
    :return: :class:`pyspark.RDD` of :class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` or :class:`mspasspy.ccore.seismic.SeismogramEnsemble`.
    """
    return (
        self.map(lambda x: (x[key], x))
        .foldByKey(
            [],
            lambda x, y: (x if isinstance(x, list) else [x])
            + (y if isinstance(y, list) else [y]),
        )
        .map(lambda x: list2Ensemble(x[1]))
    )


def mspass_dask_foldby(self, key="site_id"):
    """
    This function implements a convenient foldby method for a dask bag to generate ensembles from atomic data.  
    The concept is to assemble ensembles of :class:`mspasspy.ccore.seismic.TimeSeries` or 
    :class:`mspasspy.ccore.seismic.Seismogram` objects with a common Metadata attribute using 
    a single key.  The output is the ensemble objects we call class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` 
    and :class:`mspasspy.ccore.seismic.SeismogramEnsemble` respectively.  Note that "foldby" in this context
    acts a bit like a reduce operator BUT the data volume is not reduced;  we just bundle groups of 
    related data into ensembles.   The outputs are always larger due to the overhead of the ensemble 
    container.   That is important as be careful with this operator as it can easily create huge
    ensembles that could cause a memory fault in your workflow.  
    
    Note that because this is implemented as a method of the bag class the usage is a different from 
    the map and reduce methods.  arg0 is "self" which means it must be defined by the input bag.  
    
    Example::
    
      # preceded by set of map-reduce lines to create a bag called mydata
      mydata = mspass_dask_foldby(mydata,key="source_id")

    :param key: The key that defines the gather. By default, it will use "site_id" to produce a common station gather.
    :return: :class:`dask.bag.Bag` of :class:`mspasspy.ccore.seismic.TimeSeriesEnsemble` or :class:`mspasspy.ccore.seismic.SeismogramEnsemble`.
    """
    return self.foldby(
        lambda x: x[key],
        lambda x, y: (x if isinstance(x, list) else [x])
        + (y if isinstance(y, list) else [y]),
    ).map(lambda x: list2Ensemble(x[1]))


try:
    daskbag.Bag.mspass_foldby = mspass_dask_foldby
except NameError:
    pass
try:
    pyspark.RDD.mspass_foldby = mspass_spark_foldby
except NameError:
    pass
