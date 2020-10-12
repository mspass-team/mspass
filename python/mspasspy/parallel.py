import dask
import dask.dataframe as dd
from pyspark.sql import SparkSession
import pandas as pd

from mspasspy.algorithms.signals import filter
from mspasspy.ccore import Seismogram

def test():
    seis1 = Seismogram()
    seis2 = Seismogram()
    df = pd.DataFrame([{'a': seis1}, {'a':seis2}])
    ddf = dd.from_pandas(df, npartitions=2)
    # res = ddf.map_partitions(filter, "bandpass")

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    df2 = spark.createDataFrame(df)
    df2.show()
    # TypeError: not supported type: <class 'mspasspy.ccore.Seismogram'>

def mspass_reduce(object, **options):
    # call function
    # matin
    pass


if __name__ == "__main__":
    test()