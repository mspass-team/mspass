hasDask = True
hasSpark = True
try:
    import dask
except ImportError as e:
    hasDask = False
try:
    import pyspark
except ImportError as e:
    hasSpark = False
