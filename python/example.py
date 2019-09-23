#!/usr/bin/env python

# Below is an example modified from https://docs.obspy.org/packages/obspy.clients.fdsn.html#module-obspy.clients.fdsn

from obspy.clients.fdsn import Client
client = Client("IRIS")

from obspy import UTCDateTime
t = UTCDateTime("2010-02-27T06:45:00.000")
st = client.get_waveforms("IU", "ANMO", "00", "BH*", t, t + 60 * 60)

#from pyspark.ml.linalg import Vectors
#data = spark.sparkContext.parallelize(st).map(lambda tr: (tr.stats, Vectors.dense(tr.data)))
data = spark.sparkContext.parallelize(st).map(lambda tr: (tr.stats, tr.data.tolist()))
df = data.toDF(["metadata", "waveform"])

from pyspark.sql.functions import monotonically_increasing_id
dfmetadata = df.select("metadata.*").withColumn("tmpid", monotonically_increasing_id())
dfdata = df.select("waveform").withColumn("tmpid", monotonically_increasing_id())
df = dfmetadata.join(dfdata, "tmpid", "outer").drop("tmpid")

df.show()

df.write.format("mongo").mode("append").option("database",
"test").option("collection", "wfdisc").save()

