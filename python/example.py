#!/usr/bin/env python

# Below is an example modified from https://docs.obspy.org/packages/obspy.clients.fdsn.html#module-obspy.clients.fdsn

from obspy.clients.fdsn import Client
client = Client("IRIS")

from obspy import UTCDateTime
t = UTCDateTime("2010-02-27T06:45:00.000")
st = client.get_waveforms("IU", "ANMO", "00", "BH*", t, t + 60 * 60)

# Convert the downloaded ensemble to an RDD
data = spark.sparkContext.parallelize(st).map(lambda tr: (tr.stats, tr.data.tolist()))

# Convert RDD to dataframe with 2 columns
df = data.toDF(["metadata", "waveform"])

# Flatten the metadata struct and add a tmpid for later join
from pyspark.sql.functions import monotonically_increasing_id
dfmetadata = df.select("metadata.*").withColumn("tmpid", monotonically_increasing_id())
dfdata = df.select("waveform").withColumn("tmpid", monotonically_increasing_id())

# Save the waveform to gridfs and replace the column with the record's id
from pymongo import MongoClient
import gridfs
import pickle
dfdata = dfdata.rdd.map(lambda x: ([str(gridfs.GridFS(MongoClient('mongodb://mspass-master:27017/').gridfs_wf).put(pickle.dumps(x["waveform"])))], x["tmpid"])).cache().toDF(["wfid", "tmpid"])

# Join metadata with the wfid of saved data
df = dfmetadata.join(dfdata, "tmpid", "outer").drop("tmpid")

df.show()

df.write.format("mongo").mode("append").option("database","test").option("collection", "wfdisc").save()

