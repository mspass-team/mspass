#!/usr/bin/env python

# Thie function saves the serialized waveform in the first column of a row to GridFS and replaces it with the id of that record
def save2gridfs(row):
  from pymongo import MongoClient
  import gridfs
  import pickle
  gfs_handle = gridfs.GridFS(MongoClient(mongoUrl).gridfs_wf)
  file_id = gfs_handle.put(pickle.dumps(row["waveform"]))
  return str(file_id), row["tmpid"]

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
mongoUrl = spark.conf.get("spark.mongodb.output.uri").rsplit('/',1)[0]
dfdata = dfdata.rdd.map(save2gridfs).cache().toDF(["wfid", "tmpid"])

# Join metadata with the wfid of saved data
df = dfmetadata.join(dfdata, "tmpid", "outer").drop("tmpid")

df.show()

df.write.format("mongo").mode("append").option("database","test").option("collection", "wfdisc").save()

