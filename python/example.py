#!/usr/bin/env python

# Below is an example modified from https://docs.obspy.org/packages/obspy.clients.fdsn.html#module-obspy.clients.fdsn

from obspy.clients.fdsn import Client
client = Client("IRIS")

from obspy import UTCDateTime
t = UTCDateTime("2010-02-27T06:45:00.000")

st = client.get_waveforms("IU", "ANMO", "00", "BH*", t, t + 60 * 60)
data = spark.sparkContext.parallelize(st)
metadata = data.map(lambda tr: tr.stats)
dfMeta = metadata.toDF()
dfMeta.write.format("mongo").mode("append").option("database",
"test").option("collection", "wfdisc").save()

