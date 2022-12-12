from mspasspy.ccore.seismic import TimeSeries, TimeReferenceType
from mspasspy.db.ensembles import TimeIntervalReader
from mspasspy.db.client import DBClient
from mspasspy.db.database import Database
import numpy as np


def make_one_ts(starttime,endtime,net="XX",sta="AAK",chan="BHZ",loc="00",fill_value=1.0):
    # use dt of 1 to make everythign simpler
    npts=int(np.round(endtime-starttime))
    ts = TimeSeries(npts)
    ts.set_live()
    ts.t0 = starttime
    ts.dt = 1.0
    ts.tref = TimeReferenceType.UTC
    ts["net"] = net
    ts["sta"] = sta
    ts["chan"] = chan
    ts["loc"] = loc
    ts["endtime"] = ts.endtime()
    for i in range(npts):
        ts.data[i] = fill_value
    return ts
def test_ensembles():
    dbclient = DBClient("localhost")
    # This is for debugging.  It can be removed in final pytest code
    dbclient.drop_database("test_ensembles")
    # warning this test will fail with the regular schema due to the 
    # readonly issue with net, sta, chan, and loc
    db = Database(dbclient,"test_ensembles",db_schema="mspass_lite.yaml",md_schema="mspass_lite.yaml")
    # We need to save fake metadata in source and channel keeping this ids
    slat=10.0
    slon=20.0
    sdepth=30.0
    stime=10000.0   # meaningless origin time - don't try to use this for travel times
    doc={"lat" : slat,"lon": slon, "depth" : sdepth, "time" : stime}
    source_id = db.source.insert_one(doc).inserted_id

    rlat=25.0
    rlon=0.0
    elev=0.1
    hang=0.0
    vang=0.0 
    

    # group with this sta should merge without a problem when read back
    sta = "GOOD"
    doc = {"lat" : rlat,
           "lon" : rlon,
           "elev" : elev,
           "hang" : hang,
           "vang" : vang,
           "sta" : sta,
           "chan" : "BHZ",
           "net" : "XX",
           "loc" : "00"
           }
    channel_id_z = db.channel.insert_one(doc).inserted_id
    doc["chan"] = "BH1"
    doc.pop("_id")  # An oddity of insert_one is it modifies doc
    channel_id_h = db.channel.insert_one(doc).inserted_id
    doc.pop("_id")  # An oddity of insert_one is it modifies doc
    t0=10000.0
    trange=100.0
    number_segs = 3
    t=t0
    for i in range(number_segs):
        ts = make_one_ts(t, t + trange, sta=sta, chan="BHZ")
        ts["source_id"] = source_id
        ts["channel_id"] = channel_id_z
        db.save_data(ts, collection="wf_TimeSeries")
        ts = make_one_ts(t, t + trange, sta=sta, chan="BH1")
        ts["source_id"] = source_id
        ts["channel_id"] = channel_id_h
        db.save_data(ts, collection="wf_TimeSeries")
        t += trange

    # This group is the same but will have a gap of 10 samples between
    # each TimeSeries saved
    sta = "GAP10"
    t = t0
    doc["rlon"] = rlon + 10.0
    doc["chan"] = "BHZ"
    channel_id_z = db.channel.insert_one(doc).inserted_id
    doc.pop("_id")  # An oddity of insert_one is it modifies doc
    doc["chan"] = "BH1"
    channel_id_h = db.channel.insert_one(doc).inserted_id
    doc.pop("_id")  # An oddity of insert_one is it modifies doc
    for i in range(number_segs):
        ts = make_one_ts(t,t+trange,sta=sta,chan="BHZ")
        ts["source_id"] = source_id
        ts["channel_id"] = channel_id_z
        db.save_data(ts,collection="wf_TimeSeries")
        ts = make_one_ts(t,t+trange,sta=sta,chan="BH1")
        ts["source_id"] = source_id
        ts["channel_id"] = channel_id_h
        db.save_data(ts,collection="wf_TimeSeries")
        t += trange + 10.0

        
    # This group will have overlap of 10 samples
    sta = "Overlap10"
    t=t0
    doc["rlon"] = rlon + 10.0
    doc["chan"] = "BHZ"
    channel_id_z = db.channel.insert_one(doc).inserted_id
    doc.pop("_id")  # An oddity of insert_one is it modifies doc
    doc["chan"] = "BH1"
    channel_id_h = db.channel.insert_one(doc).inserted_id
    doc.pop("_id")  # An oddity of insert_one is it modifies doc
    for i in range(number_segs):
        ts = make_one_ts(t,t+trange,sta=sta,chan="BHZ")
        ts["source_id"] = source_id
        ts["channel_id"] = channel_id_z
        db.save_data(ts,collection="wf_TimeSeries")
        ts = make_one_ts(t,t+trange,sta=sta,chan="BH1")
        ts["source_id"] = source_id
        ts["channel_id"] = channel_id_h
        db.save_data(ts,collection="wf_TimeSeries")
        t += trange - 10.0
        
    # This group will have overlap of 10 samples but conflicting fill
    # merge should treat that as an error and kill earlier time segment
    sta = "Overlap10Bad"
    t=t0
    doc["rlon"] = rlon + 10.0
    doc["chan"] = "BHZ"
    channel_id_z = db.channel.insert_one(doc).inserted_id
    doc.pop("_id")  # An oddity of insert_one is it modifies doc
    doc["chan"] = "BH1"
    channel_id_h = db.channel.insert_one(doc).inserted_id
    doc.pop("_id")  # An oddity of insert_one is it modifies doc
    for i in range(number_segs):
        ts = make_one_ts(t,t+trange,sta=sta,chan="BHZ",fill_value=float(i+1))
        ts["source_id"] = source_id
        ts["channel_id"] = channel_id_z
        db.save_data(ts,collection="wf_TimeSeries")
        ts = make_one_ts(t,t+trange,sta=sta,chan="BH1",fill_value=float(i+1))
        ts["source_id"] = source_id
        ts["channel_id"] = channel_id_h
        db.save_data(ts,collection="wf_TimeSeries")
        t += trange - 10.0
    
    # first test the basic glue with no errors
    starttime = t0+trange - 10.0
    endtime = t0 + trange +10.0
    base_query = {"sta" : "GOOD"}
    enslist = TimeIntervalReader(db,starttime,endtime,
                           collection="wf_TimeSeries",
                           base_query=base_query)
    assert len(enslist) == 2
    for i in range(2):
        assert enslist[i].live()
    assert enslist[0]["chan"] == "BH1"
    assert enslist[1]["chan"] == "BHZ"
    assert len(enslist[0].member) == 1
    assert len(enslist[1].member) == 1
    
    # test default gap handling - kill data with gaps 
    base_query={"sta" : "GAP10"}
    enslist = TimeIntervalReader(db,starttime,endtime,
                           collection="wf_TimeSeries",
                           base_query=base_query)
    assert len(enslist) == 2
    # returns should both be marked dead here
    for i in range(2):
        assert enslist[i].dead()
    assert enslist[0]["chan"] == "BH1"
    assert enslist[1]["chan"] == "BHZ"
    
    # repeat with zero gaps set - that should ensembles marked valid
    enslist = TimeIntervalReader(db,starttime,endtime,
                           collection="wf_TimeSeries",
                           base_query=base_query,
                           zero_gaps=True)
    assert len(enslist) == 2
    for i in range(2):
        assert enslist[i].live()
        assert len(enslist[i].member) == 1
        assert enslist[i].member[0]["has_gaps"]
        gapdata = enslist[i].member[0]["gaps"]
        # These maybe should use isclose
        # gapdata is a list of dict containers definign gaps - only one here
        assert gapdata[0]["starttime"] == 10099.0
        assert gapdata[0]["endtime"] == 10110.0
    assert enslist[0]["chan"] == "BH1"
    assert enslist[1]["chan"] == "BHZ"
    
    # test overlap repair option alone 
    base_query = {"sta" : "Overlap10"}
    enslist = TimeIntervalReader(db,starttime,endtime,
                           collection="wf_TimeSeries",
                           fix_overlaps=True,
                           base_query=base_query)
    assert len(enslist) == 2
    for i in range(2):
        assert enslist[i].live()
    assert enslist[0]["chan"] == "BH1"
    assert enslist[1]["chan"] == "BHZ"
    assert len(enslist[0].member) == 1
    
    # this will work because the time interval matches start time of 
    # overlapping section
    base_query = {"sta" : "Overlap10Bad"}
    enslist = TimeIntervalReader(db,starttime,endtime,
                           collection="wf_TimeSeries",
                           fix_overlaps=True,
                           base_query=base_query)
    assert len(enslist) == 2
    for i in range(2):
        assert enslist[i].live()
    assert enslist[0]["chan"] == "BH1"
    assert enslist[1]["chan"] == "BHZ"
    assert len(enslist[0].member) == 1
    
     
    # Repeat with longer time interval to cover two overlaps 
    # This wil have all output null
    starttime -= 20.0
    endtime = t0 + 2.0*trange +30.0
    enslist = TimeIntervalReader(db,starttime,endtime,
                           collection="wf_TimeSeries",
                           fix_overlaps=True,
                           base_query=base_query)
    assert len(enslist) == 2
    for i in range(2):
        assert enslist[i].dead()
    assert enslist[0]["chan"] == "BH1"
    assert enslist[1]["chan"] == "BHZ"
    assert len(enslist[0].member) == 0
    
    # Run all the data with this expanded interval to verify the 
    # algorithm works with multiple stations in each call 
    # Simple way to do that is to just not use base_query
    enslist = TimeIntervalReader(db,starttime,endtime,
                           collection="wf_TimeSeries",
                           fix_overlaps=True,
                           )
    assert len(enslist) == 2
    for i in range(2):
        assert enslist[i].live()
    assert enslist[0]["chan"] == "BH1"
    assert enslist[1]["chan"] == "BHZ"
    assert len(enslist[0].member) == 2
    assert len(enslist[1].member) == 2
    # Repeat with zero_gaps set true - adds one more to count of recovered
    enslist = TimeIntervalReader(db,starttime,endtime,
                           collection="wf_TimeSeries",
                           fix_overlaps=True,
                           zero_gaps=True
                           )
    assert len(enslist) == 2
    for i in range(2):
        assert enslist[i].live()
    assert enslist[0]["chan"] == "BH1"
    assert enslist[1]["chan"] == "BHZ"
    assert len(enslist[0].member) == 3
    assert len(enslist[1].member) == 3
    