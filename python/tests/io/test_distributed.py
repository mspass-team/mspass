import pytest

from mspasspy.db.database import Database
from mspasspy.db.client import DBClient
from helper import (
    get_live_seismogram,
    get_live_timeseries,
    get_live_timeseries_ensemble,
    get_live_seismogram_ensemble,
)
from bson.objectid import ObjectId
from datetime import datetime
import copy
import dask
from pyspark import SparkContext
sc = SparkContext("local", "io_distributed_testing")

from mspasspy.io.distributed import (
    read_distributed_data,
    write_distributed_data,
    read_to_dataframe,
)
from mspasspy.db.normalize import ObjectIdMatcher,normalize
from mspasspy.ccore.utility import ErrorSeverity

# globals for this test module
number_atomic_wf = 5 # with 3 partitions this intentionally gives uneven sizes
number_ensemble_wf = 4   # intentionaly not same as number_atomic_wf
number_ensembles = 3   # ensemble tests create this many ensembles 
number_partitions=3  # some tests may need to change this one
testdbname = "mspass_test_db"


def make_channel_record(val,net="00",sta="sta",chan="chan",loc="00"):
    """
    Returns a dict of base attributes needed for default of 
    ObjectIdMatcher for the channel collection.   The value of 
    all numeric fields are set to input parameter val value.  
    net, sta, chan, and loc are defaulted but can be changed with kwargs 
    values if needed.   Having constant values is appropriate for this 
    test file but not for real data.
    """
    doc=dict()
    doc['sta']=sta
    doc['net']=net
    doc['chan']=chan
    doc['loc']=loc
    doc['lat']=val
    doc['lon']=val
    doc['elev']=val
    doc['starttime']=0.0   # 0 epoch time for universal match
    doc['endtime']=datetime.utcnow().timestamp()
    return doc

def make_site_record(val,net="00",sta="sta",loc="00"):
    """
    Returns a dict of base attributes needed for default of 
    ObjectIdMatcher for the site collection.   The value of 
    all numeric fields are set to input parameter val value.  
    net, sta,  and loc are defaulted but can be changed with kwargs 
    values if needed.   Having constant values is appropriate for this 
    test file but not for real data.
    """
    doc=dict()
    doc['sta']=sta
    doc['net']=net
    doc['loc']=loc
    doc['lat']=val
    doc['lon']=val
    doc['elev']=val
    doc['starttime']=0.0   # 0 epoch time for universal match
    doc['endtime']=datetime.utcnow().timestamp()
    return doc

def make_source_record(val,time=0.0):
    """
    Returns a dict of base attributes needed for default of 
    ObjectIdMatcher for the source collection.   The value of 
    all numeric fields are set to input parameter val value.  
    Having constant values is appropriate for this 
    test file but not for real data.
    
    time can be changed if desired for each entry to make something unique
    for each source
    """
    doc=dict()

    doc['lat']=val
    doc['lon']=val
    doc['depth']=val
    doc['time']=time
    doc['magnitude']=1.0
    return doc


@pytest.fixture
def atomic_time_series_generator():
    """
    Regularizes creation of wf_TimeSeries and stock normalizing 
    collection entries for TimeSeries tests with atomic read/write.  
    Generates 3 copies with different channel_id values and 
    a single source_id linking record.  
    
   Note online sources show complicated ways that might be useful 
   here to allow some things like number of items generated to 
   be a kwargs value.   For now I (glp) will leave that as a 
   simple constant set within this script as a global.
    
    Returns a list of ObjectIDs of waveforms saved to wf_TimeSeries. 
    Note each datum has linking, valid id links to source, site, 
    and channel.

    """

    
    client = DBClient("localhost")
    db = client.get_database(testdbname)

    source_doc = make_source_record(1.0)
    source_id = db.source.insert_one(source_doc).inserted_id
    wfid_list=[]
    for i in range(number_atomic_wf):
        sta="station{}".format(i)
        channel_doc = make_channel_record(float(i),sta=sta)
        channel_id = db.channel.insert_one(channel_doc).inserted_id
        site_doc = make_site_record(float(i),sta=sta)
        site_id = db.site.insert_one(site_doc).inserted_id
        test_ts = get_live_timeseries()
        test_ts["site_id"] = site_id
        test_ts["source_id"] = source_id
        test_ts["channel_id"] = channel_id
        sdret = db.save_data(test_ts,collection="wf_TimeSeries")
        # default for save_data returns a dict with _id defined. 
        # cpi;d break if the default changes
        wfid_list.append(sdret['_id'])
    yield wfid_list
    # this is cleanup code when test using this fixture exits
    # drop_database does almost nothng if name doesn't exist so 
    # we don't worry about multiple fixtures calling it
    client.drop_database(testdbname)


@pytest.fixture
def atomic_seismogram_generator():
    """
    Regularizes creation of wf_Seismogram and stock normalizing 
    collection entries for Seismogram tests with atomic read/write.  
    Generates 3 copies with different channel_id values and 
    a single source_id linking record.  
    
    Note online sources show complicated ways that might be useful 
    here to allow some things like number of items generated to 
    be a kwargs value.   For now I (glp) will leave that as a 
    simple constant set within this script as a global.
    
    Returns a list of ObjectIDs of waveforms saved to wf_Seismogram. 
    Note each datum has linking, valid id links to source, site, 
    and channel.

    """

    
    client = DBClient("localhost")
    db = client.get_database(testdbname)

    source_doc = make_source_record(1.0)
    source_id = db.source.insert_one(source_doc).inserted_id
    wfid_list=[]
    for i in range(number_atomic_wf):
        sta="station{}".format(i)
        site_doc = make_site_record(float(i),sta=sta)
        site_id = db.site.insert_one(site_doc).inserted_id
        test_ts = get_live_seismogram()
        test_ts["site_id"] = site_id
        test_ts["source_id"] = source_id
        sdret = db.save_data(test_ts,collection="wf_Seismogram")
        # default for save_data returns a dict with _id defined. 
        # cpi;d break if the default changes
        wfid_list.append(sdret['_id'])
    yield wfid_list
    # this is cleanup code when test using this fixture exits
    # drop_database does almost nothng if name doesn't exist so 
    # we don't worry about multiple fixtures calling it
    client.drop_database(testdbname)
    
@pytest.fixture
def TimeSeriesEnsemble_generator():
    client = DBClient("localhost")
    db = client.get_database(testdbname)
    
    # create multiple ensembles with different source_ids 
    # note source_id is only put in ensemble metadata container 
    # to test writers handle copying it to all members on save
    # add one site record per atomic datum for simplicity
    wfids = []   # will contain a list of ObjectId lists
    for i in range(number_ensembles):
        e = get_live_timeseries_ensemble(number_ensemble_wf)
        source_doc = make_source_record(float(i))
        source_id = db.source.insert_one(source_doc).inserted_id
        e["source_id"]=source_id
        # give sta value a name that can be used to infer ensemble number 
        # and member number
        for j in range(len(e.member)):
            sta="sta_{}_{}".format(i,j)
            site_doc = make_site_record(float(i+j),sta=sta)
            site_id = db.site.insert_one(site_doc).inserted_id
            e.member[j]["sta"] = sta
            e.member[j]["site_id"] = site_id
        e = db.save_data(e,collection="wf_TimeSeries",return_data=True)
        e_wfids = []
        for d in e.member:
            e_wfids.append(d['_id'])
        wfids.append(e_wfids)
    yield wfids
    # this is cleanup code when test using this fixture exits
    # drop_database does almost nothng if name doesn't exist so 
    # we don't worry about multiple fixtures calling it
    client.drop_database(testdbname)

@pytest.fixture
def SeismogramEnsemble_generator():
    pass

        
@pytest.mark.parametrize("format,collection", 
                         #[("spark","wf_TimeSeries")])
                         [("dask","wf_TimeSeries"),
                          ("dask","wf_Seismogram"),
                          ("spark","wf_TimeSeries"),
                          ("spark","wf_Seismogram")
                          ])
def test_read_distributed_atomic(atomic_time_series_generator,atomic_seismogram_generator,format,collection):
    """
    This function is run with multiple tests to test atomic read (mostly) and 
    limited writes with the io.distributed module. That is, read_distributed_data 
    and its inverse write_distributed_data.   Although it perhaps belongs 
    elsewhere it also tests the standalone function read_to_dataframe.  
    What it tests is controlled by the two input parameters "format" and 
    "collection".   format must be either "dask" or "spark" and 
    collection must be either "wf_TimeSeries" or "wf_Seismgoram" with the 
    default MsPASS schema.   Which combinations are tested are controlled 
    by the pytest decorator (mark_parameterize) with the incantation 
    found in the arg list for the decorator.  See numerous online sources 
    if you are (appropriately) confused by the syntax.  
    
    This function does only limited tests of the writer.   Exercising 
    additional features of the writer are found in test_write_distributed_atomic 
    and test_write_distributed_ensemble. 
    
    The function uses two fixtures that create and tear down a test database.
    Current use creates an initial set of data stored on gridfs with no 
    data_tag value.  Writer instances should each create a separate 
    data_tag for each all if additional test sections are added.  
    
    Normalization tests use new (v2+) feature of list of BasicMatcher
    subclasses.
    """
    print("Starting test with format=",format, " and collection=",collection)
    if format=="spark":
        context=sc
    else:
        context=None

    if collection=="wf_TimeSeries":
        wfid_list = atomic_time_series_generator
    elif collection=="wf_Seismogram":
        wfid_list = atomic_seismogram_generator
    client = DBClient("localhost")
    db = client.get_database(testdbname)
    # first test reader without normalization.  Save result with 
    # data tag used to read copy back below
    bag_or_rdd = read_distributed_data(db,
                                  collection=collection,
                                  format=format,
                                  spark_context=context,
                                  )
    if format=="dask":
        wfdata_list=bag_or_rdd.compute()
    elif format=="spark":
        wfdata_list=bag_or_rdd.collect()
    else:
        raise ValueError("Illegal value format=",format)
    assert len(wfdata_list)==number_atomic_wf
    for d in wfdata_list:
        wfid = d["_id"]
        assert wfid in wfid_list
    wfdata_list.clear()
    nrmlist=[]
    # repeat with normalization - use channel only for TimeSeries
    if collection=="wf_TimeSeries":
        channel_matcher = ObjectIdMatcher(db,
                            "channel",
                            attributes_to_load=['net','sta','lat','lon','elev','_id'])
        nrmlist.append(channel_matcher)
    source_matcher = ObjectIdMatcher(db,
                        "source",
                        attributes_to_load=['lat','lon','depth','time','_id'])
    nrmlist.append(source_matcher)
    
    site_matcher = ObjectIdMatcher(db,
                        "site",
                        attributes_to_load=['net','sta','lat','lon','elev','_id'])
    nrmlist.append(site_matcher)
    bag_or_rdd = read_distributed_data(db,
                                  collection=collection,
                                  format=format,
                                  normalize=nrmlist,
                                  spark_context=context,
                                  )
    if format=="dask":
        wfdata_list=bag_or_rdd.compute()
    elif format=="spark":
        wfdata_list=bag_or_rdd.collect()
    else:
        raise ValueError("Illegal value format=",format)
    
    assert len(wfdata_list)==number_atomic_wf
    for d in wfdata_list:
        wfid = d["_id"]
        assert wfid in wfid_list
        # only verify one of each collection was set
        # intentionally don't test values as that could change in fixture
        assert d.is_defined("source_lat")
        if collection=="wf_TimeSeries":
            assert d.is_defined("channel_sta")
        assert d.is_defined("site_net")
    wfdata_list.clear()
    # repeat but this time push the bag into the writer and verify it works
    bag_or_rdd = read_distributed_data(db,
                                  collection=collection,
                                  format=format,
                                  normalize=nrmlist,
                                  spark_context=context,
                                  )
    new_wfids = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=True,
                                   collection=collection,
                                   data_tag="save_number_1",
                                   format=format,
                                   )
    # Default above is assumed for return_data == False.  In that 
    # case write_distributed_data should return a list of ObjectIds
    # of the saved waveforms.  We test that in the assertions below
    assert len(new_wfids)==number_atomic_wf
    # write_distributed_data only returns a list of ObjectId written
    # unless we set return_data True.
    # verify the ids are valid - i.e. were saved
    n = db[collection].count_documents({"data_tag" : "save_number_1"})
    assert n == number_atomic_wf
    cursor=db[collection].find({"data_tag" : "save_number_1"})
    for doc in cursor:
        testid=doc['_id']
        assert testid in new_wfids
    # nake sure this works with data_tag
    bag_or_rdd = read_distributed_data(db,
                                  collection=collection,
                                  format=format,
                                  normalize=nrmlist,
                                  data_tag="save_number_1",
                                  spark_context=context,
                                  )
    if format=="dask":
        wfdata_list=bag_or_rdd.compute()
    elif format=="spark":
        wfdata_list=bag_or_rdd.collect()
    else:
        raise ValueError("Illegal value format=",format)
    assert len(wfdata_list)==number_atomic_wf
    # test basic dataframe input - dataframe converter features are tested 
    # in a different function
    cursor.rewind()
    df = read_to_dataframe(db,cursor)
    bag_or_rdd = read_distributed_data(df,
                                  db,
                                  collection=collection,
                                  format=format,
                                  normalize=nrmlist,
                                  data_tag="save_number_1",
                                  npartitions=number_partitions,
                                  spark_context=context,
                                  )
    if format=="dask":
        wfdata_list=bag_or_rdd.compute()
    elif format=="spark":
        wfdata_list=bag_or_rdd.collect()
    else:
        raise ValueError("Illegal value format=",format)
    # note this dependency on above settign new_wfids - watch out if editing
    assert len(wfdata_list)==number_atomic_wf
    for d in wfdata_list:
        wfid = d["_id"]
        assert wfid in new_wfids
        # only verify one of each collection was set
        # intentionally don't test values as that could change in fixture
        assert d.is_defined("source_lat")
        if collection=="wf_TimeSeries":
            assert d.is_defined("channel_sta")
        assert d.is_defined("site_net")
    wfdata_list.clear()
    # test container_to_merge feature
    source_id = ObjectId()
    x={"merged_source_id" : source_id}
    xlist=[]
    for i in range(number_atomic_wf):
        xlist.append(x)
    if format=="dask":
        merge_bag = dask.bag.from_sequence(xlist,npartitions=number_partitions)
    else:
        # unclear if specifying number of parittions is required with spark
        # with dask they must match for the merge process to work
        merge_bag = context.parallelize(xlist,numSlices=number_partitions)
    bag_or_rdd = read_distributed_data(df,
                                  db,
                                  collection=collection,
                                  format=format,
                                  normalize=nrmlist,
                                  container_to_merge=merge_bag,
                                  npartitions=number_partitions,
                                  data_tag="save_number_1",
                                  spark_context=context,
                                  )
    if format=="dask":
        wfdata_list=bag_or_rdd.compute()
    elif format=="spark":
        wfdata_list=bag_or_rdd.collect()
    else:
        raise ValueError("Illegal value format=",format)
    assert len(wfdata_list)==number_atomic_wf
    for d in wfdata_list:
        assert d.is_defined("merged_source_id")
        assert d["merged_source_id"] == source_id
    #TODO:  needs a test of scratchfile option for reader
    
def kill_one(d):
    """
    Used in map tests below to kill one dataum.  Frozen for now as 
    when channel_sta is sta1
    """
    print("Entering function kill_one")
    if d["site_sta"]=="station1": 
        d.kill()
        d.elog.log_error("kill_one", "test function killed this datum",ErrorSeverity.Invalid)
    return d

# functions used for  atomic writer tests
def massacre(d):
    """
    kill all
    """
    d.kill()
    d.elog.log_error("massacre", "test function killed this datum",ErrorSeverity.Invalid)
    return d

def set_one_invalid(d):
    if d["site_sta"]=="station1": 
        d["samplling_rate"]="bad_data"
    return d
    
def count_valid_ids(testlist):
    count=0
    for x in testlist:
        if x:
            count += 1
    return count
# compable functions for testing writer with ensembles
def kill_one_member(ens):
    """
    Kill member of each ensemble in a map call for tests.
    """
    ens.member[0].kill()
    ens.member[0].elog.log_error("kill_one_member", "test function killed this datum",ErrorSeverity.Invalid)
    return ens

def set_one_invalid_member(ens):
    ens.member[0]['sampling_rate']='bad_data'
    return ens

from mspasspy.algorithms.signals import detrend
@pytest.mark.parametrize("format,collection", 
                         [("dask","wf_TimeSeries"),
                          ("dask","wf_Seismogram"),
                          #("spark","wf_TimeSeries"),
                          #("spark","wf_Seismogram")
                          ])
def test_write_distributed_atomic(atomic_time_series_generator,atomic_seismogram_generator,format,collection):
    """
    Supplement to test_read_distributed_atomic focused more on the 
    writer than the reader.  Split these tests to reduce risk of 
    accumulation of dependencies of previous tests.  
    
    The writer has several features that are tested independently 
    in this function.
    """
    print("Starting test with format=",format, " and collection=",collection)
    if format=="spark":
        context=sc
    else:
        context=None
    # generators create wfid_list but we ignore it in these tests
    # Deleted to assure not misused below
    if collection=="wf_TimeSeries":
        wfid_list = atomic_time_series_generator
    elif collection=="wf_Seismogram":
        wfid_list = atomic_seismogram_generator
    del wfid_list
    client = DBClient("localhost")
    db = client.get_database(testdbname)
    
    # First test a basic read and write.  This duplicates a test in 
    # the reader but produces a saved ensemble we will reuse later
    nrmlist=[]
    if collection=="wf_TimeSeries":
        channel_matcher = ObjectIdMatcher(db,
                            "channel",
                            attributes_to_load=['net','sta','lat','lon','elev','_id'])
        nrmlist.append(channel_matcher)
    source_matcher = ObjectIdMatcher(db,
                        "source",
                        attributes_to_load=['lat','lon','depth','time','_id'])
    nrmlist.append(source_matcher)
    
    site_matcher = ObjectIdMatcher(db,
                        "site",
                        attributes_to_load=['net','sta','lat','lon','elev','_id'])
    nrmlist.append(site_matcher)
    bag_or_rdd = read_distributed_data(db,
                                  collection=collection,
                                  format=format,
                                  normalize=nrmlist,
                                  spark_context=context,
                                  )
    test_wfids = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=True,
                                   collection=collection,
                                   data_tag="save_number_1",
                                   format=format,
                                   )
    # Default above is assumed for return_data == False.  In that 
    # case write_distributed_data should return a list of ObjectIds
    # of the saved waveforms.  We test that in the assertions below
    assert len(test_wfids)==number_atomic_wf
    # write_distributed_data only returns a list of ObjectId written
    # unless we set return_data True.
    # verify the ids are valid - i.e. were saved
    n = db[collection].count_documents({"data_tag" : "save_number_1"})
    assert n == number_atomic_wf
    cursor=db[collection].find({"data_tag" : "save_number_1"})
    for doc in cursor:
        testid=doc['_id']
        assert testid in test_wfids
        
    # Read that group back in and store the list of data in by 
    # the symbol dlist0.  That and test_wfids provide the reference for 
    # writer comparisons in all tests below
    bag_or_rdd = read_distributed_data(db,
                                  collection=collection,
                                  format=format,
                                  normalize=nrmlist,
                                  spark_context=context,
                                  data_tag="save_number_1"
                                  )
    if format=="dask":
        dlist0=bag_or_rdd.compute()
    elif format=="spark":
        dlist0=bag_or_rdd.collect()
    # Sanity check to verify that did what id should have done
    assert len(dlist0)==number_atomic_wf
    
    
    # Test one explicit kill
    # set npartition to assure irregular sizes
    bag_or_rdd = read_distributed_data(db,
                                  collection=collection,
                                  format=format,
                                  normalize=nrmlist,
                                  npartitions=number_partitions,
                                  spark_context=context,
                                  data_tag="save_number_1"
                                  )
    #print("partitions in reader output=",bag_or_rdd.npartitions)
    if format=="dask":
        bag_or_rdd = bag_or_rdd.map(kill_one)
    else:
        # TODO  - kill_one map call here fails while detrend using 
        # the same synatx does not.  Same function works in spark
        # mysterious problem I'm punting while I write additional tests
        #bag_or_rdd = bag_or_rdd.map(lambda d : kill_one(d))
        bag_or_rdd = bag_or_rdd.map(lambda d : detrend(d))
    #print("partitions after map operator=",bag_or_rdd.npartitions)
    newwfidslist = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=True,
                                   collection=collection,
                                   data_tag="save_explicit_kill",
                                   format=format,
                                   )
    assert len(newwfidslist)==number_atomic_wf
    # kills are returned as None - this function counts not none values
    assert count_valid_ids(newwfidslist)==(number_atomic_wf-1)
    n = db[collection].count_documents({"data_tag" : "save_explicit_kill"})
    assert n==(number_atomic_wf-1)
    n = db["cemetery"].count_documents({})
    assert n==1
    doc=db["cemetery"].find_one()
    #from bson import json_util
    #print(json_util.dumps(doc,indent=2))
    assert "tombstone" in doc
    assert "logdata" in doc
    # Better to bulldoze the cemetery before continuing
    db.drop_collection("cemetery")
    
    
    # Exactly the same as above but using cremate option. 
    # only difference should be that creation leaves no cemetery document.
    bag_or_rdd = read_distributed_data(db,
                                  collection=collection,
                                  format=format,
                                  normalize=nrmlist,
                                  npartitions=number_partitions,
                                  spark_context=context,
                                  data_tag="save_number_1"
                                  )
    #print("partitions in reader output=",bag_or_rdd.npartitions)
    if format=="dask":
        bag_or_rdd = bag_or_rdd.map(kill_one)
    else:
        # TODO  - kill_one map call here fails while detrend using 
        # the same synatx does not.  Same function works in spark
        # mysterious problem I'm punting while I write additional tests
        #bag_or_rdd = bag_or_rdd.map(lambda d : kill_one(d))
        bag_or_rdd = bag_or_rdd.map(lambda d : detrend(d))
    #print("partitions after map operator=",bag_or_rdd.npartitions)
    newwfidslist = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=True,
                                   collection=collection,
                                   data_tag="save_explicit_kill_w_cremation",
                                   format=format,
                                   cremate=True,
                                   )
    assert len(newwfidslist)==number_atomic_wf
    # kills are returned as None - this function counts not none values
    assert count_valid_ids(newwfidslist)==(number_atomic_wf-1)
    n = db[collection].count_documents({"data_tag" : "save_explicit_kill_w_cremation"})
    assert n==(number_atomic_wf-1)
    n = db["cemetery"].count_documents({})
    assert n==0
    db.drop_collection("cemetery")
    
    # Repeat for end member killing all
    bag_or_rdd = read_distributed_data(db,
                                  collection=collection,
                                  format=format,
                                  normalize=nrmlist,
                                  npartitions=number_partitions,
                                  spark_context=context,
                                  data_tag="save_number_1"
                                  )
    #print("partitions in reader output=",bag_or_rdd.npartitions)
    if format=="dask":
        bag_or_rdd = bag_or_rdd.map(massacre)
    else:
        bag_or_rdd = bag_or_rdd.map(lambda d : massacre(d))
    #print("partitions after map operator=",bag_or_rdd.npartitions)
    newwfidslist = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=True,
                                   collection=collection,
                                   data_tag="save_kill_all",
                                   format=format,
                                   )
    assert len(newwfidslist)==number_atomic_wf
    # kills are returned as None - this function counts not none values
    assert count_valid_ids(newwfidslist)==0
    n = db["cemetery"].count_documents({})
    assert n==number_atomic_wf 
    # Better to bulldoze the cemetery before continuing
    db.drop_collection("cemetery")
    
    # Now do a kill with pedantic mode and an invalid value
    bag_or_rdd = read_distributed_data(db,
                                  collection=collection,
                                  format=format,
                                  normalize=nrmlist,
                                  npartitions=number_partitions,
                                  spark_context=context,
                                  data_tag="save_number_1"
                                  )
    #print("partitions in reader output=",bag_or_rdd.npartitions)
    if format=="dask":
        bag_or_rdd = bag_or_rdd.map(set_one_invalid)
    else:
        bag_or_rdd = bag_or_rdd.map(lambda d : set_one_invalid(d))
    #print("partitions after map operator=",bag_or_rdd.npartitions)
    newwfidslist = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=True,
                                   mode="pedantic",
                                   collection=collection,
                                   data_tag="save_pedantic_kill",
                                   format=format,
                                   )
    assert len(newwfidslist)==number_atomic_wf
    # kills are returned as None - this function counts not none values
    assert count_valid_ids(newwfidslist)==(number_atomic_wf-1)
    n = db[collection].count_documents({"data_tag" : "save_pedantic_kill"})
    assert n==(number_atomic_wf-1)
    n = db["cemetery"].count_documents({})
    assert n==1
    doc=db["cemetery"].find_one()
    #from bson import json_util
    #print(json_util.dumps(doc,indent=2))
    assert "tombstone" in doc
    assert "logdata" in doc
    # Better to bulldoze the cemetery before continuing
    db.drop_collection("cemetery")
    
    # Repeat with cremate option set True
    # Only difference should be that it leaves no cemetery document
    bag_or_rdd = read_distributed_data(db,
                                  collection=collection,
                                  format=format,
                                  normalize=nrmlist,
                                  npartitions=number_partitions,
                                  spark_context=context,
                                  data_tag="save_number_1"
                                  )
    #print("partitions in reader output=",bag_or_rdd.npartitions)
    if format=="dask":
        bag_or_rdd = bag_or_rdd.map(set_one_invalid)
    else:
        bag_or_rdd = bag_or_rdd.map(lambda d : set_one_invalid(d))
    #print("partitions after map operator=",bag_or_rdd.npartitions)
    newwfidslist = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=True,
                                   mode="pedantic",
                                   collection=collection,
                                   data_tag="save_pedantic_kill_w_cremation",
                                   format=format,
                                   cremate=True,
                                   )
    assert len(newwfidslist)==number_atomic_wf
    # kills are returned as None - this function counts not none values
    assert count_valid_ids(newwfidslist)==(number_atomic_wf-1)
    n = db[collection].count_documents({"data_tag" : "save_pedantic_kill_w_cremation"})
    assert n==(number_atomic_wf-1)
    n = db["cemetery"].count_documents({})
    assert n==0
    db.drop_collection("cemetery")
    
    #TODO  Needs additional tests for 
    # storage_mode="files" - not sure how to do cleanup
    # post_elog option - not sure we need this feature I added
    # post_history - not sure how to do this test.  Likely more useful than 
    #   post_elog as if history is turned on it would speed writes to 
    #   set this true
    
    # Now test return_data True option of reader
#    bag_or_rdd = read_distributed_data(db,
#                                  collection=collection,
#                                  format=format,
#                                  normalize=nrmlist,
#                                  spark_context=context,
#                                  npartitions=number_partitions,
#                                  data_tag="save_number_1"
#                                  )
#    bag_or_rdd = write_distributed_data(bag_or_rdd, 
#                                   db,
#                                   data_are_atomic=True,
#                                   collection=collection,
#                                   data_tag="save_pedantic_kill",
#                                   format=format,
#                                   return_data=True,
#                                   )
#    if format=="dask":
#        dlist=bag_or_rdd.compute()
#    elif format=="spark":
#        dlist=bag_or_rdd.collect()
#    assert len(dlist)==number_atomic_wf
#    # Order is not preserved so dlist and dlist0 cannot be directly 
#    # compared.  Hopefully this set of tests are sufficient to 
#    # reveal most problems that might happen with revisions
#    # change if a feature is added that might invalidate this test
#    defined_list=["live","npts","channel_lat","site_lat","source_lat"]
#    for d in dlist:
#        wfid = d['_id']
#        assert wfid not in test_wfids
#        assert d.live
#        for k in defined_list:
#            assert d.is_defined(k)
#            
#    # release the memory in dlist - bit huge but also reduces odds of 
#    # odd behavior later
#    del dlist
    
    
    
    
    
        
@pytest.mark.parametrize("format,collection", 
                         [("dask","wf_TimeSeries")],
                          )
 #                         ("dask","wf_Seismogram"),
 #                         ("spark","wf_TimeSeries"),
 #                         ("spark","wf_Seismogram")
 #                         ])
def test_read_distributed_ensemble(TimeSeriesEnsemble_generator,SeismogramEnsemble_generator,format,collection):
     print("Starting test with format=",format, " and collection=",collection)
     if format=="spark":
         context=sc
     else:
         context=None
     if collection=="wf_TimeSeries":
         wfid_list = TimeSeriesEnsemble_generator
     elif collection=="wf_Seismogram":
         wfid_list = SeismogramEnsemble_generator
     client = DBClient("localhost")
     db = client.get_database(testdbname)
     # We use source_id in this test to define ensembles.  
     # Used to generate a list of query dictionaries by source_id
     srcid_list=db.source.distinct('_id')
     querylist=[]
     for srcid in srcid_list:
         querylist.append({'source_id' : srcid})
     # first test dask reader without normalization.  Save result with 
     # data tag used to read copy back below
     bag_or_rdd = read_distributed_data(querylist,
                                   db,
                                   collection=collection,
                                   format=format,
                                   spark_context=context,
                                   )
     if format=="dask":
         wfdata_list=bag_or_rdd.compute()
     elif format=="spark":
         wfdata_list=bag_or_rdd.collect()
    
     assert len(wfdata_list) == number_ensembles
     for e in wfdata_list:
         assert e.live
         assert len(e.member) == number_ensemble_wf
         for d in e.member:
             assert d.live
             # appropriate only because we read in default promiscuous mode
             assert d.is_defined("source_id")
             
     # repeat with normalization to load source info in ensemble metadata
     # and site data into members.  Note this also tests the container_to_merge 
     # functionality as we need to use it to load source_id values to 
     # each ensemble - recycle srcid_list - caution
     # notice we have to convert to a list of dict objects
     # testing only default partition matching.  Error handlers 
     # handle case with mismatched parittioning specified explicitly
     srcdocs=[]
     for id in srcid_list:
         srcdocs.append({"source_id" : id})
     if format=="dask":
         bag_or_rdd_to_merge = dask.bag.from_sequence(srcdocs)
     else:
        bag_or_rdd_to_merge.parallelize(srcid_list)

     source_matcher = ObjectIdMatcher(db,
                        "source",
                        attributes_to_load=['lat','lon','depth','time','_id'])
    
     site_matcher = ObjectIdMatcher(db,
                        "site",
                        attributes_to_load=['net','sta','lat','lon','elev','_id'])
     bag_or_rdd = read_distributed_data(querylist,
                                   db,
                                   collection=collection,
                                   format=format,
                                   spark_context=context,
                                   container_to_merge=bag_or_rdd_to_merge,
                                   normalize=[site_matcher],
                                   normalize_ensemble=[source_matcher]
                                   )
     if format=="dask":
         wfdata_list=bag_or_rdd.compute()
     elif format=="spark":
         wfdata_list=bag_or_rdd.collect()
     
     assert len(wfdata_list) == number_ensembles
     for e in wfdata_list:
         assert e.live
         assert len(e.member) == number_ensemble_wf
         assert e.is_defined('source_id')
         assert e.is_defined('source_lat')
         for d in e.member:
             assert d.live
             assert d.is_defined("npts")
             assert d.is_defined("time_standard")
             assert d.is_defined("site_id")
             assert d.is_defined("site_lat")
             
     # now test the same read followed by a basic write - drop ensemble normalization 
     bag_or_rdd = read_distributed_data(querylist,
                                  db,
                                  collection=collection,
                                  format=format,
                                  spark_context=context,
                                  normalize=[site_matcher],
                                  )
     data_tag = "save_number_1"   # made a variable to allow changes to copy code
     wfidlists = write_distributed_data(bag_or_rdd, 
                                    db,
                                    data_are_atomic=False,
                                    collection=collection,
                                    data_tag=data_tag,
                                    format=format,
                                    )

     assert len(wfidlists)==number_ensembles
         
     # default return examined here is a list of ObjectId lists
     # here we verify they match

     for srcid in srcid_list:
         query={"source_id" : srcid, "data_tag" : data_tag}
         assert db[collection].count_documents(query) == number_ensemble_wf
         cursor = db[collection].find(query)
         for doc in cursor:
             assert 'time_standard' in doc
             assert 'site_id' in doc
             assert 'source_id' in doc
             assert doc['source_id']==srcid
             assert 'site_id' in doc
             # these should have been dropped on a save as normalization attributes
             assert 'site_lat' not in doc
             assert 'source_lat' not in doc
             assert doc['data_tag']==data_tag
             # this test is a bit more complex that might be expected
             # necessary because order is not guaranteed in the id lists
             number_hits=0
             for wfl in wfidlists:
                 wfid = doc['_id']
                 if wfid in wfl:
                     number_hits += 1
             assert number_hits==1
             
    #TODO  test sort_clause feature
                 
             
@pytest.mark.parametrize("format,collection", 
                         [("dask","wf_TimeSeries")],
                          )
 #                         ("dask","wf_Seismogram"),
 #                         ("spark","wf_TimeSeries"),
 #                         ("spark","wf_Seismogram")
 #                         ])
def test_write_distributed_ensemble(TimeSeriesEnsemble_generator,SeismogramEnsemble_generator,format,collection):
    """
    This test is a complement to test_read_distributed_ensemble and 
    test_write_distributed_atomic.   The tests here focus on handlling 
    of dead data during writes.   It assumes atomic level features are 
    covered by test in database and the atomic functions in this file.
    """
    print("Starting test with format=",format, " and collection=",collection)
    if format=="spark":
         context=sc
    else:
         context=None
    if collection=="wf_TimeSeries":
         wfid_list = TimeSeriesEnsemble_generator
    elif collection=="wf_Seismogram":
         wfid_list = SeismogramEnsemble_generator
    client = DBClient("localhost")
    db = client.get_database(testdbname)
    # We use source_id in this test to define ensembles.  
    # Used to generate a list of query dictionaries by source_id
    srcid_list=db.source.distinct('_id')
    querylist=[]
    for srcid in srcid_list:
         querylist.append({'source_id' : srcid,'data_tag' : {'$exists' : False}})

    cursor = db.wf_TimeSeries.find({})
    for doc in cursor:
        if 'data_tag' in doc:
            print(doc['data_tag'])
        else:
            print("Undefined")
    # these matchers are used in some, but not all tests below. 
    # defining them here as function scope initializations
    source_matcher = ObjectIdMatcher(db,
                       "source",
                       attributes_to_load=['lat','lon','depth','time','_id'])
   
    site_matcher = ObjectIdMatcher(db,
                       "site",
                       attributes_to_load=['net','sta','lat','lon','elev','_id'])     
     
    # this first test is comparable to the atomic version killing 
    # one item.  here, however, the kill is issued to a one member.
    bag_or_rdd = read_distributed_data(querylist,
                                 db,
                                 collection=collection,
                                 format=format,
                                 spark_context=context,
                                 normalize=[site_matcher],
                                 )
    if format=="dask":
        bag_or_rdd = bag_or_rdd.map(kill_one_member)
    else:
        bag_or_rdd = bag_or_rdd.map(lambda d : kill_one_member(d))
    data_tag = "kill_one_test"   # made a variable to allow changes to copy code
    wfidlists = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=False,
                                   collection=collection,
                                   data_tag=data_tag,
                                   format=format,
                                   )
    cursor = db.wf_TimeSeries.find({})
    for doc in cursor:
        if 'data_tag' in doc:
            print(doc['data_tag'])
        else:
            print("Undefined")
    assert len(wfidlists)==number_ensembles
    # kills should leave one tombstone per ensemble in this test
    n = db.cemetery.count_documents({})
    assert n == number_ensembles
    cursor = db.cemetery.find({})
    for doc in cursor:
        assert 'tombstone' in doc
        assert 'logdata' in doc
        
    # default return examined here is a list of ObjectId lists
    # here we verify they match

    for srcid in srcid_list:
        query={"source_id" : srcid, "data_tag" : data_tag}
        # should kill one member per ensemble so all should satisfy this test
        assert db[collection].count_documents(query) == (number_ensemble_wf-1)
        cursor = db[collection].find(query)
        for doc in cursor:
            assert 'time_standard' in doc
            assert 'site_id' in doc
            assert 'source_id' in doc
            assert doc['source_id']==srcid
            assert 'site_id' in doc
            # these should have been dropped on a save as normalization attributes
            assert 'site_lat' not in doc
            assert 'source_lat' not in doc
            assert doc['data_tag']==data_tag
            # this test is a bit more complex that might be expected
            # necessary because order is not guaranteed in the id lists
            number_hits=0
            for wfl in wfidlists:
                wfid = doc['_id']
                if wfid in wfl:
                    number_hits += 1
            assert number_hits==1
    # need to do this to simplify counts for next test
    db.drop_collection('cemetery')
    
    # repeat the same test with cremate option
    bag_or_rdd = read_distributed_data(querylist,
                                 db,
                                 collection=collection,
                                 format=format,
                                 spark_context=context,
                                 normalize=[site_matcher],
                                 )
    if format=="dask":
        bag_or_rdd = bag_or_rdd.map(kill_one_member)
    else:
        bag_or_rdd = bag_or_rdd.map(lambda d : kill_one_member(d))
    data_tag = "kill_one_cremation_test"   # made a variable to allow changes to copy code
    wfidlists = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=False,
                                   collection=collection,
                                   data_tag=data_tag,
                                   format=format,
                                   cremate=True,
                                   )
    cursor = db.wf_TimeSeries.find({})
    for doc in cursor:
        if 'data_tag' in doc:
            print(doc['data_tag'])
        else:
            print("Undefined")
    assert len(wfidlists)==number_ensembles
    # kills should leave one tombstone per ensemble in this test
    n = db.cemetery.count_documents({})
    assert n == 0

        
    # default return examined here is a list of ObjectId lists
    # here we verify they match

    for srcid in srcid_list:
        query={"source_id" : srcid, "data_tag" : data_tag}
        # should kill one member per ensemble so all should satisfy this test
        assert db[collection].count_documents(query) == (number_ensemble_wf-1)
        cursor = db[collection].find(query)
        for doc in cursor:
            assert 'time_standard' in doc
            assert 'site_id' in doc
            assert 'source_id' in doc
            assert doc['source_id']==srcid
            assert 'site_id' in doc
            # these should have been dropped on a save as normalization attributes
            assert 'site_lat' not in doc
            assert 'source_lat' not in doc
            assert doc['data_tag']==data_tag
            # this test is a bit more complex that might be expected
            # necessary because order is not guaranteed in the id lists
            number_hits=0
            for wfl in wfidlists:
                wfid = doc['_id']
                if wfid in wfl:
                    number_hits += 1
            assert number_hits==1
    # need to do this to simplify counts for next test
    db.drop_collection('cemetery')
            
    # Now test killing all ensembles - can use the same massacre (kill all) function
    # because of api consistency
    bag_or_rdd = read_distributed_data(querylist,
                                 db,
                                 collection=collection,
                                 format=format,
                                 spark_context=context,
                                 normalize=[site_matcher],
                                 )
    if format=="dask":
        bag_or_rdd = bag_or_rdd.map(massacre)
    else:
        bag_or_rdd = bag_or_rdd.map(lambda d : massacre(d))
    data_tag = "kill_all_test"   # made a variable to allow changes to copy code
    wfidlists = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=False,
                                   collection=collection,
                                   data_tag=data_tag,
                                   format=format,
                                   )
    assert len(wfidlists)==number_ensembles
    # kills should leave one tombstone per ensemble in this test
    n = db.cemetery.count_documents({})
    assert n == (number_ensembles*number_ensemble_wf)
    cursor = db.cemetery.find({})
    for doc in cursor:
        assert 'tombstone' in doc
        assert 'logdata' in doc
        
    # need to do this to simplify counts for next test
    db.drop_collection('cemetery')
    
    
    # Now test for schema failure with a mode='pedantic'   
    # Test ill kill on member and yields and output similar to 
    # the kill one test
    bag_or_rdd = read_distributed_data(querylist,
                                 db,
                                 collection=collection,
                                 format=format,
                                 spark_context=context,
                                 normalize=[site_matcher],
                                 )
    if format=="dask":
        bag_or_rdd = bag_or_rdd.map(set_one_invalid_member)
    else:
        bag_or_rdd = bag_or_rdd.map(lambda d : set_one_invalid_member(d))
    data_tag = "pedantic_write_kill_test"   # made a variable to allow changes to copy code
    wfidlists = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=False,
                                   mode='pedantic',
                                   collection=collection,
                                   data_tag=data_tag,
                                   format=format,
                                   )
    cursor = db.wf_TimeSeries.find({})
    for doc in cursor:
        if 'data_tag' in doc:
            print(doc['data_tag'])
        else:
            print("Undefined")
    assert len(wfidlists)==number_ensembles
    # kills should leave one tombstone per ensemble in this test
    n = db.cemetery.count_documents({})
    assert n == number_ensembles # one failure per ensemble
    cursor = db.cemetery.find({})
    for doc in cursor:
        assert 'tombstone' in doc
        assert 'logdata' in doc
        
    # default return examined here is a list of ObjectId lists
    # here we verify they match

    for srcid in srcid_list:
        query={"source_id" : srcid, "data_tag" : data_tag}
        # should kill one member per ensemble so all should satisfy this test
        assert db[collection].count_documents(query) == (number_ensemble_wf-1)
        cursor = db[collection].find(query)
        for doc in cursor:
            assert 'time_standard' in doc
            assert 'site_id' in doc
            assert 'source_id' in doc
            assert doc['source_id']==srcid
            assert 'site_id' in doc
            # these should have been dropped on a save as normalization attributes
            assert 'site_lat' not in doc
            assert 'source_lat' not in doc
            assert doc['data_tag']==data_tag
            # this test is a bit more complex that might be expected
            # necessary because order is not guaranteed in the id lists
            number_hits=0
            for wfl in wfidlists:
                wfid = doc['_id']
                if wfid in wfl:
                    number_hits += 1
            assert number_hits==1
    # need to do this to simplify counts for next test
    db.drop_collection('cemetery')
        
  

    
def test_read_error_handlers(atomic_time_series_generator):
    atomic_time_series_generator
    client = DBClient("localhost")
    db = client.get_database(testdbname)
    
    # now test error handlers.  First, test reade error handlers
    # illegal value for format argument
    with pytest.raises(ValueError, match="Unsupported value for format"):
        mybag = read_distributed_data(db,
                                      collection="wf_TimeSeries",
                                      format="illegal_format",
                                      )
    # illegal value for db argument when using dataframe input
    cursor=db.wf_TimeSeries.find({})
    df = read_to_dataframe(db,cursor)
    with pytest.raises(TypeError,match="Illegal type"):
        mybag = read_distributed_data(df,
                                      db=True,
                                      collection="wf_TimeSeries",
                                      format="dask",
                                      )
    # Defaulted (none) value for db with dataframe input produces a differnt exeception
    with pytest.raises(TypeError,match="An instance of Database class is required"):
        mybag = read_distributed_data(df,
                                      db=None,
                                      collection="wf_TimeSeries",
                                      format="dask",
                                      )
    # this is illegal input for arg0 test - message match is a bit cryptic
    with pytest.raises(TypeError,match="Must be a"):
        mybag = read_distributed_data(float(2),
                                      collection="wf_TimeSeries",
                                      format="dask",
                                      )
    # test error handlers for different normalization options
    # first values that aren't lists
    with pytest.raises(TypeError,match="Illegal type for normalize argument"):
        mybag = read_distributed_data(db,
                                      collection="wf_TimeSeries",
                                      normalize="bad normalize type- string",
                                      )
    with pytest.raises(TypeError,match="Illegal type for normalize_ensemble argument"):
        mybag = read_distributed_data(db,
                                      collection="wf_TimeSeries",
                                      normalize_ensemble="bad normalize_ensemble",
                                      )
    # repeat with bad data in a component of list - using one valid value and a bad value
    source_matcher = ObjectIdMatcher(db,
                        "source",
                        attributes_to_load=['lat','lon','depth','time','_id'])
    nrmlist = [source_matcher,"foobar"]
    with pytest.raises(TypeError,match="Must be subclass of BasicMatcher"):
        mybag = read_distributed_data(db,
                                      collection="wf_TimeSeries",
                                      normalize=nrmlist,
                                      )
    with pytest.raises(TypeError,match="Must be subclass of BasicMatcher"):
        mybag = read_distributed_data(db,
                                      collection="wf_TimeSeries",
                                      normalize_ensemble=nrmlist,
                                      )
        
def test_write_error_handlers(atomic_time_series_generator):
    atomic_time_series_generator
    client = DBClient("localhost")
    db = client.get_database(testdbname)
    # these two are fixed for these tests for now as all we test 
    # are argument checks done at the top of the current implementation
    # if we need to test for exceptions thrown elsewhere would need to 
    # parameterize this test for spark/dask and maybe also collection
    collection='wf_TimeSeries'   
    format='dask' 
    
    # Used repeatedly below.  that works for these tests because 
    # mybag is never altered because we test only handlers in 
    # argument test section at the top of the function.
    # if tests for errors returned from inside function are added that
    # approach will not work
    mybag = read_distributed_data(db,
                                  collection=collection,
                                  format=format,
                                  )
    with pytest.raises(TypeError,match="required arg1"):
        mybag = write_distributed_data(mybag,"wrong type - I should be a Database")

    with pytest.raises(TypeError,match="Unsupported storage_mode"):
        mybag = write_distributed_data(mybag,db,storage_mode='illegal_storage_mode')
        
    with pytest.raises(ValueError,match="Illegal value of mode"):
        mybag = write_distributed_data(mybag,db,mode='illegal_mode')
        
    with pytest.raises(ValueError,match="Illegal value of collection"):
        mybag = write_distributed_data(mybag,db,collection='illegal_collection')



    
