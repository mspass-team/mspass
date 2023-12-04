import pytest

import os,shutil
import sys

from mspasspy.db.database import Database
from mspasspy.db.client import DBClient
sys.path.append("python/tests")
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


from mspasspy.io.distributed import (
    read_distributed_data,
    write_distributed_data,
    read_to_dataframe,
)
from mspasspy.db.normalize import ObjectIdMatcher
from mspasspy.ccore.utility import ErrorSeverity

# globals for this test module
number_atomic_wf = 5 # with 3 partitions this intentionally gives uneven sizes
number_ensemble_wf = 4   # intentionaly not same as number_atomic_wf
number_ensembles = 3   # ensemble tests create this many ensembles 
number_partitions=3  # some tests may need to change this one
testdbname = "mspass_test_db"
wfdir = "python/tests/data/wf_filetestdata"

@pytest.fixture
def setup_environment():
    if os.path.exists(wfdir):
        raise Exception("test_distributed setup:  scratch directory={} exists.  Must be empty to run this test".format(wfdir))
    else:
        os.makedirs(wfdir)
        
    yield
    
    if os.path.exists(wfdir):
        shutil.rmtree(wfdir)
    
@pytest.fixture
def spark():
    sc = SparkContext("local", "io_distributed_testing")
    yield sc
    sc.stop()

# This is a set of small functions at file scope used in this test file.
def make_channel_record(val,net="00",sta="sta",chan="chan",loc="00",data_tag=None):
    """
    Returns a dict of base attributes needed for default of 
    ObjectIdMatcher for the channel collection.   The value of 
    all numeric fields are set to input parameter val value.  
    net, sta, chan, and loc are defaulted but can be changed with kwargs 
    values if needed.   Having constant values is appropriate for this 
    test file but not for real data.
    
    Use the (default None meaning turned off) data_tag value to add a 
    data_tag attribute to each record.  Needed in some cases when 
    we need to distinguish recorded added by TimeSeries and Seismogram 
    generators.  
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
    if data_tag:
        doc['data_tag']=data_tag
    return doc


def make_site_record(val,net="00",sta="sta",loc="00",data_tag=None):
    """
    Returns a dict of base attributes needed for default of 
    ObjectIdMatcher for the site collection.   The value of 
    all numeric fields are set to input parameter val value.  
    net, sta,  and loc are defaulted but can be changed with kwargs 
    values if needed.   Having constant values is appropriate for this 
    test file but not for real data.
    
    se the (default None meaning turned off) data_tag value to add a 
    data_tag attribute to each record.  Needed in some cases when 
    we need to distinguish recorded added by TimeSeries and Seismogram 
    generators.  
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
    if data_tag:
        doc['data_tag']=data_tag
    return doc

def make_source_record(val,time=0.0,data_tag=None):
    """
    Returns a dict of base attributes needed for default of 
    ObjectIdMatcher for the source collection.   The value of 
    all numeric fields are set to input parameter val value.  
    Having constant values is appropriate for this 
    test file but not for real data.
    
    time can be changed if desired for each entry to make something unique
    for each source
    
    se the (default None meaning turned off) data_tag value to add a 
    data_tag attribute to each record.  Needed in some cases when 
    we need to distinguish recorded added by TimeSeries and Seismogram 
    generators.  
    """
    doc=dict()

    doc['lat']=val
    doc['lon']=val
    doc['depth']=val
    doc['time']=time
    doc['magnitude']=1.0
    if data_tag:
        doc['data_tag']=data_tag
    return doc

def count_valid_ids(testlist):
    """
    Small test function used in tests to handle None use in ObjectId 
    list return for dead data
    """
    count=0
    for x in testlist:
        if x:
            count += 1
    return count


# This is a series of fixtures used in this module. First set are 
# data generators used for read/write distributed tests

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
    # tests using source_id need a data tag as the structure used 
    # with fixtures runs both TimeSeries and Seismogram generators 
    # for each ensemble test. Not sure how to avoid that so use 
    # this approach
    src_data_tag='timeseries'  
    for i in range(number_ensembles):
        e = get_live_timeseries_ensemble(number_ensemble_wf)
        source_doc = make_source_record(float(i),data_tag=src_data_tag)
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
        n=db.source.count_documents({})
    yield wfids
    # this is cleanup code when test using this fixture exits
    # drop_database does almost nothng if name doesn't exist so 
    # we don't worry about multiple fixtures calling it
    client.drop_database(testdbname)

@pytest.fixture
def SeismogramEnsemble_generator():
    client = DBClient("localhost")
    db = client.get_database(testdbname)
    
    # tests using source_id need a data tag as the structure used 
    # with fixtures runs both TimeSeries and Seismogram generators 
    # for each ensemble test. Not sure how to avoid that so use 
    # this approach
    src_data_tag='seismogram'  
    
    # create multiple ensembles with different source_ids 
    # note source_id is only put in ensemble metadata container 
    # to test writers handle copying it to all members on save
    # add one site record per atomic datum for simplicity
    wfids = []   # will contain a list of ObjectId lists
    for i in range(number_ensembles):
        e = get_live_seismogram_ensemble(number_ensemble_wf)
        source_doc = make_source_record(float(i),data_tag=src_data_tag)
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
        e = db.save_data(e,collection="wf_Seismogram",return_data=True)
        e_wfids = []
        for d in e.member:
            e_wfids.append(d['_id'])
        wfids.append(e_wfids)
        n=db.source.count_documents({})
    yield wfids
    # this is cleanup code when test using this fixture exits
    # drop_database does almost nothng if name doesn't exist so 
    # we don't worry about multiple fixtures calling it
    client.drop_database(testdbname)
    

# This set of fixures are a necessary evil because pyspark seems to 
# have trouble serializing functions defined and file scope and 
# passed to map operators.  The problem doesn't happen with dask.
# Found that using these fixture provided a way to work around that 
# problem and provide a clean mechanism to consistently test both 
# spark and dask implementations
@pytest.fixture    
def define_kill_one():
    def kill_one(d):
        """
        Used in map tests below to kill one dataum.  Frozen for now as 
        when channel_sta is sta1
        """
        if d["site_sta"]=="station1": 
            d.kill()
            d.elog.log_error("kill_one", "test function killed this datum",ErrorSeverity.Invalid)
        return d
    yield kill_one

@pytest.fixture    
def define_massacre():
    def massacre(d):
        """
        kill all
        """
        d.kill()
        d.elog.log_error("massacre", "test function killed this datum",ErrorSeverity.Invalid)
        return d
    yield massacre

@pytest.fixture
def define_set_one_invalid():
    def set_one_invalid(d):
        if d["site_sta"]=="station1": 
            d["samplling_rate"]="bad_data"
            d["npts"]="foobar"
        return d
    yield set_one_invalid
    

# compable functions for testing writer with ensembles
@pytest.fixture
def define_kill_one_member():
    def kill_one_member(ens):
        """
        Kill member of each ensemble in a map call for tests.
        """
        ens.member[0].kill()
        ens.member[0].elog.log_error("kill_one_member", "test function killed this datum",ErrorSeverity.Invalid)
        return ens
    yield kill_one_member

@pytest.fixture
def define_set_one_invalid_member():
    def set_one_invalid_member(ens):
        ens.member[0]['sampling_rate']='bad_data'
        return ens
    yield set_one_invalid_member

@pytest.fixture
def define_set_dir_dfile_atomic():
    def set_dir_dfile_atomic(d):
        dir=wfdir + "/" + "atomicdata"
        srcid = d['source_id']
        d['dir'] = dir
        d['dfile'] = str(srcid) + ".dat"
        return d
    yield set_dir_dfile_atomic

# This fixture is not currently used in this test file, but it 
# was retained as test could be added that might find it useful
@pytest.fixture
def define_set_dir_dfile_ensmble():
    def set_dir_dfile_ensemble(d):
        dir=wfdir + "/" + "ensembledata"
        srcid = d['source_id']
        d['dir'] = dir
        d['dfile'] = str(srcid) + ".dat"
        return d
    yield set_dir_dfile_ensemble


        
@pytest.mark.parametrize("scheduler,collection", 
                         [("dask","wf_TimeSeries"),
                          ("dask","wf_Seismogram"),
                          ("spark","wf_TimeSeries"),
                          ("spark","wf_Seismogram")
                          ])
def test_read_distributed_atomic(setup_environment,spark,atomic_time_series_generator,atomic_seismogram_generator,scheduler,collection):
    """
    This function is run with multiple tests to test atomic read (mostly) and 
    limited writes with the io.distributed module. That is, read_distributed_data 
    and its inverse write_distributed_data.   Although it perhaps belongs 
    elsewhere it also tests the standalone function read_to_dataframe.  
    What it tests is controlled by the two input parameters "scheduler" and 
    "collection".   scheduler must be either "dask" or "spark" and 
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
    print("Starting test with scheduler=",scheduler, " and collection=",collection)
    if scheduler=="spark":
        context=spark
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
                                  scheduler=scheduler,
                                  spark_context=context,
                                  )
    if scheduler=="dask":
        wfdata_list=bag_or_rdd.compute()
    elif scheduler=="spark":
        wfdata_list=bag_or_rdd.collect()
    else:
        raise ValueError("Illegal value scheduler=",scheduler)
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
                                  scheduler=scheduler,
                                  normalize=nrmlist,
                                  spark_context=context,
                                  )
    if scheduler=="dask":
        wfdata_list=bag_or_rdd.compute()
    elif scheduler=="spark":
        wfdata_list=bag_or_rdd.collect()
    else:
        raise ValueError("Illegal value scheduler=",scheduler)
    
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
                                  scheduler=scheduler,
                                  normalize=nrmlist,
                                  spark_context=context,
                                  )
    new_wfids = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=True,
                                   collection=collection,
                                   data_tag="save_number_1",
                                   scheduler=scheduler,
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
                                  scheduler=scheduler,
                                  normalize=nrmlist,
                                  data_tag="save_number_1",
                                  spark_context=context,
                                  )
    if scheduler=="dask":
        wfdata_list=bag_or_rdd.compute()
    elif scheduler=="spark":
        wfdata_list=bag_or_rdd.collect()
    else:
        raise ValueError("Illegal value scheduler=",scheduler)
    assert len(wfdata_list)==number_atomic_wf
    # test basic dataframe input - dataframe converter features are tested 
    # in a different function
    cursor.rewind()
    df = read_to_dataframe(db,cursor)
    bag_or_rdd = read_distributed_data(df,
                                  db,
                                  collection=collection,
                                  scheduler=scheduler,
                                  normalize=nrmlist,
                                  data_tag="save_number_1",
                                  npartitions=number_partitions,
                                  spark_context=context,
                                  )
    if scheduler=="dask":
        wfdata_list=bag_or_rdd.compute()
    elif scheduler=="spark":
        wfdata_list=bag_or_rdd.collect()
    else:
        raise ValueError("Illegal value scheduler=",scheduler)
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
    if scheduler=="dask":
        merge_bag = dask.bag.from_sequence(xlist,npartitions=number_partitions)
    else:
        # unclear if specifying number of parittions is required with spark
        # with dask they must match for the merge process to work
        merge_bag = context.parallelize(xlist,numSlices=number_partitions)
    bag_or_rdd = read_distributed_data(df,
                                  db,
                                  collection=collection,
                                  scheduler=scheduler,
                                  normalize=nrmlist,
                                  container_to_merge=merge_bag,
                                  npartitions=number_partitions,
                                  data_tag="save_number_1",
                                  spark_context=context,
                                  )
    if scheduler=="dask":
        wfdata_list=bag_or_rdd.compute()
    elif scheduler=="spark":
        wfdata_list=bag_or_rdd.collect()
    else:
        raise ValueError("Illegal value scheduler=",scheduler)
    assert len(wfdata_list)==number_atomic_wf
    for d in wfdata_list:
        assert d.is_defined("merged_source_id")
        assert d["merged_source_id"] == source_id
    #TODO:  needs a test of scratchfile option for reader


@pytest.mark.parametrize("scheduler,collection", 
                         [("dask","wf_TimeSeries"),
                          ("dask","wf_Seismogram"),
                          ("spark","wf_TimeSeries"),
                          ("spark","wf_Seismogram")
                          ])
def test_write_distributed_atomic(setup_environment,
                                  scheduler,
                                  collection,
                                  spark,
                                  define_kill_one,
                                  define_massacre,
                                  define_set_one_invalid,
                                  define_set_dir_dfile_atomic,
                                  atomic_time_series_generator,
                                  atomic_seismogram_generator,
                                  ):
    """
    Supplement to test_read_distributed_atomic focused more on the 
    writer than the reader.  Split these tests to reduce risk of 
    accumulation of dependencies of previous tests.  
    
    The writer has several features that are tested independently 
    in this function.
    """
    print("Starting test with scheduler=",scheduler, " and collection=",collection)
    if scheduler=="spark":
        context=spark
    else:
        context=None
    # generators create wfid_list but we ignore it in these tests
    # Deleted to assure not misused below
    if collection=="wf_TimeSeries":
        wfid_list = atomic_time_series_generator
    elif collection=="wf_Seismogram":
        wfid_list = atomic_seismogram_generator
    del wfid_list
    # Use fixtures to define map functions
    # all these test functions are implmented with a yield of the function 
    # name.  That means the lhs of the assignments below set those symbols
    # as functions we use in map operators in tests below
    kill_one = define_kill_one
    massacre = define_massacre
    set_one_invalid = define_set_one_invalid
    set_dir_dfile_atomic = define_set_dir_dfile_atomic
    
    # We need a local database handle for these tests
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
                                  scheduler=scheduler,
                                  normalize=nrmlist,
                                  npartitions=2,
                                  spark_context=context,
                                  )
    test_wfids = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=True,
                                   collection=collection,
                                   data_tag="save_number_1",
                                   scheduler=scheduler,
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
                                  scheduler=scheduler,
                                  normalize=nrmlist,
                                  spark_context=context,
                                  data_tag="save_number_1"
                                  )
    if scheduler=="dask":
        dlist0=bag_or_rdd.compute()
    elif scheduler=="spark":
        dlist0=bag_or_rdd.collect()
    # Sanity check to verify that did what id should have done
    assert len(dlist0)==number_atomic_wf
    for d in dlist0:
        assert d.live
    
    
    # Test one explicit kill
    # set npartition to assure irregular sizes
    bag_or_rdd = read_distributed_data(db,
                                  collection=collection,
                                  scheduler=scheduler,
                                  normalize=nrmlist,
                                  npartitions=number_partitions,
                                  spark_context=context,
                                  data_tag="save_number_1"
                                  )

    if scheduler=="dask":
        bag_or_rdd = bag_or_rdd.map(kill_one)
    else:
        # TODO  - kill_one map call here fails while detrend using 
        # the same synatx does not.  Same function works in spark
        # mysterious problem I'm punting while I write additional tests
        bag_or_rdd = bag_or_rdd.map(lambda d : kill_one(d))
        #from mspasspy.algorithms.signals import detrend

    newwfidslist = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=True,
                                   collection=collection,
                                   data_tag="save_explicit_kill",
                                   scheduler=scheduler,
                                   )
    assert len(newwfidslist)==number_atomic_wf
    # kills are returned as None - this function counts not none values
    assert count_valid_ids(newwfidslist)==(number_atomic_wf-1)
    n = db[collection].count_documents({"data_tag" : "save_explicit_kill"})
    assert n==(number_atomic_wf-1)
    n = db["cemetery"].count_documents({})
    assert n==1
    doc=db["cemetery"].find_one()

    assert "tombstone" in doc
    assert "logdata" in doc
    # Better to bulldoze the cemetery before continuing
    db.drop_collection("cemetery")
    
    
    # Exactly the same as above but using cremate option. 
    # only difference should be that creation leaves no cemetery document.
    bag_or_rdd = read_distributed_data(db,
                                  collection=collection,
                                  scheduler=scheduler,
                                  normalize=nrmlist,
                                  npartitions=number_partitions,
                                  spark_context=context,
                                  data_tag="save_number_1"
                                  )

    if scheduler=="dask":
        bag_or_rdd = bag_or_rdd.map(kill_one)
    else:
        bag_or_rdd = bag_or_rdd.map(lambda d : kill_one(d))
    newwfidslist = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=True,
                                   collection=collection,
                                   data_tag="save_explicit_kill_w_cremation",
                                   scheduler=scheduler,
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
                                  scheduler=scheduler,
                                  normalize=nrmlist,
                                  npartitions=number_partitions,
                                  spark_context=context,
                                  data_tag="save_number_1"
                                  )    
    if scheduler=="dask":
        bag_or_rdd = bag_or_rdd.map(massacre)
    else:
        bag_or_rdd = bag_or_rdd.map(lambda d : massacre(d))

    newwfidslist = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=True,
                                   collection=collection,
                                   data_tag="save_kill_all",
                                   scheduler=scheduler,
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
                                  scheduler=scheduler,
                                  normalize=nrmlist,
                                  npartitions=number_partitions,
                                  spark_context=context,
                                  data_tag="save_number_1"
                                  )

    if scheduler=="dask":
        bag_or_rdd = bag_or_rdd.map(set_one_invalid)
    else:
        bag_or_rdd = bag_or_rdd.map(lambda d : set_one_invalid(d))

    newwfidslist = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=True,
                                   mode="pedantic",
                                   collection=collection,
                                   data_tag="save_pedantic_kill",
                                   scheduler=scheduler,
                                   )
    assert len(newwfidslist)==number_atomic_wf
    # kills are returned as None - this function counts not none values
    assert count_valid_ids(newwfidslist)==(number_atomic_wf-1)
    n = db[collection].count_documents({"data_tag" : "save_pedantic_kill"})
    assert n==(number_atomic_wf-1)
    n = db["cemetery"].count_documents({})
    assert n==1
    doc=db["cemetery"].find_one()
    assert "tombstone" in doc
    assert "logdata" in doc
    # Better to bulldoze the cemetery before continuing
    db.drop_collection("cemetery")
    
    # Repeat with cremate option set True
    # Only difference should be that it leaves no cemetery document
    bag_or_rdd = read_distributed_data(db,
                                  collection=collection,
                                  scheduler=scheduler,
                                  normalize=nrmlist,
                                  npartitions=number_partitions,
                                  spark_context=context,
                                  data_tag="save_number_1"
                                  )
    if scheduler=="dask":
        bag_or_rdd = bag_or_rdd.map(set_one_invalid)
    else:
        bag_or_rdd = bag_or_rdd.map(lambda d : set_one_invalid(d))
    newwfidslist = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=True,
                                   mode="pedantic",
                                   collection=collection,
                                   data_tag="save_pedantic_kill_w_cremation",
                                   scheduler=scheduler,
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
    
    # test writing to files.
    # This implementation uses a new feature where file anmes are 
    # expected to be defined with dir and dfile.  We do that with a 
    # map; operator and a small function defined above
    data_tag = 'atomic_file_save'
    bag_or_rdd = read_distributed_data(db,
                                  collection=collection,
                                  scheduler=scheduler,
                                  normalize=nrmlist,
                                  spark_context=context,
                                  data_tag="save_number_1"
                                  )
    if scheduler=="dask":
        bag_or_rdd = bag_or_rdd.map(set_dir_dfile_atomic)
    else:
        bag_or_rdd = bag_or_rdd.map(lambda d : set_dir_dfile_atomic(d))
    newwfidslist = write_distributed_data(bag_or_rdd, 
                                   db,
                                   storage_mode="file",
                                   data_are_atomic=True,
                                   collection=collection,
                                   data_tag=data_tag,
                                   scheduler=scheduler,
                                   )
    query = {'data_tag' : data_tag}
    n = db[collection].count_documents(query)
    assert n == number_atomic_wf
    cursor=db[collection].find(query)
    for doc in cursor:
        assert 'dir' in doc
        assert 'dfile' in doc
        assert 'foff' in doc
        assert 'storage_mode' in doc
        assert doc['storage_mode']=='file'
        
    # verify we can read these back in 
    bag_or_rdd = read_distributed_data(db,
                                  collection=collection,
                                  scheduler=scheduler,
                                  normalize=nrmlist,
                                  spark_context=context,
                                  data_tag=data_tag,
                                  )
    if scheduler=="dask":
        wflist = bag_or_rdd.compute()
    else:
        wflist = bag_or_rdd.collect()
    assert len(wflist)==number_atomic_wf
    for d in wflist:
        assert d.live
    
    
def get_srclist_by_tag(db,data_tag)->list:
    """
    Small helper used by ensemble test to return list of source_id values
    defined by a data_tag value.   Used because generators both write 
    to source collection and we use a tag to define which go with which 
    type of wf data.
    """
    query={'data_tag' : data_tag}
    cursor=db.source.find(query)
    srcid_list=[]
    for doc in cursor:
        srcid=doc['_id']
        srcid_list.append(srcid)
    return srcid_list
        
@pytest.mark.parametrize("scheduler,collection", 
                         [("dask","wf_TimeSeries"),
                          ("dask","wf_Seismogram"),
                          ("spark","wf_TimeSeries"),
                          ("spark","wf_Seismogram")
                          ])
def test_read_distributed_ensemble(setup_environment,spark,TimeSeriesEnsemble_generator,SeismogramEnsemble_generator,scheduler,collection):
     print("Starting test with scheduler=",scheduler, " and collection=",collection)
     if scheduler=="spark":
         context=spark
     else:
         context=None
         
     # warning src_data_list values must match string set in generators
     if collection=="wf_TimeSeries":
         wfid_list = TimeSeriesEnsemble_generator
         src_data_tag = 'timeseries'
     elif collection=="wf_Seismogram":
         wfid_list = SeismogramEnsemble_generator
         src_data_tag = 'seismogram'
     client = DBClient("localhost")
     db = client.get_database(testdbname)
     # We use source_id in this test to define ensembles.  
     # Used to generate a list of query dictionaries by source_id
     # This small function fetches that list
     srcid_list = get_srclist_by_tag(db, src_data_tag)
     querylist=[]
     for srcid in srcid_list:
         querylist.append({'source_id' : srcid})
     # first test dask reader without normalization.  Save result with 
     # data tag used to read copy back below
     bag_or_rdd = read_distributed_data(querylist,
                                   db,
                                   collection=collection,
                                   scheduler=scheduler,
                                   spark_context=context,
                                   )
     if scheduler=="dask":
         wfdata_list=bag_or_rdd.compute()
     elif scheduler=="spark":
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
     if scheduler=="dask":
         bag_or_rdd_to_merge = dask.bag.from_sequence(srcdocs)
     else:
        bag_or_rdd_to_merge = context.parallelize(srcdocs)

     source_matcher = ObjectIdMatcher(db,
                        "source",
                        attributes_to_load=['lat','lon','depth','time','_id'])
    
     site_matcher = ObjectIdMatcher(db,
                        "site",
                        attributes_to_load=['net','sta','lat','lon','elev','_id'])
     bag_or_rdd = read_distributed_data(querylist,
                                   db,
                                   collection=collection,
                                   scheduler=scheduler,
                                   spark_context=context,
                                   container_to_merge=bag_or_rdd_to_merge,
                                   normalize=[site_matcher],
                                   normalize_ensemble=[source_matcher]
                                   )
     if scheduler=="dask":
         wfdata_list=bag_or_rdd.compute()
     elif scheduler=="spark":
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
                                  scheduler=scheduler,
                                  spark_context=context,
                                  normalize=[site_matcher],
                                  )
     data_tag = "save_number_1"   # made a variable to allow changes to copy code
     wfidlists = write_distributed_data(bag_or_rdd, 
                                    db,
                                    data_are_atomic=False,
                                    collection=collection,
                                    data_tag=data_tag,
                                    scheduler=scheduler,
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
                 
             
@pytest.mark.parametrize("scheduler,collection", 
                        [ ("dask","wf_TimeSeries"),
                         ("dask","wf_Seismogram"),
                         ("spark","wf_TimeSeries"),
                         ("spark","wf_Seismogram"),
                     ])
def test_write_distributed_ensemble(setup_environment,
                                    scheduler,
                                    collection,
                                    spark,
                                    define_kill_one_member,
                                    define_massacre,
                                    define_set_one_invalid_member,
                                    TimeSeriesEnsemble_generator,
                                    SeismogramEnsemble_generator,
                                    ):
    """
    This test is a complement to test_read_distributed_ensemble and 
    test_write_distributed_atomic.   The tests here focus on handlling 
    of dead data during writes.   It assumes atomic level features are 
    covered by test in database and the atomic functions in this file.
    """
    print("Starting test with scheduler=",scheduler, " and collection=",collection)
    if scheduler=="spark":
         context=spark
    else:
         context=None
    if collection=="wf_TimeSeries":
         wfid_list = TimeSeriesEnsemble_generator
         src_data_tag = 'timeseries'
    elif collection=="wf_Seismogram":
         wfid_list = SeismogramEnsemble_generator
         src_data_tag = 'seismogram'
         
    # Use fixtures to define map functions
    # all these test functions are implmented with a yield of the function 
    # name.  That means the lhs of the assignments below set those symbols
    # as functions we use in map operators in tests below
    kill_one_member = define_kill_one_member
    massacre = define_massacre
    set_one_invalid_member = define_set_one_invalid_member
    client = DBClient("localhost")
    db = client.get_database(testdbname)
    # We use source_id in this test to define ensembles.  
    # Used to generate a list of query dictionaries by source_id
    # This small function fetches that list
    srcid_list = get_srclist_by_tag(db, src_data_tag)
    querylist=[]
    for srcid in srcid_list:
         querylist.append({'source_id' : srcid,'data_tag' : {'$exists' : False}})

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
                                 scheduler=scheduler,
                                 spark_context=context,
                                 normalize=[site_matcher],
                                 )
    if scheduler=="dask":
        bag_or_rdd = bag_or_rdd.map(kill_one_member)
    else:
        bag_or_rdd = bag_or_rdd.map(lambda d : kill_one_member(d))
    data_tag = "kill_one_test"   # made a variable to allow changes to copy code
    wfidlists = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=False,
                                   collection=collection,
                                   data_tag=data_tag,
                                   scheduler=scheduler,
                                   )
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
                                 scheduler=scheduler,
                                 spark_context=context,
                                 normalize=[site_matcher],
                                 )
    if scheduler=="dask":
        bag_or_rdd = bag_or_rdd.map(kill_one_member)
    else:
        bag_or_rdd = bag_or_rdd.map(lambda d : kill_one_member(d))
    data_tag = "kill_one_cremation_test"   # made a variable to allow changes to copy code
    wfidlists = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=False,
                                   collection=collection,
                                   data_tag=data_tag,
                                   scheduler=scheduler,
                                   cremate=True,
                                   )

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
                                 scheduler=scheduler,
                                 spark_context=context,
                                 normalize=[site_matcher],
                                 )
    if scheduler=="dask":
        bag_or_rdd = bag_or_rdd.map(massacre)
    else:
        bag_or_rdd = bag_or_rdd.map(lambda d : massacre(d))
    data_tag = "kill_all_test"   # made a variable to allow changes to copy code
    wfidlists = write_distributed_data(bag_or_rdd, 
                                   db,
                                   data_are_atomic=False,
                                   collection=collection,
                                   data_tag=data_tag,
                                   scheduler=scheduler,
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
                                 scheduler=scheduler,
                                 spark_context=context,
                                 normalize=[site_matcher],
                                 )
    if scheduler=="dask":
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
                                   scheduler=scheduler,
                                   )

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
    
    # test writing to files.
    # This implementation uses a new feature where file anmes are 
    # expected to be defined with dir and dfile.  Note that 
    # with ensembles the model in parallel processing, which is tested 
    # here, is to post dir and dfile to the ensemble metadata container.
    # We do that with a map; operator and the same small function used for 
    # the comparable atomic data test
    data_tag = 'ensemble_file_save'
    srcid_list = get_srclist_by_tag(db, src_data_tag)
    # Regeneate querylist and matcher definitions with the same code 
    # as above to reduce state dependency for a small cost
    querylist=[]
    ensemble_doclist=[]
    ensemble_dir = wfdir + "/" + "ensembledata"
    for srcid in srcid_list:
         querylist.append({'source_id' : srcid,'data_tag' : {'$exists' : False}})
         doc=dict()
         doc['source_id']=srcid
         doc['dir'] = ensemble_dir
         doc['dfile'] = str(srcid) + ".dat"
         ensemble_doclist.append(doc)
         
    if scheduler=="dask":
        bag_or_rdd_to_merge = dask.bag.from_sequence(ensemble_doclist)
    else:
        bag_or_rdd_to_merge = context.parallelize(ensemble_doclist)

    source_matcher = ObjectIdMatcher(db,
                       "source",
                       attributes_to_load=['lat','lon','depth','time','_id'])
   
    site_matcher = ObjectIdMatcher(db,
                       "site",
                       attributes_to_load=['net','sta','lat','lon','elev','_id'])
    # Note we created bag_or_rdd_to_merge above that contains dir and dfile 
    # for this test. We also normalize with source_id to make sure the 
    # two uses do not collide
    bag_or_rdd = read_distributed_data(querylist,
                                  db,
                                  collection=collection,
                                  scheduler=scheduler,
                                  spark_context=context,
                                  container_to_merge=bag_or_rdd_to_merge,
                                  normalize=[site_matcher],
                                  normalize_ensemble=[source_matcher],
                                  )

    newwfidslist = write_distributed_data(bag_or_rdd, 
                                   db,
                                   storage_mode="file",
                                   data_are_atomic=False,
                                   collection=collection,
                                   data_tag=data_tag,
                                   scheduler=scheduler,
                                   )
    assert len(newwfidslist) == number_ensembles
    number_wf_expected = number_ensembles*number_ensemble_wf
    query = {'data_tag' : data_tag}
    n = db[collection].count_documents(query)
    assert n == number_wf_expected
    cursor=db[collection].find(query)
    for doc in cursor:
        # ensemble_dir was set above as a relative path but 
        # the writer turns it into an full path.  This test 
        # just verifies the relative path is present in the document
        assert ensemble_dir in doc['dir']
        assert 'dfile' in doc
        assert 'foff' in doc
        assert 'storage_mode' in doc
        assert doc['storage_mode']=='file'
        
    # verify we can read these back in
    # have to redefine querylist to do that
    querylist=[]
    srcdoclist=[]
    for srcid in srcid_list:
         querylist.append({'source_id' : srcid,'data_tag' : data_tag })
         srcdoclist.append({'source_id' : srcid})
    # note we change the container to merge because we don't want dir and 
    # dfile loaded in the ensemble container in this test
    if scheduler=="dask":
        bag_or_rdd_to_merge = dask.bag.from_sequence(srcdoclist)
    else:
        bag_or_rdd_to_merge = context.parallelize(srcdoclist)
    
    bag_or_rdd = read_distributed_data(querylist,
                                  db,
                                  collection=collection,
                                  scheduler=scheduler,
                                  spark_context=context,
                                  container_to_merge=bag_or_rdd_to_merge,
                                  normalize=[site_matcher],
                                  normalize_ensemble=[source_matcher],
                                  )
    if scheduler=="dask":
        enslist = bag_or_rdd.compute()
    else:
        enslist = bag_or_rdd.collect()
    assert len(enslist)==number_ensembles
    for ens in enslist:
        assert ens.live
        assert ens['source_id'] in srcid_list
        assert ens.is_defined('source_lat')
        assert len(ens.member)==number_ensemble_wf
        for d in ens.member:
            assert d.live
            assert d.is_defined('foff')
            assert d.is_defined('dir')
            assert d.is_defined('dfile')
            assert d['storage_mode']=='file'
        
def test_read_error_handlers(atomic_time_series_generator):
    atomic_time_series_generator
    client = DBClient("localhost")
    db = client.get_database(testdbname)
    
    # now test error handlers.  First, test reade error handlers
    # illegal value for scheduler argument
    with pytest.raises(ValueError, match="Unsupported value for scheduler"):
        mybag = read_distributed_data(db,
                                      collection="wf_TimeSeries",
                                      scheduler="illegal_scheduler",
                                      )
    # illegal value for db argument when using dataframe input
    cursor=db.wf_TimeSeries.find({})
    df = read_to_dataframe(db,cursor)
    with pytest.raises(TypeError,match="Illegal type"):
        mybag = read_distributed_data(df,
                                      db=True,
                                      collection="wf_TimeSeries",
                                      scheduler="dask",
                                      )
    # Defaulted (none) value for db with dataframe input produces a differnt exeception
    with pytest.raises(TypeError,match="An instance of Database class is required"):
        mybag = read_distributed_data(df,
                                      db=None,
                                      collection="wf_TimeSeries",
                                      scheduler="dask",
                                      )
    # this is illegal input for arg0 test - message match is a bit cryptic
    with pytest.raises(TypeError,match="Must be a"):
        mybag = read_distributed_data(float(2),
                                      collection="wf_TimeSeries",
                                      scheduler="dask",
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
    scheduler='dask' 
    
    # Used repeatedly below.  that works for these tests because 
    # mybag is never altered because we test only handlers in 
    # argument test section at the top of the function.
    # if tests for errors returned from inside function are added that
    # approach will not work
    mybag = read_distributed_data(db,
                                  collection=collection,
                                  scheduler=scheduler,
                                  )
    with pytest.raises(TypeError,match="required arg1"):
        mybag = write_distributed_data(mybag,"wrong type - I should be a Database")

    with pytest.raises(TypeError,match="Unsupported storage_mode"):
        mybag = write_distributed_data(mybag,db,storage_mode='illegal_storage_mode')
        
    with pytest.raises(ValueError,match="Illegal value of mode"):
        mybag = write_distributed_data(mybag,db,mode='illegal_mode')
        
    with pytest.raises(ValueError,match="Illegal value of scheduler="):
        mybag = write_distributed_data(mybag,db,scheduler='illegal_scheduler')
        
    with pytest.raises(ValueError,match="Illegal value of collection"):
        mybag = write_distributed_data(mybag,db,collection='illegal_collection')
    
    with pytest.raises(ValueError,match="overwrite mode is set True with storage_mode="):
        mybag = write_distributed_data(mybag,db,collection='wf_TimeSeries',overwrite=True,storage_mode="file")
    
    



    
