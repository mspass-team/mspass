#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 11:05:47 2021

@author: pavlis
"""
import os
import io
from bson.objectid import ObjectId

from obspy import (read, UTCDateTime)
from mspasspy.ccore.utility import (MsPASSError, AtomicType)
from mspasspy.ccore.io import _mseed_file_indexer
# We need this because we use obspy's reader for importing
# and always convert to mspass data objects
from mspasspy.util.converter import Trace2TimeSeries
#from mspasspy.db.database import Database


def convert_mseed_index(index_record):
    """
    Helper used to convert C++ struct/class mseed_index to a dict
    to use for saving to mongod.  Note loc is only set if it is not
    zero length - consistent with mspass approach
    
    :param index_record:  mseed_index record to convert
    :return: dict containing index data converted to dict.

    """
    o = dict()
    o['sta'] = index_record.sta
    o['net'] = index_record.net
    o['chan'] = index_record.chan
    if len(index_record.loc) > 0:
        o['loc'] = index_record.loc
    o['starttime'] = index_record.starttime
    o['last_packet_time'] = index_record.last_packet_time
    o['foff'] = index_record.foff
    o['nbytes'] = index_record.nbytes
    return o


def index_mseed_file(db, dfile, dir=None, collection='wf_miniseed'):
    """
    This is the first stage import function for handling the import of
    miniseed data.  This function scans a data file defined by a directory
    (dir arg) and dfile (file name) argument.  I builds and index it 
    writes to mongodb in the collection defined by the collection 
    argument (wf_miniseed by default).   The index is bare bones 
    miniseed tags (net, sta, chan, and loc) with a starttime tag.  
    The index is appropriate ONLY if the data on the file are created 
    by concatenating data with packets sorted by net, sta, loc, chan, time
    AND the data are contiguous in time.   The results will be unpredictable 
    if the miniseed packets do not fit that constraint.  The original 
    concept for this function came from the need to handle large files 
    produced by concanentation of miniseed single-channel files created
    by obpsy's mass_downloader.   i.e. the basic model is the input 
    files are assumed to be something comparable to running the unix 
    cat command on a set of single-channel, contingous time sequence files. 
    There are other examples that do the same thing (e.g. antelope's
    miniseed2days).  
    
    We emphasize this function only builds an index - it does not 
    convert any data.   As a result a final warning is that the 
    metadata in the wf_miniseed collection lack some common 
    attributes one might expect. That is, number of samples, 
    sample rate, and endtime.   The number of samples and endtime are 
    not saved because we intentionally do nat fully crack the input 
    file for speed.  miniseed files can (actually usually) contain 
    compressed sample data and the number of samples are not recorded 
    in the packet headers.  Hence, the number of samples in the block 
    and the endtime cannot be computed without decompressing the data.
    To provide some measurre of the time span of the data we define 
    an attribute "last_packet_time" that contains the unix epoch 
    time of the start of the last packet of the file.   How close that
    is to endtime depends mainly on the sample interval.  If a range 
    query is needed use last_packet_time where you would otherwise use 
    endtime. 
    
    Finally, note that cross referencing with the channel and/or 
    source collections should be a common step after building the 
    index with this function.  The reader found elsewhere in this
    module will transfer linking ids (i.e. channel_id and/or source_id) 
    to TimeSeries objects when it reads the data from the files 
    indexed by this function.
    
    Note to parallelize this function put a list of files in a Spark 
    RDD or a Dask bag and parallelize the call the this function.  
    That can work because MongoDB is designed for parallel operations.  
    
    
    :param db:  mspasspy.db.Database object handle to MongoDB
    :param dfile:  file name of data to be indexed.  Asssumed to be 
      the leaf node of the path - i.e. it contains no directory information
      but just the file name.
    :param dir:  directory name.  This can be a relative path from the 
      current directory be be advised it will always be converted to an 
      fully qualified path.  If it is undefined (the default) the function
      assumes the file is in the current working directory and will use 
      the result of the python getcwd command as the directory path 
      stored in the database.  
    :param collection:  is the mongodb collection name to write the 
      index data to.  The default is 'wf_miniseed'.  It should be rare 
      to use anything but the default.
    :exception: This function can throw a range of error types for 
      a long list of possible io issues.   Callers should use a 
      generic handler to avoid aborts in a large job.
    """
    dbh = db[collection]
    # If dir is not define assume current directory.  Otherwise
    # use realpath to make sure the directory is the full path
    # We store the full path in mongodb
    if dir == None:
        odir = os.getcwd()
    else:
        odir = os.path.realpath(dir)
    fname = odir+"/"+dfile
    ind = _mseed_file_indexer(fname)
    for i in ind:
        doc = convert_mseed_index(i)
        doc['dir'] = odir
        doc['dfile'] = dfile
        dbh.insert_one(doc)
        print('saved this doc', doc)


def mseed_tags_match(doc, d):
    """
    Miniseed data have the wired concept of using net:sta:chan:loc and 
    time tags as the unique definition of a block of data.  This function 
    is called by the reader to assure the right block of data is read
    from database saved index.   That can avoid, for example, problems from 
    data being overwritten on disk by modified data.  It returns true 
    only if the net, sta, chan, loc, and starttime values match.  
    Note loc is treated specially because a null loc code is not stored in
    the database index. 
    :param doc:  mongodb doc to be tested against sta
    :param d:  mspass TimeSeries object with data to be tested
    :return: boolean True if the tags all match, false if any do not match
    """
    if doc['sta'] != d['sta']:
        return False
    if doc['net'] != d['net']:
        return False
    if doc['chan'] != d['chan']:
        return False
    if 'loc' in doc:
        if doc['loc'] != d['loc']:
            return False
        else:
            # land here if loc was not set in the db
            if d.is_defined('loc'):
                if len(d['loc']) > 0:
                    return False

    if doc['starttime'] != d['starttime']:
        return False
    return True


def dbread_miniseed(db, id_or_doc, collection='wf_miniseed',
                    do_not_load=['npts', 'delta', 'starttime',
                                 'endtime', 'sampling_rate'],
                    load_history=False):
    """
    This function should be used to read miniseed data previously indexed 
    and defined in a mongodb collection (default wf_miniseed).   The 
    documents in the collection can contain only the index data or 
    extended metadata created, for example, by linking the data against
    the channel and/or source collection.   The reader assumes the 
    files originally indexed are present and visible in the same 
    location of the file system when the indexer was run on them.  
    The user is responsible assuring the paths to all files are valid. 
    For Antelope users the restriction is exactly like wfdisc in CSS3.0.
    
    The function cracks the data in the miniseed file for a single channel 
    of contiguous data defined by the document defined by arg1.  It 
    returns a mspass TimeSeries object representation of that data.   
    The metadata stored in the TimeSeries container is a copy of the 
    contents of the doc passed (or defined by the id passed) plus 
    computed attributes of the TimeSeries not defined in the wf_miniseed 
    collection (notable starttime, number of points, and stampling rate).
    
    The output of this function can be used to initialize the object level 
    history chain for a processing workflow.  If load_history is true the 
    result will be marked as  "RAW" origin with an id defined by the 
    object id of the wf_miniseed record from which it originated. 
    
    :param db:  mspass Database handle
    :param id_or_doc:  as the name implies this arg can be either 
      a MongoDB doc (python dict in pymongo) set in something 
      like a cursor operation or the ObjectId of a document to be 
      retrieved from wf_miniseed (more useful in a parallel workflow 
      where the initial RDD or Bag is defined by a list of ObjectIds).
    :param collection:  collection where the index is expected to be 
      found (default is 'wf_miniseed' and should be used unless you 
      are doing something unusual)
    :param do_not_load:  is a list of attributes in the index collection 
      doc that should not be transferred to the output TimeSeries object. 
      The default excludes attributes that are part of the TimeSeries 
      objects internal attributes (notably npts, delta, startime, endtime, 
      and sampling_rate).
    :param load_history:  when set True the history chain will be initialized
      for the output with the ObjectId of the index record set as the data's
      origin.
    """
    if isinstance(id_or_doc, dict):
        doc = id_or_doc
        data_id = doc['_id']
    elif isinstance(id_or_doc, ObjectId):
        data_id = id_or_doc
        dbh = db[collection]
        doc = dbh.find_one({'_id': id_or_doc})
    else:
        raise MsPASSError('dbload_miniseed',
                          'id_or_doc (arg1) has illegal type - must be either an ObjectId or a dict loaded from MongoDB',
                          'invalid')
    dir = doc['dir']
    dfile = doc['dfile']
    fname = dir+"/"+dfile
    fh = open(fname, 'rb')
    foff = doc['foff']
    fh.seek(foff)
    nbytes = doc['nbytes']
    raw = fh.read(nbytes)
    # This incantation is needed to convert the raw binary data that
    # is the miniseed data to a python "file-like object"
    flh = io.BytesIO(raw)
    st = read(flh)

    fh.close()
    # drop the buffer not to reduce memory use
    del raw

    # st is a "stream" but it only has one member here because we are
    # reading single net,sta,chan,loc grouping defined by the index
    # We only want the Trace object not the stream to convert
    tr = st[0]
    # Now we convert this to a TimeSeries and load other Metadata
    # Note the exclusion copy and the test verifying net,sta,chan,
    # loc, and startime all match
    ts = Trace2TimeSeries(tr)
    if mseed_tags_match(doc, ts):
        for k in doc:
           if k in do_not_load:
              continue
           ts[k] = doc[k]
    else:
       # Long informative message needed here
       message = 'Mismatched miniseed tags between db and data file='+fname+"\n"
       message += 'db tag: '+doc['net']+':'+doc['sta']+':'
       if 'loc' in doc:
          message += doc['loc']
       message += ':'
       message += doc['chan']
       message += str(UTCDateTime(doc['starttime']))
       message += '\n'
       message += 'data tag: '+ts['net']+':'+ts['sta']+':'
       if ts.is_defined('loc'):
          message += ts['loc']
       message += ':'
       message += ts['chan']
       message += ':'
       message += str(UTCDateTime(ts['starttime']))
       message += '\n'
       raise MsPASSError('dbread_miniseed', message, 'Invalid')
    if load_history:
        ts.set_as_origin('dbread_miniseed', 'SETME', str(
            data_id), AtomicType.TIMESERIES, True)
    return ts
