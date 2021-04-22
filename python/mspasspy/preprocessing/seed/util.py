import os
import io

import obspy 
from obspy import UTCDateTime

from mspasspy.ccore.utility import MsPASSError
from mspasspy.util.converter import Trace2TimeSeries

def channel_report(db,net=None,sta=None,chan=None,loc=None,time=None):
    """
    Prints a nicely formatted table of information about channel documents
    matching a set of seed codes.

    SEED uses net, sta, chan, and loc codes along with time intervals
    to define a unique set of metadata for a particular channel of
    seismic observatory data.  This function can be used to produce a
    human readable table of data linked to one or more of these keys.
    All queries are exact matches against defined keys for the
    net, sta, chan, and loc arguments.  If a time stamp is given it will
    produce an interval query for records where
    starttime<=time<=endtime.   Omiting any key will cause all values
    for that key to be returned.  e.g. specifying only net, sta, and
    loc will produce a table of all matching net, sta, loc entries
    for all channels and all times (if there are multiple time interval
    documents).

    :param db:  required mspasspy.db.Database handle.
    :param net:  seed network code
    :param sta: seed station code
    :param chan:  seed channel code
    :param loc:  seed location code
    :param time:  time stamp used as described above.  Can be either
      a unix epoch time or a UTCDateTime.

    """
    if net==None and sta==None and chan==None and loc==None:
        raise MsPASSError("channel_report usage error:  must specify at least one of sta, net, or loc",
                          "Fatal")
    query=dict()
    if net != None:
        query['net']=net
    if sta != None:
        query['sta']=sta
    if chan != None:
        query['chan']=chan
    if loc != None:
        query['loc']=loc
    if time != None:
        if isinstance(time,UTCDateTime):
            t_to_use=time.timestamp
        else:
            t_to_use=time
        query['starttime']={'$lte':t_to_use}
        query['endtime']={'$gte':t_to_use}
    dbchan=db.channel
    n=dbchan.count_documents(query)
    if n==0:
        print("site collection has no documents matching the following query")
        print(query)
        return
    print('{:>4}'.format('net'),
              '{:>8}'.format('sta'),
              '{:>6}'.format('chan'),
              '{:=^6}'.format('loc'),
              'latitude longitude starttime endtime')
    curs=dbchan.find(query)
    for doc in curs:
        print('{:>4}'.format(doc['net']),
              '{:>8}'.format(doc['sta']),
              '{:>6}'.format(doc['chan']),
              '{:=^6}'.format(doc['loc']),
              doc['lat'],doc['lon'],doc['elev'],
              UTCDateTime(doc['starttime']),UTCDateTime(doc['endtime']))

def site_report(db,net=None,sta=None,loc=None,time=None):
    """
    Prints a nicely formatted table of information about site documents
    matching a set of seed codes.

    SEED uses net, sta, chan, and loc codes along with time intervals
    to define a unique set of metadata for a particular channel of
    seismic observatory data.  This function can be used to produce a
    human readable table of data linked to one or more of these keys
    save as documents in the MsPASS site collection.   The site
    collection has no information on channels and is used only to
    define the spatial location of a "seismic station".  Hence
    not channel information will be printed by this function since
    there is none in the site collection.

    All queries are exact matches against defined keys for the
    net, sta, and loc arguments.  If a time stamp is given it will
    produce an interval query for records where
    starttime<=time<=endtime.   Omiting any key will cause all values
    for that key to be returned.  e.g. specifying only net and
    stat will produce a table of all matching net and sta entries
    for all location codes and all times (if there are multiple time interval
    documents).

    :param db:  required mspasspy.db.Database handle.
    :param net:  seed network code
    :param sta: seed station code
    :param loc:  seed location code
    :param time:  time stamp used as described above.  Can be either
      a unix epoch time or a UTCDateTime.

    """

    if net==None and sta==None and loc==None:
        raise MsPASSError("site_report usage error:  must specify at least one of sta, net, or loc",
                          "Fatal")
    query=dict()
    if net != None:
        query['net']=net
    if sta != None:
        query['sta']=sta
    if loc != None:
        query['loc']=loc
    if time != None:
        if isinstance(time,UTCDateTime):
            t_to_use=time.timestamp
        else:
            t_to_use=time
        query['starttime']={'$lte':t_to_use}
        query['endtime']={'$gte':t_to_use}
    dbsite=db.site
    n=dbsite.count_documents(query)
    if n==0:
        print("site collection has no documents matching the following query")
        print(query)
        return
    print('{:>4}'.format('net'),
              '{:>8}'.format('sta'),
              '{:=^6}'.format('loc'),
              'latitude longitude starttime endtime')
    curs=dbsite.find(query)
    for doc in curs:
        print('{:>4}'.format(doc['net']),
              '{:>8}'.format(doc['sta']),
              '{:=^6}'.format(doc['loc']),
              doc['lat'],doc['lon'],doc['elev'],
              UTCDateTime(doc['starttime']),UTCDateTime(doc['endtime']))

def _mseed_tags_match(doc, d):
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

def read_miniseed(db, id_or_doc, collection='wf_miniseed',
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
    fname = os.path.join(dir, dfile)
    fh = open(fname, 'rb')
    foff = doc['foff']
    fh.seek(foff)
    nbytes = doc['nbytes']
    raw = fh.read(nbytes)
    # This incantation is needed to convert the raw binary data that
    # is the miniseed data to a python "file-like object"
    flh = io.BytesIO(raw)
    st = obspy.read(flh)

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
    if _mseed_tags_match(doc, ts):
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
