from obspy import UTCDateTime
from mspasspy.ccore.utility import MsPASSError


def channel_report(db, net=None, sta=None, chan=None, loc=None, time=None):
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
    if net == None and sta == None and chan == None and loc == None:
        raise MsPASSError(
            "channel_report usage error:  must specify at least one of sta, net, or loc",
            "Fatal",
        )
    query = dict()
    if net != None:
        query["net"] = net
    if sta != None:
        query["sta"] = sta
    if chan != None:
        query["chan"] = chan
    if loc != None:
        query["loc"] = loc
    if time != None:
        if isinstance(time, UTCDateTime):
            t_to_use = time.timestamp
        else:
            t_to_use = time
        query["starttime"] = {"$lte": t_to_use}
        query["endtime"] = {"$gte": t_to_use}
    dbchan = db.channel
    n = dbchan.count_documents(query)
    if n == 0:
        print("site collection has no documents matching the following query")
        print(query)
        return
    print(
        "{:>4}".format("net"),
        "{:>8}".format("sta"),
        "{:>6}".format("chan"),
        "{:=^6}".format("loc"),
        "latitude longitude starttime endtime",
    )
    curs = dbchan.find(query)
    for doc in curs:
        print(
            "{:>4}".format(doc["net"]),
            "{:>8}".format(doc["sta"]),
            "{:>6}".format(doc["chan"]),
            "{:=^6}".format(doc["loc"]),
            doc["lat"],
            doc["lon"],
            doc["elev"],
            UTCDateTime(doc["starttime"]),
            UTCDateTime(doc["endtime"]),
        )


def site_report(db, net=None, sta=None, loc=None, time=None):
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

    if net == None and sta == None and loc == None:
        raise MsPASSError(
            "site_report usage error:  must specify at least one of sta, net, or loc",
            "Fatal",
        )
    query = dict()
    if net != None:
        query["net"] = net
    if sta != None:
        query["sta"] = sta
    if loc != None:
        query["loc"] = loc
    if time != None:
        if isinstance(time, UTCDateTime):
            t_to_use = time.timestamp
        else:
            t_to_use = time
        query["starttime"] = {"$lte": t_to_use}
        query["endtime"] = {"$gte": t_to_use}
    dbsite = db.site
    n = dbsite.count_documents(query)
    if n == 0:
        print("site collection has no documents matching the following query")
        print(query)
        return
    print(
        "{:>4}".format("net"),
        "{:>8}".format("sta"),
        "{:=^6}".format("loc"),
        "latitude longitude starttime endtime",
    )
    curs = dbsite.find(query)
    for doc in curs:
        print(
            "{:>4}".format(doc["net"]),
            "{:>8}".format(doc["sta"]),
            "{:=^6}".format(doc["loc"]),
            doc["lat"],
            doc["lon"],
            doc["elev"],
            UTCDateTime(doc["starttime"]),
            UTCDateTime(doc["endtime"]),
        )
