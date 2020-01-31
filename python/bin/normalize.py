import pymongo
from math import sqrt

def unique_receivers(wfcol,firstid,tolerance=0.0001,ztol=10.0):
    """
    Works through the return of a find with sort of the (potentially large) 
    list of dict containers of docs to be scanned.   Returns a subset of 
    input that have unique positions defined by the tolerance value.
    tolerance is a simple distance as hypot of the differences in degrees.
    ztol is a tolerance in elevation (default size assumes units of km)

    The algorithm assumes the input list is sorted by lat:lon values.  

    Args: 
      wfcol handle pointing at collection to be queried, sorted, and used for 
        as input the algorithm
      firstid is the initial siteid to use in this pass.  This should
        normally be created by scanning a previously created site collection
        but could be some arbitrarily large number (caution if you go there)
      tolerance - horizontal tolerance to define equalities
      ztol - elevation tolerance
    Return:
     tuple with two elements.  Element 0 will contain a deduced dict of 
     receiver attributes with unique locations.  After cross-checks it is 
     intended to be posted to MongoDB with update or insert.  Element 1 
     of a list of tuples with 0 containing the ObjectId of the parent 
     waveform (from wfcol) and 1 containing the unique siteid adetermined by 
     this algorithm.  
    """
    # this is a two key sort and find combined
    # WARNING:  google searches can yield comfusing exmaples.  pymongo 
    # syntax is deceptively different from mongo shell - don't follow a 
    # mongodb shell example
    # good source: https://kite.com/python/docs/pymongo.cursor.Cursor.sort
    allwf=wfcol.find().sort([('site_lat',pymongo.ASCENDING),
                     ('site_lon',pymongo.ASCENDING)])
    # note the cursor object allwf may need a call to the rewind method once
    # this loop finishes
    siteid=firstid
    count=0
    unique_coords={}
    idlist=[]
    firstflag=True
    for doc in allwf:
        # for unknown reasons even though doc is a dict object operator []
        # does not function and we need to use the get method
        lat=doc.get('site_lat')
        lon=doc.get('site_lon')
        elev=doc.get('site_lon')
        oid=doc.get('_id')
        if((lat==None) or (lon==None) or (elev==None)):
            print('coordinate key lookup failed for document number ',count)
            print('Document will be skipped')
        else:
            if(firstflag):
                idlist.append((oid,siteid))
                m0={}
                m0['siteid']=siteid
                m0['site_lat']=lat
                m0['site_lon']=lon
                m0['site_elev']=elev
                unique_coords[siteid]=m0
                lat0=lat
                lon0=lon
                elev0=elev
                firstflag=False
            else:
                dlat=lat-lat0
                dlon=lon-lon0
                delev=elev-elev0
                dr=sqrt(dlat*dlat+dlon*dlon)
                if( (dr>tolerance) or (delev>ztol)):
                    siteid+=1
                    m={}
                    m['siteid']=siteid
                    m['site_lat']=lat
                    m['site_lon']=lon
                    m['site_elev']=elev
                    unique_coords[siteid]=m
                    lat0=lat
                    lon0=lon
                    elev0=elev
                idlist.append((oid,siteid))
        count+=1
    return(idlist,unique_coords)
def load_site_wf(db,verbose=False):
    """
    Scans entire wf collection, extracts unique receiver locations, and 
    inserts the unique set into the site collection.   This function 
    is intended to be run on a wf collection of newly imported data 
    created by the antelope contrib program extract_events, run through 
    export_to_mspass, and then loaded to mongdb with dbsave_indexdata found
    in mspasspy.importer.   

    The algorithm is simple and should only be used in a context where 
    the wf collection matches the same assumptions made here.   That is:
    1.  Each wf document has receiver coordinates defined with the mspass
        names of: site_lon, site_lat, and site_elev.
    2.  The scaling defining if a station is 'close' to another is 
        frozen at 0.0001 degrees.  That comes from the highest precision
        digit for coordinates stored in Antelope.   If that is not 
        appropriate the function default can easily be changed for 
        a special applciation.

    The algorithm constructs a cross reference to site table using the 
    ObjectId of each entry it creates in site.  The string represention 
    of the matching doc in site to each wf entry is defined by the 
    key oid_site.  The cross-reference value for oid_site is placed 
    in each wf document.   

    Args:
      db - is the database handled returned from a call to MongoClient
           (the function will internally look up the wf and site collections)
      verbose - when true will echo some information about number of entries
          found and added to each table (default is false).

    """
    # We have to first query the site collection to get the largest value 
    # of the simple integer staid.   A couple true incantations to pymongo
    # are needed to get that - follow
    sitecol=db.site
    nsite=sitecol.find().count() # Kosher way to get size of the collection
    if(nsite==0):
        startid=0
    else:
        maxcur=sitecol.find().sort([('siteid',pymongo.DESCENDING)]).limit(1)
        maxcur.rewind()   # may not be necessary but near zero cost
        maxdoc=maxcur[0]
        maxsid=maxdoc['siteid']
        startid=maxsid+1
    wfcol=db.wf
    x=unique_receivers(wfcol,startid)
    oidxref=x[0]
    idmap=x[1]
    if(verbose):
        print("load_site_wf:  Found ",len(idmap),\
            " unique receiver locations. Adding to site collection")
    sitexref={}
    for b in idmap:
        sid=sitecol.insert_one(idmap[b])
    # Now we put sitid integer value and the ObjectId in sitecol into 
    # each wf collection document we match - should be all and assume that
    # Note for consistency and to allow the attribute to be passed to 
    # Metadata we convert to string representation
    count=1
    for a in oidxref:
        sm=idmap[a[1]]
        siteid=a[1]
        oid_site=str(sm['_id'])
        updict={'oid_site':oid_site}
        f=wfcol.update_one({'_id':a[0]},{'$set':updict})
        count+=1
    if(verbose):
        print("load_site_wf:  updated site cross reference data in ",count,
            " documents of wf collection")
