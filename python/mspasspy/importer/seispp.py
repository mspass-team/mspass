#!/usr/bin/env python3
import os
import yaml
from pymongo import MongoClient
from mspasspy import MetadataDefinitions
from mspasspy import MDtype
import numpy as np
def dbsave_indexdata(filebase,dbname,ext='d3C'):
    """
    This function is an import function for Seismogram objects created 
    by the antelope program export_to_mspass.  That program writes 
    header data as a yaml file and the sample data as a raw binary 
    fwrite of the data matrix (stored in fortran order but written 
    as a contiguous block of 3*npts (number of samples) double values. 
    This function parses the yaml file and adds three critical metadata
    entries:  dfile, dir, and foff. To get foff values the function 
    reads the binary data file and gets foff values by calls to tell.  
    It then writes these entries into MongoDB in the wf collection
    of a database.   Readers that want to read this raw data will 
    need to use dir, dfile, and foff to find the right file and read point.

    Args:
      filename is the base name of the dataset to be read and indexed.
        The function will look for filename.yaml for the header data and
        filename.ext (Arg 3 defaulting to d3C).  
      dbname is the database name MongoDB will use to write the data 
        read.
      ext is the file extension for the sample data (default is 'd3C').
    """
    # This loads default mspass schema 
    mdef=MetadataDefinitions()
    yamlfile=filebase+'.yaml'
    fh=open(yamlfile)
    d=yaml.load(fh,Loader=yaml.FullLoader)
    print('Read data for ',len(d),' objects')
    fh.close()
    # Set up to add to wf collection
    # This is not general, but works of this test with mongo running under docker
    client=MongoClient('localhost',27017)
    db=client[dbname]
    collection=db.wf
    dfile=filebase+'.'+ext
    fh=open(dfile)
    # This is needed by the numpy reader 
    dtyp=np.dtype('f8')
    dir=os.path.dirname(os.path.realpath(dfile))
    print('Setting dir =',dir,' and dfile=',dfile,' for all input')
    print('Make sure this file exists before trying to read these data')
    print('This program only builds the wf collection in the database')
    print('Readers of the raw data will access the sample data from the dir+dfile path')
    for i in range(len(d)):
        pyd={}   # this is essentially a required python declaration 
        # Since the source is assumed an antelope css3.0 database we
        # assume these will be defined.   Relating them back to the original
        # source would be impossible without these in css3.0 so shouldn't be 
        # an issue
        print('Working on sta=',d[i]['sta'],' and evid=',d[i]['evid'])
        keys=d[i].keys()
        for k in keys:
          try:
            typ=mdef.type(k)
            if(typ==MDtype.Double or typ==MDtype.Real64 or typ==MDtype.Real32):
              pyd[k]=float(d[i][k])
            elif(typ==MDtype.Int64 or typ==MDtype.Int32):
              pyd[k]=int(d[i][k])
            elif(typ==MDtype.String):
              pyd[k]=str(d[i][k])
            elif(type==MDtype.Boolean):
              pyd[k]=bool(d[i][k])
            else:
              print("undefined type for key=",k)
              print("attribute will not be copied to database")
          except RuntimeError: 
              print("key =",k," is undefined - skipped")

        pyd['dir']=dir
        pyd['dfile']=dfile
        ns=pyd['npts']
        ns3c=3*ns
        foff=fh.tell()
        pyd['foff']=foff
        wfid=collection.insert_one(pyd).inserted_id
        junk=np.fromfile(fh,dtype=dtyp,count=ns3c)
        print('Inserted to wf with Objectid=',wfid)
    print("Finished with file=",dfile)
    print("Size of wf collection is now ",collection.count()," documents")
