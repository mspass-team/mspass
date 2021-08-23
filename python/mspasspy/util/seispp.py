import os

import yaml
import numpy as np

from mspasspy.ccore.utility import MetadataDefinitions
from mspasspy.ccore.utility import MDtype
from mspasspy.ccore.utility import MsPASSError


def index_data(filebase, db, ext='d3C', verbose=False):
    """
    Import function for data from antelope export_to_mspass.

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

    :param filebase: is the base name of the dataset to be read and indexed.
        The function will look for filebase.yaml for the header data and
        filebase.ext (Arg 3 defaulting to d3C).  
    :param db: is the MongoDB database handler
    :param ext: is the file extension for the sample data (default is 'd3C').
    """
    # This loads default mspass schema
    mdef = MetadataDefinitions()
    yamlfile = filebase+'.yaml'
    fh = open(yamlfile)
    d = yaml.load(fh, Loader=yaml.FullLoader)
    if(verbose):
        print('Read data for ', len(d), ' objects')
    fh.close()
    # Set up to add to wf collection
    # This is not general, but works of this test with mongo running under docker
    collection = db.wf
    dfile = filebase+'.'+ext
    fh = open(dfile)
    # This is needed by the numpy reader
    dtyp = np.dtype('f8')
    dir = os.path.dirname(os.path.realpath(dfile))
    dfile = os.path.basename(os.path.realpath(dfile))
    if(verbose):
        print('Setting dir =', dir, ' and dfile=', dfile, ' for all input')
        print('Make sure this file exists before trying to read these data')
        print('This program only builds the wf collection in the database')
        print('Readers of the raw data will access the sample data from the dir+dfile path')
    for i in range(len(d)):
        pyd = {}   # this is essentially a required python declaration
        # Since the source is assumed an antelope css3.0 database we
        # assume these will be defined.   Relating them back to the original
        # source would be impossible without these in css3.0 so shouldn't be
        # an issue
        if(verbose):
            print('Working on sta=', d[i]['sta'], ' and evid=', d[i]['evid'])
        keys = d[i].keys()
        for k in keys:
            try:
                typ = mdef.type(k)
                if(typ == MDtype.Double or typ == MDtype.Real64 or typ == MDtype.Real32):
                    pyd[k] = float(d[i][k])
                elif(typ == MDtype.Int64 or typ == MDtype.Int32):
                    pyd[k] = int(d[i][k])
                elif(typ == MDtype.String):
                    pyd[k] = str(d[i][k])
                elif(type == MDtype.Boolean):
                    pyd[k] = bool(d[i][k])
                else:
                    # These are not optional - always print these if
                    # this happens to warn user
                    print("Warning(index_data):  undefined type for key=", k)
                    print("attribute will not be copied to database")
            except MsPASSError:
                # as above always print this as a warning
                print("Warning(index_data): key =",
                      k, " is undefined - skipped")

        pyd['dir'] = dir
        pyd['dfile'] = dfile
        ns = pyd['npts']
        ns3c = 3*ns
        foff = fh.tell()
        pyd['foff'] = foff
        wfid = collection.insert_one(pyd).inserted_id
        junk = np.fromfile(fh, dtype=dtyp, count=ns3c)
    if(verbose):
        print("Finished with file=", dfile)
        print("Size of wf collection is now ",
              collection.count_documents({}), " documents")
