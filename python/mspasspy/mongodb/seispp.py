#!/usr/bin/env python3
from bson.objectid import ObjectId
import bson.errors
import gridfs
import pickle
#from mspasspy.ccore import MetadataDefinitions
#from mspasspy.ccore import Metadata
#from mspasspy.ccore import MDtype
from mspasspy.ccore import MetadataDefinitions
from mspasspy.ccore import Metadata
from mspasspy.ccore import MDtype
from mspasspy.ccore import MongoDBConverter
from mspasspy.ccore import ErrorLogger
from mspasspy.ccore import ErrorSeverity
def dict2md(d,mdef,elog):
  """
  Function to convert python dict data returned by MongoDB find to mspass::Metadata.

  pymongo returns a python dict container from find queries to any collection.
  Simple type in returned documents can be converted to mspass::Metadata
  that are used as headers in the C++ components of mspass.  This is a core,
  bombproof routine to do that.  It handles supported types and will issue
  warning if asked to convert an unsupported type.

  Args:
      d is the dict created by MongoDB query
      mdef is the MetadataDefinitions object used to validate types against
        keys and types expected by mspass.  If a mismatch occurs the
        function attempts to convert to the type defined in mdef.  If
        a key is missing the data are still converted and a warning is issued.
      elog is expected to be an ErrorLogger object.   It should normally be
        already be a component of the Seismogram or TimeSeries with which it
        is or will be (read) associated.   Any conversions problems in
        this function will create log messages of different severity
        posted to elog.   If elog is not already linked to a Seismogram or
        TimeSeries an empty container must be created and passed AND after
        running if it is not empty it should be added to the data object
        being created.
  Returns:
      Metadata object
  """
  md=Metadata()
  for x in d.keys():
      if(x=='_id'):
        # We assume this is always a MongoDB ObjectId
        y=d[x]
        ys=str(y)
        md.put_string("wfid_string",ys)  # Frozen name in C++ constructors
      else:
        y=d[x]
        # For some strange reason bool must appear before int.  A bool will
        # pass an isinstance test for int a return a false true. Because of
        # the chain progression of if,elseif constructs this can work.
        # Note that is possible because the reverse does not misbehave
        # isinstance of a bool will not return a true if the arg is int
        if(isinstance(y,bool)):
          try:
              mdt=mdef.type(x)
              if(mdt==MDtype.Boolean):
                  md.put_bool(x,y)
              else:
                  elog.log_error("dict2md","Mismatched attribute types\n"
                    + "MongoDB value is a boolean, but schema defined by MetadataDefintions requires something else\n"
                    + "Attribute not copied - no recovery possible\n",
                    ErrorSeverity.Suspect)
          except RuntimeError:
            elog.log_error("dict2md","key="+x+" is not defined in schema defined by MetadataDefintions\n"
              + "Copying to Metadata as a boolean as was stored in MongoDB\n",
              ErrorSeverity.Complaint)
            md.put_bool(x,y)
        elif(isinstance(y,int)):
          try:
              mdt=mdef.type(x)
              # All classes of  int can be directly converted
              if( (mdt==MDtype.Integer) or (mdt==MDtype.Int32) or (mdt==MDtype.Long)
                      or (mdt==MDtype.Int64) ):
                yi=int(y)
                md.put_long(x,yi)
              # output to float is harmless, but will create a warning
              elif( (mdt==MDtype.Real) or (mdt==MDtype.Real32) or (mdt==MDtype.Double)):
                elog.log_error("dict2md","dict2md (Warning):  Mismatched attribute types\n"
                  + "Attribute returned for key="+x+" is an integer but schema demands a float\n"
                  + "Converting internally to a float\n",ErrorSeverity.Complaint)
                yf=float(y)
                md.put_double(x,yf)
              elif(mdt==MDtype.String):
                elog.log_error("dict2md","Mismatched attribute types\n"
                  + "Attribute returned for key="
                  +x
                  +" is an integer but schema demands a string\n"
                  + "Converting internally to a string\n",
                  ErrorSeverity.Complaint)
                ys=str(y)
                md.put_string(x,ys)
              elif(mdt==MDtype.Boolean):
                yi=int(y)  #  Probably unnecessary but safer
                md.put_bool(x,yi)  #  C will make 0 false and anything else true
              else:
                elog.log_error("dict2md","MetadataDefinition returned undefined type",
                        " for key="+x+"\nAttribute dropped\n",ErrorSeverity.Suspect)
          except RuntimeError:
            elog.log_error("dict2md","key="+x+" is not defined in schema defined by MetadataDefintions\n"
             + "Copying to Metadata as an integer as was stored in MongoDB",
             ErrorSeverity.Complaint)
            yi=int(y)
            md.put_long(x,yi)
        elif(isinstance(y,float)):
          try:
              mdt=mdef.type(x)
              if( (mdt==MDtype.Integer) or (mdt==MDtype.Int32) or (mdt==MDtype.Long)
                      or (mdt==MDtype.Int64) ):
                elog.log_error("dict2md","Mismatched attribute types\n"
                 + "Attribute returned for key="+x+" is float but schema demands an int\n"
                 + "Converting internally to a integer",ErrorSeverity.Complaint)
                yi=int(y)
                md.put_long(x,yi)
              # output to float is harmless, but will create a warning
              elif( (mdt==MDtype.Real) or (mdt==MDtype.Real32) or (mdt==MDtype.Double)):
                yf=float(y)
                md.put_double(x,yf)
              elif(mdt==MDtype.String):
                elog.log_error("dict2md","Mismatched attribute types\n"
                 + "Attribute returned for key="+x+" is a float but schema demands a string\n"
                 + "Converting internally to a string",ErrorSeverity.Complaint)
                ys=str(y)
                md.put_string(x,ys)
              elif(mdt==MDtype.Boolean):
                elog.log_error("dict2md","Mismatched attribute types\n"
                 + "Attribute returned for key="+x+" is a float but schema demands a boolean\n"
                 + "Attribute will not be copied - no clear conversion is possible",
                 ErrorSeverity.Suspect)
              else:
                elog.log_error("dict2md","MetadataDefinition returned undefined type",
                        " for key="+x+"Attribute ignored\n",ErrorSeverity.Suspect)
          except RuntimeError:
            elog.log_error("dict2md","key="+x+" is not defined in schema defined by MetadataDefintions\n"
             + "Copying to Metadata as a float as was stored in MongoDB\n",
             ErrorSeverity.Complaint)
            yd=float(y)
            md.put(x,yd)
        elif(isinstance(y,str)):
          try:
              mdt=mdef.type(x)
              if( (mdt==MDtype.Integer) or (mdt==MDtype.Int32) or (mdt==MDtype.Long)
                      or (mdt==MDtype.Int64) ):
                elog.log_error("dict2md","Mismatched attribute types\n"
                 + "Attribute returned for key="+x+" is string but schema demands an int\n"
                 + "Attempting to converting to a integer\n",
                 ErrorSeverity.Complaint)
                try:
                    yi=int(y)
                    md.put_long(x,yi)
                except ValueError:
                    elog.log_error("dict2md","Conversion to int for key ="+x+" failed for string="
                       +y+"found in input dict\n"
                     + "Skipping this attribute",ErrorSeverity.Suspect)
              elif( (mdt==MDtype.Real) or (mdt==MDtype.Real32) or (mdt==MDtype.Double)):
                emess="Mismatched attribute types\n"+\
                  "Attribute returned for key="+x+\
                  " is string but schema demands an int\n"+\
                  "Attempting to converting to a integer\n"
                sev=ErrorSeverity.Complaint
                try:
                    yf=float(y)
                    md.put_double(x,yi)  # use default for sev set above if it worked
                except ValueError:
                    emess+="Conversion to float failed for string="+y\
                     + "Skipping this attribute"
                    sev=ErrorSeverity.Suspect
                finally:
                    elog.log_error("dict2md",emess,sev)
              elif(mdt==MDtype.String):
                md.put_string(x,y)
              elif(mdt==MDtype.Boolean):
                emess="dict2md (Warning):  Mismatched attribute types\n"\
                 + "Attribute returned for key="+x+" is a string="+y+" but schema demands a boolean\n"\
                 + "Attempting conversion\n"
                if((y=='false') or (y=='FALSE') or (y=='0') ):
                    md.put_bool(x,0)
                    emess+="Parsed as a false and saving as such\n"
                else:
                    md.put_bool(x,1)
                    emess+="Assumed true"
                elog.log_error("dict2md",emess,ErrorSeverity.Complaint)
              else:
                elog.log_error("dict2md","dict2md(Warning):  MetadataDefinition returned undefined type",
                        " for key="+x+"\nAttribute dropped\n",
                        ErrorSeverity.Complaint)
          except RuntimeError:
            elog.log_error("dict2md","key="+x+" is not defined in schema defined by MetadataDefintions\n"
             + "Copying to Metadata as a string as was stored in MongoDB",
             ErrorSeverity.Complaint)
            md.put_string(x,y)
        else:
          # python equivalent of sprintf
          emess="data with key="+x+" is unsupported type=%s\n" % type(y)
          emess+="attribute will not be loaded\n"
          elog.log_error("dict2md",emess,ErrorSeverity.Suspect)
  return md
def dbsave_elog(elogcol,oidstr,elog):
    """
    Save error log for a data object.

    Data objects in MsPASS contain an error log object used to post any
    errors handled by processing functions.   These have diffent levels of
    severity.   This function posts each log entry into a document with
    the objectid of the parent posted as a tag.  We only post the object id
    as a simple string and make no assumptions it is valid.   To convert it
    to a valid ObjectID in Mongo would require calls to the ObjectID constuctor.
    We view that as postprocessing problem to handle.

    Args:
      elogcol is assumed to be a handle to the elog collection in MongoDB.
      oidstr is the ObjectID represented as a string.  It is normally pulled from
         Metadata with the key oid_string.
      elog is the error log object to be saved.
    Return:  List of ObjectID of inserted 
    """
    n=elog.size()
    if(n==0):
        return
    errs=elog.get_error_log()
    jobid=elog.get_job_id()
    docentry={'job_id':jobid}
    oidlst=[]
    for i in range(n):
        x=errs[i]
        docentry['algorithm']=x.algorithm
        docentry['badness']=str(x.badness)
        docentry['error_message']=x.message
        docentry['process_id']=x.p_id
        docentry['wfid_string']=oidstr
        try:
            oid=elogcol.insert_one(docentry)
            oidlst.append(oid)
        except:
            raise RuntimeError("dbsave_elog:  failure inserting error messages to elog collection")
    return oidlst
def save_data3C_to_dfile(d):
    """
    Save a Seismogram object as a pure binary dump of the sample data
    in native (Fortran) order.   The file name to write is derived from
    dir and dfile in the usual way, but frozen to unix / separator.
    Opens the file and ALWAYS appends data to the end of the file.

    Args: d is a Seismogram object for whose data is to be saved

    Returns:  -1 if failure.  Position of first data sample (foff) for success
    Exception:  Any io failures will be trapped and posted to the elog area of
      the object d.   Caller should test for negative return and post the error
      to the database to help debug data problems.  
    """
    try:
        dir=d.get_string('dir')
        dfile=d.get_string('dfile')
    except:
        d.elog.log_error("save_data3C_to_dfile",
            "Data missing dir and/or dfile - sample data were not saved",
            ErrorSeverity.Invalid)
        return -1 
    fname=dir+"/"+dfile
    try:
        fh=open(fname,mode='r+b')
        foff=fh.seek(0,2)
        # We convert the sample data to a bytearray (bytes is just an
        # immutable bytearray) to allow raw writes.  This seems to works
        # because u is a buffer object.   Seems a necessary evil because
        # pybind11 wrappers and pickle are messy.  This seems a clean
        # solution for a minimal cose (making a copy before write)
        ub=bytes(d.u)
    except:
        d.elog.log_error("save_data3C_to_dfile",
            "IO error writing data to file="+fname,
            ErrorSeverity.Invalid)
        return -1
    else:
        fh.write(ub)
    finally:
        fh.close()
        return(foff)
def save_data3C_to_gridfs(db,d):
    """
    Save a Seismogram object sample data to MongoDB gridfs_wf collection.
    Use this method for saving a Seismogram inside MongoDB.   This is
    the recommended mode for anything but data to be exported or data that
    is expected to remain static.   External files are subject to several
    issues to beware of before using them:  (1) they are subject to damage
    by other processes/program, (2) updates are nearly impossible without
    stranding (potentially large quanties) of data in the middle of files or
    corrupting a file with a careless insert, and (3) when the number of files
    gets large managing them becomes difficult.

    Args:
        db is a database handle returned by the MongodB.client object
        d is the Seismogram to be saved
    Return:
        object_id of the document used to store the data in gridfs
        -1 if something failed.  In that condition a generic error message
           is posted to elog.    Caller should dump elog only after 
           trying to do this write to preserve the log
    """
    try:
        col=db.gridfs_wf
        gfsh=gridfs.GridFS(col)
        ub=bytes(d.u)
        # pickle dumps returns its result as a byte stream - dump (without the s)
        # used in file writer writes to a file
        file_id = gfsh(pickle.dumps(ub))
    except:
        d.elog.log_error("save_data3C_to_gridfs","IO Error",
                         ErrorSeverity.Invalid)
        return -1
    else:
        return file_id
def read_data3C_from_gridfs(db,oidstr,mdef):
    """
    Reads a single Seismogram object from MongoDB defined by the ObjectId in
    wf collection.  Assumes the wf collection has a wfid (stored as a string)
    that can can be used to locate the sample data in the gridfs_wf collection
    using it's ObjectId.

    Args:
        db is the database (output of MongoDBClient) from which to read the data
    """
    # First find the document in wf collection defined by oidstr
    try:
        col=db.wf
        oid=ObjectId(oidstr)  #Intentionally assume oidstr is valid
        findkey={'_id':oid} 
        mddata=col.find_one(findkey)
        # We have to initalize wfid to empty to keep it in scope below
        wfid=ObjectId()
        wfidstr=""
        try:
            wfidstr=mddata['wfid_string']
            wfid=ObjectId(wfidstr)
        except RuntimeError:
            d=Seismogram()
            d.elog.log_error("read_data3C_from_grifs",
              "Required parameter wfid_string is not present in document with oid="+oidstr,
              ErrorSeverity.Invalid)
            return d
        except bson.errors.InvalidId:
            d=Seismogram()
            d.elog.log_error("read_data3C_from_grifs",
                "ObjectId string="+wfidstr+" appears to not be a valid objectid",
                ErrorSeverity.Invalid)
            return d
        md=dict2md(mddata,mdef)
        # Now we need to build an empty BasicTimeSeries object to be used
        # to construct our working Seismogram
        bts=BasicTimeSeries()
        bts.ns=mddata['npts']
        bts.t0=mddata['starttime']
        bts.dt=mddata['delta']
        # We construct the Seismogram now and deal with the time standard
        # issue next - easier for error handling
        d=Seismogram(bts,md,ErrorLogger())
        # Before finishing we have to handle the unusual issue in mspass
        # of handling relative and absolute time.  This is complicated by
        # needing the distinction been data that were born relative versus
        # becoming relative from absolute from a time shift.   This section
        # handles that in a robust way.   First, if the Metadata extracted from
        # MongoDB don't have the time standard defined, we assume UTC.
        try:
            trefstr=md.get_string('time_standard')
        except RuntimeError:
            d.tref=TimeReferenceType.UTC
            d.elog.log_error("read_data3C_from_grifs",
                "string attribute time_standard was not defined - defaulting to UTC",
                ErrorSeverity.Complaint)
        if(trefstr=='relative'):
            d.tref=TimeReferenceType.relative
            try:
                t0shift=d.get_double('t0_shift')
                d.force_t0_shift(t0shift)
            except:
                elog("read_data3C_from_gridfs","read_data3C_from_gridfs(WARNING):  ",
                     "Data are marked relative but t0_shift is not defined",
                     ErrorSeverity.Suspect)
        else:
            # we intentionally are loose on what trefstr is - default to utc this way
            d.tref=TimeReferenceType.UTC
        # Now try to retrieve the sample data.  Here we use pickle to 
        # retrieve the raw bits as a bytearray and then put double in the 
        # d.u matrix. 
        fscol=db.gridfs_wf
        gfsh=gridfs.GridFS(fscol)
        if(gfsh.exists(wfid)):
            fh=gfsh.find_one(wfid)
        else:
            d.elog.log_error("read_data3C_from_gridfs",
                "No match found for id string ="+wfidstr+" in gridfs_wf collection",
                ErrorSeverity.Invalid)
        return d
    except:
        d.elog.log_error("read_data3C_from_gridfs",
            "Something threw an unexpected exception\n",
            ErrorSeverity.Invalid)

def dbsave3C(db,d,mc,smode="file",mmode="save"):
    """
    Function to save mspass::Seismogram objects in MongoDB.

    This is a core method to save Seismogram objects in MongoDB.   It uses a
     feature in the C library (MongoDBConverter) along with capabilities built
    into the data object to add two important features:  (1) we can do pure
    updates to database attributes for pure Metadata procedures as well as full
    writes of new data, and (2) Seismogram has an error log feature that is
    dumped to a separate document (elog) if it has any entries.   Any data
    with sever errors are silently dropped assuming the user will use the
    error log document to backtrack problems.

    Args:
    db  MongoDB database handle.  This function will immediately attempt to
        open a connection to the wf and elog collections.  An assumption of
        that algorithm is that doing so is lightweight and the simplification of
        a single argument is preferable to requiring two args that have to be
        checked for consistency.  If you don't want to clobber an existing
        database just create an empty scratch db before calling this
        function for the first time.
    d   Seismogram object to be saved.  Not if d is marked dead (live false)
        the function attempts to write an entry in elog to save the error
        messages posted for that seismogram.
    mc   MongoDBConverter object created for schema used by d
    smode - mnemonic for SamplelMODE.   Options are currently supported:
       (1) 'file' (default) - use the dir and dfile attributes to write sample
         data as a raw dump with fwrite.  File is ALWAYS appended so user
         can either change dir and/or defile and write to a new file or
         append to the parent data.   The function will fail if dir or
         dfile are not defined in this mode.
       (2) 'gridfs' - data are stored internally in MongoDB's gridfs system
       (3) 'unchanged' - do not save the data.  This mode is required when mmode
         is set to updatemd (used for pure Metadata manipulations for efficiency)
    mmode - mnemonic for MetadataMODE.   Supported options are:
       (1) 'save' - contents are saved dropping all marked readonly (default)
       (2) 'saveall' - all Metadata attributes are saved even if marked readonly
         (most useful for temporary data saved inside a job stream)
       (3) 'updatemd' - run an update to the document of Metadata that have
         changed.  Nothing else is altered in this case. If smode is not set
         unchanged the function will throw a RuntimeError exception in
         the mode.  Similarly, if the ObjectID was set invalid, which is
         used internally whenever sample data are altered, the function will
         abort with a RuntimeError exception.
       (4) 'updateall' - both Metadata and sample data will be updated.  Note
         this mode should not be used if smode is set to 'file' as it is
         nearly guaranteed to create inaccessible holes in files.  A
         warning message is posted in this situation, but the program
         will blunder on.


    Returns:
        Number of errors posted to ErrorLogger and saved in the database
    Exception:  
        should be surrounded by a RuntimeError exception handler.  The function
        will abort with several illegal argument combinations
    """
    # First we do a series of sanity checks to avoid writing garbage
    error_count=0
    try:
        if( not ((smode=='file') or (smode=='gridfs') or (smode=='unchanged'))):
            raise RuntimeError('dbsave3C:  illegal value for smode='+smode)
        if( not ((mmode=='save') or (mmode=='saveall') or (mmode=='updatemd')
           or (mmode=='updateall') ) ):
            raise RuntimeError('dbsave3C:  illegal value for mmode='+mmode)
        if( (mmode=='updatemd') and (smode=='unchanged')):
            raise RuntimeError('dbsave3C:  Illegal combination of mmode and smode - run help(dbsav3C)')
        if( (mmode=='updateall')and(smode=='file')):
            d.elog.log_error('dbsave3C','mmode set to updateall for file mode output\n'\
                + 'This will may cause stranded data in existing files\n'\
                + 'Consider using smode set to gridfs',ErrorSeverity.Informational)
            error_count+=1
        # Now open the wf collections
        wfcol=db.wf
        if(d.live):
            if( (mmode=='save') or (mmode=='saveall') ):

                if(smode=='file'):
                    foff=save_data3C_to_dfile(d)
                    d.put_long('foff',foff)
                elif(smode=='gridfs'):
                    fileoid=save_data3C_to_gridfs(db,d)
                    d.put_string('wfid_string',fileoid.str())
                else:
                    if(not(smode=='unchanged')):
                      elog("dbsave3C","Unrecognized value for smode="+\
                        smode+" Assumed to be unchanged\n"\
                        "That means only Metadata for these data were saved and sample data were left unchanged\n",
                         ErrorSeverity.Complaint)
                      error_count+=1
                updict={}
                if(mmode=='saveall'):
                    updict=mc.all(d,True)
                else:
                    updict=mc.writeable(d,True)
                # ObjectId is dropped for now, but may want to save str representation
                newid=wfcol.insert_one(updict).inserted_id
                # Because we trap condition of an invalid mmode we can do just an else instead of This
                #elif( (mmode=='updatemd') or (mmode=="updateall")):
            else:
                # Make sure the oid string is valid
                oid=ObjectId()
                try:
                    oid=ObjectId(oidstr)
                except bson.errors.InvalidId:
                    elog("dbsave3C","Error running in updatemd mode\n" +\
                      "ObjectId string="+oidstr+" is not a valid ObjectId string\n" +\
                     "Cannot perform an update - this datum will be not be saved",
                     ErrorSeverity.Invalid)
                    error_count+=1
                else:
                # assume oid is valid, maybe should do a find_one first but for now handle with exception
                    updict={}
                    if(mmode=='updateall'):
                        updict=mc.all(d,True)
                    else:
                        updict=mc.writeable(d,True)
                    ur=wfcol.update_one({'_id': oid},updict)
                    if(ur.modified_count <=0):
                        emess="metadata attribute update failed\n "
                        if(mmode=="updateall"):
                            emess+="Sample data also will not be saved\n"
                        d.elog.log_error("dbsave3C",emess,ErrorSeverity.Invalid)
                        error_count+=1
                    elif(mmode=="updateall"):
                        if(smode=='file'):
                            save_data3C_to_dfile(d)
                        elif(smode=='gridfs'):
                        #BROKEN - this needs to be changed to an update mode
                        # Working on more primitives first, but needs to be fixed
                            save_data3C_to_gridfs(db,d)
                        else:
                            if(not(smode=='unchanged')):
                                elog("dbsave3C","Unrecognized value for smode="+\
                                  smode+" Assumed to be unchanged\n"+\
                                  "That means only Metadata for these data were saved and sample data were left unchanged",
                                  ErrorSeverity.Suspect)
                                error_count+=1

    except:
        # Not sure what of if update_one can throw an exception.  docstring does not say
        elog.d.log_error("dbsave3C","something threw an unexpected exception in update section",
                         ErrorSeverity.Invalid)
        error_count+=1
    finally:
        # always save the error log.  Done before exit in case any of the 
        # python functions posted errors
        elogcol=db.elog
        #oidstr=d.get_string('wfid_string')
        #dbsave_elog(elogcol,oidstr,d.elog)
        # this works as well and simpler - retained above temporarily
        dbsave_elog(elogcol,str(newid),d.elog)
        return error_count
        
