
"""
Tools for connecting to MongoDB.
"""
import math
import pickle
import struct

import gridfs
import pymongo
import bson.errors
import bson.objectid

from mspasspy.ccore import (BasicTimeSeries,
                            Seismogram,
                            dmatrix,
                            TimeReferenceType,
                            ErrorLogger,
                            ErrorSeverity)
from mspasspy.io.converter import dict2Metadata


def _tmatrix_from_md(md):
    """
    Helper function to parse md to build and set a transformation matrix.
    
    A Seismgogram object has an embedded transformation matrix that needs to 
    be set for the object to be properly defined.   In MsPASS we do that 
    through Metadata attributes loaded from MongoDB with special tags U11,U12, etc.
    This function fetches the 9 numbers required to define such a matrix and
    return them in a ccore.dmatrix object.  
    
    :param md:  Metadata assumed to contain U11, U12, etc. attribures
    
    :return: 3x3 transformation matrix extracted from md attributes
    :rtype: dmatrix
    :raise:   Will throw a RunTimeError if any of the tmatrix attributes are not in md
    """
    A=dmatrix(3,3)
    A[0,0]=md.get_double('U11')   # names have fortran indexing but dmatrix is C
    A[1,0]=md.get_double('U21')
    A[2,0]=md.get_double('U31')
    A[0,1]=md.get_double('U12')
    A[1,1]=md.get_double('U22')
    A[2,1]=md.get_double('U32')
    A[0,2]=md.get_double('U13')
    A[1,2]=md.get_double('U23')
    A[2,2]=md.get_double('U33')
    return A
    
def _sync_metadata(d):
    d.put_long('npts',d.ns)
    d.put_double('starttime',d.t0)
    d.put_double('delta',d.dt)
    if(d.tref == TimeReferenceType.Relative):
        d.put_string('time_standard','relative')
    else:
        d.put_string('time_standard','UTC')
    # Because of inheritance we can use this same function for both TimeSeries
    # and Seismogram objects 
    if(isinstance(d,Seismogram)):
        U=d.get_transformation_matrix()
        d.put_double('U11',U[0,0])
        d.put_double('U12',U[0,1])
        d.put_double('U13',U[0,2])
        d.put_double('U21',U[1,0])
        d.put_double('U22',U[1,1])
        d.put_double('U23',U[1,2])
        d.put_double('U31',U[2,0])
        d.put_double('U32',U[2,1])
        d.put_double('U33',U[2,2])

class Database(pymongo.database.Database):
    """
    A MongoDB database handler.

    This is a wrapper around the :class:`~pymongo.database.Database` with
    methods added to handle MsPASS data.
    """
    def load3C(self, oid, mdef, smode='gridfs'):
        """
        Loads a Seismogram object from MongoDB based on ObjectId.
        
        This is a core reader for Seismogram objects with MongoDB.  The unique
        document in the wf collection is selected by ObjectID (oid arg).  
        That oid is used to select a unique waveform document. 
        Samples can be read from system files or from gridfs.  
        
        A string attribute extracted from the database with the key 'storage_mode' is 
        normally used to tell the mode to use to fetch the sample 
        data.   If that attribute is not found the method defined in the smode
        argument will be attempted.   When smode is gridfs, the method will 
        load the Metadata from the wf collection which must include the string attribute
        wfid_string.   It access the sample data from the gridfs_wf 
        collection after creating a buffer space for the sample data.
        when smod is file the sample data is read by a C++ function that
        uses a raw binary read.
        
        This method was designed to never abort.  If the read failed the 
        boolean attribute of Seismogram called 'live' will be set false.  
        Both fatal and nonfatal errors will be posted to the elog member of 
        Seismogram.  Callers should test live and handle fatal and nonfatal 
        errors as appropriate to the algorithm.   
        
        :param oid: is the ObjectId in the wf collection to be read
        :param mdef: is a MetadataDefinitions object used to validate types stored
            in the database against what is expected for a given name.
            In reading this is a necessary cross check to reduce errors
            from incorrect expectations of the contents of a name:value pair
            found in the document associated with this waveform. 
        :param smode: sets the expected method for saving sample data.
            (Metadata are normally stored in a single document of the wf collection)
            Supported values at present are 'file' and 'gridfs' matching 
            allowed values for the storage_mode attribute.  
                
        :Returns:  Seismogram data object loaded.
        :rtype: Seismogram
        :raise:  May throw a RuntimeError exception in one of several
        error conditions.   Nonfatal errors will be posted to the error 
        log on the returned object. 
        """
        try:
            wfcol=self.wf
            findkey={'_id':oid}
            pymd=wfcol.find_one(findkey)
            # We create a temporary ErrorLogger object to hold any 
            # errors encountered in the conversion cross check agains 
            # MetadataDefinitions
            elogtmp=ErrorLogger()
            md=dict2Metadata(pymd,mdef,elogtmp)
            mode=smode
            try:
                smtest=md.get_string('storage_mode')
                mode=smtest
                if(not ((smtest=='gridfs')or(smtest=='file'))):
                    mode=smode
                    elogtmp.log_error("load3C",
                    "Required attribute storage_mode has invalid value="+smtest+\
                    "Using default of "+smode+" passed by argument smode\n",
                    ErrorSeverity.Complaint)
            except:
                elogtmp.log_error("load3C",
                "Required attribute storage_mode not found in document read from db\n" +\
                "Using default of "+smode+" passed by argument smode\n",
                ErrorSeverity.Complaint)
            if(mode=='gridfs'):
                # Like this function this one should never throw an exception 
                # but only post errors to d.elog
                d=self._read_data3C_from_gridfs(md,elogtmp)
                return d
            else:
                try:
                    # This C++ constructor can fail for a variety of reasons
                    # but all return a RuntimeError exception.  I uses 
                    # a generic catch to be safe.  I would like 
                    # to be able to retrieve the what string for the std::exception
                    # to which a RuntimeError is a subclass, 
                    # but that doesn't seem possible.  To fix this
                    # I believe we would need to implement a custom exception 
                    # in pybind11
                    d=Seismogram(md)
                    d.elog=elogtmp
                    return d
                except:
                    derr=Seismogram()
                    derr.elog.log_error("load3c",
                        "Failure in file based constructor\n"+
                        "Most likely problem is that dir and/or defile are invalid\n",
                        ErrorSeverity.Invalid)
                    return derr
                    
        except:
            # Should only land here for an unexpected exception.  To 
            # be consistent with this being equivalent to a noexcept function
            # in C++ we create an empty Seismogram and post message to 
            # it's error log.  
            derr=Seismogram()
            derr.elog.log_error("load3c","Unexpected exception - debug required for a bug fix",
                                ErrorSeverity.Invalid)
            return derr

    def save3C(self, d, mc, smode="gridfs", mmode="save"):
            """
            Save mspass::Seismogram object in MongoDB.

            This is a core method to save Seismogram objects in MongoDB.   It uses a
            feature in the C library (MongoDBConverter) along with capabilities built
            into the data object to add two important features:  (1) we can do pure
            updates to database attributes for pure Metadata procedures as well as full
            writes of new data, and (2) Seismogram has an error log feature that is
            dumped to a separate document (elog) if it has any entries.   Any data
            with sever errors are silently dropped assuming the user will use the
            error log document to backtrack problems.

            This method will immediately attempt to
            open a connection to the wf and elog collections.  An assumption of
            that algorithm is that doing so is lightweight and the simplification of
            a single argument is preferable to requiring two args that have to be
            checked for consistency.  If you don't want to clobber an existing
            database just create an empty scratch database before calling this
            method for the first time.

            :param d: Seismogram object to be saved.  Not if d is marked dead (live false)
                the method attempts to write an entry in elog to save the error
                messages posted for that seismogram.
            :param mc: MongoDBConverter object created for schema used by d
            :param smode: mnemonic for SamplelMODE.   Options are currently supported:
            (1) 'file' - use the dir and dfile attributes to write sample
                data as a raw dump with fwrite.  File is ALWAYS appended so user
                can either change dir and/or defile and write to a new file or
                append to the parent data.   The method will fail if dir or
                dfile are not defined in this mode.
            (2) 'gridfs' - (default) data are stored internally in MongoDB's gridfs system
            (3) 'unchanged' - do not save the data.  This mode is required when mmode
                is set to updatemd (used for pure Metadata manipulations for efficiency)
            :param mmode: mnemonic for MetadataMODE.   Supported options are:
            (1) 'save' - contents are saved dropping all marked readonly (default)
            (2) 'saveall' - all Metadata attributes are saved even if marked readonly
                (most useful for temporary data saved inside a job stream)
            (3) 'updatemd' - run an update to the document of Metadata that have
                changed.  Nothing else is altered in this case. If smode is not set
                unchanged the method will throw a RuntimeError exception in
                the mode.  Similarly, if the ObjectID was set invalid, which is
                used internally whenever sample data are altered, the method will
                abort with a RuntimeError exception.
            (4) 'updateall' - both Metadata and sample data will be updated.  Note
                this mode should not be used if smode is set to 'file' as it is
                nearly guaranteed to create inaccessible holes in files.  A
                warning message is posted in this situation, but the program
                will blunder on.

            :return: Number of errors posted to ErrorLogger and saved in the database
            :rtype: integer
            :raise: should be surrounded by a RuntimeError exception handler.  The method
                can abort with several illegal argument combinations
            """
            # First we do a series of sanity checks to avoid writing garbage
            error_count=0
            try:
                if( not ((smode=='file') or (smode=='gridfs') or (smode=='unchanged'))):
                    raise RuntimeError('save3C:  illegal value for smode='+smode)
                if( not ((mmode=='save') or (mmode=='saveall') or (mmode=='updatemd')
                or (mmode=='updateall') ) ):
                    raise RuntimeError('save3C:  illegal value for mmode='+mmode)
                if( (mmode=='updatemd') and (smode=='unchanged')):
                    raise RuntimeError('save3C:  Illegal combination of mmode and smode - run help(Database.save3C)')
                if( (mmode=='updateall')and(smode=='file')):
                    d.elog.log_error('save3C','mmode set to updateall for file mode output\n'\
                        + 'This will may cause stranded data in existing files\n'\
                        + 'Consider using smode set to gridfs',ErrorSeverity.Informational)
                    error_count+=1
            except RuntimeError:
                raise
            try:
                # Now open the wf collections
                wfcol=self.wf
                if(d.live):
                    #Make sure the stored attributes in a Seismogram are consistent
                    #synced with Metadata as when we save to the database we assume 
                    #use the Metadata attributes to build the update document.
                    _sync_metadata(d)
                    if( (mmode=='save') or (mmode=='saveall') ):
                        if(smode=='file'):
                            foff=self._save_data3C_to_dfile(d)
                            d.put_long('foff',foff)
                            d.put_string('storage_mode','file')
                        elif(smode=='gridfs'):
                            fileoid=self._save_data3C_to_gridfs(d)
                            d.put_string('gridfs_idstr',str(fileoid))
                            d.put_string('storage_mode','gridfs')
                        else:
                            if(not(smode=='unchanged')):
                                d.elog.log_error("save3C","Unrecognized value for smode="+\
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
                        #
                        # insert_one creates a new copy so we need to post the 
                        # new ObjectId
                        d.put_string('wfid_string',str(newid)) 
                    else:
                        # Make sure the oid string is valid
                        oid=bson.objectid.ObjectId()
                        try:
                            oidstr=d.get_string('wfid_string')
                            oid=bson.objectid.ObjectId(oidstr)
                        except RuntimeError:
                            d.elog.log_error("save3C","Error in attempting an update\n" +\
                            "Required key wfid_string, which is a string representation of parent ObjectId, not found\n" +\
                            "Cannot peform an update - updated data will not be saved",
                            ErrorSeverity.Invalid)
                            error_count += 1
                        except bson.errors.InvalidId:
                            d.elog.log_errore("save3C","Error in attempting an update\n" +\
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
                                updict=mc.modified(d,True)
                                # DEBUG
                                print('number changed=',len(updict))
                            if(len(updict)>0):
                                try:
                                    ur=wfcol.update_one({'_id': oid},{'$set':updict})
                                except:
                                    # This perhaps should be a fatal error
                                    d.elog.log_error("save3C",
                                        "Metadata update operation failed with MongoDB\n"+\
                                        "All parts of this Seismogram will be dropped",
                                        ErrorSeverity.Invalid)
                                    error_count+=1
                                    return error_count
                                # This silently skips case when no Metadata were modified
                                # That situation would be common if only the sample 
                                # data were changed and  no metadata operations
                                # were performed
                                if(ur.modified_count <=0):
                                    emess="metadata attribute update failed\n "
                                    if(mmode=="updateall"):
                                        emess+="Sample data also will not be saved\n"
                                        d.elog.log_error("save3C",emess,ErrorSeverity.Invalid)
                                        error_count+=1
                            if(mmode=="updateall"):
                                if(smode=='file'):
                                    self._save_data3C_to_dfile(d)
                                elif(smode=='gridfs'):
                                #BROKEN - this needs to be changed to an update mode
                                # Working on more primitives first, but needs to be fixed
                                    self._save_data3C_to_gridfs(d,update=True)
                                else:
                                    if(not(smode=='unchanged')):
                                        d.elog.log_error("save3C","Unrecognized value for smode="+\
                                        smode+" Assumed to be unchanged\n"+\
                                        "That means only Metadata for these data were saved and sample data were left unchanged",
                                        ErrorSeverity.Suspect)
                                        error_count+=1
            except:
                # Not sure what of if update_one can throw an exception.  docstring does not say
                d.elog.log_error("save3C",
                    "something threw an unexpected exception",
                                ErrorSeverity.Invalid)
                error_count+=1
            finally:
                # always save the error log.  Done before exit in case any of the 
                # python functions posted errors
                oidstr=d.get_string('wfid_string')
                self._save_elog(oidstr,d.elog)
                return error_count

    def _save_elog(self, oidstr, elog):
        """
        Save error log for a data object.

        Data objects in MsPASS contain an error log object used to post any
        errors handled by processing functions.   These have different levels of
        severity.   This method posts each log entry into the elog document with
        the Objectid of the parent posted as a tag.  We only post the object id
        as a simple string and make no assumptions it is valid.   To convert it
        to a valid ObjectID in Mongo would require calls to the ObjectID constuctor.
        We view that as postprocessing problem to handle.

        Args:
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
                oid=self.elog.insert_one(docentry)
                oidlst.append(oid)
            except:
                raise RuntimeError("save_elog:  failure inserting error messages to elog collection")
        return oidlst

    @staticmethod
    def _save_data3C_to_dfile(d):
        """
        Saves sample data as a binary dump of the sample data.

        Save a Seismogram object as a pure binary dump of the sample data
        in native (Fortran) order.   The file name to write is derived from
        dir and dfile in the usual way, but frozen to unix / separator.
        Opens the file and ALWAYS appends data to the end of the file.

        :param: d is a Seismogram object whose data is to be saved

        :returns:  -1 if failure.  Position of first data sample (foff) for success
        :raise:  None. Any io failures will be trapped and posted to the elog area of
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

    def _save_data3C_to_gridfs(self, d, fscol='gridfs_wf', update=False):
        """
        Save a Seismogram object sample data to MongoDB gridfs_wf collection.

        Use this method for saving a Seismogram inside MongoDB.   This is
        the recommended mode for anything but data to be exported or data that
        is expected to remain static.   External files are subject to several
        issues to beware of before using them:  (1) they are subject to damage
        by other processes/program, (2) updates are nearly impossible without
        stranding (potentially large quantities) of data in the middle of files or
        corrupting a file with a careless insert, and (3) when the number of files
        gets large managing them becomes difficult.

        :param d: is the Seismogram to be saved
        :param fscol: is the gridfs collection name to save the data in
            (default is 'gridfs_wf')
        :param update: is a Boolean. When true the existing sample data will be 
            deleted and then replaced by the data in d. When false (default) 
            the data will be saved an given a new ObjectId saved to 
            d with key gridfs_idstr.  
        :return: object_id of the document used to store the data in gridfs
            -1 if something failed.  In that condition a generic error message
            is posted to elog.    Caller should dump elog only after 
            trying to do this write to preserve the log
        :raise: Should never throw an exception, but caller should test and save 
        error log if it is not empty.
        """
        try:
            gfsh=gridfs.GridFS(self,collection=fscol)
            if(update):
                try:
                    ids=d.get_string('gridfs_idstr')
                    oldid=bson.objectid.ObjectId(ids)
                    if(gfsh.exists(oldid)):
                        gfsh.delete(oldid)
                except RuntimeError:
                    d.elog.log_error("save_data3C_to_gridfs",
                    "Error fetching object id defined by key gridfs_idstr",
                    ErrorSeverity.Complaint)
                else:
                    d.elog.log_error("save_data3C_to_gridfs",
                        "GridFS failed to delete data with gridfs_idstr="+ids,
                        ErrorSeverity.Complaint)
            ub=bytes(d.u)
            # pickle dumps returns its result as a byte stream - dump (without the s)
            # used in file writer writes to a file
            file_id = gfsh.put(pickle.dumps(ub))
            d.put_string('gridfs_idstr',str(file_id))
        except:
            d.elog.log_error("save_data3C_to_gridfs","IO Error",
                            ErrorSeverity.Invalid)
            return -1
        else:
            return file_id

    def _read_data3C_from_gridfs(self, md, elogtmp=ErrorLogger(), fscol='gridfs_wf'):
        """
        Load a Seismogram object stored as a gridfs file.

        Constructs a Seismogram object from Metadata and sample data 
        pulled from a MongoDB gridfs document.   The Metadata must contain 
        a string representation of the ObjectId of the document with the 
        key gridfs_idstr.  That string is used to generate a unique ObjectId 
        which is then used to find the unique document containing the sample
        data in the collection called gridfs_wf (optionally can be changed with 
        argument fscol)
        
        This function was designed to never throw an exception but always 
        return some form of Seismogram object. Caller should test the boolean
        'live" attribute of the return. If it is false, it means this function
        failed completely.  The error log may also contain various levels of 
        warning errors posted to it's internal ErrorLogger (elog) object. 
        
        :param md: is the Metadata object used to drive the construction.  This 
            would normally be constructed from a parent document in the wf
            collection using dict2Metadata.  A critical key is the entry gridfs_idstr
            as described above.   Several other key:value pairs are required or
            the function will abort with the result returned as invalid (live=false).
            These are:  npts, starttime, and delta.   time_reference is a special 
            switch for handling UTC versus relative time.   Default is UTC
            but relative time can be handled with the attribure t0_shift.  
            See User Manual for more about this feature.
        :param elogtmp: is an (optional) ErrorLogger object added to the Seismogram 
            object during construction. It's primary use is to preserve any
            warning errors encountered during the construction of md passed
            to the function.   
        :param fscol: is the collection name the function should use to find the 
            gridfs data document
        :return: the Seismogram object requested
        :rtype: Seismogram
        """
        # First make sure we have a valid id string.  No reason to procede if
        # not the case
        try:
            idstr=md.get_string('gridfs_idstr')
        except:
            elogtmp.log_error("read3C_from_gridfs",
                "Required attribute gridfs_idstr is not defined - null Seismogram returned",
                ErrorSeverity.Invalid)
            dbad=Seismogram()
            dbad.elog=elogtmp
            return dbad
        try:
            dataid=bson.objectid.ObjectId(idstr)
        except bson.errors.InvalidId:
            d=Seismogram()
            d.elog=elogtmp
            d.elog.log_error("read_data3C_from_grifs",
                "ObjectId string="+idstr+" appears to not be define a valid objectid",
                ErrorSeverity.Invalid)
            return d
        try:
            # Now we need to build an empty BasicTimeSeries object to be used
            # to construct our working Seismogram
            bts=BasicTimeSeries()
            bts.ns=md.get_long('npts')
            bts.t0=md.get_double('starttime')
            bts.dt=md.get_double('delta')
        except RuntimeError:
            d=Seismogram()
            d.elog.log_error("read_data3C_from_grifs",
                "One of required attributes (npts, starttime, and delta) were not defined",
                ErrorSeverity.Invalid)
            return d
        d=Seismogram(bts,md,elogtmp)
        # Before finishing we have to handle the unusual issue in mspass
        # of handling relative and absolute time.  This is complicated by
        # needing the distinction been data that were born relative versus
        # becoming relative from absolute from a time shift.   This section
        # handles that in a robust way.   First, if the Metadata extracted from
        # MongoDB don't have the time standard defined, we assume UTC.
        try:
            trefstr=md.get_string('time_standard')
            if(trefstr=='relative'):
                d.tref=TimeReferenceType.relative
                try:
                    t0shift=d.get_double('t0_shift')
                    d.force_t0_shift(t0shift)
                except:
                    d.elog.log_error("read_data3C_from_gridfs",
                        "read_data3C_from_gridfs(WARNING):  "+\
                        "Data are marked relative but t0_shift is not defined",
                            ErrorSeverity.Suspect)
        except RuntimeError:
            d.tref=TimeReferenceType.UTC
            d.elog.log_error("read_data3C_from_grifs",
                "string attribute time_standard was not defined - defaulting to UTC",
                ErrorSeverity.Complaint)
        
        else:
            # we intentionally are loose on what trefstr is - default to utc this way
            d.tref=TimeReferenceType.UTC
        # finally need to deal with the transformation matrix
        try:
            A=_tmatrix_from_md(md)
            d.set_transformation_matrix(A)
        except  RuntimeError:
            Iden=dmatrix(3,3)
            Iden.zero()
            for i in range(3):
                Iden[i,i]=1.0
            d.set_transformation_matrix(A)
            d.elog.log_error("read_data3C_from_grifs",
                "Metadata extracted from database are missing transformation matrix definition\n" +
                "Defaulting to identity matrix",ErrorSeverity.Suspect)
        # Now we actually retrieve the sample data.  
        gfsh=gridfs.GridFS(self,collection=fscol)
        # This retrieves only a handle to the file object matching ObjectId=dataid
        # This probably needs an error handler, but the documentation does not 
        # make it clear what happens if the return is null
        fh=gfsh.get(file_id=dataid)
        ub=pickle.load(fh)
        # this sets the format string in the obscure way for struct to 
        # match total number of data points.  These are converted to 
        # a tuple with that many doubles 
        fmt="@%dd" % int(len(ub)/8) 
        x=struct.unpack(fmt,ub)
        # Validate sizes. For now we post a message making the data invalid 
        # and set live false if there is a size mismatch.
        if(len(x)==(3*d.ns)):
            d.u=dmatrix(3,d.ns)
            ii=0
            for i in range(3):
                for j in range(d.ns):
                    d.u[i,j]=x[ii]
        else:
            emess="Size mismatch in sample data.  Number of points in gridfs file=%d but expected %d" \
            % (len(x),(3*d.ns))
            d.elog.log_error("read_data3C_from_gridfs",
                emess,ErrorSeverity.Invalid)
        # Necessary step for efficiency.  Seismogram constructor here 
        # incorrectly marks data copied form metadata object as changed
        # This could lead to unnecessary database transaction with updates
        d.clear_modified()   
        return d

    def _unique_sites(self, firstid, tolerance=0.0001, ztol=10.0):
        """
        Create a dict of unique receiver positions. 

        Works through the return of a find with sort of the (potentially large) 
        list of dict containers of docs to be scanned.   Returns a subset of 
        input that have unique positions defined by the tolerance value.
        tolerance is a simple distance as hypot of the differences in degrees.
        ztol is a tolerance in elevation (default size assumes units of km)

        :param firstid: is the initial site_id to use in this pass.  This should
            normally be created by scanning a previously created site collection
            but could be some arbitrarily large number (caution if you go there)
        :param tolerance:  horizontal tolerance to define equalities
        :param ztol: elevation tolerance
        :return: tuple with two elements.  Element 0 of a list of tuples with 0 
        containing the ObjectId of the parent waveform (from wf collection) and
        1 containing the unique site_id adetermined by 
        this algorithm.  Element 1 will contain a deduced dict of 
        receiver attributes with unique locations.  After cross-checks it is 
        intended to be posted to MongoDB with update or insert.  
        :rtype: tuple with 2 elements (see return for details)
        """
        # this is a two key sort and find combined
        # WARNING:  google searches can yield comfusing exmaples.  pymongo 
        # syntax is deceptively different from mongo shell - don't follow a 
        # MongoDB shell example
        # good source: https://kite.com/python/docs/pymongo.cursor.Cursor.sort
        allwf=self.wf.find().sort([('site_lat',pymongo.ASCENDING),
                        ('site_lon',pymongo.ASCENDING)])
        # note the cursor object allwf may need a call to the rewind method once
        # this loop finishes
        site_id=firstid
        count=0
        unique_coords={}
        idlist=[]
        firstflag=True
        for doc in allwf:
            # for unknown reasons even though doc is a dict object operator []
            # does not function and we need to use the get method
            lat=doc.get('site_lat')
            lon=doc.get('site_lon')
            elev=doc.get('site_elev')
            oid=doc.get('_id')
            if((lat==None) or (lon==None) or (elev==None)):
                print('coordinate key lookup failed for document number ',count)
                print('Document will be skipped')
            else:
                if(firstflag):
                    idlist.append((oid,site_id))
                    m0={}
                    m0['site_id']=site_id
                    m0['site_lat']=lat
                    m0['site_lon']=lon
                    m0['site_elev']=elev
                    unique_coords[site_id]=m0
                    lat0=lat
                    lon0=lon
                    elev0=elev
                    firstflag=False
                else:
                    dlat=lat-lat0
                    dlon=lon-lon0
                    delev=elev-elev0
                    dr=math.sqrt(dlat*dlat+dlon*dlon)
                    if( (dr>tolerance) or (delev>ztol)):
                        site_id+=1
                        m={}
                        m['site_id']=site_id
                        m['site_lat']=lat
                        m['site_lon']=lon
                        m['site_elev']=elev
                        unique_coords[site_id]=m
                        lat0=lat
                        lon0=lon
                        elev0=elev
                    idlist.append((oid,site_id))
            count+=1
        return(idlist,unique_coords)
    
    def normalize_site(self, tolerance=0.0001, ztol=10.0, verbose=False):
        """
        Normalize the site collection. 

        Scans entire wf collection, extracts unique receiver locations, and 
        inserts the unique set into the site collection.   This method 
        is intended to be run on a wf collection of newly imported data 
        created by the antelope contrib program extract_events, run through 
        export_to_mspass, and then loaded to MongoDB with 
        mspasspy.io.seispp.index_data.   

        The algorithm is simple and should only be used in a context where 
        the wf collection matches the same assumptions made here.   That is:
        1.  Each wf document has receiver coordinates defined with the mspass
            names of: site_lon, site_lat, and site_elev.
        2.  The scaling defining if a station is 'close' to another is 
            frozen at 0.0001 degrees.  That comes from the highest precision
            digit for coordinates stored in Antelope.   If that is not 
            appropriate the default can easily be changed for 
            a special applciation.

        The algorithm constructs a cross reference to site table using the 
        ObjectId of each entry it creates in site.  The string represention 
        of the matching doc in site to each wf entry is defined by the 
        key oid_site.  The cross-reference value for oid_site is placed 
        in each wf document.   

        :param tolerance:  horizontal tolerance to define equalities
        :param ztol: elevation tolerance
        :param verbose: when true will echo some information about number of entries
            found and added to each table (default is false).

        """
        # We have to first query the site collection to get the largest value 
        # of the simple integer staid.   A couple true incantations to pymongo
        # are needed to get that - follow
        sitecol=self.site
        nsite=sitecol.find().count() # Kosher way to get size of the collection
        if(nsite==0):
            startid=0
        else:
            maxcur=sitecol.find().sort([('site_id',pymongo.DESCENDING)]).limit(1)
            maxcur.rewind()   # may not be necessary but near zero cost
            maxdoc=maxcur[0]
            maxsid=maxdoc['site_id']
            startid=maxsid+1
            if(verbose):
                print('Existing site_id data found in wf collection\n'+\
                    'Setting initial site_id value to ',startid)
        x=self._unique_sites(startid,tolerance,ztol)
        oidxref=x[0]
        idmap=x[1]
        if(verbose):
            print("normalize_site:  Found ",len(idmap),\
                " unique receiver locations. Adding to site collection")
        sitexref={}
        for b in idmap:
            result=sitecol.insert_one(idmap[b])
            sid=result.inserted_id
            sitexref[b]=sid
        # Now we put sitid integer value and the ObjectId in sitecol into 
        # each wf collection document we match - should be all and assume that
        # Note for consistency and to allow the attribute to be passed to 
        # Metadata we convert to string representation
        count=1
        for a in oidxref:
            site_id=a[1]
            oid_site=sitexref[site_id]
            updict={'oid_site':oid_site}
            updict['site_id']=site_id
            self.wf.update_one({'_id':a[0]},{'$set':updict})
            count+=1
        if(verbose):
            print("normalize_site:  updated site cross reference data in ",count,
                " documents of wf collection")
    
    def _unique_sources(self, firstid, dttol=1250.0, degtol=1.0, drtol=25.0, Vsource=6.0):
        """
        Produce a set of unique coordinates from source coordinates in wf collection.

        Works through the return of a find with sort by time of the (potentially large) 
        list of dict containers of docs to be scanned.   Returns a subset of 
        input that have unique space-time positions defined by three tolerance 
        values that are used in a sequence most appropriate for earthquake 
        catalogs sorted in by time.   That sequence is:
            1.  If delta time is > dttol break and consider these distinct events
                (dttol should be of the order of P wave travel time through the earth
                to be safe).
            2.  Check if either the latitude or longitude are less than degtol. 
                If either are larger,  immediately define a new event.
            3.  Estimate a total distance in space-time units using a velocity
                multiplier defined by vsource (default 6.0 km/s).   Horizontal 
                distance of separation uses a local cartesian approximation with 
                delta longitude scaled by latitude.   Depth is used directly.  
                Time uses delta_otime*vsource.  L2 norm of that set of 4 delta 
                numbers are computed.   If the L2 norm is < drtol events 
                are considered equal.
        Note other than the difference of the above test this algorithm is 
        nearly identical to _unique_sites

        :param firstid: is the initial source_id to use in this pass.  This should
            normally be created by scanning a previously created event collection
            but could be some arbitrarily large number (caution if you go there)
        :param dttol: initial time test tolerance (see above) (default 1250 s)
        :param degtol: lat,lon degree difference test tolerance (see above - default 
            1 degree for either coordinate)
        :param drtol: space-time distance test tolerance (see above - default of 25 km
            is appropriate for teleseismic data.  Local scales require a change 
            for certain)
        :param vsource: is velocity used to convert origin time difference to a 
            distance for sum of square used for drtol test.
        
        :return:  tuple with two elements.  Element 0 
        of a list of tuples with 0 containing the ObjectId of the parent 
        waveform (from wf collection) and 1 containing the unique source_id adetermined by 
        this algorithm.  Element 1 will contain a deduced dict of 
        source attributes with unique locations.  After cross-checks it is 
        intended to be posted to MongoDB with update or insert.    
        :rtype: tuple with 2 elements (see return for details)
        """
        degtor=math.pi/180.0   # degrees tp radians - needed below
        # this is a two key sort and find combined
        # WARNING:  google searches can yield comfusing exmaples.  pymongo 
        # syntax is deceptively different from mongo shell - don't follow a 
        # MongoDB shell example
        # good source: https://kite.com/python/docs/pymongo.cursor.Cursor.sort
        allwf=self.wf.find().sort([('source_time',pymongo.ASCENDING)])
        # note the cursor object allwf may need a call to the rewind method once
        # this loop finishes
        source_id=firstid
        count=0
        unique_coords={}
        idlist=[]
        firstflag=True
        neweventflag=False
        for doc in allwf:
            # for unknown reasons even though doc is a dict object operator []
            # does not function and we need to use the get method
            lat=doc.get('source_lat')
            lon=doc.get('source_lon')
            depth=doc.get('source_depth')
            otime=doc.get('source_time')
            oid=doc.get('_id')
            if((lat==None) or (lon==None) or (depth==None) or (otime==None)):
                print('coordinate key lookup failed for document number ',count)
                print('Document will be skipped')
            else:
                if(firstflag):
                    m={}
                    m['source_id']=source_id
                    m['source_lat']=lat
                    m['source_lon']=lon
                    m['source_depth']=depth
                    m['source_time']=otime
                    unique_coords[source_id]=m
                    lat0=lat
                    lon0=lon
                    depth0=depth
                    otime0=otime
                    firstflag=False
                    neweventflag=True
                    idlist.append((oid,source_id))
                else:
                    # first test dttol, then degtol, then drtol
                    dotime=otime-otime0
                    if(dotime>dttol):
                        neweventflag=True
                    else:
                        if( (abs(lat-lat0)>degtol) or (abs(lon-lon0)>degtol)):
                            neweventflag=True
                        else:               
                            # crude radius of earth=6371 km
                            Rearth=6371.0
                            rlon=Rearth*math.cos(lat0*degtor)
                            dlatkm=Rearth*abs(lat-lat0)*degtor
                            dlonkm=rlon*abs(lon-lon0)*degtor
                            sumsq=dlatkm*dlatkm + dlonkm*dlonkm
                            dz=depth-depth0
                            sumsq += dz*dz
                            dotime=otime-otime0
                            sumsq += Vsource*Vsource*dotime*dotime
                            drst=math.sqrt(sumsq)
                            if(drst>drtol):
                                neweventflag=True
                            else:
                                neweventflag=False
                    if(neweventflag):
                        source_id+=1
                        # A weird python thing is if the next line is missing
                        # unique_coords gets overwritten for each entry with 
                        # the last value.  reason, I think, is m is a pointer and
                        # without the next line the container is a fixed memory location
                        # that gets overwritten each pass
                        m={}
                        m['source_id']=source_id
                        m['source_lat']=lat
                        m['source_lon']=lon
                        m['source_depth']=depth
                        m['source_time']=otime
                        unique_coords[source_id]=m
                        #print(source_id,':  ',m)
                        lat0=lat
                        lon0=lon
                        depth0=depth
                        otime0=otime
                    idlist.append((oid,source_id))
            count+=1
        return(idlist,unique_coords)
    
    def normalize_source(self, dttol=1250.0, degtol=1.0, drtol=25.0, verbose=False):
        """
        Normalize source collection.

        Scans entire wf collection, extracts unique source locations, and 
        inserts the unique set into the source collection.   This method 
        is intended to be run on a wf collection of newly imported data 
        created by the antelope contrib program extract_events, run through 
        export_to_mspass, and then loaded to MongoDB with 
        mspasspy.io.seispp.index_data. 
        The algorithm is nearly identical to normalize_site
        but for sources instead of receivers.  Perhaps could combine into \
        one function, but left for a different time.  
        
        The _unique_sources method sorts out the unique source locations.  This
        method does MongoDB interactions to insert the results in the source
        collection. 

        The algorithm constructs a cross reference to source collection using the 
        ObjectId of each entry it creates in source.  The string represention 
        of the matching doc in source to each wf entry is defined by the 
        key oid_source.  The cross-reference value for oid_source is placed 
        in each wf document.   

        :param dttol: initial time test tolerance (see above) (default 1250 s)
        :param degtol: lat,lon degree difference test tolerance (see above - default 
            1 degree for either coordinate)
        :param drtol: space-time distance test tolerance (see above - default of 25 km
            is appropriate for teleseismic data.  Local scales require a change 
            for certain)
        :param verbose: when true will echo some information about number of entries
            found and added to each table (default is false).

        """
        # We have to first query the source collection to get the largest value 
        # of the simple integer staid.   A couple true incantations to pymongo
        # are needed to get that - follow
        sourcecol=self.source
        nsource=sourcecol.find().count() # Kosher way to get size of the collection
        if(nsource==0):
            startid=0
        else:
            # Docs claim this is faster than a full sort 
            maxcur=sourcecol.find().sort([('source_id',pymongo.DESCENDING)]).limit(1)
            maxcur.rewind()   # may not be necessary but near zero cost
            maxdoc=maxcur[0]
            maxsid=maxdoc['source_id']
            startid=maxsid+1
            if(verbose):
                print('Existing source_id data found in wf collection\n'+\
                    'Setting initial source_id value to ',startid)
        x=self._unique_sources(startid,dttol,degtol,drtol,verbose)
        oidxref=x[0]
        idmap=x[1]
        if(verbose):
            print("normalize_source:  Found ",len(idmap),\
                " unique source locations. Adding to source collection")
        srcxref={}
        for b in idmap:
            result=sourcecol.insert_one(idmap[b])
            sid=result.inserted_id
            srcxref[b]=sid
            # debug - delete when working
        # Now we put sitid integer value and the ObjectId in sourcecol into 
        # each wf collection document we match - should be all and assume that
        # Note for consistency and to allow the attribute to be passed to 
        # Metadata we convert to string representation
        count=1
        updict={}
        for a in oidxref:
            #sm=idmap[a[1]]
            source_id=a[1]
            #oid_source=str(sm['_id'])
            updict['source_id']=source_id
            sid=srcxref[source_id]
            updict['source_oidstring']=str(sid)
            #print('updating:  source_id=',source_id,' setting oidstring=',str(sid))
            self.wf.update_one({'_id':a[0]},{'$set':updict})
            count+=1
        if(verbose):
            print("normalize_source:  updated source cross reference data in ",count,
                " documents of wf collection")


class Client(pymongo.MongoClient):
    """
    A client-side representation of MongoDB.

    This is a wrapper around the :class:`~pymongo.MongoClient` for convenience.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__default_database_name = self._MongoClient__default_database_name

    def __getitem__(self, name):
        """
        Get a database by name.
        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.
        :Parameters:
          - `name`: the name of the database to get
        """
        return Database(self, name)
    
    def get_default_database(self, default=None, codec_options=None,
        read_preference=None, write_concern=None, read_concern=None):
        if self.__default_database_name is None and default is None:
            raise pymongo.errors.ConfigurationError(
                'No default database name defined or provided.')

        return Database(
            self, self.__default_database_name or default, codec_options,
            read_preference, write_concern, read_concern)

    def get_database(self, name=None, codec_options=None, read_preference=None,
                     write_concern=None, read_concern=None):
        if name is None:
            if self.__default_database_name is None:
                raise pymongo.errors.ConfigurationError('No default database defined')
            name = self.__default_database_name

        return Database(
            self, name, codec_options, read_preference,
            write_concern, read_concern)
