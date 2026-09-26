#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module contains functions used to standardize exporting and
importing a data set.   It has two primary uses:
    1.  Create a set of files that can be archived and allow the
        data set to be reconstructed later.
    2.  Create files that simplify moving a data set from a staging
        system (typically a desktop) to an HPC system for more
        compute intensive processing.

The archive format is paired files.   One file with the extension
".json" contains Metadata that is an image of wf documents normally
managed with mspass using MongoDB.   A file with the same root path
name with default extension ".dat" holds he sample data.   Native
binary saves use 8 byte floats for the sample data but other sample
formats are possible by defining alternative "datatype" attributes.
Iniially only 8 byte floats are allowed, but extension to other
formats like a compressed format are planned.

Created on Sat Sep  5 05:54:01 2026

@author: pavlis
"""

# we use this our jsos reader and writer instead of standard pythone one
# to assure we can handle all MongoDB data types.
from bson import json_util
from pymongo.collection import Collection
from pathlib import Path
import os
from itertools import islice
from mspasspy.ccore.utility import MsPASSError, ErrorSeverity, Metadata
from mspasspy.ccore.io import _fwrite_to_file, _fread_from_file
from mspasspy.ccore.seismic import (
    TimeSeriesEnsemble,
    SeismogramEnsemble,
    TimeSeries,
    Seismogram,
)
from mspasspy.util.seismic import number_live


def block_cursor(cursor, block_size):
    """
    Used in archive_gridfs_data when handling a large collection of
    gridfs objects.   There block_size is used to control size of
    ensembles loaded into member before saving groups as files.
    block_size should consider memory constraints.

    Note this is a modification of code suggested by Gemini.

    :param cursor:  normal cursor returned by find
    :param block_size:   size of each chunk returned (last will be truncated)

    Returns a list of cursors defining each block.
    """
    while True:
        chunk = list(islice(cursor, block_size))
        if not chunk:
            break
        yield chunk


def archive_gridfs_data(
    db_collection,
    query=None,
    sort=None,
    output_directory=None,
    output_file_base="gridfs_archive",
    objects_per_file=10000,
    overwrite=True,
    verbose=False,
) -> int:
    """
    Write gridfs data to archive files. 
    
    MsPASS allows storage of sample data for seismic objects to MongoDB's 
    so called gridfs system.   Archiving data in gridfs is requires the 
    data be extracted from MongoDB storage and save to conventional 
    data files.   
    
    It is best to think of two ways to utilize this function:
        1.   If the data to be archived has a natural grouping into 
             ensemble objects call this function in a loop over the 
             ensembles. 
        2.   If not, the function assumes it is handling a large list of 
             atomic data stored in gridfs.  
    For 1 the user should make sure the file names this function wil 
    generate make sense.  For the second you need only specify a base 
    file name and the writer will automatically append a numeric 
    string for the sequence of files it generates (see below).
    In all cases the sample data are written to files suffix ".dat" 
    and a file of the same base name with suffix ".json" holds 
    the related metadata for data in the ".dat" file.  
    
    :param db_collection: MongoDB Collection object defining Metadata for 
       seismic data objects that are to be writen to archive file(s).   
       Normally either db.wf_TimeSeries or db.wf_Seismogram.   Not is the 
       object not the name string defining the collection to use.
    :param query:  optional query to define what data is to be written 
       to archive files.   This query is always appended ($and operator)
       to base query of {"storage_mode" : "gridfs"} since the purpose of this
       function is to save gridfs data to external files.  When used to 
       save data naturally grouped into ensembles this query would 
       defined a query to obtain one ensemble to be laoded and saved. 
       (e.g. query={"source_id" : sid} for a single common source gather 
       defined by content of the symbol sid).  Default is None which is taken 
       to mean save all data stored in gridfs. 
    :type query:  python dictionary assumed to defined a valid MongDB find query.
    :param sort:  optional pymongo sort clause.   Default is None which means 
       is taken to mean do not sort.  If sort is defined the value is passed directly 
       to the cursor sort method (return of find method).   The syntax for 
       sort is a bit weird so be careful.  If you get it wrong MongoDB will 
       throw an exception.
    :param output_file_base:  base name to write files created by this 
       function.   When only one file is needed the sample data file 
       will be f"{object_file_base}.dat" and the metadata file will be 
       f"{object_file_base}.json".   When multiple files are created 
       the file names get a sequence number n={1,2, ..., N_files}.  \
       For each n the sample files are called 
       and the associated metadata files are called f"{output_file_base}_{n}.json".
    :type output_file_base:  str
    :param output_directory: directory to use to write the files.  The 
       function does not test if the given path allows writes so it will 
       throw an exception if the the path is write protected.  If a directory
       by that name does not exist it will be created.  
    :type output_directory:  path like object which usually means an str 
       defining a directory name or a Path object defining a directory.
    :param objects_per_file: block size limit for files created.  This is an 
       important parameter if the number of seismic data objects stored in 
       gridfs is large or the ensembles being handled are potentially large.  
       The function works by constructing ensembles in memory.  If the number
       of documents returned when the query argument is a applied 
       (all gridfs data by default) exceeds objects_per_file, the data 
       will be blocked into ensembles no larger than this value.   Each 
       block is written to the same base name but with a sequence number 
       as described for the "output_file_base" argument.   This argument is 
       essential for writing out atomic data stored in gridfs 
       (default with no query) to avoid memory overflow.  For atomic saves 
       estimate the nominal size of each object and change this value 
       if necessary to fit for the ensembles it creates to fit in 
       available memory. 
    :param overwrite:  if true files (default) output files will silently 
       overwrite any existing files with the same name.  If False it will 
       throw an exception if an file already exists.  It will do that, 
       however, only when it hits a problem so it is possible to abort 
       in the middle of a large save.  Be careful of your naming 
       conventions to avoid such issues.  
    :param verbose:  when true prints a few informational lines.  When 
       False (default) it works silently.
    :return: number of pairs of files written.  Pairs because each ensemble 
       it saves a ".dat" and a ".json" file.
    
    """
    alg = "archive_gridfs_data"
    # note storage_mode must be gridfs for a mspass wf document to
    # put data in gridfs.  Current code base has file as default if
    # storage_mode is no set.  Hence we always have that in the query
    if query is None:
        # in this case we fetch all
        wfquery = {"storage_mode": "gridfs"}
    else:
        if not isinstance(query, dict):
            message = (
                f"query argument must be a python dictionary defining a MongoDB query\n"
            )
            message += f"Received {query} which is of type {type(query)}"
            raise MsPASSError(f"{alg}: {message}", ErrorSeverity.Fatal)
        # this and clause is normally assumed but this shouldl allow more
        # complex queries
        wfquery = {"$and": [query, {"storage_mode": "gridfs"}]}
    if not isinstance(objects_per_file, int) or objects_per_file <= 0:
        raise ValueError("objects_per_file must be a positive integer")
    ndata = db_collection.count_documents(wfquery)
    if ndata == 0:
        print(f"WARNING({alg}):  {wfquery} yields no documents.  Doing nothing")
        return 0
    nfiles = (ndata + objects_per_file - 1) // objects_per_file
    if nfiles > 1 and verbose:
        print(f"WARNING({alg}):   Number of data being handle is large")
        print("Number of atomic data to save=", ndata)
        print("Will save to ", nfiles, " files with _n appended to names")
    # now make sure we have directory to write results to
    if output_directory is None:
        outdir = Path.cwd()
    else:
        # will work if output_directory is a string or Path and throw and
        # exception otherwise
        outdir = Path(output_directory)
        # just let this throw an exception here if it isn't writable
        outdir.mkdir(parents=True, exist_ok=True)
    if verbose:
        print("Writing data files to directory ", outdir)
    if sort is None:
        cursor = db_collection.find(wfquery)
    else:
        cursor = db_collection.find(wfquery).sort(sort)
    # when nfiles is one the blocking will work normally
    # Only distinction is file naming
    count = 1
    written = 0
    for block in block_cursor(cursor, objects_per_file):
        members = [
            db_collection.database.read_data(doc, collection=db_collection.name)
            for doc in block
        ]
        ens = (
            SeismogramEnsemble()
            if isinstance(members[0], Seismogram)
            else TimeSeriesEnsemble()
        )
        for member in members:
            ens.member.append(member)
        if any(member.live for member in ens.member):
            ens.set_live()
        if nfiles == 1:
            basepath = outdir / output_file_base
        else:
            ofb = f"{output_file_base}_{count}"
            basepath = outdir / ofb
        if verbose:
            message = f"Writing sample data to file {basepath}.dat and metadata to file {basepath}.json"
            print(message)
        nsaved = save_to_archive_files(ens, basepath, overwrite=overwrite)
        if verbose:
            print(
                "Saved ",
                nsaved,
                " data objects to pair of files with base name=",
                basepath,
            )
        written += nsaved > 0
        count += 1
    return written


def save_to_archive_files(
    ens,
    base_pathname,
    datatype="f8",
    data_file_suffix=".dat",
    json_file_suffix=".json",
    overwrite=False,
) -> int:
    """
     Write content of ens to a pair of files defined by the "base_pathname"
     argument.

     The MsPASS archive feature save seismic data objects as pairs of
     two closely related files.   (1) a file containing the sample data
     with (possibly) duplicate Metadata in a format, and (2) a json
     format file contains the contents of Metadata container from all
     data stored in the sample data file.   The json file always
     resolves to a list of python dictionaries when restored.
     The data file normally contains data from multiple objects with
     an offset read position defined in the json files.  Specifically
     the writer sets the attribute "foff" to a byte offset of the
     starting read position for the sample data for the related object.
     The size of the sample arrays is set by a second fixed metadata key
     defined as "npts" and a related special attribute found only in
     archive metadata with the key "datatype".   The datatype concept
     is borrowed from css3.0 and Antelope and any uses should conform
     to Antelope naming conventions.  The default for datatype is "f8"
     which defines 8 byte IEEE, intel byte order, floating point
     numbers.   i.e., "datatype" defines the binary word structure of
     a single data sample noting that TimeSeries are stored as a
     vector of said data types while Seismogram data are stored in a
     contiguous buffer of size 3*npts (Fortran order) with array
     components defined by "datatype".

     This function is designed to be used within a workflow after
     data have been loaded and processed to some final state you want to
     save for your records/publication.   It dogmaticaly ONLY accepts
     ensemble objects for writing.   That enforces at least some level
     for rationality in handling large data sets on modern clusters.
     As is well know, with current large disk arrays used on all clusters
     large numbers of files can overwhelm disk array metadata servers.
     Hence, for a file archive things like a million sac files are very
     bad news   This function assumes the user understands that issue
     and has packaged the dataset into some rational form of ensembles.
     On the other hand, be aware that this algorithm can be very
     memory intensive if handling large ensembles as the ensemble to
     be saved has to be loaded into memory.   Today that is more of
     a concern if the function is used in a large parallel workflow
     driven by a bag of ensemble objects.

     This function handles dead data differently than a related
     operation :py:method:`Database.save_data`.  That is, the database
     save normally saves a record of killed data to the "cemetery"
     collection.   This function takes the view that bodies do not
     belong in the archive.  As a result it always silently discards any
     dead members of the ensemble it is handling.   It also does that with
     live members copied to a new container when dead members are present.

     The function normally is careful to avod overwriting existing data.
     It more-or-less assumes the model is that it is give a target
     directory that it should assume does not contain existing files
     that will conflict with the write path direction it receives
     (value of `base_pathname`).   Set the `overwrite` argument True
     if you want to force the function to destroy any existing files
     That option is most appropriate if you are rerunning a workflow
     and want to replace a set of files created previously that have
     a problem.  When debugging a workflow is dangerous to allow
     overwriting as it would be very easy to destroy data used as input
     the workflow.

     To avoid memory overflows in parallel workflows the function only
     returns a count of the number of live data it saved.

     :param ens:   ensemble object to be saved.  As noted it can contain
        dead members but dead members will be silently discarded.  If the
        entire enemble is marked dead or has no live members the
        function imediately returns 0.
     :type ens:  must be either a :py:class:`TimeSeriesEnsemble` or
        :py:class:`SeisogramEnsemble` or the function will throw a
        TypeError exception.
     :param base_pathname:   this should define a base filename either
        as a generic Path object or string.  A key qualfier is "base".
        The algorithm does two things you must be aware of: (1) it
        first separates out any directory specifications using Path.parent,
        and (2) it then uses Parent.stem as the base file name for
        writing in directory defined by (1).  It then appends the suffix
        defined by `data_file_suffix` to write the sample data file and
        appends the value of `json_file_suffix` to define the Metadata
        file.   Note the fact base_pathname is a required argument
        makes parallel processing a bit awkward with bag.map operators.
        See user manual for guidance on how to handle parallel archiving.
    :type base_pathname:   Must be either an instance of `pathlib.Path` or
        a str.  In either case the string must define an existing directory
        in the path to which you have write permission.  The function
        has no test for write access and it will throw an exception
        when it tries to write in any directory that is not writable or
        nonexistent.
    :param datatype: must define an acceptable sample data format for
        the output file containing the sample data.  Currently the
        function only accepts the default of "f8" which means double
        precision (8 byte) IEEE floating point numbers (intel byte order).
        The argument itself is a stub for future support for alternative
        data file formats.   A simple example would be to use some
        compression algorithm on the sample arrays to reduce storage.
        That could be done be defining a different acceptable value for
        "datatype" and adding code to this function and the related reader
        to handle that compression format.   A more elaborate example that
        could, in principle, be implemented this way is externally
        create a new workflow to concatenate a set of SAC files together
        and create a json file comparable to the this function generates
        to define the Metadata those SAC files contain.  That example,
        would require a modification of the reader but not this writer to
        be functional.  That is a job for someone with lots of SAC files
        they need to input into MsPASS.
     :type datatype:  str (currently can only be default of "f8" or the
        functionw will throw an exception)
     :param data_file_suffix:  suffix (also commonly called file extension) to
        append to base_pathname to define sample data output file.
     :type data_file_suffix:  str (Default of ".dat" should not be changed
         without good reason.   If changed make sure the "." is the first character
         of the string.)
     :param json_file_suffix:  suffix (also commonly called file extension) to
        append to base_pathname to define json format file used to save metadata.
     :type data_file_suffix:  str (Default of ".json" should not be changed
         without good reason.   If changed make sure the "." is the first character
         of the string.)
     :param overwrite:  boolean that controls how to handle existing files.
         By default (overwrite=False) the function will throw an exception f
         either the sample data file or the json file already exist.
         Change this to True ONLY if you are certain the workflow will
         not clobber some existing files.
    """
    alg = "save_to_archive_file"
    # generic test for an ensemble object
    # not foolproof but suitable for mspass and less pedantic isinstance
    if not hasattr(ens, "member"):
        message = f"save_to_archive_file:   arg0 has invalid type={type(ens)}\n"
        message += "Must be an ensmeble object"
        raise TypeError(message)
    if datatype != "f8":
        message = (
            f"{alg}:  datatype argument must by f8.  datatype={datatype} is not allowed"
        )
        raise ValueError(message)
    if ens.dead():
        return 0
    nlive = number_live(ens)
    # this should rarely be executed as any ensembles with no live members
    # should normally already be marked dead
    if nlive == 0:
        return 0
    if isinstance(base_pathname, str):
        base_pathname = Path(base_pathname)
    # needed to be sure ensemble metadata is copied to all members
    ens.sync_metadata()
    # dead data are not appropriate for an archive file

    if nlive < len(ens.member):
        # Could do this step with an Undertaker but instantiating an
        # Undertaker requires a database handle which would be baggage here
        # this is a simplified version of code from Undertaker
        if isinstance(ens, TimeSeriesEnsemble):
            cleaned_ens = TimeSeriesEnsemble(nlive)
            for d in ens.member:
                if d.live:
                    cleaned_ens.member.append(TimeSeries(d))
        elif isinstance(ens, SeismogramEnsemble):
            cleaned_ens = SeismogramEnsemble(nlive)
            for d in ens.member:
                if d.live:
                    cleaned_ens.member.append(Seismogram(d))
        else:
            # this exception can only happen if the object give as arg0 has a
            # member attribute but is not a mspass ensemble
            raise TypeError(
                f"{alg}:  arg0 must be a TimeSeriesEnsemble or SeismogramEnsemble"
            )
    else:
        cleaned_ens = ens

    cleaned_ens.set_live()
    abspath = base_pathname.resolve()
    outdir = abspath.parent
    dfile = base_pathname.with_suffix(data_file_suffix)
    jsonfile = base_pathname.with_suffix(json_file_suffix)
    if not overwrite:
        if dfile.is_file() or jsonfile.is_file():
            message = f"{alg}:  data file={dfile} or json file={jsonfile} exist\n"
            message += "This function will not overwrite existing files\n"
            message += (
                "Rerun with overwrite=True if you mean to overwrite existing files\n"
            )
            raise FileExistsError(message)
    # We have to save the sample data first as we need the list of foff
    # values to write to the json file.
    # The native writer appends; reset the archive before an overwrite.
    if overwrite and dfile.exists():
        dfile.unlink()
    fofflist = _fwrite_to_file(cleaned_ens, str(outdir), dfile.name)
    # intentionally do not enforce a schema on documents that will
    # define the json file
    doclist = list()
    for d in cleaned_ens.member:
        # pybind11 code sets the metadata symbol to refernce the Metadata
        # container.   d is an atomic datum here
        doc = dict(Metadata(d))
        doc = update_document_for_json_output(
            doc,
            atomic_data_type=(
                "Seismogram" if isinstance(d, Seismogram) else "TimeSeries"
            ),
        )
        doclist.append(doc)
    # add the foff values to each document.   This works because we can
    # be sure any dead data have been cremated.
    for i in range(len(doclist)):
        doclist[i]["foff"] = fofflist[i]
    # Note when json_util gets a list of documents it produces
    # a formatted version of same.  i.e. we need only one read
    # in the reader to eat this up
    with open(jsonfile, "w", encoding="utf-8") as fp:
        fp.write(json_util.dumps(doclist))
    return nlive


def read_from_archive_file(
    filepath,
    data_file_suffix=".dat",
    json_file_suffix=".json",
):
    """ """
    alg = "read_from_archive_files"
    if isinstance(filepath, str):
        filepath = Path(filepath)
    # assure filepath deosn't have an existing extension.  Note this
    # won't work if a file name has two tokens separated by ".".
    filepath = filepath.with_suffix("")
    dfile_path = filepath.with_suffix(data_file_suffix)
    jsonfile_path = filepath.with_suffix(json_file_suffix)
    if not dfile_path.is_file():
        message = f"{alg}:  data file={dfile_path} does not exist"
        raise FileExistsError(message)
    if not jsonfile_path.is_file():
        message = f"{alg}:  data file={jsonfile_path} does not exist"
        raise FileExistsError(message)
    with open(jsonfile_path, "r", encoding="utf-8") as f:
        content = f.read()
    if not content.strip():
        raise ValueError(f"{alg}: json file has no valid json data")
    doclist = json_util.loads(content)
    if not isinstance(doclist, list) or not doclist:
        raise ValueError(f"{alg}: json must contain a nonempty list of documents")
    if any(doc.get("datatype", "f8") != "f8" for doc in doclist):
        raise ValueError(f"{alg}: only datatype f8 is supported")
    # these attributes should be cleared if they are present
    keys_to_clear = ["_id", "dir", "dfile"]
    # clear junk and get two required keys to proceed
    # currently require datatype to be the same for all
    # we need to fetch atomic_data_type to create correct type of ensembles
    count = 0
    fofflist = []
    for doc in doclist:
        if count == 0:
            # we only need this once and assume all documents have them
            # as the same value.  The format may evolve and render that
            # assumption invalid
            atomic_data_type = doc["atomic_data_type"]
            if "datatype" in doc:
                datatype = doc["datatype"]
            else:
                # this is used as a signal to post a warning after ensemble
                # container is created.   Otherwise would post that message here
                datatype = "undefined"
        # let ths throw an exception if this is missing as we have to have it
        # we don't need to use fofflist but load it anyway as it is small
        fofflist.append(doc["foff"])
        # might be wise to verify npts exists to avoid a mysterous failure
        # in the C code but for now I will treat that as unnecessary
        #
        # do clear debris
        for k in keys_to_clear:
            if k in doc:
                doc.pop(k, None)
        count += 1
    n_members = len(doclist)
    if atomic_data_type == "TimeSeries":
        ens = TimeSeriesEnsemble(n_members)
    elif atomic_data_type == "Seismogram":
        ens = SeismogramEnsemble(n_members)
    else:
        message = f"json file={jsonfile_path} defines illegal value for required attribute atomic_data_type={atomic_data_type}\n"
        message += "Must be either TimeSeries or Seismogram. Repair json file"
        # throw an exception instead of returning a dead ensemble as this
        # error should not happen and if it does somethign is really wrong
        raise ValueError(message)
    if datatype == "undefined":
        message = "json file containing metadata is missing datatype attribute\n"
        message += "Defaulting to f8.  Make sure the data are valid"
        ens.elog.log_error(alg, message, ErrorSeverity.Complaint)
        datatype = "f8"
    # this is a variation of the algorithm in Database._load_ensemble_file
    # Did not use that directly as this function does not need access to
    # a Database object and there are some minor variations in concept
    reading_index = []
    count = 0
    for doc in doclist:
        try:
            md = Metadata(doc)
            if atomic_data_type == "TimeSeries":
                d = TimeSeries(md)
            else:
                d = Seismogram(md, False)
            ens.member.append(d)
            reading_index.append(count)
        except MsPASSError as merr:
            # we need to create this empty dead datum to allow the
            # c++ function to handle read cleanly.  See below
            # where reading_index is creaed
            if atomic_data_type == "TimeSeries":
                d = TimeSeries()
            else:
                d = Seismogram()
            ens.member.append(d)
            # logs this error to the ensemble elog container
            message = f"The following document from json file={jsonfile_path} caused constructor to throw a MsPASSError:\n"
            message += str(doc)
            message += "\n"
            message += f"Error message:  {merr}"
            ens.elog.log_error(alg, message, ErrorSeverity.Complaint)
        count += 1
    # Database._load_ensemble_file does a sort by foff values for efficiency
    # assume that isn't needed here as normally the json file is created
    # with the documents images in foff order
    # This reader assumes live data members have arrays allocated
    # Any errors in the above loop cause the data associated with that
    # document to be dropped leaving ony an ensemble elog entry
    wfcount = _fread_from_file(
        ens, str(dfile_path.parent), dfile_path.name, reading_index
    )
    if wfcount > 0 and number_live(ens) > 0:
        ens.set_live()
    return ens


def create_archive_index(
    db_collection,
    query=None,
    json_file_suffix=".json",
) -> int:
    """
    Creates a set of json files for all data with storage_mode=="file".

    The default "binary" format used to save sample data to files
    has an existing index in the wf_TimeSeries or wf_Seismogram collections.
    What this function does is build a json index file for every unique
    file name it fins in the database collection defined by arg0.
    The optional query can be used to limit what files are handled.
    This function is the most efficient way to create an archive of a
    set of processed waveform data.   To be most effective a workflow
    should be designed to put files of waveform data you want to archive
    in a separate directory chain.   Directories with some data indexed
    and others not would challenging to write efficiently to any archival
    system I am aware of.

    This function should only be run serial.  Experience has shown it is
    lightening fast to run and unless you have something stupid like millions
    of single channel waveform files the baggage of parallel processing is
    not necessary.  The function error handling pretty much assumes you
    are running this function interactively, but error handlers will work
    fine on a batch system as well.  What I mean by that is that
    common usage errors will throw an exception rather than catch problems
    and try to continue like MsPASS processing functions.

    :param db_collection:   MongoDB waveform collection with sample data
       files index with the attributes "dir", "dfile", and "foff".
       Any data stored with gridfs in this collection will be silently ignored.
    :type db_collection:  must be a MongoDB Collection object.   For standard
       MsPASS use should only be either db.wf_TimeSeries or db.wf_Seismogram
       where "db" is a Database object.   Note be aware pymongo treats the
       constructs like `db.wf_TimeSeries` as the same thing as
       `db["wf_TimeSeries"]`.
    :param query:   query to apply to db_collection to select subset of
       data to handle.   A typical example would be `query={"data_tag" : "final"}`
       to select only data saved with a particular data tag.   That would
       most commonly be the finished product of a processing sequence
       but could be some intermediate step where you want to move data
       between systems.
    :type query:  python dictionary defining a valid pymongo query.
    :param json_file_suffix:  file suffix to use on json files to hold
       wf document metadata.   Default is ".json".   That means if you
       have a sample data called something like "event_42.dat" indexed
       by db_collection a file called "event_42.json" will be created
       containing the documents for all seismic objects with sample
       data contained in that file.
    :type json_file_suffix:  str (default ".json").  changing this is not
       recommended but if you do be very sure you include the leading ".".

    :return: number of files written (type int)
    """
    alg = "create_archive_index"
    if not isinstance(db_collection, Collection):
        raise ValueError(f"{alg}:  arg0 must be a pymongo Collection object")
    if query is not None and not isinstance(query, dict):
        raise ValueError(f"{alg}:  query argument must be a python dictionary")
    # Preserve arbitrary selection clauses, including storage_mode and dir.
    wfquery = (
        {"$and": [query, {"storage_mode": "file"}]}
        if query
        else {"storage_mode": "file"}
    )
    if not db_collection.distinct("dir"):
        raise MsPASSError(
            f"{alg}: collection has no documents with the dir attribute set",
            ErrorSeverity.Fatal,
        )
    aliases_by_path = {}
    for doc in db_collection.find(wfquery):
        path = Path(doc["dir"]) / doc["dfile"]
        try:
            resolved = path.resolve(strict=True)
        except FileNotFoundError:
            print(
                f"{alg} (WARNING):  found document defined path={path} that does not exist"
            )
            continue
        aliases_by_path.setdefault(resolved, set()).add((doc["dir"], doc["dfile"]))
    for path, aliases in aliases_by_path.items():
        filequery = {
            "$and": [
                wfquery,
                {
                    "$or": [
                        {"dir": directory, "dfile": filename}
                        for directory, filename in aliases
                    ]
                },
            ]
        }
        documents = db_collection.find(filequery)
        if not os.access(path.parent, os.W_OK):
            raise PermissionError(
                f"You do not appear to have write permission in directory={path.parent}"
            )
        doclist = [
            update_document_for_json_output(
                dict(doc),
                atomic_data_type="Seismogram" if "tmatrix" in doc else "TimeSeries",
            )
            for doc in documents
        ]
        with open(path.with_suffix(json_file_suffix), "w", encoding="utf-8") as fp:
            fp.write(json_util.dumps(doclist))
    return len(aliases_by_path)


def update_document_for_json_output(
    doc,
    strip_id=True,
    atomic_data_type="TimeSeries",
) -> dict:
    """
    Takes input document (dictionary) doc and edits it to remove
    attributes that are appropriate for MongoDB for inconsistent with
    json file used for archive format.

    Unless proven otherwise edits are changing dir value to "." and
    removing the "._id" attribute.   Set strip_id to False if you
    need to retain the id value in the file
    """
    alg = "update_document_for_json_output"
    allowed_atomic_types = ["TimeSeries", "Seismogram"]
    if atomic_data_type not in allowed_atomic_types:
        message = (
            f"{alg}:  Illegal value for argument atomic_data_type={atomic_data_type}"
        )
        raise ValueError(message)
    doc["atomic_data_type"] = atomic_data_type
    # always reset this to this value as the json file will always
    # be written in the same directory as the data file
    doc["dir"] = "."
    if "format" in doc:
        if doc["format"] == "binary":
            doc["datatype"] = "f8"
        else:
            # for now raise an exception for other formats
            # extensions for other formats would go here
            message = (
                f"{alg}:  Archiver currently only support binary raw format storage\n"
            )
            message += "Restructure your data set or extend this function"
            raise MsPASSError(f"{alg}: {message}", ErrorSeverity.Fatal)
    else:
        # format undefined in MsPASS implies binary f8
        doc["datatype"] = "f8"
    if strip_id:
        # None is needed to make this work even if id is not defined
        doc.pop("_id", None)

    return doc
