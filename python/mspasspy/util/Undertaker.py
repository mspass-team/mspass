#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from mspasspy.db.database import elog2doc, history2doc
import pymongo

from mspasspy.ccore.seismic import (
    TimeSeries,
    Seismogram,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
    )
from mspasspy.ccore.utility import (
    ErrorSeverity,
    MsPASSError,
    Metadata,
    )

"""
This is a class for handling data marked dead.   The method names are a
programming joke, but are remarkably useful mnemonics for the functionality
they provide.

Concepts of this class are:
1.  Data marked as "dead" in MsPASS are always considered invalid and
    should not be used as part of a final product that is the goal of a workflow.
2.  What made the data invalid cannot be known without additional information.
    In MsPASS the way we always handle adding the additional information is
    with the ErrorLogger component of all data objects.  Hence, the error
    log is the way to sort out problems.
3.  There are two forms of dead data that need to be handled fundamentally
    differently.   (1) Most dead data are expected to be "killed" by
    some edit function or a processing algorithm that finds something
    wrong that won't allow it to do it's task.   (e.g. Seismogram objects
    cannot be created from TimeSeries objects without the orientation
    angles being defined in the Metadata.)  (2)  Errors created during
    construction of a data object that cause the construction to fail.
    The definitive example is constructing seismogram/timeseries objects
    from the MongoDB representation or while reading from a file can fail
    for a long list of reasons.  We give the later that colorful name
    "abortions" since they die before birth.
4.  Ensembles have more complexity than atomic objects because they are
    by definition a collection of atomic objects with additional components
    common to the ensemble.   As a result some methods in this class,
    notably "bring_out_your_dead" only make sense for ensembles.  There is
    also the distinction of an ensemble marked dead versus one or more
    members.  An ensemble marked dead is always treated as having all
    dead members.

An additional set of concepts relate to how the undertaker should handle
the dead bodies.  These are:
1.  To `bury` a dead datum means to save the elog data and a copy of any
    Metadata attributes to MongoDB.  These are stored in special "cemetery"
    collection that has no schema constraints.   Every dead datum the
    undertaker is told to "bury" will produce a document in one of two
    location:  (1) if it is a normal datum the body will be saved in "cemetery"
    (2) if is an "abortion" the body will be saved in "abortions".
2.  To `mummify` a dead datum means to not save anything but return only
    a shell of the original with a minimal memory footprint.  For atomic
    data that means to set the sample array to zero length.  For ensembles
    it means to mummify all dead members but leave the mummies in the container.
    An enemble marked dead passed through mummify will have all it's members
    mummified.
3.  To `cremate` a dead datum means to make it disappear with little to no
    trace.   When an atomic datum is cremated we return a default constructed
    version of the object.   We do that instead of a None to streamline
    use of the cremate feature in a parallel workflow.   Some but not all
    MsPASS processing functions will correctly handle None input so returning
    the ashes is preferable to just an empty symbol (None type).  Ensembles are
    simpler.  When an ensemble is cremated all dead members are vaporized
    with no trace and the member vector will contain only live members.

A datum that is defined as an "abortion" is handled a bit differently
by design.   We take a pro life stance in MsPaSS and view abortions as
always a bad thing that need to be minimized and monitored.  For that
reason they cannot be "cremated" - that makes no sense anyway since
in most cases the sample data in an aborted object are invalid anyway.
Data found to be "aborted" (regularized throught private method `is_abortion`)
are always treated differently an buried in a separate area with the
name "abortions".  The document contents also differ slightly.   Note
there is no such thing, currently, as an aborted ensemble.   Only atomic
data can satisfy that concept.  It is easy to generate an ensemble of
all aborted data, most notable when reading with 'mode="pedantic"',
but the undertaker treats such ensembles as a collection of atomic objects
and automatically buries all abortions it finds.

Documents that are the remnants of dead object saved in two collection.
by default normal killed data create records in the "cemetery" collection
while aborted data objects produce documents in the "abortions"
collection.  All contain an optional "data_tag" defined by the
Undertaker on construction.  Use a unique "data_tag" for any job
to make the source of the document unambiguous.  A common example is
that an Undertaker is defined in Database and the data_tag is passed
used by writers.

:author: Prof. Gary L. Pavlis, Dept. Earth and Atmos. Sci., Indiana University
"""


class Undertaker:
    """
    Class to handle dead data. Results are stored to two spcial
    collections defined by default as "cemetery", for regular dead bodies,
    and "abortions" for those defined as abortions.

    :param dbin:   Should be an instance of  mspasspy.db.Database that is
      used to save the remains of any bodies.

    :type dbin:  the constructor for this class only tests for that the
      handle is an instance of pymongo's Database class.  The MsPASS
      version of Database extends the pymongo version.  Thsi particular
      class references only two methods of Database: (1) the private
      method `_save_elog` and (2) the private method `_save_history`.
      Technically an alternative extension of pymongo's Database
      class that implements those two methods would be plug compatible.
      User's who might want to pull MsPASS apart and use this class
      separately could do so with an alternative Database extension than
      MsPaSs.

    :param regular_data_collection:  collection where we bury regular
    dead bodies.  Default "cemetery"
    :type regular_data_collection:  string

    :param aborted_data_collection:  collection where aborted data documents
    are buried.   Default "abortions"
    :type aborted_data_collection:  string

    :param data_tag:   tag to attach to each document.  Normally would
    be the same as the data_tag used for a particular save operation
    for data not marked dead.
    """

    def __init__(self, dbin,
                 regular_data_collection="cemetery",
                 aborted_data_collection="abortions",
                 data_tag=None,
                 ):
        """
        Constructor takes only one argument.  Expected to be a
        mspasspy.db.Database object (the mspass database handle)
        or it will throw an exception.
        """
        # shared by all error handlers as initialization
        message = "Undertaker constructor:  "
        # pymongo.database.Database is the base class for
        # Database (here meaning mspasspy.db.database.Database) but this
        # seems necessary for some contexts.   If given a base class
        # instance some class methods here will fail that use mspass
        # extensions
        if isinstance(dbin,pymongo.database.Database):
            self.db = dbin
            self.dbh = self.db.elog
        else:
            message += "arg0 must be a MsPASS Database class (mspasspy.db.Database)\n"
            message += "Type of content passed to constructor={}".format(str(type(dbin)))
            raise TypeError(message)
        if isinstance(regular_data_collection,str):
            self.regular_data_collection = regular_data_collection
        if isinstance(aborted_data_collection,str):
            self.aborted_data_collection = aborted_data_collection
        if data_tag:
            if isinstance(data_tag,str):
                self.data_tag = data_tag
            else:
                message += "Illegal type={} for data tag.  Must be str".format(str(type(data_tag)))
        else:
            # allow None type to be carried through - data tag not set in this situation
            self.data_tag = None

    def bury(self,
                      mspass_object,
                      save_history=True,
                      mummify_atomic_data=True,
                      ):
        """
        Handles dead data by saving a subset of content to database.

        MsPASS makes extensive use of the idea of "killing" data as a
        way to say it is bad and should not be considered further in
        any analysis.   There is a need to record what data was killed
        and it is preferable to do so without saving the entire
        data object.  (That is the norm in seismic reflection processing
        where data marked dead are normally carried through until
        removed through a process like a stack.)  This method
        standardizes the method of how to do that and what is saved
        as the shell of a dead datum.  That "shell" is always a minimum
        of two things:
            1.  All elog entries - essential to understand why datum was killed
            2.  The content of the Metadata container saved under a
                subdocument called "tombstone".
        If save_history is set True and the datum has history records
        they will also be saved.

        It is important to realize this method acts like an overloaded
        c++ method in that it accepts multiple data types, but handles
        them differently.
        1.  Atomic data (TimeSeries or Seismogram) marked dead
            generate a document saved to the specified collection and
            an (optional) history document.   If the mummify_atomic_data
            parameter is set True (the default) the returned copy of the
            data will be processed with the "mummify" method of this class.
            (That means the sample data are discarded and the array is set
             to zero length).
        2.  Ensembles have to handle two different situations.   If the
            entire ensemble is marked dead, all members are treated as
            dead and then processed through this method by a recursive
            call on each member.   In that situation an empty ensemble
            is returned with only ensemble metadata not empty.  If the
            ensemble is marked live the code loops over members
            calling this method recusively only on dead data.   In
            that situation the ensemble returned is edited with
            all dead data removed.   (e.g. if we started with 20 members
            and two were marked dead, the return would have 18 members.)

        :param mspass_object:   datum to be processed
        :type mspass_object:  Must be a MsPASS seismic data object
          (TimeSeries, Seismogram, TimeSeriesEnsemble, or SeismogramEnsemble)
          or the method will throw a TypeError.


        :param save_history:  If True and a datum has the optional history
          data stored with it, the history data will be stored in a
          MongoDB collection hard wired into the _save_history method of
          Database.

        :param mummify_atomic_data:  When True (default) atomic data
          marked dead will be passed through self.mummify to reduce
          memory use of the remains.   This parameter is ignored for ensembles.
        """
        # set as symbol to mesh with Database api.  It would make no sense
        # to ever set this False.  Done this way to make that clear
        save_elog = True
        # warning:  because this method may be called on a datum
        # with problematic Metadata it could fail if a metadata
        # value cannot be saved in MongoDB.
        if isinstance(mspass_object, (TimeSeries, Seismogram)):
            if mspass_object.dead():
                if self._is_abortion(mspass_object):
                    mspass_object = self.handle_abortion(mspass_object)
                else:
                    if save_elog:
                        # Note confusion that _save_elog actually does
                        # the burial in this case.  A bit of a maintenance
                        # issue so beware
                        cemeteryid = self.db._save_elog(mspass_object,
                                       collection=self.regular_data_collection,
                                       data_tag=self.data_tag,
                                       )
                        mspass_object[self.regular_data_collection + '_id'] = cemeteryid
                    if save_history:
                        self.db._save_history(mspass_object,alg_name="Undertaker.bury")
                    if mummify_atomic_data:
                        # these are not defaults.  If we saved the elog and history
                        # to the database there is no reason to keep it so we clear
                        # both with this set of options
                        self.mummify(mspass_object,post_elog=False,post_history=False)
                return mspass_object
        elif isinstance(mspass_object, (TimeSeriesEnsemble, SeismogramEnsemble)):
            if mspass_object.dead():
                # if ensemble is marked dead make sure all the membes
                # are marked dead - allows loop below to do its job also
                for d in mspass_object.member:
                    d.kill()
            # Note the indent here so all ensembles pass through this
            # loop.  Buries all dead members and returns a copy of the
            # ensembles with the bodies removed
            ensmd = mspass_object._get_ensemble_md()
            nlive = 0
            for x in mspass_object.member:
                if x.live:
                    nlive += 1
            if isinstance(mspass_object, TimeSeriesEnsemble):
                newens = TimeSeriesEnsemble(ensmd, nlive)
            elif isinstance(mspass_object, SeismogramEnsemble):
                newens = SeismogramEnsemble(ensmd, nlive)
            else:
                raise MsPASSError(
                    "Undertaker.bury",
                    "Coding error - newens constructor section has invalid type\nThat cannot happen unless the original code was incorrectly changed",
                    ErrorSeverity.Invalid,
                    )
            for x in mspass_object.member:
                if x.live:
                    newens.member.append(x)
                else:
                    self.bury(x,save_elog=save_elog,save_history=save_history,mummify_atomic_data=False)
            if nlive>0:
                newens.set_live()
            return newens
        else:
            message = "Undertaker.bury:  Datum received is not a MsPASS data object\n"
            message += "Type of arg0 received ={}".format(str(type(mspass_object)))
            raise TypeError(message)


    def bury_the_dead(self,
                     mspass_object,
                      save_history=True,
                      mummify_atomic_data=True,
                      ):
        """
        Depricated method exactly equivalent to new, and shorter name of
        simply `bury`.   With context as a member of Undertaker the
        long name was redundnant.  Note the call sequence is exactly
        the same as bury.
        """
        print("Undertaker.bury_the_dead:  depricated method.  Use shorter, equivalent bury method instead")
        print("WARNING:  may disappear in future releases")
        return self.bury(mspass_object,save_history,mummify_atomic_data)


    def cremate(self, mspass_object):
        """
        Like bury but nothing is preserved of the dead.

        Fpr atomic data it returns a default constructed (empty)
        copy of the container matching the original type.
        That avoids downstream type collisions if this method is
        called in parallel workflow to release memory.
        This method is most appropriate for ensembles.  In that case, it
        returns a copy of the ensemble with all dead data removed.
        (i.e. they are ommited from the returned copy leaving no trace.)
        If an ensemble is marked dead the return is an empty ensemble
        containing only ensemble Metadata.

        :param mspass_object:  Seismic data object.   If not a MsPASS
          seismic data object a TypeError will be thrown.
        """
        if isinstance(mspass_object,(TimeSeries,Seismogram)):
            if mspass_object.live:
                return mspass_object
            else:
                if self._is_abortion(mspass_object):
                    self.bury(mspass_object)
                # cremation of atomic objects generate default constructed ashes
                if isinstance(mspass_object,TimeSeries):
                    return TimeSeries()
                else:
                    return Seismogram
        elif isinstance(mspass_object, (TimeSeriesEnsemble, SeismogramEnsemble)):
            if mspass_object.dead():
               nlive=0
            # Note the indent here so all ensembles pass through this
            # loop.  Buries all abortions and returns a copy of the
            # ensembles with the all bodies (regular and abortions) removed
            ensmd = mspass_object._get_ensemble_md()
            if mspass_object.live():
                nlive = 0
                for x in mspass_object.member:
                    if x.live:
                        nlive += 1
            if isinstance(mspass_object, TimeSeriesEnsemble):
                newens = TimeSeriesEnsemble(ensmd, nlive)
            elif isinstance(mspass_object, SeismogramEnsemble):
                newens = SeismogramEnsemble(ensmd, nlive)
            else:
                raise MsPASSError(
                    "Undertaker.cremate:  ",
                    "Coding error - newens constructor section has invalid type\nThat cannot happen unless the original code was incorrectly changed",
                    ErrorSeverity.Invalid,
                    )
            if mspass_object.live():
                for x in mspass_object.member:
                    if x.live:
                        newens.member.append(x)
                    else:
                        # Not elif to assure kill and abortion definition
                        # are cleanly separated
                        if self._is_abortion(x):
                            self.bury(x)
                if nlive>0:
                    newens.set_live()
            return newens
        else:
            message = "Undertaker.cremate:  Datum received is not a MsPASS data object\n"
            message += "Type of arg0 received ={}".format(str(type(mspass_object)))
            raise TypeError(message)

    def bring_out_your_dead(self,
                            d,
                            bury=False,
                            save_history=True,
                            mummify_atomic_data=True,
                      ):
        """
        Seperate an ensemble into live and dead members.  Result is
        returned as a pair (tuple) of two ensembles.   First (0 component)
        is a copy of the input with the dead bodies removed.  The second
        (component 1) has the same ensemble Metadata as the input but only
        contains dead members - like the name implies stolen from a great line in
        the Monty Python movie "Search for the Holy Grail".

        :param d:  must be either a TimeSeriesEnsemble or SeismogramEnsemble of
           data to be processed.
        :param bury:  if true the bury method will be called on the
           ensemble of dead data before returning.   Note a limitation of
           using this method is there is no way to save the optional
           history data via this method.  If you need to save history
           run this with bury=False and then run bury with save_history
           true on the dead ensemble.   There is also no way to specify
           an alternative to the default collection name of "cemetery"
        :return: python list with two elements. 0 is ensemble with live data
           and 1 is ensemble with dead data.
        :rtype:  python list with two components
        """
        if not (isinstance(d, TimeSeriesEnsemble) or isinstance(d, SeismogramEnsemble)):
            message = "Undertaker.bring_out_your_dead:  "
            message += "Illegal type passed for arg0\n"
            message += "Actual type of arg0={}\n".format(str(type(d)))
            message += "Must be TimeSeriesEnsemble or SeismgoramEnsemble\n"
            raise TypeError(message)
        # This is a pybind11 wrapper not defined in C++ but useful here
        ensmd = d._get_ensemble_md()
        nlive = 0
        for x in d.member:
            if x.live:
                nlive += 1
        ndead = len(d.member) - nlive
        if isinstance(d, TimeSeriesEnsemble):
            newens = TimeSeriesEnsemble(ensmd, nlive)
            bodies = TimeSeriesEnsemble(ensmd, ndead)
        elif isinstance(d, SeismogramEnsemble):
            newens = SeismogramEnsemble(ensmd, nlive)
            bodies = SeismogramEnsemble(ensmd, ndead)
        else:
            raise MsPASSError(
                "Undertaker.bring_out_your_dead",
                "Coding error - newens constructor section has invalid type\nThat cannot happen unless the original code was incorrectly changed",
                ErrorSeverity.Invalid,
            )
        for x in d.member:
            if x.live:
                newens.member.append(x)
            else:
                bodies.member.append(x)
                # Note we don't support save_history through this
                # mechanism.
                if bury:
                    self.bury(d)
        return [newens, bodies]

    def mummify(self,mspass_object,post_elog=True,post_history=False):
        """
        Reduce memory use associated with dead data.

        For atomic data objects if they are marked dead
        the data vector/matrix is set to zero length releasing
        the dynamically allocated memory.  For Ensembles if
        the entire ensemble is marked dead all members are
        killed and this method calls itself on each member.
        For normal ensembles with mixed live and dead data
        only the data marked dead are muffified.

        Handling of

        :param mspass_object:  datum to be processed.
        """
        if not isinstance(mspass_object,
                          (TimeSeries,
                           Seismogram,
                           TimeSeriesEnsemble,
                           SeismogramEnsemble)):
            message = "Undertaker.mumify:  arg0 must be a mspass seismic data object.  Actual type received = "
            message += str(type(mspass_object))
            raise TypeError(message)
        # Note:  this method currently does nothing special for abortions
        # That should be ok because all ways we create abortions have
        # objects constructed far enough that the algorithm below shouldn't
        # fail.   Adding ways to abort could invaldate that assumption
        if mspass_object.dead():
            if post_elog:
                elog_doc=elog2doc(mspass_object.elog)
                mspass_object["error_log"]=elog_doc
                mspass_object.elog.clear()
            if isinstance(mspass_object,(TimeSeries,Seismogram)):
                # Note history only makes sense for atomic data so this
                # section needs to be here
                if post_history:
                    hisdoc = history2doc(mspass_object)
                    mspass_object["processing_history"]=hisdoc
                    mspass_object.clear_history()
                mspass_object.set_npts(0)
            elif isinstance(mspass_object,(TimeSeriesEnsemble,SeismogramEnsemble)):
                # Note this is executed if the entire ensemble is marked
                # dead.  We then force a kill of all members and mummify all
                for d in mspass_object.member:
                    d.kill()
                    d = self.mummify(d,post_elog,post_history)
        else:
            # only need to do anything if we land here if this is an ensemble
            # i.e. we will silently return an atomnic object marked live
            # for ensembles we mummify dead members
            if  isinstance(mspass_object,(TimeSeriesEnsemble,SeismogramEnsemble)):
                for d in mspass_object.member:
                    if d.dead():
                        d = self.mummify(d,post_elog,post_history)
        return mspass_object


    def handle_abortion(self,doc_or_datum,type=None):
        """
        Standardized method to handle what we call abortions (see class overview).

        This method standardizes handling of abortions.  They are always
        saved as a document in a collection set by the constructor
        (self.aborted_data_collection) that defaults to "abortions".
        The documents saved have up to 3 key-value pairs:
            "tombstone" - contents are a subdocument (dict) of the
              wf document that was aborted during construction.
            "logdata" - any error log records left by the reeader that failed.
            "type" -  string describing the expected type of data object
              that a reader was attempting to construct.   In rare
              situations it could be set to "unknown" if
              Undertaker._handle_abortion is called on a raw document
              and type is not set (see parameters below)

        :param doc_or_datum:  container defining the aborted fetus.
        :type doc_or_datum:  Must be one of `TimeSeries`, `Seismogram`, `Metadata`,
        or a python dict.   For the seismic data objects any content in
        the ErrorLogger will be saved.   For dict input an application
        should post a message to the dict with some appropriate (custom)
        key to preserve a cause for the abortion.

        :param type: string description of the type of data object
        to associate with dict input.  Default for this parameter is None
        and it is not referenced at all for normal input of TimeSeries
        and Seismogram objects.  It is ONLY referenced if arg0 is a
        dict. If type is None and the input is a dict the value assigned to
        the "type" key in the abortions document is "unknown".   The
        escape for "unknown" makes the method bombproof but may make the
        saved documents ambiguous.

        :exception:  throws a TypeError if arg0 does not obey type
        list described above.
        """
        insertion_doc=dict()
        if self.data_tag:
            insertion_doc["data_tag"] = self.data_tag
        if isinstance(doc_or_datum,(dict,Metadata)):
            if isinstance(doc_or_datum,Metadata):
                remains=dict(doc_or_datum)
            else:
                remains=doc_or_datum
            # Note made a list to be consistent with ensemble version
            insertion_doc={"tombstone": [remains]}
            if type:
                insertion_doc["type"] = type
            else:
                # this should not be entered but is safer to include it
                insertion_doc["type"] = "unknown"
        elif isinstance(doc_or_datum,(TimeSeries,Seismogram)):
            insertion_doc = {"tombstone" : dict(doc_or_datum)}
            if doc_or_datum.elog.size() > 0:
                logdata = elog2doc(doc_or_datum)
                insertion_doc["logdata"] = logdata
            insertion_doc["type"] = str(type(doc_or_datum))
        else:
            message = "Undertaker.handle_abortion:   Illegal type for arg0={}".format(str(type(doc_or_datum)))
            message += "Must be a TimeSeries, Seismogram, or a dict"
            raise TypeError(message)

        if len(insertion_doc) > 0:
            self.db[self.aborted_data_collection].insert_one(insertion_doc)

        return doc_or_datum

    @staticmethod
    def _is_abortion(d):
        """
        Internal method used to standardize test for whether a datum is
        wha we call an "abortion".   The test is trivial in this case
        because of the use of the "is_abortion" Metadata attribute in
        our readers.   Could be more complex so this design assures
        separation of the concept from the implementation.
        :param d:  datum to be tested.
        :type d: TimeSeries or Seismogram.  We do test for this as the
        cost is small and a TypeError will be thrown if d is not either of
        these types.  Considered bypassing the test but better to
        make the package more robust.

        :return: boolean True if datum is an abortion, False othewise.
        """
        if isinstance(d,(TimeSeries,Seismogram)):
            if d.is_defined("is_abortion"):
                if d["is_abortion"]:
                    return True
                else:
                    return False
            else:
                message = "Warning:  dead datum has is_abortion attribute undefined - assumed False\n"
                message += "MsPASS readers should always set this attribute"
                err = MsPASSError("Undertaker._is_abortion",message,ErrorSeverity.Complaint)
                d.elog.log_error(err)
                return False
        else:
            message = "Undertaker._is_abortion:  received an input that is an invalid type - must be either a TimeSeries or Seismogram object"
            raise TypeError(message)
