#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from mspasspy.db import Database, elog2doc, history2doc
from mspasspy.ccore.seismic import (
    TimeSeries,
    Seismogram,
    TimeSeriesEnsemble, 
    SeismogramEnsemble,
    )
from mspasspy.ccore.utility import ErrorSeverity, MsPASSError

"""
This is a class for handling data marked dead.   The method names are a bit
tongue in cheek but descriptive.
:author: Prof. Gary L. Pavlis, Dept. Earth and Atmos. Sci., Indiana University
"""


class Undertaker:
    """
    Class to handle dead data.  Primarily for ensembles, but has methods to
    save elog entries for Seismogram or TimeSeries objects marked dead.
    It also has am mummify method for reducing memory use for any data object.
    A concept of this class is any thing received that is live is not
    changed.   It only handles the dead.
    """

    def __init__(self, dbin):
        """
        Constructor takes only one argument.  Expected to be a 
        mspasspy.db.Database object (the mspass database handle) 
        or it will throw an exception.
        """
        if isinstance(dbin,Database):
            self.db = dbin
            self.dbh = self.db.elog
        else:
            message = "Undertaker constructor:  "
            message += "arg0 must be a MsPASS Database class (mspasspy.db.Database)\n"
            message += "Type of content passed to constructor={}".format(str(type(dbin)))
            raise TypeError(message)
            
        
    def bury(self, 
                      mspass_object,
                      collection="cemetery",
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
        standizes the method of how to do that and what is saved 
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
          
        :param collection:  MongoDB collection name to save results to.  
          (default is "cemetery")
        :type collection: string
        
        :param save_history:  If True and a datum has the optional history 
          data stored with it, the history data will be stored in a 
          MongoDB collection hard wired into the _save_history method of 
          Database. 
          
        :param mummify_atomic_data:  When True (default) atomic data 
          marked dead will be passed through self.mummify to reduce 
          memory use of the remains.   Thsi parameter is ignored for ensembles.
        """
        # set as symbol to mesh with Database api.  It would make no sense 
        # to ever set this False.  Done this way to make that clear
        save_elog = True   
        # warning:  because this method may be called on a datum 
        # with problematic Metadata it could fail if a metadata 
        # value cannot be saved in MongoDB.   
        if isinstance(mspass_object, (TimeSeries, Seismogram)):
            if mspass_object.dead():
                if save_elog:
                    self.db._save_elog(mspass_object,collection=collection)
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
                    self.bury(x,collection=collection,save_elog=save_elog,save_history=save_history,mummify_atomic_data=False)
            if nlive>0:
                newens.set_live()
            return newens
        else:
            message = "Undertaker.bury:  Datum received is not a MsPASS data object\n"
            message += "Type of arg0 received ={}".format(str(type(mspass_object)))
            raise TypeError(message)
        
                    
    def bury_the_dead(self,
                     mspass_object,
                      collection="cemetery",
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
        return self.bury(mspass_object,collection,save_history,mummify_atomic_data)
       

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
        if isinstance(mspass_object,TimeSeries):
            return TimeSeries()
        elif isinstance(mspass_object, Seismogram):
            return Seismogram()
        elif isinstance(mspass_object, (TimeSeriesEnsemble, SeismogramEnsemble)):
            if mspass_object.dead():
               nlive=0
            # Note the indent here so all ensembles pass through this 
            # loop.  Buries all dead members and returns a copy of the 
            # ensembles with the bodies removed
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
                            collection="cemetery",
                            save_history=True,
                            mummify_atomic_data=True,
                      ):
        """
        Seperate an ensemble into live and dead members.  

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
                for d in mspass_object.member:
                    d = self.mummify(d,post_elog,post_history)
                    d.kill()   # likely usually redundant but better to be safe
        else:
            # only need to do anything if we land here if this is an ensemble
            # i.e. we will silently return an atomnic object marked live
            if  isinstance(mspass_object,(TimeSeriesEnsemble,SeismogramEnsemble)):
                for d in mspass_object.member:
                    if d.dead():
                        d = self.mummify(d,post_elog,post_history)
        return mspass_object
