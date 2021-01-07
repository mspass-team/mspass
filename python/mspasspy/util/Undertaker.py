#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import mspasspy.db
from mspasspy.ccore.seismic import (TimeSeriesEnsemble,SeismogramEnsemble)
from mspasspy.ccore.utility import (ErrorSeverity,MsPASSError)
"""
This is a class for handling data marked dead.   The method names are a bit
tongue in cheek but descriptive.
@author: Prof. Gary L. Pavlis, Dept. Earth and Atmos. Sci., Indiana University
"""
class Undertaker(mspasspy.db.Database):
    """
    Class to handle dead data.  Many for ensembles, but has methods to
    save elog entries for Seismogram or TimeSeries objects marked dead.
    A concept of this class is any thing received that is live is not
    changed.   It only handles the dead.
    """
    def __init__(self,dbin):
        self.db=dbin
        self.dbh=self.db.elog
    def bury_the_dead(self,d,save_history=True):
        """
        Clear the contents of an ensemble and optionally save the history and
        error log of the dead.  Return the cleaned ensmble.
        """
        if not (isinstance(d,TimeSeriesEnsemble) or isinstance(d,SeismogramEnsemble)):
            raise MsPASSError('Undertaker.bury_the_dead',
                'Illegal input type - only works with ensemble objects',
                ErrorSeverity.Invalid)
        # This is a pybind11 wrapper not defined in C++ but useful here
        ensmd=d._get_ensemble_md()
        nlive=0
        for x in d.member:
            if x.live:
                nlive+=1
        if isinstance(d,TimeSeriesEnsemble):
            newens=TimeSeriesEnsemble(ensmd,nlive)
        elif isinstance(d,SeismogramEnsemble):
            newens=SeismogramEnsemble(ensmd,nlive)
        else:
            raise MsPASSError('Undertaker.bury_the_dead',
               'Coding error - newens constructor section has invalid type\nThat cannot happen unless the original code was incorrectly changed',
               ErrorSeverity.Invalid)
        for x in d.member:
            if x.live:
                newens.member.append(x)
            else:
                if save_history:
                    self._save_elog(d.id,d.elog)
        return newens

    def cremate(self,d):
        """
        Like bury_the_dead but nothing is preserved of the dead.   Functionally equivalent to 
        bury_the_dead with save_history False, but with a more memorable name.
        """
        dlive=self.bury_the_dead(d,False)
        return dlive

    def bring_out_your_dead(self,d,bury=False):
        """
        Seperate an ensemble into live and dead members.

        :param d:  must be either a TimeSeriesEnsemble or SeismogramEnsemble of
           data to be processed.
        :param bury:  if true the bury_the_dead method will be called on the
           ensemble of dead data before returning
        :return: python list with two elements. 0 is ensemble with live data
           and 1 is ensemble with dead data.
        :rtype:  python list with two components
        """
        if not (isinstance(d,TimeSeriesEnsemble) or isinstance(d,SeismogramEnsemble)):
            raise MsPASSError('Undertaker.bring_out_your_dead',
                'Illegal input type - only works with ensemble objects',
                ErrorSeverity.Invalid)
        # This is a pybind11 wrapper not defined in C++ but useful here
        ensmd=d._get_ensemble_md()
        nlive=0
        for x in d.member:
            if x.live:
                nlive+=1
        ndead=len(d.member)-nlive
        if isinstance(d,TimeSeriesEnsemble):
            newens=TimeSeriesEnsemble(ensmd,nlive)
            bodies=TimeSeriesEnsemble(ensmd,ndead)
        elif isinstance(d,SeismogramEnsemble):
            newens=SeismogramEnsemble(ensmd,nlive)
            bodies=SeismogramEnsemble(ensmd,ndead)
        else:
            raise MsPASSError('Undertaker.bring_out_your_dead',
               'Coding error - newens constructor section has invalid type\nThat cannot happen unless the original code was incorrectly changed',
               ErrorSeverity.Invalid)
        for x in d.member:
            if x.live:
                newens.member.append(x)
            else:
                bodies.member.append(x)
                if bury:
                    self._save_elog(d.id,d.elog)
        return [newens,bodies]
