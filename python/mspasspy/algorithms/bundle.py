#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This is a set of thin wrappers for C++ code to do a process we call 
"bundling".   That is, a Seismogram object can be constructed from 
3 TimeSeries objects spanning a common time period and having the 
same sample rate but with orientation pointing in 3 linearly 
independent directions.   There are a lot of complexities to assembling
such data and manipulating the data objects, which is why the 
process was implemented in C++.   The algorithms here are not completely 
generic and will likely need additions at some future data for nonstandard
data.  "standard" in this context means passive array data archived in 
the SEED (Standard Earthquake Exchange Data) format (aka miniseed which is
a standard subset of seed used for archives).  The algorithms here 
depend upon seismic channels being uniquely defined by four metadata keys:
net, sta, chan, and loc.   Bundles are formed by sorting data in the 
following key order:  net, sta, loc, chan.   The algorithms here are 
further limited to applications to ensembles that are the generalization 
of a reflection seismology "shot gather".   That means an implict assumption
is the ensembles contain data assembled from one and only one event so 
there is a one-to-one relationship between each channel and an event tag.
if data from multiple events or something other than a shot gather 
are to be handled used the BundleGroup function station by station 
sorting the inputs by some other method to create triples of channels 
that can be merged into one Seismogram object.

Created on Mon Jan 11 05:34:10 2021

@author: pavlis
"""
from mspasspy.ccore.algorithms.basic import (_bundle_seed_data,
                                             _BundleSEEDGroup)
from mspasspy.ccore.seismic import TimeSeriesEnsemble
from mspasspy.ccore.utility import (MsPASSError,
                                    ErrorSeverity)


def bundle_seed_data(ensemble):
    """
    This function can be used to take an (unordered) input ensemble of
    TimeSeries objects generated from miniseed data and produce an output
    ensemble of Seismograms produced by bundles linked to the seed name
    codes net, sta, chan, and loc.   An implicit assumption of the algorithm
    used here is that the data are a variant of a shot gather and the
    input ensemble defines one net:sta:chan:loc:time_interval for each
    record that is to be bundled.   It can only properly handle pure
    duplicates for a given net:sta:chan:loc combination.  (i.e. if
    the input has the same TimeSeries defined by net:sta:chan:loc AND
    a common start and end time).   Data with gaps broken into multiple
    net:sta:chan:loc TimeSeries with different start and end times
    will produce incomplete results.   That is, Seismograms in the output
    associated with such inputs will either be killed with an associated
    error log entry or in the best case truncated to the overlap range of
    one of the segments with the gap(s) between.

    Irregular start times of any set of TimeSeries forming a single
    bundle are subject to the same truncation or discard rules described
    in the related function Bundle3C.

    :param ensemble: is the input ensemble of TimeSeries to be processed.
    :type ensemble: :class:`~mspasspy.ccore.seismic.TimeSeriesEnsemble`
    :return: ensemble of Seismogram objects made by bundling input data
    :rtype: :class:`~mspasspy.ccore.seismic.SeismogramEnsemble`
    :exception: Can throw a MsPASSError for a number of conditions.  
    Caller should be enclosed in a handler if run on a large data set. 
    """
    if not isinstance(ensemble, TimeSeriesEnsemble):
        raise MsPASSError("bundle_seed_data:  illegal input - must be a TimeSeriesEnsemble",
                          ErrorSeverity.Invalid)
    try:
        d3c = _bundle_seed_data(ensemble)
    except Exception as err:
        raise MsPASSError('_bundle_seed_data threw an exception - see more messages below',
                          ErrorSeverity.Invalid) from err
    return d3c


def BundleSEEDGroup(d, i0=0, iend=2):
    """
    Combine a grouped set of TimeSeries into one Seismogram.

    A Seismogram object is a bundle of TimeSeries objects that define a
    nonsingular tranformation matrix that can be used to reconstruct vector
    group motion.   That requires three TimeSeries objects that have
    define directions that are linearly independent.   This function does not
    directly test for linear independence but depends upon channel codes
    to assemble one or more bundles needed to build a Seismogram.  The
    algorithm used here is simple and ONLY works if the inputs have been
    sorted so the channels define a group of three unique channel codes.
    For example,
        HHE, HHN, HHZ
    would form a typical seed channel grouping.

    The function will attempt to handle duplicates.  By that I mean
    if the group has two of the same channel code like these sequences:
        HHE, HHE, HHN, HHZ  or HHE, HHN, HHN, HHZ, HHZ
    If the duplicates are pure duplicates there is no complication and
    the result will be clean.   If the time spans of the duplicate
    channels are different the decision of which to use keys on a simple
    idea that is most appropriate for data assembled by event with
    mistakes in associations.  That is, it attempts to scans the group for
    the earliest start time.  When duplicates are found it uses the one
    with a start time closest to the minimum as the one merged to make the
    output Seismogram.

    The output will be marked as dead data with no valid data in one of
    two conditions:  (1)  less than 3 unique channel names or (2) more than
    three inputs with an inconsistent set of SEED names.   That "inconsistent"
    test is obscure and yet another example that SEED is a four letter word.
    Commentary aside, the rules are:
        1.  The net code must be defined and the same in all TimeSeries passed
        2.  The station (sta) code must also be the same for all inputs
        3.  Similarly the loc code must be the same in all inputs.
        4.  Finally, there is a more obscure test on channel names.  They must
    all have the same first two characters.   That is, BHE, BHN, BHN, BHZ
    is ok but BHE, BHN, BHZ, HHE will cause an immediate exit with no
    attempt to resolve the ambiguity - that is viewed a usage error in 
    defining the range of the bundle.


    In all cases where the bundling is not possible the function does not
    throw an exception but does four things:
        1.  Merges the Metadata of all inputs (uses the += operator so only the
            last values of duplicate keys will be preserved in the return)
        2.  If ProcessingHistory is defined in the input they history records  are
            posted to the returned Seismogram using as if the data were live but the
            number of input will always be a number different from 3.
        3.  The return is marked dead.
        4.  The function posts a (hopefully) informative message to elog of the
            returned Seismogram.

    ProcessingHistory is handled internally by this function.  If all the
    components in a group have a nonempty ProcessingHistory the data to link
    the outputs to the inputs will be posted to ProcessingHistory.

    :param d: This is assumed to be an array like object of TimeSeries data 
    that are to be used to build the Seismogram objects.  They must be 
    sorted as described above or the algorithm will fail.   Two typical 
    array like objects to use are the member attribute of a TimeSeriesEnsemble 
    or a python array constructed from a (sorted) collection of TimeSeries 
    objects.
    :type d: :class:`~mspasspy.ccore.seismic.TimeSeriesEnsemble`
    :param i0:  starting array position for constructing output(s).   
    The default is 0 which would be the normal request for an full ensemble 
    or a single grouping assembled by some other mechanism.  A nonzero is
    useful to work through a larger container one Seismogram at a time.
    :type i0: :class:`int`
    :param iend:  end array position.   The function will attempt to 
    assemble one or more Seismograms from TimeSeries in the range 
    d[i0] to d[iend].  Default is 2 for a single Seismogram without 
    duplicates. 
    :type iend: :class:`int`
    """
    d3c = _BundleSEEDGroup(d.member, i0, iend)
    return d3c
