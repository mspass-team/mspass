
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module contains utilities for working with FDSN data in seed format. 
It includes new utillities for working on AWS S3 where some FDSN networks 
now store much or all of their data.   

@author: pavlis
"""

import io
import re   # regular expression module
import yaml
import calendar
import fnmatch
import obspy
import pandas as pd
from abc import ABC, abstractmethod
from enum import Enum, auto
from contextlib import closing
from string import Formatter

import parse

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from earthscope_sdk import EarthScopeClient
from obspy import UTCDateTime

from mspasspy.ccore.utility import MsPASSError,ErrorLogger,ErrorSeverity,Metadata 
from mspasspy.ccore.seismic import TimeSeriesEnsemble,TimeSeriesVector
from mspasspy.util.seismic import has_live_data
from mspasspy.db.normalize import DataFrameCacheMatcher
from mspasspy.db.database import Database
from mspasspy.util.converter import Trace2TimeSeries, Stream2TimeSeriesEnsemble
from mspasspy.util.db_utils import fetch_dbhandle
from mspasspy.util.seismic import print_metadata
from mspasspy.io.s3client import fetch_s3_client
from mspasspy.algorithms.window import merge, WindowData
from mspasspy.algorithms.signals import detrend
#from mspasspy.io.s3client import GeoLabS3Worker, AnonymousS3Worker
# temporary for testing - this didn't exist in io when I first pulled this branch
# remove me and uncomment above for final use
from s3client import GeoLabS3Worker, AnonymousS3Worker
# for testing - will be installed eventually
from stock import datafile

class ChannelPreferences:
    """
    Generic interface to handle complexity of multiple channel codes.
    
    Modern seismology digitizers almost always record data with multiple 
    sample rates.  To make things even more ambiguous observatory stations 
    like the US GSN and related FDSN network stations often have multiple 
    sensor at the same site.  SEED sorts this out in a way that works for 
    data archives but is seriously problematic for data processing.  The 
    SEED way to handle this issue is to use 4 magic strings to 
    uniquely define a particular channel during a particular time period. 
    In MsPASS those magic name are always defined with the metadata keys 
    "net", "sta", "chan", and "loc".   This creates two awkward problems in 
    data processing:
        1.   Most applications will only want the data at one sample rate 
             from one sensor as input to their processing workflow.   Otherwise 
             there are duplicates and/or mismatch in what is expected.
        2.   Indexing with four key is problematic, especially since one 
             of the (loc) is often undefined some variable definition of Null.  
    The object created by this class implementation aims to simplify the 
    interface to addressing this problem.   
    
    The concept of this object is the user needs to define the preference of 
    what net,sta,chan, and loc values are preferred when there is an 
    ambiguity.   It works by loading an (admittedly) complicated data 
    structure from a yaml file.  Users should realize, however, that the 
    object of the class methods is to insulate you for the ugly details of 
    that data structure, which is a necessary evil to address the problem 
    it addresses.  A summary of the key methods are:
        
        `precedence` - returns the order of precedence of a particular 
          unique net, sta, chan, and loc values.
        `component` returns a generic component name for a given 
          net, sta, chan, and loc value.  The generic component names are 
          one of "X","Y","Z".   Normal practice for 3C sensors is they 
          define a right handed coordinate system and the sensor has it's 
          concept of x,y,z defined on the sensor case that defines 
          component outputs. 
        `samprate` - returns the expected sample rate in samples per second 
          for a specified net, sta, chan, and loc value.  SEED requires a 
          unique sample rate for each combination of its magic keys.  
        `preference` and `preference3C` are low level functions that return 
           raw lists that contain the data used by the precedence, 
           component, and samprate methods.  

    Most users will not need to manipulate the data in this object 
    directly but simple use it for algorithms that need to handle 
    the issue of sorting out ambiguous channels.   
    
    The key to defining this class is understanding the format of the 
    yaml file used to construct it.  Here is a brief example for a yaml 
    file for the two GSN network codes with only one defined station 
    for each network::

    .. code-block:: yaml
    
        II:
            PFO: 
                X:
                    - BH1 00 20.0
                    - BH1 10 40.0 
                    - BHE 00 20.0
                Y:
                    - BH2 00 20.0
                    - BH2 10 40.0
                    - BHN 20 40.0
                Z:
                    - BHZ 00 20.0
                    - BHZ 10 40.0
                    - BHZ 20 40.0
            default:
                X:
                    - BH1 00 20.0
                    - BH1 10 40.0 
                    - BHE 20 40.0
                Y:
                    - BH2 00 20.0
                    - BH2 10 40.0
                    - BHN 20 40.0
                Z:
                    - BHZ 00 20.0
                    - BHZ 10 40.0
                    - BHZ 20 40.0
        IU:
            ANMO: 
                X:
                    - BH1 00 40.0
                    - BH1 10 40.0 
                    - HH1 00 100.0
                    - HH1 10 100.0
                Y:
                    - BH2 00 40.0
                    - BH2 10 40.0
                    - HH2 00 100.0
                    - HH2 10 100.0
                Z:
                    - BHZ 00 40.0
                    - BHZ 10 40.0
                    - HHZ 00 100.0
                    - HHZ 10 100.0
            default:
                X:
                    - BH1 00 40.0
                    - BH1 10 40.0 
                    - HH1 00 100.0
                    - HH1 10 100.0
                Y:
                    - BH2 00 40.0
                    - BH2 10 40.0
                    - HH2 00 100.0
                    - HH2 10 100.0
                Z:
                    - BHZ 00 40.0
                    - BHZ 10 40.0
                    - HHZ 00 100.0
                    - HHZ 10 100.0

    Key things to note are:
        1.  The top of the hierarchy is the SEED net code. 
        2.  The second level section is keyed by station names (sta). 
            For this example II has an one station entry for PFO and 
            IU as one station entry for ANMO
        3.  Each net code section MUST have a "default" block as one of the 
            entries at the station level.  It defines the default 
            for any station not listed.
        4.  The 3rd level keys are always the same and define a generic 
            channel name.  They should always be the same as this example.
        4.  At the leaf level is a list of tuples.   The tuples must 
            be in the laid out as (chan loc samprate).   The order of the 
            list defines the level of preference of which channel is 
            to be tried first and which last.   e.g. for default IU "Z"
            BHZ 00 40.0 is considered highest precedence (best) with 
            HHZ 10 100.0 the lowest predence (least desirable)
        5.  The list must be the same length for all generic channels. 
            The most notable example is some stations used to use 
            codes like BHE, BHN, and BHZ but later changed them to 
            BH1, BH2, and BHZ after more precise azimuth measurements of 
            components.  It might then be temping to have a list with 
            two entries for X and Y (e.g. one for BHE and one for BH1)
            and only one for BHZ. If you do that the constructor will 
            throw a MsPASSErorr exception.
    """
    def __init__(self,
                 yaml_file_name=None,
                 ):
        """
        Class constructor.
        
        Currently the only way to construct this thing is by reading 
        a yaml file with a special structure.  See example in the class 
        docstring.
        """
        if yaml_file_name is None:
            yaml_file_name = "ChannelPreferences.yaml"
        ymlpath = datafile(yaml_file_name)
        with open(ymlpath,"r") as fh:
            self.cp = yaml.safe_load(fh)
        self._regularize_yaml_data()
        self._build_cross_references()

    def preference(self,net,sta,component)->list: 
        """
        Return a list of tuples (chancode, loccode, samplerate)
        in order of preference.  Preference means 0 is top prefernce.
        
        This is the primary method for this object.  The returned 
        list can be used in s3 readers to sort out what channels to 
        actually use.  
        
        When running this method in a data assembly loop it should 
        normally be used inside a try block to cat KeyErrors that 
        can heppen if a combination of net,sta, component has no 
        entry in the internally held data structure.  
        """
        # this maybe should be in self but it only appears one other place
        valid_component = ["X","Y","Z"]
        # safety to avoid trivial failure with case error
        component = component.upper()
        if net in self.cp:
            if sta not in self.cp[net]:
                sta = "default"
            if component in valid_component:
                return  self.cp[net][sta][component]
            else:
                message = f'Illegal value for component={component}\n'
                message = f'Must be one of:  {valid_component}'
        else:
            message = f'No data to handle requested net code = {net} '
        raise KeyError("ChannelPrefernces.preference:  " + message)
        
    def preference3C(self, net, sta)->dict:
        """
        Return a dictionary of preference lists for a three component station.
        
        This method is actually more commonly of use than the lower level 
        prefernce method. It returns the prefernce list for a three 
        component station with the dictionary keys the generic component 
        keys "X", "Y", and "Z".  This function is little more than a 
        wrapper around the preference method to build the dictionary 
        for all three components.  
        """
        result = dict()
        for component in ["X","Y","Z"]:
            plist = self.preference(net,sta,component)
            result[component] = plist
        return result
    def component(self,net, sta, chan, loc)->str:
        """
        Return the generic component matching seed network codes.
        
        Returns "undefined" if no valid match can be found.  That can 
        occur two different ways:
            1.  If net has no entries the method cannot do anything
            2.  If net is defined but chan and loc values give do not 
               match the sta or default list it will fail.   
        Caller should handle an "undefined" return.
        """
        # net must be valid for this to work - simple test works 
        # because self.chanloc is derived from sel.cp and all 
        # self.chanloc entries must have a default defined
        if net not in self.cp:
            return "undefined"            
        key = self._cross_reference_key(net, sta, chan, loc)
        if key not in self.chanloc:
           key = self._cross_reference_key(net, "default", chan, loc)
        if key in self.chanloc:
            prec,comp,sr = self.chanloc[key]
        else:
            comp = "undefined"
 
        return comp
    def precedence(self, net, sta, chan, loc)->int:
        """
        Returns the precedence value for specified net, sta, chan, and loc 
        seed code.  Return a -1 if the code is not defined in this object. 
        
        Returns -1 if no valid match can be found.  That can 
        occur two different ways:
            1.  If net has no entries the method cannot do anything
            2.  If net is defined but chan and loc values give do not 
               match the sta or default list it will fail.   
        Caller should handle a -1 return.
        """
        # net must be valid for this to work - simple test works 
        # because self.chanloc is derived from sel.cp and all 
        # self.chanloc entries must have a default defined
        if net not in self.cp:
            return -1          
        key = self._cross_reference_key(net, sta, chan, loc)
        if key not in self.chanloc:
           key = self._cross_reference_key(net, "default", chan, loc)
        if key in self.chanloc:
            prec,comp,sr = self.chanloc[key]
        else:
            prec = -1
 
        return prec
    
    def samprate(self, net, sta, chan, loc)->float:
        """
        Returns the expected sample rate for specified net, sta, chan, and loc 
        seed code.  Return a -1.0 if the code is not defined in this object. 
        
        Returns -1.0 if no valid match can be found.  That can 
        occur two different ways:
            1.  If net has no entries the method cannot do anything
            2.  If net is defined but chan and loc values give do not 
               match the sta or default list it will fail.   
        Caller should handle a -1.0 return.
        """
        # net must be valid for this to work - simple test works 
        # because self.chanloc is derived from sel.cp and all 
        # self.chanloc entries must have a default defined
        if net not in self.cp:
            return -1.0          
        key = self._cross_reference_key(net, sta, chan, loc)
        if key not in self.chanloc:
           key = self._cross_reference_key(net, "default", chan, loc)
        if key in self.chanloc:
            prec,comp,sr = self.chanloc[key]
        else:
            sr = -1.0
 
        return sr
                    
    def _regularize_yaml_data(self):
        """
        Parses the raw result read from the yaml file and validates
        the result has no errors that could cause failure.  At the 
        same time it does a type conversion of the list of strings 
        the yaml file contain for each generic channel definition
        to a tuple with expected types.  It checks:
            1.  Each net must have a "default" for sta
            2.  Checks the keys in channel section match required list
            3.  Checks the loc codes allows flexibility in how to define 
                the common case of a null value.  It accept an empty string
                and any mixed case version of none, null, or undefined.  
        The leaves of the self.cp data structure are converted to a list 
        of tuples of the form (sta, loc, samprate) with the order of the 
        list defining the order of preference.  
        
        The function with throw an exception for missing data but 
        will silently handle use of mixed cass null values for loc.
        Note the entire file is scanned and errors accumulated.  The 
        method returns only if the message buffer is emtpy.  That allows 
        parsing a potentially large yaml file and no having to run the 
        function over and over to find multiple errors.
        
        Note null loc codes in the internal structure are set to a zero 
        length string ("").   That allows the reciprocal map to be 
        constructed easily from a chan_loc string.  That is how the 
        internal cross reference used for the component method is set up.
        """
        valid_component = ["X","Y","Z"]
        # lower case because the actual token is converted to loer 
        # case before testing.  Allows things like None or NONE
        null_loc_strings = ["none","null","undefined"]
        error_message = ""
        for net in self.cp.keys():
            stations = self.cp[net]
            if "default" not in stations:
                error_message += f'definition for net={net} is missing required default definitions\n'
            for sta in stations.keys():
                channels = stations[sta]
                sizecheck=dict()
                for chan in channels.keys():
                    if chan not in valid_component:
                        error_message += f'Invalid generic channel key={chan} for {net} {sta}\n'
                    
                    # the list returned in the yaml file is list of strings
                    # we convert them to a duple of appropriate types
                    clist = channels[chan]
                    tuple_list = list()
                    for cl in clist:
                        seedchan,loc,srstr = cl.split()
                        if len(loc)==0 or loc in null_loc_strings:
                            loc = ""
                        elif len(loc)!=2:
                            error_message += f'Invalid specification for loc code = {loc} '
                            error_message += f'in channel preference list for {net} {sta}\n'
                            error_message += "FDSN standard requires loc code be exactly 2 characters"
                            raise ValueError(error_message)
                        sr = float(srstr)
                        tuple_list.append(tuple([seedchan,loc,sr]))
                    # replace the list of strings with regularized tuples
                    self.cp[net][sta][chan] = tuple_list
                    sizecheck[chan] = len(tuple_list)
                size_mismatch=False
                tsize = sizecheck[valid_component[0]]
                for i in range(1,3):
                    if sizecheck[valid_component[i]]!=tsize:
                        size_mismatch=True
                if size_mismatch:
                    error_message += f'channel preference list sizes for station {sta} are not consistent\n'
                    error_message += 'Lists for generic X, Y, and Z must be the same length\n'
                    error_message += 'Edit the yaml file and try again.'
            if len(error_message)>0:
                full_message = 'ChannelPreferences._regularize_yaml_file:  '
                full_message += 'yaml file was parsed successfully but contained one or more errors.  List of errors follow:\n'
                full_message += error_message
                raise MsPASSError(full_message,ErrorSeverity.Fatal)
            
                    
    @staticmethod
    def _cross_reference_key(net,sta,chan,loc):
        # handle loc carefully
        if loc is None:
            loc = ""
        return f"{net}_{sta}_{chan}_{loc}"
    def _build_cross_references(self):
        """
        Builds cross refererence to speed query by channel and loc code.
        
        The data structure dirctly constructed from the yaml file is naturally 
        indexed by an array index order of precedence.   We often want to 
        ask the revese question of what the precedence is for a specificied 
        channel and loc code.  This private method does that and should 
        normally be called only inside the constructor after self.cp is 
        already defined.  
        
        What it does is build a simple dictionary keyed by a composite of 
        net, sta, chan, and loc that can be used to fetch the precedence 
        value and generic component name ('X','Y', or 'Z') from seed 
        channel codes.  
        """
        self.chanloc = dict()
        for net in self.cp.keys():
            stations = self.cp[net]
            for sta in stations.keys():
                channels = stations[sta]
                for generic_chan in channels.keys():
                    preflist = self.cp[net][sta][generic_chan]
                    for pref in range(len(preflist)):    
                        [chan,loc,sr] = self.cp[net][sta][generic_chan][pref]
                    
                        nsclkey = self._cross_reference_key(net, sta, chan, loc)
                        self.chanloc[nsclkey] = [pref,generic_chan,sr]
                        


class EnsembleMatcher(DataFrameCacheMatcher):
    """
    Special matcher object used by clean_ensembles. 
    
    This class is actually a fairly generic way to create a matcher 
    using multiple string keys from DataFrame input.  It needs some 
    work to be fully generic and was constructed to use with clean_ensmbles. 
    Some work would be needed to make it a more generic interface for 
    using a DataFrame table with string index keys.  We use it here for 
    indexing with seed net and sta values in clean_ensmble. 
    """
    def __init__(self,
                 df,
                 matchkeys,
                 defaults=None,
                 ):
        """
        class constructor. 
        
        :param df:  input DataFrame.   
        :param machkeys:  list of keys of the df column names to be used 
           as the index for this DataFrame used as a cache by the object. 
        :param defaults:  placeholer currently alway None.  Put here 
            because I realized in fixing some issues with DataFrameCacheMatcher
            that DataFrameCacheMatcher needs that option.   It should 
            contain a list of defaults for any Null attributes in df. 
            For how this constructor will throw a NotImplementedError 
            if defaults is not None.
        """
        if defaults is not None:
            raise NotImplementedError("EnsembleMatcher constructor:  defaults feeature not yet implemented")
        self.matchkeys = matchkeys
        # assumes default loads all DataFrame attributes and sets 
        # prepend_collection_name False 
        # TODO:   datagrame matcher has a bug here 
        super().__init__(df,prepend_collection_name=False)
        # make sure the cache has the matchkeys defined
        cache_keys = self.cache.keys()
        message = ""
        for k in self.matchkeys:
            if k not in cache_keys:
                message += f'DataFrame used constructor does not contain required matchkey={k}\n'
        if len(message)>0:
            raise ValueError('EnsembleMatcher constructor failed:\n' + message)
                
        # set up defaults dictionary in self
        self.cache.set_index(matchkeys, inplace=True)
        self.cache.sort_index(inplace=True)
    def subset(self,mspass_object):
        """
        Subset the cache to those with data.   Note if the mspass_object is 
        missing the required key it will silently return None which means 
        the match failed.  That seems the right choice for that condition 
        but could produce mysterious results if that is not documented 
        clearly.   i.e. the caller should guarantee require keys are 
        present before trying to use this with find or find_one.
        (both methods use this as the selection)
        """
        vals2match=list()
        for k in self.matchkeys:
            if k in mspass_object:
                vals2match.append(mspass_object[k])
            else:
                vals2match.append(self.defaults[k])
        valid_key = self.cache.index.intersection([tuple(vals2match)])
        result_df = self.cache.loc[valid_key]
        return result_df
        
                    
def clean_ensemble(ens,chanpref,require_3C=True):
    """
    Clean an ensemble using the ChannelPreferences object.
    
    The complexity of modern seismic observatories is a major complication 
    in processing large data volumes.  The complexity is that one station 
    can commonly have dozens of different channels with different sample
    rates and in many cases from different sensors.   SEED distinguishes 
    those by requireing the string keys that in MsPASS we refer to as 
    net, sta, chan, and loc.   The ChannelPreferences class defined in this
    module was designed to define how a workflow should automatically decide
    which channel to use when confronted with similar data that are 
    near duplicates.   This function is designed to apply the preferences 
    defined in a ChannelPreferences object to an input ensemble (ens).  
    The output of the function is a new ensemble with what the function 
    determines are duplicates removed.  What it defines to be a duplicates 
    depends on the setting of the boolean `require_3C`.  When that is 
    False the algorithm filters data to return the generic component 
    for each station it finds in ens with the highest preference 
    defined by the ChannelPreferences.preference method for that station 
    and generic componet.   When `require_3C` is True (default) the 
    algorithm is more dogmatic and will only return data from stations 
    that have at least one complete set of a generic three-component data.  
    If more than one is present the one with the highest preference value 
    is pushed to the output and the others are dropped.  
    
    Be warned this function looks only at what exists in the ensemble not 
    the actual content of the waveforms.   There are many cases where 
    one channel of a three component set can be bad but still recorded and 
    not obviously bad. 
    
    :param ens:  input TimeSeriesEnsemble.   The function will throw a 
      TypeError if it is anything else.
    :param chanpref:   ChannelPrefernces object (defined in this module) 
      defining how duplicate channels are to handled and what channels are 
      to be retained.  Note only channels defined in chanpref at all will 
      be considered for inclusion in the output. 
    :param require_3C:   When True (default) a complete set of generic X, Y, 
      and Z components must be present for each station in ens or no data 
      from that station will appear in the output.  When False it is less 
      dogmatic and will include data with less than 3 channels.  Set False 
      if you are handling a mix of 3C and scalar data in ens.  
    """
    alg = "clean_ensemble"
    # must be dogmatic about the args
    if not isinstance(ens,TimeSeriesEnsemble):
        message = '{}:  arg0 must be a TimeSeriesEnsemble.  Received data of type={}'.format(alg,type(ens))
        raise TypeError(message)
    if not isinstance(chanpref,ChannelPreferences):
        message = '{}:  arg1 must be a ChannelPreference object.  Received data of type={}'.format(alg,type(chanpref))
        raise TypeError(message)
    if ens.dead():
        return ens
    # this is the container for the edited output
    ensout = TimeSeriesEnsemble(Metadata(ens),len(ens.member))
    # alternative was explored below.  Want to see if this would make 
    # more sense - delete me if you use this
    doclist = list()
    for i in range(len(ens.member)):
        d = ens.member[i]
        if d.live:
            if "net" in d and "sta" in d:
                doc={"net" : d["net"],
                     "sta" : d["sta"] 
                     }
                if "chan" in d:
                    doc["chan"] = d["chan"]
                if "loc" in d:
                    doc["loc"] = d["loc"]
                doc["member_index"] = i
                doclist.append(doc)
    df = pd.DataFrame(doclist)
    matcher = EnsembleMatcher(df, matchkeys=["net","sta"])
    # create a set of all unique net-sta combinations 
    netsta = dict()
    for d in ens.member:
        if d.live:
            if "net" in d and "sta" in d:
                net = d["net"]
                sta = d["sta"]
                if net in netsta:
                    staset = netsta[net]
                else:
                    staset=set()
                staset.add(sta)
                netsta[net] = staset
    for net in netsta:
        staset = netsta[net]
        for sta in staset:
            md2match = Metadata()
            md2match["net"] = net
            md2match["sta"] = sta
            mdlist,elog  = matcher.find(md2match)
            # find returns None if failed and then elog will contain something
            # for testing will pritn elog but if I use the algorithm elog 
            # whould be posted to the cleaned ensmble
            if mdlist is None:
                print("Problems handling net=",net," and sta=",sta)
                print(elog.get_error_log())
            else:
                # initialize a list with empty dictionaries
                # we fill in the dictionaries with mdlist 
                MAXCODES=5
                this_sta = list()
                for i in range(MAXCODES):
                    this_sta.append(dict())
                for md in mdlist:
                    chan = md["chan"]
                    if "loc" in md:
                        loc = md["loc"]
                    else:
                        loc = None
                    comp = chanpref.component(net,sta,chan,loc)
                    # skip channels not defined by the chanpref object
                    # these sre the returns of methods to handle that condition
                    if comp=="undefined":
                        continue
                    prec = chanpref.precedence(net,sta,chan,loc)
                    if prec<0:
                        continue
                    # for now let these throw an exception.  May need 
                    # to handle error conditions here
                    this_sta[prec][comp]=md["member_index"]
                # the logic of this loop is a bit weird and maybe could 
                # be done a cleaner way
                members2keep = list()
                for i in range(MAXCODES):
                    comps = this_sta[i]
                    # these two conditionals could be combined with an or 
                    # but this layout makes the logic clearer
                    if len(comps)==0:
                        continue
                    if require_3C and len(comps)<3:
                        continue
                    # note the way charef works it is not possible for 
                    # len(comps) to be greaer than 3
                    for k in comps:
                        members2keep.append(comps[k])
                    break
                for i in members2keep:
                    ensout.member.append(ens.member[i])
    if has_live_data(ensout)>0:
        ensout.set_live()
    return ensout

class PayloadType(Enum):
    """
    Used to define the finite set of ways data providers bundle data 
    in object store.   
      station_day - continuous data with all channels for that day in one 
        package
      channel_day - continuous data with one channel per object
      source_gather - event data bundled with all data for one event 
        bundled in one object.
        
    Others may be added but currently those are the only known options.
    Note if you are not familiar with python enum you can set a value 
    similar to C++ but without a declaration.  e.g. you can create one
    of these with a construct like:  x=PayloadType.station_day
    """
    station_day = auto()
    channel_day = auto()
    source_gather = auto()

class PayloadFormat(Enum):
    """
    Used to define the format of the payload returned by the provider. 
    
    For seismic data for now this is only miniseed.  This API, however, 
    could support almost anything by adding to this Enum and/or 
    the PayloadType Enum.
    
    Will add a couplee harmless, near zero length, values that might 
    prove useful down the road
    """
    miniseed = auto()
    stationxml = auto()
    quakeml = auto()
    
class DataAccess(Enum):
    """
    Used to define types of access setups for using AWS S3.
    
    Currently there are two established ways to setup a cloud 
    connection to seismic data archives.  There may be more so this 
    provides a standardized interface to add new methods if and when 
    they are needed.
    
    open_aws_s3 - as the name implies says a provider setup exists for 
      open data access via AWS S3 storage. 
    earthscope_aws_s3 - Earthscope currently provides data on AWS S3 \
      that are not open and require earthscope login credentials.  This 
      value means that is the expected access method. 
    """
    open_aws_s3 = auto()
    earthscope_aws_s3 = auto()

class BlankLocCodeMethod(Enum):
    empty_string = ("")
    # this defines a single char filler as done by CI.  Highly conceivable
    # someone would use a two character fill to match SEED loc size of 2
    # if that happens add a fill_string option here and set self.fill_string 
    # to that 2 character code 
    fill_character = ("_")
    def __init__(self,filler: str = ""):
        self.fill_string = filler
    def set_fill_string(self,filler: str = "_"):
        match self:
            case BlankLocCodeMethod.empty_string:
                self.fill_string = ""
            case BlankLocCodeMethod.fill_character:
                self.fill_string = filler
            
    def get_filler(self) -> str:
        """
        Returns the padding string representing an empty location code
        based on the strategy.
        """
        match self:
            case BlankLocCodeMethod.empty_string:
                return ""
            case BlankLocCodeMethod.fill_character:
                return self.fill_string

    @property
    def default_delimiter(self) -> str:
        """Default delimiter used when fill_string strategy is active."""
        return "_"

def safe_parse(format_str: str, objname: str, extra_types: dict = None) -> dict:
    """
    Regularizes error handling of variable format string parser.  
    
    The S3SeismicProvider class in this module for some operations 
    needs to do a form of inverse format handling.   The reason is that 
    all current providers use an object naming convention that contains 
    fundamental Metadata that defines the payload content.   That is 
    used in S3SeismcProvider to build a cheap data index that doesn't 
    require actually looking into data files to extract needed Metadata.
    Currently that is seed network codes and time files but it is conceivable
    similar constructs might evolve with other formats.  
    
    S3 providers use the s3 object names to define key attributes for 
    data stored in individual objects.   There is no standard so every 
    provider presntly handles this differently.  This module uses the 
    parse module to parse a format string to extract metadata from s3 object
    names.  That can fail in multiple ways.  This regularizes all possible 
    failures of the parser reformatting error messages and packaging them 
    into a MsPASSError object. That includes failure by the parser which 
    can silently return nothing in some cases.   
    
    :param format_str:  the pythons format specification to be cracked.  
    :param objname:  s3 object name that format_str would create in reverse operation
    :param extra_types:  feature of parse.compile.  See parse module documentation
    """
    alg="safe_parse"

    try:
        parser = parse.compile(format_str, extra_types=extra_types)
    except parse.RepeatNameError as e:
        message = f"Conflicting repeated field names in format string '{format_str}'\n"
        message += f"RepeatNameError content: {e}"
        raise MsPASSError(alg,message,ErrorSeverity.Invalid)
    except ValueError as e:
        message = f"Invalid format or syntax in format string='{format_str}'\n"
        message += f"ValueError message content: {e}"
        raise MsPASSError(alg,message,ErrorSeverity.Invalid)
    except Exception as e:
        message = f"Unexpected exception thrown by parse.compile for format string '{format_str}': {e}"
        raise MsPASSError(alg,message,ErrorSeverity.Invalid)
    try:
        result = parser.parse(objname)
        # parse will silently return nothing for a whole range of reasons
        # rather than return None we wrap this in a MsPASSError object too
        if result is None:
            message = f"parse failed and returned a None parsing `{objname}' with format string '{format_str}'"
            raise MsPASSError(alg,message,ErrorSeverity.Invalid)
        else:
            return result.named   # converts parser output to dict
    except MsPASSError:
        raise
    except (ValueError, TypeError) as e:
        message = f"Type conversion failed while parsing '{objname}'\n"
        message += f"parser error message content: {e}"
        raise MsPASSError(alg,message,ErrorSeverity.Invalid)
    except Exception as e:
        message = f"Unexpected runtime error while parsing '{objname}': {e}"
        raise MsPASSError(alg,message,ErrorSeverity.Invalid)

class BasicProvider(ABC):
    """
    Abstract base class defining API for a cloud data provider.
    
    This class exists to provide some standardization for the strong 
    possibility some future provider will utilize a different cloud 
    provider than AWS S3 - currently only S3 stores are used but
    there is no reason to think that will always be the case.
    """
    def __init__(self,doc=None):
        """
        Base class constructor.  May do nothing or set some common 
        attributes defined by dictionary doc.
        """
        pass
    @abstractmethod
    def networks(self)->list:
        """
        Return a list of SEED network codes this provider serves.
        """
        pass
    @abstractmethod
    def payload_type(self,key)->PayloadType:
        """
        Return the type of payload this provider returns for 
        specified string key.  For seismic data the key would be 
        something like network code, but for other types of data 
        it could be something else.   See definition of PaylodType 
        for list of what enum class PayloadType are supported.  
        """
        pass
    @abstractmethod
    def payload_format(self,key)->PayloadFormat:
        """
        Returns format of data returned for specified string key.  
        For seismic waveform data the key would normally be a network code. 
        The same could be used for station metadata (staionxml).   
        Other types of data could be supported by expanding PyloadFormat 
        options.
        """
        pass
    
        
class S3SeismicProvider(BasicProvider):
    """
    This class encapsulates the concept of a provider for waveform 
    data stored on S3. 
    
    S3 data is corrently managed my multiple organizations.   Each 
    organization manages one or more SEED network codes.   An instance 
    of this class needs to encapsulate all the information needed to 
    fetch data from a single provider.  It does not and should never hold 
    a client as an attribute.   An implementation should always hold a 
    client in memory for serial jobs and utilize the MsPASS 
    mspasspy.io.s3_worker_plugin for dask use.  Multiple clients are needed 
    if a workflow is pulling data from multiple providers as each 
    provider must have an independent client instance or nothing will work.
    
    It is important to recognize that some providers serve both 
    continuous waveform data and packaged up segmented data for 
    "events" in their catalog.  Current examples are SCEDC and NCEDC but 
    there will likely be more or already are that I'm not aware of.  
    Because event data is indexed and defined in a very different way from 
    continuous data archives different instances of this object will 
    be needed to access data from the same provider for working with 
    the continuous or event data.  This implementation cannot handle both 
    simultaneously.  
    
    On the other hand, a provider like Earthscope serve data from multiple 
    networks.  For that reason most of the class methods have a net argument 
    to sort out which net code to request.  
    
    This class extends BasicProvider by adding a long list of s3 specific 
    method documented with this class.
    
    The constructor for this class uses what are commonly called factor 
    methods to allow very divergent methods of constructing the object.  The 
    two facor methods are eastiest to show by example:
        
        .. code-block:: python
        
        # default constructor from file uses standard yaml file in data/yaml
        p = S3SeismicProvider.from_file()
        # Construct from a string defining the path
        p = S3SeismicProvider.from_file(fname_string,"Earthscope")
        # construct from MongoDB database pulling from db for provider SCEDC 
        p = S3SeismicProvider.from_database(db,"SCEDC")
        # Construct from a dictionary (doc).  Note this doesn't use factor method
        p = S3SeismicProvider(doc,"NCEDC")
    """
    def __init__(self, config_dict: dict, provider: str = "Earthscope"):
        self._from_dict(config_dict, provider)

    @classmethod
    def from_file(cls, filename: str = "S3SeismicProvider.yaml", provider: str = "Earthscope"):
        """
        Factor method to construct from a yaml file.
        """
        ymlpath = datafile(filename)
        with open(ymlpath,"r") as fh:
            ymldata = yaml.safe_load(fh)
        return cls(ymldata,provider=provider)
    @classmethod
    def from_database(cls, 
                      db: Database, 
                      provider: str="Earthscope", 
                      collection: str="S3Provider",
                      ):
        """
        Factor method to construct from a MongoDB database.
        """
        query = {"provider" : provider}
        doc = db[collection].find_one(query)
        if doc is None:
            message = f"S3SeismicProvider constructor:  collection={collection} has no documents for provider={provider}"
            raise ValueError(message)
        return cls(doc, provider=provider)
    def _from_dict(self, doc: dict, key: str):
        """
        this one does the actual work - all other factory methods are wrappers
        """
        if key not in doc:
            message = f"S3SeismicProvider constructor:   no data found for provider=={key}"
            raise ValueError(message)
        self.provider = key
        self.nets = doc[key]
    def networks(self)->list:
        """
        Return a list of net codes served by this provider.
        """
        return self.nets.keys()
    def is_open(self,net)->bool:
        """
        Return True if the data for network code `net` is open data.
        Return False if additional credentials are required.  This method 
        is mostly a wrapper for the access method
        """
        access = self.access(net)
        if access == DataAccess.open_aws_s3:
            return True
        else:
            return False
                
    def region(self, net)->str:
        """
        Return the AWS server farm location commonly tagged with the 
        keyword "region".   An assumption is one provider does not hold 
        data in mulitple server farms.  True today but perhaps needs 
        a net qualitier.  
        """
        val = self._fetch_subdoc_data("region", net, "region")
        return val
    def set_access_keys(self,net,doc):
        """
        Supply a set of access data for network code net.   doc should be 
        a dictionary like object containing required data with keys 
        to be defined.  This provides a generic mechanism to handle 
        complexity like embargoed PASSCAL data managed by Earthscope. 
        There could easily be other schemes people come up with so this 
        provides a simpler way to support such likely possibilities. 
        """
        raise RuntimeError("S3SeismicProvider.set_access_key method is not implemented yet - needs input from Earthscope")
    def payload_type(self,net)->PayloadType:
        """
        Implemention of this abstract method for S3.   Allows PayloadType 
        to be more than seismic object but sets enum by keywords.  
        A single net code can only have one payload type for this implementation.
        Returns a PayloadType object that is used internally and can 
        be printed for testing.
        """
        payload = self._fetch_subdoc_data("payload_type", net, "payload_structure")
        # cast the result into the PayloadType enum from string internal storage
        match payload:
            case "station_day":
                return PayloadType.station_day 
            case "channel_day":
                return PayloadType.channel_day
            case "source_gather" | "event_file":
                return PayloadType.source_gather
            case _:
                message = f"S3SeismicProvider.payload_type: net={net} has invalid value={payload} for payload_structure attribute"
                raise ValueError(message)
        
    def payload_format(self,net)->PayloadFormat:
        """
        Implementation of abstract method for S3.  Returns a PayloadFormat 
        object. Mostly for internal use in this implementation to make the 
        class more generic.
        """
        f = self._fetch_subdoc_data("payload_format", net, "data_format", default_value="miniseed")
        match f:
            case "miniseed":
                return PayloadFormat.miniseed
            case _:
                message = f"S3SeismicProvider.payload_format:  illegal value for data_format={f} for net={net}\n"
                message = "Currently this method only support miniseed"
                raise ValueError(message)
    def loc_code_strategy(self,net)->BlankLocCodeMethod:
        """
        Return the strategy used to handle location (loc) code for network code net.
        
        There is a universal complication in FDSN data in handling the 
        seed channel attribute called the "location code".   The other for 
        the infamous quadralateral (net, sta, and chan) by convention are 
        always required to be defined.   The location code (loc in MsPASS) 
        is often left null.   The reason is that by convention it is only 
        defined for a site with multiple sensors with comparable specs.
        Then it is a necessary evil to define which sensor's data to use.  
        FDSN providers serving data from S3 all encode the seed quadrilateral 
        in s3 object names.   Because loc is optional they then need a way 
        to set a null value in the s3 object name.   This abstracts that 
        process by returning a BlankLocCodeMethod class that defines a finite 
        set of rules for handling this problem.   This method is used 
        internally for handling s3 object names, but can be useful in 
        other workflows dealing with s3 stores by other mechanisms.  
        """
        blank_loc_val = self._fetch_subdoc_data("loc_code_strategy", net, "blank_loc_code_value", default_value="")
        match blank_loc_val:
            case "":
                return BlankLocCodeMethod.empty_string
            case _:
                if isinstance(blank_loc_val,str):
                    if len(blank_loc_val)==1:
                        blcm = BlankLocCodeMethod.fill_character
                        blcm.set_fill_string(blank_loc_val)
                        return blcm
                else:
                    message = f'S3SeismicProvider.loc_code_strategy:  illegal value for blank_loc_code_value argument = {blank_loc_val}\n'
                    message += 'Must be an empty string or a single character (default _ used by SCEDC)'
                    raise ValueError(message)
                    
        
    def prefix(self,net,object_organization="day_volumes")->str:
        """
        Return the prefix string for specified net code.  
        Optionally allow object_organization to be some other keyword. 
        Known example is "events" that would be useful for SCEDC and NCEDC 
        who serve segmented event data.
        """
        return self._fetch_subdoc_data("prefix", net, "prefix")
    def bucket(self,net,object_organization="day_volumes")->str:
        """
        Return the string defining the bucket name to access for specified 
        net code. object_organization like prefix method description.
        """
        return self._fetch_subdoc_data("bucket", net, "bucket")
    def object_format(self,net)->str:
        """
        Return the object name format string that is required for the yaml 
        file or other ways this is constructed.
        """
        return self._fetch_subdoc_data("object_name_format", net, "object_format")
    
    def access(self,net)->DataAccess:
        """
        Standardize asking the type of data access require for a network net.
        
        Curently there are two distinct ways to access FDSN data on the 
        cloud:  open data portals and through Earthscope.  The former 
        has fewer complexities and the later quite a few related to 
        data access credentials.   This method is mostly used internally 
        for defining access dependencies in code blocks, but it can be 
        useful to end users.   The return is a class that is a child of enum 
        to allow cleaner limits on the allowed values.   
        """
        if 'access' in self.nets[net]:
            access = self.nets[net]['access']
        else:
            # let earhscope be the default
            access = 'earthscope-sdk'
            print(f"S3SeismicProvider.access:  Warning, {self.provider} access value is not defined for network code={net} ")
            print("Using default='earthscope'")
        match access:
            # insist on qualifiers in case other open data portals come 
            # into existence.  Note the weird python anomaly of needing the 
            # | symbol instead of the wordy "or".  Needed because or evaluates
            # to a boolean
            case "open_s3" | "open_aws_s3":
                return DataAccess.open_aws_s3
            case "earthscope-sdk":
                return DataAccess.earthscope_aws_s3
            case _:
                message = f"S3SeismicProvider.access:  illegal value provided for access={access} for net code {net}.  Fix configuration in yaml file or Database"
                raise ValueError(message)

    def s3client(self,net):
        """
        Return an s3 client for accessing data for this network for serial workflows.
        
        This method should be used to instantiate an isntance of and s3 
        client set up appropriately to access data for the network defined
        by the net argument.  The client produces is NOT stored in this 
        object but should be held in the memory space of a driver script 
        or instantiated on dask workers for a parallel job.  Call this 
        method before calling any of the methods that require an s3 client. 
        
        :param net:  net code of the network the client should be set up 
        to access.
        """
        # this key-value pair is not hidden behind a method.  
        # is_open could beused the access is intended to be ore open 
        # ended to add flixibility.   
        access = self.access(net)
        match access:
            case DataAccess.open_aws_s3:
                region=self.region(net)
                session = boto3.Session(region_name=region)
                s3client = session.client("s3",config=Config(signature_version=UNSIGNED))
            case DataAccess.earthscope_aws_s3:
                esclient = EarthScopeClient()
                creds = esclient.user.get_aws_credentials()
                session = boto3.Session(
                    aws_access_key_id=creds.aws_access_key_id,
                    aws_secret_access_key=creds.aws_secret_access_key.get_secret_value(),
                    aws_session_token=creds.aws_session_token.get_secret_value(),
                    region_name = self.region(net)
                )
                
            case _:
                message = f'S3SeismicProvider.s3client:  access velue = {access} not supported\n'
                message += 'Must be one of:  open, earthscope-sdk'
                raise ValueError(message)
        return s3client
                
    def initialize_dask_workers(self, client, net):
        """
        Set up a dask cluster to run parallel s3 access for netcode defined by net.
        
        Setting up s3 clients on a dask cluster is a very different thing than 
        for a serial python script.   For efficiency it is essential workers
        creata a memory resident s3 client they use for interacting with 
        AWS S3.   We do that here and elsewere in MsPASS using a dask 
        WorkerPlugin implementation.   The ones used here are defined 
        in the mspass module mspasspy.io.s3client.   This method simplifies 
        setting that up for a given network by using required details 
        defined in the objects self attributes.   When running parallel 
        jobs the way this should be used is a variant of the following:
            
        ..code-block:: python
        
          # instantiate an instance of this object - change myconfiguration.yaml as appropriate
          provider = S3SeismicProvider('mmyconfiguration.yaml')
          for net in provider.networks():
              provider.initialize_dask_workers(net)
              
        For single net code providers like SCEDC the loop is not necessary
        and a more rational approach is to only run this method for net 
        codes your workflow actually requires.   
        
        :param client:  client to that defines the dask cluster to use.
        :type client:  Can be either a mspasspy.client.Client 
           (what we call a MsPASS client) or a dask.distributed.Client 
           object (what we commonly call a dask client).  If anything else 
           the method will throw a ValueError exception.
        """
        alg = "S3SeismicProvider.initialize_dask_workers"
        if is_mspass_client(client):
            dask_client = client.get_scheduler()
        elif is_dask_client(client):
            dask_client = client 
        else:
            message = f'{alg}:  client(arg0) must be either a mspass client or a dask client\n'
            message = 'Received an object of type={type(client)}'
            raise ValueError(message)
        if net not in self.networks():
            message = f'{alg}:  provider={self.provider} does not have data for net code={net}'
            raise ValueError(message)
        worker_key = f"S3client_{net}"
        access = self.access(net)
        match access:
            # note default is not needed for this match block as we can't get 
            # here without a valid access value being set via the access method
            case DataAccess.open_aws_s3:
                plugin = AnonymousS3Worker(key=worker_key,region=self.region(net))
                dask_client.register_client(plugin)
            case DataAccess.earthscope_aws_s3:
                plugin = GeoLabS3Worker(key=worker_key,region=self.region(net))
                dask_client.register_client(plugin)
                
    def leaf_content(self,s3client,doc,server_data_key="server_data",)->list:
        """
        Return the s3 equivalent of ls of a directory.
        
        All current providers serve data with object names that simulate 
        a file system.   In S3 directory path names are simulated with 
        a string that looks like a directory name used as a prefix for 
        grouping file objects.   For continuous data in all current cases 
        that is waveforms for each day stored either by station or channel.
        For event files SCEDC, for example, puts all the waveforms for 
        each event in a single miniseed file with many channels in the same 
        object.   Thus the leaf content depends upon the provider and the 
        type of data being served.   
        
        This method should return a list of object names with a prefix 
        built from the document received.   That provides a flexible way 
        to supply variable input without overloading which python doesn't 
        really support. 
    
        """
        alg = "S3SeismicProvider.leaf_content"
        # make sure s3client is valid - easy to not be so
        if not is_s3_client(s3client):
            message = f"{alg}:  arg0 does not appear to be a valid S3 client"
            raise TypeError(message)
        if "net" not in doc:
            message = f"{alg}:  arg1 document is missing required value for net attribute"
            raise ValueError(message)
        net = doc["net"]
        fmt = self.object_format(net)
        prefix = self.prefix((net))
        fmt = extract_prefix_format_string(fmt,prefix=prefix)
        bucket = self.bucket(net)
        dir_prefix = fmt.format(**doc)
        keys = set()
        server_data = {}
        # One ListObjectsV2 response is capped at 1,000 keys.  Propagate listing
        # failures rather than turning an authentication outage into an empty day.
        for response in s3client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=dir_prefix, Delimiter="/"
        ):
            keys.update(obj["Key"].split("#")[0] for obj in response.get("Contents", []))
            server_data = response.get("ResponseMetadata", {})
        return [sorted(keys), {server_data_key: server_data}]
    def day_volume_object_name(self,
                               net,
                               sta,
                               year,
                               jday,
                               chan=None,
                               loc=None,
                               include_prefix=True,
                               )->str:
        """
        Standardize generation of day volume object names from seed channel codes.
        
        Different providers create S3 object names simulating directory 
        structures universally used for organizing waveform files when 
        data is stored on a file system.  Providers structured their data 
        differently and used different naming conventions.   All, at present, 
        however, create a unique string from some combination of a 
        day definition (currently all use year and julian day (jday)) and 
        at least net and sta codes.  Those that store single channel day 
        files also include a chan code and have some method of handling 
        undefined loc codes.  This method aims to always yield a valid object 
        name based on the rules for that net code defined by the 
        "object_format" keyed value loaded into the object's self area.   
        This function should work provided it gets input that is rational. 
        When loading station payload data like Earthscope only net and sta 
        are required.  For single channel objects chan is require and 
        the input yaml file or database entry must contain a definition 
        with the verbose key named "blank_loc_code_value".   It determines 
        what (if anything) replaces a blank loc code in constructing the 
        object name.  The two know examples today are NCEDC drops loc 
        code so the value of that is "".  SCEDC inserts an underscore so 
        the value is "_".   The MsPASS team intends to keep a master yaml 
        file up to date if other net codes emerge with any other anomalies
        in naming.  The point is to use this method as the standard way 
        to construct an object for the data you want to retrieve based on 
        seed net codes and time.  Use the listing method if you want to 
        use the actual holdings to drive processing.  
        
        The interpray of the net, sta, year, jday, chan, and loc arguments 
        is as described above.  The only additional complexity is that
        if include_prefix is set False (default is True) the name 
        will be returned with the prefix string missing.   The default 
        returns a string that can be used as a value passsed to the Key 
        argument of s3 clients get_object method. 
        
        """
        alg = '3SeismicProvider.day_volume_object_name'
        # frozen mspass namespace hre
        doc = {"net" : net,
               "sta" : sta,
               "year" : year,
               "jday" : jday,
               }
        fmt = self.object_format(net)
        keys = []
        for _, field_name, _, _ in Formatter().parse(fmt):
            if field_name is not None:
                # Split by ':' in case there's a format specifier (e.g., 'jday:03d' -> 'jday')
                clean_key = field_name.split(":")[0]
                keys.append(clean_key)

        # need this in case string duplicates a key  which is common for providers
        unique_keys = list(dict.fromkeys(keys))
        # sta and loc arem't alway required and are handled in irregular ways
        # this validates input and handles anomalies using optional data
        # from the yaml file
        if "loc" in unique_keys:
            if loc is None and "blank_loc_code_value" in self.nets[net]:
                loc = self.nets[net]["blank_loc_code_value"]
            else:
                message='S3SeismicProvider.day_volume_object_name:  '
                message = f'{alg}:  loc code is required by net {net} but object has no definition for handling a null loc code\n'
                message += 'Edit yaml file used to build this object'
                raise ValueError(message)
            doc['loc'] = loc
        if "chan" in unique_keys:
            if chan is None:
                message = f'{alg}:  chan code is required by network={net}.  You must specify a channel code for chan argument'
                raise ValueError(message)
            doc['chan'] = chan
        objname = parse_format_string(fmt, doc)
        if include_prefix:
            objname = self.prefix(net) + objname
        return objname
        
    def get_year_day_object_names(self,
                               s3client,
                               net,
                               year,
                               jday,
                               separator="/",
                               server_data_key="server_data",
                               )->list:
        """
        Get a list of object names for a specified day this provider holds. 
        
        All providers I know of today serve day volumes as individual s3 object.
        When payload_format is "station_day" only net, sta, year, and jday 
        are needed to define a unique object.  For "channel_day" chan and 
        loc need to be specified to get a unique match.  By default this 
        method should return a list of all object names for a specified 
        julian day (jday), year, and network code.  
        
        This is a less generic method that is similar in concept to 
        the leaf_content method.  This is a convenience function for 
        day volumes that, at least at the present, have some common 
        structure between providers.   
        """
        # make sure s3client is valid - easy to not be so
        if not is_s3_client(s3client):
            message = "S3SeismicProvider.year_day_object_names:  arg0 does not appear to be a valid S3 client"
            raise TypeError(message)
        fmt = self.object_format(net)
        prefix = self.prefix((net))
        fmt = extract_prefix_format_string(fmt,prefix=prefix)
        bucket = self.bucket(net)
        # this works only because the argument symbol names match 
        # required namespace for the format string.  String formatting 
        # will silently ignore extra kwargs in a format so even if all 
        # aren't defined this works
        day_prefix = fmt.format(net=net,year=year,jday=jday)
        keys = set()
        server_data = {}
        # One ListObjectsV2 response is capped at 1,000 keys.  Propagate listing
        # failures rather than turning an authentication outage into an empty day.
        for response in s3client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=day_prefix, Delimiter="/"
        ):
            keys.update(obj["Key"].split("#")[0] for obj in response.get("Contents", []))
            server_data = response.get("ResponseMetadata", {})
        return [sorted(keys), {server_data_key: server_data}]
        
    def get_event_volume_object_names(self,
                                 s3client,
                                 event_id,
                                 )->list:
        """
        If a network serves event data use this method to return all 
        objects for that event.  event_id can be whatever is used.   
        I think this should be a list as normally one would want all 
        data for a given event.  May need another method to just a single 
        channel for a particular event or a group of channels. TBD - I need 
        to dig into SCEDC and NCEDC documentation a bit.

        """
        raise RuntimeError("get_event_volume_object_names not yet implemented")
    def exists(self,s3client, object_name)->bool:
        """
        Abstract the ugly process of validing if an object exists with 
        boto3 functions.  Return True if boto3 list_objects_v2 shows
        the object_name exists.   Normally would be run following a 
        call to the "day_volume_object_name" or "event_object_name"
        methods.
        """
        raise RuntimeError("exists method not yet implemented")
    def get_raw_object_data(self,
                            s3client, 
                            net, 
                            object_name, 
                            use_net_prefix=False)->tuple:
        """
        Fetch the raw vector of bytes for a particular object.
        
        There are two stages in getting data from S3:  (1) pulling 
        the linear block of data (bytes) holding the content the object 
        defines and (2) passing the bytes to a reader that knows how to 
        handle that binary blob of data.   This method abstracts step 1 
        for S3.  It makes this method more generic as the same method
        can, for example, be used to download day volumes of any structure 
        or event data bundled into a single object.   
        
        The function handles any exception in a MsPASS way.   That is, 
        errors are posted to an ErrorLogger object.  The function always 
        returns a tuple.  Component 0 of that tuple contains the byte 
        string of data download. If, however, something went wrong 
        component 0 will contain a None AND component 1 will contain 
        an ErrorLogger object that is not empty.  Callers should test 
        the return and trap either the condition that 0 is None or that
        the ErrorLogger has content.   
        
        :param s3client:  s3 client to use for downloading this object
        :param net:  network code (string) to which this object is associated.
        :param object_name:   normally the Key value to use to access the 
          data requested from the bucket defined internally for specified 
          net value.  If use_net_prefix is set True (default is False) 
          the Key value is constructed by prepending the "prefix" for this 
          network code (self.prefix[net] value).  
        :return:  tuple with 0 being the data array and 1 containing an 
          ErrorLogger object.  On success the ErrorLogger will be empty. 
          The size method of ErrorLogger should be test for validity of 
          the content in 0.
        """
        alg = "S3SeismicProvider.get_object"
        # make sure s3client is valid - easy to not be so
        if not is_s3_client(s3client):
            message = f"{alg}:  arg0 does not appear to be a valid S3 client"
            raise TypeError(message)
        bucket = self.bucket(net)
        elog = ErrorLogger()
        try:
            if use_net_prefix:
                fullkey = self.prefix(net) + object_name
            else:
                fullkey = object_name
            response = s3client.get_object(Bucket=bucket,Key=fullkey)
            with closing(response["Body"]) as body:
                data = body.read()
            return [data,elog]
        except ClientError as e:
            message = 'get_object method failed for object name={}'.format(object_name)
            message += 'Content of boto3.ClientErro exception caught={}'.format(str(e))
            elog.log_error(alg,message,ErrorSeverity.Invalid)
            return [None,elog]
        except Exception as ex:
            message = 'get_object method failed for object name={}'.format(object_name)
            message += 'Something threw an unexpected exception with this message: '
            message += str(ex)
            elog.log_error(alg,message,ErrorSeverity.Invalid)
            return [None,elog]
    def decode_waveform_data(self,
                        rawbytes,
                        net,
                        chanlist=None,
                        preferences=None,
                        obspy_select_string=None,
                        auxmd=None,
                        ):
        """
        Decode byte stream downloaded with get_object. 
        
        There are two stages in getting data from S3:  (1) pulling 
        the linear block of data (bytes) holding the content the object 
        defines and (2) passing the bytes to a reader that knows how to 
        handle that binary blob of data.  This method abstracts step 2 
        for seismic waveform data.   It uses internally stored attributes 
        loaded during construction to know how to actually do the decoding. 
        A key idea of this object is to hide those details behind this 
        function so this function always returns either nothing or a 
        valid MsPASS seismic data object. It will throw an exception only 
        if something completely unexpected happens.  
        
        Currently, these are known complexities this function handles.  
        The list will likely grow with experience as real data have 
        warts that I can't see from this distance:
            1.   For station bundles like Earthscope serves some stations 
                 like the TA put log data in miniseed packets.  obspy's reader 
                 handles that is a mismatch of concept with mspass seismic 
                 data objects.  To handle that the function immediately 
                 clears any channels from an internally generated obspy 
                 Stream that have the channel code "LOG".
            2.   The output type of this method is different for single 
                 channel objects and station bundled objects.  When 
                 used with the former a TimeSeries is returned while for 
                 the later a TimeSeriesEnsemble is returned. 
            3.   With station bundles one almost never wants all the channels.  
                 There are optional filters for handling that with the 
                 keys "obspy_select_string", "chanlist", and "preferences".  
                 See below for how each option is used to filter the data 
                 that is actually returned. The default for stationed 
                 bundled data is to return everything sans the LOG data.
                 Note also that the function enforces a rule that only 
                 one of the filter options can be enabled.   It will throw 
                 a ValueError exception if more than one are enabled. 
        
        :param rawbytes: byte string returned by get_object to be decoded.
        :param net:  network code of data to decode.  That is needed because 
           different net codes can have different data structure and formats.
        :param obspy_select_string: optional string to filter data returned 
           by applying the Stream.select method.  Is is CRITICAL to realize this\
           string only can be used for channel select.  i.e. the actual usage 
           internall is `stream = stream.select(channel=obspy_select_string)`
           Default is None which means that line is not executed.
        :param chanlist:  optionally return only channels matching codes in a
            list.  e.g. if `chanlist=['BHE','BHN',"BHZ"] return only data 
            with those chan codes.   Note that cannot filter with loc codes 
            but is simpler than the prefereces option for some data.  
            This options is disabled by default. 
        :param preferences: filter data using a ChannelPreferences object.
           A ChannelPreferences is a generic way to select only a subset 
           defined by rules you define.   It is most useful for handling 
           GSN data where most now have multiple sensors and which channels 
           are definitive for that station are ambiguous.  It can also be 
           useful if channel names change or any other time dependent 
           channel naming is a concern.  See the docstring for 
           ChannelPreferences for how to define that beast.  Default is 
           None which disables this option.
        :param auxmd: optional auxiliary metadata to post to the output. 
           For ensemble returns these data are posted to the ensembles 
           metadata container while for TimeSeries they are posted to 
           the Metadata container of the TimeSeries.
        """
        alg = "S3SeismicProvider.decode_raw_data"
        # some miniseed data contain things other than seismic data that 
        # break conversions to MsPASS TimeSeries. Starting with LOG but 
        # may need to be added to with experience
        chan_to_always_delete = ["LOG"]
        # make sure all the select options are turned off or only one 
        # of the options is enabled.
        _validate_selection_method(alg, chanlist, preferences, obspy_select_string)
        ptype = self.payload_type(net)
        pfmt = self.payload_format(net)
        # for onw we only handle mseed
        if pfmt == PayloadFormat.miniseed:
            fmt = "mseed"   # what obspy requires 
        else:
            message = f"{alg}:  cannot handle payload format={pfmt} defined for net={net}"
            raise ValueError(message)
        try:
            with io.BytesIO(rawbytes) as buffer:
                stream = obspy.read(buffer, format=fmt)
            del rawbytes
            match ptype:
                case PayloadType.station_day:
                    # note the logic her only works because we guarantee 
                    # multiple options cannot be set
                    if obspy_select_string is not None:
                        stream = stream.select(channel=obspy_select_string)
                    # clean out channels that will break Stream2TimeSeriesEnsemble
                    stream.traces[:] = [tr for tr in stream if tr.stats.channel not in chan_to_always_delete]
                    mspass_object = Stream2TimeSeriesEnsemble(stream)
                    if chanlist is not None:
                        mspass_object = self._select_channels(mspass_object,chanlist)
                    if preferences is not None:
                        mspass_object = clean_ensemble(mspass_object,preferences)
                case PayloadType.channel_day:
                    mspass_object = Trace2TimeSeries(stream[0])
                case PayloadType.source_gather:
                    mspass_object = Stream2TimeSeriesEnsemble(stream)
                case _:
                    # this trap is probably unnecessary
                    message = f'{alg}:  objects internal payload type={ptype} is undefined\n'
                    message = 'This should not happen and implies something is corrupted'
                    raise ValueError(message)
            del stream
            if auxmd is not None:
                mspass_object += auxmd
        except Exception as e:
            message = f'{alg}:  Something threw an unexpected exception this message\n{e}'
        return mspass_object




    def objname2doc(self, net: str, objname: str, verbose: bool=False) -> dict:
        """
        Parse an S3 object name from an FDSN provider and return the 
        metdata parsed as a python dictionary.
        
        This method is the workhorse of the index_day method used to 
        build a MongoDB index for a scalable method for handling S3 data. 
        Currently all provider index their seismic day files with a complicated
        s3 object name that mimics a file system.   That name string always 
        contains a set of metadata we often want to split apart as more 
        easily accessible values.  This function provides a generic solution 
        to that problem returning a dictionary where the keys are any 
        of the string in the format string defining the object name.  It 
        fetches the format information with method of this object called 
        "object_format".  net is required as an argument to that method 
        as the format can be network specific.   That is essential as some 
        providers (e.g. SCEDC is a known villain but there is no reason to 
        think they will be the only ones to create such an abomination.) 
        
        This function can be thought of as the inverse of the generic 
        function below called parse_format_string.  I creates a string 
        from an input document.  This does the reverse and creates a 
        document from a string with a particular format.  An example 
        can make this clearer.  The standard yaml file to construct an 
        S3SeismicProvider has this entry for the format for net code "TA" 
        (Earthscope transportable array) served by provider "Earthscope":
        "{net:2}/{year}/{jday:03d}/{sta}.{net:2}.{year}.{jday:03d}".  
        An example full path objname handled by this function is:
        "miniseed/TA/2004/131/109C.TA.2004.131".   Noting that 
        "miniseed" is the result of the prefix method for the "TA" net code,
        "TA" is the "{net:2}" field, "2004" is the "{year}", etc.   That 
        particular example would be parsed to this dict (in json format)::
            
            "s3key": "miniseed/TA/2004/131/109C.TA.2004.131",
            "net": "TA",
            "year": 2004,
            "jday": 131,
            "starttime": 1084147200.0,
            "endtime": 1084233600.0,
            "sta": "109C",

        where "s3key" is inserted by this function.  It is the value 
        of objname passed to the function.  
        
        That may sound simpler than it is, but variations in the details 
        add a huge amount of complexity to make this function bombproof. 
        There are so many ways this can go wrong the approach used was to 
        try a sequence of approaches to crack the format string.  It tries 
        the following approaches in sequence.  "In sequence" means it only 
        goes to the next approach if it fails::
            
            1)  Apply the format string directly.
            2)  Edit the format string to force fixed length seed codes. 
                i.e. net is forced to 2, chan is forced to 3, year is 
                forced to 4, and loc (if defined) is set to 2 characters.
            3)  The last ditch approach splits the format string into 
                tokens using all currently used separators.  That is, 
                "/",".", and "_".   objname is then split into tokens 
                using the same separators.   At this point we enter the 
                danger zone of undefined loc codes and how a provider 
                handles that.  There are currently only two ways that is 
                done defined by the enum subclass BlankLocCodeMethod.  
                That is either an empty string or a single character.  
                Currently SCEDC does that and inserts an extra "_" 
                when the loc code is undefined.   This function 
                handles those two examples now.  If someone does something 
                different in the future it could easily break this parser.  
                One more vote for why SEED is a four-letter word.   
        
        This function is bombproof and should never throw an exception. 
        If none of the parsing approachs succeeed it will return an 
        empty dictionary.   To diagnose problems with enable the verbose 
        option that is Fale by default.  
        """

        def enforce_types(doc):
            """
            Force year and jday to be always be converted to integers.
            
            The parser used by objname2doc will sometimes return year 
            and jday as a string.  That happens with simple strings used
            to define the tokens like "{year}".   One an also specify a
            specific format like "{year:04d}" and the parser will return 
            an int.  for consistnecy this guarantee year and day are always 
            returned by objname2doc as integers.  "doc" is assumed to be 
            a dictionary.  It is edited in place and the edited result 
            returend.
            """
            int_keys = ["year","jday"]
            for k in int_keys:
                if k in doc:
                    if isinstance(doc[k],str):
                        # if conversion fails from a parsing error
                        # cooerce the error into a MsPASSError to be 
                        # consistent with safe_parse
                        try:
                            doc[k] = int(doc[k])
                        except ValueError:
                            message = f"string field parsed for {k} = {doc[k]} could not be converted to an integer.  Assumed to be a parsing error"
                            raise MsPASSError("enforce_types",message,ErrorSeverity.Invalid)
            return doc
        FDSN_S3_FIELD_SEPARATORS="./_"
        format_str = self.object_format(net)
        # Fetch prefix with prefix method and strip it if present
        prefix = self.prefix(net).rstrip('/') + '/'

        if objname.startswith(prefix):
            objname = objname[len(prefix):]
    
        # 1. Strip optional leading S3 path prefixes (e.g., 'continuous_waveforms/')
        if "/" in format_str and not format_str.startswith("/"):
            rel_format_prefix = format_str.split("{")[0]
            if rel_format_prefix in objname:
                objname = objname[objname.find(rel_format_prefix):]
        # first try to parse with what we are given. 
        try:
            result = safe_parse(format_str, objname)
            result = enforce_types(result)
            return result
        except MsPASSError as e1:
            if verbose:
                print("objname2doc failed with objname=",objname, " using format=",format_str)
                print("Error message:  ",e1)
                print("Trying fixed field editing of format string")
        enhanced_format_str = format_str
    
        # 2. Unify specifiers for fields that repeat or sit adjacent to others:
        
        if '{net}' in format_str:
            enhanced_format_str = enhanced_format_str.replace('{net}', '{net:2}')
        if '{chan}' in format_str:
            enhanced_format_str = enhanced_format_str.replace('{chan}', '{chan:3}')
        if '{loc}' in format_str:
            enhanced_format_str = enhanced_format_str.replace('{loc}', '{loc:2}')
        if '{year}' in format_str:
            enhanced_format_str = enhanced_format_str.replace('{year}', '{year:04d}')
        # currently all providers using leading 0s to make jday a 3 character field
        # this is fragile as that assumption could be violated in the future
        # with this logic it should be ok if the format string sets it correctly
        if '{jday}' in format_str:
            enhanced_format_str = enhanced_format_str.replace('{jday}', '{jday:03d}')
        try:
            result = safe_parse(enhanced_format_str, objname)
            result = enforce_types(result)
            return result
        except MsPASSError as e2:
            if verbose:
                print("objname2doc failed with objectname=",
                      objname,
                      " using edited format string=",
                      enhanced_format_str)
                print("Error message:  ",e2)
                print("Trying to split format string and combine pieces")
        try:
            # Last ditch method - split the format string into tokens 
            # and extract what we can.  
            result = self.split_and_parse(enhanced_format_str,objname,FDSN_S3_FIELD_SEPARATORS,verbose=verbose)
            if result is None:
                locstrategy = self.loc_code_strategy(net)
                match locstrategy:
                    case BlankLocCodeMethod.fill_character:
                        fill_char = locstrategy.get_filler()
                        if fill_char  not in FDSN_S3_FIELD_SEPARATORS:
                            split_str =  FDSN_S3_FIELD_SEPARATORS + fill_char
                        else:
                            split_str = FDSN_S3_FIELD_SEPARATORS
                    case BlankLocCodeMethod.empty_string:
                        fill_char = ""
                        split_str = FDSN_S3_FIELD_SEPARATORS
                # remove the {loc} field in the format string
                enhanced_format_str = re.sub(r'\{loc:2\}', fill_char, enhanced_format_str)
                result = self.split_and_parse(enhanced_format_str,objname,split_str,verbose=verbose)
                if result is None:
                    if verbose:
                        print("objname2doc:   Giving up - returning an empty dictionary")
                    result = dict()
                else:
                    try:
                        result = enforce_types(result)
                    except MsPASSError as e:
                        if verbose:
                            message="objname2doc:  split and parse method failed\n"
                            message+="year and/or jday strings could not be converted to int\n"
                            message+= f"Error posted: {e}"
                            print(message)
                        return dict()
            return result
        except Exception as e:
            print("objname2doc:  Something threw unexpected exception with message: ",e)
            print("Returning emtpy dictionary")
            return dict()
                    
    def split_and_parse(self,format_str,objname,separators,verbose=False)->dict | None:
        """
        Companion to above.  Splits format_str and objname into 
        tokens using regex string separators to define split.  
        Returns a None if the number of tokens from format and objname 
        do not match - that is how we handle null loc code values
        that will create that mismatch because the loc value is missing.
        """
        # Only keys matching this list will be treated as valid 
        # It means the output may not be complete but it won't fail
        allowed_keys = ["net","vnet","sta","chan","loc","year","jday"]
        split_str = f"[{separators}]+"
        f_tokens = re.split(split_str,format_str)
        obj_tokens = re.split(split_str,objname)
        if len(f_tokens) != len(obj_tokens):
            return None
        else:
            N = len(f_tokens)
            doc = dict()
            for i in range(N):
                try:
                    tokdoc = safe_parse(f_tokens[i], obj_tokens[i])
                    # only insert key-value paris where the key makes sense
                    # warn if it was set before and changed
                    for k in tokdoc:
                        if k in allowed_keys:
                            if k not in doc:
                                doc[k] = tokdoc[k]
                            elif doc[k] != tokdoc[k]:
                                # make this bombproof
                                # this will overwrite if ther eare more than 
                                # one inconsistent values associated with k
                                # not possible with an existing provider
                                key = k + "_v2"
                                doc[key] = tokdoc[k]
                                if verbose:
                                    print("Warning:  key=",k," had conflicting values of ",doc[k]," and ",tokdoc[k])
                                    print("Saving ",tokdoc[k]," with key=",key)
                except MsPASSError as e:
                    if verbose:
                        print(f"split_and_parse:  failure with format {f_tokens[i]} and {obj_tokens[i]}")
                        print("Error message: ",e)
                    
            return doc

 
    @staticmethod
    def _select_channels_by_list(ens,chanlist)->TimeSeriesEnsemble:
        """
        From input ensemble ens return a copy with only channels matching
        those in chanlist.  
        """
        # this constructor copies metadata and calls std::reserve for 
        # the size given as arg1.   The size may be too large but too large 
        # is better than too small here to avoid reallocs.   The size of the 
        # pointer array created by reserve is tiny
        ensout = TimeSeriesEnsemble(Metadata(ens),len(ens.member))

        for d in ens.member:
            if "chan" in d:
                chan = d["chan"]
                if chan in chanlist:
                    ensout.member.append(d)
        # the constructor used does not set ensout live.  This is a failsafe
        # way to only set it live if ensout has any live data.  
        # the most common case here will be an empty ensemble. 
        # note the alternative number_live function will not work this context
        if has_live_data(ensout):
            ensout.set_live()
        return ensout
    def _fetch_subdoc_data(self, method_name, net, key, default_value=None):
        """
        Common code to fetch attributes from self.nets used by 
        convenience methods that fetch data from that data structure 
        by concept.  
        
        method_name is needed to create an unambiguous message idenfying method 
           using this private method
        net is the netword code 
        key is the attribute being fetched from the document for net in self.nets[net]
        default_value should be set of the method has a fixed default that 
        can be used if an attribute is not defined in self.nets for net code net
        """
        if net not in self.nets:
            message = f"{self.__name__}.{method_name}:  This provider has no data for net code={net}"
            raise ValueError(message)
        else:
            doc = self.nets[net]
            if key in doc:
                return doc[key]
            elif default_value is not None:
                return default_value 
            else:
                message = f"{self.__name__}.{method_name}:  provider data for net code={net} is missing required attribute={key}"
                raise ValueError(message)
                
def parse_format_string(format_string,doc)->str:
    """
    Return a string using format specification with values from doc. 
    
    This function is a generic way to create a string using values stored 
    in a dictionary.   The format string follows that used in strings 
    but is guided driven by the dictionary data.   It is easiest to 
    understand form an example.   It we defined
    `format_string="datadir/{net}/{year}"` and had:
        
        doc={"net": TA,  "year" : 2010}
    
    The output of this function would be "datadir/TA?2010".
    
    The function looks for keys in the format string and will throw an 
    informative messsage in a ValueError if keys are missing in doc.
    """
    # first get unique keys from the format string using string.Formatter
    keys = []
    for _, field_name, _, _ in Formatter().parse(format_string):
        if field_name is not None:
            # Split by ':' in case there's a format specifier (e.g., 'jday:03d' -> 'jday')
            clean_key = field_name.split(":")[0]
            keys.append(clean_key)

    # need this in case string duplicates a key  which is common for providers
    unique_keys = list(dict.fromkeys(keys))
    # make sure the doc has all those keys before we try to create the 
    # object name
    missing_keys=list()
    for key in unique_keys:
        if key not in doc:
            missing_keys.append(key)
    if len(missing_keys)>0:
        message = f"parse_format_string:  missing required key(s)={missing_keys}"
        raise ValueError(message)
    # now load this dictionary with the key-value pairs we need
    # don't use doc directly as it could contain extra stuff
    format_variables = dict()
    for k in unique_keys:
        format_variables[k] = doc[k]
    result = format_string.format(**format_variables)
    return result
def extract_prefix_format_string(fmt,separator="/",prefix=None)->str:
    """
    Extract the prefix (dir equivalent) from the provider's object format string.
    
    Several methods of S3SeismicProvider need to construct a format string 
    that defines only the prefix for object store.   The format string
    cached in the S3SeismicProvider data area defines the full object name. 
    All providers currently simulate file systems so what this returns is 
    a format string that is like a directory name.  
    
    :param fmt:  full format string including a trailing portion defining a 
       unique object - that is stripped by this function.
    :param separator:  Today all providers use a unix directory separator 
      for defining a path so "/" is the default.  For Winders, however, 
      this could be "\" but not providers at present simulate a Winders 
      file system.  
    :param prefix:  optional constant prefix to add at front of return \
      string.   Used in this module to prepend the "prefix" value 
      returned by the prefix method.  This string should contain a trailing 
      separator character but if it does not it will always be silenetly 
      added.  This is needed to match an oddity of how boto3 handles 
      separators.   
    :return:  edited verison of fmt with trailing portion removed.
    """
    tokens = fmt.split(separator)
    if prefix is None:
        outstr = ""
    else:
        n = len(prefix)
        if prefix[n-1] == separator:
            outstr = prefix
        else:
            outstr = prefix + separator
    # this should work even if tokens has no leading dir elements at all
    # should not happen but at least it shouldn't throw some mysterous 
    # exception. 
    for i in range(len(tokens)-1):
        outstr += tokens[i]
        outstr += "/"
    return outstr
    
def is_s3_client(obj: object) -> bool:
    """
    Fast but not bombproof check if obj is an s3 client object.   

    Several methods of S3SeismicProvider require a valid s3 client object 
    as input.  Checking of a client is valid, says Gemini, is not so 
    trivial.  This code is a variant of what was suggesed by Gemini 
    for a fast way to validate if obj is likely a valid s3 client object. 
    Apparently you can't know for sure without using it and initiating a 
    network overhead.  This avoids that and just does a sanity check to 
    avoid obvious errors that would be easy for any user to commit.
    
    What this does is look if obj has some attributes the are in a valid
    s3 client object.
    """
    if not hasattr(obj, "meta"):
        return False
    service_model = getattr(obj.meta, "service_model", None)
    if service_model.service_name != "s3":
        return False
    # these are methods the clients used here must have defined do 
    # we check they exist too
    required_methods = ["list_objects_v2", "get_object", "head_bucket"]
    for method in required_methods:
        if not callable(getattr(obj, method, None)):
            return False
    return True
        
def is_dask_client(obj) -> bool:
    """
    Fast way to check if an obj is a dask client without needing to 
    import the monster dask.distributed module.  
    """
    # Check class name and module path without requiring dask to be pre-imported
    return (
        type(obj).__name__ == "Client"
        and type(obj).__module__ == "dask.distributed.client"
    )
def is_mspass_client(obj) -> bool:
    """
    Returns True if obj is an instance of mspasspy.client.Client without importing mspasspy.
    Use similar to is_dask_client
    """
    cls = getattr(obj, "__class__", None)
    if cls is None:
        return False
    return (
        cls.__name__ == "Client"
        and cls.__module__ == "mspasspy.client"
    )
    

def index_day(net,
              year,
              jday,
              dbname_or_handle,
              provider,
              s3_client=None,
              dask_worker_key="geolab_s3_client",
              channel_select=None,
              collection="wf_s3",
              parse_failure_collection="s3_parse_failures",
              verbose=False,
              print_summary=False,
              )->tuple:
    """
    Low level function to build an index for data stored on S3 for a 
    single net code for one day.  
    
    Day files are currently stored by providers on s3 with object names 
    that mimic a file system where the file names encode key metadata 
    needed to locate a specific station's data or a particular channel of 
    data.  Experience has shown that indexing the data objects outside 
    a provider with MongoDB provides a very efficient way to extract 
    pieces of the giant archive all providers maintain that are needed for 
    a particular research project.  This function provides a generic way 
    to do that process for any current network hosting data on S3.  
    This function writes documents to a MongoDB in a collection that 
    by default is called 'wf_s3'.   Those documents provide an approximate 
    index of day volumes for a data set of interest.    This function 
    needs be be thought of as an indexer for one network day.   i.e. what 
    it does is create documents for each object stored for network code 
    net for a specified year and jday.  (required args 0 and 1).  
    The main value this adds over a simpler scheme is the documents this 
    function produces contain the following metadata:
        net - network code
        starttime - approximate start of day volume
        endtime - approximate end of day volume
        s3key - s3 object name this index entry applies to
        year - year the data in this day volume was recorded
        jay - day of the year this day volume was recorded
        
    In addition, if the object name encodes them these may be defined
        sta - station code (this should always be defined unless there is an error)
        chan - channel code
        loc - location code (if applicable)  Beware it may be parsed as an empty string
        
    Most users should expect to use this code in some varation of this 
    serial code:
        
        ..code-block:: python
        
            for net in network_code_list:
                for year,day in year_day_list:
                    nset,ndoc = index_day(net,year,day,db,provider,s3client)
    
    For serial processing s3_client must be defined.  Default assumes 
    parallel processing with dask with workers fetching the s3 client 
    via the plugin using the key defined by the dask_worker_key argument.  
    Similarly dbname_or_handle uses the standard mspass mechanism of 
    getting a database handle via the fetch_dbhandle function.
    
    :param net:   network code to index.  Must be defined for S3SeismicProvider 
       object passed by provider argument or this function will fail.
    :type net:  string 
    :param year:
    :param jday:   all current providers store data in the equivalent of a day 
       directory structure.  year and jday define what prefix is used to 
       fetch the proper contents.  Be aware the provider.get_year_day_object_names 
       is used to fetch the leaf object name for net, year, and day tuples.
    :param dbname_or_handle:  As he name suggests but use the actual Database 
       object only for serial workflows.  In a parallel workflow you must use 
       the name only.
    :param provider:  Instance of an S3SeismicProvider that defines data for
       the specified network code net.  
    :param s3_client:  parallel jobs should use the default None for this 
       optional argument.  That usage assumes a dask worker can fetch 
       an s3 client with the key defined by the dask_worker_key argument.
       For serial workflows this must define an instance of an s3 client 
       appropriate for this network code.  Normal use for serial jobs 
       should be to create one from the provider.s3client method to a
       assure consistency.  
    :param dask_worker_key:  name defining s3 client for dask workers.  
       Ignore for serial jobs.
    :param channel_select:   string defining a "standard glob pattern" 
       for selection of channels.   By default (set None) all channels 
       for a day will be indexed.   Use this with and expression like 
       "BH?" or "H*" to limit channel codes indexed.   Note this 
       has no effect with station_day payloads as available channels 
       can only be determined by examining the payload content.  
       For station_day formats it is almost always desirable as providers
       like SCEDC have large numbers of channels on each data that are 
       not normally desired (e.g. state of health channels). The index 
       can get huge if all channels are included from providres with 
       channel_day payloads. 
    :param collection:  MongoDB collection name to write documents created 
       by this function.  Default of "wf_s3" should be changed with caution.
    :param parse_failure_collection:  s3 object names that could not be 
       successfully parsed to build an index entry will be inserted in this 
       collection name.  It is a bit like the cemetery collection as it holds 
       failures.  Necessary evil as the volume of error messages when parse 
       errors occur can be overwhelming.  Most parsing errors result from 
       format string errors in the yaml file so any use of that format 
       can produce a flood of errors. Hence, they are pushed to a special 
       collection.
    :param verbose: as the name suggests.  Use this to debug parsing errors 
       for s3 object names.  Best enabled after a run creates a ton of 
       documents in the parse_failure_collection collection.   WARNING:  if 
       you are having parsing errors it do not run verbose with a large 
       number of days as the error output can be huge. 
       
     :return: tuple containing a summery of results::  
         
         component 0 - number of documents saved to wf_s3
         component 1 - number of s3 object names handled
         component 2 - number dropped from parsing failures
         component 3 - number skipped based on channel_select option
         component 4 - dictionary of inconsistent net codes
    
      You should find 0+2+3 = 1.  Any content of the dictionary returne din 
      component 4 should be viewed as bad.  If the dictionary in 4 is not 
      empty it means there is a naming issue of s3 objects for the net 
      codes with counts there.  The values are the number of object names 
      with a net value encoded that does not match the net code expected.  
    """
    alg = "index_day"
    db = fetch_dbhandle(dbname_or_handle)
    if s3_client is None:
        s3_client = fetch_s3_client(parallel=True,
                                    worker_data_key=dask_worker_key,
                                    )
    elif not is_s3_client(s3_client):
        message = f'{alg}:  Illegal value for s3_client argument={type(s3_client)}\n'
        message += 'Must be a boto3 client object'
        raise ValueError(message)
    # let this throw an exception if provider is ill defined
    s3objlist,auxmd = provider.get_year_day_object_names(s3_client,net,year,jday)
    doclist=list()
    number_failures=0
    n_skipped=0
    inconsistent_net_codes = dict()
    for objname in s3objlist:
        doc = dict()
        doc["s3key"] = objname
        doc["net"] = net
        doc["year"] = year
        doc["jday"] = jday
        starttime = UTCDateTime(year=year, julday=jday)
        doc["starttime"] = starttime.timestamp
        endtime = starttime.timestamp + 86400.0
        doc["endtime"] = endtime
        if auxmd is not None:
            for k in auxmd.keys():
                # this maybe should warn if k overwrites an existing key
                doc[k] = auxmd[k]
        doc_from_objname = provider.objname2doc(net,objname,verbose=verbose)
        
        # provider.objnam2doc should never fail but if it fails completely 
        # it returns an empty dict container. We treat those documents 
        # a bit like dead seismic data object pushing them as we find 
        # them to a special collection
        if len(doc_from_objname)==0:
            db[parse_failure_collection].insert_one(doc)
            number_failures += 1
        else:
            # handle null loc codes consistently have null's not in 
            # the document at all
            if "loc" in doc_from_objname:
                locstrategy = provider.loc_code_strategy(net)
                match locstrategy:
                    case BlankLocCodeMethod.fill_character:
                        fill_char = locstrategy.get_filler()
                        if doc_from_objname["loc"] == fill_char:
                            doc_from_objname.pop("loc")
                    case BlankLocCodeMethod.empty_string:
                        if len(doc_from_objname["loc"])==0:
                            doc_from_objname.pop("loc")
                        
            if "net" not in doc_from_objname:
                doc_from_objname["net"] = "undefined"
            if doc_from_objname["net"] != net:
                badnet = doc_from_objname["net"]
                if badnet in inconsistent_net_codes:
                    val = inconsistent_net_codes[badnet]
                    val += 1
                    inconsistent_net_codes[badnet] = val
                else:
                    inconsistent_net_codes[badnet] = 1
            else:
                # apply the channel_select clause if defined
                if "chan" in doc_from_objname and channel_select is not None:
                    chan = doc_from_objname['chan']
                    if not fnmatch.fnmatch(chan,channel_select):
                        n_skipped += 1
                        continue
                # add any new names parsed and verify matching ones are consistent
                for k in doc_from_objname.keys():
                    if k in doc:
                        if doc[k] != doc_from_objname[k]:
                            print(f'{alg} (WARNING):  value for {k} expected does not match that parsed from s3object={objname}. Using {doc[k]}')
                    else:
                        doc[k] = doc_from_objname[k]
                # add data from provider
                doc['payload_type'] = str(provider.payload_type(net))
                doc['payload_format'] = str(provider.payload_format(net))
                doc['bucket'] = provider.bucket(net)
                doc['open_data'] = provider.is_open(net)
                doc['s3object_format_string'] = provider.object_format(net)
                doc['region'] = provider.region(net)
                doc['prefix'] = provider.prefix(net)
                doclist.append(doc)
    if print_summary:
        print(f"Summary result for {net} for year {year} and day {jday}")
        print("Number of s3 objects handled for this day=",len(s3objlist))
        print("Number of documents saved=",len(doclist))
        print("Number dropped from parsing errors=",number_failures)
        print("Number of documents removed by select_channel filter=",n_skipped)
        print("Inconsistent network code counts of dropped items:")
        print_metadata(inconsistent_net_codes)
    if len(doclist)==0:
        return [0,len(s3objlist),number_failures,n_skipped,inconsistent_net_codes]
    else:
        # for some unknown reason pymongo modifies doclist content 
        # in the insert_many method.  A weird consequence is we need to 
        # make a deep copy of each doc send insert_many
        unique_docs = [doc.copy() for doc in doclist]
        result = db[collection].insert_many(unique_docs)
        return [len(result.inserted_ids),
                len(s3objlist),
                number_failures,
                n_skipped,
                inconsistent_net_codes]
    
def days_needed(t0, start, end, pad=100.0) -> list:
    """
    Computes a range of days from a time interval t0+start to t0+tend.
    
    Reading a particular time window from continuous data stored as day 
    files is a generic problem.  Two common ways it comes up is needing to 
    handle a time series longer than one day.   The other is in extracing 
    time windows of data round a particular time period of interest like 
    a predicted or measured arrival time or a time period relative to a 
    source origin time.  The complication this function generalizes is 
    that any time the time periods spans one or more day boundaries multiple
    day volumes need to be read.  This function does nothing but return 
    a list of year,day tuples for a time space defined by the 
    range t0 + start - pad to t0 + end + pad.   t0 can be either a float, 
    in which case it is assumed to be an epoch time, or a UTCDateTime object. 
    The time ranges (start and end arguments == arg1 and arg2) are assumed 
    to be seconds relative to t0.  
    Optional pad in seconds.  That is, normally the function returns a list of
    year-day pairs defined as the range t0+start-pad to t0+end+pad.
    That is necessary because miniseed day files usually have inexact start times
    due to compresssion.

    Return as a list of (year,day) tuples.
    """
    alg = "days_needed"
    if isinstance(t0,UTCDateTime):
        t0 = t0.timestamp
    if not isinstance(t0,float):
        message = f'{alg}:  illegal type {type(t0)} for t0 (arg0) argument'
        raise ValueError(message)
    if end < start:
        message = "{}:  start time={} is less than end time={}".format(
            alg, start, end,
        )
        raise ValueError(message)
    tsutc = UTCDateTime(t0 + start - pad)
    teutc = UTCDateTime(t0 + end + pad)
    result = list()
    if tsutc.year == teutc.year:
        yr = tsutc.year
        for jday in range(tsutc.julday, teutc.julday + 1):
            result.append([yr, jday])
    else:
        # complexity required to cross year boundary
        # Warning is appropriate but very long period data make it only a warning
        if (teutc.year - tsutc.year) != 1:
            message = f"{alg} (WARNING): "
            message += "Time window of {} -> {} spans multiple years\n".format(
                tsutc, teutc
            )
            message += "Using the list returned may cause a memory allocation fault unless you are loading only low sample rate data"
            print(message)
        for yr in range(tsutc.year,teutc.year+1):
            if yr == tsutc.year:
                jdaystart = tsutc.julday
            else:
                jdaystart = 1
            if yr == teutc.year:
                jdayend = teutc.julday
            else:
                if calendar.isleap(yr):
                    jdayend = 366
                else:
                    jdayend = 365
            for jday in range(jdaystart,jdayend+1):
                result.append([yr,jday])
    return result

def build_day_processing_blocks(
    df,
    start,
    end,
    dbname_or_handle,
    pad=100.0,
    channel_select="B*",
    maxdays=2,
    collection="wf_s3",
    auxkeys=["source_id"],
) -> list:
    """
    This function takes a (potentially large) DataFrame df of arrival
    time data and returns a grouped list of documents that can be used to
    efficiently extract waveform data segments containing those arrival
    times.

    The data structure returned is a bit complex because it provides a
    clean, if not obvious, way to work efficiently within constraints of
    GeoLab and the current implementation of the MsPASS Database.
    That is, the only scalable method of output on GeoLab at present is to
    push data to an output s3 bucket on their scratch bucket.   The only
    way we have a present to do that is to serialize data with pickle and
    push the bits to s3 objects.   The idea of the output of this function
    is to organize the processing to produce ensembles that are essentially
    segmented day files.   i.e. the groupings define all data that have a
    starttime on a particular day.   A subtle element of this approach is
    that some events will be split across day volumes in the rare situation
    where the set of arrival times for that event cross a day boundary.

    To handle that complexity the output is a list of lists.   The
    outer list of the return is of the form:
        [data_list, error_list]
    The content of "data_list" is itself a list of pairs (2 element array/list)
    with conetent:
        [yearday_string,document_list]
    where "yearday_string" is a like it sound (e.g. 2004122 for julian
    day 122 of 2004).  document_list is a list of dictionaries that are used
    downstream in parallel by a function called `get_s3_segments`.

    The complexity of the outer list component I called `error_list` above is
    needed as a necessary evil as an error return.   Because the input to
    this function is normally huge the list of failures can also be huge.
    Simply printing every failure can overwhelm the jupyter server when this
    is run on GeoLab (the expectation).   The list is a list of MongoDB
    queries that yield no data.  The caller should handle it appropriately.
    The expectation at present is to save the content as a pickle file
    in the run directory for later inspection.

    This function uses the DataFrame api in combination with pymongo
    bo build the output.  It assumes an index to the earthscope s3
    archive already existss for the time period to be handled.
    That index is expected to live in a collection defined by the
    "collction" argument (default is "wf_s3").

    The input DataFrame must contain the following required attributes:
      'time' - either a theoretical or measured arrival time used as a basis for
               what segments are generated. The functio aims to extract a
               window of size time+start-pad to time+end+pad for each row of df.
      'net' - SEED station net code
      'sta' - SEED station code

    Optional:   the keys defined by "auxkeys" are also expected to be in the
      input DataFrame but will be silently ignored if they are not present.
      The default expects "source_id" which is one way to provide a
      cross reference to source data.  Something of that sort is
      normally required to make the results easier to use since
      arrivals by definition are associated with some source.
      The idea is this function is generic, however, and it does not
      need to be dogmatic about source data.  The main concept is to
      extract event segments.  Use data in auxkeys as metadata that define
      what those segments are.

    A perspective on this function is it takes an input DataFrame with
    the required metadata listed above and convert to an input that
    can be used to implement processing by day groups.   The day groups
    provide a reasonably managable size package to push data as pickled
    objects to an output s3 bucket.   The primary content of each document
    in the output structure is:

        's3objects' - list of s3 object name to be retrieved as an atomic operation
           Note when more than 1 reader needs to assume the pieces
           need to be glued together.  Currently the list can be no longer
           than 2.
        'net' - SEED net code
        'sta' - SEED station code
        'channel_select' - echo of the channel_select argument
           (used in s3 reader to select matching channel codes only)
        'arrivals' - list of dictionaries defining data to extract
        'start' - relative start time for windowing
        'end' - relative end time for windowing

    As noted any valid data match key defined by "auxdata" will also be
    posted to those documents.

    Finally, note the 'arrivals' dictionaries each contain a 'time' attribute
    that defines the arrival time from which a window is computed.
    Any other data in tha dictionary is treated as auxiliary Metadata that
    is to be copied to the output.  Note downstream uses of that aux
    data must handle it carefully to make sure the contents do not contain
    metadata that conflict with metadata used inside children of
    BasicTimeSeries data (e.g. starttime).  Also note the start end
    range will be modified from input by pad value.

    """
    db = fetch_dbhandle(dbname_or_handle)
    df = df.sort_values("time")
    # Add two columns to the DataFrame that are used below to generate
    # dayfiles to load to handle which arrivals.
    yrdaylists = list()
    yrdaystr = list()  # string version to be hashable - needed by groupby below
    for row in df.itertuples():
        ydlist = days_needed(row.time, start, end, pad=pad)
        if len(ydlist) > maxdays:
            # this should only happen if the user makes an input error for start and/or end
            message = (
                "Number of days for time={} and range={}-{} is too large\n".format(
                    row.time, start, end
                )
            )
            message += (
                "Number of days this would try to return={} but maxdays={}".format(
                    len(ydlist), maxdays
                )
            )
            raise ValueError(message)
        # ydstr = f"{ydlist[0]}{ydlist[1]}"
        # ydstr = str(ydlist)
        ydstr = f"{ydlist[0][0]}{ydlist[0][1]}"
        # print(ydlist[0][0])
        # print(ydlist[0][1])
        yrdaylists.append(ydlist)
        yrdaystr.append(ydstr)
    df["yrdaylist"] = yrdaylists
    df["yearday"] = yrdaystr

    result = []
    fail_list = []
    for _, gdf in df.groupby("yearday", sort=False):
        # The first window need not span all the days used by later windows.
        required_days = sorted(
            {tuple(day) for days in gdf["yrdaylist"] for day in days}
        )
        query = {"$or": [{"year": yr, "jday": day} for yr, day in required_days]}
        holdings = {}
        for holding in db[collection].find(query):
            holdings.setdefault((holding["net"], holding["sta"]), []).append(holding)
        if not holdings:
            fail_list.append(query)
            continue
        first_day = gdf["yrdaylist"].iloc[0][0]
        jday_tag = f"{first_day[0]}_{first_day[1]}"
        daylist = []
        for (net, sta), station_rows in gdf.groupby(["net", "sta"]):
            objects = set()
            arrivals, starts, ends = [], [], []
            available = holdings.get((net, sta), [])
            for row in station_rows.to_dict("records"):
                days = {tuple(day) for day in row["yrdaylist"]}
                matching = [h for h in available if (h["year"], h["jday"]) in days]
                if not matching:
                    fail_list.append(
                        {
                            "net": net,
                            "sta": sta,
                            "$or": [
                                {"year": yr, "jday": day} for yr, day in sorted(days)
                            ],
                        }
                    )
                    continue
                # An arrival appears once, even when it needs two day objects.
                # A DataFrame join with the holdings would multiply arrivals.
                arrival = {"time": row["time"]}
                arrival.update({key: row[key] for key in auxkeys if key in row})
                arrivals.append(arrival)
                starts.append(row["time"] + start - pad)
                ends.append(row["time"] + end + pad)
                objects.update(h["s3key"] for h in matching)
            if arrivals:
                daylist.append(
                    {
                        "net": net,
                        "sta": sta,
                        "channel_select": channel_select,
                        "s3objects": sorted(objects),
                        "arrival": arrivals,
                        "start": starts,
                        "end": ends,
                        "jday_tag": jday_tag,
                    }
                )
        if daylist:
            result.append([jday_tag, daylist])
    return [result, fail_list]

class SelectionMethod(Enum):
    chanlist = auto()
    select_string = auto()
    preferences = auto()
    invalid = auto()

def _validate_selection_method(alg,chanlist,preferences,obspy_select_string)->bool:
    """
    Standardize handling of selection method. 
    
    Multiple functions need to deal with the multiple mechanisms used 
    in this module to handle filtering extraneous waveform data from 
    downloaded s3 objects.   Note that enteres only if PayloadType is 
    stationc_days.  
    
    The arg names match those used in all functions in this module.  
    The error messages returned in illegal conditions docment what they 
    are expected to be as well.  The only exception is alg which is the 
    used to post the name of the calling function on messages
    
    Returns True on success.  Failure will currently always cause an 
    exceotion to be raised
    """
    test_tuple = (chanlist, preferences, obspy_select_string)
    if test_tuple == (None, None, None):
        # this is acceptable and means no filtering will be done
        # that is, all are None
        return True
    if sum(v is not None for v in test_tuple) != 1:
        message = f'{alg}:  illegal data filter options specified\n'
        message += 'Only one of the options "chanlist", "preferences", or "obspy_select_string" can be defined'
        # This could be a MsPASSError but ValueError is stock and suffici8ent
        # here as this is not special to MsPASS
        raise ValueError(message)
    if chanlist is not None:
        if not isinstance(chanlist,list):
            message = f'{alg}:  Illegal input for chanlist option.\n'
            message += 'Should be a list but received an object of type={type(chanlist)}'
            raise TypeError(message)
    if preferences is not None:
        if not isinstance(preferences,ChannelPreferences):
            message = f'{alg}:  Illegal input for preferences option.\n'
            message += 'Should be a ChannelPreferences object but received an object of type={type(preferences)}'
            raise TypeError(message)
    if obspy_select_string is not None:
        if not isinstance(obspy_select_string,str):
            message = f'{alg}:  Illegal input for obspy_select_string option.\n'
            message += 'Should be a string but received an object of type={type(obspy_select_string)}'
            raise TypeError(message)
    return True

def merge_channels(tsvector,
                   fix_overlaps=True,
                   zero_gaps=False,
                   )->TimeSeriesVector:
    """
    Helper for get_s3_segments to merge multiple channels in when 
    multiple day volumes are being processed.
    
    Complication is that we have to split out different channel codes 
    before we run merge when the payload has multiple channels together. 
    This algorithm is only useful for (mini)seed data where we can 
    assume the attributes "chan" and "loc" mean something.  The 
    algorith also ASSUMES net and sta are the same for all the 
    TimeSeries data in tsvector.   It works by assembling all data 
    with a common chan and loc combination and running merge on 
    each unique chan-loc combination it finds.  Result is a new 
    TimeSeriesVector with the multiple day segments merged.
    
    fix_overlap and zero_gaps are arguments to merge and are passed 
    directly.  See the docstring for merge to understand what they do.  
    
    This could be marked as an internal function but there are likely 
    other uses of the function outside of the FDSN module for working 
    with data stored on s3.  Be warned it has no safeties, however, 
    as it isn't really intended for public use.  '
    """
    def chan_loc_key(md):
        sta = md['sta']
        if "loc" not in md:
            loc=""
        return f"_{sta}_{loc}_"
    index = dict()
    i = 0
    for ts in tsvector:
        key = chan_loc_key(ts)
        if key in index:
            val = index[key] 
            val.append(i)
            index[key] = val 
        i += 1
    result = TimeSeriesVector()
    for key in index:
        ivals = index[key]
        tsv_k = TimeSeriesVector()
        for i in ivals:
            tsv_k.append(tsvector[i])
        # don't call merge unless there are multiple TimeSeries to be merged
        # I think merge handles that but this is cleaner and clearer
        # note this does nothing if tsv_k is empty.  That shouldn't happen 
        # but a useful feature
        if len(tsv_k)==1:
            result.append(tsv_k[0])
        else:    
            ts = merge(tsv_k,
                   fix_overlaps=fix_overlaps,
                   zero_gaps=zero_gaps,
                   )
        result.append(ts)
        # manually clear the debris here as tgese cab be large
        del tsv_k
    # note result may contain dead bodies from merge failures
    return result
    
def get_s3_segments(
    doc,
    provider,
    s3_client=None,
    dask_worker_key="geolab_s3_client",
    chanlist=None,
    preferences=None,
    obspy_select_string=None,
    metadata_copy_list=None,
    detrend_type="None",
    short_segment_handling="kill",
    fix_overlaps=True,
    zero_gaps=False,
    jday_tag_key="jday_tag",
) -> TimeSeriesEnsemble:
    """
    Companion to build_day_processing_blocks to load data defined by 
    each document that function creates.  
    
    
    
    Low level function to take content of doc, which is assumed to
    be a componet of the output of run_by_days, and return a
    TimeSeries ensemble of waveform segments the doc defines.
    
    
    TODO:   needs a generic way to handle lists of metadata when 
    multiple segments are loaded from day volumes.  prototype used a
    list linked to a particular key, but tha seems a bit complex for the 
    average user to handle.  That is the fallback but need some 
    thought on alternatives. Currently set as fixed key "segment_metadata"
    """
    alg = "get_s3_segments"
    IMMUTABLE_METADATA = [
        "starttime",
        "endtime",
        "delta",
        "npts",
        "utc_convertible",
        "time_standard",
    ]
    if s3_client is None:
        s3_client = fetch_s3_client(parallel=True,
                                    worker_data_key=dask_worker_key,
                                    )
    elif not is_s3_client(s3_client):
        message = f'{alg}:  Illegal value for s3_client argument={type(s3_client)}\n'
        message += 'Must be a boto3 client object'
        raise ValueError(message)
    if not isinstance(provider, S3SeismicProvider):
        message = f'{alg}:  provider (arg1) is type={type(provider)}.   Must be a S3SeismicProvider object'
        raise ValueError(message)
    # this function does return a boolean but we ignore it for now as 
    # failures always raise an exdeption and we want exceptions to abort 
    # a workflow using this function if they are thrown by this function.
    _validate_selection_method(alg, chanlist, preferences, obspy_select_string)
    # defined here so if the strm is empty we return a default
    # constructed ensemble with error log content
    result = TimeSeriesEnsemble()
    # assume net is defined as none of this works without it 
    net = doc['net']
    # Always add the bucket name to the doc so it will be saved for both success and failure
    bucket = provider.bucket(net)
    doc["Bucket"] = bucket
    # need this in loop below to control behavior 
    payload_type = provider.payload_type(net)
    # always put this in the ensemble metadata so there is always a way to identify a problem child
    auxmd = Metadata()
    auxmd["s3_request_document"] = doc
    if metadata_copy_list is not None:
        for k in metadata_copy_list:
            if k in doc and k not in IMMUTABLE_METADATA:
                auxmd[k] = doc[k]
                
    s3objects = doc["s3objects"]
    if len(s3objects) == 0:
        message = "Request document had no entries in the list defined by the s3objects key"
        result.elog.log_error(alg, message, ErrorSeverity.Invalid)
        return result
    alldata=TimeSeriesVector()
    for s3key in s3objects:
        rawbytes,elog = provider.get_raw_object_data(s3_client,
                                                     net,
                                                     s3key,
                                                     )
        if elog.size()>0:
            # if the log is empty it means the function above failed. 
            # push the entires there to the empty, dead ensemble and 
            # return that in this condition
            result.elog += elog
            return result 
        
        match payload_type:
            case PayloadType.channel_day:
                daydata = provider.decode_waveform_data(rawbytes,net,auxmd=auxmd)
                # in this case daydata is a TimeSeries so we just append it if live
                if daydata.live:
                    alldata.append(daydata)
            case PayloadType.station_day:
                daydata = provider.decode_waveform_data(rawbytes,
                                                    net,
                                                    chanlist=chanlist,
                                                    preferences=preferences,
                                                    obspy_select_string=obspy_select_string,
                                                    auxmd=auxmd,
                                                    )
                # here daydata is an ensemble so we need a loop 
                for d in daydata.member:
                    if d.live:
                        alldata.append(d)
            case _:
                message = f'{alg}:  S3SeismicProvider defined an illegal Payload_type = {payload_type}\n'
                message += f'Error in S3SeismicProfider likely for net code={net}'
                raise ValueError(message)
    # now we have to handle merging data if the data span multiple days
    if len(s3objects)>1:
        match payload_type:
            case PayloadType.channel_day:
                # this logic handles the case where only  one of expected 
                # mulltiple days were actually loaded
                if len(alldata)>1:
                    d = merge(alldata,
                              fix_overlaps=fix_overlaps,
                              zero_gaps=zero_gaps,
                              )
                alldata = TimeSeriesVector()
                alldata.append(d)
            case PayloadType.station_day:
                alldata = merge_channels(alldata,
                                         fix_overlaps=fix_overlaps,
                                         zero_gaps=zero_gaps,
                                         )
    # alldata should now contain one or more continuous time series 
    # that we can cut up using the time windows defined by the input 
    # document
    if detrend_type in ("simple", "constant", "linear", "polynomial", "spline"):
        for i in range(len(alldata)):
            alldata[i] = detrend(alldata[i], type=detrend_type)
    result = TimeSeriesEnsemble(len(alldata))
    stlist = doc["start"]
    etlist = doc["end"]
    # this should maybe be optional
    segmdlist = doc['segment_metadata']
    # assume stlist and etlist are always the same size
    for i in range(len(stlist)):
        stime = stlist[i]
        etime = etlist[i]
        segmd = segmdlist[i]
        for d in alldata:
            d_seg_i = WindowData(d,
                                 stime,
                                 etime,
                                 short_segment_handling=short_segment_handling,
                                 )
            # append the living and the dead to allow sorting out after the funeral
            for k in segmd:
                if k not in IMMUTABLE_METADATA:
                    d_seg_i[k] = segmd[i]
            result.member.append(d_seg_i)
    # alldata can be huge so delete it now since we are done with hit
    del alldata
    if has_live_data(result):
        result.set_live()
        if jday_tag_key in doc:
            result[jday_tag_key] = doc[jday_tag_key]
        else:
            message = f"No value found for key={jday_tag_key} - may cause problems downstream"
            result.elog.log_error(alg, message, ErrorSeverity.Complaint)
    return result


