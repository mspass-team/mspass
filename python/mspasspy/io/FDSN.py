
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module contains utilities for working with FDSN data in seed format. 
It includes new utillities for working on AWS S3 where some FDSN networks 
now store much or all of their data.   

@author: pavlis
"""

import io
import yaml
import obspy
import pandas as pd
from abc import ABC, abstractmethod
from enum import Enum, auto
from contextlib import closing
from string import Formatter
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from earthscope_sdk import EarthScopeClient

from mspasspy.ccore.utility import MsPASSError,ErrorLogger,ErrorSeverity,Metadata 
from mspasspy.ccore.seismic import TimeSeriesEnsemble
from mspasspy.util.seismic import has_live_data
from mspasspy.db.normalize import DataFrameCacheMatcher
from mspasspy.db.database import Database
from mspasspy.util.converter import Trace2TimeSeries, Stream2TimeSeriesEnsemble
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
                        else:
                            # this should be rare but is quite possible if data 
                            # are entered by hand.  Currently all loc codes 
                            # that are not null resolve to a int
                            try:
                                n = int(loc)
                            except ValueError:
                                error_message += f'Invalid specification for loc code = {loc} '
                                error_message += f'in channel preference list for {net} {sta}\n'
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
        query = {"provider" : provider}
        doc = db[collection].find_one(query)
        if doc is None:
            message = f"S3SeismicProvider constructor:  collection={collection} has no documents for provider={provider}"
            raise ValueError(message)
        return cls(doc, provider=provider)
    def _from_dict(self, doc: dict, key: str):
        """
        this one does the actual work - all other are wrappers
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
        Return False if additional credentials are required.
        """
        val = self._fetch_subdoc_data("is_open", net, "access",default_value=False)
        return val
                
        
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
        select_count = sum(arg is not None for arg in (chanlist, preferences, obspy_select_string))
        if select_count not in (0,1):
            message = f'{alg}:  illegal select options\n'
            message += 'Only one of the arguments chanlist, preferences, or obspy_select_string should be defined\n'
            message += f'{select_count} options were enabled'
            raise ValueError(message)
        if chanlist is not None:
            if not isinstance(chanlist,list):
                message = f'{alg}:  illegal type={type(chanlist)} for chanlist argument -must be a python list'
                raise TypeError(message)
        if preferences is not None:
            if not isinstance(preferences,ChannelPreferences):
                message = f'{alg}:  illegal type={type(preferences)} for preferences argument -must be a python list'
                raise TypeError(message)
        if chanlist is not None:
            if not isinstance(obspy_select_string,str):
                message = f'{alg}:  illegal type={type(obspy_select_string)} for obspy_select_string argument -must be a python list'
                raise TypeError(message)
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
    
        
        
    
        
    

