#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 14:56:53 2024

@author: pavlis
"""
import os
import yaml
from mspasspy.ccore.utility import MsPASSError
from mspasspy.ccore.seismic import (TimeSeries,
                                    Seismogram,
                                    TimeSeriesEnsemble,
                                    SeismogramEnsemble,
                                    _CoreTimeSeries,
                                    )

class Janitor:
    """
    Generic handler to clean up the Metadata namespace of a MsPASS data object.
    
    In data processing it is common for Metadata attributes to become 
    inconsistent with the data.   An example is a "chan" attribute makes
    no sense if the data have passed through an algorithm to convert a set
    of TimeSeries objects into Seismogram objects.  The name of the class
    is meant as a memory device that the object is used to clear attributes 
    that are junk/trash that need to be removed.  
    
    There are two fundamentally different conceptual ways for the Janitor 
    to handle the trash.  First, is to discard them forever.   That is the 
    approach of the method called `clean`.  When `clean` is called on a 
    datum the inconsistent attributes (trash) are thrown away
    (like garbage sent to a landfill).  The alternative is the to 
    bag up the garbage and put it somewhere until you are ready to 
    deal with it.  That s the idea of the two methods called 
    `collect_trash` and `bag_trash`.   `collect_trash` removes 
    trash attributes from the object but returns the trash in 
    a container (implmented a dictonary).   The `bag_trash` takes 
    the result from `collect_trash` and posts it back to the 
    object as a subdocument (a python dictionary) with a specified 
    key.  That mode can be useful for an experimental algorithm where 
    you may need to pull some trash from the bag later but need to 
    get the debris out of the way for understanding.   
    
    Handling of ensemles is potentially ambiguous as both the ensemble 
    container itself and all the members have a Metadata container.  
    For that reason the class has a seperate set of keys that define 
    attributes to be retained for ensembles.   The default for enssembles 
    is an empty list because in most cases the ensemble metadata is 
    loaded from a normalizing collection and does not need to be retained. 
    (e.g. source attributes for a common source gather)
    
    This class can be thought of as an inline version of the 
    `clean` and `clean_collection` methods of 
    :py:class:`mspasspy.db.database.Database`.  That is, the database
    versions can be used to do a similar operation of data previously 
    stored in MongoDB.   This methds of this class would normally occur as the 
    function in a map operator or in an assignment in a serial loop.   
    
    The defaults for the class are designed to be appropriate for 
    stock use.  The variable args for the constructor can be used to 
    override the list of attribute keys that the Janitor should treat 
    as not junk.  The default are loaded from a yaml file.   You can also 
    change the namespace by specifying an alterate name for the yaml file. 
    The file is expected to contain keys to retain with the dictionary 
    keys "TimeSeries" and "Seismogram".   See the default in 
    mspass/data/yaml/Janitor.yaml to see the format.
    
    :param keeper_file:   yaml file containing the keys to be retained 
      for TimeSeries and Seismogram objects.
    :type keeper_file:  string defining yaml file name.  Default is None 
      which assumed a default path of $MSPASS_HOME/data/yaml/Janitor.yaml.
    :param TimeSeries_keepers:  Use to override list defined in yaml 
      file for TimeSeries objects.  If defined, it 
      should be list of attributes to be retained.  Use this option 
      with caution as the list is not checked for required Metadata.  
      Use the default yaml file for guidance.
    :type TimeSeries_keepers:  assumeed to be a list of strings of 
      attributes to be retained for TimeSeries objects.  
      Default is None which causes this 
      argument to be ignored and using the yaml file to define the 
      list of attributes to be retained. 
    :param Seismogram_keepers:  Use to override list defined in yaml file
      for Seismogram objects.  If defined, it should 
      be a list of  attributes to be retained.  Use this option 
      with caution as the list is not checked for required Metadata.  
      Use the default yaml file for guidance.
    :type Seismogram_keepers:  assumeed to be a list of strings of 
      attributes to be retained.  Default is None which causes this 
      argument to be ignored and using the yaml file to define the 
      list of attributes to be retained.
    :param ensemble_keepers:  Use to override the content of a 
      yaml file.   If defined it replaces the yaml file defnition of 
      what attribute keys should be retained.   
    :type ensemble_keepers:  list of strings defining keys of 
      attributes that should not be treated as junk and retained.  
    :param process_ensemble_members:   boolean controlling behavior 
      with ensemble objects.  With ensembles there are two possible 
      definitions of what Metadata container is to be cleaned.  That is, 
      the ensemble itself has a Metadata container and each atomic 
      member has a Metdata container.  When True the cleaning opeators 
      are applied to the members.  When False the ensemble container is 
      handled if the datum is an ensemble.  This argument is ignored 
      when processing atomic data.  
      
    """
    def __init__(self,
                 keepers_file=None,
                 TimeSeries_keepers=None,
                 Seismogram_keepers=None,
                 ensemble_keepers=None,
                 process_ensemble_members=True,
                 ):
        if keepers_file is None:
            if "MSPASS_HOME" in os.environ:
                keepers_file = (
                    os.path.abspath(os.environ["MSPASS_HOME"])
                        + "/data/yaml/Janitor.yaml"
                        )
            else:
                keepers_file = os.path.abspath(
                    os.path.dirname(__file__) + "/../data/yaml/Janitor.yaml"
                        )
        elif not os.path.isfile(keepers_file):
            if "MSPASS_HOME" in os.environ:
                keepers_file = os.path.join(
                        os.path.abspath(os.environ["MSPASS_HOME"]), "data/yaml", keepers_file
                        )
        else:
            keepers_file = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../data/yaml", keepers_file)
                )
        try:
            with open(keepers_file, "r") as stream:
                keepers_dict = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            raise MsPASSError(
                "Janitor constructor:  Parser failed reading keepers_file=" + keepers_file, "Fatal"
                ) from e
        except EnvironmentError as e:
            raise MsPASSError(
                "Janitor constructor:  Cannot open keepers_file = " + keepers_file, "Fatal"
                ) from e
        self._parse_yaml_file(keepers_dict)
        self.process_ensemble_members=process_ensemble_members
        if TimeSeries_keepers:
            self.TimeSeries_keepers = TimeSeries_keepers
        if Seismogram_keepers:
            self.Seismogram_keepers = Seismogram_keepers
        if ensemble_keepers:
            self.ensemle_keepers = ensemble_keepers


    def clean(self,datum):
        """
        Process datum to remove all Metadata with keys not defined in 
        this instance of Janitor.  Returns the datum with the attributes
        it treats as junk removed.  For ensembles if 
        self.process_ensemble_members s True the operation will be appplied 
        on all ensemble members.  When False the Metadata container will 
        be altered.  
        
        :param datum:   data objet to be processed.   
        """
        dtype = self._validate_datum(datum)
        if datum.dead():
            return datum
        if dtype=="ensemble":
            for k in datum.keys():
                if k not in self.ensemble_keepers:
                    datum.erase(k)
            if self.process_ensemble_members:
                for i in range (len(datum.member)):
                    datum.member[i] = self.clean(datum.member[i])
        else:
            # this uses _CoreTimeSeries as a base class to TimeSeries 
            # to be more robust - resolves True fr a TimeSeries
            if isinstance(datum,_CoreTimeSeries):
                keepers = self.TimeSeries_keepers
            else:
                # currently this can only be Seismogram - if data 
                # types supported is exteded needs a change 
                keepers = self.Seismogram_keepers
            for k in datum.keys():
                if k not in keepers:
                    datum.erase(k)
        return datum
                
                
    def collect_trash(self,datum)->dict:
        """
        Processes datum by extracting attributes that this 
        instance of Janitor does not define as a keeper.  It then 
        clears the attributes it treats as junk before returning 
        the attributes it cleared in a python dictionary.   When 
        run on ensembles the self.ensemble_keeper slist is used to 
        edit the ensemble Metadata container.   When datum is 
        an ensemble the ensemble members are not altered.   
        """
        dtype = self._validate_datum(datum)
        if datum.dead():
            return datum
        if dtype=="ensemble":
            keepers = self.ensemble_keepers
        else:
            # this uses _CoreTimeSeries as a base class to TimeSeries 
            # to be more robust - resolves True fr a TimeSeries
            if isinstance(datum,_CoreTimeSeries):
                keepers = self.TimeSeries_keepers
            else:
                # currently this can only be Seismogram - if data 
                # types supported is exteded needs a change 
                keepers = self.Seismogram_keepers
        result = dict()
        for k in datum.keys():
            if k not in keepers:
                result[k] = datum[k]
                datum.erase(k)
        return result
    
    def bag_trash(self,
                  datum,
                  trashbag_key="trash",
                  ensemble_trashbag_key="ensemble_trash"):
        """
        This method bundles up trash in a python dictionary and posts it 
        back to the datum with a specified key.   This allows the 
        trash data to be retained but put into the auxiliary trash bag 
        container to simplify the datum's Metadata namespace.  The idea
        is to allow an algorithm to pull junk from the trashcan if necesary 
        at a later stage.  
        
        Note for ensembles there are two different entities that are handled
        separately.   Any attributes in the ensemble's Metadata are 
        processed against self.ensemble_keepers and any junk is bagged into 
        a dictionary stored back in to the ensemble's Metadata container 
        with the key defined by the ensemble_trashbag_key.   Members 
        are only processed if self.prococss_ensemble_members is True.  
        When True the members will also be passed through this 
        method with the handling depending on the type of the members. 
        
        :param datum:  MsPASS data objet to be processed.
        :type datum:  Must be a MsPASS data object 
          (`TimeSeries`,`Seismogram`, `TimeSeriesEnsemble`, or `SeismogramEnsemle`)
          or this method will raise a MsPASSError exception marked Fatal.
        :param trashbag_key:   dictinary key used to post the trash bag 
          for atomic data or the members of ensembles when self.process_ensemble_members 
          is True.  
        :type trashbag_key:  str (default "trash")
        :param ensemble_trashbag_key:  dictionary key to use post the 
          trashbag dictionary constructed from an ensemble's Metadata 
          container.  Ignored for atomic data.  Note this key should 
          be distinct from trashbag_key or the member trashbags will be 
          overwritten by the ensemble trashbag if the ensemble is saved.
        :type ensemble_trashbag_key:  str (default "ensemble_trash"
        
        """
        dtype = self._validate_datum(datum)
        if datum.dead():
            return datum

        if dtype=="ensemble":
            trash = self.collect_trash(datum)
            datum[ensemble_trashbag_key] = trash
            if self.process_ensemble_members:
                for i in range (len(datum.member)):
                    trash = self.collect_trash(datum.member[i])
                    datum.member[i][trashbag_key] = trash
        else:
            trash = self.collect_trash(datum)
            datum[trashbag_key] = trash
        return datum
    
    def add2keepers(self,key,keeper_type="atomic"):
        """
        Adds a new key to the namespace for a data type.
        
        It is often useful to extend the namespace during processing 
        without having to create a special instance of a Janitor from 
        a yaml file.   Use this method to add a key to the list of 
        attributes defined as a keeper.
        
        :param key:  key of the attribute to add to a the list of keeper 
           names in this Janitor.   Note if the name already exists in 
           the list this method does nothing. 
        :type key:  str
        :param keeper_type:  defines the data to to which key should be 
          added.  Normal use is one the following keywword whose use 
          should be clear:  "TimeSeries","Seismogam", and "ensemble". 
          Also accepts the special keyword "atomic" which means the 
          key is added to the list of keepers for both TimeSeries and 
          Seismogram objects.  
        :type keeper_type:  str (default "atomic")
        """
        if keeper_type=="atomic":
            self.add2keepers(key,"TimeSeries")
            self.add2keepers(key,"Seismogram")
        elif keeper_type == "TimeSeries":
            if key not in self.TimeSeries_keepers:
                self.TimeSeries_keepers.append(key)
        elif keeper_type == "Seismogram":
            if key not in self.Seismogram_keepers:
                self.Seismogram_keepers.append(key)
        elif keeper_type == "ensemble":
            if key not in self.ensemble_keepers:
                self.ensemble_keepers.append(key)
        else:
            message = "Janitor.add2keepers:  illegal value for keeper_type={}".format(keeper_type)
            raise ValueError(message)
            
            
    def  _parse_yaml_file(self,
                              keepers_dict,
                              TimeSeries_key="TimeSeries",
                              Seismogram_key="Seismogram",
                              ensemble_key="Ensemble"
                              ):
        """
        Internal method to parse dictionary returned by pyyaml 
        parsing of a specified keepers definition.  kwarg 
        vales specify keys for lists for TimeSeries and Seismogram 
        the parser expects to see.  This method will throw an 
        exception if either are missing.  Normally called only by 
        constructor, but could be used to redefine a Janitor with a 
        parsed result from a different yaml file.  Normally that would 
        be silly, however, as the same thing would be clearer with 
        just redefining the instance of Janitor.  This was made a method 
        largely to encapsulate the translation step the yaml dict to 
        the pair of lists the constructor needs to define.
        """
        if TimeSeries_key in keepers_dict and Seismogram_key in keepers_dict:
            self.TimeSeries_keepers=keepers_dict[TimeSeries_key]
            self.Seismogram_keepers=keepers_dict[Seismogram_key]
        else:
            message = "Janitor._parse_yaml_file:  "
            message += "Missing required keys = {} and {} for TimeSeries and Seismogram object keepers defintions respectively".format(TimeSeries_key,Seismogram_key)
            raise MsPASSError(message,"Fatal")
        if ensemble_key in keepers_dict:
            self.ensemble_keepers=keepers_dict[ensemble_key]
        else:
            self.ensemble_keepers=[]
            

    def _validate_datum(self,datum)->str:
        """
        Called internally by processing methods to validate the 
        type of a datum input to processing methods through 
        required arg0.   Returns  "atomic" if datum is an atomic datum 
        (TimeSeries or Seismogram) or  "ensemble" if the datum is a valid 
        ensemble object (TimeSeriesEnsemble or SeismgoramEnsemble).  
        Will raise a MsPASSError if the datum is not a valid mspass
        data object.
        """
        if isinstance(datum,(TimeSeries,Seismogram)):
            return "atomic"
        elif isinstance(datum,(TimeSeriesEnsemble,SeismogramEnsemble)):
            return "ensemble"
        else:
            message="Cannot process input datum of type={}".format(str(type(datum)))
            raise MsPASSError(message,"Fatal")
            
class MiniseedJanitor(Janitor):
    """
    Convenience class for handling data read from wf_miniseed. 
    
    Data loaded from miniseed files in MsPASS tend to have some debris 
    that is not needed once the data are loaded into memory for processng. 
    This is a conveniene class that loads a different yaml file to 
    create a stock Janitor for handling data loaded from wf_miniseed.
    
    WARNING:  use this class only on data immediately after loading 
    from wf_miniseed.  It will eliminate miniseed specific debris that 
    is a perfect example of why a Janitor is useful.   It should not, however, 
    be used after any processing that loads additional attributes or 
    it will almost certainly delete useful attributes.  
    """
    def __init__(self):
        super().__init__("MiniseedJanitor.yaml")
        
        
    
    