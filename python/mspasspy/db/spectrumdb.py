#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from mspasspy.ccore.seismic import PowerSpectrum
from mspasspy.ccore.utility import MsPASSError,ErrorSeverity
import pymongo
from bson import ObjectId
import pickle

class BasicDatabase(ABC):
    """
    Abstract base class for database handle.
    
    The idea of this class is to abstract basic functionality required 
    for a database handle.  What those methods should be is dependent 
    upon the application.  As a base class for the MsPaSS framework 
    it is pretty simple.   As the focus is data processing we only 
    require implementation of a basic reader and writer for whatever 
    data type the handle supports.
    
    The model used here is differnt from that in the original MsPASS 
    impelementation.   There MongoDB is assumed to be the underlying 
    engine and we use the MongoDB concept of a collection
    (a collection == relation in a relational database) to control what 
    information is extracted.  The idea of subclasses of this base 
    is that the handle would interact with one and only one type of 
    data object.  The idea of this approach is that it provides an 
    easier mechanism for a user to extend MsPASS to support different 
    data types than native seismic data objects.   
    
    Be warned this class is a prototype and may change.   
    """
    def __init__(self,
                 name,
                 type_list,
                 *args,
                 **kwargs,
                 ):
        """
        Base class constructor
        
        :param name:   all database systems use the concept of a string that 
        defines a particular instance of a database stored in that system.  
        Subclasses should call this constructor to set that name. 
        :type name:  string
        
        :parm type_list:  Because this class is aimed at supporting 
        management of one or more data types we neee a clean way to define 
        what those types are.  This argument serves that purpose.  It should 
        contain a list of python types that can be tested with isintance 
        to verify data being handled are of the right type. 
        :type type_list:  list of python types that will work in a loop of 
        isinstance tests.  
        
        """
        self.name = name
        self.type_list = type_list
    def data_valid(self,d)->bool:
        """
        Tests if input datum d has a type supported by this handle.   
        Returns a True if the answer is yes and false if the answer is no. 
        Callers should handle the condition of false that would almost alway 
        be an error.  
        """
        for typ in self.type_list:
            if isinstance(d,typ):
                return True
        return False
    @abstractmethod
    def read_data(self):
        """
        Read one datum.
        
        Concrete implementation must implement this method.  It would 
        normally contain some identifier in the arg list to select one an d
        only one entry from the database.   It would then return run an 
        algorithm to construct and return the atomic data with wich this 
        class is associated.  
        """
        pass
    @abstractmethod
    def save_data(self,d):
        """
        Save one datum.
        
        Concrete implementations must implement this method.  It will save 
        datum d by whatever scheme is used for the implementation. 
        """
        pass
    @abstractmethod
    def verify(self):
        """
        Verify the validity of the data with which this handle is associated. 
        
        Any real database needs a way to verify the contents are "clean" as 
        defined by the needs of the system.   This method should implement 
        whatever algorithm is appropriate to verify the contents are valid 
        in the sense that read_data operations will not fail or some more 
        elaborate requirement is satisified.   Most implementations will 
        want to use a list of verify tests that implement different 
        algorithms that define what "clean" means.  
        
        Concrete implementations must implement this method. 
        """
        pass
    
class SpectrumDatabase(BasicDatabase,pymongo.database.Database):
    def __init__(self,
                 name,
                 *args,
                 type_list=[PowerSpectrum],
                 collection="PowerSpectrum",
                 **kwargs,
                 ):
        BasicDatabase.__init__(name,type_list)
        super(SpectrumDatabase, self).__init__(*args, **kwargs)
        self.collection = self[collection]
        
    def save_data(self,datum,exclude=None,metadata2save=None,format="pickle"):
        if self.data_valid():
            if metadata2save:
                doc=dict()
                for key in metadata2save:
                    # if running this mode silently ignore any
                    # metadata defined in the keep list but not defined 
                    # in the datum.   Questionable behavior
                    if datum.is_defined(key):
                        val = datum[key]
                        doc[key] = val
            else:   
                doc=dict(datum)
                if exclude:
                    for key in exclude:
                        doc.pop(key)
            doc["serialized_data"] = pickle.dumps(datum)
            recid = self.collection.insert_one(doc).inserted_id
            return recid
            
        else:
            message = "SpectrumDatabase.save_data:  illegal data type for arg0. Found type={typ} - only support PowerSpectrum".format(typ=type(datum))
            raise MsPASSError(message,ErrorSeverity.Fatal)
        
    def read_data(self,
                  id_or_doc,
                  required=["npts"],
                  override=None,
                  ):
        """
        """
        if isinstance(id_or_doc,ObjectId):
            oid=id_or_doc
        else:
            oid = id_or_doc["_id"]
        doc = self.collection.find_one({'_id' : oid})
        if doc:
            testkey="serialized_data"
            if testkey in doc:
                datum = pickle.loads(doc[testkey])
                if required:
                    for key in required:
                        datum[key] = doc[key]
                if override:
                    for key in override:
                        if key in doc:
                            datum[key] = doc[key]
            else:
                message = "SpectrumDatabase.read_data:  missing required key=serialized_data - expected to contain datum serialized with pickle"
                raise MsPASSError(message,ErrorSeverity.Fatal)
            return datum
        else:
            message="SpectrumDatabase.read_data: no document with ObjectId={oid} was found in PowerSpectrum collection".format(oid=str(oid))
            raise MsPASSError(message,ErrorSeverity.Invalid)
            
    def verify(self):
        cursor=self.collection.find[{}]
        nprocessed=0
        nvalid=0
        testkey = "serialized_data"
        for doc in cursor:
            if testkey in doc:
                nvalid += 1
            nprocessed += 1
            
        return([nprocessed,nvalid])
            
            
            
    