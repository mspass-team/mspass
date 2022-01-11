from abc import ABC, abstractmethod
from mspasspy.ccore.seismic import (TimeSeries,
                                    Seismogram,
                                    TimeSeriesEnsemble,
                                    SeismogramEnsemble)
from mspasspy.ccore.utility import (MsPASSError,
                                    ErrorSeverity)
# Use of abstract base class based on:
# https://python-course.eu/oop/the-abc-of-abstract-base-classes.php
class Executioner(ABC):
    """
    Abstract base class for family of python classes used for killing
    mspass data objects failing to pass a particular metric.
    It is made abstract to define required methods a particular instance
    must create.  As in any good OOP that also means subclasses can
    add additional methods.   This class should be used only as base 
    class as it has no functionality by itself.
    """

    @abstractmethod
    def kill_if_true(self,d):
        """
        This method should run a test on d that will call the kill method 
        on MsPASS atomic object d if the test defined by the implementation 
        fails.  This is the main working method of this class of function 
        objects.
        """
        pass

    def input_is_valid(self,d):
        """
        This is a common method all instances of this class needed for
        mspass.  It uses isinstance tests of d to standardize the test that
        the input is valid data.  It returns True if d is one a valid data
        object known to mspass.  Returns false it not.  Caller must decide
        what to do if the function returns false.
        """
        if isinstance(d,(TimeSeries,Seismogram,TimeSeriesEnsemble,SeismogramEnsemble)):
            return True
        else:
            return False
    def log_kill(self,d,testname,message,severity=ErrorSeverity.Informational):
        """
        This base class method is used to standardize the error logging 
        functionality of all Executioners.   It writes a standardized 
        message to simplify writing of subclasses - they need only 
        define the testname (normally the name of the subclass) and 
        format a specific message to be posted.  
        
        Note most subclasses will may want to include a verbose option 
        (or the reciprocal silent) and only write log messages when 
        verbose is set true.  
        
        :param d:  MsPASS data object to which elog message is to be 
          written.
        :param testname: is the string assigned to the "algorithm" field 
        of the message posted to d.elog.
        :param message:  specialized message to post - this string is added 
        to an internal generic message.
        :param severity:  ErrorSeverity to assign to elog message 
        (See ErrorLogger docstring).  Default is Informational
        """
        if self.input_is_valid(d):
            kill_message = "Killed by kill_if_true method.  Reason:\n"
            kill_message += message
            d.elog.log_error(testname,kill_message,severity)
        else:
            raise MsPASSError("Execution.log_kill method received invalid data;  must be a MsPASS data object",
                              ErrorSeverity.Fatal)


class MetadataGT(Executioner):
    """
    Implementation of Executioner using a greater than test of a 
    Metadata attribute.  Both the metadata key and the threshold 
    for the kill test are set on creation.  This implementation should 
    work on any value pairs for which the > operator in python works. 
    That is true for pairs of numeric types, for example, but will fail 
    if one of the pair is a string (an error anyway for any rational use of this).
    It should also work for any pair of pyobjects for which operator > is defined, 
    although anything nonstandard should be tested carefully before using 
    this class for editing data with such nonstandard types.  This was 
    mostly intended for simple numeric attributes.
    """
    def __init__(self,key,value,verbose=False):
        """
        One and only constructor.  Sets the parameters that define this 
        tester.
        
        :param key:  key used for extracting a Metadata component for test
        :param value:  is the threshold value for the test.
        :param verbose:  if true informational messages will be posted to 
        the elog area of d.  When false (default) kills are silent.  That 
        default is intentioanl to reduce the size of elog data in large data 
        sets that often require extensive editing.  Set true if you need 
        details on why data were killed or for debugging. 
        """
        self.key = key
        self.value = value
        self.verbose = verbose
    def kill_if_true(self,d):
        """
        Implementation of this abstract method for this tester.  
        Kills d if the d[self.key] > value stored with the class.
        
        Returns a (potentially edited) copy of the input to allow use in 
        a parallel map operation.
        """
        if self.input_is_valid(d):
            if d.dead():
                return d
            if self.key in d:
                testval = d[self.key]
                if testval > self.value:
                    d.kill()
                    if self.verbose:
                        message = "Value associated with key={key} of {dval} is greater than test value={value}".format(
                            key=self.key,dval=d[self.key],value=self.value)
                        self.log_kill(d,"MetadataGT",message)
        else:
            raise MsPASSError("MetadataGT received invalid input data",ErrorSeverity.Fatal)
        return d

class MetadataGE(Executioner):
    """
    Implementation of Executioner using a greater than or equal test of a 
    Metadata attribute.  Both the metadata key and the threshold 
    for the kill test are set on creation.  This implementation should 
    work on any value pairs for which the >= operator in python works. 
    That is true for pairs of numeric types, for example, but will fail 
    if one of the pair is a string (an error anyway for any rational use of this).
    It should also work for any pair of pyobjects for which operator >= is defined, 
    although anything nonstandard should be tested carefully before using 
    this class for editing data with such nonstandard types.  This was 
    mostly intended for simple numeric attributes.
    """
    def __init__(self,key,value,verbose=False):
        """
        One and only constructor.  Sets the parameters that define this 
        tester.
        
        :param key:  key used for extracting a Metadata component for test
        :param value:  is the threshold value for the test.
        :param verbose:  if true informational messages will be posted to 
        the elog area of d.  When false (default) kills are silent.  That 
        default is intentioanl to reduce the size of elog data in large data 
        sets that often require extensive editing.  Set true if you need 
        details on why data were killed or for debugging. 
        """
        self.key = key
        self.value = value
        self.verbose = verbose
    def kill_if_true(self,d):
        """
        Implementation of this abstract method for this tester.  
        Kills d if the d[self.key] >= value stored with the class.
        
        Returns a (potentially edited) copy of the input to allow use in 
        a parallel map operation.
        """
        if self.input_is_valid(d):
            if d.dead():
                return d
            if self.key in d:
                testval = d[self.key]
                if testval >= self.value:
                    d.kill()
                    if self.verbose:
                        message = "Value associated with key={key} of {dval} is >= test value={value}".format(
                            key=self.key,dval=d[self.key],value=self.value)
                        self.log_kill(d,"MetadataGE",message)
        else:
            raise MsPASSError("MetadataGE received invalid input data",ErrorSeverity.Fatal)
        return d

class MetadataLT(Executioner):
    """
    Implementation of Executioner using a lessthan test of a 
    Metadata attribute.  Both the metadata key and the threshold 
    for the kill test are set on creation.  This implementation should 
    work on any value pairs for which the < operator in python works. 
    That is true for pairs of numeric types, for example, but will fail 
    if one of the pair is a string (an error anyway for any rational use of this).
    It should also work for any pair of pyobjects for which operator < is defined, 
    although anything nonstandard should be tested carefully before using 
    this class for editing data with such nonstandard types.  This was 
    mostly intended for simple numeric attributes.
    """
    def __init__(self,key,value,verbose=False):
        """
        One and only constructor.  Sets the parameters that define this 
        tester.
        
        :param key:  key used for extracting a Metadata component for test
        :param value:  is the threshold value for the test.
        :param verbose:  if true informational messages will be posted to 
        the elog area of d.  When false (default) kills are silent.  That 
        default is intentioanl to reduce the size of elog data in large data 
        sets that often require extensive editing.  Set true if you need 
        details on why data were killed or for debugging. 
        """
        self.key = key
        self.value = value
        self.verbose = verbose
    def kill_if_true(self,d):
        """
        Implementation of this abstract method for this tester.  
        Kills d if the d[self.key] > value stored with the class.
        
        Returns a (potentially edited) copy of the input to allow use in 
        a parallel map operation.
        """
        if self.input_is_valid(d):
            if d.dead():
                return d
            if self.key in d:
                testval = d[self.key]
                if testval < self.value:
                    d.kill()
                    if self.verbose:
                        message = "Value associated with key={key} of {dval} is < than test value={value}".format(
                            key=self.key,dval=d[self.key],value=self.value)
                        self.log_kill(d,"MetadataLT",message)
        else:
            raise MsPASSError("MetadataLT received invalid input data",ErrorSeverity.Fatal)
        return d

class MetadataLE(Executioner):
    """
    Implementation of Executioner using a less than or equal test of a 
    Metadata attribute.  Both the metadata key and the threshold 
    for the kill test are set on creation.  This implementation should 
    work on any value pairs for which the <= operator in python works. 
    That is true for pairs of numeric types, for example, but will fail 
    if one of the pair is a string (an error anyway for any rational use of this).
    It should also work for any pair of pyobjects for which operator >= is defined, 
    although anything nonstandard should be tested carefully before using 
    this class for editing data with such nonstandard types.  This was 
    mostly intended for simple numeric attributes.
    """
    def __init__(self,key,value,verbose=False):
        """
        One and only constructor.  Sets the parameters that define this 
        tester.
        
        :param key:  key used for extracting a Metadata component for test
        :param value:  is the threshold value for the test.
        :param verbose:  if true informational messages will be posted to 
        the elog area of d.  When false (default) kills are silent.  That 
        default is intentioanl to reduce the size of elog data in large data 
        sets that often require extensive editing.  Set true if you need 
        details on why data were killed or for debugging. 
        """
        self.key = key
        self.value = value
        self.verbose = verbose
    def kill_if_true(self,d):
        """
        Implementation of this abstract method for this tester.  
        Kills d if the d[self.key] <= value stored with the class.
        
        Returns a (potentially edited) copy of the input to allow use in 
        a parallel map operation.
        """
        if self.input_is_valid(d):
            if d.dead():
                return d
            if self.key in d:
                testval = d[self.key]
                if testval <= self.value:
                    d.kill()
                    if self.verbose:
                        message = "Value associated with key={key} of {dval} is <= to test value={value}".format(
                            key=self.key,dval=d[self.key],value=self.value)
                        self.log_kill(d,"MetadataLT",message)
        else:
            raise MsPASSError("MetadataLE received invalid input data",ErrorSeverity.Fatal)
        return d



class MetadataEQ(Executioner):
    """
    Implementation of Executioner using an equality test of a 
    Metadata attribute.  Both the metadata key and the threshold 
    for the kill test are set on creation.  This implementation should 
    work on any value pairs for which the == operator in python works. 
    That is true for pairs of numeric types, for example, but will fail 
    if one of the pair is a string (an error anyway for any rational use of this).
    It should also work for any pair of pyobjects for which operator >= is defined, 
    although anything nonstandard should be tested carefully before using 
    this class for editing data with such nonstandard types.  This was 
    mostly intended for simple numeric attributes.  It can be used for 
    booleans even though few would write an if statement testing if two 
    booleans were equal.
    """
    def __init__(self,key,value,verbose=False):
        """
        One and only constructor.  Sets the parameters that define this 
        tester.
        
        :param key:  key used for extracting a Metadata component for test
        :param value:  is the threshold value for the test.
        :param verbose:  if true informational messages will be posted to 
        the elog area of d.  When false (default) kills are silent.  That 
        default is intentioanl to reduce the size of elog data in large data 
        sets that often require extensive editing.  Set true if you need 
        details on why data were killed or for debugging. 
        """
        self.key = key
        self.value = value
        self.verbose = verbose
    def kill_if_true(self,d):
        """
        Implementation of this abstract method for this tester.  
        Kills d if the d[self.key] == value stored with the class.
        
        Returns a (potentially edited) copy of the input to allow use in 
        a parallel map operation.
        """
        if self.input_is_valid(d):
            if d.dead():
                return d
            if self.key in d:
                testval = d[self.key]
                if testval == self.value:
                    d.kill()
                    if self.verbose:
                        message = "Value associated with key={key} of {dval} is equal to test value={value}".format(
                            key=self.key,dval=d[self.key],value=self.value)
                        self.log_kill(d,"MetadataEQ",message)
        else:
            raise MsPASSError("MetadataEQ received invalid input data",ErrorSeverity.Fatal)
        return d
    
class MetadataNE(Executioner):
    """
    Implementation of Executioner using an not equal (NE) test of a 
    Metadata attribute.  Both the metadata key and the threshold 
    for the kill test are set on creation.  This implementation should 
    work on any value pairs for which the != operator in python works. 
    That is true for pairs of numeric types, for example, but will fail 
    if one of the pair is a string (an error anyway for any rational use of this).
    It should also work for any pair of pyobjects for which operator >= is defined, 
    although anything nonstandard should be tested carefully before using 
    this class for editing data with such nonstandard types.  This was 
    mostly intended for simple numeric attributes.  It can be used for 
    booleans even though few would write an if statement testing if two 
    booleans were equal.
    """
    def __init__(self,key,value,verbose=False):
        """
        One and only constructor.  Sets the parameters that define this 
        tester.
        
        :param key:  key used for extracting a Metadata component for test
        :param value:  is the threshold value for the test.
        :param verbose:  if true informational messages will be posted to 
        the elog area of d.  When false (default) kills are silent.  That 
        default is intentioanl to reduce the size of elog data in large data 
        sets that often require extensive editing.  Set true if you need 
        details on why data were killed or for debugging. 
        """
        self.key = key
        self.value = value
        self.verbose = verbose
    def kill_if_true(self,d):
        """
        Implementation of this abstract method for this tester.  
        Kills d if the d[self.key] != value stored with the class.
        
        Returns a (potentially edited) copy of the input to allow use in 
        a parallel map operation.
        """
        if self.input_is_valid(d):
            if d.dead():
                return d
            if self.key in d:
                testval = d[self.key]
                if testval == self.value:
                    d.kill()
                    if self.verbose:
                        message = "Value associated with key={key} of {dval} is != to test value={value}".format(
                            key=self.key,dval=d[self.key],value=self.value)
                        self.log_kill(d,"MetadataNE",message)
        else:
            raise MsPASSError("MetadataNE received invalid input data",ErrorSeverity.Fatal)
        return d
    
class MetadataDefined(Executioner):
    """
    Implementation of Executioner using an existence test.  The 
    constructor loads only a key string.   The test for the kill_if_true 
    method is then simply for the existence of a value associated with 
    the loaded key.   Data will be killed if the defined key exists 
    in the Metadata (header).   
    """
    def __init__(self,key,verbose=False):
        """
        One and only constructor.  Sets the key used for the existence test.
        
        :param key:  key used for existence test.
        :param verbose:  if true informational messages will be posted to 
        the elog area of d.  When false (default) kills are silent.  That 
        default is intentioanl to reduce the size of elog data in large data 
        sets that often require extensive editing.  Set true if you need 
        details on why data were killed or for debugging. 
        """
        self.key = key
        self.verbose = verbose
    def kill_if_true(self,d):
        """
        Implementation of this abstract method for this tester.  
        Kills d if self.key is defined for this datum
        
        Returns a (potentially edited) copy of the input to allow use in 
        a parallel map operation.
        """
        if self.input_is_valid(d):
            if d.dead():
                return d
            if d.is_defined(self.key):
                d.kill()
                if self.verbose:
                    message = "Metadata key={key} is defined".format(key=self.key)
                    self.log_kill(d,"MetadataDefined",message)
        else:
            raise MsPASSError("MetadataDefined received invalid input data",ErrorSeverity.Fatal)
        return d
    
class MetadataUndefined(Executioner):
    """
    Implementation of Executioner using an nonexistence test.  The 
    constructor loads only a key string.   The test for the kill_if_true 
    method is then simply for the existence of a value associated with 
    the loaded key.   Data will be killed if the defined key does not exists 
    (Undefined) in the Metadata (header).   
    
    This class is a useful prefilter to apply to any algorithm that 
    requires a particular metadata attribute.  Use FiringSquad to define 
    a chain of required metadata to prefilter data input to such an algorithm.
    """
    def __init__(self,key,verbose=False):
        """
        One and only constructor.  Sets the key used for the existence test.
        
        :param key:  key used for existence test.
        :param verbose:  if true informational messages will be posted to 
        the elog area of d.  When false (default) kills are silent.  That 
        default is intentioanl to reduce the size of elog data in large data 
        sets that often require extensive editing.  Set true if you need 
        details on why data were killed or for debugging. 
        """
        self.key = key
        self.verbose = verbose
    def kill_if_true(self,d):
        """
        Implementation of this abstract method for this tester.  
        Kills d if self.key is not defined for this datum
        
        Returns a (potentially edited) copy of the input to allow use in 
        a parallel map operation.
        """
        if self.input_is_valid(d):
            if d.dead():
                return d
            if not d.is_defined(self.key):
                d.kill()
                if self.verbose:
                    message = "Metadata key={key} is not defined".format(key=self.key)
                    self.log_kill(d,"MetadataUndefined",message)
        else:
            raise MsPASSError("MetadataUndefined received invalid input data",ErrorSeverity.Fatal)
        return d

class MetadataInterval(Executioner):
    """
    Implementation of Executioner based on a numeric range of values. 
    i.e. it tests if a metadata value is in a range defined by 
    an upper and lower value.   Ranges have a minor complication in 
    handling the edge condition: should or should the test not include 
    the edge?   Rather than write different functions for the four possible 
    combinations of <=, <, >=, and > that define a range we use 
    constructor arguments (use_lower_edge and use_upper_edge) to 
    turn inclusion of the boundary on and off.  
    
    In this context an interval test also has two logical alternatives. 
    One may want to keep data inside an interval (most common and default)
    or delete data within a specified interval.   That logic is controlled by 
    the kill_if_outside boolean
    
    Intervals mostly make sense only for numeric types (int and float), but 
    can be used with strings.  In reality this function should work with any 
    object for which the operators >, <, >=, and >= are defined but that is
    adventure land if you try.  
    """
    def __init__(self, key, lower_endpoint, upper_endpoint, 
                 use_lower_edge=True, use_upper_edge=True, 
                 kill_if_outside=True, verbose=False):
        """
        One and only constructor.  Sets the parameters that define this 
        tester.
        
        :param key:  key used for extracting a Metadata component for test
        :param lower_endpoint:  value defining the lower bound of the range test
        :param upper_endpoint:  value defining the upper_bound of the range test
        :parame use_lower_edge: if true (default) the lower range test uses 
          >= lower_endpoint.  When false uses >.
        :param use_upper_edge: if true (default) the upper range test uses 
          <= uper_endpoint.  When false uses <. 
        :param kill_if_outside:  boolean controlling logic of how test test 
          is applied.  When true (default) data are killed when data are outside the 
          specified range.  When false data are killed that are inside the 
          specified range.  The default is True because that type of 
          test is much more common than the opposite.  (e.g. retain source-receiver 
          distances within specified range)
        :param verbose:  if true informational messages will be posted to 
        the elog area of d.  When false (default) kills are silent.  That 
        default is intentioanl to reduce the size of elog data in large data 
        sets that often require extensive editing.  Set true if you need 
        details on why data were killed or for debugging. 
        """
        self.key = key
        self.lower_endpoint = lower_endpoint
        self.upper_endpoint = upper_endpoint
        self.use_lower_edge = use_lower_edge
        self.use_upper_edge = use_upper_edge
        self.kill_if_outside = kill_if_outside
        self.verbose = verbose
    def kill_if_true(self,d):
        """
        Implementation of this abstract method for this tester.  
        Kills d if the d[self.key] <= value stored with the class.
        
        Returns a (potentially edited) copy of the input to allow use in 
        a parallel map operation.
        """
        if self.input_is_valid(d):
            if d.dead():
                return d
            if self.key in d:
                testval = d[self.key]
                # These two are used to defined boolean result of lower 
                # and upper tests. Truth of result is and of the two
                # inverting the test (inside_test false) inverts the logic (not)
                upper_range_test=False
                lower_range_test=False
                if self.use_lower_edge:
                    if testval >= self.lower_endpoint:
                        lower_range_test = True
                    else:
                        lower_range_test = False
                else:
                    if testval > self.lower_endpoint:
                        lower_range_test = True
                    else:
                        lower_range_test = False
                if self.use_upper_edge:
                    if testval <= self.upper_endpoint:
                        upper_range_test = True
                    else:
                        upper_range_test = False
                else:
                    if testval < self.upper_endpoint:
                        upper_range_test = True
                    else:
                        upper_range_test = False
                
                death_sentence = lower_range_test and upper_range_test
                # double negatives are a bit weird here but the logic 
                # requires the logic be inverted if testing outside the 
                # defined interval
                if self.kill_if_outside:
                    death_sentence = not death_sentence
                        
                if death_sentence:
                    d.kill()
                    if self.verbose:
                        message1 = "Value associated with key={key} of {dval} failed range test\n".format(key=self.key,dval=testval)
                        message2 ="Interval range is {lower} to {upper}.  ".format(lower=self.lower_endpoint,upper=self.upper_endpoint)
                        message3 = "Test booleans:  lowerEQ={lower}, upperEQ={upper}, kill_inside={inside}".format(lower=self.use_lower_edge,
                                            upper=self.use_upper_edge, inside=self.kill_if_outside) 
                        self.log_kill(d,"MetadataInterval",message1+message2+message3)
        else:
            raise MsPASSError("MetadataInterval received invalid input data",ErrorSeverity.Fatal)
        return d

class FiringSquad(Executioner):
    """
    Used to apply multiple Executioners in a single pass - hence the name 
    FiringSquare image; facing more than one thing that could kill you. 
    The implementation in kill_if_true iterates through a list of 
    Executioners.  Once the datum is killed it is immediately returned.  
    If the Executions are running in verbose mode that means some tests 
    can shadow others.   It is like a firing squad where the guns are fired
    in sequence and the body is removed as soon as there is a hit.  The 
    victim walks away only if all the guns miss.  
    
    Note the class has a += operator to allow appending additional 
    tests to the chain.
    """
    def __init__(self,executioner_list):
        """
        One and only constructor.  executioner_list does not literally 
        have to be a list container.  It can be any container that is iterable so
        a list, tuple, or array can be used.   Internally the contents 
        are copied to a python list container so this the contents of 
        executioner_list are treated as immutable.
        
        The constructor will throw a MsPASSError exception if any of the 
        contents of executioner_list is not a child of the 
        Executioner class. 
        """
        for ex in executioner_list:
            if not isinstance(ex,Executioner):
                raise MsPASSError("FiringSquad constructor:  invalid input.  Expected array of Executioners", ErrorSeverity.Fatal)

        # to allow flexibility of the structure used for input we should 
        # copy the executioner_list.  Further, this assure the 
        # result will iterate correctly and allow for append
        self.executioners = list()
        for ex in executioner_list:
            self.executioners.append(ex)

    def kill_if_true(self,d):
        """
        Implementation of base class method.  In this case failure is 
        defined as not passing one of the set of tests loaded  when 
        the object was created.  As noted earlier the tests are performed 
        in the same order they were passed to the constructor of added on 
        with the += operator.
        :param d: is a mspass data object to be checked 
        """
        if self.input_is_valid(d):
            if d.dead():
                return d
            for killer in self.executioners:
                killer.kill_if_true(d)
                if d.dead():
                    break
            return d
        else:
            raise MsPASSError("FiringSquad received invalid input data",ErrorSeverity.Fatal)
    def __iadd__(self,other):
        """
        Defines the += operator for the class. In that case it means a new 
        test is appended.   Raises a MsPASSError set Fatal other is not a 
        subclass of Executioner.
        """
        if isinstance(other,Executioner):
            self.executioners.append(other)
        else:
            raise MsPASSError("FiringSquare operator +=  rhs is not an Executioner",
                              ErrorSeverity.Fatal)
        return self
