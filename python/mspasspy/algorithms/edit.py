from abc import ABC, abstractmethod
from mspasspy.ccore.seismic import (
    TimeSeries,
    Seismogram,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
)
from mspasspy.ccore.utility import MsPASSError, ErrorSeverity
from mspasspy.util.decorators import mspass_method_wrapper

# These internals are used throughout this module to reduce redundnacy
# in the code base.  I'm putting them here because it will be less confusing
# to a reader where they come from - a common alternative is to put them
# at the end of the file on interwoven


def _input_is_valid(d):
    """
    This internal function standardizes the test to certify the
    input datum, d, is or is not a valid MsPASS data object.   Putting it
    in one place makes extending the code base for other data types much
    easier.  It uses an isinstance tests of d to standardize the test that
    the input is valid data.  It returns True if d is one a valid data
    object known to mspass.  Returns false it not.  Caller must decide
    what to do if the function returns false.
    """
    return isinstance(
        d, (TimeSeries, Seismogram, TimeSeriesEnsemble, SeismogramEnsemble)
    )


def _is_ensemble(d):
    """
    Standardized (simple) test if the data are an ensemble.  Returns true if d is
    an ensemble.  false otherwise.   Will yield a potentially misleading
    False if d is anything but one of the MsPASS ensemble objects.
    e.g. if d is something like a string it will correctly be returned
    as False, but what happens next depends on the algorithm.  Best practice
    should be to precede a call to this function with a call to _input_is_valid
    and trap data that is not a MsPASS data object.
    """
    return isinstance(d, (TimeSeriesEnsemble, SeismogramEnsemble))


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

    def __call__(self, d):
        """
        This method can make the object callable and easier to use.
        After an instance is iniated, instead of explicitly calling
        kill_if_true, one can just call this object.
        For example, instead of:
        int_tester.kill_if_true(enscpy, apply_to_members=True)
        We can just call this:
        int_tester(enscpy, apply_to_members=True)
        """
        self.kill_if_true(d)

    @abstractmethod
    def kill_if_true(self, d):
        """
        This method should run a test on d that will call the kill method
        on MsPASS atomic object d if the test defined by the implementation
        fails.  This is the main working method of this class of function
        objects.
        """
        pass

    def edit_ensemble_members(self, ensemble):
        """
        Subclasses should call this method if the input data are an
        ensemble.   A trick of inheritance allows the algorithm of self
        to then be applied to whole ensemble.  Putting this in the
        base class avoids the duplication of duplicate code in all
        subclasses.

        """
        if ensemble.live:
            for d in ensemble.member:
                self.kill_if_true(d)
        return ensemble

    def log_kill(self, d, testname, message, severity=ErrorSeverity.Informational):
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
        if _input_is_valid(d):
            kill_message = "Killed by kill_if_true method.  Reason:\n"
            kill_message += message
            d.elog.log_error(testname, kill_message, severity)
        else:
            raise MsPASSError(
                "Execution.log_kill method received invalid data;  must be a MsPASS data object",
                ErrorSeverity.Fatal,
            )


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

    def __init__(self, key, value, verbose=False):
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

    @mspass_method_wrapper
    def kill_if_true(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        """
        Implementation of this abstract method for this tester.
        Kills d if the d[self.key] > value stored with the class.

        Returns a (potentially edited) copy of the input to allow use in
        a parallel map operation.
        """
        if _input_is_valid(d):
            if d.dead():
                return d
            if _is_ensemble(d) and apply_to_members:
                self.edit_ensemble_members(d)
            elif self.key in d:
                # because we don't have an else this logic will silently
                # do nothing if key is not defined
                testval = d[self.key]
                if testval > self.value:
                    d.kill()
                    if self.verbose:
                        message = "Value associated with key={key} of {dval} is greater than test value={value}".format(
                            key=self.key, dval=d[self.key], value=self.value
                        )
                        self.log_kill(d, "MetadataGT", message)
        else:
            raise MsPASSError(
                "MetadataGT received invalid input data", ErrorSeverity.Fatal
            )
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

    def __init__(self, key, value, verbose=False):
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

    @mspass_method_wrapper
    def kill_if_true(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        """
        Implementation of this abstract method for this tester.
        Kills d if the d[self.key] >= value stored with the class.

        Returns a (potentially edited) copy of the input to allow use in
        a parallel map operation.
        """
        if _input_is_valid(d):
            if d.dead():
                return d
            if _is_ensemble(d) and apply_to_members:
                self.edit_ensemble_members(d)
            elif self.key in d:
                testval = d[self.key]
                if testval >= self.value:
                    d.kill()
                    if self.verbose:
                        message = "Value associated with key={key} of {dval} is >= test value={value}".format(
                            key=self.key, dval=d[self.key], value=self.value
                        )
                        self.log_kill(d, "MetadataGE", message)
        else:
            raise MsPASSError(
                "MetadataGE received invalid input data", ErrorSeverity.Fatal
            )
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

    def __init__(self, key, value, verbose=False):
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

    @mspass_method_wrapper
    def kill_if_true(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        """
        Implementation of this abstract method for this tester.
        Kills d if the d[self.key] > value stored with the class.

        Returns a (potentially edited) copy of the input to allow use in
        a parallel map operation.
        """
        if _input_is_valid(d):
            if d.dead():
                return d
            if _is_ensemble(d) and apply_to_members:
                self.edit_ensemble_members(d)
            elif self.key in d:
                testval = d[self.key]
                if testval < self.value:
                    d.kill()
                    if self.verbose:
                        message = "Value associated with key={key} of {dval} is < than test value={value}".format(
                            key=self.key, dval=d[self.key], value=self.value
                        )
                        self.log_kill(d, "MetadataLT", message)
        else:
            raise MsPASSError(
                "MetadataLT received invalid input data", ErrorSeverity.Fatal
            )
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

    def __init__(self, key, value, verbose=False):
        """
        One and only constructor.  Sets the parameters that define this
        tester.

        :param key:  key used for extracting a Metadata component for test
        :param value:  is the threshold value for the test.
        :param verbose:  if true informational messages will be posted to
          thee elog area of d.  When false (default) kills are silent.  That
          default is intentioanl to reduce the size of elog data in large data
          sets that often require extensive editing.  Set true if you need
          details on why data were killed or for debugging.
        """
        self.key = key
        self.value = value
        self.verbose = verbose

    @mspass_method_wrapper
    def kill_if_true(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        """
        Implementation of this abstract method for this tester.
        Kills d if the d[self.key] <= value stored with the class.

        Returns a (potentially edited) copy of the input to allow use in
        a parallel map operation.
        """
        if _input_is_valid(d):
            if d.dead():
                return d
            if _is_ensemble(d) and apply_to_members:
                self.edit_ensemble_members(d)
            elif self.key in d:
                testval = d[self.key]
                if testval <= self.value:
                    d.kill()
                    if self.verbose:
                        message = "Value associated with key={key} of {dval} is <= to test value={value}".format(
                            key=self.key, dval=d[self.key], value=self.value
                        )
                        self.log_kill(d, "MetadataLT", message)
        else:
            raise MsPASSError(
                "MetadataLE received invalid input data", ErrorSeverity.Fatal
            )
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

    def __init__(self, key, value, verbose=False):
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

    @mspass_method_wrapper
    def kill_if_true(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        """
        Implementation of this abstract method for this tester.
        Kills d if the d[self.key] == value stored with the class.

        Returns a (potentially edited) copy of the input to allow use in
        a parallel map operation.
        """
        if _input_is_valid(d):
            if d.dead():
                return d
            if _is_ensemble(d) and apply_to_members:
                self.edit_ensemble_members(d)
            elif self.key in d:
                testval = d[self.key]
                if testval == self.value:
                    d.kill()
                    if self.verbose:
                        message = "Value associated with key={key} of {dval} is equal to test value={value}".format(
                            key=self.key, dval=d[self.key], value=self.value
                        )
                        self.log_kill(d, "MetadataEQ", message)
        else:
            raise MsPASSError(
                "MetadataEQ received invalid input data", ErrorSeverity.Fatal
            )
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

    def __init__(self, key, value, verbose=False):
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

    @mspass_method_wrapper
    def kill_if_true(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        """
        Implementation of this abstract method for this tester.
        Kills d if the d[self.key] != value stored with the class.

        Returns a (potentially edited) copy of the input to allow use in
        a parallel map operation.
        """
        if _input_is_valid(d):
            if d.dead():
                return d
            if _is_ensemble(d) and apply_to_members:
                self.edit_ensemble_members(d)
            elif self.key in d:
                testval = d[self.key]
                if testval != self.value:
                    d.kill()
                    if self.verbose:
                        message = "Value associated with key={key} of {dval} is != to test value={value}".format(
                            key=self.key, dval=d[self.key], value=self.value
                        )
                        self.log_kill(d, "MetadataNE", message)
        else:
            raise MsPASSError(
                "MetadataNE received invalid input data", ErrorSeverity.Fatal
            )
        return d


class MetadataDefined(Executioner):
    """
    Implementation of Executioner using an existence test.  The
    constructor loads only a key string.   The test for the kill_if_true
    method is then simply for the existence of a value associated with
    the loaded key.   Data will be killed if the defined key exists
    in the Metadata (header).
    """

    def __init__(self, key, verbose=False):
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

    @mspass_method_wrapper
    def kill_if_true(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        """
        Implementation of this abstract method for this tester.
        Kills d if self.key is defined for this datum

        Returns a (potentially edited) copy of the input to allow use in
        a parallel map operation.
        """
        if _input_is_valid(d):
            if d.dead():
                return d
            if _is_ensemble(d) and apply_to_members:
                self.edit_ensemble_members(d)
            elif d.is_defined(self.key):
                d.kill()
                if self.verbose:
                    message = "Metadata key={key} is defined".format(key=self.key)
                    self.log_kill(d, "MetadataDefined", message)
        else:
            raise MsPASSError(
                "MetadataDefined received invalid input data", ErrorSeverity.Fatal
            )
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

    def __init__(self, key, verbose=False):
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

    @mspass_method_wrapper
    def kill_if_true(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        """
        Implementation of this abstract method for this tester.
        Kills d if self.key is not defined for this datum

        Returns a (potentially edited) copy of the input to allow use in
        a parallel map operation.
        """
        if _input_is_valid(d):
            if d.dead():
                return d
            if _is_ensemble(d) and apply_to_members:
                self.edit_ensemble_members(d)
            elif not d.is_defined(self.key):
                d.kill()
                if self.verbose:
                    message = "Metadata key={key} is not defined".format(key=self.key)
                    self.log_kill(d, "MetadataUndefined", message)
        else:
            raise MsPASSError(
                "MetadataUndefined received invalid input data", ErrorSeverity.Fatal
            )
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

    def __init__(
        self,
        key,
        lower_endpoint,
        upper_endpoint,
        use_lower_edge=True,
        use_upper_edge=True,
        kill_if_outside=True,
        verbose=False,
    ):
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

    @mspass_method_wrapper
    def kill_if_true(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        """
        Implementation of this abstract method for this tester.
        Kills d if the d[self.key] <= value stored with the class.

        Returns a (potentially edited) copy of the input to allow use in
        a parallel map operation.
        """
        if _input_is_valid(d):
            if d.dead():
                return d
            if _is_ensemble(d) and apply_to_members:
                self.edit_ensemble_members(d)
            elif self.key in d:
                testval = d[self.key]
                # These two are used to defined boolean result of lower
                # and upper tests. Truth of result is and of the two
                # inverting the test (inside_test false) inverts the logic (not)
                upper_range_test = False
                lower_range_test = False
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
                        message1 = "Value associated with key={key} of {dval} failed range test\n".format(
                            key=self.key, dval=testval
                        )
                        message2 = "Interval range is {lower} to {upper}.  ".format(
                            lower=self.lower_endpoint, upper=self.upper_endpoint
                        )
                        message3 = "Test booleans:  lowerEQ={lower}, upperEQ={upper}, kill_inside={inside}".format(
                            lower=self.use_lower_edge,
                            upper=self.use_upper_edge,
                            inside=self.kill_if_outside,
                        )
                        self.log_kill(
                            d, "MetadataInterval", message1 + message2 + message3
                        )
        else:
            raise MsPASSError(
                "MetadataInterval received invalid input data", ErrorSeverity.Fatal
            )
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

    def __init__(self, executioner_list):
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
            if not isinstance(ex, Executioner):
                raise MsPASSError(
                    "FiringSquad constructor:  invalid input.  Expected array of Executioners",
                    ErrorSeverity.Fatal,
                )

        # to allow flexibility of the structure used for input we should
        # copy the executioner_list.  Further, this assure the
        # result will iterate correctly and allow for append
        self.executioners = list()
        for ex in executioner_list:
            self.executioners.append(ex)

    @mspass_method_wrapper
    def kill_if_true(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        """
        Implementation of base class method.  In this case failure is
        defined as not passing one of the set of tests loaded  when
        the object was created.  As noted earlier the tests are performed
        in the same order they were passed to the constructor of added on
        with the += operator.
        :param d: is a mspass data object to be checked
        """
        if _input_is_valid(d):
            if d.dead():
                return d
            if _is_ensemble(d) and apply_to_members:
                self.edit_ensemble_members(d)
            else:
                for killer in self.executioners:
                    killer.kill_if_true(d, apply_to_members=apply_to_members)
                    if d.dead():
                        break
            return d
        else:
            raise MsPASSError(
                "FiringSquad received invalid input data", ErrorSeverity.Fatal
            )

    def __iadd__(self, other):
        """
        Defines the += operator for the class. In that case it means a new
        test is appended.   Raises a MsPASSError set Fatal other is not a
        subclass of Executioner.
        """
        if isinstance(other, Executioner):
            self.executioners.append(other)
        else:
            raise MsPASSError(
                "FiringSquare operator +=  rhs is not an Executioner",
                ErrorSeverity.Fatal,
            )
        return self


# Start base class and subclasses for header math operators


class MetadataOperator(ABC):
    """
    Base class for a set of inline Metadata editors. That is, there
    are many instances where Metadata attributes need to be altered
    during a workflow where it is either unnecessary or inappropriate
    to access the database.   e.g. we have found updates in MongoDB
    can be very slow if done one transaction at a time so it can
    streamline processing to edit metadata on the fly.   This base
    class defines the API for a suite of tools for editing metadata
    on the fly.
    """

    @abstractmethod
    def apply(self, d, apply_to_members=False, fast_mode=False):
        """
        Implementations are required to implement this method.  It must
        accept a MsPASS data object, d, (TimeSeries, Seismogram, TimeSeriesEnsemble,
        or SeimogramEnsemble) and apply the operator the implementation defines
        to the Metadata of d.   It should throw a MsPASSError exception if
        d is not one of the expected data types.  It should, on the other
        hand, handle d==None gracefully and do nothing if d is None.
        Ensemble handlers need to deal with an ambiguity in which metadata
        the operation refers to.  Both the overall ensemble container and
        individual members have (normally independent) Metadata containers.
        Implementations of this method should have a apply_to_members
        argument as in the signature for this base class.   When True
        the editing is done by a loop over all members while if
        False (which should be the default) the ensemble's container
        should be used.

        This method must return an (potentially but not guarnateed) edited
        version of d.  That is essential to allow this method to be used
        as the function in a parallel map operator.

        If a required metadata key is missing this method should do nothing
        unless a verbose flag is set.  In that case it should log that as
        an error.   Best practice for any use of operators based on this
        base class is to apply a kill operator for (MetadataDefined) on
        all data before running a calculator.    That way you can guarantee
        the data needed for all operations is valid before trying to
        do calculations on on required metadata.   There is some overhead
        in validating metadata so all implementations should include
        use of "fast_mode".  When set true the safeties will be bypassed
        for speed.  That includes at least the two requried methods
        "check_keys" and "check_operation".  Implementations may add
        other safties.
        """
        pass

    @abstractmethod
    def check_keys(self, d):
        """
        All implementation should implement this checker even if all it contains
        is a pass.  It should validate the keys are defined in data to be
        handled in the apply method.   An early call in apply should always
        be to call this method.
        """
        pass

    @abstractmethod
    def check_operation(self, d):
        """
        All implementations should implement this method even if they choose
        to ignore it.   It should be used to guarantee the operator the class
        defines will succeed on the datum sent to the apply method.
        e.g. any standard arithmetic operations will throw a TypeError if
        one of the operands is a string.   This method should be used to
        make the operator as bombproof as possible logging a message
        to the datum rather than aborting if there is an issue.  Some
        classes may want to implement this as pass because it makes
        no sense - e.g. setting a constant value.   Those are the
        exception, however, so the api dogmatically demands these
        be implemented even if they do nothing.
        """
        pass

    def edit_ensemble_members(self, ensemble):
        """
        Subclasses should call this method if the input data are an
        ensemble.   A trick of inheritance allows the algorithm of self
        to then be applied to whole ensemble.  Putting this in the
        base class avoids the duplication of duplicate code in all \
        subclasses.

        """
        if ensemble.live:
            for d in ensemble.member:
                self.apply(d)
        return ensemble

    def log_edit(self, d, testname, message, severity=ErrorSeverity.Informational):
        """
        This base class method is used to standardize the error logging
        functionality of all editors.   It writes a standardized
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
        if _input_is_valid(d):
            edit_message = "Altered by apply method of this class.  Details:\n"
            edit_message += message
            d.elog.log_error(testname, edit_message, severity)
        else:
            raise MsPASSError(
                "MetadataOperator.apply method received invalid data;  must be a MsPASS data object",
                ErrorSeverity.Fatal,
            )


class ChangeKey(MetadataOperator):
    def __init__(self, oldkey, newkey, erase_old=True):
        # important to validate type of keys to avoid unexpected exceptions
        # when used
        if isinstance(oldkey, str) and isinstance(newkey, str):
            self.oldkey = oldkey
            self.newkey = newkey
            self.erase_old = erase_old
        else:
            message = (
                "ChangeKey constructor usage error.  ChangeKey(oldkey,newkey)\n"
                + "oldkey and newkey must be strings defining metadata keys - check types"
            )
            raise MsPASSError(message, ErrorSeverity.Fatal)

    @mspass_method_wrapper
    def apply(
        self,
        d,
        *args,
        apply_to_members=False,
        fast_mode=False,
        verbose=False,
        handles_enembles=True,
        **kwargs,
    ):
        if _input_is_valid(d):
            if d.dead():
                return d
            else:
                if not fast_mode:
                    keysok = self.check_keys(d, apply_to_members)
                    if not keysok:
                        if verbose:
                            message = "key=" + self.oldkey + " is not defined"
                            self.log_edit(
                                d, "ChangeKey", message, ErrorSeverity.Invalid
                            )
                        return d
            # from here on we can assume operation is ok
            if _is_ensemble(d) and apply_to_members:
                for m in d.member:
                    if m.dead():
                        continue
                    val = m[self.oldkey]
                    m[self.newkey] = val
                    if self.erase_old:
                        m.erase(self.oldkey)
            else:
                # Land here if for all atomic data and if asked
                # to only modify the ensemble (global) Metadata container.
                # i.e. when apply_to_member is false.  Makes the logic a
                # bit confusing but reduces redundant code
                val = d[self.oldkey]
                d[self.newkey] = val
                if self.erase_old:
                    d.erase(self.oldkey)
            return d
        else:
            raise MsPASSError(
                "ChangeKey.apply:   input is not a valid MsPASS data object",
                ErrorSeverity.Fatal,
            )

    def check_keys(self, d, apply_to_members):
        if _is_ensemble(d) and apply_to_members:
            for m in d.member:
                if m.dead():
                    continue
                if not m.is_defined(self.oldkey):
                    return False
            return True
        else:
            if d.is_defined(self.oldkey):
                return True
            else:
                return False

    def check_operation(self, d):
        """
        there is no operation on a ChangeKey so this method does nothing.
        Because it is an abstract base we have to have this stub.

        """
        pass


class SetValue(MetadataOperator):
    """
    Used to set a specified metadata key to a constant value.  Note any
    existing value of Metadata associated with the key defined in the
    operator always be overwritten.

    Example:  to set the value of key = 'a' to constant 2.0

      op = SetValue('a',2.0)
      d = op.apply(d)
    """

    def __init__(self, key, constant_value=0):
        """
        Constructor for this operator.

        :param key: string defining key for Metadata to be set.
        :param constant_value:  is the value to use for setting the
          attribute linked to the key.  Note we do no type checking so
          beware of implicit integer conversion (e.g. use 2.0 if you mean
          want a float 2.0 not 2 which python would auto cast to an int)
        """
        self.key = key
        self.value = constant_value

    @mspass_method_wrapper
    def apply(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        """
        Used to apply this operator to Metadata of a MsPASS data object.
        Use of decorator adds common MsPASS arguments as call options.

        :param d:  datum to which operator is to be applied.  d must be a
          valid MsPASS data object or this method will throw a fatal MsPASSError
          exception.  If d is marked dead it will be silently ignored.
        :param apply_to_members:  when true and d is an ensemble object the
          operator is applied to the members.  When false the metadata for the
          ensemble will be altered.  This parameter is ignored for atomic
          data types.  Default is False.

        :return:  always returns a (usually) edited copy of the input d.
          When the input d is dead the copy will always be unaltered.
          Note the copy is a shallow copy which in python means we just return
          the equivalent of a pointer to the caller.  Important for efficiency
          as d can be very large for some ensembles.

        """
        if _input_is_valid(d):
            if d.dead():
                return d
            else:
                if _is_ensemble(d) and apply_to_members:
                    d = self.edit_ensemble_members(d)
                else:
                    d[self.key] = self.value
                return d
        else:
            raise MsPASSError(
                "SetValue.apply:   input is not a valid MsPASS data object",
                ErrorSeverity.Fatal,
            )

    def check_keys(self, d):
        """
        Useless implementation of required abstract base method.  In this
        case all it does is test if the stored value of key is a string.
        Returns true if the key is a string and false otherwise.

        """
        if isinstance(self.key, str):
            return True
        else:
            return False

    def check_operation(self, d):
        pass


# all arithmetic operators need an output key to place result that can be the same as one of the inputs
# These are unary - operate using a constant value set on constructoin
class Add(MetadataOperator):
    """
    Used to implement += operator on a specified Metadata key.
    Example:  to add 2 to data, d, with key='icdp' could use this

      op = Add('icdp',2)
      d = op.apply(d)
    """

    def __init__(self, key, value_to_add=1):
        self.key = key
        self.value = value_to_add

    @mspass_method_wrapper
    def apply(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        elognametag = "Metadata Add Operator"
        if _input_is_valid(d):
            if d.dead():
                return d
            else:
                if _is_ensemble(d) and apply_to_members:
                    d = self.edit_ensemble_members(d)
                else:
                    if not self.check_keys(d):
                        message = "Required key=" + self.key + " not defined"
                        self.log_edit(d, elognametag, message, ErrorSeverity.Invalid)
                        d.kill()
                        return d
                    elif not self.check_operation(d):
                        message = (
                            "The += operator fails on Metadata with key="
                            + self.key
                            + "\nAn incompatible type likely is stored with that key"
                        )
                        d.kill()
                        return d

                    val = d[self.key]
                    val += self.value
                    d[self.key] = val
            return d
        else:
            raise MsPASSError(
                "Add.apply:   input is not a valid MsPASS data object",
                ErrorSeverity.Fatal,
            )

    def check_keys(self, d):
        """
        checks that the key defined for the operator is defined for d.
        Returns true if is defined and false if not.  the method assumes
        d is a valid child of Metadata so the is_defined method will
        not generate an exception.  that means this method should ALWAYS
        be called after a test with _input_is_valid.
        """
        if d.is_defined(self.key):
            return True
        else:
            return False

    def check_operation(self, d):
        """
        This method checks to make sure the value works with the operation
        required.  It provides a (required by the base class) standardization
        of testing for validity of the operation.
        """
        # we catch any exception - think we could be explicity with a TypeError
        # but this is more robust
        try:
            testval = d[self.key]
            testval += self.value
        except:
            return False
        else:
            return True


class Multiply(MetadataOperator):
    """
    Used to implement *= operator on a specified Metadata key.
    Example:  to multiple metadata in, d, with key='Pamp' by 2.5
    you could use this

      op = Multiply('Pamp',2.5)
      d = op.apply(d)
    """

    def __init__(self, key, value_to_multiply=1):
        self.key = key
        self.value = value_to_multiply

    @mspass_method_wrapper
    def apply(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        elognametag = "Metadata Multiply Operator"
        if _input_is_valid(d):
            if d.dead():
                return d
            else:
                if _is_ensemble(d) and apply_to_members:
                    d = self.edit_ensemble_members(d)
                else:
                    if not self.check_keys(d):
                        message = "Required key=" + self.key + " not defined"
                        self.log_edit(d, elognametag, message, ErrorSeverity.Invalid)
                        d.kill()
                        return d
                    elif not self.check_operation(d):
                        message = (
                            "The *= operator fails on Metadata with key="
                            + self.key
                            + "\nAn incompatible type likely is stored with that key"
                        )
                        d.kill()
                        return d

                    val = d[self.key]
                    val *= self.value
                    d[self.key] = val
            return d
        else:
            raise MsPASSError(
                "Multiply.apply:   input is not a valid MsPASS data object",
                ErrorSeverity.Fatal,
            )

    def check_keys(self, d):
        """
        checks that the key defined for the operator is defined for d.
        Returns true if is defined and false if not.  the method assumes
        d is a valid child of Metadata so the is_defined method will
        not generate an exception.  that means this method should ALWAYS
        be called after a test with _input_is_valid.
        """
        if d.is_defined(self.key):
            return True
        else:
            return False

    def check_operation(self, d):
        """
        This method checks to make sure the value works with the operation
        required.  It provides a (required by the base class) standardization
        of testing for validity of the operation.
        """
        # we catch any exception - think we could be explicity with a TypeError
        # but this is more robust
        try:
            testval = d[self.key]
            testval *= self.value
        except:
            return False
        else:
            return True


class Subtract(MetadataOperator):
    """
    Used to implement -= operator on a specified Metadata key.
    Example:  to subtract 2 from metadata, d, with key='icdp' could use this

      op = Subtract('icdp',2)
      d = op.apply(d)
    """

    def __init__(self, key, value_to_subtract):
        self.key = key
        self.value = value_to_subtract

    @mspass_method_wrapper
    def apply(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        elognametag = "Metadata Subtract Operator"
        if _input_is_valid(d):
            if d.dead():
                return d
            else:
                if _is_ensemble(d) and apply_to_members:
                    d = self.edit_ensemble_members(d)
                else:
                    if not self.check_keys(d):
                        message = "Required key=" + self.key + " not defined"
                        self.log_edit(d, elognametag, message, ErrorSeverity.Invalid)
                        d.kill()
                        return d
                    elif not self.check_operation(d):
                        message = (
                            "The -= operator fails on Metadata with key="
                            + self.key
                            + "\nAn incompatible type likely is stored with that key"
                        )
                        d.kill()
                        return d

                    val = d[self.key]
                    val -= self.value
                    d[self.key] = val
            return d
        else:
            raise MsPASSError(
                "Subtract.apply:   input is not a valid MsPASS data object",
                ErrorSeverity.Fatal,
            )

    def check_keys(self, d):
        """
        checks that the key defined for the operator is defined for d.
        Returns true if is defined and false if not.  the method assumes
        d is a valid child of Metadata so the is_defined method will
        not generate an exception.  that means this method should ALWAYS
        be called after a test with _input_is_valid.
        """
        if d.is_defined(self.key):
            return True
        else:
            return False

    def check_operation(self, d):
        """
        This method checks to make sure the value works with the operation
        required.  It provides a (required by the base class) standardization
        of testing for validity of the operation.
        """
        # we catch any exception - think we could be explicity with a TypeError
        # but this is more robust
        try:
            testval = d[self.key]
            testval -= self.value
        except:
            return False
        else:
            return True


class Divide(MetadataOperator):
    """
    Used to implement /= operator on a specified Metadata key.
    Example:  to divide metadata in, d, with key='Pamp' by 2.0
    you could use this

      op = Divide('Pamp',2.-)
      d = op.apply(d)
    """

    def __init__(self, key, value_to_divide=1):
        self.key = key
        self.value = value_to_divide

    @mspass_method_wrapper
    def apply(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        elognametag = "Metadata Divide Operator"
        if _input_is_valid(d):
            if d.dead():
                return d
            else:
                if _is_ensemble(d) and apply_to_members:
                    d = self.edit_ensemble_members(d)
                else:
                    if not self.check_keys(d):
                        message = "Required key=" + self.key + " not defined"
                        self.log_edit(d, elognametag, message, ErrorSeverity.Invalid)
                        d.kill()
                        return d
                    elif not self.check_operation(d):
                        message = (
                            "The /= operator fails on Metadata with key="
                            + self.key
                            + "\nAn incompatible type likely is stored with that key"
                        )
                        d.kill()
                        return d

                    val = d[self.key]
                    val /= self.value
                    d[self.key] = val
            return d
        else:
            raise MsPASSError(
                "Divide.apply:   input is not a valid MsPASS data object",
                ErrorSeverity.Fatal,
            )

    def check_keys(self, d):
        """
        checks that the key defined for the operator is defined for d.
        Returns true if is defined and false if not.  the method assumes
        d is a valid child of Metadata so the is_defined method will
        not generate an exception.  that means this method should ALWAYS
        be called after a test with _input_is_valid.
        """
        if d.is_defined(self.key):
            return True
        else:
            return False

    def check_operation(self, d):
        """
        This method checks to make sure the value works with the operation
        required.  It provides a (required by the base class) standardization
        of testing for validity of the operation.
        """
        # we catch any exception - think we could be explicity with a TypeError
        # but this is more robust
        try:
            testval = d[self.key]
            testval /= self.value
        except:
            return False
        else:
            return True


class IntegerDivide(MetadataOperator):
    """
    Used to implement // operator on a specified Metadata key.
    The // operator is a bit obscure but it implements the common
    need to truncate a division result to an integer.  This will work
    on floats but the result will always be close to and integer
    value as if the operation were done with integers.  Note
    also IntegerDivide is the complement to Mod which returns the remainder
    of such a division.

    Example:  to apply integer division to metadata in, d, with key='icdp' by 5
    you could use this

      op = IntegerDivide('icdp',5)
      d = op.apply(d)
    """

    def __init__(self, key, value=1):
        self.key = key
        self.value = value

    @mspass_method_wrapper
    def apply(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        elognametag = "Metadata IntegerDivide Operator"
        if _input_is_valid(d):
            if d.dead():
                return d
            else:
                if _is_ensemble(d) and apply_to_members:
                    d = self.edit_ensemble_members(d)
                else:
                    if not self.check_keys(d):
                        message = "Required key=" + self.key + " not defined"
                        self.log_edit(d, elognametag, message, ErrorSeverity.Invalid)
                        d.kill()
                        return d
                    elif not self.check_operation(d):
                        message = (
                            "The // operator fails on Metadata with key="
                            + self.key
                            + "\nAn incompatible type likely is stored with that key"
                        )
                        d.kill()
                        return d

                    val = d[self.key]
                    # there is a //= operator but it is obscure in this context
                    val = val // self.value
                    d[self.key] = val
            return d
        else:
            raise MsPASSError(
                "IntegerDivide.apply:   input is not a valid MsPASS data object",
                ErrorSeverity.Fatal,
            )

    def check_keys(self, d):
        """
        checks that the key defined for the operator is defined for d.
        Returns true if is defined and false if not.  the method assumes
        d is a valid child of Metadata so the is_defined method will
        not generate an exception.  that means this method should ALWAYS
        be called after a test with _input_is_valid.
        """
        if d.is_defined(self.key):
            return True
        else:
            return False

    def check_operation(self, d):
        """
        This method checks to make sure the value works with the operation
        required.  It provides a (required by the base class) standardization
        of testing for validity of the operation.
        """
        # we catch any exception - think we could be explicity with a TypeError
        # but this is more robust
        try:
            testval = d[self.key]
            testval = testval // self.value
        except:
            return False
        else:
            return True


class Mod(MetadataOperator):
    """
    Used to implement % operator on a specified Metadata key.
    The % operator is a bit obscure but it implements the common
    need return the remainder of a divide operation.   It is commonly
    used, for example, in cmp processing where survey flag numbers can
    often be converted to channel numbers for simple multichannel cable
    geometries.

    This operator will work any numeric type but it is most commonly used
    for integer attributes.

    Example:  to convert the metadata associated with the key 'ichan' that
    are currently counting by 1 to numbers that cycle from 0 to 23 us this:

      op = Mod('ix',24)
      d = op.apply(d)
    """

    # note default would zero everything if used
    def __init__(self, key, value=1):
        self.key = key
        self.value = value

    @mspass_method_wrapper
    def apply(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        elognametag = "Metadata Mod Operator"
        if _input_is_valid(d):
            if d.dead():
                return d
            else:
                if _is_ensemble(d) and apply_to_members:
                    d = self.edit_ensemble_members(d)
                else:
                    if not self.check_keys(d):
                        message = "Required key=" + self.key + " not defined"
                        self.log_edit(d, elognametag, message, ErrorSeverity.Invalid)
                        d.kill()
                        return d
                    elif not self.check_operation(d):
                        message = (
                            "The % operator fails on Metadata with key="
                            + self.key
                            + "\nAn incompatible type likely is stored with that key"
                        )
                        d.kill()
                        return d

                    val = d[self.key]
                    # there is a //= operator but it is obscure in this context
                    val = val % self.value
                    d[self.key] = val
            return d
        else:
            raise MsPASSError(
                "Mod.apply:   input is not a valid MsPASS data object",
                ErrorSeverity.Fatal,
            )

    def check_keys(self, d):
        """
        checks that the key defined for the operator is defined for d.
        Returns true if is defined and false if not.  the method assumes
        d is a valid child of Metadata so the is_defined method will
        not generate an exception.  that means this method should ALWAYS
        be called after a test with _input_is_valid.
        """
        if d.is_defined(self.key):
            return True
        else:
            return False

    def check_operation(self, d):
        """
        This method checks to make sure the value works with the operation
        required.  It provides a (required by the base class) standardization
        of testing for validity of the operation.
        """
        # we catch any exception - think we could be explicity with a TypeError
        # but this is more robust
        try:
            testval = d[self.key]
            testval = testval % self.value
        except:
            return False
        else:
            return True


# These are binary operators using two keys set in constructor
class Add2(MetadataOperator):
    """
    Used to implement + operator that adds two Metadata attributes
    together.  The attributes are feteched with two keys set when
    the operator is constructed.  Let a be the value in a datum
    associated with key2 (arg1 of constructor) and b be the
    value associated with key2 (arg2 of constructor).  The apply
    method of this class computes a+b and sets the Metadata attribute
    defined by key0 (arg0 of constructor) to that value (a+b)
    i.e. d[key0] = d[key1] + d[key2]

    Note key0 can be the same as either key1 or key2. The contents of the
    left hand side (key0) are always set by this operator unless that
    input was previously marked dead.  Further key1 and key2 can be the
    same although it is hard to conceive how that could be useful.

    Example:  to compute ix as the d['sx'] + d['chan'] use

      op = Add2('ix1','sx','chan')
      d = op.apply(d)
    """

    def __init__(self, key0, key1, key2):
        self.key0 = key0
        self.key1 = key1
        self.key2 = key2

    @mspass_method_wrapper
    def apply(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        elognametag = "Metadata Add2 Operator"
        if _input_is_valid(d):
            if d.dead():
                return d
            else:
                if _is_ensemble(d) and apply_to_members:
                    d = self.edit_ensemble_members(d)
                else:
                    if not self.check_keys(d):
                        message = (
                            "Required keys key1="
                            + self.key1
                            + " and/or key2="
                            + self.key2
                            + " not defined"
                        )
                        self.log_edit(d, elognametag, message, ErrorSeverity.Invalid)
                        d.kill()
                        return d
                    elif not self.check_operation(d):
                        message = (
                            "The + operator failed with Metadata fetched with keys="
                            + self.key1
                            + " and "
                            + self.key2
                            + "\nAn incompatible type likely is stored with that key"
                        )
                        d.kill()
                        return d
                    val1 = d[self.key1]
                    val2 = d[self.key2]
                    d[self.key0] = val1 + val2
                return d
        else:
            raise MsPASSError(
                "Add2.apply:   input is not a valid MsPASS data object",
                ErrorSeverity.Fatal,
            )

    def check_keys(self, d):
        """
        checks that the keys required for the operator are defined for d.
        If either required key are missing from d return False.  Return
        True if both are set in d. The method assumes
        d is a valid child of Metadata so that the is_defined method will
        not generate an exception.  that means this method should ALWAYS
        be called after a test with _input_is_valid.
        """
        if d.is_defined(self.key1) and d.is_defined(self.key2):
            return True
        else:
            return False

    def check_operation(self, d):
        """
        This method checks to make sure the value works with the operation
        required.  It provides a (required by the base class) standardization
        of testing for validity of the operation.
        """
        # we catch any exception - think we could be explicity with a TypeError
        # but this is more robust
        try:
            val1 = d[self.key1]
            val2 = d[self.key2]
            testval = val1 + val2
        except:
            return False
        else:
            return True


class Multiply2(MetadataOperator):
    """
    Used to implement * operator that multiplies two Metadata attributes
    together.  The attributes are feteched with two keys set when
    the operator is constructed.  Let a be the value in a datum
    associated with key2 (arg1 of constructor) and b be the
    value associated with key2 (arg2 of constructor).  The apply
    method of this class computes a*b and sets the Metadata attribute
    defined by key0 (arg0 of constructor) to that value (a*b)
    i.e. d[key0] = d[key1] * d[key2]

    Note key0 can be the same as either key1 or key2. The contents of the
    left hand side (key0) are always set by this operator unless that
    input was previously marked dead.  Further key1 and key2 can be the
    same although it is hard to conceive how that could be useful.

    Example:  to compute ix as the d['sx'] * d['chan'] use

      op = Multiply2('ix1','sx','chan')
      d = op.apply(d)
    """

    def __init__(self, key0, key1, key2):
        self.key0 = key0
        self.key1 = key1
        self.key2 = key2

    @mspass_method_wrapper
    def apply(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        elognametag = "Metadata Multiply2 Operator"
        if _input_is_valid(d):
            if d.dead():
                return d
            else:
                if _is_ensemble(d) and apply_to_members:
                    d = self.edit_ensemble_members(d)
                else:
                    if not self.check_keys(d):
                        message = (
                            "Required keys key1="
                            + self.key1
                            + " and/or key2="
                            + self.key2
                            + " not defined"
                        )
                        self.log_edit(d, elognametag, message, ErrorSeverity.Invalid)
                        d.kill()
                        return d
                    elif not self.check_operation(d):
                        message = (
                            "The * operator failed with Metadata fetched with keys="
                            + self.key1
                            + " and "
                            + self.key2
                            + "\nAn incompatible type likely is stored with that key"
                        )
                        d.kill()
                        return d
                    val1 = d[self.key1]
                    val2 = d[self.key2]
                    d[self.key0] = val1 * val2
                return d
        else:
            raise MsPASSError(
                "Multiply2.apply:   input is not a valid MsPASS data object",
                ErrorSeverity.Fatal,
            )

    def check_keys(self, d):
        """
        checks that the keys required for the operator are defined for d.
        If either required key are missing from d return False.  Return
        True if both are set in d. The method assumes
        d is a valid child of Metadata so that the is_defined method will
        not generate an exception.  that means this method should ALWAYS
        be called after a test with _input_is_valid.
        """
        if d.is_defined(self.key1) and d.is_defined(self.key2):
            return True
        else:
            return False

    def check_operation(self, d):
        """
        This method checks to make sure the value works with the operation
        required.  It provides a (required by the base class) standardization
        of testing for validity of the operation.
        """
        # we catch any exception - think we could be explicity with a TypeError
        # but this is more robust
        try:
            val1 = d[self.key1]
            val2 = d[self.key2]
            testval = val1 * val2
        except:
            return False
        else:
            return True


class Subtract2(MetadataOperator):
    """
    Used to implement - operator that computes the difference of two Metadata
    attributes.  The attributes are feteched with two keys set when
    the operator is constructed.  Let a be the value in a datum
    associated with key2 (arg1 of constructor) and b be the
    value associated with key2 (arg2 of constructor).  The apply
    method of this class computes a-b and sets the Metadata attribute
    defined by key0 (arg0 of constructor) to that value (a-b)
    i.e. d[key0] = d[key1] - d[key2]

    Note key0 can be the same as either key1 or key2. The contents of the
    left hand side (key0) are always set by this operator unless that
    input was previously marked dead.  Further key1 and key2 can be the
    same although it is hard to conceive how that could be useful.

    Example:  to compute ix as the d['sx'] - d['chan'] use

      op = Subtract2('ix1','sx','chan')
      d = op.apply(d)
    """

    def __init__(self, key0, key1, key2):
        self.key0 = key0
        self.key1 = key1
        self.key2 = key2

    @mspass_method_wrapper
    def apply(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        elognametag = "Metadata Subtract2 Operator"
        if _input_is_valid(d):
            if d.dead():
                return d
            else:
                if _is_ensemble(d) and apply_to_members:
                    d = self.edit_ensemble_members(d)
                else:
                    if not self.check_keys(d):
                        message = (
                            "Required keys key1="
                            + self.key1
                            + " and/or key2="
                            + self.key2
                            + " not defined"
                        )
                        self.log_edit(d, elognametag, message, ErrorSeverity.Invalid)
                        d.kill()
                        return d
                    elif not self.check_operation(d):
                        message = (
                            "The - operator failed with Metadata fetched with keys="
                            + self.key1
                            + " and "
                            + self.key2
                            + "\nAn incompatible type likely is stored with that key"
                        )
                        d.kill()
                        return d
                    val1 = d[self.key1]
                    val2 = d[self.key2]
                    d[self.key0] = val1 - val2
                return d
        else:
            raise MsPASSError(
                "Subtract2.apply:   input is not a valid MsPASS data object",
                ErrorSeverity.Fatal,
            )

    def check_keys(self, d):
        """
        checks that the keys required for the operator are defined for d.
        If either required key are missing from d return False.  Return
        True if both are set in d. The method assumes
        d is a valid child of Metadata so that the is_defined method will
        not generate an exception.  that means this method should ALWAYS
        be called after a test with _input_is_valid.
        """
        if d.is_defined(self.key1) and d.is_defined(self.key2):
            return True
        else:
            return False

    def check_operation(self, d):
        """
        This method checks to make sure the value works with the operation
        required.  It provides a (required by the base class) standardization
        of testing for validity of the operation.
        """
        # we catch any exception - think we could be explicity with a TypeError
        # but this is more robust
        try:
            val1 = d[self.key1]
            val2 = d[self.key2]
            testval = val1 - val2
        except:
            return False
        else:
            return True


class Divide2(MetadataOperator):
    """
    Used to implement / operator that divides two Metadata attributes.
    The attributes are feteched with two keys set when
    the operator is constructed.  Let a be the value in a datum
    associated with key2 (arg1 of constructor) and b be the
    value associated with key2 (arg2 of constructor).  The apply
    method of this class computes a/b and sets the Metadata attribute
    defined by key0 (arg0 of constructor) to that value (a/b)
    i.e. d[key0] = d[key1] / d[key2]

    Note key0 can be the same as either key1 or key2. The contents of the
    left hand side (key0) are always set by this operator unless that
    input was previously marked dead.  Further key1 and key2 can be the
    same although it is hard to conceive how that could be useful.

    Example:  to compute ix as the d['sx'] / d['chan'] use

      op = Divide2('ix1','sx','chan')
      d = op.apply(d)
    """

    def __init__(self, key0, key1, key2):
        self.key0 = key0
        self.key1 = key1
        self.key2 = key2

    @mspass_method_wrapper
    def apply(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        elognametag = "Metadata Divide2 Operator"
        if _input_is_valid(d):
            if d.dead():
                return d
            else:
                if _is_ensemble(d) and apply_to_members:
                    d = self.edit_ensemble_members(d)
                else:
                    if not self.check_keys(d):
                        message = (
                            "Required keys key1="
                            + self.key1
                            + " and/or key2="
                            + self.key2
                            + " not defined"
                        )
                        self.log_edit(d, elognametag, message, ErrorSeverity.Invalid)
                        d.kill()
                        return d
                    elif not self.check_operation(d):
                        message = (
                            "The / operator failed with Metadata fetched with keys="
                            + self.key1
                            + " and "
                            + self.key2
                            + "\nAn incompatible type likely is stored with that key"
                        )
                        d.kill()
                        return d
                    val1 = d[self.key1]
                    val2 = d[self.key2]
                    d[self.key0] = val1 / val2
                return d
        else:
            raise MsPASSError(
                "Divide2.apply:   input is not a valid MsPASS data object",
                ErrorSeverity.Fatal,
            )

    def check_keys(self, d):
        """
        checks that the keys required for the operator are defined for d.
        If either required key are missing from d return False.  Return
        True if both are set in d. The method assumes
        d is a valid child of Metadata so that the is_defined method will
        not generate an exception.  that means this method should ALWAYS
        be called after a test with _input_is_valid.
        """
        if d.is_defined(self.key1) and d.is_defined(self.key2):
            return True
        else:
            return False

    def check_operation(self, d):
        """
        This method checks to make sure the value works with the operation
        required.  It provides a (required by the base class) standardization
        of testing for validity of the operation.
        """
        # we catch any exception - think we could be explicity with a TypeError
        # but this is more robust
        try:
            val1 = d[self.key1]
            val2 = d[self.key2]
            testval = val1 / val2
        except:
            return False
        else:
            return True


class IntegerDivide2(MetadataOperator):
    """
    Used to implement // operator between two Metadata attributes.
    The attributes are feteched with two keys set when
    the operator is constructed.  Let a be the value in a datum
    associated with key2 (arg1 of constructor) and b be the
    value associated with key2 (arg2 of constructor).  The apply
    method of this class computes a//b and sets the Metadata attribute
    defined by key0 (arg0 of constructor) to that value (a+b)
    i.e. d[key0] = d[key1] // d[key2]

    Note key0 can be the same as either key1 or key2. The contents of the
    left hand side (key0) are always set by this operator unless that
    input was previously marked dead.  Further key1 and key2 can be the
    same although it is hard to conceive how that could be useful.

    Example:  to compute ix as the d['sx'] // d['chan'] use

      op = IntegerDivide2('ix1','sx','chan')
      d = op.apply(d)
    """

    def __init__(self, key0, key1, key2):
        self.key0 = key0
        self.key1 = key1
        self.key2 = key2

    @mspass_method_wrapper
    def apply(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        elognametag = "Metadata IntegerDivide2 Operator"
        if _input_is_valid(d):
            if d.dead():
                return d
            else:
                if _is_ensemble(d) and apply_to_members:
                    d = self.edit_ensemble_members(d)
                else:
                    if not self.check_keys(d):
                        message = (
                            "Required keys key1="
                            + self.key1
                            + " and/or key2="
                            + self.key2
                            + " not defined"
                        )
                        self.log_edit(d, elognametag, message, ErrorSeverity.Invalid)
                        d.kill()
                        return d
                    elif not self.check_operation(d):
                        message = (
                            "The // operator failed with Metadata fetched with keys="
                            + self.key1
                            + " and "
                            + self.key2
                            + "\nAn incompatible type likely is stored with that key"
                        )
                        d.kill()
                        return d
                    val1 = d[self.key1]
                    val2 = d[self.key2]
                    d[self.key0] = val1 // val2
                return d
        else:
            raise MsPASSError(
                "IntegerDivide2.apply:   input is not a valid MsPASS data object",
                ErrorSeverity.Fatal,
            )

    def check_keys(self, d):
        """
        checks that the keys required for the operator are defined for d.
        If either required key are missing from d return False.  Return
        True if both are set in d. The method assumes
        d is a valid child of Metadata so that the is_defined method will
        not generate an exception.  that means this method should ALWAYS
        be called after a test with _input_is_valid.
        """
        if d.is_defined(self.key1) and d.is_defined(self.key2):
            return True
        else:
            return False

    def check_operation(self, d):
        """
        This method checks to make sure the value works with the operation
        required.  It provides a (required by the base class) standardization
        of testing for validity of the operation.
        """
        # we catch any exception - think we could be explicity with a TypeError
        # but this is more robust
        try:
            val1 = d[self.key1]
            val2 = d[self.key2]
            testval = val1 // val2
        except:
            return False
        else:
            return True


class Mod2(MetadataOperator):
    """
    Used to implement % operator between two Metadata attributes.
    The attributes are feteched with two keys set when
    the operator is constructed.  Let a be the value in a datum
    associated with key2 (arg1 of constructor) and b be the
    value associated with key2 (arg2 of constructor).  The apply
    method of this class computes a+b and sets the Metadata attribute
    defined by key0 (arg0 of constructor) to that value (a%b)
    i.e. d[key0] = d[key1] % d[key2]

    Note key0 can be the same as either key1 or key2. The contents of the
    left hand side (key0) are always set by this operator unless that
    input was previously marked dead.  Further key1 and key2 can be the
    same although it is hard to conceive how that could be useful.

    Example:  to compute ix as the d['sx'] % d['chan'] use

      op = Mod2('ix1','sx','chan')
      d = op.apply(d)
    """

    def __init__(self, key0, key1, key2):
        self.key0 = key0
        self.key1 = key1
        self.key2 = key2

    @mspass_method_wrapper
    def apply(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        elognametag = "Metadata Mod2 Operator"
        if _input_is_valid(d):
            if d.dead():
                return d
            else:
                if _is_ensemble(d) and apply_to_members:
                    d = self.edit_ensemble_members(d)
                else:
                    if not self.check_keys(d):
                        message = (
                            "Required keys key1="
                            + self.key1
                            + " and/or key2="
                            + self.key2
                            + " not defined"
                        )
                        self.log_edit(d, elognametag, message, ErrorSeverity.Invalid)
                        d.kill()
                        return d
                    elif not self.check_operation(d):
                        message = (
                            "The % operator failed with Metadata fetched with keys="
                            + self.key1
                            + " and "
                            + self.key2
                            + "\nAn incompatible type likely is stored with that key"
                        )
                        d.kill()
                        return d
                    val1 = d[self.key1]
                    val2 = d[self.key2]
                    d[self.key0] = val1 % val2
                return d
        else:
            raise MsPASSError(
                "Mod2.apply:   input is not a valid MsPASS data object",
                ErrorSeverity.Fatal,
            )

    def check_keys(self, d):
        """
        checks that the keys required for the operator are defined for d.
        If either required key are missing from d return False.  Return
        True if both are set in d. The method assumes
        d is a valid child of Metadata so that the is_defined method will
        not generate an exception.  that means this method should ALWAYS
        be called after a test with _input_is_valid.
        """
        if d.is_defined(self.key1) and d.is_defined(self.key2):
            return True
        else:
            return False

    def check_operation(self, d):
        """
        This method checks to make sure the value works with the operation
        required.  It provides a (required by the base class) standardization
        of testing for validity of the operation.
        """
        # we catch any exception - think we could be explicity with a TypeError
        # but this is more robust
        try:
            val1 = d[self.key1]
            val2 = d[self.key2]
            testval = val1 % val2
        except:
            return False
        else:
            return True


# Note this class was patterned closely after FiringSquad - the approach
# is identical.  I did little more than edit a copy of FiringSquad to produce
# this class
class MetadataOperatorChain(MetadataOperator):
    """
    Used to apply multiple a chain of arithmetic operators to derive
    computed metadata attributes.  Very elaborate calculations can be
    done through this class by chaining appropriate atomic operators
    defined elsewhere in the module (i.e. Add, Subtract, etc.).
    The operation chain is defined by a python list of the atomic operators.
    When the apply method of this class is called the list of operators
    are applied sequentially in list order.

    Note the class has a += operator to allow appending additional
    operators to the chain.
    """

    def __init__(self, operator_list):
        """
        One and only constructor.  operator_list does not literally
        have to be a list container.  It can be any container that is iterable so
        a list, tuple, or array can be used.   Internally the contents
        are copied to a python list container so this the contents of
        operator_list sent to the constructor are treated not unintentionally
        modified.

        The constructor will throw a MsPASSError exception if any of the
        contents of operator_list is not a child of the
        MetadataOperator base class.
        """
        for ex in operator_list:
            if not isinstance(ex, MetadataOperator):
                raise MsPASSError(
                    "MetadataOperatorChain constructor:  invalid input.  Expected iterable container of MetadataOperator objects",
                    ErrorSeverity.Fatal,
                )

        # to allow flexibility of the structure used for input we should
        # copy the operator_list.  Further, this assure the
        # result will iterate correctly and allow for append
        self.oplist = list()
        for ex in operator_list:
            self.oplist.append(ex)

    @mspass_method_wrapper
    def apply(
        self,
        d,
        *args,
        apply_to_members=False,
        handles_ensembles=True,
        checks_arg0_type=True,
        **kwargs,
    ):
        """
        Implementation of base class method.  In this case failure is
        defined as not passing one of the set of tests loaded  when
        the object was created.  As noted earlier the tests are performed
        in the same order they were passed to the constructor of added on
        with the += operator.
        :param d: is a mspass data object to be checked
        """
        if _input_is_valid(d):
            if d.dead():
                return d
            if _is_ensemble(d) and apply_to_members:
                self.edit_ensemble_members(d)
            else:
                for op in self.oplist:
                    op.apply(d, apply_to_members=apply_to_members)
                    if d.dead():
                        break
            return d
        else:
            raise MsPASSError(
                "MetadataOperatorChain received invalid input data", ErrorSeverity.Fatal
            )

    # These two virtual methods have to be defined but they do nothing in
    # this context.  We depend on the atomic operators to implement these
    # checks
    def check_keys(self, d):
        pass

    def check_operation(self, d):
        pass

    def __iadd__(self, other):
        """
        Defines the += operator for the class. In that case it means a new
        operator is appended.   Raises a MsPASSError set Fatal other is not a
        subclass of MetadataOperator.
        """
        if isinstance(other, MetadataOperator):
            self.oplist.append(other)
        else:
            raise MsPASSError(
                "MetadataOperatorChain:   operator +=  rhs is not a child of MetadataOperator",
                ErrorSeverity.Fatal,
            )
        return self


# other editors to implement that do not match the abstract base class model


def erase_metadata(d, keylist, apply_to_members=False):
    """
    This editor clears the contents of any data associated with a list of
    Metadata keys.   If there is no data for that key it will silently do
    nothing.  If the input is an ensemble and apply_to_members is True
    all the algorithm will run on the metadata of each member in a loop.
    If false only the ensemble (global) metadata are handled.

    :param d:  must be a valid MsPASS data object. If not the function
    will throw an exception.   If the datum is marked dead it will
    silently just return the contents.

    :param keylist:  python list of strings to use as keywords.  Any matching
    keys the metadata of d will be cleared.

    :param apply_to_members:  is a boolean controlling how the function
    should handle ensembles.  Then set true the erase algorithm will be
    applied in a loop over the member vector of an input ensemble.  When
    False (default) only the ensemble's metadata is checked.  This parameter
    is ignored if the input is an atomic data type.

    :return:  edited copy of d
    """
    if _input_is_valid(d):
        if _is_ensemble(d) and apply_to_members:
            for m in d.member:
                for k in keylist:
                    # erase silently does nothing if the key is not defined so
                    # no error handler is needed here
                    m.erase(k)
        else:
            for k in keylist:
                d.erase(k)
        return d
    else:
        raise MsPASSError(
            "erase_metadata:  input is not a valid MsPASS data object",
            ErrorSeverity.Fatal,
        )
