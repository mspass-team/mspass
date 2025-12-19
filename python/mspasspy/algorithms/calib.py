import pickle
import numpy as np
from mspasspy.ccore.seismic import TimeSeries, TimeSeriesEnsemble
from mspasspy.ccore.utility import MsPASSError, ErrorSeverity
from mspasspy.util.decorators import mspass_method_wrapper
from mspasspy.util.db_utils import fetch_dbhandle
from obspy import UTCDateTime


class ApplyCalibEngine:
    """
    A special case of response correction is a simple scalar multiply by a
    constant used to convert data from raw counts to the (now) standard units
    of nm/s.  For many applications using broadband data that correction
    is sufficient to assure ensembles of signals are on a common amplitude
    scale.  A basic rule if the passband of the processing is inside the
    instrument passband a full response correction is not alway necessary.
    A case in point is reeiver function processing where the passband of
    actual data is inside the passband of most "broadband sensors".

    This class can be used to process TimeSeries or TimeSeriesEnsemble
    objects to convert data from raw counts to units of nm/s.

    The effort required to extract the conversion factor from the
    archain response format of seed and station xml is not trivial.
    We rely here on the conversion of downloaded station metadata from
    fdsn sources via web services and obspy's downloading methods.
    Obspy converts that archain data to what they call an Inventory
    object.  In MsPASS we disaggregate the complicated Inventory object
    into a set of MongoDB documents with one entry for each
    seed time period fr each channel of data.   Inside that document is a
    an attibute with the tag "serialized_channel_data" that contains
    the detailed response data serialized with pickle.   The
    constructor for this object runs pickle.loads on that data to yield
    an obspy Response object.  We only extract the "sensitivity" value from
    Response.   A major complication is unit restrictions and invalid
    Response objects.  These are handled by the constructor as described
    below.   The current implementation can only handle Response objects
    with "input units" of meters per second and "output units" of counts.
    Note input and output is because the Response object uses
    sensitivity = 1/calib.  i.e. sensitivity is the scale to convert
    physical units to counts which backward from what this class is
    designed for - converting counts to physical units of nm/s.
    """

    def __init__(
        self,
        dbname_or_handle,
        query=None,
        collection="channel",
        ounits=["counts", "COUNTS", "count", "COUNT"],
        iunits=["m/s", "M/S"],
        response_data_key="serialized_channel_data",
        verbose=False,
    ):
        """
        Constructor for this object loads the serialized response data
        and builds an internal cache to cross-reference channel_id with
        calib values.

        :param dbname_or_handle: Either a string (database name) or a Database instance.
           In parallel mode, pass the database name (string) to avoid serializing Database objects.
           MongoDB Database handle assumed to contained a channel collection created from an Inventory object with
           the MsPASS database method `save_inventory`.
        :param query:  optional MongoDB query operator to apply to
           collection before using.  Size, for exampe, might be reduced
           by using a time range query.
        :type query:  python dictionary.  Default is None which is taken
           to mean use the entire colleciton without a query.
        :param collection:  optional alternative collection name.
           Rarely if ever should be changed as the code here has dependence
           on MsPASS specific keys and the way we disaggregate obspy's
           Inventory object.
        :type collection:  str (default "channel")
        :param ounits:  list of acceptable "output unit" definitions,
           The default is various perturbation of"counts" .
        :type ounits:  list of string defining acceptable units.
        :param iunits:   list of acceptable names for "input units" in
          the Response data structure.   Default is known to work with
          most FDSN data for velocity instruments.   You would  likely
          change this list only if you wanted to wanted to use
          accelerometer data or some other nonstandard unit data.
          Note "input units" in FDSN jargon is kind of confusing in this
          context as the purpose of this object is to convert raw data to
          physical units so this object's output is actually the units
          of iunits.
        :type inunits:  list of string names that must match xml posted
          units.  The default should almost always be sufficient.
        :param response_data_key: key used to access the serialized
          response data from each MongoDB document parsed by this construtor.
        :type response_data_key:  str (default is key sed in mspass and should
          not normally be chaged. )
        :param verbose:  When set True (default is False) will print
          error messages when it finds response data it cannot handle.
          Turned off by default because a database with a few thousand
          channels can generate a lot of error if you mix up acceleration
          data with normal velocity sensor data.  This class will not
          handle acceleration channels as converting acceleration data to
          velocity data is true response correction not a simple calib
          correction.
        """
        db = fetch_dbhandle(dbname_or_handle)
        
        if query:
            cursor = db[collection].find(query)
        else:
            cursor = db[collection].find({})
        self.calib = dict()
        try:
            for doc in cursor:
                if response_data_key in doc:
                    chandata = pickle.loads(doc[response_data_key])
                    resp = chandata.response
                    sens = resp.instrument_sensitivity
                    if sens is None and verbose:
                        stastr = self._parse_stadata(doc)
                        print(stastr, " invalid instrument response")
                        print("pickle.loads returned this:  ", str(resp))
                    elif sens:
                        if verbose:
                            message = self._parse_stadata(doc) + ":  "
                        if sens.input_units and sens.output_units:
                            if sens.input_units in iunits:
                                if sens.output_units in ounits:
                                    if sens.value is None:
                                        if verbose:
                                            message += "units ok but sensitivity value is undefined"
                                            print(message)
                                    elif sens.value <= 0.0:
                                        if verbose:
                                            message += "sensitivity value is 0 or negative - treating as undefined"
                                            print(message)
                                    else:
                                        id = doc["_id"]
                                        # inventory saves a "sensitivity" value
                                        # which is he reciprocal of calib
                                        self.calib[str(id)] = 1e9 / sens.value
                                elif verbose:
                                    message += "Illegal output_unit value="
                                    message += sens.output_units + "\n"
                                    print(message)
                            elif verbose:
                                message += "Illegal input_units value="
                                message += sens.input_units + "\n"
                                print(message)
                        elif verbose:
                            message += (
                                "sensitivity data is undefined in this response object"
                            )
                            print(message)
                elif verbose:
                    stastr = self._parse_stadata(doc)
                    print(
                        stastr,
                        " does not contain pickled response data - keey=",
                        response_data_key,
                    )
        finally:
            cursor.close()
        if len(self.calib) == 0:
            message = (
                "ApplyCalibEngine construtor:  Database has no valid response data"
            )
            raise MsPASSError(message, ErrorSeverity.Invalid)

    @mspass_method_wrapper
    def apply_calib(
        self,
        d,
        *args,
        id_key="channel_id",
        kill_if_undefined=True,
        handles_ensembles=True,
        checks_arg0_type=True,
        handles_dead_data=True,
        **kwargs,
    ):
        """
        Use this method to apply a calib value to a TimeSeries or all the
        live members of a TimeSeriesEnsemble.

        This is the processing function used for applying calibration.
        It is a method instead of a function because the class allows the
        calib values to be cached inside the object.   The input(s) must
        contain a MongoDB ObjectId that can be matched with the ids
        loaded in the cache by the constructor.   By default the key
        used is "channel_id".  That can be changed with the "id_key"
        argument.  If a match is found all
        the sample valued are multiplied by calib AND the calib attribute is
        set.   Note a complaint will be issued if calib was found to already
        be defined in  AND is not 1.0 (a default sometimes appropriate)

        This method can easily become a mass murderer.   By default any
        datum with an undefined calib value in the cache of this object will
        be marked dead on return with a error message posted to its elog
        container.  If the `kill_if_undefined` argument is set False
        such data will not be killed but an elog message will be posted
        marked complaint.

        :param d:  input data to process
        :type d:  `TimeSeries` or `TimeSeriesEnsemle`  This method will
           raise a ValueError exception if d is anything else.
        :param kill_if_undefined:  bolean that when True (default) will
           cause any datum for which a matching calib cannot be found to be
           killed.   Note this operation is atomic so for ensembles only
           members that fail the match will be killed.
        :return:  copy of input with amplitudes multiplied by calib factor.
        """
        if isinstance(d, TimeSeries):
            alg = "ApplyCalibEngine.apply_calib"
            if d.live:
                if id_key in d:
                    idstr = str(d[id_key])
                    if idstr in self.calib:
                        this_calib = self.calib[idstr]
                        if "calib" in d:
                            if not np.isclose(d["calib"], 1.0):
                                old_calib = d["calib"]
                                new_metadata_calib = old_calib * this_calib
                                message = "calib was already defined in this datum as {}\n".format(
                                    d["calib"]
                                )
                                message += "Data will be multiplied by calib defined in this object={}\n".format(
                                    this_calib
                                )
                                message += "New calib in header = {}\n".format(
                                    new_metadata_calib
                                )
                                message += "Amplitudes may be wrong with this datum"
                                d.elog.log_error(alg, message, ErrorSeverity.Complaint)
                                d["calib"] = new_metadata_calib
                            else:
                                d["calib"] = this_calib
                        d.data *= this_calib
                    else:
                        message = "calib factor could not be determined\n"
                        message += "Response data missing or flawed"
                        if kill_if_undefined:
                            d.elog.log_error(alg, message, ErrorSeverity.Invalid)
                            d.kill()
                        else:
                            d.elog.log_error(alg, message, ErrorSeverity.Complaint)
                else:
                    message = "Missing required channel_id need to get calib factor"
                    if kill_if_undefined:
                        d.elog.log_error(alg, message, ErrorSeverity.Invalid)
                        d.kill()
                    else:
                        d.elog.log_error(alg, message, ErrorSeverity.Complaint)
        elif isinstance(d, TimeSeriesEnsemble):
            for i in range(len(d.member)):
                d.member[i] = self.apply_calib(d.member[i])
        else:
            message = "Illegal input data type={}".format(str(type(d)))
            raise ValueError(message)
        return d

    def size(self):
        """
        Return size of the cache of calib values stored in the object.
        """
        return len(self.calib)

    def _parse_stadata(self, doc, null_value="Undefined") -> str:
        """
        Internal method to cautiously parse document doc to
        always a string of the form net:sta:chan:loc.  Any keys
        not defined will be replaced with null_value string.
        """
        outstr = ""
        count = 0
        for k in ["net", "sta", "chan", "loc"]:
            if k in doc:
                outstr += doc[k]
            else:
                outstr += null_value
            if count < 3:
                outstr += ":"
            count += 1
        outstr += " for time range "
        count = 0
        for k in ["starttime", "endtime"]:
            if k in doc:
                outstr += str(UTCDateTime(doc[k]))
            else:
                outstr += null_value
            if count == 0:
                outstr += "->"
        return outstr
