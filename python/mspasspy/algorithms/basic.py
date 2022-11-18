from mspasspy.util.decorators import mspass_func_wrapper
from mspasspy.ccore.algorithms.basic import LinearTaper, CosineTaper, VectorTaper
from mspasspy.ccore.utility import MsPASSError, ErrorSeverity
from mspasspy.util import logging_helper
import mspasspy.ccore.algorithms.basic as bsc
from mspasspy.ccore.seismic import (
    TimeSeries,
    TimeSeriesEnsemble,
    Seismogram,
    SeismogramEnsemble,
)


@mspass_func_wrapper
def ExtractComponent(
    data,
    component,
    *args,
    object_history=False,
    alg_name=None,
    alg_id=None,
    dryrun=False,
    inplace_return=False,
    function_return_key=None,
    **kwargs
):
    """
    Extract single component from three-component data.

    The function creates a scalar TimeSeries object from a three component Seismogram object
    Or a TimeSeriesEnsemble object from a SeismogramEnsemble object

    :param data: data object to extract from.
    :type data: either :class:`~mspasspy.ccore.seismic.Seismogram`
     or :class:`~mspasspy.ccore.seismic.SeismogramEnsemble`
    :param component: the index of component that will be extracted, it can only be 0, 1, or 2
    :type component: :class:`int`
    :param object_history: True to preserve the processing history. For details, refer to
     :class:`~mspasspy.util.decorators.mspass_func_wrapper`.
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_func_wrapper.
    :param inplace_return: True to return data in mspass_func_wrapper. This is set false to
     handle exception directly in the function, without passing it to mspass_func_wrapper.
    :param function_return_key:  Some functions one might want to wrap with this decorator
     return something that is appropriate to save as Metadata.  If so, use this argument to
     define the key used to set that field in the data that is returned.
    """
    if isinstance(data, Seismogram):
        try:
            d = bsc.ExtractComponentSeismogram(data, component)
            return d
        except Exception as err:
            data.elog.log_error("ExtractComponent", str(err), ErrorSeverity.Invalid)
            empty = TimeSeries()
            empty.load_history(data)
            empty.kill()
            return empty
    elif isinstance(data, SeismogramEnsemble):
        if data.dead():
            empty = TimeSeriesEnsemble()
            empty.elog = data.elog
            empty.kill()
            return empty
        try:
            d = TimeSeriesEnsemble(bsc.ExtractComponentEnsemble(data, component))
            # second copy to convert type from CoreTimeSeriesEnsemble to TimeSeriesEnsemble
            return d
        except Exception as err:
            logging_helper.ensemble_error(
                data, "ExtractComponent", err, ErrorSeverity.Invalid
            )
            empty = TimeSeriesEnsemble()
            empty.elog = data.elog
            empty.kill()
            return empty


@mspass_func_wrapper
def ator(
    data,
    tshift,
    *args,
    object_history=False,
    alg_name=None,
    alg_id=None,
    dryrun=False,
    inplace_return=True,
    function_return_key=None,
    **kwargs
):
    """
    Absolute to relative time conversion.

    Sometimes we want to convert data from absolute time (epoch times)
    to a relative time standard.  Examples are conversions to travel
    time using an event origin time or shifting to an arrival time
    reference frame. This operation simply switches the tref
    variable and alters t0 by tshift.

    :param data: data object to be converted.
    :type data: either :class:`mspasspy.ccore.seismic.TimeSeries` or :class:`mspasspy.ccore.seismic.Seismogram`
    :param tshift: time shift applied to data before switching data to relative time mode.
    :type tshift: :class:`float`
    :param object_history: True to preserve the processing history. For details, refer to
     :class:`~mspasspy.util.decorators.mspass_func_wrapper`.
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_func_wrapper.
    :param inplace_return: True to return data in mspass_func_wrapper. This is necessary to be used in mapreduce.
    :param function_return_key:  Some functions one might want to wrap with this decorator
     return something that is appropriate to save as Metadata.  If so, use this argument to
     define the key used to set that field in the data that is returned.
    """
    data.ator(tshift)


@mspass_func_wrapper
def rtoa(
    data,
    *args,
    object_history=False,
    alg_name=None,
    alg_id=None,
    dryrun=False,
    inplace_return=True,
    function_return_key=None,
    **kwargs
):
    """
    Relative to absolute time conversion.

    Sometimes we want to convert data from relative time to
    to an UTC time standard.  An example would be converting
    segy shot data to something that could be processed like earthquake
    data in a css3.0 database. This function returns data
    previously converted to relative back to UTC using the
    internally stored time shift attribute.

    :param data: data object to be converted.
    :type data: either :class:`~mspasspy.ccore.seismic.TimeSeries` or :class:`~mspasspy.ccore.seismic.Seismogram`
    :param object_history: True to preserve the processing history. For details, refer to
     :class:`~mspasspy.util.decorators.mspass_func_wrapper`.
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_func_wrapper.
    :param inplace_return: True to return data in mspass_func_wrapper. This is necessary to be used in mapreduce.
    :param function_return_key:  Some functions one might want to wrap with this decorator
     return something that is appropriate to save as Metadata.  If so, use this argument to
     define the key used to set that field in the data that is returned.
    """
    data.rtoa()


@mspass_func_wrapper
def rotate(
    data,
    rotate_param,
    *args,
    object_history=False,
    alg_name=None,
    alg_id=None,
    dryrun=False,
    inplace_return=True,
    function_return_key=None,
    **kwargs
):
    """
    Rotate data using a P wave type coordinate definition.

    This function can apply three different types of rotation depending on the type of parameter given.
    If a :class:`~mspasspy.ccore.utility.SphericalCoordinate` is given, it will rotate the data
    into a coordinate system defined by the direction defined by the spherical coordinate.  The data are
    rotated such that x1 becomes the transverse component, x2 becomes radial, and x3 becomes longitudinal.

    If an unite vector of three components that defines the direction of x3 direction (longitudinal) is give,
    it will turn the vector into a :class:`~mspasspy.ccore.utility.SphericalCoordinate` object and calles the
    related rotate with it.

    If a :class:`float` number is given, it will rotate the horizontal components by this much angle in radians.

    :param data: data object to be rotated.
    :type data: :class:`~mspasspy.ccore.seismic.Seismogram`
    :param rotate_param: the parameter that defines the rotation.
    :type rotate_param: see above for details.
    :param object_history: True to preserve the processing history. For details, refer to
     :class:`~mspasspy.util.decorators.mspass_func_wrapper`.
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_func_wrapper.
    :param inplace_return: True to return data in mspass_func_wrapper. This is necessary to be used in mapreduce.
    :param function_return_key:  Some functions one might want to wrap with this decorator
     return something that is appropriate to save as Metadata.  If so, use this argument to
     define the key used to set that field in the data that is returned.
    """
    data.rotate(rotate_param)


@mspass_func_wrapper
def rotate_to_standard(
    data,
    *args,
    object_history=False,
    alg_name=None,
    alg_id=None,
    dryrun=False,
    inplace_return=True,
    function_return_key=None,
    **kwargs
):
    """
    Apply inverse transformation matrix to return data to cardinal direction components.

    It is frequently necessary to make certain a set of three component data are oriented
    to the standard reference frame (EW, NS, Vertical).  This function does this.
    For efficiency it checks the components_are_cardinal variable and does nothing if
    it is set true.  Otherwise, it applies the inverse transformation and then sets this variable true.
    Note even if the current transformation matrix is not orthogonal it will be put back into
    cardinal coordinates.

    :param data: data object to be rotated.
    :type data: :class:`~mspasspy.ccore.seismic.Seismogram`
    :param object_history: True to preserve the processing history. For details, refer to
     :class:`~mspasspy.util.decorators.mspass_func_wrapper`.
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_func_wrapper.
    :param inplace_return: True to return data in mspass_func_wrapper. This is necessary to be used in mapreduce.
    :param function_return_key:  Some functions one might want to wrap with this decorator
     return something that is appropriate to save as Metadata.  If so, use this argument to
     define the key used to set that field in the data that is returned.
    :exception: :class:`~mspasspy.ccore.utility.MsPASSError` thrown if the an inversion of the
        transformation matrix is required and that matrix is singular.  This can happen if the
        transformation matrix is incorrectly defined or the actual data are coplanar.
    """
    data.rotate_to_standard()


@mspass_func_wrapper
def free_surface_transformation(
    data,
    uvec,
    vp0,
    vs0,
    *args,
    object_history=False,
    alg_name=None,
    alg_id=None,
    dryrun=False,
    inplace_return=True,
    function_return_key=None,
    **kwargs
):
    """
    Computes and applies the Kennett [1991] free surface transformation matrix.

    Kennett [1991] gives the form for a free surface transformation operator
    that reduces to a nonorthogonal transformation matrix when the wavefield is
    not evanescent.  On output x1 will be transverse, x2 will be SV (radial),
    and x3 will be longitudinal.

    :param data: data object to be transformed.
    :type data: :class:`~mspasspy.ccore.seismic.Seismogram`
    :param uvec: slowness vector of the incident wavefield
    :type uvec: :class:`~mspasspy.ccore.seismic.SlownessVector`
    :param vp0: Surface P wave velocity
    :type vp0: :class:`float`
    :param vs0: Surface S wave velocity.
    :type vs0: :class:`float`
    :param object_history: True to preserve the processing history. For details, refer to
     :class:`~mspasspy.util.decorators.mspass_func_wrapper`.
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_func_wrapper.
    :param inplace_return: True to return data in mspass_func_wrapper. This is necessary to be used in mapreduce.
    :param function_return_key:  Some functions one might want to wrap with this decorator
     return something that is appropriate to save as Metadata.  If so, use this argument to
     define the key used to set that field in the data that is returned.
    """
    data.free_surface_transformation(uvec, vp0, vs0)


@mspass_func_wrapper
def transform(
    data,
    matrix,
    *args,
    object_history=False,
    alg_name=None,
    alg_id=None,
    dryrun=False,
    inplace_return=True,
    function_return_key=None,
    **kwargs
):
    """
    Applies an arbitrary transformation matrix to the data.

    i.e. after calling this function the data will have been multiplied by the matrix
    and the transformation matrix will be updated.  The later allows cascaded
    transformations to data.

    :param data: data object to be transformed.
    :type data: :class:`~mspasspy.ccore.seismic.Seismogram`
    :param matrix: a 3x3 matrix that defines the transformation.
    :type matrix: :class:`numpy.array`
    :param object_history: True to preserve the processing history. For details, refer to
     :class:`~mspasspy.util.decorators.mspass_func_wrapper`.
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_func_wrapper.
    :param inplace_return: True to return data in mspass_func_wrapper. This is necessary to be used in mapreduce.
    :param function_return_key:  Some functions one might want to wrap with this decorator
     return something that is appropriate to save as Metadata.  If so, use this argument to
     define the key used to set that field in the data that is returned.
    """
    data.transform(matrix)


@mspass_func_wrapper
def linear_taper(
    data,
    t0head,
    t1head,
    t1tail,
    t0tail,
    *args,
    object_history=False,
    alg_name=None,
    alg_id=None,
    dryrun=False,
    inplace_return=True,
    function_return_key=None,
    **kwargs
):
    """
    Taper front and/or end of a data object with a linear taper.

    Linear tapers are defined here as a time spanning a ramp running from 0 to 1.
    Data will be zeroed on each end of a 0 mark and a linear weight applied between
    0 points and 1 points.  Postive ramp slope on left and negative slope ramp on
    right. Setting t0 == t1 will disable the taper on the specified end (e.g., t0head == t1head).

    :param data: data object to be processed.
    :type data: either :class:`~mspasspy.ccore.seismic.TimeSeries` or :class:`~mspasspy.ccore.seismic.Seismogram`
    :param t0head: t0 of the head taper
    :type t0head: :class:`float`
    :param t1head: t1 of the head taper
    :type t1head: :class:`float`
    :param t1tail: t1 of the tail taper
    :type t1tail: :class:`float`
    :param t0tail: t0 of the tail taper
    :type t0tail: :class:`float`
    :param object_history: True to preserve the processing history. For details, refer to
     :class:`~mspasspy.util.decorators.mspass_func_wrapper`.
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_func_wrapper.
    :param inplace_return: True to return data in mspass_func_wrapper. This is necessary to be used in mapreduce.
    :param function_return_key:  Some functions one might want to wrap with this decorator
     return something that is appropriate to save as Metadata.  If so, use this argument to
     define the key used to set that field in the data that is returned.
    """
    taper = LinearTaper(t0head, t1head, t1tail, t0tail)
    taper.apply(data)


@mspass_func_wrapper
def cosine_taper(
    data,
    t0head,
    t1head,
    t1tail,
    t0tail,
    *args,
    object_history=False,
    alg_name=None,
    alg_id=None,
    dryrun=False,
    inplace_return=True,
    function_return_key=None,
    **kwargs
):
    """
    Taper front and/or end of a data object with a half cosine function.

    A cosine taper is a common, simple approach to taper data.  When applied at the
    front it defnes a half cycle of a cosine curve +1.0 in range -pi to 0.  On
    the right it defines the same function for the range 0 to pi.  The period
    of the left and right operator can be different.  Turn off left or right by
    giving illegal start and end points and the operator will silently be
    only one sided.

    :param data: data object to be processed.
    :type data: either :class:`~mspasspy.ccore.seismic.TimeSeries` or :class:`~mspasspy.ccore.seismic.Seismogram`
    :param t0head: t0 of the head taper
    :type t0head: :class:`float`
    :param t1head: t1 of the head taper
    :type t1head: :class:`float`
    :param t1tail: t1 of the tail taper
    :type t1tail: :class:`float`
    :param t0tail: t0 of the tail taper
    :type t0tail: :class:`float`
    :param object_history: True to preserve the processing history. For details, refer to
     :class:`~mspasspy.util.decorators.mspass_func_wrapper`.
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_func_wrapper.
    :param inplace_return: True to return data in mspass_func_wrapper. This is necessary to be used in mapreduce.
    :param function_return_key:  Some functions one might want to wrap with this decorator
     return something that is appropriate to save as Metadata.  If so, use this argument to
     define the key used to set that field in the data that is returned.
    """
    taper = CosineTaper(t0head, t1head, t1tail, t0tail)
    taper.apply(data)


@mspass_func_wrapper
def vector_taper(
    data,
    taper_array,
    *args,
    object_history=False,
    alg_name=None,
    alg_id=None,
    dryrun=False,
    inplace_return=True,
    function_return_key=None,
    **kwargs
):
    """
    Apply a general taper defined by a vector to the data object.

    This method provides a simple way to build a taper from a set of uniformly
    spaced points. The apply methods will dogmatically only accept input
    data of the same length as the taper defined in the operator.

    :param data: data object to be processed.
    :type data: either :class:`~mspasspy.ccore.seismic.TimeSeries` or :class:`~mspasspy.ccore.seismic.Seismogram`
    :param taper_array: the array that defines the taper
    :type taper_array: :class:`numpy.array`
    :param object_history: True to preserve the processing history. For details, refer to
     :class:`~mspasspy.util.decorators.mspass_func_wrapper`.
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_func_wrapper.
    :param inplace_return: True to return data in mspass_func_wrapper. This is necessary to be used in mapreduce.
    :param function_return_key:  Some functions one might want to wrap with this decorator
     return something that is appropriate to save as Metadata.  If so, use this argument to
     define the key used to set that field in the data that is returned.
    """
    taper = VectorTaper(taper_array)
    taper.apply(data)
