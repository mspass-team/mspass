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
    SlownessVector,
)
from mspasspy.ccore.utility import SphericalCoordinate
import numpy as np

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
            d = bsc._ExtractComponent(data, component)
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
            d = TimeSeriesEnsemble(bsc._ExtractComponent(data, component))
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
    
    If inversion of the transformation matrix is not possible (e.g. two components are colinear)
    an error thrown by the C++ function is caught, posted to elog, and the 
    datum return will be marked dead.

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
    try:
        data.rotate_to_standard()
    except MsPASSError as merr:
        data.elog.log_error(merr)
        data.kill()
        


@mspass_func_wrapper
def free_surface_transformation(
    data,
    uvec=None,
    vp0=None,
    vs0=None,
    ux_key="ux",
    uy_key="uy",
    vp0_key="vp0",
    vs0_key="vs0",
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
    that defines a transformation matrix that provides optimal separation 
    of P, SV, and SH for a receiver at the free surface of an isotropic medium 
    with constant P and S velocity.   The transformation is only valid if the 
    incident wavefield is not evanescent (i.e. at or past the SV critical angle).
    It is important to realize the tranformation matrix does not define 
    an orthogonal transformation.  It is also, of course, only an approximation 
    since it is strictly correct only for a constant velocity medium. 
    The tranformation operator requires: (1) a slowness vector, (2) an 
    estimate of surface P wave velocity, and (3) an estimate of surface 
    shear wave velocity.   In all cases the input for the slowness 
    vector uses a local cartesian system with the vector specified as 
    component ux and uy.  ux is the component of the slowness of 
    an incident wavefield in the geographic x direction, which means 
    positive at local east.  uy is the complementary component for the 
    y direction defined by local north.  The components of the slowness
    vector are assumed to be in units of s/km.   (Warning:  obspy's and
    other implementations of the commonly used tau-p calculator return 
    slowness (ray parameter) in spherical units of s/radian - r sin(theta)/v)
    
    The required parameters (slowness vector and velocities) can be 
    passed to the function one of two ways.   Because it is much simpler 
    to implement in a map operator the default expect those parameters to 
    be set in the data object's Metadata container.  The default keys for 
    fetching each attribute are defined by the four arguments 
    "ux_key", "uy_key", "vp0_key", and "vs0_key".   All four have 
    standard defaults defined in the function signature and below.  
    The Metadata fetching algorithm can be overridden by defining 
    the same data through the three optional arguments with the key 
    names "uvec", "vp0", and "vs0".   (see below for detailed descriptions)
    
    The switching between Metadata fetching and a constant arguments 
    is not all or none.   If a slowness vector is defined through uvec
    it will always override metadata.  Handing of vp0 and vs0 is independent. 
    i.e. you can define uvec and not define vp0 and vs0 or conversely 
    you can (more commonly) define vp0 and vs0 but not define uvec.  
    In all cases not defining an arg is a signal to fetch it from 
    Metadata.  
    
    When this function is applied to ensembles and the Metadata fetch 
    approach is used (default) the function assumes all ensemble members 
    have the required metadata keys defined.  Any that do not are killed. 
    The same happens for atomic data passed to this function if any of 
    the required keys are missing.  In fact, you should realize the 
    ensemble algorithm simply applies this function in a recursion over all 
    the members. 
    
    The output components are in the order defined in Kennett's original
    paper.  The order is 0=SH, 1=SV, 2=L.   

    :param data: data object to be transformed.  For ensembles the transformation 
    is applied to all members.  Note the Metadata fetch mechanism is the only 
    recommended way to handle ensembles.  An elog message will be posted 
    to the ensemble's elog container if you try to use a constant slowness 
    vector passed via uvec. It will not warn about constant vp0 and vs0 
    as that case is common.
    :type data: :class:`~mspasspy.ccore.seismic.Seismogram` or 
    :class:`~mspasspy.ccore.seismic.SeismogramEnsemble`
    :param ux_key:  key to use to fetch EW component of slowness vector 
    from Metadata container.  Default is "ux".
    :type ux_key:  string
    :param uy_key:  key to use to fetch NS component of slowness vector 
    from Metadata container.  Default is "uy".
    :type uy_key:  string
    :param vp0_key:  key to use to fetch free surface P wave velocity 
    from Metadata container.  Default is "vp0".
    :type vp0_key:  string
    :param vs0_key: key to use to fetch free surface S wave velocity
    from Metadata container.  Default is "vs0".
    :type vs0_key:  string
    :param uvec: slowness vector of the incident wavefield defined via 
    custom C++ class :class:`~mspasspy.ccore.seismic.SlownessVector`.  
    Default is None which is taken as a signal to fetch the slowness vector 
    components from Metadata using ux_key and uy_key.
    :type uvec: :class:`~mspasspy.ccore.seismic.SlownessVector`
    :param vp0: Surface P wave velocity.  Default is None which is taken 
    as a signal to fetch this quantity from Metadata using the vp0_key.
    :type vp0: :class:`float`
    :param vs0: Surface S wave velocity.  Default is None which is taken 
    as a signal to fetch this quantity from Metadata using the vs0_key.
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

    if isinstance(data,Seismogram):
        if uvec:
            slowness_vector = uvec
        else:
            if data.is_defined(ux_key) and data.is_defined(uy_key):
                ux = data[ux_key]
                uy = data[uy_key]
                slowness_vector=SlownessVector(ux,uy)
            else:
                message = "required Metadata with keys ="+ux_key+" and "+uy_key
                message += " are not defined - datum killed"
                data.elog.log_error("free_surface_transformation",message,ErrorSeverity.Invalid)
                data.kill()
        if vp0:
            P_fs = vp0
        else:
            if data.is_defined(vp0_key):
                P_fs = data[vp0_key]
            else:
                message = "required Metadata attribute "+vp0_key+" is not defined - datum killed"
                data.elog.log_error("free_surface_transformation",message,ErrorSeverity.Invalid)
                data.kill()
        if vs0:
            S_fs = vs0
        else:
            if data.is_defined(vs0_key):
                S_fs = data[vs0_key]
            else:
                message = "required Metadata attribute "+vs0_key+" is not defined - datum killed"
                data.elog.log_error("free_surface_transformation",message,ErrorSeverity.Invalid)
                data.kill()
        if data.live:
            data.free_surface_transformation(slowness_vector,P_fs,S_fs)
                
    elif isinstance(data,SeismogramEnsemble):
        if uvec:
            message="Using a constant slowness vector and surface velocities for all ensemble members\n"
            message="That is ill advised unless this is an event gather for a small aperture array"
            data.elog.log_error("free_surface_transformation",message,ErrorSeverity.Complaint)
        for i in range(len(data.member)):
            # this is a recursion so be aware
            data.member[i] = free_surface_transformation(
                                                data.member[i],
                                                ux_key=ux_key,
                                                uy_key=uy_key,
                                                vp0_key=vp0_key,
                                                vs0_key=vs0_key,
                                                uvec=uvec,
                                                vp0=vp0,
                                                vs0=vs0,
                                                object_history=object_history,
                                                alg_name=alg_name,
                                                alg_id=alg_id,
                                                dryrun=dryrun,
                                                inplace_return=inplace_return,
                                                function_return_key=function_return_key,
                                            )
    else:
        message="free_surface_transform received invalid type for arg0 of {}\n".format(type(data))
        message += "Must be either a Seismogram or SeismogramEnsemble"
        raise ValueError(message)
    return data


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
def transform_to_RTZ(data,
                     key='seaz',
                     phi=None,
                     angle_units='degrees',
                     key_is_backazimuth=True,
                     object_history=False,
                     alg_name=None,
                     alg_id=None,
                     dryrun=False,
                     inplace_return=False,
                     function_return_key=None,
                 ):
    """
    Applies coordinate transform to RTZ version of ray coordinates.
    
    RTZ is the simplest transformation for three component data to 
    what is commonly called ray coordinates.  R-radial (SV), T-transverse
    (SH). and Z=vertical (bad measure of longitudinal).  The function 
    first forces the data to cardinal direction using rotate_to_standard 
    and then rotates the coordinates around the vertical axis.   The 
    rotation can be defined in one of two ways.  The default behavior 
    is to attempt to extract the back azimuth from the station to the 
    source with the key defined by the "key" argument (defaults to 'seaz').
    That behavior will be overriden if the "phi" kwarg value is set.  
    Phi is assumed to be the angle to rotate the coordinates using the 
    math convention for the phi angle with positive anticlockwise.  
    The units of the value retrieved with the key argument or the value 
    passed via the phi argument are by default assumed to be in degrees. 
    If using the phi argument you specify the angle in radians if you 
    also set the "angle_units" argument to "radians".
    
    :param data:   data object to be transformed
    :type data:  :class:`~mspasspy.ccore.seismic.Seismogram` or 
     :class:`~mspass.ccore.seismic.SeismogramEnsemble`.
    :param key:  key to use to fetch back azimuth value (assumed degrees always)
    :type key:  string
    :param phi:   angle to rotate around vertical to define the transformation 
    (positive anticlockwise convention NOT azimuth convention)  Default is None 
    which means ignore this parameter and use key.  Setting this value to 
    something other than None causes the key method to be overridden.
    :type phi:  float
    :param angle_units:  should be either 'degrees' (default) or 'radians'.
    An invalid value will be treated as an attempt to switch to radians 
    but will generate an elog warning message.  This argument is ignored unless
    phi is not null (None type)
    :type angle_units: string
    :param key_is_backazimuth: boolean that when True (default) assumes the 
    angle extrated with the key argument value is a backazimuth in degrees. 
    If set False, the angle will be assumed to be a rotation angle in 
    with the anticlockwise positive convention.  
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_func_wrapper.
    :param inplace_return: True to return data in mspass_func_wrapper. This is necessary to be used in mapreduce.
    :param function_return_key:  Some functions one might want to wrap with this decorator
     return something that is appropriate to save as Metadata.  If so, use this argument to
     define the key used to set that field in the data that is returned.
     
    :return:  transformed version of input.  For ensembles the entire ensemble 
    is transformed.
    """
    if phi:
        if angle_units=="degrees":
            phi_rad = np.radians(phi)
        else:
            if angle_units != "radians":
                message = "Illegal value for angle_units argument="+angle_units
                message += " assuming radians"
                data.elog.log_error("transform_to_RTZ",message,ErrorSeverity.Complaint)
            phi_rad = phi
        use_metadata = False
    else:
        use_metadata=True
    if isinstance(data,Seismogram):
        if use_metadata:
            if data.is_defined(key):
                phi_deg = data[key]
                if key_is_backazimuth:
                    az = phi_deg + 180.0  
                    phi_deg = 90.0 - az
                phi_rad = np.radians(phi_deg)
            else:
                message = "required Metadata key="+key+" not defined for this datum - killed"
                data.elog.log_error("transform_to_RTZ",message,ErrorSeverity.Invalid)
                data.kill()
                return data 
        try:
            data.rotate_to_standard()
            data.rotate(phi_rad)
        except MsPASSError as merr:
            data.log_error(merr)
            data.kill()
    elif isinstance(data,SeismogramEnsemble):
        for i in range(len(data.member)):
            # this is a recursion so be aware
            data.member[i] = transform_to_RTZ(
                                    data.member[i],
                                    key=key,
                                    phi=phi,
                                    angle_units=angle_units,
                                    object_history=object_history,
                                    alg_name=alg_name,
                                    alg_id=alg_id,
                                    dryrun=dryrun,
                                    inplace_return=inplace_return,
                                    function_return_key=function_return_key,
                                )
    else:
        message="transform_to_RTZ received invalid type for arg0 of {}\n".format(type(data))
        message += "Must be either a Seismogram or SeismogramEnsemble"
        raise ValueError(message)
    return data

@mspass_func_wrapper
def transform_to_LQT(data,
                     seaz_key='seaz',
                     ema_key='ema',
                     phi=None,
                     theta=None,
                     angle_units='degrees',
                     object_history=False,
                     alg_name=None,
                     alg_id=None,
                     dryrun=False,
                     inplace_return=False,
                     function_return_key=None,
                 ):
    """
    Applies coordinate transform to LQT version of ray coordinates.
    
    LQT is an orthogonal coordinate transformation for Seismogram 
    objects that cause the output data to have x1-Longitudinal (positive up
    normally set to the predicted emergence angle of P particle motion),
    x2 - Q a rotated radial direction (in propagation direction but tilted by theta).
    and x3 - transverse (T) in direction to define a right handed coordinate 
    system.  The function produces this transformation 
    by the product f three transformation:
        1.  Uses rotate_to_standard to assure we start from cardinal directions.
        2.  Transformation to what might be called TQL using the Seismogram 
            C++ method rotate using a SphericalCoordinate definition.
        3.  Transformation to LQT to rearrange the order of the Seismogram 
            data matrix (also requires a sign change on T to keep the output 
            right handed)
        
    The form of the transformation can be specified in one of two 
    completely different ways.   By default the function attempts to 
    extract the back azimuth from station to event with using the 
    metadata key defined by 'seaz_key' (default 'seaz') and the P 
    emergence angle with the metadata key defined by the 'ema_key' 
    argument (default 'ema').   If the arguments 'phi' and 'theta' are 
    defined they are assumed angles to be used to defined the 
    transformation and no attempt will be made to fetch the value from 
    Metaata (Rarely a good idea with ensemble but can be useful for atomic data.
    Using Metadata is strongly preferred because it also preserves what 
    the angles used where.  They are not if the argument approach is used.)
    Metadata values are required to be in degree units.  If the argument 
    approach is used radian values can to used if you also set 
    the argument "angle_units" to "radians".
    
    Note the operation handles the singular case of theta==0.0 where it 
    simply uses the phi value and rotates the coordinates in a variant of 
    RTZ (variant because order is different).      
    :param seaz_key:  key to use to fetch back azimuth value (assumed degrees always)
    :type key:  string (default "seaz")
    :param ema_key:  key to use to fetch emergence angle defining L direction 
    relative to local vertical.  
    :type ema_key:  string (defautl "ema")
    :param phi:   angle to rotate around vertical to define the transformation 
    (positive anticlockwise convention NOT azimuth convention)  Default is None 
    which means ignore this parameter and use key.  Setting this value to 
    something other than None causes the Metadata fetch method to be overridden.
    WARNING:  this is not backazimuth but angle relative to E direction.
    Note that is not at all what is expected when using a Metadata key
    :type phi:  float
    :param theta:   angle relative to local vertical for defining the L 
    coordinate direction.  It is the same as theta in spherical coordinates 
    with emergence angle pointing upward.  Default is None which causes the 
    algorithm to automatically assume the Metadata key method should be used.
    :type theta:  float
    :param angle_units:  should be either 'degrees' (default) or 'radians'.
    An invalid value will be treated as an attempt to switch to radians 
    but will generate an elog warning message.  This argument is ignored unless
    phi is not null (None type)
    :type angle_units: string
    :param alg_name: alg_name is the name the func we are gonna save while preserving the history.
    :type alg_name: :class:`str`
    :param alg_id: alg_id is a unique id to record the usage of func while preserving the history.
    :type alg_id: :class:`bson.objectid.ObjectId`
    :param dryrun: True for dry-run, which return "OK". Used in the mspass_func_wrapper.
    :param inplace_return: True to return data in mspass_func_wrapper. This is necessary to be used in mapreduce.
    :param function_return_key:  Some functions one might want to wrap with this decorator
     return something that is appropriate to save as Metadata.  If so, use this argument to
     define the key used to set that field in the data that is returned.
     
     :return:  transformed version of input.  For ensembles the entire ensemble 
     is transformed.
    """
    if phi and theta:
        if angle_units=="degrees":
            phi_rad = np.radians(phi)
            theta_rad = np.radians(theta)
        else:
            if angle_units != "radians":
                message = "Illegal value for angle_units argument="+angle_units
                message += " assuming radians"
                data.elog.log_error("transform_to_LQT",message,ErrorSeverity.Complaint)
            phi_rad = phi
            theta_rad = theta
        use_metadata = False
    else:
        use_metadata=True
    if isinstance(data,Seismogram):
        if use_metadata:
            if data.is_defined(seaz_key) and data.is_defined(ema_key):
                seaz = data[seaz_key]
                # need azimuth not back azimuth
                az = seaz-180.0
                phi_deg = 90.0 - az
                # trig funtions work the same if angle wraps so no neeed to test
                # for fixed range
                phi_rad = np.radians(phi_deg)
                ema_deg = data[ema_key]
                theta_rad = np.radians(ema_deg)
            else:
                message = "At least one of required metadata keys="
                message += seaz_key + " and " + ema_key
                message += " are not defined in this datume - killed"
                data.elog.log_error("transform_to_LQT",message,ErrorSeverity.Invalid)
                data.kill()
                return data
        try:
            data.rotate_to_standard()
            sc = SphericalCoordinate()
            sc.phi = phi_rad
            sc.theta = theta_rad
            sc.radius=1.0  # not strictly necessary but prudent
            # rotate method is overloaded allowing this type of input
            data.rotate(sc)
            # The rotate method returns the data in TQL order.  
            # Standard convention is LQT so we reorder the data
            # This could be done more efficiently with vector operators 
            # but we need to use the transform method to assure the 
            # internal tranformation matrix is properly updated
            a=np.zeros([3,3])
            a[0,2]=1.0
            a[1,1]=1.0
            a[2,0]=-1.0
            data.transform(a)
            #x = np.array(data.data[0,:])
            #data.data[0,:] = data.data[2,:]  
            # change the sign of T to keep coordinates right handed == LQT
            #data.data[2,:] = -x  
        except MsPASSError as merr:
            data.log_error(merr)
            data.kill()
    elif isinstance(data,SeismogramEnsemble):
        for i in range(len(data.member)):
            # this is a recursion so be aware
            data.member[i] = transform_to_LQT(
                                    data.member[i],
                                    seaz_key=seaz_key,
                                    ema_key=ema_key,
                                    phi=phi,
                                    theta=theta,
                                    angle_units=angle_units,
                                    object_history=object_history,
                                    alg_name=alg_name,
                                    alg_id=alg_id,
                                    dryrun=dryrun,
                                    inplace_return=inplace_return,
                                    function_return_key=function_return_key,
                                )
    else:
        message="transform_to_LQT received invalid type for arg0 of {}\n".format(type(data))
        message += "Must be either a Seismogram or SeismogramEnsemble"
        raise ValueError(message)
    return data


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
