

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass10SeismogramE_docstring[] = R"CHIMERA_STRING( 
 A three-component seismogram is a common concept in seismology. The concept
 used here is that a three-component seismogram is a time series with a 3-vector
 as the data at each time step.  As a result the data are stored internally as
 a matrix with row defining the component number (C indexing 0,1,2) and
 the column defining the time variable.
 The object inherits common concepts of a time series through the
 BasicTimeSeries object.  Auxiliary parameters are defined for the object
 through inheritance of a Metadata object.
)CHIMERA_STRING";

constexpr char _ZN6mspass10Seismogram18rotate_to_standardEv_docstring[] = R"CHIMERA_STRING( Apply inverse transformation matrix to return data to cardinal direction components.
 It is frequently necessary to make certain a set of three component data are oriented
 to the standard reference frame (EW, NS, Vertical).  This function does this.
 For efficiency it checks the components_are_cardinal variable and does nothing if
 it is set true.  Otherwise, it applies the inverse transformation and then sets this variable true.
 Note even if the current transformation matrix is not orthogonal it will be put back into
 cardinal coordinates.
 
)CHIMERA_STRING";

constexpr char _ZN6mspass10Seismogram6rotateERNS_19SphericalCoordinateE_docstring[] = R"CHIMERA_STRING( Rotate data using a P wave type coordinate definition.
 In seismology the longitudinal motion direction of a P wave defines a direction
 in space.  This method rotates the data into a coordinate system defined by a
 direction passed through the argument.  The data are rotated such that x1 becomes
 the transverse component, x2 becomes radial, and x3 becomes longitudinal.  In the
 special case for a vector pointing in the x3 direction the data are not altered.
 The transformation matrix is effectively the matrix product of two coordinate rotations:
 (1) rotation around x3 by angle phi and (2) rotation around x1 by theta.
The sense of this transformation is confusing because of a difference in
convention between spherical coordinates and standard earth coordinates.
In particular, orientation on the earth uses a convention with x2 being
the x2 axis and bearings are relative to that with a standard azimuth
measured clockwise from north.  Spherical coordinate angle phi (used here)
is measured counterclockwise relative to the x1 axis, which is east in
standard earth coordinates. This transformation is computed using a phi
angle.   To use this then to compute a transformation to standard ray
coordinates with x2 pointing in the direction of wavefront advance,
phi should be set to pi/2-azimuth which gives the phi angle needed to rotate
x2 to radial.  This is extremely confusing because in spherical coordinates
it would be more intuitive to rotate x1 to radial, but this is NOT the
convention used here.  In general to use this feature the best way to avoid
this confusion is to use the PMHalfSpaceModel procedure to compute a
SphericalCoordinate object consistent with given propagation direction
defined by a slowness vector.  Alternatively, use the free_surface_transformation
method defined below.
)CHIMERA_STRING";

constexpr char _ZN6mspass10Seismogram6rotateEPKd_docstring[] = R"CHIMERA_STRING( Rotate data using a P wave type coordinate definition.
 In seismology the longitudinal motion direction of a P wave defines a direction
 in space.  This method rotates the data into a coordinate system defined by a
 direction passed through the argument.  The data are rotated such that x1 becomes
 the transverse component, x2 becomes radial, and x3 becomes longitudinal.  In the
 special case for a vector pointing in the x3 direction the data are not altered.
 This method effectively turns nu into a SphericalCoordinate object and calles the
 related rotate method that has a SphericalCoordinate object as an argument.  The
 potential confusion of orientation is not as extreme here.  After the transformation
 x3prime will point in the direction of nu, x2 will be in the x3-x3prime plane (rotation by
 theta) and orthogonal to x3prime, and x1 will be horizontal and perpendicular to x2prime
 and x3prime.
)CHIMERA_STRING";

constexpr char _ZN6mspass10Seismogram6rotateEd_docstring[] = R"CHIMERA_STRING( 
          A common transformation in 3C processing is a rotation of the
          horizontal components by an angle.  This leaves the vertical
          (assumed here x3) unaltered.   This routine rotates the horizontals
          by angle phi using with positive phi counterclockwise as in
          polar coordinates and the azimuth angle of spherical coordinates.
          
)CHIMERA_STRING";

constexpr char _ZN6mspass10Seismogram9transformEPA3_Kd_docstring[] = R"CHIMERA_STRING( Applies an arbitrary transformation matrix to the data.
 i.e. after calling this method the data will have been multiplied by the matrix a
 and the transformation matrix will be updated.  The later allows cascaded
 transformations to data.
)CHIMERA_STRING";

constexpr char _ZN6mspass10Seismogram27free_surface_transformationENS_14SlownessVectorEdd_docstring[] = R"CHIMERA_STRING( Computes and applies the Kennett [1991] free surface transformation matrix.
 Kennett [1991] gives the form for a free surface transformation operator
 that reduces to a nonorthogonal transformation matrix when the wavefield is
 not evanescent.  On output x1 will be transverse, x2 will be SV (radial),
 and x3 will be longitudinal.
)CHIMERA_STRING";


} // namespace

void _ZN6mspass10SeismogramE()
{
    

    ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::Seismogram, ::boost::python::bases<mspass::Metadata, mspass::BasicTimeSeries > >("Seismogram", _ZN6mspass10SeismogramE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[]() -> mspass::Seismogram * { return new mspass::Seismogram(); }, ::boost::python::default_call_policies()))
        .def("__init__", ::boost::python::make_constructor(+[](const int nsamples) -> mspass::Seismogram * { return new mspass::Seismogram(nsamples); }, ::boost::python::default_call_policies(), (::boost::python::arg("nsamples"))))
        .def("__init__", ::boost::python::make_constructor(+[](const std::vector<mspass::TimeSeries> & ts) -> mspass::Seismogram * { return new mspass::Seismogram(ts); }, ::boost::python::default_call_policies(), (::boost::python::arg("ts"))))
        .def("__init__", ::boost::python::make_constructor(+[](const std::vector<mspass::TimeSeries> & ts, const int component_to_clone) -> mspass::Seismogram * { return new mspass::Seismogram(ts, component_to_clone); }, ::boost::python::default_call_policies(), (::boost::python::arg("ts"), ::boost::python::arg("component_to_clone"))))
        .def("rotate_to_standard", +[](mspass::Seismogram *self) { self->rotate_to_standard(); }, _ZN6mspass10Seismogram18rotate_to_standardEv_docstring)
        .def("rotate", +[](mspass::Seismogram *self, mspass::SphericalCoordinate & sc) { self->rotate(sc); }, _ZN6mspass10Seismogram6rotateERNS_19SphericalCoordinateE_docstring, (::boost::python::arg("sc")))
        .def("rotate", +[](mspass::Seismogram *self, const double * nu) { self->rotate(nu); }, _ZN6mspass10Seismogram6rotateEPKd_docstring, (::boost::python::arg("nu")))
        .def("rotate", +[](mspass::Seismogram *self, const double phi) { self->rotate(phi); }, _ZN6mspass10Seismogram6rotateEd_docstring, (::boost::python::arg("phi")))
        .def("transform", +[](mspass::Seismogram *self, const double a[3][3]) { self->transform(a); }, _ZN6mspass10Seismogram9transformEPA3_Kd_docstring, (::boost::python::arg("a")))
        .def("free_surface_transformation", +[](mspass::Seismogram *self, const mspass::SlownessVector u, const double vp0, const double vs0) { self->free_surface_transformation(u, vp0, vs0); }, _ZN6mspass10Seismogram27free_surface_transformationENS_14SlownessVectorEdd_docstring, (::boost::python::arg("u"), ::boost::python::arg("vp0"), ::boost::python::arg("vs0")))
        .def_readwrite("u", &mspass::Seismogram::u)
    ;

    
}


