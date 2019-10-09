

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass19SphericalCoordinateE_docstring[] = R"CHIMERA_STRING( Spherical coordinates come up in a lot of contexts in Earth Science data
 processing.  Note actual coodinate system can depend on context.
 For whole Earth models it can define global coordinates, but in three component
 seismograms the normal convention of geographical coordinates is always assumed.
)CHIMERA_STRING";


} // namespace

void _ZN6mspass19SphericalCoordinateE()
{
    

    ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::SphericalCoordinate, ::boost::noncopyable >("SphericalCoordinate", _ZN6mspass19SphericalCoordinateE_docstring, boost::python::no_init)
        .def_readwrite("radius", &mspass::SphericalCoordinate::radius)
        .def_readwrite("theta", &mspass::SphericalCoordinate::theta)
        .def_readwrite("phi", &mspass::SphericalCoordinate::phi)
    ;

    
}


