

#include <mspass/mspass.h>
#include <boost/python.hpp>


void _ZN6mspass21SphericalToUnitVectorERKNS_19SphericalCoordinateE()
{
    

    ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    ::boost::python::scope parent_scope(parent_object);

    ::boost::python::def("SphericalToUnitVector", +[](const mspass::SphericalCoordinate & sc) -> double * { return mspass::SphericalToUnitVector(sc); }, (::boost::python::arg("sc")), ::boost::python::return_value_policy<::boost::python::return_by_value>());

    
}


