

#include <mspass/mspass.h>
#include <boost/python.hpp>


void _ZN6mspass21UnitVectorToSphericalEPKd()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::def("UnitVectorToSpherical", +[](const double * nu) -> mspass::SphericalCoordinate { return mspass::UnitVectorToSpherical(nu); }, (::boost::python::arg("nu")));

    
}


