

#include <mspass/mspass.h>
#include <boost/python.hpp>


void _ZN6mspass3degEd()
{
    

    ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    ::boost::python::scope parent_scope(parent_object);

    ::boost::python::def("deg", +[](const double theta_rad) -> double { return mspass::deg(theta_rad); }, (::boost::python::arg("theta_rad")));

    
}


