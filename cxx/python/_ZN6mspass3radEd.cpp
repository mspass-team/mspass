

#include <mspass/mspass.h>
#include <boost/python.hpp>


void _ZN6mspass3radEd()
{
    

    ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    ::boost::python::scope parent_scope(parent_object);

    ::boost::python::def("rad", +[](const double theta_deg) -> double { return mspass::rad(theta_deg); }, (::boost::python::arg("theta_deg")));

    
}


