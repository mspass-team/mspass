

#include <mspass/mspass.h>
#include <boost/python.hpp>


void _ZN6mspassL9pfarr_keyB5cxx11E()
{
    

    ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    ::boost::python::scope parent_scope(parent_object);

    ::boost::python::scope().attr("pfarr_key") = mspass::pfarr_key;

    
}


