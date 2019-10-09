

#include <mspass/mspass.h>
#include <boost/python.hpp>


void _ZN6mspass6MDtypeE()
{
    

    ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    ::boost::python::scope parent_scope(parent_object);

    ::boost::python::enum_<mspass::MDtype>("MDtype")
        .value("Real", mspass::MDtype::Real)
        .value("Real32", mspass::MDtype::Real32)
        .value("Double", mspass::MDtype::Double)
        .value("Real64", mspass::MDtype::Real64)
        .value("Integer", mspass::MDtype::Integer)
        .value("Int32", mspass::MDtype::Int32)
        .value("Long", mspass::MDtype::Long)
        .value("Int64", mspass::MDtype::Int64)
        .value("String", mspass::MDtype::String)
        .value("Boolean", mspass::MDtype::Boolean)
        .value("Invalid", mspass::MDtype::Invalid)
    ;

    
}


