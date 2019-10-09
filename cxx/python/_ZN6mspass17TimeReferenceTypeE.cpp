

#include <mspass/mspass.h>
#include <boost/python.hpp>


void _ZN6mspass17TimeReferenceTypeE()
{
    

    ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    ::boost::python::scope parent_scope(parent_object);

    ::boost::python::enum_<mspass::TimeReferenceType>("TimeReferenceType")
        .value("UTC", mspass::TimeReferenceType::UTC)
        .value("Relative", mspass::TimeReferenceType::Relative)
    ;

    
}


