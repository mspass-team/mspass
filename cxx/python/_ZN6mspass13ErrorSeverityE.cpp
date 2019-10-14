

#include <mspass/mspass.h>
#include <boost/python.hpp>


void _ZN6mspass13ErrorSeverityE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::enum_<mspass::ErrorSeverity>("ErrorSeverity")
        .value("Fatal", mspass::ErrorSeverity::Fatal)
        .value("Invalid", mspass::ErrorSeverity::Invalid)
        .value("Suspect", mspass::ErrorSeverity::Suspect)
        .value("Complaint", mspass::ErrorSeverity::Complaint)
        .value("Debug", mspass::ErrorSeverity::Debug)
        .value("Informational", mspass::ErrorSeverity::Informational)
    ;

    
}


