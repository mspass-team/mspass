

#include <mspass/mspass.h>
#include <boost/python.hpp>


void _ZN6mspass14data_directoryB5cxx11Ev()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::def("data_directory", +[]() -> std::string { return mspass::data_directory(); });

    
}


