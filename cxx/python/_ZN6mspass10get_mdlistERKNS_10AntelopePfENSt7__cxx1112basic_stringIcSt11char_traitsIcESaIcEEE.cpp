

#include <mspass/mspass.h>
#include <boost/python.hpp>


void _ZN6mspass10get_mdlistERKNS_10AntelopePfENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::def("get_mdlist", +[](const mspass::AntelopePf & m, const std::string tag) -> mspass::MetadataList { return mspass::get_mdlist(m, tag); }, (::boost::python::arg("m"), ::boost::python::arg("tag")));

    
}


