

#include <mspass/mspass.h>
#include <boost/python.hpp>


void _ZN6mspass22copy_selected_metadataERKNS_8MetadataERS0_RKNSt7__cxx114listINS_16Metadata_typedefESaIS6_EEE()
{
    

    ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    ::boost::python::scope parent_scope(parent_object);

    ::boost::python::def("copy_selected_metadata", +[](const mspass::Metadata & mdin, mspass::Metadata & mdout, const mspass::MetadataList & mdlist) -> int { return mspass::copy_selected_metadata(mdin, mdout, mdlist); }, (::boost::python::arg("mdin"), ::boost::python::arg("mdout"), ::boost::python::arg("mdlist")));

    
}


