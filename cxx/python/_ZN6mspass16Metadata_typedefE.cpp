

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass16Metadata_typedefE_docstring[] = R"CHIMERA_STRING( 
)CHIMERA_STRING";


} // namespace

void _ZN6mspass16Metadata_typedefE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::Metadata_typedef >("Metadata_typedef", _ZN6mspass16Metadata_typedefE_docstring, boost::python::no_init)
        .def_readwrite("tag", &mspass::Metadata_typedef::tag)
        .def_readwrite("mdt", &mspass::Metadata_typedef::mdt)
    ;

    
}


