

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass16MetadataGetErrorE_docstring[] = R"CHIMERA_STRING( 
 This is a convenience class used to construct a more informative
 set of errors when get operations fail.  
)CHIMERA_STRING";


} // namespace

void _ZN6mspass16MetadataGetErrorE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::MetadataGetError, ::boost::noncopyable, ::boost::python::bases<mspass::MsPASSError > >("MetadataGetError", _ZN6mspass16MetadataGetErrorE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[](std::string key, std::string Texpected) -> mspass::MetadataGetError * { return new mspass::MetadataGetError(key, Texpected); }, ::boost::python::default_call_policies(), (::boost::python::arg("key"), ::boost::python::arg("Texpected"))))
        .def("__init__", ::boost::python::make_constructor(+[](const char * boostmessage, std::string key, std::string Texpected, std::string Tactual) -> mspass::MetadataGetError * { return new mspass::MetadataGetError(boostmessage, key, Texpected, Tactual); }, ::boost::python::default_call_policies(), (::boost::python::arg("boostmessage"), ::boost::python::arg("key"), ::boost::python::arg("Texpected"), ::boost::python::arg("Tactual"))))
    ;

    
}


