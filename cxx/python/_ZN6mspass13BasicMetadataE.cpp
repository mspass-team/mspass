

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass13BasicMetadataE_docstring[] = R"CHIMERA_STRING( 
A core idea in MsPASS is the idea of a generic header that allows storage and
retrieval of arbitrary attributes.   This base class forces support for
the standard basic data types.
)CHIMERA_STRING";


} // namespace

void _ZN6mspass13BasicMetadataE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::BasicMetadata, ::boost::noncopyable >("BasicMetadata", _ZN6mspass13BasicMetadataE_docstring, boost::python::no_init)
        .def("get_int", +[](const mspass::BasicMetadata *self, const std::string key) -> int { return self->get_int(key); }, (::boost::python::arg("key")))
        .def("get_double", +[](const mspass::BasicMetadata *self, const std::string key) -> double { return self->get_double(key); }, (::boost::python::arg("key")))
        .def("get_bool", +[](const mspass::BasicMetadata *self, const std::string key) -> bool { return self->get_bool(key); }, (::boost::python::arg("key")))
        .def("get_string", +[](const mspass::BasicMetadata *self, const std::string key) -> std::string { return self->get_string(key); }, (::boost::python::arg("key")))
        .def("put", +[](mspass::BasicMetadata *self, const std::string key, const double val) { self->put(key, val); }, (::boost::python::arg("key"), ::boost::python::arg("val")))
        .def("put", +[](mspass::BasicMetadata *self, const std::string key, const int val) { self->put(key, val); }, (::boost::python::arg("key"), ::boost::python::arg("val")))
        .def("put", +[](mspass::BasicMetadata *self, const std::string key, const bool val) { self->put(key, val); }, (::boost::python::arg("key"), ::boost::python::arg("val")))
        .def("put", +[](mspass::BasicMetadata *self, const std::string key, const std::string val) { self->put(key, val); }, (::boost::python::arg("key"), ::boost::python::arg("val")))
    ;

    
}


