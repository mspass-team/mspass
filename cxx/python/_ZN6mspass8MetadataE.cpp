

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass8MetadataE_docstring[] = R"CHIMERA_STRING( 
A core idea in MsPASS is the idea of a generic header that allows storage and
retrieval of arbitrary attributes.   This base class forces support for
the standard basic data types.
)CHIMERA_STRING";

constexpr char _ZNK6mspass8Metadata10get_doubleENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring[] = R"CHIMERA_STRING(  Get a real number from the Metadata object.
  
)CHIMERA_STRING";

constexpr char _ZNK6mspass8Metadata7get_intENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring[] = R"CHIMERA_STRING(  Get an integer from the Metadata object.
  
)CHIMERA_STRING";

constexpr char _ZNK6mspass8Metadata8get_longENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring[] = R"CHIMERA_STRING(  Get a long integer from the Metadata object.
  
)CHIMERA_STRING";

constexpr char _ZNK6mspass8Metadata10get_stringENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring[] = R"CHIMERA_STRING(  Get a string from the Metadata object.
  Note the string in this case can be quite large.  If the string
  was parsed from an Antelope Pf nested Tbl and Arrs can be extracted
  this way and parsed with pf routines.
  
)CHIMERA_STRING";

constexpr char _ZNK6mspass8Metadata8get_boolENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring[] = R"CHIMERA_STRING(  Get a  boolean parameter from the Metadata object.
  This method never throws an exception assuming that if the
  requested parameter is not found it is false.
  
)CHIMERA_STRING";


} // namespace

void _ZN6mspass8MetadataE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::Metadata, ::boost::python::bases<mspass::BasicMetadata > >("Metadata", _ZN6mspass8MetadataE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[]() -> mspass::Metadata * { return new mspass::Metadata(); }, ::boost::python::default_call_policies()))
        .def("__init__", ::boost::python::make_constructor(+[](std::ifstream & ifs) -> mspass::Metadata * { return new mspass::Metadata(ifs); }, ::boost::python::default_call_policies(), (::boost::python::arg("ifs"))))
        .def("__init__", ::boost::python::make_constructor(+[](std::ifstream & ifs, const std::string form) -> mspass::Metadata * { return new mspass::Metadata(ifs, form); }, ::boost::python::default_call_policies(), (::boost::python::arg("ifs"), ::boost::python::arg("form"))))
        .def("get_double", +[](const mspass::Metadata *self, const std::string key) -> double { return self->get_double(key); }, _ZNK6mspass8Metadata10get_doubleENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring, (::boost::python::arg("key")))
        .def("get_int", +[](const mspass::Metadata *self, const std::string key) -> int { return self->get_int(key); }, _ZNK6mspass8Metadata7get_intENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring, (::boost::python::arg("key")))
        .def("get_long", +[](const mspass::Metadata *self, const std::string key) -> long { return self->get_long(key); }, _ZNK6mspass8Metadata8get_longENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring, (::boost::python::arg("key")))
        .def("get_string", +[](const mspass::Metadata *self, const std::string key) -> std::string { return self->get_string(key); }, _ZNK6mspass8Metadata10get_stringENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring, (::boost::python::arg("key")))
        .def("get_bool", +[](const mspass::Metadata *self, const std::string key) -> bool { return self->get_bool(key); }, _ZNK6mspass8Metadata8get_boolENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring, (::boost::python::arg("key")))
        .def("put", +[](mspass::Metadata *self, const std::string key, const double val) { self->put(key, val); }, (::boost::python::arg("key"), ::boost::python::arg("val")))
        .def("put", +[](mspass::Metadata *self, const std::string key, const int val) { self->put(key, val); }, (::boost::python::arg("key"), ::boost::python::arg("val")))
        .def("put", +[](mspass::Metadata *self, const std::string key, const bool val) { self->put(key, val); }, (::boost::python::arg("key"), ::boost::python::arg("val")))
        .def("put", +[](mspass::Metadata *self, const std::string key, const std::string val) { self->put(key, val); }, (::boost::python::arg("key"), ::boost::python::arg("val")))
    ;

    
}


