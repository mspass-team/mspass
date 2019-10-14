

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass23AttributeCrossReferenceE_docstring[] = R"CHIMERA_STRING( 
  Data formats commonly have a frozen namespace with which people
  are very familiar.  An example is SAC where scripts commonly manipulate
  header attribute by a fixed set of names.  For good reasons one may
  want to use a different naming convention internally in a piece of
  software that loads data using an external format but wishes to use a
  different set of names internally.  This object simplifies the task
  of managing the differences in internal and external names 
)CHIMERA_STRING";

constexpr char _ZNK6mspass23AttributeCrossReference8internalENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring[] = R"CHIMERA_STRING( Get internal name for attribute with external name key.
)CHIMERA_STRING";

constexpr char _ZNK6mspass23AttributeCrossReference8externalENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring[] = R"CHIMERA_STRING( Get external name for attribute with internal name key.
)CHIMERA_STRING";

constexpr char _ZNK6mspass23AttributeCrossReference4typeENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring[] = R"CHIMERA_STRING( Get type information for attribute with internal name key.
)CHIMERA_STRING";

constexpr char _ZNK6mspass23AttributeCrossReference4sizeEv_docstring[] = R"CHIMERA_STRING( Return number of entries in the cross reference map. 
)CHIMERA_STRING";

constexpr char _ZN6mspass23AttributeCrossReference3putENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6__docstring[] = R"CHIMERA_STRING( Add a new entry to the map.
          This method is used to extend the namespace.
          
)CHIMERA_STRING";

constexpr char _ZNK6mspass23AttributeCrossReference14internal_namesB5cxx11Ev_docstring[] = R"CHIMERA_STRING( Return the set of internal names defined by this object.
          Returns an std::set container of strings that are the internal
          names defined by this object. 
)CHIMERA_STRING";

constexpr char _ZNK6mspass23AttributeCrossReference14external_namesB5cxx11Ev_docstring[] = R"CHIMERA_STRING( Return the set of external names defined by this object.
          Returns an std::set container of strings that are the external
          names defined by this object. 
)CHIMERA_STRING";


} // namespace

void _ZN6mspass23AttributeCrossReferenceE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::AttributeCrossReference >("AttributeCrossReference", _ZN6mspass23AttributeCrossReferenceE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[]() -> mspass::AttributeCrossReference * { return new mspass::AttributeCrossReference(); }, ::boost::python::default_call_policies()))
        .def("__init__", ::boost::python::make_constructor(+[](const std::string lines_to_parse) -> mspass::AttributeCrossReference * { return new mspass::AttributeCrossReference(lines_to_parse); }, ::boost::python::default_call_policies(), (::boost::python::arg("lines_to_parse"))))
        .def("__init__", ::boost::python::make_constructor(+[](const std::list<std::string> & lines) -> mspass::AttributeCrossReference * { return new mspass::AttributeCrossReference(lines); }, ::boost::python::default_call_policies(), (::boost::python::arg("lines"))))
        .def("__init__", ::boost::python::make_constructor(+[](const std::map<std::string, std::string> internal2external, const mspass::MetadataList & mdlist) -> mspass::AttributeCrossReference * { return new mspass::AttributeCrossReference(internal2external, mdlist); }, ::boost::python::default_call_policies(), (::boost::python::arg("internal2external"), ::boost::python::arg("mdlist"))))
        .def("internal", +[](const mspass::AttributeCrossReference *self, const std::string key) -> std::string { return self->internal(key); }, _ZNK6mspass23AttributeCrossReference8internalENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring, (::boost::python::arg("key")))
        .def("external", +[](const mspass::AttributeCrossReference *self, const std::string key) -> std::string { return self->external(key); }, _ZNK6mspass23AttributeCrossReference8externalENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring, (::boost::python::arg("key")))
        .def("type", +[](const mspass::AttributeCrossReference *self, const std::string key) -> mspass::MDtype { return self->type(key); }, _ZNK6mspass23AttributeCrossReference4typeENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring, (::boost::python::arg("key")))
        .def("size", +[](const mspass::AttributeCrossReference *self) -> int { return self->size(); }, _ZNK6mspass23AttributeCrossReference4sizeEv_docstring)
        .def("put", +[](mspass::AttributeCrossReference *self, const std::string intern, const std::string ext) { self->put(intern, ext); }, _ZN6mspass23AttributeCrossReference3putENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEES6__docstring, (::boost::python::arg("intern"), ::boost::python::arg("ext")))
        .def("internal_names", +[](const mspass::AttributeCrossReference *self) -> std::set<std::string> { return self->internal_names(); }, _ZNK6mspass23AttributeCrossReference14internal_namesB5cxx11Ev_docstring)
        .def("external_names", +[](const mspass::AttributeCrossReference *self) -> std::set<std::string> { return self->external_names(); }, _ZNK6mspass23AttributeCrossReference14external_namesB5cxx11Ev_docstring)
    ;

    
}


