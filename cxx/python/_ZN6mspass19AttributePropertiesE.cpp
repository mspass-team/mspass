

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass19AttributePropertiesE_docstring[] = R"CHIMERA_STRING( 
 This object is used to define the relationship between a parameter stored
 externally (originally conceived as a database attribute general enough for
 any externally imposed naming convention) with a particular name with some
 internal naming convention.  This object defines the relationship for one
 internal parameter and it's external properties.  Arrays of these objects
 or (as used in the AttributeMap object in MsPASS) associate arrays can be
 used to define relationships for a complete set of parameter names.  Note this
could have been called a struct since all the attributes are public.
)CHIMERA_STRING";

constexpr char _ZNK6mspass19AttributeProperties20fully_qualified_nameB5cxx11Ev_docstring[] = R"CHIMERA_STRING( 
	Database attribute names can and often do occur in multiple tables.  This method
	returns a full name that is used to uniquely define that attribute in a particular
	table.  Although this could be generalized, for the present this is always returned
	in the form used by Antelope/datascope:  that is the string is if the form table.attribute.
)CHIMERA_STRING";


} // namespace

void _ZN6mspass19AttributePropertiesE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::AttributeProperties >("AttributeProperties", _ZN6mspass19AttributePropertiesE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[]() -> mspass::AttributeProperties * { return new mspass::AttributeProperties(); }, ::boost::python::default_call_policies()))
        .def("__init__", ::boost::python::make_constructor(+[](const std::string _arg0_) -> mspass::AttributeProperties * { return new mspass::AttributeProperties(_arg0_); }, ::boost::python::default_call_policies(), (::boost::python::arg("_arg0_"))))
        .def("fully_qualified_name", +[](const mspass::AttributeProperties *self) -> std::string { return self->fully_qualified_name(); }, _ZNK6mspass19AttributeProperties20fully_qualified_nameB5cxx11Ev_docstring)
        .def_readwrite("db_attribute_name", &mspass::AttributeProperties::db_attribute_name)
        .def_readwrite("db_table_name", &mspass::AttributeProperties::db_table_name)
        .def_readwrite("internal_name", &mspass::AttributeProperties::internal_name)
        .def_readwrite("mdt", &mspass::AttributeProperties::mdt)
        .def_readwrite("is_key", &mspass::AttributeProperties::is_key)
    ;

    
}


