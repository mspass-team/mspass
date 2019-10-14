

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass12AttributeMapE_docstring[] = R"CHIMERA_STRING( 
 This object is used to link a set of internally defined parameters tagged with
 a name to an external name convention.  The working model for external names is
 attribute names defined in a relational database schema, but the concept
 involved is more general.  That is, the intent of this interface is a general
 way to between one set of parameter names and another.  This could be used,
 for example, to map between header variable names in SEGY or SAC and some
 internal name convention.  The relation of the map defined by this object is
 implicitly assumed to be one-to-one because of the use of the STL map to
 define the relationship.  Because the map is keyed by the internal name
 lookup is also intended only for finding the external names associated with a
 particular internal parameter.  The primary use of this object in the MsPASS
 library is to define a global mapping operator for a particular database
 schema.  That is, the most common construct is to build this object
 early on using a call like:  AttributeMap("css3.0").
)CHIMERA_STRING";

constexpr char _ZNK6mspass12AttributeMap7aliasesENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring[] = R"CHIMERA_STRING( Returns a list of aliases for a key.
	A universal issue in a relational database interface is that an
	attribute can occur in more than one table.  One can give a fully
	qualified name through this interface, but it is often convenient to
	have a simple name (the alias) that is a shorthand for a particular
	instance of that attribute in one table.  Further, it is sometimes
	useful to have a list of possible meanings for an alias that can
	be searched in order.  Thus this method returns a list of AttributeProperties
	that are tied to an alias.  The idea would be that the caller would
	try each member of this list in order before throwing an error.
	
)CHIMERA_STRING";

constexpr char _ZNK6mspass12AttributeMap7aliasesB5cxx11EPKc_docstring[] = R"CHIMERA_STRING( Overload for literals. 
)CHIMERA_STRING";

constexpr char _ZNK6mspass12AttributeMap11aliastablesENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring[] = R"CHIMERA_STRING( Returns an ordered list of table names to try in extracting an alias named.
	Aliases present an issue on input.  Because many attribute names appear in
	multiple tables (an essential thing, in fact, for a relational database to work)
	input of an attribute that is a generic label for such an attribute can be
	problematic.  This method returns an ordered list of tables that provide
	guidance for extracting an attribute defined by such a generic name.  The
	order is very important as readers will generally need to try qualfied names
	for each table in the list returned by this method.  Hence the order matters
	and the list should be inclusive but no longer than necessary as long
	lists could generate some overead problems in some situations.
	
)CHIMERA_STRING";

constexpr char _ZNK6mspass12AttributeMap11aliastablesB5cxx11EPKc_docstring[] = R"CHIMERA_STRING( Overload for literals
)CHIMERA_STRING";

constexpr char _ZNK6mspass12AttributeMap8is_aliasENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring[] = R"CHIMERA_STRING( Check if an attribute name is an alias.
	For efficiency and convience it is useful to have a simple way to
	ask if an attribute name is defined as an alias.  This abstracts this
	process.
	
)CHIMERA_STRING";

constexpr char _ZNK6mspass12AttributeMap8is_aliasEPKc_docstring[] = R"CHIMERA_STRING( Overloaded for string literal. 
)CHIMERA_STRING";


} // namespace

void _ZN6mspass12AttributeMapE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::AttributeMap >("AttributeMap", _ZN6mspass12AttributeMapE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[]() -> mspass::AttributeMap * { return new mspass::AttributeMap(); }, ::boost::python::default_call_policies()))
        .def("__init__", ::boost::python::make_constructor(+[](const std::string tag) -> mspass::AttributeMap * { return new mspass::AttributeMap(tag); }, ::boost::python::default_call_policies(), (::boost::python::arg("tag"))))
        .def("aliases", +[](const mspass::AttributeMap *self, const std::string key) -> std::map<std::string, mspass::AttributeProperties> { return self->aliases(key); }, _ZNK6mspass12AttributeMap7aliasesENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring, (::boost::python::arg("key")))
        .def("aliases", +[](const mspass::AttributeMap *self, const char * key) -> std::map<std::string, mspass::AttributeProperties> { return self->aliases(key); }, _ZNK6mspass12AttributeMap7aliasesB5cxx11EPKc_docstring, (::boost::python::arg("key")))
        .def("aliastables", +[](const mspass::AttributeMap *self, const std::string key) -> std::list<std::string> { return self->aliastables(key); }, _ZNK6mspass12AttributeMap11aliastablesENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring, (::boost::python::arg("key")))
        .def("aliastables", +[](const mspass::AttributeMap *self, const char * key) -> std::list<std::string> { return self->aliastables(key); }, _ZNK6mspass12AttributeMap11aliastablesB5cxx11EPKc_docstring, (::boost::python::arg("key")))
        .def("is_alias", +[](const mspass::AttributeMap *self, const std::string key) -> bool { return self->is_alias(key); }, _ZNK6mspass12AttributeMap8is_aliasENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring, (::boost::python::arg("key")))
        .def("is_alias", +[](const mspass::AttributeMap *self, const char * key) -> bool { return self->is_alias(key); }, _ZNK6mspass12AttributeMap8is_aliasEPKc_docstring, (::boost::python::arg("key")))
        .def_readwrite("attributes", &mspass::AttributeMap::attributes)
    ;

    
}


