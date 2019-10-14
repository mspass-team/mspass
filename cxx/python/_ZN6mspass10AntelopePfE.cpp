

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass10AntelopePfE_docstring[] = R"CHIMERA_STRING( 
   This object encapsulates the Antelope concept of a parameter
file in a single wrapper.   The main constructor is actually
act much like the Antelope pfread procedure.
   Internally this object does not use an antelope Pf at all
directly, but it is a child of Metadata.  Simple attributes
(i.e. key-value pairs) are posted directly to the Metadata 
associative array (map) container.  Note that the
parser attempts to guess the type of each value given in the
obvious ways (periods imply real numbers, e or E imply real
numbers, etc.) but the algorithm used may not be foolproof. The get
methods are from the Metadata object.  Be warned like the Metadata 
object the type of an entry for a key can change and will be the last
one set.   
    An Antelope Tbl in a pf file is converted to an stl list
container of stl::string's that contain the input lines.  This is
a strong divergence from the tbl interface of Antelope, but one
I judged a reasonable modernization.   Similarly, Arr's are
converted to what I am here calling a "branch".   Branches
are map indexed containers with the key pointing to nested
versions of this sampe object type. This is in keeping with the way
Arr's are used in antelope, but with an object flavor instead of
the pointer style of pfget_arr.   Thus, get_branch returns a
AntelopePf object instead of a pointer that has to be memory
managed.
    A final note about this beast is that the entire thing was
created with a tacit assumption the object itself is not huge.
i.e. this implementation may not scale well if applied to very
large (millions) line pf files.  This is a job for something like 
MongoDB.   View this as a convenient format for building a Metadata
object.  Note, a code fragment to use this to create lower level
metadata would go like this:
  AntelopePf pfdata("example");   //parameter file constructor
  Metadata md(dynamic_cast
<Metadata
&
>(pfdata);
This version for MsPASS is derived from a similar thing called a 
PfStyleMetadata object in antelope contrib.   
)CHIMERA_STRING";

constexpr char _ZNK6mspass10AntelopePf7get_tblENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring[] = R"CHIMERA_STRING( 
      Antelope has the idea of a tbl, which is a list of
      lines that are parsed independently.  This is the
      get method to extract one of these by its key.
      
)CHIMERA_STRING";

constexpr char _ZNK6mspass10AntelopePf10get_branchENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring[] = R"CHIMERA_STRING( 
       This method is used for nested Arr constructs.
       This returns a copy of the branch defined by key
       attached to this object.   The original from the parent
       is retained.
       
       
)CHIMERA_STRING";

constexpr char _ZNK6mspass10AntelopePf8arr_keysB5cxx11Ev_docstring[] = R"CHIMERA_STRING( Return a list of keys for branches (Arrs) in the pf file. 
)CHIMERA_STRING";

constexpr char _ZNK6mspass10AntelopePf8tbl_keysB5cxx11Ev_docstring[] = R"CHIMERA_STRING( Return a list of keys for Tbls in the pf.
)CHIMERA_STRING";

constexpr char _ZN6mspass10AntelopePf17ConvertToMetadataEv_docstring[] = R"CHIMERA_STRING( 
     The Metadata parent of this object only handles name:value pairs.
     The values can, however, be any object boost::any can handle.  
     The documentation says that means it is copy constructable.  
     For now this method returns an object containin the boost::any 
     values.   Any Arr and Tbl entries are pushed directly to the 
     output Metadata using boost::any and the two keys defined as 
     const strings at the top of this file (pftbl_key and pfarr_key).   
     
)CHIMERA_STRING";

constexpr char _ZN6mspass10AntelopePf7pfwriteERSo_docstring[] = R"CHIMERA_STRING( 
       This is functionally equivalent to the Antelope pfwrite
       procedure, but is a member of this object.   A feature of
       the current implementation is that all simply type parameters
       will usually be listed twice in the output file.   The reason
       is that the constructor attempts to guess type, but to allow
       for mistakes all simple parameters are also treated as string
       variables so get methods are more robust.
       
)CHIMERA_STRING";


} // namespace

void _ZN6mspass10AntelopePfE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::AntelopePf, ::boost::python::bases<mspass::Metadata > >("AntelopePf", _ZN6mspass10AntelopePfE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[]() -> mspass::AntelopePf * { return new mspass::AntelopePf(); }, ::boost::python::default_call_policies()))
        .def("__init__", ::boost::python::make_constructor(+[](std::string pfbase) -> mspass::AntelopePf * { return new mspass::AntelopePf(pfbase); }, ::boost::python::default_call_policies(), (::boost::python::arg("pfbase"))))
        .def("__init__", ::boost::python::make_constructor(+[](std::list<std::string> lines) -> mspass::AntelopePf * { return new mspass::AntelopePf(lines); }, ::boost::python::default_call_policies(), (::boost::python::arg("lines"))))
        .def("get_tbl", +[](const mspass::AntelopePf *self, const std::string key) -> std::list<std::string> { return self->get_tbl(key); }, _ZNK6mspass10AntelopePf7get_tblENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring, (::boost::python::arg("key")))
        .def("get_branch", +[](const mspass::AntelopePf *self, const std::string key) -> mspass::AntelopePf { return self->get_branch(key); }, _ZNK6mspass10AntelopePf10get_branchENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring, (::boost::python::arg("key")))
        .def("arr_keys", +[](const mspass::AntelopePf *self) -> std::list<std::string> { return self->arr_keys(); }, _ZNK6mspass10AntelopePf8arr_keysB5cxx11Ev_docstring)
        .def("tbl_keys", +[](const mspass::AntelopePf *self) -> std::list<std::string> { return self->tbl_keys(); }, _ZNK6mspass10AntelopePf8tbl_keysB5cxx11Ev_docstring)
        .def("ConvertToMetadata", +[](mspass::AntelopePf *self) -> mspass::Metadata { return self->ConvertToMetadata(); }, _ZN6mspass10AntelopePf17ConvertToMetadataEv_docstring)
//        .def("pfwrite", +[](mspass::AntelopePf *self, std::ostream & ofs) { self->pfwrite(ofs); }, _ZN6mspass10AntelopePf7pfwriteERSo_docstring, (::boost::python::arg("ofs")))
    ;

    
}


