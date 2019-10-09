

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass15AntelopePfErrorE_docstring[] = R"CHIMERA_STRING( 
  This error object is similar to that for Metadata but tags
  all errors cleanly as originating from this child of Metadata.
  Note MsPASSError is a child of std::exception, so catch that
  to most easily fetch messages coming from this beast.
)CHIMERA_STRING";


} // namespace

void _ZN6mspass15AntelopePfErrorE()
{
    

    ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::AntelopePfError, ::boost::noncopyable, ::boost::python::bases<mspass::MsPASSError > >("AntelopePfError", _ZN6mspass15AntelopePfErrorE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[]() -> mspass::AntelopePfError * { return new mspass::AntelopePfError(); }, ::boost::python::default_call_policies()))
        .def("__init__", ::boost::python::make_constructor(+[](std::string mess) -> mspass::AntelopePfError * { return new mspass::AntelopePfError(mess); }, ::boost::python::default_call_policies(), (::boost::python::arg("mess"))))
        .def("__init__", ::boost::python::make_constructor(+[](const char * mess) -> mspass::AntelopePfError * { return new mspass::AntelopePfError(mess); }, ::boost::python::default_call_policies(), (::boost::python::arg("mess"))))
        .def("log_error", +[](mspass::AntelopePfError *self) { self->log_error(); })
    ;

    
}


