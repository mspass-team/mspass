

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass11MsPASSErrorE_docstring[] = R"CHIMERA_STRING( 
 This is the generic error object thrown by the MsPASS library.
 it is similar in concept to basic error objects described in various
 books by Stroustrup.  The base object contains only a simple
 generic message and a virtual log_error method common to all
 MsPASS error objects that are it's descendents.
)CHIMERA_STRING";

constexpr char _ZN6mspass11MsPASSError9log_errorEv_docstring[] = R"CHIMERA_STRING( Sends error message thrown by MsPASS library functions to standard error.
)CHIMERA_STRING";

constexpr char _ZN6mspass11MsPASSError9log_errorERSo_docstring[] = R"CHIMERA_STRING( Overloaded method for sending error message to other than stderr. 
)CHIMERA_STRING";

constexpr char _ZN6mspass11MsPASSError8severityEv_docstring[] = R"CHIMERA_STRING( Return error severity as the enum value. 
)CHIMERA_STRING";


} // namespace

void _ZN6mspass11MsPASSErrorE()
{
    

    ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::MsPASSError >("MsPASSError", _ZN6mspass11MsPASSErrorE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[]() -> mspass::MsPASSError * { return new mspass::MsPASSError(); }, ::boost::python::default_call_policies()))
        .def("__init__", ::boost::python::make_constructor(+[](const std::string mess, const char * howbad) -> mspass::MsPASSError * { return new mspass::MsPASSError(mess, howbad); }, ::boost::python::default_call_policies(), (::boost::python::arg("mess"), ::boost::python::arg("howbad"))))
        .def("__init__", ::boost::python::make_constructor(+[](const std::string mess) -> mspass::MsPASSError * { return new mspass::MsPASSError(mess); }, ::boost::python::default_call_policies(), (::boost::python::arg("mess"))))
        .def("__init__", ::boost::python::make_constructor(+[](const std::string mess, mspass::ErrorSeverity s) -> mspass::MsPASSError * { return new mspass::MsPASSError(mess, s); }, ::boost::python::default_call_policies(), (::boost::python::arg("mess"), ::boost::python::arg("s"))))
        .def("__init__", ::boost::python::make_constructor(+[](const char * mess, mspass::ErrorSeverity s) -> mspass::MsPASSError * { return new mspass::MsPASSError(mess, s); }, ::boost::python::default_call_policies(), (::boost::python::arg("mess"), ::boost::python::arg("s"))))
        .def("log_error", +[](mspass::MsPASSError *self) { self->log_error(); }, _ZN6mspass11MsPASSError9log_errorEv_docstring)
        .def("log_error", +[](mspass::MsPASSError *self, std::ostream & ofs) { self->log_error(ofs); }, _ZN6mspass11MsPASSError9log_errorERSo_docstring, (::boost::python::arg("ofs")))
        .def("severity", +[](mspass::MsPASSError *self) -> mspass::ErrorSeverity { return self->severity(); }, _ZN6mspass11MsPASSError8severityEv_docstring)
        .def_readwrite("message", &mspass::MsPASSError::message)
        .def_readwrite("badness", &mspass::MsPASSError::badness)
    ;

    
}


