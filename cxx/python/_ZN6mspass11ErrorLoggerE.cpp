

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass11ErrorLoggerE_docstring[] = R"CHIMERA_STRING( 
This class is intended mainly to be added to data objects in mspass to
provide a scalable, thread safe method for logging errors.  Atomic mspass data
objects (e.g. seismograms and time series objects) all use this class to
log errors and mark data with ambiguous states.   The log can explain why
data is an invalid state, but can also contain debug information normally
enabled by something like a verbose option to a program.  
)CHIMERA_STRING";

constexpr char _ZN6mspass11ErrorLogger9log_errorERNS_11MsPASSErrorE_docstring[] = R"CHIMERA_STRING( Logs one error message.
  
)CHIMERA_STRING";

constexpr char _ZN6mspass11ErrorLogger11log_verboseENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring[] = R"CHIMERA_STRING( 
  Frequently programs need a verbose option to log something of interest
  that is not an error but potentially useful.   This alternate logging
  method posts the string mess and marks it Informational. Returns
  the size of the log after insertion.
)CHIMERA_STRING";

constexpr char _ZN6mspass11ErrorLogger12worst_errorsB5cxx11Ev_docstring[] = R"CHIMERA_STRING( Return an std::list container with most serious error level marked. 
)CHIMERA_STRING";


} // namespace

void _ZN6mspass11ErrorLoggerE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::ErrorLogger >("ErrorLogger", _ZN6mspass11ErrorLoggerE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[]() -> mspass::ErrorLogger * { return new mspass::ErrorLogger(); }, ::boost::python::default_call_policies()))
        .def("__init__", ::boost::python::make_constructor(+[](int job, std::string alg) -> mspass::ErrorLogger * { return new mspass::ErrorLogger(job, alg); }, ::boost::python::default_call_policies(), (::boost::python::arg("job"), ::boost::python::arg("alg"))))
        .def("set_job_id", +[](mspass::ErrorLogger *self, int jid) { self->set_job_id(jid); }, (::boost::python::arg("jid")))
        .def("set_algorithm", +[](mspass::ErrorLogger *self, std::string alg) { self->set_algorithm(alg); }, (::boost::python::arg("alg")))
        .def("get_job_id", +[](mspass::ErrorLogger *self) -> int { return self->get_job_id(); })
        .def("get_algorithm", +[](mspass::ErrorLogger *self) -> std::string { return self->get_algorithm(); })
        .def("log_error", +[](mspass::ErrorLogger *self, mspass::MsPASSError & merr) -> int { return self->log_error(merr); }, _ZN6mspass11ErrorLogger9log_errorERNS_11MsPASSErrorE_docstring, (::boost::python::arg("merr")))
        .def("log_verbose", +[](mspass::ErrorLogger *self, std::string mess) -> int { return self->log_verbose(mess); }, _ZN6mspass11ErrorLogger11log_verboseENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE_docstring, (::boost::python::arg("mess")))
        .def("get_error_log", +[](mspass::ErrorLogger *self) -> std::list<mspass::LogData> { return self->get_error_log(); })
        .def("size", +[](mspass::ErrorLogger *self) -> int { return self->size(); })
        .def("worst_errors", +[](mspass::ErrorLogger *self) -> std::list<mspass::LogData> { return self->worst_errors(); }, _ZN6mspass11ErrorLogger12worst_errorsB5cxx11Ev_docstring)
    ;

    
}


