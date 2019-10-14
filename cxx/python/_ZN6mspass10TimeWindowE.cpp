

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass10TimeWindowE_docstring[] = R"CHIMERA_STRING(Time windows are a common concept in time series analysis and seismology
in particular.  The object definition here has no concept of a time
standard.  It simply defines an interval in terms of a pair of 
real numbers.  
)CHIMERA_STRING";

constexpr char _ZN6mspass10TimeWindow5shiftEd_docstring[] = R"CHIMERA_STRING(// Returns a new time window translated by tshift argument.
)CHIMERA_STRING";

constexpr char _ZN6mspass10TimeWindow6lengthEv_docstring[] = R"CHIMERA_STRING(// Returns the window length
)CHIMERA_STRING";


} // namespace

void _ZN6mspass10TimeWindowE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::TimeWindow >("TimeWindow", _ZN6mspass10TimeWindowE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[]() -> mspass::TimeWindow * { return new mspass::TimeWindow(); }, ::boost::python::default_call_policies()))
        .def("__init__", ::boost::python::make_constructor(+[](double ts, double te) -> mspass::TimeWindow * { return new mspass::TimeWindow(ts, te); }, ::boost::python::default_call_policies(), (::boost::python::arg("ts"), ::boost::python::arg("te"))))
        .def("shift", +[](mspass::TimeWindow *self, double tshift) -> mspass::TimeWindow { return self->shift(tshift); }, _ZN6mspass10TimeWindow5shiftEd_docstring, (::boost::python::arg("tshift")))
        .def("length", +[](mspass::TimeWindow *self) -> double { return self->length(); }, _ZN6mspass10TimeWindow6lengthEv_docstring)
        .def_readwrite("start", &mspass::TimeWindow::start)
        .def_readwrite("end", &mspass::TimeWindow::end)
    ;

    
}


