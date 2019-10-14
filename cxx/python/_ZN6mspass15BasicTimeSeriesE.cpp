

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass15BasicTimeSeriesE_docstring[] = R"CHIMERA_STRING( 
This is a mostly abstract class defining data and methods shared by all
data objects that are time series.  To this library time series means
data sampled on a 1d, uniform grid defined by a sample rate, start time,
and number of samples.  Derived types can be scalar, vector, complex, or
any data that is uniformly sampled.
)CHIMERA_STRING";

constexpr char _ZNK6mspass15BasicTimeSeries4timeEi_docstring[] = R"CHIMERA_STRING(Get the time of sample i.
It is common to need to ask for the time of a given sample.
This standardizes this common operation in an obvious way.
//
)CHIMERA_STRING";

constexpr char _ZNK6mspass15BasicTimeSeries13sample_numberEd_docstring[] = R"CHIMERA_STRING(Inverse of time function.  That is,  it returns the integer position
of a given time t within a time series.  The returned number is
not tested for validity compared to the data range.  This is the
callers responsibility as this is a common error condition that
should not require the overhead of an exception.
)CHIMERA_STRING";

constexpr char _ZNK6mspass15BasicTimeSeries7endtimeEv_docstring[] = R"CHIMERA_STRING(Returns the end time (time associated with last data sample)
of this data object.
)CHIMERA_STRING";

constexpr char _ZN6mspass15BasicTimeSeries4atorEd_docstring[] = R"CHIMERA_STRING(Absolute to relative time conversion.
Sometimes we want to convert data from absolute time (epoch times)
to a relative time standard.  Examples are conversions to travel
time using an event origin time or shifting to an arrival time
reference frame.  This operation simply switches the tref
variable and alters t0 by tshift.
)CHIMERA_STRING";

constexpr char _ZN6mspass15BasicTimeSeries4rtoaEd_docstring[] = R"CHIMERA_STRING(  Relative to absolute time conversion.
 Sometimes we want to convert data from relative time to
 to an absolute time standard.  An example would be converting
 segy shot data to something that could be processed like earthquake
 data in a css3.0 database.
 This operation simply switches the tref
 variable and alters t0 by tshift.
NOTE:  This method is maintained only for backward compatibility.   May be depricated
   in favor of method that uses internally stored private shift variable.
)CHIMERA_STRING";

constexpr char _ZN6mspass15BasicTimeSeries4rtoaEv_docstring[] = R"CHIMERA_STRING( Relative to absolute time conversion.
 Sometimes we want to convert data from relative time to
 to an UTC time standard.  An example would be converting
 segy shot data to something that could be processed like earthquake
 data in a css3.0 database.
 This method returns data previously converted to relative back to UTC using the
 internally stored time shift attribute. 
)CHIMERA_STRING";

constexpr char _ZN6mspass15BasicTimeSeries5shiftEd_docstring[] = R"CHIMERA_STRING( Shift the reference time.
  Sometimes we need to shift the reference time t0.  An example is a moveout correction.
  This method shifts the reference time by dt.   Note a positive dt means data aligned to
  zero will be shifted left because relative time is t-t0.
)CHIMERA_STRING";

constexpr char _ZNK6mspass15BasicTimeSeries14time_referenceEv_docstring[] = R"CHIMERA_STRING( Return the reference time.
  We distinguish relative and UTC time by a time shift constant
  stored with the object.   This returns the time shift to return
  data to an epoch time.
  
)CHIMERA_STRING";


} // namespace

void _ZN6mspass15BasicTimeSeriesE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::BasicTimeSeries >("BasicTimeSeries", _ZN6mspass15BasicTimeSeriesE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[]() -> mspass::BasicTimeSeries * { return new mspass::BasicTimeSeries(); }, ::boost::python::default_call_policies()))
        .def("time", +[](const mspass::BasicTimeSeries *self, const int i) -> double { return self->time(i); }, _ZNK6mspass15BasicTimeSeries4timeEi_docstring, (::boost::python::arg("i")))
        .def("sample_number", +[](const mspass::BasicTimeSeries *self, double t) -> int { return self->sample_number(t); }, _ZNK6mspass15BasicTimeSeries13sample_numberEd_docstring, (::boost::python::arg("t")))
        .def("endtime", +[](const mspass::BasicTimeSeries *self) -> double { return self->endtime(); }, _ZNK6mspass15BasicTimeSeries7endtimeEv_docstring)
        .def("ator", +[](mspass::BasicTimeSeries *self, const double tshift) { self->ator(tshift); }, _ZN6mspass15BasicTimeSeries4atorEd_docstring, (::boost::python::arg("tshift")))
        .def("rtoa", +[](mspass::BasicTimeSeries *self, const double tshift) { self->rtoa(tshift); }, _ZN6mspass15BasicTimeSeries4rtoaEd_docstring, (::boost::python::arg("tshift")))
        .def("rtoa", +[](mspass::BasicTimeSeries *self) { self->rtoa(); }, _ZN6mspass15BasicTimeSeries4rtoaEv_docstring)
        .def("shift", +[](mspass::BasicTimeSeries *self, const double dt) { self->shift(dt); }, _ZN6mspass15BasicTimeSeries5shiftEd_docstring, (::boost::python::arg("dt")))
        .def("time_reference", +[](const mspass::BasicTimeSeries *self) -> double { return self->time_reference(); }, _ZNK6mspass15BasicTimeSeries14time_referenceEv_docstring)
        .def_readwrite("live", &mspass::BasicTimeSeries::live)
        .def_readwrite("dt", &mspass::BasicTimeSeries::dt)
        .def_readwrite("t0", &mspass::BasicTimeSeries::t0)
        .def_readwrite("ns", &mspass::BasicTimeSeries::ns)
        .def_readwrite("tref", &mspass::BasicTimeSeries::tref)
    ;

    
}


