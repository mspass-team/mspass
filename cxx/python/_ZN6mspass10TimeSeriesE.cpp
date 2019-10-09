

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass10TimeSeriesE_docstring[] = R"CHIMERA_STRING( 
This data object extends BasicTimeSeries mainly by adding a vector of
scalar data.  It uses a Metadata object to contain auxiliary parameters
that aren't essential to define the data object, but which are necessary
for some algorithms.
)CHIMERA_STRING";


} // namespace

void _ZN6mspass10TimeSeriesE()
{
    

    ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::TimeSeries, ::boost::python::bases<mspass::Metadata, mspass::BasicTimeSeries > >("TimeSeries", _ZN6mspass10TimeSeriesE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[]() -> mspass::TimeSeries * { return new mspass::TimeSeries(); }, ::boost::python::default_call_policies()))
        .def("__init__", ::boost::python::make_constructor(+[](int nsin) -> mspass::TimeSeries * { return new mspass::TimeSeries(nsin); }, ::boost::python::default_call_policies(), (::boost::python::arg("nsin"))))
        .def_readwrite("s", &mspass::TimeSeries::s)
    ;

    
}


