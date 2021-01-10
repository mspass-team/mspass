#include <pybind11/pybind11.h>

#include <mspass/algorithms/algorithms.h>
#include <mspass/algorithms/Butterworth.h>
#include <mspass/utility/Metadata.h>

namespace mspass {
namespace mspasspy {

namespace py=pybind11;
using namespace std;
using namespace mspass::seismic;
using namespace mspass::algorithms;

PYBIND11_MODULE(basic, m) {
  m.attr("__name__") = "mspasspy.ccore.algorithms.basic";
  m.doc() = "A submodule for algorithms namespace of ccore with common algorithms";

  py::class_<mspass::algorithms::Butterworth>
              (m,"Butterworth","Butterworth filter operator processing object")
    .def(py::init<>())
    .def(py::init<const bool, const bool, const bool,
        const double, const double, const double, const double,
	const double, const double, const double, const double,
	const double> ())
    .def(py::init<const mspass::utility::Metadata&>())
    .def(py::init<const bool, const bool, const bool,
        const int, const double, const int, const double, const double>())
    .def(py::init<const Butterworth&>())
    .def("impulse_response",&Butterworth::impulse_response,
         "Return impulse response")
    .def("transfer_function",&Butterworth::transfer_function,
         "Return transfer function in a complex valued array")
    .def("change_dt",&Butterworth::change_dt,
         "Change sample interval defining the operator (does not change corners) ")
    /* Note we intentionally do not overload CoreTimeSeries and CoreSeismogram.
    They do not handle errors as gracefully */
    .def("apply",py::overload_cast<mspass::seismic::TimeSeries&>
         (&Butterworth::apply),"Apply the predefined filter to a TimeSeries object")
    .def("apply",py::overload_cast<std::vector<double>&>(&Butterworth::apply),
    	"Apply the predefined filter to a vector of data")
    .def("apply",py::overload_cast<mspass::seismic::Seismogram&>
         (&Butterworth::apply),
         "Apply the predefined filter to a 3c Seismogram object")
    .def("dt",&Butterworth::current_dt,
      "Current sample interval used for nondimensionalizing frequencies")
    .def("low_corner",&Butterworth::low_corner,"Return low frequency f3d point")
    .def("high_corner",&Butterworth::high_corner,"Return high frequency 3db point")
    .def("npoles_low",&Butterworth::npoles_low,
      "Return number of poles for the low frequency (high-pass aka low-cut) filter definition")
    .def("npoles_high",&Butterworth::npoles_high,
      "Return number of poles for the high frequency (low-pass aka high-cut) filter definition")
    .def("filter_type",&Butterworth::filter_type,
      "Return a description of the filter type")
    .def("is_zerophase",&Butterworth::is_zerophase,
      "Returns True if operator defines a zerophase filter")
  ;
  m.def("ArrivalTimeReference",
      py::overload_cast<Seismogram&,std::string,TimeWindow>
          (&ArrivalTimeReference),
          "Shifts data so t=0 is a specified arrival time",
      py::return_value_policy::copy,
      py::arg("d"),
      py::arg("key"),
      py::arg("window")
  );

  m.def("ArrivalTimeReference",
      py::overload_cast<Ensemble<Seismogram>&,std::string,TimeWindow>
          (&ArrivalTimeReference),
          "Shifts data so t=0 is a specified arrival time",
      py::return_value_policy::copy,
      py::arg("d"),
      py::arg("key"),
      py::arg("window")
  );

  /* overload_cast would not work on this name because of a strange limitation with templated functions
   * used for the Ensemble definition.   */
  m.def("ExtractComponent",static_cast<TimeSeries(*)(const Seismogram&,const unsigned int)>(&ExtractComponent),
  	"Extract component as a TimeSeries object",
      py::return_value_policy::copy,
      py::arg("tcs"),
      py::arg("component")
  );

  m.def("EnsembleComponent",static_cast<Ensemble<TimeSeries>(*)(const Ensemble<Seismogram>&,const unsigned int)>(&ExtractComponent),
  	"Extract one component from a 3C ensemble",
      py::return_value_policy::copy,
      py::arg("d"),
      py::arg("component")
  );

  m.def("agc",&agc,"Automatic gain control a Seismogram",
    py::return_value_policy::copy,
    py::arg("d"),
    py::arg("twin") )
  ;

  m.def("_WindowData",&WindowData,"Reduce data to window inside original",
    py::return_value_policy::copy,
    py::arg("d"),
    py::arg("twin") )
  ;

  m.def("_WindowData3C",&WindowData3C,"Reduce data to window inside original",
    py::return_value_policy::copy,
    py::arg("d"),
    py::arg("twin") )
  ;
  m.def("_bundle_seed_data",&bundle_seed_data,
    "Create SeismogramEnsemble from sorted TimeSeriesEnsemble",
    py::return_value_policy::copy,
    py::arg("d") )
    ;
  m.def("_BundleGroup",&BundleGroup,
    "Bundle a seed grouping of TimeSeries that form one Seismogram",
    py::return_value_policy::copy,
    py::arg("d"),
    py::arg("i0"),
    py::arg("iend") )
    ;
}

} // namespace mspasspy
} // namespace mspass
