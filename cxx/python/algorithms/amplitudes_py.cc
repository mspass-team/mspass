#include <pybind11/pybind11.h>

#include <mspass/algorithms/amplitudes.h>

namespace mspass {
namespace mspasspy {

namespace py=pybind11;
using namespace std;
using namespace mspass::seismic;
using namespace mspass::algorithms::amplitudes;

PYBIND11_MODULE(amplitudes, m) {
  m.attr("__name__") = "mspasspy.ccore.algorithms.amplitudes";
  m.doc() = "A submodule for amplitudes namespace of ccore.algorithms"; 

  /* Amplitude functions - overloads */
  m.def("PeakAmplitude",py::overload_cast<const CoreTimeSeries&>(&PeakAmplitude),
    "Compute amplitude as largest absolute amplitude",
    py::return_value_policy::copy,py::arg("d") )
  ;
  m.def("PeakAmplitude",py::overload_cast<const CoreSeismogram&>(&PeakAmplitude),
    "Compute amplitude as largest vector amplitude",
    py::return_value_policy::copy,py::arg("d") )
  ;
  m.def("RMSAmplitude",py::overload_cast<const CoreTimeSeries&>(&RMSAmplitude),
    "Compute amplitude from rms of signal",
    py::return_value_policy::copy,py::arg("d") )
  ;
  m.def("RMSAmplitude",py::overload_cast<const CoreSeismogram&>(&RMSAmplitude),
    "Compute amplitude as rms on all 3 components",
    py::return_value_policy::copy,py::arg("d") )
  ;
  m.def("MADAmplitude",py::overload_cast<const CoreTimeSeries&>(&MADAmplitude),
    "Compute amplitude from median absolute deviation (MAD) of signal",
    py::return_value_policy::copy,py::arg("d") )
  ;
  m.def("MADAmplitude",py::overload_cast<const CoreSeismogram&>(&MADAmplitude),
    "Compute amplitude as median of vector amplitudes",
    py::return_value_policy::copy,py::arg("d") )
  ;
  m.def("PerfAmplitude",py::overload_cast<const CoreTimeSeries&,const double>(&PerfAmplitude),
    "Compute amplitude of signal using clip percentage metric",
    py::return_value_policy::copy,py::arg("d"),py::arg("perf") )
  ;
  m.def("PerfAmplitude",py::overload_cast<const CoreSeismogram&,const double>(&PerfAmplitude),
    "Compute amplitude of signal using clip percentage metric",
    py::return_value_policy::copy,py::arg("d"),py::arg("perf") )
  ;
  py::enum_<ScalingMethod>(m,"ScalingMethod")
    .value("Peak",ScalingMethod::Peak)
    .value("RMS",ScalingMethod::RMS)
    .value("ClipPerc",ScalingMethod::ClipPerc)
    .value("MAD",ScalingMethod::MAD)
  ;
  /* We give the python names for these functions a trailing underscore as
  a standard hit they are not to be used directly - should be hidden behing
  python functions that simply the api and (more importantly) add an optional
  history preservation. */
  m.def("_scale",py::overload_cast<Seismogram&,const ScalingMethod,const double>(&scale<Seismogram>),
    "Scale a Seismogram object with a chosen amplitude metric",
    py::return_value_policy::copy,
    py::arg("d"),py::arg("method"),py::arg("level") )
  ;
  m.def("_scale",py::overload_cast<TimeSeries&,const ScalingMethod,const double>(&scale<TimeSeries>),
    "Scale a TimeSeries object with a chosen amplitude metric",
    py::return_value_policy::copy,
    py::arg("d"),py::arg("method"),py::arg("level") )
  ;
  m.def("_scale_ensemble_members",py::overload_cast<Ensemble<Seismogram>&,
          const ScalingMethod&, const double>(&scale_ensemble_members<Seismogram>),
    "Scale each member of a SeismogramEnsemble individually by selected metric",
    py::return_value_policy::copy,
    py::arg("d"),py::arg("method"),py::arg("level") )
  ;
  m.def("_scale_ensemble_members",py::overload_cast<Ensemble<TimeSeries>&,
          const ScalingMethod&, const double>(&scale_ensemble_members<TimeSeries>),
    "Scale each member of a TimeSeriesEnsemble individually by selected metric",
    py::return_value_policy::copy,
    py::arg("d"),py::arg("method"),py::arg("level") )
  ;
  m.def("_scale_ensemble",py::overload_cast<Ensemble<Seismogram>&,
          const ScalingMethod&, const double, const bool>(&scale_ensemble<Seismogram>),
    "Apply a uniform scale to a SeismogramEnsemble using average member estimates by a selected method",
    py::return_value_policy::copy,
    py::arg("d"),py::arg("method"),py::arg("level"),py::arg("use_mean") )
  ;
  m.def("_scale_ensemble",py::overload_cast<Ensemble<TimeSeries>&,
          const ScalingMethod&, const double, const bool>(&scale_ensemble<TimeSeries>),
    "Apply a uniform scale to a TimeSeriesEnsemble using average member estimates by a selected method",
    py::return_value_policy::copy,
    py::arg("d"),py::arg("method"),py::arg("level"),py::arg("use_mean") )
  ;
  
}

} // namespace mspasspy
} // namespace mspass