#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <mspass/algorithms/amplitudes.h>

namespace mspass {
namespace mspasspy {

namespace py=pybind11;
using namespace std;
using namespace mspass::seismic;
using namespace mspass::algorithms::amplitudes;
using mspass::algorithms::TimeWindow;


struct MADAmplitudeFunctor {
  double operator()(const CoreTimeSeries& d) const {
    return MADAmplitude(d);
  }
  double operator()(const CoreSeismogram& d) const {
    return MADAmplitude(d);
  }
};

struct PeakAmplitudeFunctor {
  double operator()(const CoreTimeSeries& d) const {
    return PeakAmplitude(d);
  }
  double operator()(const CoreSeismogram& d) const {
    return PeakAmplitude(d);
  }
};

struct RMSAmplitudeFunctor {
  double operator()(const CoreTimeSeries& d) const {
    return RMSAmplitude(d);
  }
  double operator()(const CoreSeismogram& d) const {
    return RMSAmplitude(d);
  }
};

struct PercAmplitudeFunctor {
  double operator()(const CoreTimeSeries& d, double perf) const {
    return PercAmplitude(d, perf);
  }
  double operator()(const CoreSeismogram& d, double perf) const {
    return PercAmplitude(d, perf);
  }
};

PYBIND11_MODULE(amplitudes, m) {
  m.attr("__name__") = "mspasspy.ccore.algorithms.amplitudes";
  m.doc() = "A submodule for amplitudes namespace of ccore.algorithms";

  /* Amplitude functions - functors for pickle support (issue #680) */
  py::class_<PeakAmplitudeFunctor>(m, "PeakAmplitudeFunctor",
      "Callable wrapper for peak amplitude; picklable for Dask.")
    .def(py::init<>())
    .def("__call__",
         static_cast<double (PeakAmplitudeFunctor::*)(const CoreTimeSeries&) const>(
             &PeakAmplitudeFunctor::operator()),
         "Compute amplitude as largest absolute amplitude",
         py::return_value_policy::copy, py::arg("d"))
    .def("__call__",
         static_cast<double (PeakAmplitudeFunctor::*)(const CoreSeismogram&) const>(
             &PeakAmplitudeFunctor::operator()),
         "Compute amplitude as largest vector amplitude",
         py::return_value_policy::copy, py::arg("d"))
    .def(py::pickle(
        [](const PeakAmplitudeFunctor&) { return py::make_tuple(); },
        [](py::tuple) { return PeakAmplitudeFunctor(); }));
  m.attr("PeakAmplitude") = PeakAmplitudeFunctor();

  py::class_<RMSAmplitudeFunctor>(m, "RMSAmplitudeFunctor",
      "Callable wrapper for RMS amplitude; picklable for Dask.")
    .def(py::init<>())
    .def("__call__",
         static_cast<double (RMSAmplitudeFunctor::*)(const CoreTimeSeries&) const>(
             &RMSAmplitudeFunctor::operator()),
         "Compute amplitude from rms of signal",
         py::return_value_policy::copy, py::arg("d"))
    .def("__call__",
         static_cast<double (RMSAmplitudeFunctor::*)(const CoreSeismogram&) const>(
             &RMSAmplitudeFunctor::operator()),
         "Compute amplitude as rms on all 3 components",
         py::return_value_policy::copy, py::arg("d"))
    .def(py::pickle(
        [](const RMSAmplitudeFunctor&) { return py::make_tuple(); },
        [](py::tuple) { return RMSAmplitudeFunctor(); }));
  m.attr("RMSAmplitude") = RMSAmplitudeFunctor();
  py::class_<MADAmplitudeFunctor>(m, "MADAmplitudeFunctor",
      "Callable wrapper for MAD amplitude; picklable for Dask.")
    .def(py::init<>())
    .def("__call__",
         static_cast<double (MADAmplitudeFunctor::*)(const CoreTimeSeries&) const>(
             &MADAmplitudeFunctor::operator()),
         "Compute amplitude from median absolute deviation (MAD) of signal",
         py::return_value_policy::copy, py::arg("d"))
    .def("__call__",
         static_cast<double (MADAmplitudeFunctor::*)(const CoreSeismogram&) const>(
             &MADAmplitudeFunctor::operator()),
         "Compute amplitude as median of vector amplitudes",
         py::return_value_policy::copy, py::arg("d"))
    .def(py::pickle(
        [](const MADAmplitudeFunctor&) {
          return py::make_tuple();  
        },
        [](py::tuple) {
          return MADAmplitudeFunctor();
        }));
  m.attr("MADAmplitude") = MADAmplitudeFunctor();

  py::class_<PercAmplitudeFunctor>(m, "PercAmplitudeFunctor",
      "Callable wrapper for percentile amplitude; picklable for Dask.")
    .def(py::init<>())
    .def("__call__",
         static_cast<double (PercAmplitudeFunctor::*)(const CoreTimeSeries&, double) const>(
             &PercAmplitudeFunctor::operator()),
         "Compute amplitude of signal using clip percentage metric",
         py::return_value_policy::copy, py::arg("d"), py::arg("perf"))
    .def("__call__",
         static_cast<double (PercAmplitudeFunctor::*)(const CoreSeismogram&, double) const>(
             &PercAmplitudeFunctor::operator()),
         "Compute amplitude of signal using clip percentage metric",
         py::return_value_policy::copy, py::arg("d"), py::arg("perf"))
    .def(py::pickle(
        [](const PercAmplitudeFunctor&) { return py::make_tuple(); },
        [](py::tuple) { return PercAmplitudeFunctor(); }));
  m.attr("PercAmplitude") = PercAmplitudeFunctor();
  py::enum_<ScalingMethod>(m,"ScalingMethod")
    .value("Peak",ScalingMethod::Peak)
    .value("RMS",ScalingMethod::RMS)
    .value("ClipPerc",ScalingMethod::ClipPerc)
    .value("MAD",ScalingMethod::MAD)
  ;
  /* We give the python names for these functions a leading underscore as
  a standard hint they are not to be used directly - should be hidden behing
  python functions that simply the api and (more importantly) add an optional
  history preservation. */
  m.def("_scale",py::overload_cast<Seismogram&,
      const ScalingMethod,
         const double,
            const TimeWindow>(&scale<Seismogram>),
    "Scale a Seismogram object with a chosen amplitude metric",
    py::return_value_policy::copy,
    py::arg("d"),py::arg("method"),py::arg("level"),py::arg("window") )
  ;
  m.def("_scale",py::overload_cast<TimeSeries&,
    const ScalingMethod,
      const double,
         const TimeWindow>(&scale<TimeSeries>),
    "Scale a TimeSeries object with a chosen amplitude metric",
    py::return_value_policy::copy,
    py::arg("d"),py::arg("method"),py::arg("level"),py::arg("window") )
  ;
  m.def("_scale_ensemble_members",py::overload_cast<Ensemble<Seismogram>&,
          const ScalingMethod&,
            const double,
               const TimeWindow>(&scale_ensemble_members<Seismogram>),
    "Scale each member of a SeismogramEnsemble individually by selected metric",
    py::return_value_policy::copy,
    py::arg("d"),py::arg("method"),py::arg("level"),py::arg("window") )
  ;
  m.def("_scale_ensemble_members",py::overload_cast<Ensemble<TimeSeries>&,
          const ScalingMethod&,
            const double,
               const TimeWindow>(&scale_ensemble_members<TimeSeries>),
    "Scale each member of a TimeSeriesEnsemble individually by selected metric",
    py::return_value_policy::copy,
    py::arg("d"),py::arg("method"),py::arg("level"),py::arg("window") )
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
  py::class_<BandwidthData>(m,"BandwidthData","Defines the frequency domain bandwidth of data")
    .def(py::init<>())
    .def("bandwidth_fraction",&BandwidthData::bandwidth_fraction,
       "Return ratio of estimated bandwidth to total bandwidth of original data")
    .def("bandwidth",&BandwidthData::bandwidth,
        "Return bandwidth in dB (bandwidth_fraction in dB)")
    .def_readwrite("low_edge_f",&BandwidthData::low_edge_f,
         "Low frequency limit of pass band")
    .def_readwrite("high_edge_f",&BandwidthData::high_edge_f,
         "High frequency limit of pass band")
    .def_readwrite("low_edge_snr",&BandwidthData::low_edge_snr,
        "Signal-to-noise ratio at frequency low_edge_f")
    .def_readwrite("high_edge_snr",&BandwidthData::high_edge_snr,
        "Signal-to-noise ratio at frequency high_edge_f")
    .def_readwrite("f_range",&BandwidthData::f_range,
         "Total frequency range of signal spectrum used for snr estimate")
  ;
  m.def("EstimateBandwidth",&EstimateBandwidth,"Estimate signal bandwidth estimate of power spectra of signal and noise",
    py::return_value_policy::copy,
    py::arg("signal_df"),
    py::arg("signal_power_spectrum"),
    py::arg("noise_power_spectrum"),
    py::arg("srn_threshold"),
    py::arg("time_bandwidth_product"),
    py::arg("high_frequency_search_start"),
    py::arg("fix_high_edge_to_fhs")
    )
  ;
  m.def("BandwidthStatistics",&BandwidthStatistics,
      "Compute statistical summary of snr in a passband returned by EstimateBandwidth - Returned in Metadata container",
    py::return_value_policy::copy,
    py::arg("signal_spectrum"),
    py::arg("noise_spectrum"),
    py::arg("bandwidth_data")
    )
  ;

}

} // namespace mspasspy
} // namespace mspass
