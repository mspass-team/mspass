#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <pybind11/operators.h>


#include <mspass/algorithms/deconvolution/WaterLevelDecon.h>
#include <mspass/algorithms/deconvolution/LeastSquareDecon.h>
#include <mspass/algorithms/deconvolution/MultiTaperXcorDecon.h>
#include <mspass/algorithms/deconvolution/MultiTaperSpecDivDecon.h>
#include <mspass/algorithms/deconvolution/GeneralIterDecon.h>
#include <mspass/algorithms/deconvolution/CNR3CDecon.h>
PYBIND11_MAKE_OPAQUE(std::vector<double>);


namespace mspass {
namespace mspasspy {

namespace py=pybind11;
using namespace std;
using namespace mspass::utility;
using namespace mspass::seismic;
using namespace mspass::algorithms::deconvolution;


/* Trampoline class for BasicDeconOperator */
class PyBasicDeconOperator : public BasicDeconOperator
{
public:
  void change_parameter(const Metadata &md)
  {
    PYBIND11_OVERLOAD_PURE(
      void,
      BasicDeconOperator,
      change_parameter,
    );
  }
};
/* This is is needed here because ScalarDecon has multiple pure virtual methods
   overridden by all scalar trace decon operators */
class PyScalarDecon : public ScalarDecon
{
public:
  void process()
  {
    PYBIND11_OVERLOAD_PURE(
        void,
        ScalarDecon,
        process
    );
  }
  CoreTimeSeries actual_output()
  {
    PYBIND11_OVERLOAD_PURE(
        CoreTimeSeries,
        ScalarDecon,
        actual_output
    );
  }
  CoreTimeSeries inverse_wavelet()
  {
    PYBIND11_OVERLOAD_PURE(
        CoreTimeSeries,
        ScalarDecon,
        inverse_wavelet
    );
  }
  CoreTimeSeries inverse_wavelet(double)
  {
    PYBIND11_OVERLOAD_PURE(
        CoreTimeSeries,
        ScalarDecon,
        inverse_wavelet
    );
  }
  Metadata QCMetrics()
  {
    PYBIND11_OVERLOAD_PURE(
        Metadata,
        ScalarDecon,
        QCMetrics
    );
  }
};

PYBIND11_MODULE(deconvolution, m) {
  m.attr("__name__") = "mspasspy.ccore.algorithms.deconvolution";
  m.doc() = "A submodule for deconvolution namespace of ccore.algorithms";

  /* Need this to support returns of std::vector in children of ScalarDecon*/
  //py::bind_vector<std::vector<double>>(m, "DoubleVector");

  py::class_<std::vector<double>>(m, "DoubleVector")
    .def(py::init<>())
    .def("clear", &std::vector<double>::clear)
    .def("pop_back", &std::vector<double>::pop_back)
    .def("__len__", [](const std::vector<double> &v) { return v.size(); })
    .def("__iter__", [](std::vector<double> &v) {
       return py::make_iterator(v.begin(), v.end());
    }, py::keep_alive<0, 1>())
  ;
  /* this is a set of deconvolution related classes*/
  py::class_<ScalarDecon,PyScalarDecon>(m,"ScalarDecon","Base class for scalar TimeSeries data")
    .def("load",&ScalarDecon::load,
    py::arg("w"),py::arg("d"),"Load data and wavelet to use to construct deconvolutions operator")
    .def("loaddata",&ScalarDecon::loaddata,py::arg("d"))
    .def("loadwavelet",&ScalarDecon::loadwavelet,py::arg("w"))
    .def("process",&ScalarDecon::process)
    .def("getresult",&ScalarDecon::getresult,
            "Fetch vector of deconvolved data - after calling process")
    .def("change_parameter",&ScalarDecon::changeparameter,"Change deconvolution parameters")
    .def("change_shaping_wavelet",&ScalarDecon::change_shaping_wavelet,
            "Change the shaping wavelet applied to output")
    .def("actual_output",&ScalarDecon::actual_output,"Return actual output of inverse*wavelet")
    .def("ideal_output",&ScalarDecon::ideal_output,"Return ideal output of for inverse")
    .def("inverse_wavelet",py::overload_cast<>(&ScalarDecon::inverse_wavelet))
    .def("inverse_wavelet",py::overload_cast<double>(&ScalarDecon::inverse_wavelet))
    .def("QCMetrics",&ScalarDecon::QCMetrics,"Return ideal output of for inverse")
  ;
  py::class_<WaterLevelDecon,ScalarDecon>(m,"WaterLevelDecon","Water level frequency domain operator")
    .def(py::init<const Metadata>())
    .def("changeparameter",&WaterLevelDecon::changeparameter,"Change operator parameters")
    .def("process",&WaterLevelDecon::process,"Process previously loaded data")
    .def("actual_output",&WaterLevelDecon::actual_output,"Return actual output of inverse*wavelet")
    .def("inverse_wavelet",py::overload_cast<>(&WaterLevelDecon::inverse_wavelet))
    .def("inverse_wavelet",py::overload_cast<double>(&WaterLevelDecon::inverse_wavelet))
    .def("QCMetrics",&WaterLevelDecon::QCMetrics,"Return ideal output of for inverse")
  ;
  py::class_<LeastSquareDecon,ScalarDecon>(m,"LeastSquareDecon","Water level frequency domain operator")
    .def(py::init<const Metadata>())
    .def("changeparameter",&LeastSquareDecon::changeparameter,"Change operator parameters")
    .def("process",&LeastSquareDecon::process,"Process previously loaded data")
    .def("actual_output",&LeastSquareDecon::actual_output,"Return actual output of inverse*wavelet")
    .def("inverse_wavelet",py::overload_cast<>(&LeastSquareDecon::inverse_wavelet))
    .def("inverse_wavelet",py::overload_cast<double>(&LeastSquareDecon::inverse_wavelet))
    .def("QCMetrics",&LeastSquareDecon::QCMetrics,"Return ideal output of for inverse")
  ;
  py::class_<MultiTaperSpecDivDecon,ScalarDecon>(m,"MultiTaperSpecDivDecon","Water level frequency domain operator")
    .def(py::init<const Metadata>())
    .def("changeparameter",&MultiTaperSpecDivDecon::changeparameter,"Change operator parameters")
    .def("process",&MultiTaperSpecDivDecon::process,"Process previously loaded data")
    .def("loadnoise",&MultiTaperSpecDivDecon::loadnoise,"Load noise data for regularization")
    .def("load",&MultiTaperSpecDivDecon::load,"Load all data, wavelet, and noise")
    .def("actual_output",&MultiTaperSpecDivDecon::actual_output,"Return actual output of inverse*wavelet")
    .def("inverse_wavelet",py::overload_cast<>(&MultiTaperSpecDivDecon::inverse_wavelet))
    .def("inverse_wavelet",py::overload_cast<double>(&MultiTaperSpecDivDecon::inverse_wavelet))
    .def("QCMetrics",&MultiTaperSpecDivDecon::QCMetrics,"Return ideal output of for inverse")
    .def("get_taperlen",&MultiTaperSpecDivDecon::get_taperlen,"Get length of the Slepian tapers used by the operator")
    .def("get_number_tapers",&MultiTaperSpecDivDecon::get_number_tapers,"Get number of Slepian tapers used by the operator")
    .def("get_time_bandwidth_product",&MultiTaperSpecDivDecon::get_time_bandwidth_product,"Get time bandwidt product of Slepian tapers used by the operator")
  ;
  py::class_<FFTDeconOperator>(m,"FFTDeconOperator","Base class used by frequency domain deconvolution methods")
    .def(py::init<>())
    .def("change_size",&FFTDeconOperator::change_size,"Change fft buffer size")
    .def("get_size",&FFTDeconOperator::get_size,"Get current fft buffer size")
    .def("change_shift",&FFTDeconOperator::change_shift,"Change reference time shift")
    .def("get_shift",&FFTDeconOperator::get_shift,"Get current reference time shift")
    .def("df",&FFTDeconOperator::df,"Get frequency bin size")
  ;
  py::class_<MultiTaperXcorDecon,ScalarDecon>(m,"MultiTaperXcorDecon","Water level frequency domain operator")
    .def(py::init<const Metadata>())
    .def("changeparameter",&MultiTaperXcorDecon::changeparameter,"Change operator parameters")
    .def("process",&MultiTaperXcorDecon::process,"Process previously loaded data")
    .def("loadnoise",&MultiTaperXcorDecon::loadnoise,"Load noise data for regularization")
    .def("load",&MultiTaperXcorDecon::load,"Load all data, wavelet, and noise")
    .def("actual_output",&MultiTaperXcorDecon::actual_output,"Return actual output of inverse*wavelet")
    .def("inverse_wavelet",py::overload_cast<>(&MultiTaperXcorDecon::inverse_wavelet))
    .def("inverse_wavelet",py::overload_cast<double>(&MultiTaperXcorDecon::inverse_wavelet))
    .def("QCMetrics",&MultiTaperXcorDecon::QCMetrics,"Return ideal output of for inverse")
    .def("get_taperlen",&MultiTaperXcorDecon::get_taperlen,"Get length of the Slepian tapers used by the operator")
    .def("get_number_tapers",&MultiTaperXcorDecon::get_number_tapers,"Get number of Slepian tapers used by the operator")
    .def("get_time_bandwidth_product",&MultiTaperXcorDecon::get_time_bandwidth_product,"Get time bandwidt product of Slepian tapers used by the operator")
  ;
  /* this wrapper is propertly constructed, but for now we disable it
   * because it has known bugs that need to be squashed.  It should be
   * turned back on if and when those bugs are squashed.
  py::class_<GeneralIterDecon,ScalarDecon>(m,"GeneralIterDecon","Water level frequency domain operator")
    .def(py::init<AntelopePf&>())
    .def("changeparameter",&GeneralIterDecon::changeparameter,"Change operator parameters")
    .def("process",&GeneralIterDecon::process,"Process previously loaded data")
    .def("loadnoise",&GeneralIterDecon::loadnoise,"Load noise data for regularization")
    .def("load",py::overload_cast<const CoreSeismogram&,const TimeWindow>
            (&GeneralIterDecon::load),"Load data")
    .def("actual_output",&GeneralIterDecon::actual_output,"Return actual output of inverse*wavelet")
    .def("inverse_wavelet",py::overload_cast<>(&GeneralIterDecon::inverse_wavelet))
    .def("inverse_wavelet",py::overload_cast<double>(&GeneralIterDecon::inverse_wavelet))
    .def("QCMetrics",&GeneralIterDecon::QCMetrics,"Return ideal output of for inverse")
  ;
  */

  py::class_<CNR3CDecon,FFTDeconOperator>(m,"CNR3CDecon",
       "Colored noise regularized three component deconvolution")
    .def(py::init<>())
    .def(py::init<const AntelopePf&>())
    .def(py::init<const CNR3CDecon&>())
    .def("change_parameters",&CNR3CDecon::change_parameters,
        "Change operator definition")
    .def("loaddata",py::overload_cast<Seismogram&,const int,const bool>(&CNR3CDecon::loaddata),
        "Load data defining wavelet by one data component")
    .def("loaddata",py::overload_cast<Seismogram&,const bool>(&CNR3CDecon::loaddata),
        "Load data only with optional noise")
    .def("loadnoise_data",py::overload_cast<const Seismogram&>(&CNR3CDecon::loadnoise_data),
        "Load noise to use for regularization from a seismogram")
    .def("loadnoise_data",py::overload_cast<const PowerSpectrum&>(&CNR3CDecon::loadnoise_data),
        "Load noise to use for regularization from a seismogram")
    .def("loadnoise_wavelet",py::overload_cast<const TimeSeries&>(&CNR3CDecon::loadnoise_wavelet),
        "Load noise to use for regularization from a seismogram")
    .def("loadnoise_wavelet",py::overload_cast<const PowerSpectrum&>(&CNR3CDecon::loadnoise_wavelet),
        "Load noise to use for regularization from a seismogram")
    .def("loadwavelet",&CNR3CDecon::loadwavelet,
        "Load an externally determined wavelet for deconvolution")
    .def("process",&CNR3CDecon::process,"Process data previously loaded")
    .def("ideal_output",&CNR3CDecon::ideal_output,
        "Return ideal output for this operator")
    .def("actual_output",&CNR3CDecon::actual_output,"Return actual output computed for current wavelet")
    .def("inverse_wavelet",&CNR3CDecon::inverse_wavelet,
        "Return time domain form of inverse wavelet")
    .def("QCMetrics",&CNR3CDecon::QCMetrics,
        "Return set of quality control metrics for this operator")
    .def("wavelet_noise_spectrum",&CNR3CDecon::wavelet_noise_spectrum,
        "Return power spectrum of noise used for regularization")
    .def("data_noise_spectrum",&CNR3CDecon::data_noise_spectrum,
        "Return power spectrum of noise on 3C data used to define shaping wavelet")
    .def("wavelet_spectrum",&CNR3CDecon::wavelet_spectrum,
         "Return power spectrum of wavelet used for deconvolutions")
    .def("data_spectrum",&CNR3CDecon::data_spectrum,
         "Return average power spectrum of signal on all 3 components of the data")
  ;

  py::class_<MTPowerSpectrumEngine>(m,"MTPowerSpectrumEngine",
      "Processing object used compute multitaper power spectrum estimates from time series data")
    .def(py::init<>())
    .def(py::init<const int, const double, const int>(),
      "Parameterized constructor:  nsamples, tbp, ntapers")
    .def(py::init<const MTPowerSpectrumEngine&>(),"Copy constructor")
    .def("apply",py::overload_cast<const mspass::seismic::TimeSeries&>(&MTPowerSpectrumEngine::apply),
      "Compute from data in a TimeSeries container")
    .def("apply",py::overload_cast<const std::vector<double>&>(&MTPowerSpectrumEngine::apply),
      "Compute from data stored in a simple vector container")
    .def("df",&MTPowerSpectrumEngine::df,"Return frequency bin size")
    .def("taper_length",&MTPowerSpectrumEngine::taper_length,
      "Return number of samples assumed by the operator for input data to be processed")
    .def("time_bandwidth_product",&MTPowerSpectrumEngine::time_bandwidth_product,
      "Return the time-bandwidth product of this operator")
    .def("number_tapers",&MTPowerSpectrumEngine::number_tapers,
      "Return the number of tapers this operator uses for power spectrum estimates")
    .def("set_df",&MTPowerSpectrumEngine::set_df,
      "Change the assumed frequency bin sample interval")
  ;
  m.def("circular_shift",&circular_shift,"Time-domain circular shift operator",
      py::return_value_policy::copy,
      py::arg("d"),
      py::arg("i0") )
    ;
}

} // namespace mspasspy
} // namespace mspass
