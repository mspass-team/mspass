#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <pybind11/operators.h>
#include <pybind11/embed.h>

#include <boost/archive/text_oarchive.hpp>

#include <mspass/algorithms/deconvolution/ComplexArray.h>
#include <mspass/algorithms/deconvolution/ShapingWavelet.h>
#include <mspass/algorithms/deconvolution/WaterLevelDecon.h>
#include <mspass/algorithms/deconvolution/LeastSquareDecon.h>
#include <mspass/algorithms/deconvolution/TimeDomainLeastSquareDecon.h>
#include <mspass/algorithms/deconvolution/MultiTaperXcorDecon.h>
#include <mspass/algorithms/deconvolution/MultiTaperSpecDivDecon.h>
#include <mspass/algorithms/deconvolution/NoiseStableDecon.h>
#include <mspass/algorithms/deconvolution/TimeDomainGIDDecon.h>
#include <mspass/algorithms/deconvolution/FrequencyDomainGIDDecon.h>
#include <mspass/algorithms/deconvolution/CNRDeconEngine.h>
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
  py::bind_vector<std::vector<double>>(m, "DoubleVector");
  /* All the frequency domain operators use this class internally.
   * Useful still to have bindings to the underlying class. */
  py::class_<ComplexArray>(m,"ComplexArray","Complex valued Fortran style array implementation used in MsPASS Decon opertors")
    //.def(py::init<std::vector<Complex64&>())
    .def(py::init<int,double *>())
    .def(py::init<const ComplexArray&>())
    .def("conj",&ComplexArray::conj,"Convert array elements to complex conjugates")
    .def("abs",&ComplexArray::abs,"Return DoubleVector of complex magnitudes")
    .def("rms",&ComplexArray::rms,"Return rms of array content")
    .def("norm2",&ComplexArray::norm2,"Return L2 norm of array content")
    .def("phase",&ComplexArray::phase,"Return DoubleVector of phase of components")
    .def("size",&ComplexArray::size,"Return number of components in the array")
    ;
  /* All frequency domain methods uses this class internally as well.
   * It actually contains a ComplexArray.   Useful to have these bindings
   * for testing and inspection of the result of a get_shaping_wavelet method.
   * */
  py::class_<ShapingWavelet>(m,"ShapingWavelet","Shaping wavelet object used in MsPASS decon frequency domain decon operators")
    .def(py::init<const Metadata&,int>())
    .def(py::init<int,double,int,double,double,int>())
    .def(py::init<double,double,int>())
    .def("impulse_response",&ShapingWavelet::impulse_response,"Return the impulse response of the wavelet in a CoreTimeSeries container")
    .def("df",&ShapingWavelet::freq_bin_size,"Return frequency bin size (Hz)")
    .def("dt",&ShapingWavelet::sample_interval,"Return sample interval of wavelet in the time domain")
    .def("type",&ShapingWavelet::type,"Return the string description of the type of signal this wavelet defines")
    .def("size",&ShapingWavelet::size,"Size of the complex array defining the wavelet internally")
    ;



  /* this is a set of deconvolution related classes*/
  py::class_<ScalarDecon,PyScalarDecon>(m,"ScalarDecon","Base class for scalar TimeSeries data")
    .def("load",&ScalarDecon::load,
    py::arg("w"),py::arg("d"),"Load data and wavelet to use to construct deconvolutions operator")
    .def("loaddata",&ScalarDecon::loaddata,py::arg("d"))
    .def("loadwavelet",&ScalarDecon::loadwavelet,py::arg("w"))
    .def("process",&ScalarDecon::process)
    .def("getresult",&ScalarDecon::getresult,
            "Fetch vector of deconvolved data - after calling process"
        )
    .def("change_parameter",&ScalarDecon::changeparameter,"Change deconvolution parameters")
    .def("change_shaping_wavelet",&ScalarDecon::change_shaping_wavelet,
            "Change the shaping wavelet applied to output"
        )
    .def("get_shaping_wavelet",&ScalarDecon::get_shaping_wavelet,
	    "Get the shaping wavelet used by this operator")
    .def("actual_output",&ScalarDecon::actual_output,
      "Return actual output/resolution kernel of inverse*wavelet"
      )
    .def("resolution_kernel",&ScalarDecon::resolution_kernel,
      "Return actual output/resolution kernel of inverse*wavelet"
      )
    .def("output_shaping_wavelet",&ScalarDecon::output_shaping_wavelet,
      "Return the output shaping wavelet ws(t)"
      )
    .def("ideal_output",&ScalarDecon::ideal_output,
      "Legacy alias for output_shaping_wavelet"
      )
    .def("inverse_wavelet",py::overload_cast<>(&ScalarDecon::inverse_wavelet))
    .def("inverse_wavelet",py::overload_cast<double>(&ScalarDecon::inverse_wavelet))
    .def("QCMetrics",&ScalarDecon::QCMetrics,"Return QC metrics")
  ;
  py::class_<WaterLevelDecon,ScalarDecon>(m,"WaterLevelDecon","Water level frequency domain operator")
    .def(py::init<const Metadata>())
    .def("changeparameter",&WaterLevelDecon::changeparameter,"Change operator parameters")
    .def("process",&WaterLevelDecon::process,
      "Process previously loaded data")
    .def("actual_output",&WaterLevelDecon::actual_output,
      "Return actual output of inverse*wavelet")
    .def("inverse_wavelet",py::overload_cast<>(&WaterLevelDecon::inverse_wavelet))
    .def("inverse_wavelet",py::overload_cast<double>(&WaterLevelDecon::inverse_wavelet))
    .def("QCMetrics",&WaterLevelDecon::QCMetrics,"Return QC metrics")
    .def(py::pickle(
      [](const WaterLevelDecon &self) {
        pybind11::gil_scoped_acquire acquire;
        stringstream sstm;
        boost::archive::text_oarchive artm(sstm);
        artm<<self;
        pybind11::tuple r_tuple = py::make_tuple(sstm.str());
        pybind11::gil_scoped_release release;
        return r_tuple;
      },
      [](py::tuple t) {
        pybind11::gil_scoped_acquire acquire;
        stringstream sstm(t[0].cast<std::string>());
        boost::archive::text_iarchive artm(sstm);
        WaterLevelDecon lsd;
        artm >> lsd;
        pybind11::gil_scoped_release release;
        return lsd;
      }
    ))
  ;
  py::class_<LeastSquareDecon,ScalarDecon>(m,"LeastSquareDecon","Damped least squares frequency domain operator")
    .def(py::init<const Metadata>())
    .def("changeparameter",&LeastSquareDecon::changeparameter,"Change operator parameters")
    .def("process",&LeastSquareDecon::process,
      "Process previously loaded data")
    .def("actual_output",&LeastSquareDecon::actual_output,
      "Return actual output of inverse*wavelet")
    .def("inverse_wavelet",py::overload_cast<>(&LeastSquareDecon::inverse_wavelet))
    .def("inverse_wavelet",py::overload_cast<double>(&LeastSquareDecon::inverse_wavelet))
    .def("QCMetrics",&LeastSquareDecon::QCMetrics,
      "Return QC metrics")
    .def(py::pickle(
      [](const LeastSquareDecon &self) {
        pybind11::gil_scoped_acquire acquire;
        stringstream sstm;
        boost::archive::text_oarchive artm(sstm);
        artm<<self;
        pybind11::tuple r_tuple = py::make_tuple(sstm.str());
        pybind11::gil_scoped_release release;
        return r_tuple;
      },
      [](py::tuple t) {
        pybind11::gil_scoped_acquire acquire;
        stringstream sstm(t[0].cast<std::string>());
        boost::archive::text_iarchive artm(sstm);
        LeastSquareDecon lsd;
        artm >> lsd;
        ShapingWavelet sw=lsd.get_shaping_wavelet();
        ComplexArray w(*sw.wavelet());
        pybind11::gil_scoped_release release;
        return lsd;
      }
    ))
  ;
  py::class_<NoiseStableDecon,ScalarDecon>(m,"NoiseStableDecon",
      "Noise-aware stable frequency-domain inverse operator used by NS-GID")
    .def(py::init<const Metadata>())
    .def("changeparameter",&NoiseStableDecon::changeparameter,
      "Change operator parameters")
    .def("process",&NoiseStableDecon::process,
      "Process previously loaded data")
    .def("loadnoise",py::overload_cast<const std::vector<double>&>(&NoiseStableDecon::loadnoise),
      "Load a noise time series vector used to estimate inverse-gain stability")
    .def("loadnoise",py::overload_cast<const CoreTimeSeries&>(&NoiseStableDecon::loadnoise),
      "Load a noise CoreTimeSeries used to estimate inverse-gain stability")
    .def("loadnoise",py::overload_cast<const PowerSpectrum&>(&NoiseStableDecon::loadnoise),
      "Load a noise PowerSpectrum used to estimate inverse-gain stability")
    .def("actual_output",&NoiseStableDecon::actual_output,
      "Return actual output of inverse*wavelet")
    .def("inverse_wavelet",py::overload_cast<>(&NoiseStableDecon::inverse_wavelet))
    .def("inverse_wavelet",py::overload_cast<double>(&NoiseStableDecon::inverse_wavelet))
    .def("QCMetrics",&NoiseStableDecon::QCMetrics,
      "Return NS-GID inverse operator QC metrics")
    .def("max_gain",&NoiseStableDecon::max_gain)
    .def("gain_cap",&NoiseStableDecon::gain_cap)
  ;
  py::class_<TimeDomainLeastSquareDecon,ScalarDecon>(m,"TimeDomainLeastSquareDecon",
      "Damped least-squares time-domain operator for cropped linear convolution")
    .def(py::init<const Metadata>())
    .def("changeparameter",&TimeDomainLeastSquareDecon::changeparameter,
      "Change operator parameters")
    .def("process",&TimeDomainLeastSquareDecon::process,
      "Process previously loaded data")
    .def("actual_output",&TimeDomainLeastSquareDecon::actual_output,
      "Return actual output of inverse*wavelet")
    .def("inverse_wavelet",py::overload_cast<>(&TimeDomainLeastSquareDecon::inverse_wavelet))
    .def("inverse_wavelet",py::overload_cast<double>(&TimeDomainLeastSquareDecon::inverse_wavelet))
    .def("QCMetrics",&TimeDomainLeastSquareDecon::QCMetrics,
      "Return quality metrics for the time-domain least-squares solve")
    .def(py::pickle(
      [](const TimeDomainLeastSquareDecon &self) {
        pybind11::gil_scoped_acquire acquire;
        stringstream sstm;
        boost::archive::text_oarchive artm(sstm);
        artm<<self;
        pybind11::tuple r_tuple = py::make_tuple(sstm.str());
        pybind11::gil_scoped_release release;
        return r_tuple;
      },
      [](py::tuple t) {
        pybind11::gil_scoped_acquire acquire;
        stringstream sstm(t[0].cast<std::string>());
        boost::archive::text_iarchive artm(sstm);
        TimeDomainLeastSquareDecon lsd;
        artm >> lsd;
        pybind11::gil_scoped_release release;
        return lsd;
      }
    ))
  ;
  py::class_<MultiTaperSpecDivDecon,ScalarDecon>(
    m,
    "MultiTaperSpecDivDecon",
    "Multitaper power-stabilized spectral-division deconvolution operator")
    .def(py::init<const Metadata>())
    .def("changeparameter",&MultiTaperSpecDivDecon::changeparameter,"Change operator parameters")
    .def("process",&MultiTaperSpecDivDecon::process,
      "Process previously loaded data")
    .def("loadnoise",&MultiTaperSpecDivDecon::loadnoise,
      "Load noise data for regularization")
    .def("load",&MultiTaperSpecDivDecon::load,
      "Load all data, wavelet, and noise")
    .def("actual_output",&MultiTaperSpecDivDecon::actual_output,
      "Return actual output of inverse*wavelet")
    .def("inverse_wavelet",py::overload_cast<>(&MultiTaperSpecDivDecon::inverse_wavelet))
    .def("inverse_wavelet",py::overload_cast<double>(&MultiTaperSpecDivDecon::inverse_wavelet))
    .def("QCMetrics",&MultiTaperSpecDivDecon::QCMetrics,"Return QC metrics")
    .def("get_taperlen",&MultiTaperSpecDivDecon::get_taperlen,"Get length of the Slepian tapers used by the operator")
    .def("get_number_tapers",&MultiTaperSpecDivDecon::get_number_tapers,"Get number of Slepian tapers used by the operator")
    .def("get_number_outputs",&MultiTaperSpecDivDecon::get_number_outputs,"Get number of combined output products retained by the operator")
    .def("get_time_bandwidth_product",&MultiTaperSpecDivDecon::get_time_bandwidth_product,"Get time bandwidt product of Slepian tapers used by the operator")
    .def(py::pickle(
      [](const MultiTaperSpecDivDecon &self) {
        pybind11::gil_scoped_acquire acquire;
        stringstream sstm;
        boost::archive::text_oarchive artm(sstm);
        artm<<self;
        pybind11::tuple r_tuple = py::make_tuple(sstm.str());
        pybind11::gil_scoped_release release;
        return r_tuple;
      },
      [](py::tuple t) {
        pybind11::gil_scoped_acquire acquire;
        stringstream sstm(t[0].cast<std::string>());
        boost::archive::text_iarchive artm(sstm);
        MultiTaperSpecDivDecon lsd;
        artm >> lsd;
        pybind11::gil_scoped_release release;
        return lsd;
      }
    ))
  ;
  py::class_<FFTDeconOperator>(m,"FFTDeconOperator","Base class used by frequency domain deconvolution methods")
    .def(py::init<>())
    .def("change_size",&FFTDeconOperator::change_size,"Change fft buffer size")
    .def("get_size",&FFTDeconOperator::get_size,"Get current fft buffer size")
    .def("change_shift",&FFTDeconOperator::change_shift,"Change reference time shift")
    .def("get_shift",&FFTDeconOperator::get_shift,"Get current reference time shift")
    .def("df",&FFTDeconOperator::df,"Get frequency bin size")
  ;
  py::class_<MultiTaperXcorDecon,ScalarDecon>(
    m,
    "MultiTaperXcorDecon",
    "Multitaper source-power-stabilized deconvolution operator")
    .def(py::init<const Metadata>())
    .def("changeparameter",&MultiTaperXcorDecon::changeparameter,"Change operator parameters")
    .def("process",&MultiTaperXcorDecon::process,
      "Process previously loaded data")
    .def("loadnoise",&MultiTaperXcorDecon::loadnoise,
      "Load noise data for regularization")
    .def("load",&MultiTaperXcorDecon::load,"Load all data, wavelet, and noise")
    .def("actual_output",&MultiTaperXcorDecon::actual_output,
      "Return actual output of inverse*wavelet")
    .def("inverse_wavelet",py::overload_cast<>(&MultiTaperXcorDecon::inverse_wavelet))
    .def("inverse_wavelet",py::overload_cast<double>(&MultiTaperXcorDecon::inverse_wavelet))
    .def("QCMetrics",&MultiTaperXcorDecon::QCMetrics,"Return QC metrics")
    .def("get_taperlen",&MultiTaperXcorDecon::get_taperlen,"Get length of the Slepian tapers used by the operator")
    .def("get_number_tapers",&MultiTaperXcorDecon::get_number_tapers,"Get number of Slepian tapers used by the operator")
    .def("get_time_bandwidth_product",&MultiTaperXcorDecon::get_time_bandwidth_product,"Get time bandwidt product of Slepian tapers used by the operator")
    .def(py::pickle(
      [](const MultiTaperXcorDecon &self) {
        pybind11::gil_scoped_acquire acquire;
        stringstream sstm;
        boost::archive::text_oarchive artm(sstm);
        artm<<self;
        pybind11::tuple r_tuple = py::make_tuple(sstm.str());
        pybind11::gil_scoped_release release;
        return r_tuple;
      },
      [](py::tuple t) {
        pybind11::gil_scoped_acquire acquire;
        stringstream sstm(t[0].cast<std::string>());
        boost::archive::text_iarchive artm(sstm);
        MultiTaperXcorDecon lsd;
        artm >> lsd;
        pybind11::gil_scoped_release release;
        return lsd;
      }
    ))
  ;
  py::class_<TimeDomainGIDDecon,ScalarDecon>(m,"TimeDomainGIDDecon",
      "Generalized iterative deconvolution operator for three-component receiver functions")
    .def(py::init<const AntelopePf&>())
    .def("changeparameter",&TimeDomainGIDDecon::changeparameter,"Change operator parameters")
    .def("process",&TimeDomainGIDDecon::process,"Process previously loaded data")
    .def("loadnoise",py::overload_cast<const CoreSeismogram&,mspass::algorithms::TimeWindow>(&TimeDomainGIDDecon::loadnoise),
      "Load noise data for regularization from a Seismogram window")
    .def("loadnoise",py::overload_cast<const TimeSeries&>(&TimeDomainGIDDecon::loadnoise),
      "Load externally supplied scalar noise")
    .def("loadnoise",py::overload_cast<const CoreTimeSeries&>(&TimeDomainGIDDecon::loadnoise),
      "Load externally supplied scalar noise")
    .def("loadnoise",py::overload_cast<const PowerSpectrum&>(&TimeDomainGIDDecon::loadnoise),
      "Load externally supplied noise power spectrum")
    .def("loadwavelet",py::overload_cast<const TimeSeries&>(&TimeDomainGIDDecon::loadwavelet),
      "Load an externally supplied wavelet used for all components")
    .def("loadwavelet",py::overload_cast<const CoreTimeSeries&>(&TimeDomainGIDDecon::loadwavelet),
      "Load an externally supplied wavelet used for all components")
    .def("clear_external_wavelet",&TimeDomainGIDDecon::clear_external_wavelet,
      "Clear any previously loaded external wavelet")
    .def("clear_external_noise",&TimeDomainGIDDecon::clear_external_noise,
      "Clear any previously loaded external noise or noise spectrum")
    .def("deconvolution_window_start",
      &TimeDomainGIDDecon::deconvolution_window_start,
      "Return start time of the configured deconvolution window")
    .def("deconvolution_window_end",
      &TimeDomainGIDDecon::deconvolution_window_end,
      "Return end time of the configured deconvolution window")
    .def("noise_window_start",
      &TimeDomainGIDDecon::noise_window_start,
      "Return start time of the configured noise window")
    .def("noise_window_end",
      &TimeDomainGIDDecon::noise_window_end,
      "Return end time of the configured noise window")
    .def("load",py::overload_cast<const CoreSeismogram&,mspass::algorithms::TimeWindow>
            (&TimeDomainGIDDecon::load),"Load data")
    .def("load",py::overload_cast<const CoreSeismogram&,mspass::algorithms::TimeWindow,mspass::algorithms::TimeWindow>
            (&TimeDomainGIDDecon::load),"Load data and noise windows")
    .def("getresult",&TimeDomainGIDDecon::getresult,
            "Return the deconvolved three-component receiver function")
    .def("sparse_output",&TimeDomainGIDDecon::sparse_output,
            "Return the raw sparse impulse response from the GID iteration")
    .def("ideal_output",&TimeDomainGIDDecon::ideal_output,
            "Legacy alias for output_shaping_wavelet")
    .def("actual_output",&TimeDomainGIDDecon::actual_output,
            "Return actual output/resolution kernel of the inverse operator")
    .def("inverse_wavelet",py::overload_cast<>(&TimeDomainGIDDecon::inverse_wavelet))
    .def("inverse_wavelet",py::overload_cast<double>(&TimeDomainGIDDecon::inverse_wavelet))
    .def("QCMetrics",&TimeDomainGIDDecon::QCMetrics,"Return QC metrics")
  ;

  py::class_<FrequencyDomainGIDDecon,ScalarDecon>(m,"FrequencyDomainGIDDecon",
      "Frequency-domain generalized iterative deconvolution operator for three-component receiver functions")
    .def(py::init<const AntelopePf&>())
    .def("changeparameter",&FrequencyDomainGIDDecon::changeparameter,"Change operator parameters")
    .def("process",&FrequencyDomainGIDDecon::process,"Process previously loaded data")
    .def("loadnoise",py::overload_cast<const CoreSeismogram&,mspass::algorithms::TimeWindow>(&FrequencyDomainGIDDecon::loadnoise),
      "Load noise data for regularization from a Seismogram window")
    .def("loadnoise",py::overload_cast<const TimeSeries&>(&FrequencyDomainGIDDecon::loadnoise),
      "Load externally supplied scalar noise")
    .def("loadnoise",py::overload_cast<const CoreTimeSeries&>(&FrequencyDomainGIDDecon::loadnoise),
      "Load externally supplied scalar noise")
    .def("loadnoise",py::overload_cast<const PowerSpectrum&>(&FrequencyDomainGIDDecon::loadnoise),
      "Load externally supplied noise power spectrum")
    .def("loadwavelet",py::overload_cast<const TimeSeries&>(&FrequencyDomainGIDDecon::loadwavelet),
      "Load an externally supplied wavelet used for all components")
    .def("loadwavelet",py::overload_cast<const CoreTimeSeries&>(&FrequencyDomainGIDDecon::loadwavelet),
      "Load an externally supplied wavelet used for all components")
    .def("clear_external_wavelet",&FrequencyDomainGIDDecon::clear_external_wavelet,
      "Clear any previously loaded external wavelet")
    .def("clear_external_noise",&FrequencyDomainGIDDecon::clear_external_noise,
      "Clear any previously loaded external noise or noise spectrum")
    .def("deconvolution_window_start",
      &FrequencyDomainGIDDecon::deconvolution_window_start,
      "Return start time of the configured deconvolution window")
    .def("deconvolution_window_end",
      &FrequencyDomainGIDDecon::deconvolution_window_end,
      "Return end time of the configured deconvolution window")
    .def("noise_window_start",
      &FrequencyDomainGIDDecon::noise_window_start,
      "Return start time of the configured noise window")
    .def("noise_window_end",
      &FrequencyDomainGIDDecon::noise_window_end,
      "Return end time of the configured noise window")
    .def("load",py::overload_cast<const CoreSeismogram&,mspass::algorithms::TimeWindow>
            (&FrequencyDomainGIDDecon::load),"Load data")
    .def("load",py::overload_cast<const CoreSeismogram&,mspass::algorithms::TimeWindow,mspass::algorithms::TimeWindow>
            (&FrequencyDomainGIDDecon::load),"Load data and noise windows")
    .def("getresult",&FrequencyDomainGIDDecon::getresult,
            "Return the deconvolved three-component receiver function")
    .def("sparse_output",&FrequencyDomainGIDDecon::sparse_output,
            "Return the raw sparse impulse response from the GID iteration")
    .def("ideal_output",&FrequencyDomainGIDDecon::ideal_output,
            "Legacy alias for output_shaping_wavelet")
    .def("actual_output",&FrequencyDomainGIDDecon::actual_output,
            "Return actual output/resolution kernel of the inverse operator")
    .def("inverse_wavelet",py::overload_cast<>(&FrequencyDomainGIDDecon::inverse_wavelet))
    .def("inverse_wavelet",py::overload_cast<double>(&FrequencyDomainGIDDecon::inverse_wavelet))
    .def("QCMetrics",&FrequencyDomainGIDDecon::QCMetrics,"Return QC metrics")
  ;

  py::class_<CNRDeconEngine,FFTDeconOperator>(m,"CNRDeconEngine",
       "Colored noise regularized deconvolution engine - used for single station and array data")
    /* A default constructor this object is always invalid so we don't include this binding.
    .def(py::init<>())
    */
    .def(py::init<const AntelopePf&>())
    /* This overloaded version is not currently used in python functions that
    use this operator.   Left in the binding code for flexilitity but could be
    deleted*/
    .def("initialize_inverse_operator_TS",
        py::overload_cast<const TimeSeries&,const TimeSeries&>(&CNRDeconEngine::initialize_inverse_operator),
        "Load required data to initialize frequency domain inverse operator - overloaded version using time domain noise vector")
    .def("initialize_inverse_operator",
        py::overload_cast<const TimeSeries&,const PowerSpectrum&>(&CNRDeconEngine::initialize_inverse_operator),
        "Load required data to initialize frequency domain inverse operator - overloaded version using precomputed power spectrum of noise")
    .def("process",&CNRDeconEngine::process,
        "Deconvolve Seismogram data using inverse operator loaded previously - shape to specified bandwidth arg1 to arg2 frequency")
    .def("get_operator_dt",&CNRDeconEngine::get_operator_dt,"Return operator sample interval")
    .def("compute_noise_spectrum",
        py::overload_cast<const TimeSeries&>(&CNRDeconEngine::compute_noise_spectrum),
        "Computes a noise spectrum from a TimeSeries object using the same multitaper parameters as the inverse operator"
        )

    .def("compute_noise_spectrum_3C",
        py::overload_cast<const Seismogram&>(&CNRDeconEngine::compute_noise_spectrum),
        "Computes a noise spectrum from a Seismogram object using the same multitaper parameters as the inverse operator with average of three components")
    .def("ideal_output",&CNRDeconEngine::ideal_output,
      "Legacy alias for output_shaping_wavelet")
    .def("output_shaping_wavelet",&CNRDeconEngine::output_shaping_wavelet,
      "Return the output shaping wavelet ws(t)")
    .def("actual_output",&CNRDeconEngine::actual_output,
      "Return actual output/resolution kernel for the supplied wavelet")
    .def("resolution_kernel",&CNRDeconEngine::resolution_kernel,
      "Return actual output/resolution kernel for the supplied wavelet")
    .def("inverse_wavelet",&CNRDeconEngine::inverse_wavelet,
        "Return the time-domain inverse operator computed form current frequency domain operator")
    .def("QCMetrics",&CNRDeconEngine::QCMetrics,"Return a Metadata container of QC metrics computed by this algorithm")
    .def(py::pickle(
      [](const CNRDeconEngine &self) {
        pybind11::gil_scoped_acquire acquire;
        stringstream sstm;
        boost::archive::text_oarchive artm(sstm);
        artm<<self;
        pybind11::tuple r_tuple = py::make_tuple(sstm.str());
        pybind11::gil_scoped_release release;
        return r_tuple;
      },
      [](py::tuple t) {
        pybind11::gil_scoped_acquire acquire;
        CNRDeconEngine lsd;
	try {
            stringstream sstm(t[0].cast<std::string>());
            boost::archive::text_iarchive artm(sstm);
            artm >> lsd;
        } catch (const boost::archive::archive_exception& e) {
            std::cerr << "CNRDeconEngine Archive exception: " << e.what() << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "CNRDeconEngine Standard exception: " << e.what() << std::endl;
        }
        pybind11::gil_scoped_release release;
        return lsd;
      }
    ))
  ;


/* This algorithm was the prototype for CNRDeconEngine.   It is depricated with prejudice which here means I'm
   removing the bindings for the old version.   The original algorithm will be placed for at least a while in some
   dustbin.  When that is gone remove this comment.  GLP - July 2024 */
  /*
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
        "Legacy alias for output_shaping_wavelet")
    .def("output_shaping_wavelet",&CNR3CDecon::output_shaping_wavelet,
        "Return the output shaping wavelet ws(t)")
    .def("actual_output",&CNR3CDecon::actual_output,
        "Return actual output/resolution kernel computed for current wavelet")
    .def("resolution_kernel",&CNR3CDecon::resolution_kernel,
        "Return actual output/resolution kernel computed for current wavelet")
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
  */

  py::class_<MTPowerSpectrumEngine>(m,"MTPowerSpectrumEngine",
      "Processing object used compute multitaper power spectrum estimates from time series data")
    .def(py::init<>())
    .def(py::init<const int, const double, const int, const int, const double>(),
      "Parameterized constructor:  nsamples, tbp, ntapers, nfft, dt")
    .def(py::init<const int, const double, const int>(),
        "Parameterized constructor:  nsamples, tbp, ntapers(nfft=2*nsamples, dt=1.0")
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
    .def("nf",&MTPowerSpectrumEngine::nf,"Return number of frequency bins in this operator")
    .def("nfft",&MTPowerSpectrumEngine::fftsize,"Return size of fft workspace in this operator")
    /* We do pickle for this object in a different way than I've ever done this
    before.  This object can be define by only 5 numbers that are expanded in
    the constructor into what can be very large arrays.  An untested hypothesis that
    this approach builds on is that it is cheaper to recompute the tapers when
    this object gets serialized than it is to serialize, move, and deserialize
    the large arrays.   It definitely simplifies this binding code.  If the
    result proves ponderously slow that hypothesis should be tested.
    */
    .def(py::pickle(
      [](const MTPowerSpectrumEngine& self)
      {
        return py::make_tuple(self.taper_length(),self.time_bandwidth_product(),self.number_tapers(),self.fftsize(),self.dt());
      },
      [](py::tuple t)
      {
        pybind11::gil_scoped_acquire acquire;
        int taperlen=t[0].cast<int>();
        double tbp=t[1].cast<double>();
        int ntapers=t[2].cast<int>();
        int nfft=t[3].cast<int>();
        double dt=t[4].cast<double>();
        MTPowerSpectrumEngine engine(taperlen,tbp,ntapers,nfft,dt);
        pybind11::gil_scoped_release release;
        return engine;
      }
    ))
  ;
  m.def("circular_shift",&circular_shift,"Time-domain circular shift operator",
      py::call_guard<py::gil_scoped_release>(),
      py::return_value_policy::copy,
      py::arg("d"),
      py::arg("i0") )
    ;
}

} // namespace mspasspy
} // namespace mspass
