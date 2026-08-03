#include <iostream>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

#include "mspass/utility/AntelopePf.h"
#include "mspass/algorithms/deconvolution/ComplexArray.h"
#include "mspass/algorithms/deconvolution/FFTDeconOperator.h"
#include "mspass/algorithms/deconvolution/CNRDeconEngine.h"
#include "mspass/algorithms/deconvolution/LeastSquareDecon.h"
#include "mspass/algorithms/deconvolution/MTPowerSpectrumEngine.h"
#include "mspass/algorithms/deconvolution/MultiTaperSpecDivDecon.h"
#include "mspass/algorithms/deconvolution/MultiTaperXcorDecon.h"
#include "mspass/algorithms/deconvolution/NoiseStableDecon.h"
#include "mspass/algorithms/deconvolution/WaterLevelDecon.h"
#include "mspass/seismic/PowerSpectrum.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/TimeSeries.h"
using namespace std;
using namespace mspass::algorithms::deconvolution;
using mspass::utility::AntelopePf;
using mspass::utility::MsPASSError;
using mspass::utility::pfread;
using mspass::utility::Metadata;
using mspass::seismic::PowerSpectrum;
using mspass::seismic::Seismogram;
using mspass::seismic::TimeReferenceType;
using mspass::seismic::TimeSeries;

void CheckCondition(const bool condition,const char *expression,
                    const char *file,const int line)
{
  if(!condition) {
    ostringstream message;
    message << file << ":" << line << ": test check failed: " << expression;
    throw runtime_error(message.str());
  }
}
#define CHECK(...) CheckCondition(static_cast<bool>((__VA_ARGS__)), \
                                  #__VA_ARGS__,__FILE__,__LINE__)

const std::string test_fname([]() {
  const char *tmpdir_env = std::getenv("TMPDIR");
  std::string tmpdir((tmpdir_env == nullptr) ? "/tmp" : tmpdir_env);
  if (tmpdir.empty())
    tmpdir = "/tmp";
  if (tmpdir.back() != '/')
    tmpdir += "/";
  return tmpdir + "mspass_decon_serialization_" +
         std::to_string(static_cast<long>(getpid())) + ".txt";
}());
struct SerializationTempFileCleanup {
  ~SerializationTempFileCleanup() { std::remove(test_fname.c_str()); }
};
struct ComplexArrayArchivePayload {
  int nsamp;
  vector<FortranComplex64> data;
  template<class Archive>
  void serialize(Archive& ar,const unsigned int version) {
    ar & nsamp;
    ar & data;
  }
};
template <class T> void save_data(const T& d)
{
    std::ofstream ofs(test_fname);
    boost::archive::text_oarchive oa(ofs);
    oa << d;
}
template <class T> T restore_data()
{
    std::ifstream ifs(test_fname);
    boost::archive::text_iarchive ia(ifs);
    T d;
    ia >> d;
    return d;
}
template <class T> void restore_data_into(T& d)
{
    std::ifstream ifs(test_fname);
    boost::archive::text_iarchive ia(ifs);
    ia >> d;
}
bool shaping_wavelets_match(ShapingWavelet& s1,
         ShapingWavelet& s2)
{
  if(s1.freq_bin_size()!=s2.freq_bin_size())return false;
  if(s1.sample_interval() != s2.sample_interval())return false;
  if(s1.type() != s2.type())return false;
  if(s1.size() != s2.size())return false;
  /* This may fail if precision is not sufficient*/
  CHECK(s1.size()>0);  // next pointless if not true
  ComplexArray *w1 = s1.wavelet();
  ComplexArray *w2 = s2.wavelet();
  cout << "Size of wavelts being tested = "<<s1.size()<<endl;
  if(w1->rms() != w2->rms()) return false;
  cout << "rms of s1 wavelet in test="<<w1->rms()<<endl;
  return true;
}

TimeSeries make_cnr_wavelet()
{
  const int n(201);
  const double dt(0.05);
  TimeSeries result(n);
  result.set_t0(-5.0);
  result.set_dt(dt);
  result.set_tref(TimeReferenceType::Relative);
  for(int i=0;i<n;++i) {
    const double t=result.t0()+dt*static_cast<double>(i);
    result.s[i]=exp(-0.5*(t/0.2)*(t/0.2));
  }
  result.set_live();
  return result;
}

Seismogram make_cnr_datum()
{
  const int n(701);
  const double dt(0.05);
  Seismogram result(n);
  result.set_t0(-5.0);
  result.set_dt(dt);
  result.set_tref(TimeReferenceType::Relative);
  for(int k=0;k<3;++k) {
    for(int i=0;i<n;++i) {
      const double t=result.t0()+dt*static_cast<double>(i);
      result.u(k,i)=(k+1)*exp(-0.5*(t/0.3)*(t/0.3));
      result.u(k,i)+=0.25*(k+1)*exp(-0.5*((t-8.0)/0.4)*((t-8.0)/0.4));
    }
  }
  result.set_live();
  return result;
}

PowerSpectrum make_cnr_noise_spectrum()
{
  vector<double> power(202,0.01);
  return PowerSpectrum(Metadata(),power,0.05,"copy_assignment_test",0.0,
                       0.05,701);
}

void assert_timeseries_equal(const TimeSeries& lhs,const TimeSeries& rhs)
{
  CHECK(lhs.live() == rhs.live());
  CHECK(lhs.npts()==rhs.npts());
  CHECK(lhs.t0()==rhs.t0());
  CHECK(lhs.dt()==rhs.dt());
  for(int i=0;i<lhs.npts();++i)
    CHECK(abs(lhs.s[i]-rhs.s[i])<=
           1.0e-12*(1.0+max(abs(lhs.s[i]),abs(rhs.s[i]))));
}

void assert_core_timeseries_equal(const mspass::seismic::CoreTimeSeries& lhs,
                                  const mspass::seismic::CoreTimeSeries& rhs)
{
  CHECK(lhs.live() == rhs.live());
  CHECK(lhs.npts()==rhs.npts());
  CHECK(lhs.t0()==rhs.t0());
  CHECK(lhs.dt()==rhs.dt());
  for(size_t i=0;i<lhs.npts();++i)
    CHECK(abs(lhs.s[i]-rhs.s[i])<=
           1.0e-12*(1.0+max(abs(lhs.s[i]),abs(rhs.s[i]))));
}

void assert_seismogram_equal(const Seismogram& lhs,const Seismogram& rhs)
{
  CHECK(lhs.live() == rhs.live());
  CHECK(lhs.npts()==rhs.npts());
  CHECK(lhs.t0()==rhs.t0());
  CHECK(lhs.dt()==rhs.dt());
  for(int k=0;k<3;++k)
    for(int i=0;i<lhs.npts();++i)
      CHECK(lhs.u(k,i)==rhs.u(k,i));
}

void assert_cnr_qc_equal(CNRDeconEngine& lhs,CNRDeconEngine& rhs)
{
  Metadata lqc(lhs.QCMetrics());
  Metadata rqc(rhs.QCMetrics());
  CHECK(lqc.get_int("decon_operator_nfft")==
         rqc.get_int("decon_operator_nfft"));
  CHECK(lqc.get_int("decon_operator_sample_shift")==
         rqc.get_int("decon_operator_sample_shift"));
  const vector<string> real_keys={
      "decon_sample_interval","cnr_regularization_bandwidth_fraction",
      "waveletbf","maxsnr0","maxsnr1","maxsnr2",
      "signalbf0","signalbf1","signalbf2"};
  for(const auto& key : real_keys)
    CHECK(lqc.get_double(key)==rqc.get_double(key));
}

void initialize_scalar(LeastSquareDecon& engine,const vector<double>& wavelet,
                       const vector<double>& data,const vector<double>&)
{
  engine.ScalarDecon::load(wavelet,data);
  engine.process();
}
void initialize_scalar(WaterLevelDecon& engine,const vector<double>& wavelet,
                       const vector<double>& data,const vector<double>&)
{
  engine.ScalarDecon::load(wavelet,data);
  engine.process();
}
void initialize_scalar(MultiTaperXcorDecon& engine,
                       const vector<double>& wavelet,
                       const vector<double>& data,const vector<double>& noise)
{
  engine.load(wavelet,data,noise);
  engine.process();
}
void initialize_scalar(MultiTaperSpecDivDecon& engine,
                       const vector<double>& wavelet,
                       const vector<double>& data,const vector<double>& noise)
{
  engine.load(wavelet,data,noise);
  engine.process();
}
void initialize_scalar(NoiseStableDecon& engine,const vector<double>& wavelet,
                       const vector<double>& data,const vector<double>& noise)
{
  engine.ScalarDecon::load(wavelet,data);
  engine.loadnoise(noise);
  engine.process();
}

template<class Engine>
void assert_scalar_qc_equal(Engine& lhs,Engine& rhs)
{
  Metadata lqc(lhs.QCMetrics());
  Metadata rqc(rhs.QCMetrics());
  const vector<string> string_keys={
      "decon_operator","decon_shaping_wavelet_type",
      "multitaper_operator_type"};
  const vector<string> bool_keys={
      "decon_processed","decon_input_loaded","multitaper_processed",
      "ns_gid_use_reliability_taper"};
  const vector<string> int_keys={
      "decon_data_npts","decon_wavelet_npts","decon_output_npts",
      "decon_shaping_wavelet_nfft","decon_operator_nfft",
      "decon_operator_sample_shift","multitaper_operator_nfft",
      "multitaper_taper_length","multitaper_number_tapers",
      "multitaper_number_outputs","ns_gid_operator_nfft"};
  const vector<string> double_keys={
      "decon_shaping_wavelet_dt","damping_factor",
      "least_square_damping_factor","water_level",
      "water_level_regularization_fraction",
      "multitaper_time_bandwidth_product","multitaper_damping_factor",
      "ns_gid_gain_max_requested","ns_gid_gain_max_actual",
      "ns_gid_mu_min","ns_gid_alpha","ns_gid_noise_amplification",
      "ns_gid_effective_bandwidth_fraction"};
  for(const auto& key : string_keys) {
    CHECK(lqc.is_defined(key)==rqc.is_defined(key));
    if(lqc.is_defined(key))CHECK(lqc.get_string(key)==rqc.get_string(key));
  }
  for(const auto& key : bool_keys) {
    CHECK(lqc.is_defined(key)==rqc.is_defined(key));
    if(lqc.is_defined(key))CHECK(lqc.get_bool(key)==rqc.get_bool(key));
  }
  for(const auto& key : int_keys) {
    CHECK(lqc.is_defined(key)==rqc.is_defined(key));
    if(lqc.is_defined(key))CHECK(lqc.get_int(key)==rqc.get_int(key));
  }
  for(const auto& key : double_keys) {
    CHECK(lqc.is_defined(key)==rqc.is_defined(key));
    if(lqc.is_defined(key))CHECK(lqc.get_double(key)==rqc.get_double(key));
  }
}

template<class Engine>
void assert_scalar_engine_equal(Engine& lhs,Engine& rhs)
{
  CHECK(lhs.get_size()==rhs.get_size());
  CHECK(lhs.get_shift()==rhs.get_shift());
  CHECK(lhs.getresult()==rhs.getresult());
  TimeSeries lhs_actual(lhs.actual_output());
  TimeSeries rhs_actual(rhs.actual_output());
  assert_timeseries_equal(lhs_actual,rhs_actual);
  assert_core_timeseries_equal(lhs.inverse_wavelet(0.0),
                               rhs.inverse_wavelet(0.0));
  assert_scalar_qc_equal(lhs,rhs);
}

template<class Engine>
void exercise_scalar_fft_copy_contract(const Metadata& md,
                                       const vector<double>& wavelet,
                                       const vector<double>& data,
                                       const vector<double>& noise)
{
  Engine source(md);
  initialize_scalar(source,wavelet,data,noise);

  Engine copied(source);
  assert_scalar_engine_equal(source,copied);

  Engine empty_target;
  empty_target=source;
  assert_scalar_engine_equal(source,empty_target);

  Metadata different_md(md);
  different_md.put("deconvolution_data_window_end",60.0);
  different_md.put("operator_nfft",4096);
  Engine different_target(different_md);
  vector<double> alternate_wavelet(wavelet);
  vector<double> alternate_data(data);
  for(size_t i=0;i<alternate_wavelet.size();++i)
    alternate_wavelet[i]=(i%17==0) ? 0.5 : 0.0;
  for(size_t i=0;i<alternate_data.size();++i)
    alternate_data[i]=(i%29==0) ? -0.25 : 0.0;
  initialize_scalar(different_target,alternate_wavelet,alternate_data,noise);
  CHECK(different_target.get_size()!=source.get_size());
  different_target=source;
  assert_scalar_engine_equal(source,different_target);

  Engine& self_alias(different_target);
  different_target=self_alias;
  assert_scalar_engine_equal(source,different_target);
}

int main(int argc, char **argv)
{
    SerializationTempFileCleanup cleanup;
    cout << "Testing serialization of ComplexArray" <<endl;
    ComplexArray z(10);
    std::vector<double> x;
    for(auto i=0;i<10;++i) x.push_back((double)i);
    z = ComplexArray(10,x);
    save_data<ComplexArray>(z);
    ComplexArray z2;
    z2 = restore_data<ComplexArray>();
    CHECK(z.size() == z2.size());
    for(auto i=0;i<10;++i) CHECK(z[i] == z2[i]);
    ComplexArray initialized_z(3,-1.0);
    restore_data_into(initialized_z);
    CHECK(initialized_z.size()==z.size());
    for(int i=0;i<z.size();++i)CHECK(initialized_z[i]==z[i]);
    const vector<Complex64> initialized_z_snapshot(
        [&initialized_z]() {
          vector<Complex64> result;
          for(int i=0;i<initialized_z.size();++i)
            result.push_back(initialized_z[i]);
          return result;
        }());
    const auto assert_complex_load_rejected=
        [&initialized_z,&initialized_z_snapshot](const int archived_count,
                                                  const size_t vector_size) {
          ComplexArrayArchivePayload payload;
          payload.nsamp=archived_count;
          payload.data.resize(vector_size);
          save_data(payload);
          bool rejected(false);
          try {
            restore_data_into(initialized_z);
          } catch (MsPASSError &err) {
            rejected=true;
          }
          CHECK(rejected);
          CHECK(initialized_z.size()==
                 static_cast<int>(initialized_z_snapshot.size()));
          for(int i=0;i<initialized_z.size();++i)
            CHECK(initialized_z[i]==initialized_z_snapshot[i]);
        };
    assert_complex_load_rejected(-1,0);
    assert_complex_load_rejected(4,3);
    assert_complex_load_rejected(2,3);
    cout << "Testing serialization of FFTDeconOperator" <<endl;
    AntelopePf pf=pfread("./RFdeconProcessor.pf");
    cout << "Testing ShapingWavelet FFT resource paths" << endl;
    ShapingWavelet empty_shaping;
    bool empty_impulse_rejected(false);
    try {
      empty_shaping.impulse_response();
    } catch (MsPASSError &err) {
      empty_impulse_rejected=true;
    }
    CHECK(empty_impulse_rejected);
    ShapingWavelet metadata_shaping(pf.get_branch("WaterLevel"));
    CHECK(metadata_shaping.impulse_response().npts()==metadata_shaping.size());
    ShapingWavelet ricker_shaping(1.0,0.05,256);
    CHECK(ricker_shaping.impulse_response().npts()==256);
    ShapingWavelet data_shaping(make_cnr_wavelet(),256);
    CHECK(data_shaping.impulse_response().npts()==256);
    cout << "Testing scalar FFT copy construction and assignment" << endl;
    vector<double> scalar_wavelet(701,0.0);
    vector<double> scalar_data(701,0.0);
    vector<double> scalar_noise(601,0.0);
    for(size_t i=0;i<scalar_wavelet.size();++i) {
      const double t=0.05*static_cast<double>(i);
      scalar_wavelet[i]=exp(-0.5*((t-5.0)/0.25)*((t-5.0)/0.25));
      scalar_data[i]=scalar_wavelet[i];
      if(i>=160)
        scalar_data[i]+=0.3*scalar_wavelet[i-160];
    }
    for(size_t i=0;i<scalar_noise.size();++i) {
      const double t=0.05*static_cast<double>(i);
      scalar_noise[i]=0.01*sin(2.0*M_PI*3.7*t);
    }
    cout << "  LeastSquareDecon" << endl;
    exercise_scalar_fft_copy_contract<LeastSquareDecon>(
        pf.get_branch("LeastSquare"),scalar_wavelet,scalar_data,scalar_noise);
    cout << "  WaterLevelDecon" << endl;
    exercise_scalar_fft_copy_contract<WaterLevelDecon>(
        pf.get_branch("WaterLevel"),scalar_wavelet,scalar_data,scalar_noise);
    cout << "  MultiTaperXcorDecon" << endl;
    exercise_scalar_fft_copy_contract<MultiTaperXcorDecon>(
        pf.get_branch("MultiTaperXcor"),scalar_wavelet,scalar_data,
        scalar_noise);
    cout << "  MultiTaperSpecDivDecon" << endl;
    exercise_scalar_fft_copy_contract<MultiTaperSpecDivDecon>(
        pf.get_branch("MultiTaperSpecDiv"),scalar_wavelet,scalar_data,
        scalar_noise);
    cout << "  NoiseStableDecon" << endl;
    exercise_scalar_fft_copy_contract<NoiseStableDecon>(
        pf.get_branch("LeastSquare"),scalar_wavelet,scalar_data,scalar_noise);
    WaterLevelDecon wl(pf.get_branch("WaterLevel"));
    FFTDeconOperator fftdo(dynamic_cast<FFTDeconOperator&>(wl));
    save_data<FFTDeconOperator>(fftdo);
    FFTDeconOperator fftdo2=restore_data<FFTDeconOperator>();
    CHECK(fftdo.get_size() == fftdo2.get_size());
    CHECK(fftdo.operator_shift() == fftdo2.operator_shift());
    cout << "Testing FFT resource reconfiguration validation" << endl;
    Metadata invalid_fft_md(pf.get_branch("WaterLevel"));
    invalid_fft_md.put("operator_nfft",0);
    bool constructor_rejected_zero_nfft(false);
    try {
      FFTDeconOperator bad_fft(invalid_fft_md);
    } catch (MsPASSError &err) {
      constructor_rejected_zero_nfft=true;
    }
    CHECK(constructor_rejected_zero_nfft);
    const int original_fft_size=fftdo.get_size();
    const int original_fft_shift=fftdo.get_shift();
    Metadata initialized_fft_md(pf.get_branch("WaterLevel"));
    initialized_fft_md.put("operator_nfft",2*original_fft_size);
    FFTDeconOperator initialized_fft_target(initialized_fft_md);
    CHECK(initialized_fft_target.get_size()!=original_fft_size);
    restore_data_into(initialized_fft_target);
    CHECK(initialized_fft_target.get_size()==original_fft_size);
    CHECK(initialized_fft_target.get_shift()==original_fft_shift);

    /* A serialized default object is an intentionally invalid zero-nfft
       archive.  Loading it must leave an initialized target usable. */
    FFTDeconOperator zero_fft_archive;
    save_data(zero_fft_archive);
    bool zero_fft_archive_rejected(false);
    try {
      restore_data_into(initialized_fft_target);
    } catch (MsPASSError &err) {
      zero_fft_archive_rejected=true;
    }
    CHECK(zero_fft_archive_rejected);
    CHECK(initialized_fft_target.get_size()==original_fft_size);
    CHECK(initialized_fft_target.get_shift()==original_fft_shift);
    ComplexArray fft_identity(original_fft_size,1.0);
    CHECK(initialized_fft_target
               .FourierInverse(fft_identity,fft_identity,0.05,0.0)
               .npts()==static_cast<size_t>(original_fft_size));
    bool changeparameter_rejected_zero_nfft(false);
    try {
      fftdo.changeparameter(invalid_fft_md);
    } catch (MsPASSError &err) {
      changeparameter_rejected_zero_nfft=true;
    }
    CHECK(changeparameter_rejected_zero_nfft);
    CHECK(fftdo.get_size()==original_fft_size);
    CHECK(fftdo.get_shift()==original_fft_shift);
    bool change_size_rejected_zero_nfft(false);
    try {
      fftdo.change_size(0);
    } catch (MsPASSError &err) {
      change_size_rejected_zero_nfft=true;
    }
    CHECK(change_size_rejected_zero_nfft);
    CHECK(fftdo.get_size()==original_fft_size);
    CHECK(fftdo.get_shift()==original_fft_shift);
    fftdo.change_size(2*original_fft_size);
    CHECK(fftdo.get_size()==2*original_fft_size);
    CHECK(fftdo.get_shift()==original_fft_shift);
    Metadata restore_fft_md(pf.get_branch("WaterLevel"));
    restore_fft_md.put("operator_nfft",original_fft_size);
    fftdo.changeparameter(restore_fft_md);
    CHECK(fftdo.get_size()==original_fft_size);
    CHECK(fftdo.get_shift()==original_fft_shift);
    cout << "Testing serialization of WaterLevelDecon" <<endl;
    save_data<WaterLevelDecon>(wl);
    WaterLevelDecon wl2(restore_data<WaterLevelDecon>());
    // compare the sahping wavelets
    cout << "Testng if shaping wavelets were serialized corectly"<<endl;
    ShapingWavelet sw1 = wl.get_shaping_wavelet();
    ShapingWavelet sw2 = wl2.get_shaping_wavelet();
    CHECK(shaping_wavelets_match(sw1,sw2));
    cout << "Testing serialization of LeastSquareDecon" <<endl;
    LeastSquareDecon lsd(pf.get_branch("LeastSquare"));
    save_data<LeastSquareDecon>(lsd);
    LeastSquareDecon lsd2(restore_data<LeastSquareDecon>());
    cout << "Testing serialization of MultiTaperXcorDecon" <<endl;
    MultiTaperXcorDecon mtd(pf.get_branch("MultiTaperXcor"));
    save_data<MultiTaperXcorDecon>(mtd);
    MultiTaperXcorDecon mtd2(restore_data<MultiTaperXcorDecon>());
    /* There are limited methods we can use for assert with these empty operators*/
    CHECK(mtd.get_taperlen() == mtd2.get_taperlen());
    CHECK(mtd.get_number_tapers() == mtd2.get_number_tapers());
    CHECK(mtd.get_time_bandwidth_product() == mtd2.get_time_bandwidth_product());
    cout << "Testing serialization of MultiTaperSpecDivDecon" <<endl;
    MultiTaperSpecDivDecon mtd3(pf.get_branch("MultiTaperSpecDiv"));
    save_data<MultiTaperSpecDivDecon>(mtd3);
    MultiTaperSpecDivDecon mtd4(restore_data<MultiTaperSpecDivDecon>());
    CHECK(mtd3.get_taperlen() == mtd4.get_taperlen());
    CHECK(mtd3.get_number_tapers() == mtd4.get_number_tapers());
    CHECK(mtd3.get_time_bandwidth_product() == mtd4.get_time_bandwidth_product());
    cout << "Testing multitaper direct constructor input length validation" << endl;
    vector<double> mt_valid(mtd.get_taperlen(), 1.0);
    vector<double> mt_overlong(mtd.get_taperlen() + 1, 1.0);
    bool constructor_rejected_overlong_data(false);
    try {
      MultiTaperXcorDecon bad_xcor_data(pf.get_branch("MultiTaperXcor"),
                                        mt_valid, mt_valid, mt_overlong);
    } catch (MsPASSError &err) {
      constructor_rejected_overlong_data = true;
    }
    CHECK(constructor_rejected_overlong_data);
    bool constructor_rejected_overlong_noise(false);
    try {
      MultiTaperSpecDivDecon bad_specdiv_noise(
          pf.get_branch("MultiTaperSpecDiv"), mt_overlong, mt_valid, mt_valid);
    } catch (MsPASSError &err) {
      constructor_rejected_overlong_noise = true;
    }
    CHECK(constructor_rejected_overlong_noise);
    cout << "Testing multitaper power spectrum copy construction" << endl;
    const auto mt_constructor_rejected=[](const double tbp,const int ntapers,
                                           const double dt) {
      try {
        MTPowerSpectrumEngine bad_mte(16,tbp,ntapers,16,dt);
      } catch (MsPASSError &err) {
        return true;
      }
      return false;
    };
    CHECK(mt_constructor_rejected(0.0,1,0.05));
    CHECK(mt_constructor_rejected(-1.0,1,0.05));
    CHECK(mt_constructor_rejected(numeric_limits<double>::quiet_NaN(),
                                   1,0.05));
    CHECK(mt_constructor_rejected(numeric_limits<double>::infinity(),
                                   1,0.05));
    CHECK(mt_constructor_rejected(2.0,0,0.05));
    CHECK(mt_constructor_rejected(2.0,-1,0.05));
    CHECK(mt_constructor_rejected(2.0,1,0.0));
    CHECK(mt_constructor_rejected(2.0,1,-0.05));
    CHECK(mt_constructor_rejected(2.0,1,
                                   numeric_limits<double>::quiet_NaN()));
    CHECK(mt_constructor_rejected(2.0,1,
                                   numeric_limits<double>::infinity()));
    CHECK(mt_constructor_rejected(0.25,1,0.05));
    MTPowerSpectrumEngine boundary_mte(16,0.5,2,16,0.05);
    CHECK(boundary_mte.number_tapers()==1);
    MTPowerSpectrumEngine default_mte;
    MTPowerSpectrumEngine default_mte_copy(default_mte);
    CHECK(default_mte_copy.taper_length() == 0);
    CHECK(default_mte_copy.number_tapers() == 0);
    CHECK(default_mte_copy.fftsize() == 0);
    CHECK(default_mte_copy.time_bandwidth_product() == 0.0);
    CHECK(default_mte_copy.dt() == 1.0);
    CHECK(default_mte_copy.df() == 1.0);
    bool default_set_df_rejected(false);
    try {
      default_mte.set_df(0.05);
    } catch (MsPASSError &err) {
      default_set_df_rejected=true;
    }
    CHECK(default_set_df_rejected);
    CHECK(default_mte.dt()==1.0);
    CHECK(default_mte.df()==1.0);
    bool mt_constructor_rejected_zero_winsize(false);
    try {
      MTPowerSpectrumEngine bad_mte(0,4.0,3,0);
    } catch (MsPASSError &err) {
      mt_constructor_rejected_zero_winsize=true;
    }
    CHECK(mt_constructor_rejected_zero_winsize);

    /* CNR owns two default MTPowerSpectrumEngine members.  Copying a default
       CNR object therefore exercises the same zero-resource copy path through
       the containing type. */
    CNRDeconEngine default_cnr;
    CNRDeconEngine default_cnr_copy(default_cnr);
    CHECK(default_cnr_copy.get_size() == default_cnr.get_size());
    CHECK(default_cnr_copy.get_shift() == default_cnr.get_shift());

    cout << "Testing serialization of MTPowerSpectrumEngine" <<endl;
    /* The power spectrum engine is a bit different but the tests are similar.  
       Main thing is has more methods to verify the serialization */
    MTPowerSpectrumEngine mte(512,4.0,3);
    const double original_mte_dt=mte.dt();
    const double original_mte_df=mte.df();
    const vector<double> invalid_sample_intervals={
        0.0,-1.0,numeric_limits<double>::quiet_NaN(),
        numeric_limits<double>::infinity()};
    for(const double bad_dt : invalid_sample_intervals) {
      bool rejected(false);
      try {
        mte.set_df(bad_dt);
      } catch (MsPASSError &err) {
        rejected=true;
      }
      CHECK(rejected);
      CHECK(mte.dt()==original_mte_dt);
      CHECK(mte.df()==original_mte_df);
    }
    CHECK(mte.set_df(0.05)>0.0);
    CHECK(mte.dt()==0.05);
    CHECK(mte.set_df(original_mte_dt)==original_mte_df);
    MTPowerSpectrumEngine mte_copy(mte);
    CHECK(mte.df() == mte_copy.df());
    CHECK(mte.taper_length() == mte_copy.taper_length());
    CHECK(mte.time_bandwidth_product() ==
           mte_copy.time_bandwidth_product());
    CHECK(mte.number_tapers() == mte_copy.number_tapers());
    CHECK(mte.fftsize() == mte_copy.fftsize());
    CHECK(mte.dt() == mte_copy.dt());
    vector<double> mt_power_input(mte.taper_length());
    for(size_t i=0;i<mt_power_input.size();++i) {
      const double sample=static_cast<double>(i);
      mt_power_input[i]=sin(0.03*sample)+0.25*cos(0.11*sample);
    }
    const vector<double> mt_power(mte.apply(mt_power_input));
    const vector<double> mt_power_copy(mte_copy.apply(mt_power_input));
    CHECK(mt_power.size() == mt_power_copy.size());
    for(size_t i=0;i<mt_power.size();++i)
      CHECK(abs(mt_power[i]-mt_power_copy[i]) <=
             1.0e-12*(1.0+max(abs(mt_power[i]),abs(mt_power_copy[i]))));
    save_data<MTPowerSpectrumEngine>(mte);
    MTPowerSpectrumEngine mte2(restore_data<MTPowerSpectrumEngine>());
    CHECK(mte.df() == mte2.df());
    CHECK(mte.taper_length() == mte2.taper_length());
    CHECK(mte.time_bandwidth_product() == mte2.time_bandwidth_product());
    CHECK(mte.number_tapers() == mte2.number_tapers());
    CHECK(mte.fftsize() == mte2.fftsize());
    CHECK(mte.dt() == mte2.dt());
    std::vector<double> f1,f2;
    f1 = mte.frequencies();
    f2 = mte2.frequencies();
    CHECK(f1.size() == f2.size());
    CHECK(f1[0] == f2[0]);
    int n=f1.size()-1;
    CHECK(f1[n]==f2[n]);
    MTPowerSpectrumEngine initialized_mte_target(256,3.0,2);
    restore_data_into(initialized_mte_target);
    CHECK(initialized_mte_target.fftsize()==mte.fftsize());
    CHECK(initialized_mte_target.taper_length()==mte.taper_length());
    const vector<double> initialized_mte_power(
        initialized_mte_target.apply(mt_power_input));
    CHECK(initialized_mte_power.size()==mt_power.size());
    for(size_t i=0;i<mt_power.size();++i)
      CHECK(abs(mt_power[i]-initialized_mte_power[i]) <=
             1.0e-12*(1.0+max(abs(mt_power[i]),
                              abs(initialized_mte_power[i]))));

    save_data(default_mte);
    bool zero_mte_archive_rejected(false);
    try {
      restore_data_into(initialized_mte_target);
    } catch (MsPASSError &err) {
      zero_mte_archive_rejected=true;
    }
    CHECK(zero_mte_archive_rejected);
    CHECK(initialized_mte_target.fftsize()==mte.fftsize());
    CHECK(initialized_mte_target.taper_length()==mte.taper_length());
    const vector<double> rollback_mte_power(
        initialized_mte_target.apply(mt_power_input));
    CHECK(rollback_mte_power.size()==mt_power.size());
    for(size_t i=0;i<mt_power.size();++i)
      CHECK(abs(mt_power[i]-rollback_mte_power[i]) <=
             1.0e-12*(1.0+max(abs(mt_power[i]),
                              abs(rollback_mte_power[i]))));
    /* CNRDeconEngine requires a different pf file for now.  Could put it in branch 
       for this test file but for now made a separate file. */
    cout << "Reading pf to create CNRDeconEngine instance"<<endl;
    AntelopePf pfcnr = pfread("./CNRDeconEngine.pf");
    AntelopePf invalid_mtp_pf(pfcnr);
    invalid_mtp_pf.put("time_bandwidth_product",0.0);
    bool cnr_rejected_invalid_mtp_tbp(false);
    try {
      CNRDeconEngine bad_cnr(invalid_mtp_pf);
    } catch (MsPASSError &err) {
      cnr_rejected_invalid_mtp_tbp=true;
    }
    CHECK(cnr_rejected_invalid_mtp_tbp);
    invalid_mtp_pf=pfcnr;
    invalid_mtp_pf.put("number_tapers",0L);
    bool cnr_rejected_invalid_mtp_ntapers(false);
    try {
      CNRDeconEngine bad_cnr(invalid_mtp_pf);
    } catch (MsPASSError &err) {
      cnr_rejected_invalid_mtp_ntapers=true;
    }
    CHECK(cnr_rejected_invalid_mtp_ntapers);
    cout << "Calling constructor for CNRDeconEngine"<<endl;
    CNRDeconEngine e1(pfcnr);
    cout << "Testing serialization writer"<<endl;
    save_data<CNRDeconEngine>(e1);
    /* this usage tests default constructor and operator= as a side benefit*/
    CNRDeconEngine e2;
    cout << "Testing serialization reader"<<endl;
    e2 = restore_data<CNRDeconEngine>();

    cout << "Testing initialized CNR copy construction and assignment" << endl;
    TimeSeries cnr_wavelet(make_cnr_wavelet());
    Seismogram cnr_datum(make_cnr_datum());
    PowerSpectrum cnr_noise(make_cnr_noise_spectrum());
    e1.initialize_inverse_operator(cnr_wavelet,cnr_noise);
    Seismogram source_result(e1.process(cnr_datum,cnr_noise));

    CNRDeconEngine copied(e1);
    assert_cnr_qc_equal(e1,copied);

    CNRDeconEngine assigned(pfcnr);
    Metadata longer_window(dynamic_cast<const Metadata&>(pfcnr));
    longer_window.put("deconvolution_data_window_start",-10.0);
    longer_window.put("deconvolution_data_window_end",80.0);
    longer_window.put("operator_nfft",8192);
    assigned.changeparameter(longer_window);
    CHECK(assigned.get_size()!=e1.get_size());
    CHECK(assigned.get_shift()!=e1.get_shift());
    assigned=e1;
    CHECK(assigned.get_size()==e1.get_size());
    CHECK(assigned.get_shift()==e1.get_shift());
    assert_cnr_qc_equal(e1,assigned);

    CNRDeconEngine& assigned_alias(assigned);
    assigned=assigned_alias;
    assert_cnr_qc_equal(e1,assigned);

    Seismogram copied_result(copied.process(cnr_datum,cnr_noise));
    Seismogram assigned_result(assigned.process(cnr_datum,cnr_noise));
    assert_seismogram_equal(source_result,copied_result);
    assert_seismogram_equal(source_result,assigned_result);
    assert_timeseries_equal(e1.actual_output(cnr_wavelet),
                            copied.actual_output(cnr_wavelet));
    assert_timeseries_equal(e1.actual_output(cnr_wavelet),
                            assigned.actual_output(cnr_wavelet));
    assert_timeseries_equal(e1.inverse_wavelet(cnr_wavelet,0.0),
                            copied.inverse_wavelet(cnr_wavelet,0.0));
    assert_timeseries_equal(e1.inverse_wavelet(cnr_wavelet,0.0),
                            assigned.inverse_wavelet(cnr_wavelet,0.0));
    assert_cnr_qc_equal(e1,copied);
    assert_cnr_qc_equal(e1,assigned);

} 
