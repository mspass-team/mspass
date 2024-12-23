#include <iostream>
#include <fstream>
#include <string>

#include <boost/archive/tmpdir.hpp>

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
#include "mspass/algorithms/deconvolution/WaterLevelDecon.h"
using namespace std;
using namespace mspass::algorithms::deconvolution;
using mspass::utility::AntelopePf;
using mspass::utility::pfread;

const std::string test_fname("serialization_output");
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
bool shaping_wavelets_match(ShapingWavelet& s1,
         ShapingWavelet& s2)
{
  if(s1.freq_bin_size()!=s2.freq_bin_size())return false;
  if(s1.sample_interval() != s2.sample_interval())return false;
  if(s1.type() != s2.type())return false;
  if(s1.size() != s2.size())return false;
  /* This may fail if precision is not sufficient*/
  assert(s1.size()>0);  // next pointless if not true
  ComplexArray *w1 = s1.wavelet();
  ComplexArray *w2 = s2.wavelet();
  cout << "Size of wavelts being tested = "<<s1.size()<<endl;
  if(w1->rms() != w2->rms()) return false;
  cout << "rms of s1 wavelet in test="<<w1->rms()<<endl;
  return true;
}

int main(int argc, char **argv)
{
    cout << "Testing serialization of ComplexArray" <<endl;
    ComplexArray z(10);
    std::vector<double> x;
    for(auto i=0;i<10;++i) x.push_back((double)i);
    z = ComplexArray(10,x);
    save_data<ComplexArray>(z);
    ComplexArray z2;
    z2 = restore_data<ComplexArray>();
    assert (z.size() == z2.size());
    for(auto i=0;i<10;++i) assert(z[i] == z2[i]);
    cout << "Testing serialization of FFTDeconOperator" <<endl;
    //AntelopePf pf("RFdeconProcessor.pf");
    AntelopePf pf=pfread("./decon_serialization/RFdeconProcessor.pf");
    WaterLevelDecon wl(pf.get_branch("WaterLevel"));
    FFTDeconOperator fftdo(dynamic_cast<FFTDeconOperator&>(wl));
    save_data<FFTDeconOperator>(fftdo);
    FFTDeconOperator fftdo2=restore_data<FFTDeconOperator>();
    assert(fftdo.get_size() == fftdo2.get_size());
    assert(fftdo.operator_shift() == fftdo2.operator_shift());
    cout << "Testing serialization of WaterLevelDecon" <<endl;
    save_data<WaterLevelDecon>(wl);
    WaterLevelDecon wl2(restore_data<WaterLevelDecon>());
    // compare the sahping wavelets
    cout << "Testng if shaping wavelets were serialized corectly"<<endl;
    ShapingWavelet sw1 = wl.get_shaping_wavelet();
    ShapingWavelet sw2 = wl2.get_shaping_wavelet();
    assert(shaping_wavelets_match(sw1,sw2));
    cout << "Testing serialization of LeastSquareDecon" <<endl;
    LeastSquareDecon lsd(pf.get_branch("LeastSquare"));
    save_data<LeastSquareDecon>(lsd);
    LeastSquareDecon lsd2(restore_data<LeastSquareDecon>());
    cout << "Testing serialization of MultiTaperXcorDecon" <<endl;
    MultiTaperXcorDecon mtd(pf.get_branch("MultiTaperXcor"));
    save_data<MultiTaperXcorDecon>(mtd);
    MultiTaperXcorDecon mtd2(restore_data<MultiTaperXcorDecon>());
    /* There are limited methods we can use for assert with these empty operators*/
    assert(mtd.get_taperlen() == mtd2.get_taperlen());
    assert(mtd.get_number_tapers() == mtd2.get_number_tapers());
    assert(mtd.get_time_bandwidth_product() == mtd2.get_time_bandwidth_product());
    cout << "Testing serialization of MultiTaperSpecDivDecon" <<endl;
    MultiTaperSpecDivDecon mtd3(pf.get_branch("MultiTaperSpecDiv"));
    save_data<MultiTaperSpecDivDecon>(mtd3);
    MultiTaperSpecDivDecon mtd4(restore_data<MultiTaperSpecDivDecon>());
    assert(mtd3.get_taperlen() == mtd4.get_taperlen());
    assert(mtd3.get_number_tapers() == mtd4.get_number_tapers());
    assert(mtd3.get_time_bandwidth_product() == mtd4.get_time_bandwidth_product());
    cout << "Testing serialization of MTPowerSpectrumEngine" <<endl;
    /* The power spectrum engine is a bit different but the tests are similar.  
       Main thing is has more methods to verify the serialization */
    MTPowerSpectrumEngine mte(512,4.0,3);
    save_data<MTPowerSpectrumEngine>(mte);
    MTPowerSpectrumEngine mte2(restore_data<MTPowerSpectrumEngine>());
    assert(mte.df() == mte2.df());
    assert(mte.taper_length() == mte2.taper_length());
    assert(mte.time_bandwidth_product() == mte2.time_bandwidth_product());
    assert(mte.number_tapers() == mte2.number_tapers());
    assert(mte.fftsize() == mte2.fftsize());
    assert(mte.dt() == mte2.dt());
    std::vector<double> f1,f2;
    f1 = mte.frequencies();
    f2 = mte2.frequencies();
    assert(f1.size() == f2.size());
    assert(f1[0] == f2[0]);
    int n=f1.size()-1;
    assert(f1[n]==f2[n]);
    /* CNRDeconEngine requires a different pf file for now.  Could put it in branch 
       for this test file but for now made a separate file. */
    cout << "Reading pf to create CNRDeconEngine instance"<<endl;
    AntelopePf pfcnr = pfread("./decon_serialization/CNRDeconEngine.pf");
    //AntelopePf pfcnr("CNRDeconEngine.pf");
    cout << "Calling constructor for CNRDeconEngine"<<endl;
    CNRDeconEngine e1(pfcnr);
    cout << "Testing serialization writer"<<endl;
    save_data<CNRDeconEngine>(e1);
    /* this usage tests default constructor and operator= as a side benefit*/
    CNRDeconEngine e2;
    cout << "Testing serialization reader"<<endl;
    e2 = restore_data<CNRDeconEngine>();

} 
