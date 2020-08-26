#include <math.h>
#include "mspass/utility/Metadata.h"
using namespace std;
using mspass::Metadata;
namespace mspass{
int ComputeTaperLength(const Metadata& md)
{
    try {
        double ts,te,dt;
        ts=md.get<double>("deconvolution_data_window_start");
        te=md.get<double>("deconvolution_data_window_end");
        dt=md.get<double>("target_sample_interval");
        return round((te-ts)/dt) + 1;
    } catch(...) {
        throw;
    };
}
}
