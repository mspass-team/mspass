#ifndef __GID_DECON_UTIL_H__
#define __GID_DECON_UTIL_H__
#include "mspass/algorithms/TimeWindow.h"
#include "mspass/algorithms/deconvolution/ThreeCSpike.h"
#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/AntelopePf.h"
#include "mspass/utility/Metadata.h"
#include "mspass/utility/dmatrix.h"
#include <list>
#include <string>
#include <vector>

namespace mspass::algorithms::deconvolution {
IterDeconType ParseGIDDeconType(const mspass::utility::Metadata &md,
                                const std::string &caller);
double GetDoubleDefault(const mspass::utility::Metadata &md,
                        const std::string &key, const double default_value);
int GetIntDefault(const mspass::utility::Metadata &md, const std::string &key,
                  const int default_value);
bool GetBoolDefault(const mspass::utility::Metadata &md,
                    const std::string &key, const bool default_value);
void ValidateProbability(const double p, const std::string &key,
                         const std::string &caller);
void ValidatePositive(const double x, const std::string &key,
                      const std::string &caller);
void ValidateNonnegative(const double x, const std::string &key,
                         const std::string &caller);
void ValidatePositiveInteger(const int x, const std::string &key,
                             const std::string &caller);
void ValidateThreeComponentIndex(const int component, const std::string &key,
                                 const std::string &caller);
void PutPrefixedMetadata(mspass::utility::Metadata &target,
                         const mspass::utility::Metadata &source,
                         const std::string &prefix);
std::string AntelopePfToText(const mspass::utility::AntelopePf &pf,
                             const int indent = 0);
std::vector<double> ThreeCAmplitudes(mspass::utility::dmatrix &d);
void ValidateGIDLeafWindow(const mspass::utility::AntelopePf &mdleaf,
                           const mspass::algorithms::TimeWindow &fftwin,
                           const std::string &leaf_name,
                           const std::string &base_error);
void ValidateGIDLeafOperatorMetadata(
    const mspass::utility::Metadata &md,
    const mspass::algorithms::TimeWindow &fftwin, const double target_dt,
    const std::string &caller, const bool allow_noise_window_keys = false);
void ValidateExternalTimeSeriesSampleInterval(
    const mspass::seismic::TimeSeries &d, const double target_dt,
    const std::string &caller);
double FIRSelfOverlap(const std::vector<double> &fir, const int col0_i,
                      const int col0_j, const int ncols);
double FIRDataOverlap(const std::vector<double> &fir,
                      const mspass::seismic::CoreSeismogram &target,
                      const int component, const int col0);
std::vector<double> SolveDenseSystem(const std::vector<std::vector<double>> &a,
                                     const std::vector<double> &b,
                                     const std::string &caller);
void RefitSpikeAmplitudes(std::list<ThreeCSpike> &spikes,
                          const mspass::seismic::CoreSeismogram &target,
                          const std::vector<double> &actual_o_fir,
                          const int actual_o_0,
                          const double ridge_beta = 1.0e-10);
} // namespace mspass::algorithms::deconvolution
#endif
