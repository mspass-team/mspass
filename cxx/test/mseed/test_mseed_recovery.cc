#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include "libmseed.h"
#include "mspass/io/mseed_index.h"

using mspass::io::mseed_file_indexer;
using std::string;
using std::vector;

namespace {
void Check(const bool condition, const char *expression, const char *file,
           const int line) {
  if (!condition) {
    std::ostringstream message;
    message << file << ":" << line << ": check failed: " << expression;
    throw std::runtime_error(message.str());
  }
}
#define CHECK(expression) Check((expression), #expression, __FILE__, __LINE__)

class TempFiles {
public:
  string path(const string &label) {
    const string result = "/tmp/mspass_mseed_recovery_" +
                          std::to_string(static_cast<long>(getpid())) + "_" +
                          label;
    paths.push_back(result);
    return result;
  }
  ~TempFiles() {
    for (const auto &path : paths)
      std::remove(path.c_str());
  }

private:
  vector<string> paths;
};

void CollectRecord(char *record, int record_length, void *handler_data) {
  auto *records = static_cast<vector<vector<char>> *>(handler_data);
  records->emplace_back(record, record + record_length);
}

vector<char> PackRecord(const string &sid, const nstime_t starttime,
                        const double samprate, const int64_t sample_count) {
  MS3Record *msr = msr3_init(nullptr);
  CHECK(msr != nullptr);
  CHECK(sid.size() < sizeof(msr->sid));
  std::strcpy(msr->sid, sid.c_str());
  msr->formatversion = 3;
  msr->reclen = 256;
  msr->pubversion = 1;
  msr->starttime = starttime;
  msr->samprate = samprate;
  msr->encoding = DE_INT32;
  vector<int32_t> samples(static_cast<size_t>(sample_count), 1);
  msr->samplecnt = sample_count;
  msr->numsamples = sample_count;
  msr->datasamples = samples.data();
  msr->datasize = samples.size() * sizeof(int32_t);
  msr->sampletype = 'i';

  vector<vector<char>> records;
  int64_t packed_samples(0);
  const int packed_records = msr3_pack(msr, CollectRecord, &records,
                                       &packed_samples, MSF_FLUSHDATA, 0);
  msr->datasamples = nullptr;
  msr->datasize = 0;
  msr3_free(&msr);
  CHECK(packed_records == 1);
  CHECK(packed_samples == sample_count);
  CHECK(records.size() == 1);
  return records.front();
}

string WriteParts(TempFiles &files, const string &label,
                  const vector<vector<char>> &parts) {
  const string path = files.path(label);
  std::ofstream output(path, std::ios::binary);
  CHECK(output.is_open());
  for (const auto &part : parts)
    output.write(part.data(), static_cast<std::streamsize>(part.size()));
  output.close();
  CHECK(output.good());
  return path;
}

void TestHalfSampleBoundary(TempFiles &files) {
  const string sid("XFDSN:XX_GAP_00_B_H_Z");
  const nstime_t base(1600000000000000000LL);
  const int64_t npts(4);
  const double rate(100.0);
  const nstime_t duration = npts * NSTMODULUS / 100;
  const nstime_t half_sample = NSTMODULUS / 200;
  const nstime_t second_start = base + duration + half_sample;
  const nstime_t third_start = second_start + duration + half_sample + 1;
  const vector<vector<char>> records{PackRecord(sid, base, rate, npts),
                                     PackRecord(sid, second_start, rate, npts),
                                     PackRecord(sid, third_start, rate, npts)};
  const auto path = WriteParts(files, "half_sample", records);

  const auto result = mseed_file_indexer(path, true, true);
  CHECK(result.first.size() == 2);
  CHECK(result.first[0].npts == 2 * npts + 1);
  CHECK(result.first[0].foff == 0);
  CHECK(result.first[0].nbytes == records[0].size() + records[1].size());
  CHECK(result.first[1].foff == result.first[0].nbytes);
  CHECK(result.first[1].nbytes == records[2].size());
  CHECK(result.second.size() == 1);

  const auto unsplit = mseed_file_indexer(path, false, false);
  CHECK(unsplit.first.size() == 1);
  CHECK(unsplit.first[0].npts == 3 * npts + 1);
}

void TestGapUsesFullTimeGrid(TempFiles &files) {
  const string sid("XFDSN:XX_GAP_00_B_H_Z");
  const nstime_t base(1610000000000000000LL);
  const int64_t npts(4);
  const double rate(20.0);
  const int64_t missing_samples(3);
  const auto first = PackRecord(sid, base, rate, npts);
  const nstime_t second_start =
      base + (npts + missing_samples) * NSTMODULUS / 20;
  const auto second = PackRecord(sid, second_start, rate, npts);
  const auto path = WriteParts(files, "gap", {first, second});

  const auto combined = mseed_file_indexer(path, false, false);
  CHECK(combined.first.size() == 1);
  const auto &index = combined.first.front();
  CHECK(index.npts == 2 * npts + missing_samples);
  CHECK(index.nbytes == first.size() + second.size());
  CHECK(std::fabs(index.endtime -
                  (index.starttime + static_cast<double>(index.npts - 1) /
                                         index.samprate)) < 1.0e-12);
  const double data_end = MS_NSTIME2EPOCH(
      static_cast<double>(second_start + (npts - 1) * NSTMODULUS / 20));
  CHECK(std::fabs(index.endtime - data_end) < 1.0e-6);

  const auto segmented = mseed_file_indexer(path, true, false);
  CHECK(segmented.first.size() == 2);
  CHECK(segmented.first[0].npts == npts);
  CHECK(segmented.first[1].npts == npts);
}

void TestSampleRateTolerance(TempFiles &files) {
  const string sid("XFDSN:XX_DRIFT_00_B_H_Z");
  const nstime_t base(1620000000000000000LL);
  const int64_t npts(10);
  const double nominal_rate(100.0);
  const double drifted_rate(100.005);
  const auto first = PackRecord(sid, base, nominal_rate, npts);
  const auto second =
      PackRecord(sid, base + npts * NSTMODULUS / 100, drifted_rate, npts);
  const auto path = WriteParts(files, "rate_drift", {first, second});

  const auto default_tolerance = mseed_file_indexer(path, false, false);
  CHECK(default_tolerance.first.size() == 1);
  CHECK(default_tolerance.first[0].samprate == nominal_rate);
  CHECK(default_tolerance.first[0].npts == 2 * npts);

  const auto strict_tolerance = mseed_file_indexer(path, false, false, 1.0e-6);
  CHECK(strict_tolerance.first.size() == 2);
  CHECK(strict_tolerance.first[0].samprate == nominal_rate);
  CHECK(strict_tolerance.first[1].samprate == drifted_rate);
}
} // namespace

int main() {
  try {
    TempFiles files;
    TestHalfSampleBoundary(files);
    TestGapUsesFullTimeGrid(files);
    TestSampleRateTolerance(files);
    return 0;
  } catch (const std::exception &error) {
    std::fprintf(stderr, "%s\n", error.what());
  }
  return 1;
}
