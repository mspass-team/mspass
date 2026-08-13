#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include "libmseed.h"
#include "mspass/io/mseed_index.h"
#include "mspass/utility/MsPASSError.h"

using mspass::io::mseed_file_indexer;
using mspass::io::mseed_index;
using mspass::utility::ErrorSeverity;
using mspass::utility::MsPASSError;
using std::string;
using std::vector;

namespace {
void CheckCondition(const bool condition, const char *expression,
                    const char *file, const int line) {
  if (!condition) {
    std::ostringstream message;
    message << file << ":" << line << ": test check failed: " << expression;
    throw std::runtime_error(message.str());
  }
}
#define CHECK(...)                                                             \
  CheckCondition(static_cast<bool>((__VA_ARGS__)), #__VA_ARGS__, __FILE__,     \
                 __LINE__)

void CheckClose(const double actual, const double expected,
                const double tolerance, const char *expression,
                const char *file, const int line) {
  if (std::fabs(actual - expected) > tolerance) {
    std::ostringstream message;
    message << file << ":" << line << ": test check failed: " << expression
            << " (actual=" << std::setprecision(17) << actual
            << ", expected=" << expected << ")";
    throw std::runtime_error(message.str());
  }
}
#define CHECK_CLOSE(actual, expected, tolerance)                               \
  CheckClose((actual), (expected), (tolerance), #actual " ~= " #expected,      \
             __FILE__, __LINE__)

class TempFiles {
public:
  string make_path(const string &label) {
    const string path = "/tmp/mspass_mseed_index_" +
                        std::to_string(static_cast<long>(getpid())) + "_" +
                        std::to_string(paths.size()) + "_" + label;
    paths.push_back(path);
    return path;
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
  vector<int32_t> samples(static_cast<size_t>(sample_count));
  for (int64_t i = 0; i < sample_count; ++i)
    samples[static_cast<size_t>(i)] = static_cast<int32_t>(i + 1);
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

string WriteRecords(TempFiles &temp_files, const string &label,
                    const vector<vector<char>> &records) {
  const string path = temp_files.make_path(label);
  std::ofstream output(path, std::ios::binary);
  CHECK(output.is_open());
  for (const auto &record : records)
    output.write(record.data(), static_cast<std::streamsize>(record.size()));
  output.close();
  CHECK(output.good());
  return path;
}

void CheckEndtimeFormula(const mseed_index &index) {
  const double expected =
      index.starttime + static_cast<double>(index.npts - 1) / index.samprate;
  CHECK(index.endtime == expected);
}

size_t CountLines(const string &text) {
  return static_cast<size_t>(std::count(text.begin(), text.end(), '\n'));
}

size_t CountOccurrences(const string &text, const string &needle) {
  size_t count(0), position(0);
  while ((position = text.find(needle, position)) != string::npos) {
    ++count;
    position += needle.size();
  }
  return count;
}

bool RateChanges(const double first, const double second) {
  const long double lhs = std::fabs(static_cast<long double>(second) - first);
  const long double rhs =
      1.0e-12L * std::max(std::fabs(static_cast<long double>(first)),
                          std::fabs(static_cast<long double>(second)));
  return lhs > rhs;
}

void TestTimeTearBoundaries(TempFiles &temp_files) {
  const vector<int> sample_rates{1, 20, 50, 100, 500};
  const string sid("XFDSN:XX_TEST_00_B_H_Z");
  const int64_t packet_npts(4);
  size_t test_number(0);
  for (const int rate : sample_rates) {
    const nstime_t base =
        1600000000000000000LL +
        static_cast<nstime_t>(test_number++) * 1000000000000LL;
    const nstime_t packet_duration =
        packet_npts * static_cast<nstime_t>(NSTMODULUS) / rate;
    const nstime_t half_sample = static_cast<nstime_t>(NSTMODULUS) / (2 * rate);
    const nstime_t second_start = base + packet_duration + half_sample;
    const nstime_t third_start =
        second_start + packet_duration + half_sample + 1;
    vector<vector<char>> records{
        PackRecord(sid, base, rate, packet_npts),
        PackRecord(sid, second_start, rate, packet_npts),
        PackRecord(sid, third_start, rate, packet_npts)};
    const string path = WriteRecords(
        temp_files, "time_boundary_" + std::to_string(rate), records);

    std::ostringstream diagnostics;
    std::streambuf *saved_cerr = std::cerr.rdbuf(diagnostics.rdbuf());
    std::pair<vector<mseed_index>, mspass::utility::ErrorLogger> result;
    try {
      result = mseed_file_indexer(path, true, true);
    } catch (...) {
      std::cerr.rdbuf(saved_cerr);
      throw;
    }
    std::cerr.rdbuf(saved_cerr);

    CHECK(result.second.size() == 0);
    CHECK(result.first.size() == 2);
    const auto &first = result.first[0];
    const auto &second = result.first[1];
    CHECK(first.net == "XX");
    CHECK(first.sta == "TEST");
    CHECK(first.loc == "00");
    CHECK(first.chan == "BHZ");
    CHECK(first.foff == 0);
    CHECK(first.nbytes == records[0].size() + records[1].size());
    CHECK(first.npts == static_cast<size_t>(2 * packet_npts));
    CHECK(first.samprate == rate);
    CHECK_CLOSE(first.starttime, MS_NSTIME2EPOCH(static_cast<double>(base)),
                0.0);
    CHECK_CLOSE(first.last_packet_time,
                MS_NSTIME2EPOCH(static_cast<double>(second_start)), 0.0);
    CheckEndtimeFormula(first);

    CHECK(second.foff == first.nbytes);
    CHECK(second.nbytes == records[2].size());
    CHECK(second.npts == static_cast<size_t>(packet_npts));
    CHECK(second.samprate == rate);
    CHECK_CLOSE(second.starttime,
                MS_NSTIME2EPOCH(static_cast<double>(third_start)), 0.0);
    CHECK(second.last_packet_time == second.starttime);
    CheckEndtimeFormula(second);

    const string output = diagnostics.str();
    CHECK(CountLines(output) == 1);
    CHECK(output.find("time tear at packet 3") != string::npos);
    CHECK(output.find(sid) != string::npos);
    CHECK(output.find("previous expected time") != string::npos);
    CHECK(output.find("actual start") != string::npos);

    std::ostringstream quiet_diagnostics;
    saved_cerr = std::cerr.rdbuf(quiet_diagnostics.rdbuf());
    try {
      result = mseed_file_indexer(path, true, false);
    } catch (...) {
      std::cerr.rdbuf(saved_cerr);
      throw;
    }
    std::cerr.rdbuf(saved_cerr);
    CHECK(result.first.size() == 2);
    CHECK(quiet_diagnostics.str().empty());

    const auto unsplit = mseed_file_indexer(path, false, true);
    CHECK(unsplit.first.size() == 1);
    CHECK(unsplit.first[0].npts == static_cast<size_t>(3 * packet_npts));
  }
}

void TestSampleRateBoundaries(TempFiles &temp_files) {
  const string sid("XFDSN:XX_RATE_00_B_H_Z");
  const double base_rate(100.0);
  double boundary_rate(base_rate);
  double beyond_rate(base_rate);
  for (int i = 0; i < 100000; ++i) {
    const double candidate =
        std::nextafter(boundary_rate, std::numeric_limits<double>::infinity());
    if (RateChanges(base_rate, candidate)) {
      beyond_rate = candidate;
      break;
    }
    boundary_rate = candidate;
  }
  CHECK(boundary_rate > base_rate);
  CHECK(!RateChanges(base_rate, boundary_rate));
  CHECK(RateChanges(base_rate, beyond_rate));

  const nstime_t base(1610000000000000000LL);
  const int64_t packet_npts(4);
  const nstime_t second_start =
      base + packet_npts * static_cast<nstime_t>(NSTMODULUS) / 100;
  vector<vector<char>> at_boundary{
      PackRecord(sid, base, base_rate, packet_npts),
      PackRecord(sid, second_start, boundary_rate, packet_npts)};
  const string boundary_path =
      WriteRecords(temp_files, "rate_boundary", at_boundary);
  const auto boundary_result = mseed_file_indexer(boundary_path, false, false);
  CHECK(boundary_result.first.size() == 1);
  CHECK(boundary_result.first[0].npts == static_cast<size_t>(2 * packet_npts));
  CHECK(boundary_result.first[0].samprate == base_rate);
  CHECK(boundary_result.first[0].nbytes ==
        at_boundary[0].size() + at_boundary[1].size());
  CheckEndtimeFormula(boundary_result.first[0]);

  vector<vector<char>> beyond_boundary{
      PackRecord(sid, base, base_rate, packet_npts),
      PackRecord(sid, second_start, beyond_rate, packet_npts)};
  const string beyond_path =
      WriteRecords(temp_files, "rate_beyond", beyond_boundary);
  const auto beyond_result = mseed_file_indexer(beyond_path, false, false);
  CHECK(beyond_result.first.size() == 2);
  CHECK(beyond_result.first[0].foff == 0);
  CHECK(beyond_result.first[0].nbytes == beyond_boundary[0].size());
  CHECK(beyond_result.first[0].samprate == base_rate);
  CHECK(beyond_result.first[1].foff == beyond_boundary[0].size());
  CHECK(beyond_result.first[1].nbytes == beyond_boundary[1].size());
  CHECK(beyond_result.first[1].samprate == beyond_rate);
  CHECK(beyond_result.first[0].npts == static_cast<size_t>(packet_npts));
  CHECK(beyond_result.first[1].npts == static_cast<size_t>(packet_npts));
  CheckEndtimeFormula(beyond_result.first[0]);
  CheckEndtimeFormula(beyond_result.first[1]);
}

void TestOneDiagnosticPerTimeTear(TempFiles &temp_files) {
  const string sid("XFDSN:XX_VERBOSE_00_B_H_Z");
  const double rate(20.0);
  const int64_t packet_npts(4);
  const nstime_t base(1615000000000000000LL);
  const nstime_t packet_duration = 200000000LL;
  const nstime_t beyond_half_sample = 25000001LL;
  const nstime_t second_start = base + packet_duration + beyond_half_sample;
  const nstime_t third_start =
      second_start + packet_duration + beyond_half_sample;
  const vector<vector<char>> records{
      PackRecord(sid, base, rate, packet_npts),
      PackRecord(sid, second_start, rate, packet_npts),
      PackRecord(sid, third_start, rate, packet_npts)};
  const string path = WriteRecords(temp_files, "verbose_tears", records);

  std::ostringstream diagnostics;
  std::streambuf *saved_cerr = std::cerr.rdbuf(diagnostics.rdbuf());
  std::pair<vector<mseed_index>, mspass::utility::ErrorLogger> result;
  try {
    result = mseed_file_indexer(path, true, true);
  } catch (...) {
    std::cerr.rdbuf(saved_cerr);
    throw;
  }
  std::cerr.rdbuf(saved_cerr);
  CHECK(result.first.size() == 3);
  CHECK(CountLines(diagnostics.str()) == 2);
  CHECK(CountOccurrences(diagnostics.str(), "time tear at packet") == 2);
  CHECK(diagnostics.str().find("time tear at packet 2") != string::npos);
  CHECK(diagnostics.str().find("time tear at packet 3") != string::npos);
}

void TestSidAndRecordBounds(TempFiles &temp_files) {
  const nstime_t base(1620000000000000000LL);
  vector<vector<char>> records{
      PackRecord("XFDSN:AA_FIRST_01_L_H_Z", base, 20.0, 3),
      PackRecord("XFDSN:BB_SECOND_02_H_H_N", base + 150000000LL, 20.0, 7)};
  CHECK(records[0].size() != records[1].size());
  const string path = WriteRecords(temp_files, "sid_change", records);
  const auto result = mseed_file_indexer(path, true, false);
  CHECK(result.first.size() == 2);
  CHECK(result.first[0].net == "AA");
  CHECK(result.first[0].sta == "FIRST");
  CHECK(result.first[0].loc == "01");
  CHECK(result.first[0].chan == "LHZ");
  CHECK(result.first[0].foff == 0);
  CHECK(result.first[0].nbytes == records[0].size());
  CHECK(result.first[0].npts == 3);
  CHECK(result.first[1].net == "BB");
  CHECK(result.first[1].sta == "SECOND");
  CHECK(result.first[1].loc == "02");
  CHECK(result.first[1].chan == "HHN");
  CHECK(result.first[1].foff == records[0].size());
  CHECK(result.first[1].nbytes == records[1].size());
  CHECK(result.first[1].npts == 7);
  CheckEndtimeFormula(result.first[0]);
  CheckEndtimeFormula(result.first[1]);
}

void ExpectInvalidParse(const string &path) {
  bool threw(false);
  try {
    static_cast<void>(mseed_file_indexer(path, true, false));
  } catch (const MsPASSError &error) {
    threw = true;
    CHECK(error.severity() == ErrorSeverity::Invalid);
    CHECK(string(error.what()).find("ms3_readmsr_r") != string::npos);
  }
  CHECK(threw);
}

void TestEmptyAndCorruption(TempFiles &temp_files) {
  const string empty_path = WriteRecords(temp_files, "empty", {});
  const auto empty_result = mseed_file_indexer(empty_path, true, false);
  CHECK(empty_result.first.empty());
  CHECK(empty_result.second.size() == 0);

  const vector<char> valid =
      PackRecord("XFDSN:XX_DAMAGE_00_B_H_Z", 1630000000000000000LL, 100.0, 8);
  vector<char> damaged(valid);
  damaged.back() ^= 0x01;
  const string before_path =
      WriteRecords(temp_files, "damage_before", {damaged, valid});
  const string after_path =
      WriteRecords(temp_files, "damage_after", {valid, damaged});
  ExpectInvalidParse(before_path);
  ExpectInvalidParse(after_path);

  vector<char> junk(64, static_cast<char>(0x7f));
  const string junk_before =
      WriteRecords(temp_files, "junk_before", {junk, valid});
  const string junk_after =
      WriteRecords(temp_files, "junk_after", {valid, junk});
  ExpectInvalidParse(junk_before);
  ExpectInvalidParse(junk_after);
}

void TestExistingFixture(const string &path) {
  const auto result = mseed_file_indexer(path, true, false);
  CHECK(result.second.size() == 0);
  CHECK(result.first.size() == 5);
  CHECK(result.first[0].sta == "E2000");
  CHECK(result.first[0].chan == "VHE");
  CHECK(result.first[0].foff == 0);
  CHECK(result.first[0].nbytes == 4096);
  CHECK(result.first[0].npts == 70);
  CHECK_CLOSE(result.first[0].samprate, 0.1, 0.0001);
  CHECK(result.first[1].sta == "E2000");
  CHECK(result.first[1].foff == 4096);
  CHECK(result.first[1].npts == 2434403);
  CHECK(result.first[2].sta == "A2000");
  CHECK(result.first[2].chan == "UHE");
  CHECK(result.first[2].foff == 1921024);
  CHECK(result.first[2].nbytes == 4096);
  CHECK(result.first[2].npts == 15);
  CHECK_CLOSE(result.first[2].samprate, 0.01, 0.0001);
  CHECK(result.first[3].sta == "A2000");
  CHECK(result.first[3].foff == 1925120);
  CHECK(result.first[3].npts == 47205);
  for (size_t i = 0; i < result.first.size(); ++i) {
    CHECK(result.first[i].nbytes > 0);
    CHECK(result.first[i].npts > 0);
    CHECK(result.first[i].samprate > 0.0);
    CheckEndtimeFormula(result.first[i]);
    if (i > 0)
      CHECK(result.first[i].foff ==
            result.first[i - 1].foff + result.first[i - 1].nbytes);
  }
}
} // namespace

int main(int argc, char **argv) {
  try {
    CHECK(argc == 2);
    TempFiles temp_files;
    TestTimeTearBoundaries(temp_files);
    TestSampleRateBoundaries(temp_files);
    TestOneDiagnosticPerTimeTear(temp_files);
    TestSidAndRecordBounds(temp_files);
    TestEmptyAndCorruption(temp_files);
    TestExistingFixture(argv[1]);
    std::cout << "MiniSEED index contract tests passed" << std::endl;
    return EXIT_SUCCESS;
  } catch (const std::exception &error) {
    std::cerr << error.what() << std::endl;
  } catch (...) {
    std::cerr << "Unknown exception in MiniSEED index contract test"
              << std::endl;
  }
  return EXIT_FAILURE;
}
