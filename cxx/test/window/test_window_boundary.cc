#include "mspass/algorithms/TimeWindow.h"
#include "mspass/algorithms/algorithms.h"
#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/seismic/CoreTimeSeries.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/ProcessingHistory.h"
#include <cmath>
#include <iostream>
#include <limits>
#include <string>

using mspass::algorithms::TimeWindow;
using mspass::algorithms::WindowData;
using mspass::seismic::CoreSeismogram;
using mspass::seismic::CoreTimeSeries;
using mspass::seismic::Seismogram;
using mspass::seismic::TimeSeries;
using mspass::utility::AtomicType;
using mspass::utility::ErrorSeverity;
using mspass::utility::MsPASSError;
using mspass::utility::NodeData;

namespace {
int failure_count{0};

void check(const bool condition, const std::string &message) {
  if (!condition) {
    std::cerr << message << '\n';
    ++failure_count;
  }
}

void populate(CoreTimeSeries &data) {
  for (std::size_t sample = 0; sample < data.npts(); ++sample)
    data.s[sample] = static_cast<double>(sample + 1);
}

void populate(CoreSeismogram &data) {
  for (std::size_t sample = 0; sample < data.npts(); ++sample)
    for (std::size_t component = 0; component < 3; ++component)
      data.u(component, sample) =
          100.0 * static_cast<double>(component + 1) + sample;
}

void check_samples(const CoreTimeSeries &data, const std::string &label) {
  for (std::size_t sample = 0; sample < data.npts(); ++sample)
    check(data.s[sample] == static_cast<double>(sample + 1),
          label + " changed scalar sample " + std::to_string(sample));
}

void check_samples(const CoreSeismogram &data, const std::string &label) {
  for (std::size_t sample = 0; sample < data.npts(); ++sample)
    for (std::size_t component = 0; component < 3; ++component)
      check(data.u(component, sample) ==
                100.0 * static_cast<double>(component + 1) + sample,
            label + " changed 3C sample " + std::to_string(sample));
}

void check_empty_samples(const TimeSeries &data, const std::string &label) {
  check(data.s.empty(), label + " retained scalar samples");
}

void check_empty_samples(const Seismogram &data, const std::string &label) {
  check(data.u.columns() == 0, label + " retained 3C samples");
}

bool same_node(const NodeData &left, const NodeData &right) {
  return left.status == right.status && left.uuid == right.uuid &&
         left.type == right.type && left.stage == right.stage &&
         left.algorithm == right.algorithm && left.algid == right.algid;
}

template <class History>
bool same_history_graph(const History &left, const History &right) {
  const auto left_nodes = left.get_nodes();
  const auto right_nodes = right.get_nodes();
  if (left_nodes.size() != right_nodes.size())
    return false;
  auto left_node = left_nodes.begin();
  auto right_node = right_nodes.begin();
  for (; left_node != left_nodes.end(); ++left_node, ++right_node)
    if (left_node->first != right_node->first ||
        !same_node(left_node->second, right_node->second))
      return false;
  return true;
}

template <class Waveform>
void check_success(const Waveform &parent, const double end,
                   const std::string &label) {
  try {
    const auto result = WindowData(parent, TimeWindow(parent.t0(), end));
    check(result.live(), label + " returned dead data");
    check(result.npts() == parent.npts(), label + " changed sample count");
    check(result.t0() == parent.t0(), label + " changed start time");
    check(result.endtime() == parent.endtime(), label + " changed end time");
    check(result.get_string("boundary_marker") == "preserve-me",
          label + " did not preserve metadata");
    check_samples(result, label);
  } catch (const std::exception &error) {
    check(false, label + " unexpectedly threw: " + error.what());
  }
}

template <class Waveform>
void check_core_rejection(const Waveform &parent, const double end,
                          const std::string &label) {
  bool rejected{false};
  try {
    static_cast<void>(WindowData(parent, TimeWindow(parent.t0(), end)));
  } catch (const MsPASSError &error) {
    rejected = true;
    check(error.severity() == ErrorSeverity::Invalid,
          label + " threw with non-Invalid severity");
  } catch (const std::exception &error) {
    rejected = true;
    check(false, label + " threw the wrong exception: " + error.what());
  }
  check(rejected, label + " did not reject the out-of-grid endpoint");
  check(parent.live(), label + " mutated the parent live state");
  check(parent.npts() == 5, label + " mutated the parent sample count");
  check_samples(parent, label + " parent");
}

template <class Waveform>
void check_history_success(const Waveform &parent, const double end,
                           const std::string &label) {
  try {
    const auto result = WindowData(parent, TimeWindow(parent.t0(), end));
    check(result.live(), label + " returned dead data");
    check(result.npts() == parent.npts(), label + " changed sample count");
    check(result.t0() == parent.t0(), label + " changed start time");
    check(result.endtime() == parent.endtime(), label + " changed end time");
    check(result.get_string("boundary_marker") == "preserve-me",
          label + " did not preserve metadata");
    check(result.jobname() == parent.jobname(),
          label + " changed history jobname");
    check(result.jobid() == parent.jobid(), label + " changed history jobid");
    check(same_node(result.current_nodedata(), parent.current_nodedata()),
          label + " changed current history node");
    check(same_history_graph(result, parent),
          label + " changed the history graph");
    check(result.elog.size() == parent.elog.size(),
          label + " changed the error log");
    check_samples(result, label);
  } catch (const std::exception &error) {
    check(false, label + " unexpectedly threw: " + error.what());
  }
}

template <class Waveform>
void check_history_rejection(const Waveform &parent, const double end,
                             const std::string &label) {
  const auto result = WindowData(parent, TimeWindow(parent.t0(), end));
  check(result.dead(), label + " did not return dead data");
  check(result.npts() == 0, label + " did not clear npts");
  check_empty_samples(result, label);
  check(result.get_string("boundary_marker") == "preserve-me",
        label + " did not preserve metadata");
  check(result.t0() == parent.t0(), label + " changed start time metadata");
  check(result.dt() == parent.dt(),
        label + " changed sample interval metadata");
  check(result.jobname() == parent.jobname(),
        label + " changed history jobname");
  check(result.jobid() == parent.jobid(), label + " changed history jobid");
  check(result.is_empty() == parent.is_empty(),
        label + " changed history state");
  check(result.stage() == parent.stage(),
        label + " changed history stage count");
  check(same_node(result.current_nodedata(), parent.current_nodedata()),
        label + " changed current history node");
  check(same_history_graph(result, parent),
        label + " changed the history graph");
  check(result.elog.size() == parent.elog.size() + 1,
        label + " did not append exactly one log entry");
  int invalid_count{0};
  for (const auto &entry : result.elog.get_error_log())
    if (entry.badness == ErrorSeverity::Invalid)
      ++invalid_count;
  check(invalid_count == 1, label + " did not append one Invalid log entry");
  check(result.elog.get_error_log().back().badness == ErrorSeverity::Invalid,
        label + " final log entry is not Invalid");
  check(parent.live(), label + " mutated the parent live state");
  check(parent.npts() == 5, label + " mutated the parent sample count");
  check_samples(parent, label + " parent");
}

template <class Waveform> Waveform make_core_data() {
  Waveform data(5);
  data.set_t0(10.0);
  data.set_dt(0.5);
  data.set_live();
  data.put_string("boundary_marker", "preserve-me");
  populate(data);
  return data;
}

template <class Waveform>
void exercise_core_overload(const std::string &label) {
  const Waveform parent = make_core_data<Waveform>();
  const double final_sample = parent.endtime();
  const double half_sample_tie = final_sample + 0.5 * parent.dt();
  const double below_tie =
      std::nextafter(half_sample_tie, -std::numeric_limits<double>::infinity());

  check_success(parent, final_sample, label + " final sample");
  check_success(parent, below_tie, label + " below half-sample tie");
  check_core_rejection(parent, half_sample_tie,
                       label + " exact half-sample tie");
  check_core_rejection(parent, final_sample + parent.dt(),
                       label + " final sample plus dt");
}

template <class Waveform>
void exercise_history_overload(const AtomicType type,
                               const std::string &label) {
  Waveform parent = make_core_data<Waveform>();
  parent.set_jobname("boundary-job");
  parent.set_jobid("boundary-job-id");
  parent.set_as_origin("boundary-source", "source-id", "boundary-uuid", type,
                       true);
  parent.elog.log_error("fixture", "preexisting informational entry",
                        ErrorSeverity::Informational);
  const double final_sample = parent.endtime();
  const double half_sample_tie = final_sample + 0.5 * parent.dt();
  const double below_tie =
      std::nextafter(half_sample_tie, -std::numeric_limits<double>::infinity());

  check_history_success(parent, final_sample, label + " final sample");
  check_history_success(parent, below_tie, label + " below half-sample tie");
  check_history_rejection(parent, half_sample_tie,
                          label + " exact half-sample tie");
  check_history_rejection(parent, final_sample + parent.dt(),
                          label + " final sample plus dt");
}
} // namespace

int main() {
  exercise_core_overload<CoreTimeSeries>("CoreTimeSeries");
  exercise_core_overload<CoreSeismogram>("CoreSeismogram");
  exercise_history_overload<TimeSeries>(AtomicType::TIMESERIES, "TimeSeries");
  exercise_history_overload<Seismogram>(AtomicType::SEISMOGRAM, "Seismogram");
  return failure_count == 0 ? 0 : 1;
}
