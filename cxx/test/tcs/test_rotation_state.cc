#include "mspass/seismic/CoreSeismogram.h"
#include "mspass/utility/SphericalCoordinate.h"
#include <algorithm>
#include <array>
#include <cmath>
#include <iostream>
#include <sstream>
#include <stdexcept>

using mspass::seismic::CoreSeismogram;
using mspass::utility::dmatrix;
using mspass::utility::SphericalCoordinate;

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

bool NearlyEqual(const double left, const double right) {
  const double scale = std::max({1.0, std::abs(left), std::abs(right)});
  return std::abs(left - right) <= 1.0e-12 * scale;
}

CoreSeismogram MakeTestSeismogram() {
  CoreSeismogram result(2);
  result.set_dt(0.125);
  result.set_t0(-2.0);
  result.set_live();
  const double samples[3][2] = {{1.0, -4.0}, {2.0, 5.0}, {3.0, -6.0}};
  for (size_t component = 0; component < 3; ++component)
    for (size_t sample = 0; sample < 2; ++sample)
      result.u(component, sample) = samples[component][sample];
  return result;
}

void CheckMatrix(const dmatrix &actual,
                 const std::array<std::array<double, 3>, 3> &expected) {
  CHECK(actual.rows() == 3);
  CHECK(actual.columns() == 3);
  for (size_t row = 0; row < 3; ++row)
    for (size_t column = 0; column < 3; ++column)
      CHECK(NearlyEqual(actual(row, column), expected[row][column]));
}

void CheckSamples(const CoreSeismogram &actual, const CoreSeismogram &original,
                  const std::array<std::array<double, 3>, 3> &transform) {
  CHECK(actual.u.rows() == original.u.rows());
  CHECK(actual.u.columns() == original.u.columns());
  for (size_t sample = 0; sample < actual.u.columns(); ++sample) {
    for (size_t row = 0; row < 3; ++row) {
      double expected = 0.0;
      for (size_t column = 0; column < 3; ++column)
        expected += transform[row][column] * original.u(column, sample);
      CHECK(NearlyEqual(actual.u(row, sample), expected));
    }
  }
}

void CheckSameState(const CoreSeismogram &actual,
                    const CoreSeismogram &expected) {
  CHECK(actual.npts() == expected.npts());
  CHECK(NearlyEqual(actual.dt(), expected.dt()));
  CHECK(NearlyEqual(actual.t0(), expected.t0()));
  CHECK(actual.live() == expected.live());
  CHECK(actual.time_is_relative() == expected.time_is_relative());
  CHECK(actual.cardinal() == expected.cardinal());
  CHECK(actual.orthogonal() == expected.orthogonal());
  CHECK(actual.u.rows() == expected.u.rows());
  CHECK(actual.u.columns() == expected.u.columns());
  for (size_t row = 0; row < actual.u.rows(); ++row)
    for (size_t column = 0; column < actual.u.columns(); ++column)
      CHECK(NearlyEqual(actual.u(row, column), expected.u(row, column)));

  const dmatrix actual_transform(actual.get_transformation_matrix());
  const dmatrix expected_transform(expected.get_transformation_matrix());
  CHECK(actual_transform.rows() == expected_transform.rows());
  CHECK(actual_transform.columns() == expected_transform.columns());
  for (size_t row = 0; row < actual_transform.rows(); ++row)
    for (size_t column = 0; column < actual_transform.columns(); ++column)
      CHECK(NearlyEqual(actual_transform(row, column),
                        expected_transform(row, column)));
}

void TestRotation(
    const double theta,
    const std::array<std::array<double, 3>, 3> &expected_transform,
    const bool restore) {
  const CoreSeismogram original(MakeTestSeismogram());
  CoreSeismogram rotated(original);
  SphericalCoordinate direction;
  direction.radius = 1.0;
  direction.phi = 0.0;
  direction.theta = theta;

  rotated.rotate(direction);

  CheckMatrix(rotated.get_transformation_matrix(), expected_transform);
  CheckSamples(rotated, original, expected_transform);
  CHECK(!rotated.cardinal());
  CHECK(rotated.orthogonal());

  if (restore) {
    rotated.rotate_to_standard();
    CheckSameState(rotated, original);
  }
}

int main() {
  try {
    const std::array<std::array<double, 3>, 3> theta_zero = {
        {{{0.0, -1.0, 0.0}}, {{1.0, 0.0, 0.0}}, {{0.0, 0.0, 1.0}}}};
    const std::array<std::array<double, 3>, 3> theta_half_pi = {
        {{{0.0, -1.0, 0.0}}, {{0.0, 0.0, -1.0}}, {{1.0, 0.0, 0.0}}}};
    const std::array<std::array<double, 3>, 3> theta_pi = {
        {{{1.0, 0.0, 0.0}}, {{0.0, 1.0, 0.0}}, {{0.0, 0.0, -1.0}}}};

    TestRotation(0.0, theta_zero, false);
    TestRotation(M_PI_2, theta_half_pi, false);
    TestRotation(M_PI, theta_pi, true);
  } catch (const std::exception &error) {
    std::cerr << error.what() << std::endl;
    return 1;
  }
  return 0;
}
