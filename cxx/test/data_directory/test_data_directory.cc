#include "mspass/utility/utility.h"
#include <cstdlib>
#include <iostream>
#include <string>

int main() {
  const char *home = std::getenv("MSPASS_HOME");
  if (home == nullptr) {
    std::cerr << "MSPASS_HOME was not set for the test" << std::endl;
    return 1;
  }

  const std::string expected = std::string(home) + "/data";
  const std::string actual = mspass::utility::data_directory();
  if (actual != expected) {
    std::cerr << "data_directory returned " << actual << ", expected "
              << expected << std::endl;
    return 1;
  }
  return 0;
}
