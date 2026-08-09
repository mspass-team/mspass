#include "data_directory.h"
#include "mspass/utility/MsPASSError.h"
#include <cstdlib>
#include <string>

namespace {
std::string &python_package_data_directory() {
  static std::string directory;
  return directory;
}
} // namespace

namespace mspass::utility {
void detail::set_python_package_data_directory(const std::string &directory) {
  python_package_data_directory() = directory;
}

/* Standardizes top level directory for mspass */
std::string data_directory() {
  const std::string mspass_home_envname("MSPASS_HOME");
  /* Note man page for getenv says explicitly the return of getenv should not
                  be touched - i.e. don't free it*/
  const char *base = std::getenv(mspass_home_envname.c_str());
  if (base != nullptr)
    return std::string(base) + "/data";

  const std::string package_datadir = python_package_data_directory();
  if (!package_datadir.empty())
    return package_datadir;

  throw MsPASSError(
      "mspass::utility::data_directory:  MSPASS_HOME is not set and the "
      "mspasspy package data directory is unavailable");
}
} // namespace mspass::utility
