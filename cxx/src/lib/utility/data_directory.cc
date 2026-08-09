#include "mspass/utility/MsPASSError.h"
#include <Python.h>
#include <cstdlib>
#include <string>

namespace {
std::string python_package_data_directory() {
  if (!Py_IsInitialized())
    return std::string();

  const PyGILState_STATE gil_state = PyGILState_Ensure();
  std::string datadir;
  PyObject *modules = PyImport_GetModuleDict();
  PyObject *package = PyDict_GetItemString(modules, "mspasspy");
  if (package != nullptr) {
    PyObject *package_file = PyObject_GetAttrString(package, "__file__");
    if (package_file != nullptr && package_file != Py_None) {
      const char *path = PyUnicode_AsUTF8(package_file);
      if (path != nullptr) {
        const std::string filename(path);
        const std::size_t separator = filename.find_last_of("/\\");
        if (separator != std::string::npos)
          datadir = filename.substr(0, separator) + "/data";
      }
    }
    Py_XDECREF(package_file);
  }
  if (PyErr_Occurred())
    PyErr_Clear();
  PyGILState_Release(gil_state);
  return datadir;
}
} // namespace

namespace mspass::utility {
/* Standardizes top level directory for mspass */
std::string data_directory() {
  const std::string mspass_home_envname("MSPASS_HOME");
  /* Note man page for getenv says explicitly the return of getenv should not
                  be touched - i.e. don't free it*/
  const char *base = getenv(mspass_home_envname.c_str());
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
