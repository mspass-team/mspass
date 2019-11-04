#include <fstream>
#include <mspass/utility/AttributeMap.h>
#include <mspass/utility/SphericalCoordinate.h>
#include <mspass/seismic/SlownessVector.h>
#include <mspass/seismic/TimeWindow.h>
#include <mspass/utility/Metadata.h>
#include <pybind11/pybind11.h>
namespace py=pybind11;

using mspass::AttributeMap;
using mspass::SphericalCoordinate;
using mspass::SlownessVector;
using mspass::TimeWindow;
using mspass::BasicMetadata;
using mspass::Metadata;
class PyBasicMetadata : public BasicMetadata
{
public:
  using BasicMetadata::BasicMetadata;
  int get_int(const std::string key) const
  {
    PYBIND11_OVERLOAD_PURE(
      int,
      mspass::BasicMetadata,
      get_int,
      key
    );
  };
  double get_double(const std::string key) const
  {
    PYBIND11_OVERLOAD_PURE(
      double,
      mspass::BasicMetadata,
      get_double,
      key
    );
  };
  bool get_bool(const std::string key) const
  {
    PYBIND11_OVERLOAD_PURE(
      bool,
      mspass::BasicMetadata,
      get_bool,
      key
    );
  };
  std::string get_string(const std::string key) const
  {
    PYBIND11_OVERLOAD_PURE(
      std::string,
      mspass::BasicMetadata,
      get_string,
      key
    );
  };
  void put(const std::string key,const double val) override
  {
    PYBIND11_OVERLOAD_PURE(
      void,
      mspass::BasicMetadata,
      put,
      key,
      val
    );
  };
  void put(const std::string key,const int val) override
  {
    PYBIND11_OVERLOAD_PURE(
      void,
      mspass::BasicMetadata,
      put,
      key,
      val
    );
  };
  void put(const std::string key,const bool val) override
  {
    PYBIND11_OVERLOAD_PURE(
      void,
      mspass::BasicMetadata,
      put,
      key,
      val
    );
  };
  void put(const std::string key,const std::string val) override
  {
    PYBIND11_OVERLOAD_PURE(
      void,
      mspass::BasicMetadata,
      put,
      key,
      val
    );
  };
};
/* Documentation says this is needed for c11 compilation, but cannot make
it work. Preserve for now.
template <typename... Args>
using overload_cast_ = pybind11::detail::overload_cast_impl<Args...>;
*/
PYBIND11_MODULE(mspasspy,m)
{
  py::class_<mspass::AttributeMap>(m,"AttributeMap")
    .def(py::init<>())
    .def(py::init<const std::string>() )
    //.def("aliastables",(std::list<std::string> (AttributeMap::*)(const std::string)) &AttributeMap::aliastables,"Return list of alias names associated with a key")
    //.def("aliastables",py::overload_cast<const std::string>(&mspass::AttributeMap::aliastables))
    //.def("aliastables",py::overload_cast<const char *>(&AttributeMap::aliastables))
  ;

  py::class_<mspass::SphericalCoordinate>(m,"SphericalCoordinate")
    .def_readwrite("radius", &mspass::SphericalCoordinate::radius)
    .def_readwrite("theta", &mspass::SphericalCoordinate::theta)
    .def_readwrite("phi", &mspass::SphericalCoordinate::phi)
  ;

  py::class_<mspass::SlownessVector>(m,"SlownessVector")
    .def(py::init<const double, const double, const double>())
    .def("mag",&SlownessVector::mag)
    .def("azimuth",&SlownessVector::azimuth)
    .def("baz",&SlownessVector::baz)
    .def_readwrite("ux",&SlownessVector::ux)
    .def_readwrite("uy",&SlownessVector::uy)
  ;

  py::class_<mspass::TimeWindow>(m,"TimeWindow")
    .def(py::init<const double, const double>())
    .def("shift",&TimeWindow::shift)
    .def("length",&TimeWindow::length)
    .def_readwrite("start",&TimeWindow::start)
    .def_readwrite("end",&TimeWindow::end)
  ;

  py::class_<mspass::BasicMetadata,PyBasicMetadata>(m,"BasicMetadata")
    .def(py::init<>())
    .def("get_double",&mspass::BasicMetadata::get_double,py::return_value_policy::automatic)
    .def("get_int",&mspass::BasicMetadata::get_int)
    .def("get_bool",&mspass::BasicMetadata::get_bool)
    .def("get_string",&mspass::BasicMetadata::get_string)
    .def("put",py::overload_cast<const std::string,const double>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const int>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const bool>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const std::string>(&BasicMetadata::put))
  ;


  py::class_<mspass::Metadata,mspass::BasicMetadata>(m,"Metadata")
    .def(py::init<>())
    .def(py::init<std::ifstream&,const std::string>())
    .def("get_double",&mspass::Metadata::get_double)
    .def("get_int",&mspass::Metadata::get_int)
    .def("get_long",&mspass::Metadata::get_long)
    .def("get_bool",&mspass::Metadata::get_bool)
    .def("get_string",&mspass::Metadata::get_string)
    /* These use a feature I not able to make work.   They are supposed to
    require c14 but even when I turn that on all of these cause hard to
    understand (for me anyway) compilation errors.   For now will use the
    more inefficient method where the overloads are tried sequentially.  */
    .def("put",py::overload_cast<const std::string,const double>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const double>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const int>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const bool>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const std::string>(&BasicMetadata::put))
  ;

}
