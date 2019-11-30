#include <fstream>
#include <exception>
#include <mspass/utility/MsPASSError.h>
#include <mspass/utility/AttributeMap.h>
#include <mspass/utility/SphericalCoordinate.h>
#include <mspass/seismic/SlownessVector.h>
#include <mspass/seismic/TimeWindow.h>
#include <mspass/utility/Metadata.h>
#include <mspass/utility/AntelopePf.h>
//Note this loads BasicTimeSeries that is used also by Seismogram and TimeSeries
#include <mspass/seismic/TimeSeries.h>
#include <mspass/seismic/Seismogram.h>
#include <mspass/utility/MetadataDefinitions.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

namespace py=pybind11;

using std::exception;
using mspass::AttributeMap;
using mspass::SphericalCoordinate;
using mspass::SlownessVector;
using mspass::TimeWindow;
using mspass::MDtype;
using mspass::BasicMetadata;
using mspass::Metadata;
using mspass::AntelopePf;
using mspass::Metadata_typedef;
using mspass::TimeReferenceType;
using mspass::BasicTimeSeries;
using mspass::CoreTimeSeries;
using mspass::TimeSeries;
using mspass::CoreSeismogram;
using mspass::dmatrix;
using mspass::Seismogram;
using mspass::MsPASSError;
using mspass::ErrorSeverity;
using mspass::LogData;
using mspass::ErrorLogger;
using mspass::pfread;
using mspass::MDDefFormat;
using mspass::MetadataDefinitions;
/* We enable this gem for reasons explain in the documentation for pybinde11
at this url:  https://pybind11.readthedocs.io/en/master/advanced/cast/stl.html
Upshot is we need the py::bind line at the start of the module definition.
Note a potential issue is any vector<double> in this module will share this
binding.  Don't think there are any collisions on the C side but worth a warning*/
PYBIND11_MAKE_OPAQUE(std::vector<double>);

/* This is what the pybind11 documentation calls a trampoline class for
needed to handle virtual function in the abstract base class BasicMetadata. */

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

/* Trampoline class for BasicTimeSeries */
class PyBasicTimeSeries : public BasicTimeSeries
{
public:
  using BasicTimeSeries::BasicTimeSeries;
  /* BasicTimeSeries has virtual methods that are not pure because
  forms that contain gap handlers need additional functionality.
  We thus use a different qualifier to PYBIND11_OVERLOAD macro here.
  i.e. omit the PURE part of the name*/
  void ator(const double tshift)
  {
    PYBIND11_OVERLOAD(
      void,
      mspass::BasicTimeSeries,
      ator,
      tshift);
  }
  void rtoa(const double tshift)
  {
    PYBIND11_OVERLOAD(
      void,
      mspass::BasicTimeSeries,
      ator,
      tshift);
  }
  void rota()
  {
    PYBIND11_OVERLOAD(
      void,
      mspass::BasicTimeSeries,
      rtoa,

    );
  }
  void shift(const double dt)
  {
    PYBIND11_OVERLOAD(
      void,
      mspass::BasicTimeSeries,
      shift,
      dt);
  }
  double time_reference() const
  {
    PYBIND11_OVERLOAD(
      double,
      mspass::BasicTimeSeries,
      time_reference,
    );
  }
};
/* Documentation says this is needed for c11 compilation, but cannot make
it work. Preserve for now.
template <typename... Args>
using overload_cast_ = pybind11::detail::overload_cast_impl<Args...>;
*/
PYBIND11_MODULE(mspasspy,m)
{
  py::bind_vector<std::vector<double>>(m, "Vector");

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
  py::enum_<mspass::MDtype>(m,"MDtype")
    .value("Real",MDtype::Real)
    .value("Real32",MDtype::Real32)
    .value("Double",MDtype::Double)
    .value("Real64",MDtype::Real64)
    .value("Integer",MDtype::Integer)
    .value("Int32",MDtype::Int32)
    .value("Long",MDtype::Long)
    .value("Int64",MDtype::Int64)
    .value("String",MDtype::String)
    .value("Boolean",MDtype::Boolean)
    .value("Invalid",MDtype::Invalid)
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
    .def("keys",&Metadata::keys)
    .def("modified",&Metadata::modified)
    .def(py::self += py::self)
    .def(py::self + py::self)
  ;

  py::class_<mspass::AntelopePf,Metadata>(m,"AntelopePf")
    .def(py::init<>())
    .def(py::init<std::string>())
    .def(py::init<std::list<string>>())
    .def("get_tbl",&mspass::AntelopePf::get_tbl)
    .def("get_branch",&mspass::AntelopePf::get_branch)
    .def("arr_keys",&mspass::AntelopePf::arr_keys)
    .def("tbl_keys",&mspass::AntelopePf::tbl_keys)
    .def("ConvertToMetadata",&mspass::AntelopePf::ConvertToMetadata)
  ;

  py::class_<mspass::Metadata_typedef>(m,"Metadata_typedef")
    .def(py::init<>())
    .def_readwrite("tag",&Metadata_typedef::tag)
    .def_readwrite("mdt",&Metadata_typedef::mdt)
  ;

  py::enum_<mspass::TimeReferenceType>(m,"TimeReferenceType")
    .value("Relative",TimeReferenceType::Relative)
    .value("UTC",TimeReferenceType::UTC)
  ;

  py::class_<mspass::BasicTimeSeries,PyBasicTimeSeries>(m,"BasicTimeSeries")
    .def(py::init<>())
    .def("time",&mspass::BasicTimeSeries::time)
    .def("sample_number",&mspass::BasicTimeSeries::sample_number)
    .def("endtime",&mspass::BasicTimeSeries::endtime)
    .def("rtoa",py::overload_cast<const double>(&mspass::BasicTimeSeries::rtoa))
    .def("rtoa",py::overload_cast<>(&mspass::BasicTimeSeries::rtoa))
    .def("ator",&mspass::BasicTimeSeries::ator)
    .def("shift",&mspass::BasicTimeSeries::shift)
    .def("time_reference",&mspass::BasicTimeSeries::time_reference)
    .def_readwrite("live",&BasicTimeSeries::live)
    .def_readwrite("dt",&BasicTimeSeries::dt)
    .def_readwrite("t0",&BasicTimeSeries::t0)
    .def_readwrite("ns",&BasicTimeSeries::ns)
    .def_readonly("tref",&BasicTimeSeries::tref)
  ;
  py::class_<mspass::CoreTimeSeries,mspass::BasicTimeSeries,mspass::Metadata>(m,"CoreTimeSeries")
    .def(py::init<>())
    .def(py::init<const int>())
    .def(py::self += py::self)
    .def_readwrite("s",&CoreTimeSeries::s)
  ;
  /* We need this definition to bind dmatrix to a numpy array as described
  in this section of pybind11 documentation:\
  https://pybind11.readthedocs.io/en/stable/advanced/pycpp/numpy.html
  Leans heavily on example here:
  https://github.com/pybind/pybind11/blob/master/tests/test_buffers.cpp
  */
  py::class_<mspass::dmatrix>(m, "dmatrix", py::buffer_protocol())
   .def(py::init<>())
   .def(py::init<int,int>())
   .def(py::init([](py::buffer const b) {
            py::buffer_info info = b.request();
            if (info.format != py::format_descriptor<double>::format() || info.ndim != 2)
                throw std::runtime_error("dmatrix python wrapper:  Incompatible buffer format!");
            auto v = new dmatrix(info.shape[0], info.shape[1]);
            memcpy(v->get_address(0,0), info.ptr, sizeof(double) * v->rows() * v->columns());
            return v;
        }))
   .def_buffer([](mspass::dmatrix &m) -> py::buffer_info {
        return py::buffer_info(
            m.get_address(0,0),                               /* Pointer to buffer */
            sizeof(double),                          /* Size of one scalar */
            py::format_descriptor<double>::format(), /* Python struct-style format descriptor */
            2,                                      /* Number of dimensions */
            { m.rows(), m.columns() },                 /* Buffer dimensions */
            { sizeof(double),             /* Strides (in bytes) for each index - inverted from example*/
              sizeof(double) * m.rows() }
        );
      })
    .def("rows",&dmatrix::rows)
    .def("columns",&dmatrix::columns)
    .def("__getitem__", [](const dmatrix &m, std::pair<int, int> i) {
          if (i.first >= m.rows() || i.second >= m.columns())
              throw py::index_error();
          return m(i.first, i.second);
      })
      .def("__setitem__", [](dmatrix &m, std::pair<int, int> i, double v) {
          if (i.first >= m.rows() || i.second >= m.columns())
              throw py::index_error();
          m(i.first, i.second) = v;
      })
    ;
  py::class_<mspass::CoreSeismogram,mspass::BasicTimeSeries,mspass::Metadata>(m,"CoreSeismogram")
    .def(py::init<>())
    .def(py::init<const int>())
    .def(py::init<const std::vector<mspass::CoreTimeSeries>&,const int>())
    .def("rotate_to_standard",&CoreSeismogram::rotate_to_standard)
    .def("rotate",py::overload_cast<SphericalCoordinate&>(&CoreSeismogram::rotate))
    .def("rotate",py::overload_cast<const double[3]>(&CoreSeismogram::rotate))
    .def("rotate",py::overload_cast<const double>(&CoreSeismogram::rotate))
    /* The following pair of methods have adaptor issues for pybind11.
    I think they are best implemented with wrappers in Seismogram that work
    around the issue.  See README_Implementation_TODO */
    //.def("transform",&CoreSeismogram::transform)
    .def("free_surface_tranformation",&CoreSeismogram::free_surface_transformation)
    //.def("transformation_matrix",&CoreSeismogram::transformation_matrix)
    .def(py::self += py::self)
    /* Place holder for data array.   Probably want this exposed through
    Seismogram api */
    .def_readwrite("u",&CoreSeismogram::u)
  ;
  py::enum_<mspass::ErrorSeverity>(m,"ErrorSeverity")
    .value("Fatal",ErrorSeverity::Fatal)
    .value("Invalid",ErrorSeverity::Invalid)
    .value("Suspect",ErrorSeverity::Suspect)
    .value("Complaint",ErrorSeverity::Complaint)
    .value("Debug",ErrorSeverity::Debug)
    .value("Informational",ErrorSeverity::Informational)
  ;
  py::class_<std::exception>(m,"std_exception")
    .def("what",&std::exception::what)
  ;
  py::class_<mspass::MsPASSError,std::exception>(m,"MsPASSError")
    .def(py::init<>())
    .def(py::init<const std::string,const char *>())
    .def(py::init<const std::string,mspass::ErrorSeverity>())
    .def("what",&mspass::MsPASSError::what)
  ;
  m.def("pfread",&mspass::pfread,"parameter file reader",
      py::return_value_policy::copy,
      py::arg("pffile")
  );
  m.def("get_mdlist",&mspass::get_mdlist,"retrieve list with keys and types",
    py::return_value_policy::copy
  );
  py::enum_<mspass::MDDefFormat>(m,"MDDefFormat")
    .value("PF",MDDefFormat::PF)
    .value("SimpleText",MDDefFormat::SimpleText)
  ;
  py::class_<mspass::MetadataDefinitions>(m,"MetadataDefinitions")
    .def(py::init<>())
    .def(py::init<std::string,mspass::MDDefFormat>())
    .def("concept",&mspass::MetadataDefinitions::concept)
    .def("type",&mspass::MetadataDefinitions::type)
    .def("add",&mspass::MetadataDefinitions::add)
    .def("has_alias",&mspass::MetadataDefinitions::has_alias)
    .def("aliases",&mspass::MetadataDefinitions::aliases)
    .def("unique_name",&mspass::MetadataDefinitions::unique_name)
    .def("add_alias",&mspass::MetadataDefinitions::add_alias)
    .def("keys",&mspass::MetadataDefinitions::keys)
    .def(py::self += py::self)
  ;
/* These are needed for mspass extensions of Core data objects */
  py::class_<mspass::LogData>(m,"LogData")
    .def(py::init<>())
    .def(py::init<int,std::string,mspass::MsPASSError&>())
    .def_readwrite("job_id",&LogData::job_id)
    .def_readwrite("p_id",&LogData::p_id)
    .def_readwrite("algorithm",&LogData::algorithm)
    .def_readwrite("badness",&LogData::badness)
    .def_readwrite("message",&LogData::message)
  ;
  py::class_<mspass::ErrorLogger>(m,"ErrorLogger")
    .def(py::init<>())
    .def(py::init<int,std::string>())
    .def("set_job_id",&mspass::ErrorLogger::set_job_id)
    .def("set_algorithm",&mspass::ErrorLogger::set_algorithm)
    .def("get_job_id",&mspass::ErrorLogger::get_job_id)
    .def("get_algorithm",&mspass::ErrorLogger::get_algorithm)
    .def("log_error",&mspass::ErrorLogger::log_error)
    .def("log_verbose",&mspass::ErrorLogger::log_verbose)
    .def("get_error_log",&mspass::ErrorLogger::get_error_log)
    .def("size",&mspass::ErrorLogger::size)
    .def("worst_errors",&mspass::ErrorLogger::worst_errors)
  ;
  py::class_<mspass::MsPASSCoreTS>(m,"MsPASSCoreTS")
    .def(py::init<>())
    .def("set_id",&mspass::MsPASSCoreTS::set_id)
    .def("get_id",&mspass::MsPASSCoreTS::get_id)
    ;
  /* These two APIs are incomplete but are nontheless mostly wrappers for
  Core versions of same */
  py::class_<mspass::TimeSeries,mspass::CoreTimeSeries,mspass::MsPASSCoreTS>
                                                (m,"TimeSeries")
    .def(py::init<>())
    .def(py::init<CoreTimeSeries,std::string>())
    ;
  py::class_<mspass::Seismogram,mspass::CoreSeismogram,mspass::MsPASSCoreTS>
                                                (m,"Seismogram")
    .def(py::init<>())
    .def(py::init<CoreSeismogram,std::string>())
    ;
}
