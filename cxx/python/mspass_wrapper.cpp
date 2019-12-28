#include <fstream>
#include <exception>
#include <functional>
#include <boost/any.hpp>
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
#include <mspass/algorithms/algorithms.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

namespace pybind11 { namespace detail {
  template <> struct type_caster<boost::any> {
  public:
    PYBIND11_TYPE_CASTER(boost::any, _("boost::any"));
    bool load(handle src, bool) {
      /*Always return false since we are not converting
        any PyObject into boost::any.
        */
      return false;
    }
    static handle cast(boost::any src, return_value_policy /* policy */, handle /* parent */) {
      auto it = toPythonMap.find(src.type());
      if(it != toPythonMap.end()){
        return it->second(src);
      }else{
        std::cerr << "WARNING: Could not convert directly to Python type. Trying to cast as pybind11::object." << std::endl;
        return boost::any_cast<pybind11::object>(src);
      }
    }
  private:
    static std::map<std::type_index, std::function<handle(boost::any const&)>> toPythonMap;
    static std::map<std::type_index, std::function<handle(boost::any const&)>> createToPythonMap() {
      std::map<std::type_index, std::function<handle(boost::any const&)>> m;
      m[typeid(long)]        = [](boost::any const& x) { return PyLong_FromLong(boost::any_cast<long>(x));};
      m[typeid(int)]         = [](boost::any const& x) { return PyLong_FromLong(boost::any_cast<int>(x));};
      m[typeid(double)]      = [](boost::any const& x) { return PyFloat_FromDouble(boost::any_cast<double>(x));};
      m[typeid(bool)]        = [](boost::any const& x) { return PyBool_FromLong(boost::any_cast<bool>(x));};
      m[typeid(std::string)] = [](boost::any const& x) { return PyUnicode_FromString(boost::any_cast<std::string>(x).c_str());};
      return m;
    }
  };
  std::map<std::type_index, std::function<handle(boost::any const&)>>
  type_caster<boost::any>::toPythonMap = type_caster<boost::any>::createToPythonMap();
}} // namespace pybind11::detail

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
using mspass::MsPASSCoreTS;
using mspass::agc;

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
  int get_int(const std::string key) const override
  {
    PYBIND11_OVERLOAD_PURE(
      int,
      mspass::BasicMetadata,
      get_int,
      key
    );
  };
  double get_double(const std::string key) const override
  {
    PYBIND11_OVERLOAD_PURE(
      double,
      mspass::BasicMetadata,
      get_double,
      key
    );
  };
  bool get_bool(const std::string key) const override
  {
    PYBIND11_OVERLOAD_PURE(
      bool,
      mspass::BasicMetadata,
      get_bool,
      key
    );
  };
  std::string get_string(const std::string key) const override
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
PYBIND11_MODULE(mspasspy,m)
{
  py::bind_vector<std::vector<double>>(m, "Vector");

  py::class_<mspass::SphericalCoordinate>(m,"SphericalCoordinate","Enscapsulates concept of spherical coordinates")
    .def_readwrite("radius", &mspass::SphericalCoordinate::radius,"R of spherical coordinates")
    .def_readwrite("theta", &mspass::SphericalCoordinate::theta,"zonal angle of spherical coordinates")
    .def_readwrite("phi", &mspass::SphericalCoordinate::phi,"azimuthal angle of spherical coordinates")
  ;

  py::class_<mspass::SlownessVector>(m,"SlownessVector","Encapsulate concept of slowness vector describing wave propagation")
    .def(py::init<const double, const double, const double>())
    .def("mag",&SlownessVector::mag,"Return the magnitude of the slowness vector")
    .def("azimuth",&SlownessVector::azimuth,"Return the azimuth of propagation defined by this slowness vector")
    .def("baz",&SlownessVector::baz,"Return the so called back azimuth defined by a slowness vector")
    .def_readwrite("ux",&SlownessVector::ux,"Slowness component in the x (Easting) direction")
    .def_readwrite("uy",&SlownessVector::uy,"Slowness component in the y (Northing) direction")
  ;

  py::class_<mspass::TimeWindow>(m,"TimeWindow","Simple description of a time window")
    .def(py::init<const double, const double>(),"Construct from start and end time")
    .def("shift",&TimeWindow::shift,"Shift the reference time by a specified number of seconds")
    .def("length",&TimeWindow::length,"Return the size of the window in seconds")
    .def_readwrite("start",&TimeWindow::start,"Start time of the window")
    .def_readwrite("end",&TimeWindow::end,"End time of the window")
  ;

  py::class_<mspass::BasicMetadata,PyBasicMetadata>(m,"BasicMetadata")
    .def(py::init<>())
    .def("get_double",&mspass::BasicMetadata::get_double,py::return_value_policy::automatic)
    .def("get_int",&mspass::BasicMetadata::get_int)
    .def("get_bool",&mspass::BasicMetadata::get_bool)
    .def("get_string",&mspass::BasicMetadata::get_string)
    .def("put",py::overload_cast<const std::string,const double>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const bool>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const int>(&BasicMetadata::put))
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
    .def("get_double",&mspass::Metadata::get_double,"Retrieve a real number by a specified key")
    .def("get_int",&mspass::Metadata::get_int,"Return an integer number by a specified key")
    .def("get_long",&mspass::Metadata::get_long,"Return a long integer by a specified key")
    .def("get_bool",&mspass::Metadata::get_bool,"Return a (C) boolean defined by a specified key")
    .def("get_string",&mspass::Metadata::get_string,"Return a string indexed by a specified key")
    .def("get",&mspass::Metadata::get_any,"Return the value indexed by a specified key")
    /* These use a feature I not able to make work.   They are supposed to
    require c14 but even when I turn that on all of these cause hard to
    understand (for me anyway) compilation errors.   For now will use the
    more inefficient method where the overloads are tried sequentially.  */
    .def("put",py::overload_cast<const std::string,const double>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const bool>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const int>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const std::string>(&BasicMetadata::put))
    .def("keys",&Metadata::keys,"Return a list of the keys of all defined attributes")
    .def("modified",&Metadata::modified,"Return a list of all attributes that have been changes since construction")
    .def("is_defined",&Metadata::is_defined,"Test if a key has a value set")
    .def("clear",&Metadata::clear,"Clears contents associated with a key")
    .def(py::self += py::self)
    .def(py::self + py::self)
  ;

  py::class_<mspass::AntelopePf,Metadata>(m,"AntelopePf")
    .def(py::init<>())
    .def(py::init<std::string>(),"Construct from a file")
    .def(py::init<std::list<string>>(),"Construct from a list of strings defining lines to be parsed")
    .def("get_tbl",&mspass::AntelopePf::get_tbl,"Fetch contents of a block of the pf file defined by Tbl&")
    .def("get_branch",&mspass::AntelopePf::get_branch,"Fetch contents of a block of the pf defined by an Arr&")
    .def("arr_keys",&mspass::AntelopePf::arr_keys,"Return a list of all branch (&Arr) keys")
    .def("tbl_keys",&mspass::AntelopePf::tbl_keys,"Fetch a list of keys for all &Tbl blocks")
    .def("ConvertToMetadata",&mspass::AntelopePf::ConvertToMetadata,"Convert to a flat Metadata space (no branches)")
  ;

  py::class_<mspass::Metadata_typedef>(m,"Metadata_typedef")
    .def(py::init<>())
    .def_readwrite("tag",&Metadata_typedef::tag,"Name key for this metadata")
    .def_readwrite("mdt",&Metadata_typedef::mdt,"Type of any value associated with this key")
  ;

  py::enum_<mspass::TimeReferenceType>(m,"TimeReferenceType")
    .value("Relative",TimeReferenceType::Relative)
    .value("UTC",TimeReferenceType::UTC)
  ;

  py::class_<mspass::BasicTimeSeries,PyBasicTimeSeries>(m,"BasicTimeSeries","Core common concepts for uniformly sampled 1D data")
    .def(py::init<>())
    .def("time",&mspass::BasicTimeSeries::time,"Return the computed time for a sample number (integer)")
    .def("sample_number",&mspass::BasicTimeSeries::sample_number,"Return the sample index number for a specified time")
    .def("endtime",&mspass::BasicTimeSeries::endtime,"Return the (computed) end time of a time series")
    .def("rtoa",py::overload_cast<const double>(&mspass::BasicTimeSeries::rtoa))
    .def("rtoa",py::overload_cast<>(&mspass::BasicTimeSeries::rtoa))
    .def("ator",&mspass::BasicTimeSeries::ator,"Switch time standard from absolute (UTC) to a relative time scale")
    .def("shift",&mspass::BasicTimeSeries::shift,"Shift time reference by a specified number of seconds")
    .def("time_reference",&mspass::BasicTimeSeries::time_reference,"Return time standard")
    .def_readwrite("live",&BasicTimeSeries::live,"True of data is valid, false if invalid")
    .def_readwrite("dt",&BasicTimeSeries::dt,"Sample interval")
    .def_readwrite("t0",&BasicTimeSeries::t0,"Time of sample 0")
    .def_readwrite("ns",&BasicTimeSeries::ns,"Number of samples in this time series")
    .def_readwrite("tref",&BasicTimeSeries::tref,"Defines time standard for this time series")
  ;
  py::class_<mspass::CoreTimeSeries,mspass::BasicTimeSeries,mspass::Metadata>(m,"CoreTimeSeries","Defines basic concepts of a scalar time series")
    .def(py::init<>())
    .def(py::init<const int>())
    .def(py::self += py::self)
    .def_readwrite("s",&CoreTimeSeries::s,"Actual samples are stored in this data vector")
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
    .def("rows",&dmatrix::rows,"Rows in the matrix")
    .def("columns",&dmatrix::columns,"Columns in the matrix")
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
  py::class_<mspass::CoreSeismogram,mspass::BasicTimeSeries,mspass::Metadata>(m,"CoreSeismogram","Defines basic concepts of a three-component seismogram")
    .def(py::init<>())
    .def(py::init<const int>())
    .def(py::init<const std::vector<mspass::CoreTimeSeries>&,const int>())
    .def("rotate_to_standard",&CoreSeismogram::rotate_to_standard,"Transform data to cardinal coordinates")
    .def("rotate",py::overload_cast<SphericalCoordinate&>(&CoreSeismogram::rotate),"3D rotation defined by spherical coordinate angles")
    .def("rotate",py::overload_cast<const double[3]>(&CoreSeismogram::rotate),"3D rotation defined a unit vector direction")
    .def("rotate",py::overload_cast<const double>(&CoreSeismogram::rotate),"2D rotation about the vertical axis")
    /* The following pair of methods have adaptor issues for pybind11.
    I think they are best implemented with wrappers in Seismogram that work
    around the issue.  See README_Implementation_TODO */
    //.def("transform",&CoreSeismogram::transform)
    .def("free_surface_tranformation",&CoreSeismogram::free_surface_transformation,"Apply free surface transformation operator to data")
    //.def("transformation_matrix",&CoreSeismogram::transformation_matrix)
    .def(py::self += py::self)
    /* Place holder for data array.   Probably want this exposed through
    Seismogram api */
    .def_readwrite("u",&CoreSeismogram::u)
  ;
  m.def("ArrivalTimeReference",&mspass::ArrivalTimeReference,"Shifts data so t=0 is a specified arrival time",
      py::return_value_policy::copy,
      py::arg("d"),
      py::arg("key"),
      py::arg("window")
  );
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
  py::class_<mspass::MetadataDefinitions>(m,"MetadataDefinitions","Load a catalog of valid metadata names with types defined")
    .def(py::init<>())
    .def(py::init<std::string,mspass::MDDefFormat>())
    .def("concept",&mspass::MetadataDefinitions::concept,"Return a string with a brief description of the concept this attribute captures")
    .def("type",&mspass::MetadataDefinitions::type,"Return a description of the type of this attribute")
    .def("add",&mspass::MetadataDefinitions::add,"Append a new attribute to the catalog")
    .def("has_alias",&mspass::MetadataDefinitions::has_alias,"Returns true if a specified key as an alterate name - alias")
    .def("aliases",&mspass::MetadataDefinitions::aliases,"Return a list of aliases for a particular key")
    .def("unique_name",&mspass::MetadataDefinitions::unique_name,"Returns the unique key name associated with an alias")
    .def("add_alias",&mspass::MetadataDefinitions::add_alias,"Add an alias for a particular atrribute key")
    .def("keys",&mspass::MetadataDefinitions::keys,"Return a list of all valid keys")
    .def(py::self += py::self)
  ;
/* These are needed for mspass extensions of Core data objects */
  py::class_<mspass::LogData>(m,"LogData","Many mspass create error and log messages with this structure")
    .def(py::init<>())
    .def(py::init<int,std::string,mspass::MsPASSError&>())
    .def_readwrite("job_id",&LogData::job_id,"Return the job id defined for this log message")
    .def_readwrite("p_id",&LogData::p_id,"Return the process id of the procedure that threw the defined message")
    .def_readwrite("algorithm",&LogData::algorithm,"Return the algorithm of the procedure that threw the defined message")
    .def_readwrite("badness",&LogData::badness,"Return a error level code")
    .def_readwrite("message",&LogData::message,"Return the actual posted message")
  ;
  py::class_<mspass::ErrorLogger>(m,"ErrorLogger","Used to post any nonfatal errors without aborting a program of family of parallel programs")
    .def(py::init<>())
    .def(py::init<int,std::string>())
    .def("set_job_id",&mspass::ErrorLogger::set_job_id)
    .def("set_algorithm",&mspass::ErrorLogger::set_algorithm)
    .def("get_job_id",&mspass::ErrorLogger::get_job_id)
    .def("get_algorithm",&mspass::ErrorLogger::get_algorithm)
    .def("log_error",py::overload_cast<const mspass::MsPASSError&>(&mspass::ErrorLogger::log_error),"log error thrown as MsPASSError")
    .def("log_error",py::overload_cast<const std::string,const mspass::ErrorSeverity>(&mspass::ErrorLogger::log_error),"log a message at a specified severity level")
    .def("log_verbose",&mspass::ErrorLogger::log_verbose)
    .def("get_error_log",&mspass::ErrorLogger::get_error_log)
    .def("size",&mspass::ErrorLogger::size)
    .def("worst_errors",&mspass::ErrorLogger::worst_errors)
  ;
  py::class_<mspass::MsPASSCoreTS>(m,
               "MsPASSCoreTS","class to extend a data object to integrate with MongoDB")
    .def(py::init<>())
    .def("set_id",&mspass::MsPASSCoreTS::set_id,"Set the mongodb unique id to associate with this object")
    .def("get_id",&mspass::MsPASSCoreTS::get_id,"Return the mongodb uniqueid associated with a data object")
    .def_readwrite("elog",&mspass::MsPASSCoreTS::elog,"Error logger object");
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
  /* For now algorithm functions will go here.  These may eventually be 
     moved to a different module. */
  m.def("agc",&mspass::agc,"Automatic gain control a Seismogram",
    py::return_value_policy::copy,
    py::arg("d"),
    py::arg("twin") )
  ;
}
