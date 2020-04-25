#include <fstream>
#include <sstream>
#include <iomanip>
#include <exception>
#include <functional>
#include <boost/any.hpp>
#include <boost/archive/text_oarchive.hpp>
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
// These includes are objects only visible from the python interpreter
#include "MongoDBConverter.h"
#include <mspass/algorithms/algorithms.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <pybind11/numpy.h>

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
using std::stringstream;
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
using mspass::MongoDBConverter;
using mspass::agc;
using mspass::ExtractComponent;

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
PYBIND11_MODULE(ccore,m)
{
  m.attr("__name__") = "mspasspy.ccore";

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
    .def("put",py::overload_cast<const std::string,const double>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const bool>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const int>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const std::string>(&BasicMetadata::put))
    .def("put_double",&Metadata::put_double,"Interface class for doubles")
    .def("put_bool",&Metadata::put_bool,"Interface class for boolean")
    .def("put_string",&Metadata::put_string,"Interface class for strings")
    .def("put_long",&Metadata::put_long,"Interface class for long ints")
    /* Intentionally do NOT enable put_int.   Found type skew problems if
     * called from python.   Best avoided.
    .def("put_int",&Metadata::put_int,"Interface class for generic ints")
    */
    .def("keys",&Metadata::keys,"Return a list of the keys of all defined attributes")
    .def("type",&Metadata::type,"Return a demangled typename for value associated with a key")
    .def("modified",&Metadata::modified,"Return a list of all attributes that have been changes since construction")
    .def("clear_modified",&Metadata::clear_modified,"Clear container used to mark altered Metadata")
    /*  For unknown reasons could not make this overload work.  
     *  Ended up commenting out char * section of C++ code - baggage in python
     *  anyway.  
    .def("is_defined",py::overload_cast<const std::string>(&Metadata::is_defined))
    .def("is_defined",py::overload_cast<const char *>(&Metadata::is_defined))
    */
    .def("is_defined",&Metadata::is_defined,"Test if a key has a defined value")
    .def("append_chain",&Metadata::append_chain,"Create or append to a string attribute that defines a chain")
    .def("clear",&Metadata::clear,"Clears contents associated with a key")
    .def("change_key",&Metadata::change_key,"Change key to access an attribute")
    .def(py::self += py::self)
    .def(py::self + py::self)
    /* these are need to allow the class to be pickled*/
    .def(py::pickle(
      [](const Metadata &self) {
        string sbuf;
        sbuf=serialize_metadata(self);
        return py::make_tuple(sbuf);
      },
      [](py::tuple t) {
       string sbuf=t[0].cast<std::string>();
       return mspass::Metadata(mspass::restore_serialized_metadata(sbuf));
     }
     ))
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
    .def("shifted",&mspass::BasicTimeSeries::shifted,"Return true if data are UTC standard with a time shift applied")
    .def("force_t0_shift",&mspass::BasicTimeSeries::force_t0_shift,"Force a time shift value to make data shifted UTC in relative time")
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
    .def("zero",&dmatrix::zero,"Initialize a matrix to all zeros")
    .def(py::self += py::self,"Operator +=")
    .def(py::self -= py::self,"Operator -=")
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
    .def("get_transformation_matrix",&CoreSeismogram::get_transformation_matrix,"Fetch transformation matrix as a dmatrix object")
    .def("set_transformation_matrix",&CoreSeismogram::set_transformation_matrix,"Set transformation matrix using a dmatrix object")
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
  m.def("ExtractComponent",&mspass::ExtractComponent,
  	"Extract component as a TimeSeries object",
      py::return_value_policy::copy,
      py::arg("tcs"),
      py::arg("component")
  );
  /*
  m.def("ExtractComponent",
        py::overload_cast<mspass::CoreSeismogram&,int>(&mspass::ExtractComponent),
  	"Extract component as a TimeSeries object",
      py::return_value_policy::copy,
      py::arg("tcs"),
      py::arg("component")
  );
  */
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
    .value("YAML",MDDefFormat::YAML)
  ;
  py::class_<mspass::MetadataDefinitions>(m,"MetadataDefinitions","Load a catalog of valid metadata names with types defined")
    .def(py::init<>())
    .def(py::init<string>())
    .def(py::init<std::string,mspass::MDDefFormat>())
    .def("is_defined",&mspass::MetadataDefinitions::is_defined,"Test if a key is defined")
    .def("concept",&mspass::MetadataDefinitions::concept,"Return a string with a brief description of the concept this attribute captures")
    .def("type",&mspass::MetadataDefinitions::type,"Return a description of the type of this attribute")
    .def("add",&mspass::MetadataDefinitions::add,"Append a new attribute to the catalog")
    .def("has_alias",&mspass::MetadataDefinitions::has_alias,"Returns true if a specified key as an alterate name - alias")
    .def("is_alias",&mspass::MetadataDefinitions::is_alias,"Return true if a key is an alias")
    .def("aliases",&mspass::MetadataDefinitions::aliases,"Return a list of aliases for a particular key")
    .def("unique_name",&mspass::MetadataDefinitions::unique_name,"Returns the unique key name associated with an alias")
    .def("add_alias",&mspass::MetadataDefinitions::add_alias,"Add an alias for a particular atrribute key")
    .def("keys",&mspass::MetadataDefinitions::keys,"Return a list of all valid keys")
    .def("writeable",&mspass::MetadataDefinitions::writeable,"Test if an attribute should be saved")
    .def("readonly",&mspass::MetadataDefinitions::readonly,"Test if an attribute is marked readonly")
    .def("set_readonly",&mspass::MetadataDefinitions::set_readonly,"Force an attribute to be marked readonly")
    .def("set_writeable",&mspass::MetadataDefinitions::set_writeable,"Force an attribute to be marked as writeable")
    .def("is_normalized",&mspass::MetadataDefinitions::is_normalized,"Test to see if an attribute is stored in a master collection (table)")
    .def("unique_id_key",&mspass::MetadataDefinitions::unique_id_key,"Return the key for a unique id to fetch an attribute from a master collection (table)")
    .def("collection",&mspass::MetadataDefinitions::collection,"Return the table (collection) name for an attribute defined in a master table")
    .def("normalize_data",&mspass::MetadataDefinitions::normalize_data,"Faster method to return unique_id_key and table name")
    .def("apply_aliases",&mspass::MetadataDefinitions::apply_aliases,"Apply a set of alias names to Metadata or child of Metadata")
    .def("clear_aliases",&mspass::MetadataDefinitions::clear_aliases,"Clear aliases in a Metadata or child of Metadata")
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
    .def(py::init<int>())
    .def("set_job_id",&mspass::ErrorLogger::set_job_id)
    .def("get_job_id",&mspass::ErrorLogger::get_job_id)
    .def("log_error",py::overload_cast<const mspass::MsPASSError&>(&mspass::ErrorLogger::log_error),"log error thrown as MsPASSError")
    .def("log_error",py::overload_cast<const std::string,const std::string,const mspass::ErrorSeverity>(&mspass::ErrorLogger::log_error),"log a message at a specified severity level")
    .def("log_verbose",&mspass::ErrorLogger::log_verbose,"Log an informational message - tagged as log message")
    .def("get_error_log",&mspass::ErrorLogger::get_error_log,"Return all posted entries")
    .def("size",&mspass::ErrorLogger::size,"Return number of entries in this log")
    .def("worst_errors",&mspass::ErrorLogger::worst_errors,"Return a list of only the worst errors")
  ;
  py::class_<mspass::MsPASSCoreTS>(m,
               "MsPASSCoreTS","class to extend a data object to integrate with MongoDB")
    .def(py::init<>())
    .def("set_id",&mspass::MsPASSCoreTS::set_id,"Set the mongodb unique id to associate with this object")
    .def("get_id",&mspass::MsPASSCoreTS::get_id,"Return the mongodb uniqueid associated with a data object")
    .def_readwrite("elog",&mspass::MsPASSCoreTS::elog,"Error logger object");
  py::class_<mspass::Seismogram,mspass::CoreSeismogram,mspass::MsPASSCoreTS>
                                                (m,"Seismogram")
    .def(py::init<>())
    .def(py::init<CoreSeismogram,std::string>())
    .def(py::init<BasicTimeSeries,Metadata,ErrorLogger>())
    .def(py::init<Metadata>())
    .def(py::pickle(
      [](const Seismogram &self) {
        string sbuf;
        sbuf=serialize_metadata(self);
        stringstream ssbts;
        ssbts << std::setprecision(15);
        boost::archive::text_oarchive arbts(ssbts);
        arbts << dynamic_cast<const BasicTimeSeries&>(self);
        stringstream sscorets;
        boost::archive::text_oarchive arcorets(sscorets);
        arcorets<<dynamic_cast<const MsPASSCoreTS&>(self);
        /* these are behind getter/setters */
        bool cardinal=self.cardinal();
        bool orthogonal=self.orthogonal();
        dmatrix tmatrix=self.get_transformation_matrix();
        stringstream sstm;
        boost::archive::text_oarchive artm(sstm);
        artm<<tmatrix;
        /* This is a very slow solution, but using the axiom make it work
        before you make it fast*/
        stringstream ssu;
        boost::archive::text_oarchive aru(ssu);
        aru<<self.u;
        return py::make_tuple(sbuf,ssbts.str(),sscorets.str(),
          cardinal, orthogonal,sstm.str(),
          ssu.str());
      },
      [](py::tuple t) {
       string sbuf=t[0].cast<std::string>();
       Metadata md;
       md=mspass::Metadata(mspass::restore_serialized_metadata(sbuf));
       stringstream ssbts(t[1].cast<std::string>());
       boost::archive::text_iarchive arbts(ssbts);
       BasicTimeSeries bts;
       arbts>>bts;
       stringstream sscorets(t[2].cast<std::string>());
       boost::archive::text_iarchive arcorets(sscorets);
       MsPASSCoreTS corets;
       arcorets>>corets;
       bool cardinal=t[3].cast<bool>();
       bool orthogonal=t[4].cast<bool>();
       stringstream sstm(t[5].cast<std::string>());
       boost::archive::text_iarchive artm(sstm);
       dmatrix tmatrix;
       artm>>tmatrix;
       stringstream ssu(t[6].cast<std::string>());
       boost::archive::text_iarchive aru(ssu);
       dmatrix u;
       aru>>u;
       return Seismogram(bts,md,corets,cardinal,orthogonal,tmatrix,u);
     }
     ))
    ;
    py::class_<mspass::TimeSeries,mspass::CoreTimeSeries,mspass::MsPASSCoreTS>(m,"TimeSeries","mspass scalar time series data object")
      .def(py::init<>())
      .def(py::init<const mspass::CoreTimeSeries&,const std::string>())
      .def(py::pickle(
        [](const TimeSeries &self) {
          string sbuf;
          sbuf=serialize_metadata(self);
          stringstream ssbts;
          ssbts << std::setprecision(15);
          boost::archive::text_oarchive arbts(ssbts);
          arbts << dynamic_cast<const BasicTimeSeries&>(self);
          stringstream sscorets;
          boost::archive::text_oarchive arcorets(sscorets);
          arcorets<<dynamic_cast<const MsPASSCoreTS&>(self);
          /*This creates a numpy array alias from the vector container
          without a move or copy of the data */
          py::array_t<double, py::array::f_style> darr(self.ns,&(self.s[0]));
          return py::make_tuple(sbuf,ssbts.str(),sscorets.str(),darr);
        },
        [](py::tuple t) {
         string sbuf=t[0].cast<std::string>();
         Metadata md;
         md=mspass::Metadata(mspass::restore_serialized_metadata(sbuf));
         stringstream ssbts(t[1].cast<std::string>());
         boost::archive::text_iarchive arbts(ssbts);
         BasicTimeSeries bts;
         arbts>>bts;
         stringstream sscorets(t[2].cast<std::string>());
         boost::archive::text_iarchive arcorets(sscorets);
         MsPASSCoreTS corets;
         arcorets>>corets;
         /* There might be a faster way to do this than a copy like
         this but for now this, like Seismogram, is make it work before you
         make it fast */
         py::array_t<double, py::array::f_style> darr;
         darr=t[3].cast<py::array_t<double, py::array::f_style>>();
         /* A tad dangerous assuming bts.ns matches actual size of darr, but it should
         in this context. */
         std::vector<double> d;
         d.reserve(bts.ns);
         for(int i=0;i<bts.ns;++i) d.push_back(darr.at(i));
         return TimeSeries(bts,md,corets,d);;
       }
     ));

  /* This object is in a separate pair of files in this directory.  */
  py::class_<mspass::MongoDBConverter>(m,"MongoDBConverter","Metadata translator from C++ object to python")
      .def(py::init<>())
      .def(py::init<const mspass::MetadataDefinitions>())
      .def(py::init<const std::string>())
      .def("modified",&mspass::MongoDBConverter::modified,
         py::arg("d"), py::arg("verbose")=false,
         "Return dict of modified Metadata attributes")
      .def("selected",&mspass::MongoDBConverter::selected,
         py::arg("d"),
         py::arg("keys"),
         py::arg("noabort")=false,
         py::arg("verbose")=true,
         "Return dict of fields defined in input list")
      .def("all",&mspass::MongoDBConverter::all,
         py::arg("d"),py::arg("verbose")=true,
         "Return dict of all Metadata attributes")
      .def("writeable",&mspass::MongoDBConverter::writeable,
          py::arg("d"),py::arg("verbose")=true,
          "Return dict of all Metadata attributes defined to be mutable")
      .def("badkeys",&mspass::MongoDBConverter::badkeys,
         "Return a python list of any keys that are not defined in input object")
    ;
  /* For now algorithm functions will go here.  These may eventually be
     moved to a different module. */
  m.def("agc",&mspass::agc,"Automatic gain control a Seismogram",
    py::return_value_policy::copy,
    py::arg("d"),
    py::arg("twin") )
  ;
  m.def("WindowData",&mspass::WindowData,"Reduce data to window inside original",
    py::return_value_policy::copy,
    py::arg("d"),
    py::arg("twin") )
  ;
  m.def("WindowData3C",&mspass::WindowData3C,"Reduce data to window inside original",
    py::return_value_policy::copy,
    py::arg("d"),
    py::arg("twin") )
  ;
}
