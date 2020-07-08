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
#include <mspass/seismic/Ensemble.h>
// These includes are objects only visible from the python interpreter
#include "MongoDBConverter.h"
#include <mspass/algorithms/algorithms.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <pybind11/numpy.h>
#include <pybind11/operators.h>

namespace py=pybind11;

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
      if(it != toPythonMap.end())
        return it->second(src);
      else
        return boost::any_cast<py::object>(src).inc_ref();
    }
  private:
    static std::map<std::type_index, std::function<handle(boost::any const&)>> toPythonMap;
    static std::map<std::type_index, std::function<handle(boost::any const&)>> createToPythonMap() {
      std::map<std::type_index, std::function<handle(boost::any const&)>> m;
      m[typeid(long)]        = [](boost::any const& x) { return py::cast(boost::any_cast<long>(x)).release();};
      m[typeid(int)]         = [](boost::any const& x) { return py::cast(boost::any_cast<int>(x)).release();};
      m[typeid(double)]      = [](boost::any const& x) { return py::cast(boost::any_cast<double>(x)).release();};
      m[typeid(bool)]        = [](boost::any const& x) { return py::cast(boost::any_cast<bool>(x)).release();};
      m[typeid(std::string)] = [](boost::any const& x) { return py::cast(boost::any_cast<std::string>(x)).release();};
      return m;
    }
  };
  std::map<std::type_index, std::function<handle(boost::any const&)>>
  type_caster<boost::any>::toPythonMap = type_caster<boost::any>::createToPythonMap();
}} // namespace pybind11::detail

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
using mspass::BasicProcessingHistory;
using mspass::ProcessingHistory;
using mspass::Ensemble;
using mspass::MongoDBConverter;
using mspass::agc;
using mspass::ExtractComponent;

/* We enable this gem for reasons explain in the documentation for pybinde11
at this url:  https://pybind11.readthedocs.io/en/master/advanced/cast/stl.html
Upshot is we need the py::bind line at the start of the module definition.
Note a potential issue is any vector<double> in this module will share this
binding.  Don't think there are any collisions on the C side but worth a warning.
 April 2020
 Added bindings for vector of TimeSeries and Seismogram objects to support Ensembles*/
PYBIND11_MAKE_OPAQUE(std::vector<double>);
PYBIND11_MAKE_OPAQUE(std::vector<TimeSeries>);
PYBIND11_MAKE_OPAQUE(std::vector<Seismogram>);

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
      rtoa);
  }
  void shift(const double dt)
  {
    PYBIND11_OVERLOAD(
      void,
      mspass::BasicTimeSeries,
      shift,
      dt);
  }
  void set_dt(const double sample_interval)
  {
    PYBIND11_OVERLOAD(
      void,
      mspass::BasicTimeSeries,
      set_dt,
      sample_interval);
  }
  void set_npts(const size_t npts)
  {
    PYBIND11_OVERLOAD(
      void,
      mspass::BasicTimeSeries,
      set_npts,
      npts);
  }
  void set_t0(const double d0in)
  {
    PYBIND11_OVERLOAD(
      void,
      mspass::BasicTimeSeries,
      set_t0,
      d0in);
  }
};

/* Trampoline class for BasicProcessingHistory - new for 2020 API change */

class PyBasicProcessingHistory : public BasicProcessingHistory
{
public:
  using BasicProcessingHistory::BasicProcessingHistory;
  size_t number_of_stages() override
  {
    PYBIND11_OVERLOAD(
      size_t,
      mspass::BasicProcessingHistory,
      number_of_stages,  //exta comma needed for reasons given in pybind docs
    );
  }
};


/* Special iterator data structure for python */
struct PyMetadataIterator {
    PyMetadataIterator(const Metadata &md, py::object ref) : md(md), ref(ref) { }

    std::string next() {
        if (index == md.end())
            throw py::stop_iteration();
        std::string key = index->first;
        ++index;
        return key;
    }

    const Metadata &md;
    py::object ref; // keep a reference
    std::map<std::string,boost::any>::const_iterator index = md.begin();
};

/* The following is needed for the binding to access protected members */
class Publicdmatrix : public mspass::dmatrix {
public:
    using mspass::dmatrix::ary;
    using mspass::dmatrix::length;
    using mspass::dmatrix::nrr;
    using mspass::dmatrix::ncc;
};

PYBIND11_MODULE(ccore,m)
{
  m.attr("__name__") = "mspasspy.ccore";

  /* We need one of these for each std::vector container to make them function correctly*/
  py::bind_vector<std::vector<double>>(m, "DoubleVector");
  py::bind_vector<std::vector<TimeSeries>>(m, "TimeSeriesVector");
  py::bind_vector<std::vector<Seismogram>>(m, "SeismogramVector");

  py::class_<mspass::SphericalCoordinate>(m,"SphericalCoordinate","Enscapsulates concept of spherical coordinates")
    .def(py::init<>())
    .def(py::init<const SphericalCoordinate&>())
    .def(py::init([](py::array_t<double> uv) {
      py::buffer_info info = uv.request();
      if (info.ndim != 1 || info.shape[0] != 3)
        throw py::value_error("SphericalCoordinate expects a vector of 3 elements");
      return mspass::UnitVectorToSpherical(static_cast<double*>(info.ptr));
    }))
    /* The use of capsule to avoid copy is found at
       https://github.com/pybind/pybind11/issues/1042 */
    .def_property_readonly("unit_vector", [](const SphericalCoordinate& self) {
      double* v = SphericalToUnitVector(self);
      auto capsule = py::capsule(v, [](void *v) { delete[] reinterpret_cast<double*>(v); });
      return py::array(3, v, capsule);
    },"Return the unit vector equivalent to direction defined in sphereical coordinates")
    .def_readwrite("radius", &mspass::SphericalCoordinate::radius,"R of spherical coordinates")
    .def_readwrite("theta", &mspass::SphericalCoordinate::theta,"zonal angle of spherical coordinates")
    .def_readwrite("phi", &mspass::SphericalCoordinate::phi,"azimuthal angle of spherical coordinates")
  ;

  py::class_<mspass::SlownessVector>(m,"SlownessVector","Encapsulate concept of slowness vector describing wave propagation")
    .def(py::init<>())
    .def(py::init<const SlownessVector&>())
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
  py::class_<mspass::Metadata,mspass::BasicMetadata> md(m,"Metadata");
  md.def(py::init<>())
    .def(py::init<const Metadata&>())
    .def(py::init<std::ifstream&,const std::string>())
    /* The order of the following type check matters. Note that due to
     * the pybind11's asymmetric conversion behavior from bytes to string,
     * we have to handle bytes before strings. The same applies to the
     * put and __setitem__ methods.
    */
    .def(py::init([](py::dict d) {
      auto md = new Metadata();
      for(auto i : d) {
        if(py::isinstance<py::float_>(i.second))
          md->put(std::string(py::str(i.first)), (i.second.cast<double>()));
        else if(py::isinstance<py::bool_>(i.second))
          md->put(std::string(py::str(i.first)), (i.second.cast<bool>()));
        else if(py::isinstance<py::int_>(i.second))
          md->put(std::string(py::str(i.first)), (i.second.cast<long>()));
        else if(py::isinstance<py::bytes>(i.second))
          md->put_object(std::string(py::str(i.first)), py::reinterpret_borrow<py::object>(i.second));
        else if(py::isinstance<py::str>(i.second))
          md->put(std::string(py::str(i.first)), std::string(py::str(i.second)));
        else
          md->put_object(std::string(py::str(i.first)), py::reinterpret_borrow<py::object>(i.second));
      }
      return md;
    }))
    .def("get_double",&mspass::Metadata::get_double,"Retrieve a real number by a specified key")
    .def("get_long",&mspass::Metadata::get_long,"Return a long integer by a specified key")
    .def("get_bool",&mspass::Metadata::get_bool,"Return a (C) boolean defined by a specified key")
    .def("get_string",&mspass::Metadata::get_string,"Return a string indexed by a specified key")
    .def("get",&mspass::Metadata::get_any,"Return the value indexed by a specified key")
    .def("__getitem__",&mspass::Metadata::get_any,"Return the value indexed by a specified key")
    .def("put", [](Metadata& md, const py::bytes k, const py::object v) {
      if(py::isinstance<py::float_>(v))
        md.put(std::string(py::str(k.attr("__str__")())), (v.cast<double>()));
      else if(py::isinstance<py::bool_>(v))
        md.put(std::string(py::str(k.attr("__str__")())), (v.cast<bool>()));
      else if(py::isinstance<py::int_>(v))
        md.put(std::string(py::str(k.attr("__str__")())), (v.cast<long>()));
      else if(py::isinstance<py::bytes>(v))
        md.put_object(std::string(py::str(k.attr("__str__")())), v);
      else if(py::isinstance<py::str>(v))
        md.put(std::string(py::str(k.attr("__str__")())), std::string(py::str(v)));
      else
        md.put_object(std::string(py::str(k.attr("__str__")())), v);
    })
    .def("put",py::overload_cast<const std::string,const double>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const bool>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const long>(&Metadata::put_long))
    .def("put",[](Metadata& md, const std::string k, const py::bytes v) {
        md.put_object(k, py::reinterpret_borrow<py::object>(v));
    })
    .def("put",py::overload_cast<const std::string,const std::string>(&BasicMetadata::put))
    .def("put",py::overload_cast<const std::string,const py::object>(&Metadata::put_object))
    .def("__setitem__", [](Metadata& md, const py::bytes k, const py::object v) {
      if(py::isinstance<py::float_>(v))
        md.put(std::string(py::str(k.attr("__str__")())), (v.cast<double>()));
      else if(py::isinstance<py::bool_>(v))
        md.put(std::string(py::str(k.attr("__str__")())), (v.cast<bool>()));
      else if(py::isinstance<py::int_>(v))
        md.put(std::string(py::str(k.attr("__str__")())), (v.cast<long>()));
      else if(py::isinstance<py::bytes>(v))
        md.put_object(std::string(py::str(k.attr("__str__")())), v);
      else if(py::isinstance<py::str>(v))
        md.put(std::string(py::str(k.attr("__str__")())), std::string(py::str(v)));
      else
        md.put_object(std::string(py::str(k.attr("__str__")())), v);
    })
    .def("__setitem__",py::overload_cast<const std::string,const double>(&BasicMetadata::put))
    .def("__setitem__",py::overload_cast<const std::string,const bool>(&BasicMetadata::put))
    .def("__setitem__",py::overload_cast<const std::string,const long>(&Metadata::put_long))
    .def("__setitem__",[](Metadata& md, const std::string k, const py::bytes v) {
        md.put_object(k, py::reinterpret_borrow<py::object>(v));
    })
    .def("__setitem__",py::overload_cast<const std::string,const std::string>(&BasicMetadata::put))
    .def("__setitem__",py::overload_cast<const std::string,const py::object>(&Metadata::put_object))
    .def("put_double",&Metadata::put_double,"Interface class for doubles")
    .def("put_bool",&Metadata::put_bool,"Interface class for boolean")
    .def("put_string",&Metadata::put_string,"Interface class for strings")
    .def("put_long",&Metadata::put_long,"Interface class for long ints")
    /* Intentionally do NOT enable put_int.   Found type skew problems if
     * called from python.   Best avoided.
    .def("put_int",&Metadata::put_int,"Interface class for generic ints")
    */
    .def("keys",&Metadata::keys,"Return a list of the keys of all defined attributes")
    .def("type",[](const Metadata &md, const std::string &key) -> std::string {
      std::string typ = md.type(key);
      if(typ == "pybind11::object")
        return py::str(boost::any_cast<pybind11::object>(md.get_any(key)).get_type());
      else if (typ.substr(0,26) == "std::__cxx11::basic_string")
        return std::string("string");
      else
        return typ;
    },"Return a demangled typename for value associated with a key")
    .def("modified",&Metadata::modified,"Return a list of all attributes that have been changes since construction")
    .def("clear_modified",&Metadata::clear_modified,"Clear container used to mark altered Metadata")
    /*  For unknown reasons could not make this overload work.
     *  Ended up commenting out char * section of C++ code - baggage in python
     *  anyway.
    .def("is_defined",py::overload_cast<const std::string>(&Metadata::is_defined))
    .def("is_defined",py::overload_cast<const char *>(&Metadata::is_defined))
    */
    .def("is_defined",&Metadata::is_defined,"Test if a key has a defined value")
    .def("__contains__",&Metadata::is_defined,"Test if a key has a defined value")
    .def("append_chain",&Metadata::append_chain,"Create or append to a string attribute that defines a chain")
    .def("clear",&Metadata::clear,"Clears contents associated with a key")
    .def("__delitem__",&Metadata::clear,"Clears contents associated with a key")
    .def("__len__",&Metadata::size,"Return len(self)")
    .def("__iter__", [](py::object s) { return PyMetadataIterator(s.cast<const Metadata &>(), s); })
    .def("__reversed__", [](const Metadata &s) -> Metadata {
      throw py::type_error(std::string("'") +
        py::cast(s).attr("__class__").attr("__name__").cast<std::string>() +
        "' object is not reversible");
    })
    .def("__str__", [](const Metadata &s) -> std::string {
      if(s.size() == 0)
        return "{}";
      std::string strout("{");
      for(auto index = s.begin(); index != s.end(); ++index) {
        std::string key = index->first;
        key = std::string(py::repr(py::cast(key)));
        if(index->second.type() == typeid(py::object)) {
          py::str val = py::repr(boost::any_cast<py::object>(index->second));
          key = key + ": " + std::string(val) + ", ";
        }
        else if (index->second.type() == typeid(double))
          key = key + ": " + std::to_string(boost::any_cast<double>(index->second)) + ", ";
        else if (index->second.type() == typeid(bool) &&
                 boost::any_cast<bool>(index->second) == true)
          key = key + ": True, ";
        else if (index->second.type() == typeid(bool) &&
                 boost::any_cast<bool>(index->second) == false)
          key = key + ": False, ";
        else if (index->second.type() == typeid(long))
          key = key + ": " + std::to_string(boost::any_cast<long>(index->second)) + ", ";
        /* The py::repr function will get the double/single
         * quotes right based on the content of the string */
        else
          key = key + ": " + std::string(py::repr(py::cast(
                boost::any_cast<string>(index->second)))) + ", ";
        strout += key;
      }
      strout.pop_back();
      strout.pop_back();
      return strout + "}";
    })
    .def("__repr__", [](const Metadata &s) -> std::string {
      return py::cast(s).attr("__class__").attr("__name__").cast<std::string>() +
          "(" + std::string(py::str(py::cast(s).attr("__str__")())) + ")";
    })
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
  py::class_<PyMetadataIterator>(md, "Metadata_keyIterator")
    .def("__iter__", [](PyMetadataIterator &it) -> PyMetadataIterator& { return it; })
    .def("__next__", &PyMetadataIterator::next);

  py::class_<mspass::AntelopePf,Metadata>(m,"AntelopePf")
    .def(py::init<>())
    .def(py::init<const AntelopePf&>())
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
/* Intentionally ommit the following from python bindings:
  timetype method
  set_tref
  */
  py::class_<mspass::BasicTimeSeries,PyBasicTimeSeries>(m,"BasicTimeSeries","Core common concepts for uniformly sampled 1D data")
    .def(py::init<>())
    .def("time",&mspass::BasicTimeSeries::time,"Return the computed time for a sample number (integer)")
    .def("sample_number",&mspass::BasicTimeSeries::sample_number,"Return the sample index number for a specified time")
    .def("endtime",&mspass::BasicTimeSeries::endtime,"Return the (computed) end time of a time series")
    .def("shifted",&mspass::BasicTimeSeries::shifted,"Return True if the data have been time shifted to relative time")
    .def("rtoa",py::overload_cast<const double>(&mspass::BasicTimeSeries::rtoa))
    .def("rtoa",py::overload_cast<>(&mspass::BasicTimeSeries::rtoa))
    .def("ator",&mspass::BasicTimeSeries::ator,"Switch time standard from absolute (UTC) to a relative time scale")
    .def("shift",&mspass::BasicTimeSeries::shift,"Shift time reference by a specified number of seconds")
    .def("time_reference",&mspass::BasicTimeSeries::time_reference,"Return time standard")
    .def("shifted",&mspass::BasicTimeSeries::shifted,"Return true if data are UTC standard with a time shift applied")
    .def("force_t0_shift",&mspass::BasicTimeSeries::force_t0_shift,"Force a time shift value to make data shifted UTC in relative time")
    .def("live",&mspass::BasicTimeSeries::live,"Return True if the data are marked valid (not dead)")
    .def("dead",&mspass::BasicTimeSeries::dead,"Return true if the data are marked bad and should not be used")
    .def("kill",&mspass::BasicTimeSeries::kill,"Mark this data object bad = dead")
    .def("set_live",&mspass::BasicTimeSeries::set_live,"Undo a kill (mark data ok)")
    .def("dt",&mspass::BasicTimeSeries::dt,"Return the sample interval (normally in second)")
    .def("samprate",&mspass::BasicTimeSeries::samprate,"Return the sample rate (usually in Hz)")
    .def("time_is_UTC",&mspass::BasicTimeSeries::time_is_UTC,"Return true if t0 is a UTC epoch time")
    .def("time_is_relative",&mspass::BasicTimeSeries::time_is_relative,"Return true if t0 is not UTC=some relative time standard like shot time")
    .def("npts",&mspass::BasicTimeSeries::npts,"Return the number of time samples in this object")
    .def("t0",&mspass::BasicTimeSeries::t0,"Return the time of the first sample of data in this time series")
    .def("set_dt",&mspass::BasicTimeSeries::set_dt,"Set the data time sample interval")
    .def("set_npts",&mspass::BasicTimeSeries::set_npts,"Set the number of data samples in this object")
    .def("set_t0",&mspass::BasicTimeSeries::set_t0,"Set time of sample 0 (t0) - does not check if consistent with time standard")
    .def_property("npts",[](const mspass::BasicTimeSeries &self) {
        return self.npts();
      },[](mspass::BasicTimeSeries &self, size_t npts) {
        self.set_npts(npts);
      },"Number of samples in this object")
    .def_property("t0",[](const mspass::BasicTimeSeries &self) {
        return self.t0();
      },[](mspass::BasicTimeSeries &self, double t0) {
        self.set_t0(t0);
      },"The time of the first sample of data in this object")
    .def_property("dt",[](const mspass::BasicTimeSeries &self) {
        return self.dt();
      },[](mspass::BasicTimeSeries &self, double dt) {
        self.set_dt(dt);
      },"The sample interval (normally in second)")
    .def_property("live",[](const mspass::BasicTimeSeries &self) {
        return self.live();
      },[](mspass::BasicTimeSeries &self, bool b) {
        if(b)
          self.set_live();
        else
          self.kill();
      },"Whether the data is valid or not")
    .def_property("tref",[](const mspass::BasicTimeSeries &self) {
        return self.timetype();
      },[](mspass::BasicTimeSeries &self, TimeReferenceType tref) {
        self.set_tref(tref);
      },"Time reference standard for this data object")
  ;
  py::class_<mspass::CoreTimeSeries,mspass::BasicTimeSeries,mspass::Metadata>(m,"CoreTimeSeries","Defines basic concepts of a scalar time series")
    .def(py::init<>())
    .def(py::init<const CoreTimeSeries&>())
    .def(py::init<const size_t>())
    .def("set_dt",&CoreTimeSeries::set_dt,
      "Set data sample interval (overrides BasicTimeSeries virtual method)")
    .def("set_npts",&CoreTimeSeries::set_npts,
      "Set data number of samples (overrides BasicTimeSeries virtual method)")
    .def("set_t0",&CoreTimeSeries::set_t0,
      "Set data definition of time of sample 0 (overrides BasicTimeSeries virtual method)")
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
    .def(py::init<size_t,size_t>())
    .def(py::init<const dmatrix&>())
    /* This is the copy constructor wrapper */
    .def(py::init([](py::array_t<double, py::array::f_style | py::array::forcecast> b) {
      py::buffer_info info = b.request();
      if (info.ndim != 2)
        throw std::runtime_error("dmatrix python wrapper:  Incompatible buffer dimension!");
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
    .def(py::self + py::self,"Operator +")
    .def("__add__", [](const dmatrix &a, py::object b) {
      return py::module::import("mspasspy.ccore").attr("dmatrix")(
        py::cast(a).attr("__getitem__")(py::reinterpret_steal<py::slice>(
        PySlice_New(Py_None, Py_None, Py_None))).attr("__add__")(b));
    })
    .def(py::self - py::self,"Operator -")
    .def("__sub__", [](const dmatrix &a, py::object b) {
      return py::module::import("mspasspy.ccore").attr("dmatrix")(
        py::cast(a).attr("__getitem__")(py::reinterpret_steal<py::slice>(
        PySlice_New(Py_None, Py_None, Py_None))).attr("__sub__")(b));
    })
    .def(py::self += py::self,"Operator +=")
    .def(py::self -= py::self,"Operator -=")
    .def("__getitem__", [](dmatrix &m, py::slice slice) {
      size_t start, stop, step, slicelength;
      if (!slice.compute(m.rows(), &start, &stop, &step, &slicelength))
        throw py::error_already_set();
      double* packet;
      try{
        packet = m.get_address(start,0);
      } catch (MsPASSError e) {
        packet = nullptr;
      }
      std::vector<ssize_t> size(2);
      size[0] = slicelength;
      size[1] = m.columns();
      std::vector<ssize_t> stride(2);
      stride[0] = sizeof(double) * step;
      stride[1] = sizeof(double) * m.rows();
      // The following undocumented trick is from
      // https://github.com/pybind/pybind11/issues/323
      py::str dummyDataOwner;
      py::array rows(py::dtype(py::format_descriptor<double>::format()), size,
                    stride, packet, dummyDataOwner);
      return rows;
    })
    .def("__getitem__", [](const dmatrix &m, std::pair<int, int> i) {
      return py::cast(m).attr("__getitem__")(py::reinterpret_steal<py::slice>(
        PySlice_New(Py_None, Py_None, Py_None))).attr("__getitem__")(i);
    })
    .def("__getitem__", [](dmatrix &m, int i) {
      return py::cast(m).attr("__getitem__")(py::reinterpret_steal<py::slice>(
        PySlice_New(Py_None, Py_None, Py_None))).attr("__getitem__")(i);
    })
    .def("__getitem__", [](const dmatrix &m, std::pair<py::slice, int> i) {
      return py::cast(m).attr("__getitem__")(py::reinterpret_steal<py::slice>(
        PySlice_New(Py_None, Py_None, Py_None))).attr("__getitem__")(i);
    })
    .def("__getitem__", [](const dmatrix &m, std::pair<int, py::slice> i) {
      return py::cast(m).attr("__getitem__")(py::reinterpret_steal<py::slice>(
        PySlice_New(Py_None, Py_None, Py_None))).attr("__getitem__")(i);
    })
    .def("__getitem__", [](const dmatrix &m, std::pair<py::slice, py::slice> i) {
      return py::cast(m).attr("__getitem__")(py::reinterpret_steal<py::slice>(
        PySlice_New(Py_None, Py_None, Py_None))).attr("__getitem__")(i);
    })
    .def("__setitem__", [](dmatrix &m, int i, py::object const b) {
      py::cast(m).attr("__getitem__")(py::reinterpret_steal<py::slice>(
        PySlice_New(Py_None, Py_None, Py_None))).attr("__setitem__")(i, b);
    })
    .def("__setitem__", [](dmatrix &m, py::slice i, py::object const b) {
      py::cast(m).attr("__getitem__")(py::reinterpret_steal<py::slice>(
        PySlice_New(Py_None, Py_None, Py_None))).attr("__setitem__")(i, b);
    })
    .def("__setitem__", [](dmatrix &m, std::pair<int, int> i, py::object const b) {
      py::cast(m).attr("__getitem__")(py::reinterpret_steal<py::slice>(
        PySlice_New(Py_None, Py_None, Py_None))).attr("__setitem__")(i, b);
    })
    .def("__setitem__", [](dmatrix &m, std::pair<py::slice, int> i, py::object const b) {
      py::cast(m).attr("__getitem__")(py::reinterpret_steal<py::slice>(
        PySlice_New(Py_None, Py_None, Py_None))).attr("__setitem__")(i, b);
    })
    .def("__setitem__", [](dmatrix &m, std::pair<int, py::slice> i, py::object const b) {
      py::cast(m).attr("__getitem__")(py::reinterpret_steal<py::slice>(
        PySlice_New(Py_None, Py_None, Py_None))).attr("__setitem__")(i, b);
    })
    .def("__setitem__", [](dmatrix &m, std::pair<py::slice, py::slice> i, py::object const b) {
      py::cast(m).attr("__getitem__")(py::reinterpret_steal<py::slice>(
        PySlice_New(Py_None, Py_None, Py_None))).attr("__setitem__")(i, b);
    })
    .def("__str__", [](const dmatrix &m) -> std::string {
      return std::string(py::str(py::cast(m).attr("__getitem__")(py::reinterpret_steal<py::slice>(
        PySlice_New(Py_None, Py_None, Py_None))).attr("__str__")()));;
    })
    .def("__repr__", [](const dmatrix &m) -> std::string {
      std::string strout("dmatrix(");
      strout += std::string(py::str(py::cast(m).attr("__str__")())) + ")";
      size_t pos = strout.find('\n');
      while(pos != string::npos)
      {
        strout.insert(++pos, 8, ' ');
        pos = strout.find('\n', pos);
      }
      return strout;
    })
  ;
  py::class_<mspass::CoreSeismogram,mspass::BasicTimeSeries,mspass::Metadata>(m,"CoreSeismogram","Defines basic concepts of a three-component seismogram")
    .def(py::init<>())
    .def(py::init<const CoreSeismogram&>())
    .def(py::init<const size_t>())
    .def(py::init<const std::vector<mspass::CoreTimeSeries>&,const unsigned int>())
    .def(py::init<const mspass::Metadata&,const bool>(),"Construct from Metadata with read from file option")
    .def("set_dt",&CoreSeismogram::set_dt,
      "Set data sample interval (overrides BasicTimeSeries virtual method)")
    .def("set_npts",&CoreSeismogram::set_npts,
      "Set data number of samples (overrides BasicTimeSeries virtual method)")
    .def("set_t0",&CoreSeismogram::set_t0,
      "Set data definition of time of sample 0 (overrides BasicTimeSeries virtual method)")
    .def("endtime",&mspass::CoreSeismogram::endtime,"Return the (computed) end time of a time series")
    .def("rotate_to_standard",&CoreSeismogram::rotate_to_standard,"Transform data to cardinal coordinates")
    .def("rotate",py::overload_cast<SphericalCoordinate&>(&CoreSeismogram::rotate),"3D rotation defined by spherical coordinate angles")
    .def("rotate",py::overload_cast<const double>(&CoreSeismogram::rotate),"2D rotation about the vertical axis")
    .def("rotate",[](CoreSeismogram &self, py::array_t<double> tm) {
      py::buffer_info info = tm.request();
      if (info.ndim != 1 || info.shape[0] != 3)
        throw py::value_error("rotate expects a vector of 3 elements");
      self.rotate(static_cast<double*>(info.ptr));
    },"3D rotation defined a unit vector direction")
    .def("transform", [](CoreSeismogram &self, py::array_t<double, py::array::c_style | py::array::forcecast> tm) {
      py::buffer_info info = tm.request();
      if (info.ndim != 2 || info.shape[0] != 3 || info.shape[1] != 3)
        throw py::value_error("transform expects a 3x3 matrix");
      self.transform(static_cast<double(*)[3]>(info.ptr));
    },"Applies an arbitrary transformation matrix to the data")
    .def("free_surface_transformation",&CoreSeismogram::free_surface_transformation,"Apply free surface transformation operator to data")
    .def_property("transformation_matrix",
      [](const CoreSeismogram &self){
        dmatrix tm = self.get_transformation_matrix();
        auto v = static_cast<Publicdmatrix&>(tm).ary;
        std::vector<double>* c = new std::vector<double>(std::move(v));
        auto capsule = py::capsule(c, [](void *x) { delete reinterpret_cast<std::vector<double>*>(x); });
        std::vector<ssize_t> size(2,3);
        std::vector<ssize_t> stride(2);
        stride[0] = sizeof(double);
        stride[1] = sizeof(double) * 3;
        return py::array(py::dtype(py::format_descriptor<double>::format()), size, stride, c->data(), capsule);
      },
      [](CoreSeismogram &self, py::array_t<double, py::array::c_style | py::array::forcecast> tm) {
        py::buffer_info info = tm.request();
        if (info.ndim != 2 || info.shape[0] != 3 || info.shape[1] != 3)
          throw py::value_error("transform expects a 3x3 matrix");
        self.set_transformation_matrix(static_cast<double(*)[3]>(info.ptr));
      },"3x3 transformation matrix")
    .def(py::self += py::self)
    /* Place holder for data array.   Probably want this exposed through
    Seismogram api */
    .def_readwrite("u",&CoreSeismogram::u)
    /*
    .def_property("ns",[](const CoreSeismogram &self) {
      return self.u.columns();
    },[](CoreSeismogram &self, size_t columns) {
      if(self.u.rows() == 0) {
        static_cast<Publicdmatrix&>(self.u).nrr = 3;
        static_cast<Publicdmatrix&>(self.u).ncc = columns;
        static_cast<Publicdmatrix&>(self.u).length = 3 * columns;
        static_cast<Publicdmatrix&>(self.u).ary.resize(static_cast<Publicdmatrix&>(self.u).length);
      } else if(columns == 0){
        static_cast<Publicdmatrix&>(self.u).nrr = 0;
        static_cast<Publicdmatrix&>(self.u).ncc = 0;
        static_cast<Publicdmatrix&>(self.u).length = 0;
        static_cast<Publicdmatrix&>(self.u).ary.resize(0);
      } else {
        static_cast<Publicdmatrix&>(self.u).ncc = columns;
        static_cast<Publicdmatrix&>(self.u).length = self.u.rows() * columns;
        static_cast<Publicdmatrix&>(self.u).ary.resize(static_cast<Publicdmatrix&>(self.u).length);
      }
    },"Number of samples in this time series")
    */
  ;

  m.def("ArrivalTimeReference",
      py::overload_cast<mspass::Seismogram&,std::string,mspass::TimeWindow>
          (&mspass::ArrivalTimeReference),
          "Shifts data so t=0 is a specified arrival time",
      py::return_value_policy::copy,
      py::arg("d"),
      py::arg("key"),
      py::arg("window")
  );
  m.def("ArrivalTimeReference",
      py::overload_cast<mspass::Ensemble<mspass::Seismogram>&,std::string,mspass::TimeWindow>
          (&mspass::ArrivalTimeReference),
          "Shifts data so t=0 is a specified arrival time",
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
    .def(py::init<const MsPASSError&>())
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
    .def(py::init<int,std::string,std::string,mspass::ErrorSeverity>())
    .def(py::init([](py::dict d) {
      auto ld = new LogData();
      for(auto i : d) {
        if(std::string(py::str(i.first)) == "job_id")
          ld->job_id = i.second.cast<long>();
        else if(std::string(py::str(i.first)) == "p_id")
          ld->p_id = i.second.cast<long>();
        else if(std::string(py::str(i.first)) == "algorithm")
          ld->algorithm = i.second.cast<std::string>();
        else if(std::string(py::str(i.first)) == "message")
          ld->message = i.second.cast<std::string>();
        else if(std::string(py::str(i.first)) == "badness")
          ld->badness = i.second.cast<mspass::ErrorSeverity>();
      }
      return ld;
    }))
    .def_readwrite("job_id",&LogData::job_id,"Return the job id defined for this log message")
    .def_readwrite("p_id",&LogData::p_id,"Return the process id of the procedure that threw the defined message")
    .def_readwrite("algorithm",&LogData::algorithm,"Return the algorithm of the procedure that threw the defined message")
    .def_readwrite("badness",&LogData::badness,"Return a error level code")
    .def_readwrite("message",&LogData::message,"Return the actual posted message")
    .def("__str__", [](const LogData &ld) -> std::string {
      return std::string("{'job_id': ") + std::to_string(ld.job_id) +
        ", 'p_id': " + std::to_string(ld.p_id) +
        ", 'algorithm': " + ld.algorithm +
        ", 'message': " + ld.message + ", 'badness': " +
        std::string(py::str(py::cast(ld.badness))) + "}";
    })
    .def("__repr__", [](const LogData &ld) -> std::string {
      std::string strout("LogData(");
      return strout + std::string(py::str(py::cast(ld).attr("__str__")())) + ")";
    })
  ;
  py::class_<mspass::ErrorLogger>(m,"ErrorLogger","Used to post any nonfatal errors without aborting a program of family of parallel programs")
    .def(py::init<>())
    .def(py::init<const ErrorLogger&>())
    .def(py::init<int>())
    .def("set_job_id",&mspass::ErrorLogger::set_job_id)
    .def("get_job_id",&mspass::ErrorLogger::get_job_id)
    .def("log_error",py::overload_cast<const mspass::MsPASSError&>(&mspass::ErrorLogger::log_error),"log error thrown as MsPASSError")
    .def("log_error",py::overload_cast<const std::string,const std::string,const mspass::ErrorSeverity>(&mspass::ErrorLogger::log_error),"log a message at a specified severity level")
    .def("log_verbose",&mspass::ErrorLogger::log_verbose,"Log an informational message - tagged as log message")
    .def("get_error_log",&mspass::ErrorLogger::get_error_log,"Return all posted entries")
    .def("size",&mspass::ErrorLogger::size,"Return number of entries in this log")
    .def("__len__",&mspass::ErrorLogger::size,"Return number of entries in this log")
    .def("worst_errors",&mspass::ErrorLogger::worst_errors,"Return a list of only the worst errors")
    .def("__getitem__", [](ErrorLogger &self, size_t i) {
      return py::cast(self).attr("get_error_log")().attr("__getitem__")(i);
    })
  ;
  /* New classes in 2020 API revision - object level history preservation */
  py::enum_<mspass::ProcessingStatus>(m,"ProcessingStatus")
    .value("RAW",mspass::ProcessingStatus::RAW)
    .value("ORIGIN",mspass::ProcessingStatus::ORIGIN)
    .value("VOLATILE",mspass::ProcessingStatus::VOLATILE)
    .value("SAVED",mspass::ProcessingStatus::SAVED)
    .value("UNDEFINED",mspass::ProcessingStatus::UNDEFINED)
  ;

  py::class_<mspass::BasicProcessingHistory,PyBasicProcessingHistory>
  //py::class_<mspass::BasicProcessingHistory>
      (m,"BasicProcessingHistory","Base class - hold job history data")
    .def(py::init<>())
    .def("jobid",&mspass::BasicProcessingHistory::jobid,
      "Return job id string")
    .def("jobname",&mspass::BasicProcessingHistory::jobname,
      "Return job name string defining main python script driving this processing chain")
    .def("set_jobid",&mspass::BasicProcessingHistory::set_jobid,
      "Set a unique id so jobname + id is unique")
    .def("set_jobname",&mspass::BasicProcessingHistory::set_jobname,
      "Set the base job name defining the main python script for this run")
  ;


  py::class_<mspass::ProcessingHistory,mspass::BasicProcessingHistory>
    (m,"ProcessingHistory","Used to save object level processing history.")
    .def(py::init<>())
    .def(py::init<const std::string,const std::string>())
    .def(py::init<const ProcessingHistory&>())
    .def("is_raw",&mspass::ProcessingHistory::is_raw,
      "Return True if the data are raw data with no previous processing")
    .def("is_origin",&mspass::ProcessingHistory::is_origin,
       "Return True if the data are marked as an origin - commonly an intermediate save")
    .def("is_volatile",&mspass::ProcessingHistory::is_volatile,
      "Return True if the data are unsaved, partially processed data")
    .def("is_saved",&mspass::ProcessingHistory::is_saved,
      "Return True if the data are saved and history can be cleared")
    .def("number_of_stages",&mspass::ProcessingHistory::number_of_stages,
      "Return count of the number of processing steps applied so far")
    //.def("current_stage",&mspass::ProcessingHistory::current_stage,
    //  "Return the data defining the current stage as ProcessingHistoryRecord")
    .def("set_as_origin",&mspass::ProcessingHistory::set_as_origin,
      "Load data defining this as the top of a processing history chain")
      // Here need an alias set_as_raw that make set_as_origin define as raw
    .def("new_reduction",&mspass::ProcessingHistory::new_reduction,
      "Set up history chain to define the current data as result of reduction - output form mulitple inputs")
    .def("new_map",&mspass::ProcessingHistory::new_map,
      "Set history chain to define the current data as a one-to-one map from parent")
    .def("add_one_input",&mspass::ProcessingHistory::add_one_input,
      "Companion to new_reduction used to add a single input datum after call to new_reduction")
    .def("add_many_inputs",&mspass::ProcessingHistory::add_many_inputs,
      "Companion to new_reduction used to add a single input datum after call to new_reduction")
    .def("map_as_saved",&mspass::ProcessingHistory::map_as_saved,
      "Load data defining this as the end of chain that was or will soon be saved")
    .def("clear",&mspass::ProcessingHistory::clear,
      "Clear this history chain - use with caution")
    .def("stage",&mspass::ProcessingHistory::stage,
      "Return the current stage number (counter of processing stages applied in this run)")
    .def("id",&mspass::ProcessingHistory::id,"Return current uuid")
    .def("newid",&mspass::ProcessingHistory::newid,"Create a new uuid for current data")
    .def("set_id",&mspass::ProcessingHistory::set_id,"Set current uuid to valued passed")
    .def("algorithm_history",&mspass::ProcessingHistory::algorithm_history,
      "Return a list of all algorithms applied so far to produce current data object")
    .def("data_processed_by",&mspass::ProcessingHistory::data_processed_by,
      "Return a list of the uuids of all data processed by a specified algorithm instance")
    .def("inputs",&mspass::ProcessingHistory::inputs,
      "Return a list of uuids of all data that were inputs to defined uuid (current or any ancestor)")
  ;


  py::class_<mspass::Seismogram,mspass::CoreSeismogram,mspass::ProcessingHistory>
                                                (m,"Seismogram")
    .def(py::init<>())
    .def(py::init<const CoreSeismogram&>())
    .def(py::init<const CoreSeismogram&,const std::string>())
    /* Don't think we really want to expose this to python if we don't need to
    .def(py::init<const BasicTimeSeries&,const Metadata&, const CoreSeismogram,
      const ProcessingHistory&, const ErrorLogger&,
      const bool,const bool, const dmatrix&,const dmatrix&>())
      */
    .def(py::init<const Metadata&,std::string,std::string,std::string,std::string>())
    .def(py::init<const Seismogram&>())
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
        arcorets<<dynamic_cast<const ProcessingHistory&>(self);
        // these are behind getter/setters
        bool cardinal=self.cardinal();
        bool orthogonal=self.orthogonal();
        dmatrix tmatrix=self.get_transformation_matrix();
        stringstream sstm;
        boost::archive::text_oarchive artm(sstm);
        artm<<tmatrix;
        // This is a very slow solution, but using the axiom make it work
        //before you make it fast
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
       ProcessingHistory corets;
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

    py::class_<mspass::TimeSeries,mspass::CoreTimeSeries,mspass::ProcessingHistory>(m,"TimeSeries","mspass scalar time series data object")
      .def(py::init<>())
      .def(py::init<const CoreTimeSeries&>())
      .def(py::init<const TimeSeries&>())
      .def(py::init<const mspass::CoreTimeSeries&,const std::string>())
      // Not sure this constructor needs to be exposed to python
      /*
      .def(py::init<const mspass::BasicTimeSeries&,const mspass::Metadata&,
        const ProcessingHistory&, const std::vector&)
        */

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
          arcorets<<dynamic_cast<const ProcessingHistory&>(self);
          //This creates a numpy array alias from the vector container
          //without a move or copy of the data
          py::array_t<double, py::array::f_style> darr(self.s.size(),&(self.s[0]));
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
         ProcessingHistory corets;
         arcorets>>corets;
         // There might be a faster way to do this than a copy like
         //this but for now this, like Seismogram, is make it work before you
         //make it fast
         py::array_t<double, py::array::f_style> darr;
         darr=t[3].cast<py::array_t<double, py::array::f_style>>();
         py::buffer_info info = darr.request();
         std::vector<double> d;
         d.resize(info.shape[0]);
         memcpy(d.data(), info.ptr, sizeof(double) * d.size());
         return TimeSeries(bts,md,corets,d);;
       }
     ))
     ;
  /* Wrappers for Ensemble containers. With pybind11 we need to explicitly declare the types to
     be supported by the container.  Hence, we have two nearly identical blocks below for TimeSeries
     and Seismogram objects.  May want to add CoreTimeSeries and CoreSeismogram objects, but for now
     we will only suport top level objects.

     Note also the objects are stored in and std::vector container with the name member.  It appears
     the index operator is supported out of the box with pybind11 wrapprs so constructs like member[i]
     will be handled. */
  py::class_<mspass::Ensemble<TimeSeries>,mspass::Metadata>(m,"TimeSeriesEnsemble","Gather of scalar time series objects")
    .def(py::init<>())
    .def(py::init<const size_t >())
    .def(py::init<const Metadata&, const size_t>())
    .def(py::init<const Ensemble<TimeSeries>&>())
    .def("update_metadata",&mspass::Ensemble<TimeSeries>::update_metadata,"Update the ensemble header (metadata)")
    .def("sync_metadata",&mspass::Ensemble<TimeSeries>::sync_metadata,"Copy ensemble metadata to all members")
    // Note member is an std::container - requires py::bind_vector lines at the start of this module defintions
    //    to function properlty
    .def_readwrite("member",&mspass::Ensemble<TimeSeries>::member,
            "Vector of TimeSeries objects defining the ensemble")
    .def("__getitem__", [](mspass::Ensemble<TimeSeries> &self, const size_t i) {
      return self.member.at(i);
    })
    .def("__getitem__",&mspass::Metadata::get_any)
    .def("__setitem__", [](mspass::Ensemble<TimeSeries> &self, const size_t i, const TimeSeries ts) {
      self.member.at(i) = ts;
    })
    // Based on this issue: https://github.com/pybind/pybind11/issues/974
    // there seems to be no clean solution for the following code duplicate.
    // Except that the following lambda function could be replaced with a
    // reusable function. (Some thing TODO)
    .def("__setitem__", [](Metadata& md, const py::bytes k, const py::object v) {
      if(py::isinstance<py::float_>(v))
        md.put(std::string(py::str(k.attr("__str__")())), (v.cast<double>()));
      else if(py::isinstance<py::bool_>(v))
        md.put(std::string(py::str(k.attr("__str__")())), (v.cast<bool>()));
      else if(py::isinstance<py::int_>(v))
        md.put(std::string(py::str(k.attr("__str__")())), (v.cast<long>()));
      else if(py::isinstance<py::bytes>(v))
        md.put_object(std::string(py::str(k.attr("__str__")())), v);
      else if(py::isinstance<py::str>(v))
        md.put(std::string(py::str(k.attr("__str__")())), std::string(py::str(v)));
      else
        md.put_object(std::string(py::str(k.attr("__str__")())), v);
    })
    .def("__setitem__",py::overload_cast<const std::string,const double>(&BasicMetadata::put))
    .def("__setitem__",py::overload_cast<const std::string,const bool>(&BasicMetadata::put))
    .def("__setitem__",py::overload_cast<const std::string,const long>(&Metadata::put_long))
    .def("__setitem__",[](Metadata& md, const std::string k, const py::bytes v) {
        md.put_object(k, py::reinterpret_borrow<py::object>(v));
    })
    .def("__setitem__",py::overload_cast<const std::string,const std::string>(&BasicMetadata::put))
    .def("__setitem__",py::overload_cast<const std::string,const py::object>(&Metadata::put_object))
  ;
  py::class_<mspass::Ensemble<Seismogram>,mspass::Metadata>(m,"SeismogramEnsemble","Gather of vector(3c) time series objects")
    .def(py::init<>())
    .def(py::init<const size_t >())
    .def(py::init<const Metadata&, const size_t>())
    .def(py::init<const Ensemble<Seismogram>&>())
    .def("update_metadata",&mspass::Ensemble<Seismogram>::update_metadata,"Update the ensemble header (metadata)")
    .def("sync_metadata",&mspass::Ensemble<Seismogram>::sync_metadata,"Copy ensemble metadata to all members")
    // Note member is an std::container - requires py::bind_vector lines at the start of this module defintions
    //    to function properlty
    .def_readwrite("member",&mspass::Ensemble<Seismogram>::member,
            "Vector of Seismogram objects defining the ensemble")
    .def("__getitem__", [](mspass::Ensemble<Seismogram> &self, const size_t i) {
      return self.member.at(i);
    })
    .def("__getitem__",&mspass::Metadata::get_any)
    .def("__setitem__", [](mspass::Ensemble<Seismogram> &self, const size_t i, const Seismogram ts) {
      self.member.at(i) = ts;
    })
    .def("__setitem__", [](Metadata& md, const py::bytes k, const py::object v) {
      if(py::isinstance<py::float_>(v))
        md.put(std::string(py::str(k.attr("__str__")())), (v.cast<double>()));
      else if(py::isinstance<py::bool_>(v))
        md.put(std::string(py::str(k.attr("__str__")())), (v.cast<bool>()));
      else if(py::isinstance<py::int_>(v))
        md.put(std::string(py::str(k.attr("__str__")())), (v.cast<long>()));
      else if(py::isinstance<py::bytes>(v))
        md.put_object(std::string(py::str(k.attr("__str__")())), v);
      else if(py::isinstance<py::str>(v))
        md.put(std::string(py::str(k.attr("__str__")())), std::string(py::str(v)));
      else
        md.put_object(std::string(py::str(k.attr("__str__")())), v);
    })
    .def("__setitem__",py::overload_cast<const std::string,const double>(&BasicMetadata::put))
    .def("__setitem__",py::overload_cast<const std::string,const bool>(&BasicMetadata::put))
    .def("__setitem__",py::overload_cast<const std::string,const long>(&Metadata::put_long))
    .def("__setitem__",[](Metadata& md, const std::string k, const py::bytes v) {
        md.put_object(k, py::reinterpret_borrow<py::object>(v));
    })
    .def("__setitem__",py::overload_cast<const std::string,const std::string>(&BasicMetadata::put))
    .def("__setitem__",py::overload_cast<const std::string,const py::object>(&Metadata::put_object))
  ;
  /* This object is in a separate pair of files in this directory.  */
  py::class_<mspass::MongoDBConverter>(m,"MongoDBConverter","Metadata translator from C++ object to python")
      .def(py::init<>())
      /* We intentionally do no wrap the copy constructor as this
         thing should probably not be copied in a python script */
      //.def(py::init<const mspass::MetadataDefinitions>())
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
  /* These are a pair of (four actually - overloaded) procedures to aid
  python programs in building history chains.  See C++ doxygen definitions */
/* Temporarily disabled
  m.def("append_input",&mspass::append_input<TimeSeries>,
    "Use the history chain of a TimeSeries to define it as an input for an algorithm to define ProcessingHistory",
    py::return_value_policy::copy,
    py::arg("rec"),
    py::arg("d") )
  ;
  m.def("append_input",&mspass::append_input<Seismogram>,
    "Use the history chain of a Seismogram to define it as an input for an algorithm to define ProcessingHistory",
    py::return_value_policy::copy,
    py::arg("rec"),
    py::arg("d") )
  ;
  m.def("set_inputs",&mspass::set_inputs<TimeSeries>,
    "Set inputs in a history chain when inputs are an Ensemble of TimeSeries objects",
    py::return_value_policy::copy,
    py::arg("rec"),
    py::arg("d") )
  ;
  m.def("set_inputs",&mspass::set_inputs<Seismogram>,
    "Set inputs in a history chain when inputs are an Ensemble of Seismogram objects",
    py::return_value_policy::copy,
    py::arg("rec"),
    py::arg("d") )
  ;
  */

}
