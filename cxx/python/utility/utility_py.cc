#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <pybind11/operators.h>

#include <boost/archive/text_oarchive.hpp>

#include <mspass/utility/AntelopePf.h>
#include <mspass/utility/AttributeMap.h>
#include <mspass/utility/dmatrix.h>
#include <mspass/utility/Metadata.h>
#include <mspass/utility/MetadataDefinitions.h>
#include <mspass/utility/ProcessingHistory.h>
#include <mspass/utility/SphericalCoordinate.h>

#include "python/utility/Publicdmatrix_py.h"
#include "python/utility/boost_any_converter_py.h"

namespace mspass {
namespace mspasspy {

namespace py=pybind11;
using namespace std;
using namespace mspass::utility;

/* This is what the pybind11 documentation calls a trampoline class for
needed to handle virtual function in the abstract base class BasicMetadata. */
class PyBasicMetadata : public BasicMetadata {
public:
  int get_int(const std::string key) const override {
    PYBIND11_OVERLOAD_PURE(
      int,
      BasicMetadata,
      get_int,
      key
    );
  };
  double get_double(const std::string key) const override {
    PYBIND11_OVERLOAD_PURE(
      double,
      BasicMetadata,
      get_double,
      key
    );
  };
  bool get_bool(const std::string key) const override {
    PYBIND11_OVERLOAD_PURE(
      bool,
      BasicMetadata,
      get_bool,
      key
    );
  };
  std::string get_string(const std::string key) const override {
    PYBIND11_OVERLOAD_PURE(
      std::string,
      BasicMetadata,
      get_string,
      key
    );
  };
  void put(const std::string key,const double val) override {
    PYBIND11_OVERLOAD_PURE(
      void,
      BasicMetadata,
      put,
      key,
      val
    );
  };
  void put(const std::string key,const int val) override {
    PYBIND11_OVERLOAD_PURE(
      void,
      BasicMetadata,
      put,
      key,
      val
    );
  };
  void put(const std::string key,const bool val) override {
    PYBIND11_OVERLOAD_PURE(
      void,
      BasicMetadata,
      put,
      key,
      val
    );
  };
  void put(const std::string key,const std::string val) override {
    PYBIND11_OVERLOAD_PURE(
      void,
      BasicMetadata,
      put,
      key,
      val
    );
  };
};

/* Trampoline class for BasicProcessingHistory - new for 2020 API change */
class PyBasicProcessingHistory : public BasicProcessingHistory {
public:
  size_t number_of_stages() override {
    PYBIND11_OVERLOAD(
      size_t,
      BasicProcessingHistory,
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

/* The following Python C API are needed to construct the PyMsPASSError
   exception with pybind11. This is following the example from:
   https://www.pierov.org/2020/03/01/python-custom-exceptions-c-extensions/
*/
static PyObject *MsPASSError_tp_str(PyObject *selfPtr)
{
  py::str ret;
  try {
    py::handle self(selfPtr);
    py::tuple args = self.attr("args");
    ret = py::str(args[0]);
  } catch (py::error_already_set &e) {
    ret = "";
  }
  /* ret will go out of scope when returning, therefore increase its reference
  count, and transfer it to the caller (like PyObject_Str). */
  ret.inc_ref();
  return ret.ptr();
}

static PyObject *MsPASSError_get_message(PyObject *selfPtr, void *closure)
{
  return MsPASSError_tp_str(selfPtr);
}

static PyObject *MsPASSError_get_severity(PyObject *selfPtr, void *closure)
{
  try {
    py::handle self(selfPtr);
    py::tuple args = self.attr("args");
    py::object severity;
    if(args.size() < 2)
      severity = py::cast(ErrorSeverity::Fatal);
    else {
      severity = args[1];
      if(py::isinstance<py::str>(args[1])) {
        severity = py::cast(string2severity(std::string(py::str(args[1]))));
      } else if(!py::isinstance(args[1], py::cast(ErrorSeverity::Fatal).get_type())) {
        severity = py::cast(ErrorSeverity::Fatal);
      }
    }
    severity.inc_ref();
    return severity.ptr();
  } catch (py::error_already_set &e) {
    /* We could simply backpropagate the exception with e.restore, but
    exceptions like OSError return None when an attribute is not set. */
    py::none ret;
    ret.inc_ref();
    return ret.ptr();
  }
}

static PyGetSetDef MsPASSError_getsetters[] = {
  {"message", (getter)MsPASSError_get_message, NULL, NULL},
  {"severity", (getter)MsPASSError_get_severity, NULL, NULL},
  {NULL}
};

PYBIND11_MODULE(utility, m) {
  m.attr("__name__") = "mspasspy.ccore.utility";
  m.doc() = "A submodule for utility namespace of ccore"; 

  py::class_<SphericalCoordinate>(m,"SphericalCoordinate","Enscapsulates concept of spherical coordinates")
    .def(py::init<>())
    .def(py::init<const SphericalCoordinate&>())
    .def(py::init([](py::array_t<double> uv) {
      py::buffer_info info = uv.request();
      if (info.ndim != 1 || info.shape[0] != 3)
        throw py::value_error("SphericalCoordinate expects a vector of 3 elements");
      return UnitVectorToSpherical(static_cast<double*>(info.ptr));
    }))
    /* The use of capsule to avoid copy is found at
       https://github.com/pybind/pybind11/issues/1042 */
    .def_property_readonly("unit_vector", [](const SphericalCoordinate& self) {
      double* v = SphericalToUnitVector(self);
      auto capsule = py::capsule(v, [](void *v) { delete[] reinterpret_cast<double*>(v); });
      return py::array(3, v, capsule);
    },"Return the unit vector equivalent to direction defined in sphereical coordinates")
    .def_readwrite("radius", &SphericalCoordinate::radius,"R of spherical coordinates")
    .def_readwrite("theta", &SphericalCoordinate::theta,"zonal angle of spherical coordinates")
    .def_readwrite("phi", &SphericalCoordinate::phi,"azimuthal angle of spherical coordinates")
  ;

  py::class_<BasicMetadata,PyBasicMetadata>(m,"BasicMetadata")
    .def(py::init<>())
  ;

  py::enum_<MDtype>(m,"MDtype")
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

  py::class_<Metadata,BasicMetadata> md(m,"Metadata");
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
    .def("get_double",&Metadata::get_double,"Retrieve a real number by a specified key")
    .def("get_long",&Metadata::get_long,"Return a long integer by a specified key")
    .def("get_bool",&Metadata::get_bool,"Return a (C) boolean defined by a specified key")
    .def("get_string",&Metadata::get_string,"Return a string indexed by a specified key")
    .def("get",&Metadata::get_any,"Return the value indexed by a specified key")
    .def("__getitem__",&Metadata::get_any,"Return the value indexed by a specified key")
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
       return Metadata(restore_serialized_metadata(sbuf));
     }
     ))
  ;
  py::class_<PyMetadataIterator>(md, "Metadata_keyIterator")
    .def("__iter__", [](PyMetadataIterator &it) -> PyMetadataIterator& { return it; })
    .def("__next__", &PyMetadataIterator::next);

  py::class_<AntelopePf,Metadata>(m,"AntelopePf")
    .def(py::init<>())
    .def(py::init<const AntelopePf&>())
    .def(py::init<std::string>(),"Construct from a file")
    .def(py::init<std::list<string>>(),"Construct from a list of strings defining lines to be parsed")
    .def("get_tbl",&AntelopePf::get_tbl,"Fetch contents of a block of the pf file defined by Tbl&")
    .def("get_branch",&AntelopePf::get_branch,"Fetch contents of a block of the pf defined by an Arr&")
    .def("arr_keys",&AntelopePf::arr_keys,"Return a list of all branch (&Arr) keys")
    .def("tbl_keys",&AntelopePf::tbl_keys,"Fetch a list of keys for all &Tbl blocks")
    .def("ConvertToMetadata",&AntelopePf::ConvertToMetadata,"Convert to a flat Metadata space (no branches)")
  ;

  py::class_<Metadata_typedef>(m,"Metadata_typedef")
    .def(py::init<>())
    .def_readwrite("tag",&Metadata_typedef::tag,"Name key for this metadata")
    .def_readwrite("mdt",&Metadata_typedef::mdt,"Type of any value associated with this key")
  ;
  
  /* We need this definition to bind dmatrix to a numpy array as described
  in this section of pybind11 documentation:\
  https://pybind11.readthedocs.io/en/stable/advanced/pycpp/numpy.html
  Leans heavily on example here:
  https://github.com/pybind/pybind11/blob/master/tests/test_buffers.cpp
  */
  py::class_<dmatrix>(m, "dmatrix", py::buffer_protocol())
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
    .def_buffer([](dmatrix &m) -> py::buffer_info {
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
      return py::module_::import("mspasspy.ccore.utility").attr("dmatrix")(
        py::cast(a).attr("__getitem__")(py::reinterpret_steal<py::slice>(
        PySlice_New(Py_None, Py_None, Py_None))).attr("__add__")(b));
    })
    .def(py::self - py::self,"Operator -")
    .def("__sub__", [](const dmatrix &a, py::object b) {
      return py::module_::import("mspasspy.ccore.utility").attr("dmatrix")(
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
      } catch (MsPASSError& e) {
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
  
  py::enum_<ErrorSeverity>(m,"ErrorSeverity")
    .value("Fatal",ErrorSeverity::Fatal)
    .value("Invalid",ErrorSeverity::Invalid)
    .value("Suspect",ErrorSeverity::Suspect)
    .value("Complaint",ErrorSeverity::Complaint)
    .value("Debug",ErrorSeverity::Debug)
    .value("Informational",ErrorSeverity::Informational)
  ;
  
  /* The following magic were based on the great example from:
    https://www.pierov.org/2020/03/01/python-custom-exceptions-c-extensions/
    This appears to be the cleanest and easiest way to implement a custom
    exception with pybind11.

    The PyMsPASSError is the python object inherited from the base exception
    object on the python side. This is done by the PyErr_NewException call.
    The register_exception_translator is the pybind11 interface that catches
    and translates any MsPASSError thrown from the C++ side and throws a new
    PyMsPASSError to python side.
  */
  static PyObject *PyMsPASSError = PyErr_NewException("mspasspy.ccore.MsPASSError", NULL, NULL);
  if (PyMsPASSError) {
    PyTypeObject *as_type = reinterpret_cast<PyTypeObject *>(PyMsPASSError);
    as_type->tp_str = MsPASSError_tp_str;
    for (int i = 0; MsPASSError_getsetters[i].name != NULL; i++) {
      PyObject *descr = PyDescr_NewGetSet(as_type, MsPASSError_getsetters+i);
      auto dict = py::reinterpret_borrow<py::dict>(as_type->tp_dict);
      dict[py::handle(PyDescr_NAME(descr))] = py::handle(descr);
    }

    Py_XINCREF(PyMsPASSError);
    m.add_object("MsPASSError", py::handle(PyMsPASSError));
  }
  py::register_exception_translator([](std::exception_ptr p) {
    try {
      if (p) {
        std::rethrow_exception(p);
      }
    } catch (MsPASSError &e) {
      py::tuple args(2);
      args[0] = e.what();
      args[1] = e.severity();
      PyErr_SetObject(PyMsPASSError, args.ptr());
    }
  });

  /* this set of functions are companions to MsPASSError needed as a
  workaround for problem that MsPASSError method are not visible to
  error handlers for a caught MsPASSError exception. */
  m.def("error_says_data_bad",&error_says_data_bad,
    "Test if what message from MsPASSError defines data as invalid and should be killed")
  ;
  m.def("error_severity_string",&parse_message_error_severity,
    "Return a string defining error severity of a MsPASSError exception")
  ;
  m.def("error_severity",&message_error_severity,
    "Return an ErrorSeverity object defining severity of a MsPASSError being handled")
  ;
  m.def("pfread",&pfread,"parameter file reader",
      py::return_value_policy::copy,
      py::arg("pffile")
  );
  m.def("get_mdlist",&get_mdlist,"retrieve list with keys and types",
    py::return_value_policy::copy
  );

  py::enum_<MDDefFormat>(m,"MDDefFormat")
    .value("PF",MDDefFormat::PF)
    .value("YAML",MDDefFormat::YAML)
  ;

  py::class_<MetadataDefinitions>(m,"MetadataDefinitions","Load a catalog of valid metadata names with types defined")
    .def(py::init<>())
    .def(py::init<string>())
    .def(py::init<std::string,MDDefFormat>())
    .def("is_defined",&MetadataDefinitions::is_defined,"Test if a key is defined")
    .def("concept",&MetadataDefinitions::concept,"Return a string with a brief description of the concept this attribute captures")
    .def("type",&MetadataDefinitions::type,"Return a description of the type of this attribute")
    .def("add",&MetadataDefinitions::add,"Append a new attribute to the catalog")
    .def("has_alias",&MetadataDefinitions::has_alias,"Returns true if a specified key as an alterate name - alias")
    .def("is_alias",&MetadataDefinitions::is_alias,"Return true if a key is an alias")
    .def("aliases",&MetadataDefinitions::aliases,"Return a list of aliases for a particular key")
    .def("unique_name",&MetadataDefinitions::unique_name,"Returns the unique key name associated with an alias")
    .def("add_alias",&MetadataDefinitions::add_alias,"Add an alias for a particular atrribute key")
    .def("keys",&MetadataDefinitions::keys,"Return a list of all valid keys")
    .def("writeable",&MetadataDefinitions::writeable,"Test if an attribute should be saved")
    .def("readonly",&MetadataDefinitions::readonly,"Test if an attribute is marked readonly")
    .def("set_readonly",&MetadataDefinitions::set_readonly,"Force an attribute to be marked readonly")
    .def("set_writeable",&MetadataDefinitions::set_writeable,"Force an attribute to be marked as writeable")
    .def("is_normalized",&MetadataDefinitions::is_normalized,"Test to see if an attribute is stored in a master collection (table)")
    .def("unique_id_key",&MetadataDefinitions::unique_id_key,"Return the key for a unique id to fetch an attribute from a master collection (table)")
    .def("collection",&MetadataDefinitions::collection,"Return the table (collection) name for an attribute defined in a master table")
    .def("normalize_data",&MetadataDefinitions::normalize_data,"Faster method to return unique_id_key and table name")
    .def("apply_aliases",&MetadataDefinitions::apply_aliases,"Apply a set of alias names to Metadata or child of Metadata")
    .def("clear_aliases",&MetadataDefinitions::clear_aliases,"Clear aliases in a Metadata or child of Metadata")
    .def(py::self += py::self)
  ;

  /* These are needed for mspass extensions of Core data objects */
  py::class_<LogData>(m,"LogData","Many mspass create error and log messages with this structure")
    .def(py::init<>())
    .def(py::init<int,std::string,MsPASSError&>())
    .def(py::init<int,std::string,std::string,ErrorSeverity>())
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
          ld->badness = i.second.cast<ErrorSeverity>();
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
        ", 'algorithm': " + std::string(py::repr(py::cast(ld.algorithm))) +
        ", 'message': " + std::string(py::repr(py::cast(ld.message))) + ", 'badness': " +
        std::string(py::str(py::cast(ld.badness))) + "}";
    })
    .def("__repr__", [](const LogData &ld) -> std::string {
      std::string strout("LogData(");
      return strout + std::string(py::str(py::cast(ld).attr("__str__")())) + ")";
    })
  ;

  py::class_<ErrorLogger>(m,"ErrorLogger","Used to post any nonfatal errors without aborting a program of family of parallel programs")
    .def(py::init<>())
    .def(py::init<const ErrorLogger&>())
    .def(py::init<int>())
    .def("set_job_id",&ErrorLogger::set_job_id)
    .def("get_job_id",&ErrorLogger::get_job_id)
    .def("log_error",py::overload_cast<const MsPASSError&>(&ErrorLogger::log_error),"log error thrown as MsPASSError")
    .def("log_error",py::overload_cast<const std::string,const std::string,const ErrorSeverity>(&ErrorLogger::log_error),"log a message at a specified severity level")
    .def("log_verbose",&ErrorLogger::log_verbose,"Log an informational message - tagged as log message")
    .def("get_error_log",&ErrorLogger::get_error_log,"Return all posted entries")
    .def("size",&ErrorLogger::size,"Return number of entries in this log")
    .def("__len__",&ErrorLogger::size,"Return number of entries in this log")
    .def("worst_errors",&ErrorLogger::worst_errors,"Return a list of only the worst errors")
    .def("__getitem__", [](ErrorLogger &self, size_t i) {
      return py::cast(self).attr("get_error_log")().attr("__getitem__")(i);
    })
  ;

  /* New classes in 2020 API revision - object level history preservation */
  py::enum_<ProcessingStatus>(m,"ProcessingStatus")
    .value("RAW",ProcessingStatus::RAW)
    .value("ORIGIN",ProcessingStatus::ORIGIN)
    .value("VOLATILE",ProcessingStatus::VOLATILE)
    .value("SAVED",ProcessingStatus::SAVED)
    .value("UNDEFINED",ProcessingStatus::UNDEFINED)
  ;

  py::enum_<AtomicType>(m,"AtomicType")
    .value("TIMESERIES",AtomicType::TIMESERIES)
    .value("SEISMOGRAM",AtomicType::SEISMOGRAM)
    .value("UNDEFINED",AtomicType::UNDEFINED)
  ;

  py::class_<BasicProcessingHistory,PyBasicProcessingHistory>
  //py::class_<BasicProcessingHistory>
      (m,"BasicProcessingHistory","Base class - hold job history data")
    .def(py::init<>())
    .def("jobid",&BasicProcessingHistory::jobid,
      "Return job id string")
    .def("jobname",&BasicProcessingHistory::jobname,
      "Return job name string defining main python script driving this processing chain")
    .def("set_jobid",&BasicProcessingHistory::set_jobid,
      "Set a unique id so jobname + id is unique")
    .def("set_jobname",&BasicProcessingHistory::set_jobname,
      "Set the base job name defining the main python script for this run")
  ;

  py::class_<NodeData>
    (m,"NodeData","Data structure used in ProcessingHistory to processing tree node data")
    .def(py::init<>())
    .def(py::init<const NodeData&>())
    .def_readwrite("status",&NodeData::status,"ProcessingStatus value at this node")
    .def_readwrite("uuid",&NodeData::uuid,"uuid of data stage associated with this node")
    .def_readwrite("algorithm",&NodeData::algorithm,"algorithm that created data linked to this node position")
    .def_readwrite("algid",&NodeData::algid,
      "id defining an instance of a particular algorithm (defines what parameter choices were used)")
    .def_readwrite("stage",&NodeData::stage,
      "Processing stage counter for this node of the processing tree")
    .def_readwrite("type",&NodeData::type,"Type of data this process handled as this input")
    .def("__str__", [](const NodeData &nd) -> std::string {
      return std::string("{'status': ") + std::string(py::str(py::cast(nd.status))) +
        ", 'uuid': " + std::string(py::repr(py::cast(nd.uuid))) +
        ", 'algorithm': " + std::string(py::repr(py::cast(nd.algorithm))) +
        ", 'algid': " + std::string(py::repr(py::cast(nd.algid))) +
        ", 'stage': " + std::to_string(nd.stage) +
        ", 'type': " + std::string(py::str(py::cast(nd.type))) +
        "}";
    })
    .def("__repr__", [](const NodeData &nd) -> std::string {
      std::string strout("NodeData(");
      return strout + std::string(py::str(py::cast(nd).attr("__str__")())) + ")";
    })
    .def(py::pickle(
      [](const NodeData &self) {
        stringstream ssnd;
        boost::archive::text_oarchive arnd(ssnd);
        arnd<<self;
        return py::make_tuple(ssnd.str());
      },
      [](py::tuple t) {
        stringstream ssnd(t[0].cast<std::string>());
        boost::archive::text_iarchive arnd(ssnd);
        NodeData nd;
        arnd>>nd;
        return nd;
      }
     ))
  ;

  py::class_<ProcessingHistory,BasicProcessingHistory>
    (m,"ProcessingHistory","Used to save object level processing history.")
    .def(py::init<>())
    .def(py::init<const std::string,const std::string>())
    .def(py::init<const ProcessingHistory&>())
    .def("is_empty",&ProcessingHistory::is_empty,
      "Return true if the processing chain is empty")
    .def("is_raw",&ProcessingHistory::is_raw,
      "Return True if the data are raw data with no previous processing")
    .def("is_origin",&ProcessingHistory::is_origin,
       "Return True if the data are marked as an origin - commonly an intermediate save")
    .def("is_volatile",&ProcessingHistory::is_volatile,
      "Return True if the data are unsaved, partially processed data")
    .def("is_saved",&ProcessingHistory::is_saved,
      "Return True if the data are saved and history can be cleared")
    .def("number_of_stages",&ProcessingHistory::number_of_stages,
      "Return count of the number of processing steps applied so far")
    .def("set_as_origin",&ProcessingHistory::set_as_origin,
      "Load data defining this as the top of a processing history chain",
      py::arg("alg"),
      py::arg("algid"),
      py::arg("uuid"),
      py::arg("type"),
      py::arg("define_as_raw") = false)
    .def("set_as_raw",[](ProcessingHistory &self, const string alg,const string algid,
      const string uuid,const AtomicType typ){
        self.set_as_origin(alg, algid, uuid, typ, true);
      },
      "Load data defining this as the raw input of a processing history chain")
    .def("new_ensemble_process",&ProcessingHistory::new_ensemble_process,
      "Set up history chain to define the current data as result of reduction - output form multiple inputs",
      py::arg("alg"),
      py::arg("algid"),
      py::arg("type"),
      py::arg("parents"),
      py::arg("create_newid") = true)
    .def("add_one_input",&ProcessingHistory::add_one_input,
      "A single input datum after initialization with new_ensemble_process or accumulate",
      py::arg("newinput"))
    .def("add_many_inputs",&ProcessingHistory::add_many_inputs,
      "Add multiple inputs after initialization with new_ensemble_process or accumulate",
      py::arg("inputs"))
    .def("accumulate",&ProcessingHistory::accumulate,
      "History accumulator for spark reduce operators",
      py::arg("alg"),
      py::arg("algid"),
      py::arg("type"),
      py::arg("newinput")
      )
    .def("_merge",&ProcessingHistory::merge,
      "Merge the history nodes from another",
      py::arg("newinput")
      )
    .def("new_map",py::overload_cast<const std::string,const std::string,
      const AtomicType,const ProcessingStatus>
        (&ProcessingHistory::new_map),
      "Set history chain to define the current data as a one-to-one map from parent",
      py::arg("alg"),
      py::arg("algid"),
      py::arg("type"),
      py::arg("newstatus") = ProcessingStatus::VOLATILE)
    .def("new_map",py::overload_cast<const std::string,const std::string,
      const AtomicType,const ProcessingHistory&,
      const ProcessingStatus>
        (&ProcessingHistory::new_map),
      "Set history chain to define the current data as a one-to-one map from parent",
      py::arg("alg"),
      py::arg("algid"),
      py::arg("type"),
      py::arg("data_to_clone"),
      py::arg("newstatus") = ProcessingStatus::VOLATILE)
    .def("map_as_saved",&ProcessingHistory::map_as_saved,
      "Load data defining this as the end of chain that was or will soon be saved")
    .def("clear",&ProcessingHistory::clear,
      "Clear this history chain - use with caution")
    .def("get_nodes", &ProcessingHistory::get_nodes,
      "Retrieve the nodes multimap that defines the tree stucture branches")
    .def("stage",&ProcessingHistory::stage,
      "Return the current stage number (counter of processing stages applied in this run)")
    .def("id",&ProcessingHistory::id,"Return current uuid")
    .def("created_by",&ProcessingHistory::created_by ,"Return the algorithm name and id that created current node")
    .def("current_nodedata",&ProcessingHistory::current_nodedata,"Return all the attributes of current")
    .def("newid",&ProcessingHistory::newid,"Create a new uuid for current data")
    .def("set_id",&ProcessingHistory::set_id,"Set current uuid to valued passed")
    .def("inputs",&ProcessingHistory::inputs,
      "Return a list of uuids of all data that were inputs to defined uuid (current or any ancestor)")
    .def("number_inputs",py::overload_cast<const std::string>(&ProcessingHistory::number_inputs, py::const_),
      "Return the number of inputs used to generate a specified uuid of the process chain")
    .def("number_inputs",py::overload_cast<>(&ProcessingHistory::number_inputs, py::const_),
      "Return the number of inputs used to create the current data")
    .def_readwrite("elog",&ProcessingHistory::elog)
    .def("__str__", [](const ProcessingHistory &ph) -> std::string {
      return std::string(py::str(py::cast(ph.current_nodedata())));
    })
    .def("__repr__", [](const ProcessingHistory &ph) -> std::string {
      std::string strout("ProcessingHistory(");
      return strout + std::string(py::str(py::cast(ph).attr("__str__")())) + ")";
    })
    .def(py::pickle(
      [](const ProcessingHistory &self) {
        stringstream ssph;
        boost::archive::text_oarchive arph(ssph);
        arph<<self;
        return py::make_tuple(ssph.str());
      },
      [](py::tuple t) {
        stringstream ssph(t[0].cast<std::string>());
        boost::archive::text_iarchive arph(ssph);
        ProcessingHistory ph;
        arph>>ph;
        return ph;
      }
     ))
  ;
  
  /* this pair of functions are potentially useful for interactive queries of
  ProcessingHistory data */
  m.def("algorithm_history",&algorithm_history,
    "Return a list of algorithms applied to produce current data object",
    py::return_value_policy::copy,
    py::arg("h"))
  ;
  m.def("algorithm_outputs",&algorithm_outputs,
    "Return a list of uuids of data created by a specified algorithm",
    py::return_value_policy::copy,
    py::arg("h"),
    py::arg("algorithm"),
    py::arg("algid") )
  ;
}

} // namespace mspasspy
} // namespace mspass