#include <boost/any.hpp>
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#define MSPASS_COMMA ,

namespace pybind11 { namespace detail {
  namespace py=pybind11;
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

  template <class T, class U> struct type_caster<std::multimap<T,U>> {
  public:
    PYBIND11_TYPE_CASTER(std::multimap<T MSPASS_COMMA U>, _("std::multimap<T MSPASS_COMMA U>"));
    bool load(handle src, bool) {
      /*Always return false since we are not converting
        any PyObject into std::multimap.
        */
      return false;
    }
    static handle cast(std::multimap<T,U> src, return_value_policy /* policy */, handle /* parent */) {
      py::dict d;
      for (auto it=src.begin(); it!=src.end(); ++it) {
        if(d.attr("get")((*it).first).is(py::none()))
          d.attr("__setitem__")((*it).first, py::list());
        d.attr("__getitem__")((*it).first).attr("append")((*it).second);
      }
      return d.release();
    }
  };
}} // namespace pybind11::detail