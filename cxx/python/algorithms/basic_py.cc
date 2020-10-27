#include <pybind11/pybind11.h>

#include <mspass/algorithms/algorithms.h>

namespace mspass {
namespace mspasspy {

namespace py=pybind11;
using namespace std;
using namespace mspass::seismic;
using namespace mspass::algorithms;

PYBIND11_MODULE(basic, m) {
  m.attr("__name__") = "mspasspy.ccore.algorithms.basic";
  m.doc() = "A submodule for algorithms namespace of ccore"; 

  m.def("ArrivalTimeReference",
      py::overload_cast<Seismogram&,std::string,TimeWindow>
          (&ArrivalTimeReference),
          "Shifts data so t=0 is a specified arrival time",
      py::return_value_policy::copy,
      py::arg("d"),
      py::arg("key"),
      py::arg("window")
  );

  m.def("ArrivalTimeReference",
      py::overload_cast<Ensemble<Seismogram>&,std::string,TimeWindow>
          (&ArrivalTimeReference),
          "Shifts data so t=0 is a specified arrival time",
      py::return_value_policy::copy,
      py::arg("d"),
      py::arg("key"),
      py::arg("window")
  );

  /* overload_cast would not work on this name because of a strange limitation with templated functions
   * used for the Ensemble definition.   */
  m.def("ExtractComponent",static_cast<TimeSeries(*)(const Seismogram&,const unsigned int)>(&ExtractComponent),
  	"Extract component as a TimeSeries object",
      py::return_value_policy::copy,
      py::arg("tcs"),
      py::arg("component")
  );

  m.def("EnsembleComponent",static_cast<Ensemble<TimeSeries>(*)(const Ensemble<Seismogram>&,const unsigned int)>(&ExtractComponent),
  	"Extract one component from a 3C ensemble",
      py::return_value_policy::copy,
      py::arg("d"),
      py::arg("component")
  );

  m.def("agc",&agc,"Automatic gain control a Seismogram",
    py::return_value_policy::copy,
    py::arg("d"),
    py::arg("twin") )
  ;

  m.def("WindowData",&WindowData,"Reduce data to window inside original",
    py::return_value_policy::copy,
    py::arg("d"),
    py::arg("twin") )
  ;

  m.def("WindowData3C",&WindowData3C,"Reduce data to window inside original",
    py::return_value_policy::copy,
    py::arg("d"),
    py::arg("twin") )
  ;
}

} // namespace mspasspy
} // namespace mspass