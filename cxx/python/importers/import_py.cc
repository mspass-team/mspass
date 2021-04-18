#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <pybind11/operators.h>
#include <pybind11/embed.h>

#include "mspass/import/mseed_index.h"

PYBIND11_MAKE_OPAQUE(std::vector<mspass::import::mseed_index>);
namespace mspass {
namespace mspasspy {

namespace py=pybind11;
using namespace std;
using namespace mspass::import;

PYBIND11_MODULE(import,m){
  py::bind_vector<std::vector<mseed_index>>(m,"MseedIndex");
  py::class_<mseed_index>(m,"mseed_index",
    "Index data for time ordered miniseed files")
    .def(py::init<>(),"Default constructor")
    .def(py::init<const mseed_index&>(),"Copy constructor")
    .def_readwrite("net",&mseed_index::net,"SEED network code for data")
    .def_readwrite("sta",&mseed_index::sta,"SEED station code for data")
    .def_readwrite("loc",&mseed_index::loc,"SEED location code for data")
    .def_readwrite("chan",&mseed_index::chan,"SEED channel code for data")
    .def_readwrite("foff",&mseed_index::foff,
       "offset in bytes to first packet of data block")
    .def_readwrite("nbytes",&mseed_index::nbytes,
      "number of bytes of data in this block of data")
    .def_readwrite("starttime",&mseed_index::starttime,
      "Start time (epoch seconds) of this block of data")
    .def_readwrite("last_packet_time",&mseed_index::last_packet_time,
      "Time tag of last packe of data in this block - less than endtime")
    ;  
  m.def("_mseed_file_indexer",&mseed_file_indexer,
    "Builds an index for a miniseed file",
    py::return_value_policy::copy,
    py::arg("file") )
  ;
}
}   // namespace mspasspy
}  // namespace mspas
