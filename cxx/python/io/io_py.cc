#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <pybind11/operators.h>
#include <pybind11/embed.h>

#include "mspass/io/mseed_index.h"

PYBIND11_MAKE_OPAQUE(std::vector<mspass::io::mseed_index>);
namespace mspass {
namespace mspasspy {

namespace py=pybind11;
using namespace std;
using namespace mspass::io;

PYBIND11_MODULE(io,m){
  m.attr("__name__") = "mspasspy.ccore.io";
  m.doc() = "A submodule for io namespace of ccore";

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
    .def_readwrite("npts",&mseed_index::npts,
      "Computed number of samples from packet headers")
    .def_readwrite("samprate",&mseed_index::samprate,
      "Data sample rate (sps) for this block of data")
    .def_readwrite("starttime",&mseed_index::starttime,
      "Start time (epoch seconds) of this block of data")
    .def_readwrite("endtime",&mseed_index::endtime,
      "Time estimated for end of block computed from npts and samprate")
    .def_readwrite("last_packet_time",&mseed_index::last_packet_time,
      "Time tag of last packe of data in this block - less than endtime")
    ;
  m.def("_mseed_file_indexer",&mseed_file_indexer,
    "Builds an index for a miniseed file returning std::pair with index and ErrorLogger object",
    py::return_value_policy::copy,
    py::arg("file"),
    py::arg("segment") = false,
    py::arg("verbose") = false
    )
  ;
}
}   // namespace mspasspy
}  // namespace mspas
