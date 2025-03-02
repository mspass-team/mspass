#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <sstream>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

#include <mspass/algorithms/algorithms.h>
#include <mspass/algorithms/Butterworth.h>
#include <mspass/algorithms/Taper.h>
#include <mspass/algorithms/TimeWindow.h>
#include <mspass/utility/Metadata.h>

namespace mspass {
namespace mspasspy {

namespace py=pybind11;
using namespace std;
using namespace mspass::seismic;
using namespace mspass::algorithms;

/* Trampoline class for BasicTaper - used for family of taper classes*/
class PyBasicTaper : public BasicTaper
{
public:
  int apply(TimeSeries &d)
  {
    PYBIND11_OVERLOAD_PURE(
        int,
        BasicTaper,
        TimeSeries &,
        d);
  }
  int apply(Seismogram &d)
  {
    PYBIND11_OVERLOAD_PURE(
        int,
        BasicTaper,
        Seismogram &,
        d);
  }
  double get_t0head()
  {
    PYBIND11_OVERLOAD_PURE(
      double,
      BasicTaper,
      get_t0head
    );
  }
  double get_t1head()
  {
    PYBIND11_OVERLOAD_PURE(
      double,
      BasicTaper,
      get_t1head
    );
  }
};

PYBIND11_MODULE(basic, m) {
  m.attr("__name__") = "mspasspy.ccore.algorithms.basic";
  m.doc() = "A submodule for algorithms namespace of ccore with common algorithms";

  py::class_<mspass::algorithms::Butterworth>
              (m,"Butterworth","Butterworth filter operator processing object")
    .def(py::init<>())
    .def(py::init<const bool, const bool, const bool,
        const double, const double, const double, const double,
	const double, const double, const double, const double,
	const double> ())
    .def(py::init<const mspass::utility::Metadata&>())
    .def(py::init<const bool, const bool, const bool,
        const int, const double, const int, const double, const double>())
    .def(py::init<const Butterworth&>())
    .def("impulse_response",&Butterworth::impulse_response,
         "Return impulse response")
    .def("transfer_function",&Butterworth::transfer_function,
         "Return transfer function in a complex valued array")
    .def("change_dt",&Butterworth::change_dt,
         "Change sample interval defining the operator (does not change corners) ")
    /* Note we intentionally do not overload CoreTimeSeries and CoreSeismogram.
    They do not handle errors as gracefully */
    .def("apply",py::overload_cast<mspass::seismic::TimeSeries&>
         (&Butterworth::apply),
         "Apply the predefined filter to a TimeSeries object")
    .def("apply",py::overload_cast<mspass::seismic::Seismogram&>
         (&Butterworth::apply),
         "Apply the predefined filter to a 3c Seismogram object")
    .def("dt",&Butterworth::current_dt,
      "Current sample interval used for nondimensionalizing frequencies")
    .def("low_corner",&Butterworth::low_corner,"Return low frequency f3d point")
    .def("high_corner",&Butterworth::high_corner,"Return high frequency 3db point")
    .def("npoles_low",&Butterworth::npoles_low,
      "Return number of poles for the low frequency (high-pass aka low-cut) filter definition")
    .def("npoles_high",&Butterworth::npoles_high,
      "Return number of poles for the high frequency (low-pass aka high-cut) filter definition")
    .def("filter_type",&Butterworth::filter_type,
      "Return a description of the filter type")
    .def("is_zerophase",&Butterworth::is_zerophase,
      "Returns True if operator defines a zerophase filter")
  ;
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
  m.def("_ExtractComponent",static_cast<TimeSeries(*)(const Seismogram&,const unsigned int)>(&ExtractComponent),
  	"Extract component as a TimeSeries object",
      py::return_value_policy::copy,
      py::arg("tcs"),
      py::arg("component")
  );

  m.def("_ExtractComponent",static_cast<Ensemble<TimeSeries>(*)(const Ensemble<Seismogram>&,const unsigned int)>(&ExtractComponent),
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
  m.def("_WindowData",py::overload_cast<const TimeSeries&,const TimeWindow&>(&WindowData),
          "Reduce data to window inside original",
    py::return_value_policy::copy,
    py::arg("d"),
    py::arg("twin") )
  ;

  m.def("_WindowData3C",py::overload_cast<const Seismogram&,const TimeWindow&>(&WindowData),
              "Reduce data to window inside original",
    py::return_value_policy::copy,
    py::arg("d"),
    py::arg("twin") )
  ;

  m.def("splice_segments",&splice_segments,"Splice a time sorted list of TimeSeries data into a continuous block",
    py::return_value_policy::copy,
    py::arg("segments"),
    py::arg("save_history")
  );

  m.def("repair_overlaps",&repair_overlaps,"Attempt to remove redundant, matching overlapping data segments",
    py::return_value_policy::copy,
    py::arg("segments")
  );

  /* The following line is necessary for the Vectors to be recognized. Reference:
     https://pybind11.readthedocs.io/en/stable/advanced/misc.html#partitioning-code-over-multiple-extension-modules
  */
  py::module_::import("mspasspy.ccore.seismic");

  m.def("_bundle_seed_data",&bundle_seed_data,
    "Create SeismogramEnsemble from sorted TimeSeriesEnsemble",
    py::return_value_policy::copy,
    py::arg("d") )
  ;

  m.def("_BundleSEEDGroup",&BundleSEEDGroup,
    "Bundle a seed grouping of TimeSeries into one or more Seismogram objects",
    py::return_value_policy::copy,
    py::arg("d"),
    py::arg("i0"),
    py::arg("iend") )
  ;
  m.def("seed_ensemble_sort",&seed_ensemble_sort,R"mspass_doc(
      Sort a TimeSeriesEnsemble with a natural order with seed name codes.

      The seed standard tags every single miniseed record with four string keys
      that seed uses to uniquely define a single data channel.  In MsPASS the keys
      used for these name keys are:  net, sta, chan, and loc.  This function
      applies the same sort algorithm used in the bundle_seed_data algorithm to
      allow clean grouping into channels that can be assembled into
      three component (Seismogram) bundles.  That means we sort the ensemble
      data with the four keys in this order:  net, sta, loc, chan.

      We provide this function because the process of doing such a sort is far
      from trivial to do in a robust way.   A python programmer has easier tools
      for sorting BUT those standard tools cannot handle a common data problem
      that can be encountered with real data.  That is, there is a high
      probability not all the seed keys are defined.   In particular, data
      coming from a system based on the css3.0 relational data base (e.g. Antelope)
      may not have net or loc set.   The sorting algorith here handle null net or
      loc codes cleanly by treating the null case as a particular value.   Without
      those safeties the code would throw an error if net or loc were null.

      Note this algorithm alters the ensemble it receives in place.

      :param d: is the ensemble to be sorted.
    )mspass_doc",
    py::arg("d") )
  ;
  py::class_<TimeWindow>(m,"TimeWindow","Simple description of a time window")
    .def(py::init<>(),"Default constructor")
    .def(py::init<const double, const double>(),"Construct from start and end time")
    .def(py::init<const TimeWindow&>(),"Copy constuctor")
    .def("shift",&TimeWindow::shift,"Shift the reference time by a specified number of seconds")
    .def("length",&TimeWindow::length,"Return the size of the window in seconds")
    .def_readwrite("start",&TimeWindow::start,"Start time of the window")
    .def_readwrite("end",&TimeWindow::end,"End time of the window")
    .def(py::pickle(
        [](const TimeWindow& self)
        {
          return py::make_tuple(self.start,self.end);
        },
        [](py::tuple t)
        {
          return TimeWindow(t[0].cast<double>(),t[1].cast<double>());
        }
      ))
  ;
  py::class_<BasicTaper,PyBasicTaper>(m,"BasicTaper",
                    "Base class for family of taper algorithms")
      /* BasicTaper is a nearly pure abstract type.   It has some
      nonvirtual methods but we don't need them in mspass we will not define
      them in these binds.  The following is a residual from trying (incorrectly)
      to define the virtual methods.
    .def(py::init<>())
    .def("apply",py::overload_cast<TimeSeries&>(&BasicTaper::apply),
        "TimeSeries overload of base class")
    .def("apply",py::overload_cast<Seismogram&>(&BasicTaper::apply))
    .def("get_t0head",&BasicTaper::get_t0head)
    .def("get_t1head",&BasicTaper::get_t1head)
    */
  ;
  py::class_<LinearTaper,BasicTaper>(m,"LinearTaper",
      "Define a ramp taper function")
    .def(py::init<>())
    .def(py::init<const double, const double, const double, const double>())
    .def("apply",py::overload_cast<TimeSeries&>(&LinearTaper::apply),
      "Apply taper to a scalar TimeSeries object")
    .def("apply",py::overload_cast<Seismogram&>(&LinearTaper::apply),
      "Apply taper to a Seismogram (3C) object")
    .def("get_t0head",&LinearTaper::get_t0head,
      "Return time of end of zero zone - taper sets data with time < this value 0")
    .def("get_t1head",&LinearTaper::get_t1head,
      "Return time of end front end taper - point after this time are not altered until end taper zone is reached")
    .def("get_t0tail",&LinearTaper::get_t0tail,
      "Return time of taper end - data with t > this value will be zeroed")
    .def("get_t1tail",&LinearTaper::get_t1tail,
      "Return start time of end taper")
    .def(py::pickle(
        [](const LinearTaper &self) {
          double t0h,t1h,t1t,t0t;
          t0h=self.get_t0head();
          t1h=self.get_t1head();
          t0t=self.get_t0tail();
          t1t=self.get_t1tail();
          return py::make_tuple(t0h,t1h,t1t,t0t);
        },
        [](py::tuple t) {
          double t0h,t1h,t1t,t0t;
          t0h = t[0].cast<double>();
          t1h = t[1].cast<double>();
          t1t = t[2].cast<double>();
          t0t = t[3].cast<double>();
          return LinearTaper(t0h,t1h,t1t,t0t);
        }
       ))
  ;
  py::class_<CosineTaper,BasicTaper>(m,"CosineTaper",
      "Define a taper using a half period cosine function")
    .def(py::init<>())
    .def(py::init<const double, const double, const double, const double>())
    .def("apply",py::overload_cast<TimeSeries&>(&CosineTaper::apply),
            "Apply taper to a scalar TimeSeries object")
    .def("apply",py::overload_cast<Seismogram&>(&CosineTaper::apply),
            "Apply taper to a Seismogram (3C) object")
    .def("get_t0head",&CosineTaper::get_t0head,
      "Return time of end of zero zone - taper sets data with time < this value 0")
    .def("get_t1head",&CosineTaper::get_t1head,
      "Return time of end front end taper - point after this time are not altered until end taper zone is reached")
    .def("get_t0tail",&CosineTaper::get_t0tail,
      "Return time of taper end - data with t > this value will be zeroed")
    .def("get_t1tail",&CosineTaper::get_t1tail,
      "Return start time of end taper")
    .def(py::pickle(
        [](const CosineTaper &self) {
          double t0h,t1h,t1t,t0t;
          t0h=self.get_t0head();
          t1h=self.get_t1head();
          t0t=self.get_t0tail();
          t1t=self.get_t1tail();
          return py::make_tuple(t0h,t1h,t1t,t0t);
        },
        [](py::tuple t) {
          double t0h,t1h,t1t,t0t;
          t0h = t[0].cast<double>();
          t1h = t[1].cast<double>();
          t1t = t[2].cast<double>();
          t0t = t[3].cast<double>();
          return CosineTaper(t0h,t1h,t1t,t0t);
        }
       ))
  ;
  py::class_<VectorTaper,BasicTaper>(m,"VectorTaper",
      "Define generic taper function with a parallel vector of weights")
    .def(py::init<>())
    .def(py::init<const std::vector<double>>())
    .def("apply",py::overload_cast<TimeSeries&>(&VectorTaper::apply),
      "Apply taper to a scalar TimeSeries object")
    .def("apply",py::overload_cast<Seismogram&>(&VectorTaper::apply),
        "Apply taper to a Seismogram (3C) object")
    .def(py::pickle(
      [](const VectorTaper& self)
      {
        ostringstream oss;
        boost::archive::text_oarchive oa(oss);
        oa << self;
        return py::make_tuple(oss.str());
      },
      [](py::tuple t)
      {
        string serialized_taper(t[0].cast<string>());
        istringstream iss(serialized_taper);
        boost::archive::text_iarchive ia(iss);
        VectorTaper taper;
        ia >> taper;
        return taper;
      }
    ))
  ;
  py::class_<TopMute>(m,"_TopMute",
    "Defines a top mute operator (one sided taper")
    .def(py::init<>())
    .def(py::init<const double, const double, const std::string>())
    .def(py::init<const TopMute&>())
    .def("apply",py::overload_cast<TimeSeries&>(&TopMute::apply),
      "Apply to a TimeSeries object")
    .def("apply",py::overload_cast<Seismogram&>(&TopMute::apply),
      "Apply to a Seismogram object")
    .def("get_t0",&TopMute::get_t0,
      "Return the zero end time for marking the start of the mute")
    .def("get_t1",&TopMute::get_t1,
      "Return the time the mute goes to 1 - the end time of the taper")
    .def("taper_type",&TopMute::taper_type,
       "Return a string defining type of taper defining this mute")
    .def(py::pickle(
        [](const TopMute &self) {
          double t0,t1;
          t0=self.get_t0();
          t1=self.get_t1();
          string taper_type(self.taper_type());
          return py::make_tuple(t0,t1,taper_type);
        },
        [](py::tuple t) {
          double t0,t1;
          std::string taper_type;
          t0 = t[0].cast<double>();
          t1 = t[1].cast<double>();
          taper_type = t[2].cast<std::string>();
          return TopMute(t0,t1,taper_type);
        }
       ))
  ;
}

} // namespace mspasspy
} // namespace mspass
