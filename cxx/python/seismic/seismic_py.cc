#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <pybind11/operators.h>
#include <pybind11/embed.h>

#include <boost/archive/text_oarchive.hpp>

#include <mspass/seismic/keywords.h>
#include <mspass/seismic/SlownessVector.h>
#include <mspass/seismic/TimeSeries.h>
#include <mspass/seismic/Seismogram.h>
#include <mspass/seismic/Ensemble.h>
#include <mspass/seismic/PowerSpectrum.h>
#include <mspass/seismic/DataGap.h>
#include <mspass/seismic/TimeSeriesWGaps.h>

#include <mspass/algorithms/TimeWindow.h>

#include "python/utility/Publicdmatrix_py.h"
#include "python/utility/boost_any_converter_py.h"

/* We enable this gem for reasons explain in the documentation for pybinde11
at this url:  https://pybind11.readthedocs.io/en/master/advanced/cast/stl.html
Upshot is we need the py::bind line at the start of the module definition.
Note a potential issue is any vector<double> in this module will share this
binding.  Don't think there are any collisions on the C side but worth a warning.
 April 2020
 Added bindings for vector of TimeSeries and Seismogram objects to support Ensembles*/
PYBIND11_MAKE_OPAQUE(std::vector<double>);
PYBIND11_MAKE_OPAQUE(std::vector<mspass::seismic::TimeSeries>);
PYBIND11_MAKE_OPAQUE(std::vector<mspass::seismic::Seismogram>);

namespace mspass {
namespace mspasspy {

namespace py=pybind11;
using namespace std;
using namespace mspass::utility;
using namespace mspass::seismic;
using mspass::algorithms::TimeWindow;

/* Trampoline class for BasicTimeSeries */
class PyBasicTimeSeries : public BasicTimeSeries
{
public:
  /* BasicTimeSeries has virtual methods that are not pure because
  forms that contain gap handlers need additional functionality.
  We thus use a different qualifier to PYBIND11_OVERLOAD macro here.
  i.e. omit the PURE part of the name*/
  void ator(const double tshift)
  {
    PYBIND11_OVERLOAD(
      void,
      BasicTimeSeries,
      ator,
      tshift);
  }
  void rtoa()
  {
    PYBIND11_OVERLOAD(
      void,
      BasicTimeSeries,
      rtoa);
  }
  void shift(const double dt)
  {
    PYBIND11_OVERLOAD(
      void,
      BasicTimeSeries,
      shift,
      dt);
  }
  void set_dt(const double sample_interval)
  {
    PYBIND11_OVERLOAD(
      void,
      BasicTimeSeries,
      set_dt,
      sample_interval);
  }
  void set_npts(const size_t npts)
  {
    PYBIND11_OVERLOAD(
      void,
      BasicTimeSeries,
      set_npts,
      npts);
  }
  void set_t0(const double d0in)
  {
    PYBIND11_OVERLOAD(
      void,
      BasicTimeSeries,
      set_t0,
      d0in);
  }
};
/* Trampoline class for BasicSpectrum */
class PyBasicSpectrum : public BasicSpectrum
{
public:
  std::vector<double> frequencies() const
  {
    PYBIND11_OVERLOAD_PURE(
      std::vector<double>,
      BasicSpectrum,
      frequencies,
    );
  }
  double frequency(const int sample_number) const
  {
    PYBIND11_OVERLOAD_PURE(
      double,
      BasicSpectrum,
      frequency,
      sample_number
    );
  }
  size_t nf() const
  {
    PYBIND11_OVERLOAD_PURE(
      size_t,
      BasicSpectrum,
      nf,
    );
  }
  double Nyquist() const
  {
    PYBIND11_OVERLOAD_PURE(
      double,
      BasicSpectrum,
      Nyquist,
    );
  }
};

PYBIND11_MODULE(seismic, m) {
  m.attr("__name__") = "mspasspy.ccore.seismic";
  m.doc() = "A submodule for seismic namespace of ccore";

  /* Define the keywords here*/
  py::object scope = py::module_::import("__main__").attr("__dict__");
  /* The following hack on __name__ is needed to avoid contaminating
     user's namespace of __main__. This way _KeywordDict is defined
     under mspasspy.ccore.seismic instead of __main__. */
  /* TODO: Note that we could also disable editing by overriding
     methods such as __setattr__ and __setitem__, but it is not
     essential for now. */
  py::exec(R"(
        __name__ = "mspasspy.ccore.seismic"
        class _KeywordDict(dict):
          def __init__(self, *args, **kwargs):
              super(_KeywordDict, self).__init__(*args, **kwargs)
              self.__dict__ = self
        __name__ = "__main__"
    )", scope);
  m.attr("_KeywordDict") = scope["_KeywordDict"];
  auto Keywords = scope["_KeywordDict"]();
  Keywords["npts"] = SEISMICMD_npts;
  Keywords["delta"] = SEISMICMD_dt;
  Keywords["starttime"] = SEISMICMD_t0;
  Keywords["sampling_rate"] = SEISMICMD_sampling_rate;
  Keywords["site_lat"] = SEISMICMD_rlat;
  Keywords["site_lon"] = SEISMICMD_rlon;
  Keywords["site_elev"] = SEISMICMD_relev;
  Keywords["channel_lat"] = SEISMICMD_clat;
  Keywords["channel_lon"] = SEISMICMD_clon;
  Keywords["channel_elev"] = SEISMICMD_celev;
  Keywords["channel_hang"] = SEISMICMD_hang;
  Keywords["channel_vang"] = SEISMICMD_vang;
  Keywords["source_lat"] = SEISMICMD_slat;
  Keywords["source_lon"] = SEISMICMD_slon;
  Keywords["source_depth"] = SEISMICMD_sdepth;
  Keywords["source_time"] = SEISMICMD_stime;
  Keywords["dfile"] = SEISMICMD_dfile;
  Keywords["dir"] = SEISMICMD_dir;
  Keywords["foff"] = SEISMICMD_foff;
  Keywords["tmatrix"] = SEISMICMD_tmatrix;
  Keywords["uuid"] = SEISMICMD_uuid;
  Keywords["rawdata"] = SEISMICMD_rawdata;
  Keywords["net"] = SEISMICMD_net;
  Keywords["sta"] = SEISMICMD_sta;
  Keywords["chan"] = SEISMICMD_chan;
  Keywords["loc"] = SEISMICMD_loc;
  m.attr("Keywords") = Keywords;
  py::exec(R"(
        import __main__
        del __main__.__dict__['_KeywordDict']
    )", scope);

  /* We need one of these for each std::vector container to make them function correctly*/
  py::bind_vector<std::vector<double>> (m, "DoubleVector", py::buffer_protocol())
    .def(py::init<>())
    .def("__add__", [](const std::vector<double> &a, py::object b) {
      return py::module_::import("mspasspy.ccore.seismic").attr("DoubleVector")(
        py::array(a.size(), a.data(), py::none()).attr("__add__")(b));
    })
    .def("__sub__", [](const std::vector<double> &a, py::object b) {
      return py::module_::import("mspasspy.ccore.seismic").attr("DoubleVector")(
        py::array(a.size(), a.data(), py::none()).attr("__sub__")(b));
    })
    .def("__mul__", [](const std::vector<double> &a, py::object b) {
      return py::module_::import("mspasspy.ccore.seismic").attr("DoubleVector")(
        py::array(a.size(), a.data(), py::none()).attr("__mul__")(b));
    })
    .def("__truediv__", [](const std::vector<double> &a, py::object b) {
      return py::module_::import("mspasspy.ccore.seismic").attr("DoubleVector")(
        py::array(a.size(), a.data(), py::none()).attr("__truediv__")(b));
    })
  ;

  /* We define the following as global such that it can be used in the algorithms.basic module.
     The usage is documented here:
     https://pybind11.readthedocs.io/en/stable/advanced/cast/stl.html#binding-stl-containers
  */
  py::bind_vector<std::vector<TimeSeries>>(m, "TimeSeriesVector", pybind11::module_local(false));
  py::bind_vector<std::vector<Seismogram>>(m, "SeismogramVector", pybind11::module_local(false));

  py::class_<SlownessVector>(m,"SlownessVector","Encapsulate concept of slowness vector describing wave propagation")
    .def(py::init<>())
    .def(py::init<const SlownessVector&>())
    /* This obscure syntax is used for setting keyword args for a constructor.
    We want it here because we want to normally default az0.
    This obscure trick came from:  https://github.com/pybind/pybind11/issues/579*/
    .def(py::init<const double, const double, const double>(),
      py::arg("ux")=0.0,py::arg("uy")=0.0,py::arg("az0")=0.0
    )
    .def("mag",&SlownessVector::mag,"Return the magnitude of the slowness vector")
    .def("azimuth",&SlownessVector::azimuth,"Return the azimuth of propagation defined by this slowness vector")
    .def("baz",&SlownessVector::baz,"Return the so called back azimuth defined by a slowness vector")
    .def_readwrite("ux",&SlownessVector::ux,"Slowness component in the x (Easting) direction")
    .def_readwrite("uy",&SlownessVector::uy,"Slowness component in the y (Northing) direction")
    .def(py::pickle(
      [](const SlownessVector &self) {
          return py::make_tuple(self.ux, self.uy, self.azimuth());
      },
      [](py::tuple t) {
        double xbuf = t[0].cast<double>();
        double ybuf = t[1].cast<double>();
        double abuf = t[2].cast<double>();
        SlownessVector sv(xbuf, ybuf, abuf);
        return sv;
      }
     ))
  ;

  py::enum_<TimeReferenceType>(m,"TimeReferenceType")
    .value("Relative",TimeReferenceType::Relative)
    .value("UTC",TimeReferenceType::UTC)
  ;

  /* Intentionally ommit the following from python bindings:
  timetype method
  set_tref
  */
  py::class_<BasicTimeSeries,PyBasicTimeSeries>(m,"_BasicTimeSeries","Core common concepts for uniformly sampled 1D data")
    .def(py::init<>())
    .def("time",&BasicTimeSeries::time,"Return the computed time for a sample number (integer)")
    .def("sample_number",&BasicTimeSeries::sample_number,"Return the sample index number for a specified time")
    .def("endtime",&BasicTimeSeries::endtime,"Return the (computed) end time of a time series")
    .def("shifted",&BasicTimeSeries::shifted,"Return True if the data have been time shifted to relative time")
    .def("rtoa",&BasicTimeSeries::rtoa,"Restore relative time to absolute if possible")
    .def("ator",&BasicTimeSeries::ator,"Switch time standard from absolute (UTC) to a relative time scale")
    .def("shift",&BasicTimeSeries::shift,"Shift time reference by a specified number of seconds")
    .def("time_reference",&BasicTimeSeries::time_reference,"Return time standard")
    .def("shifted",&BasicTimeSeries::shifted,"Return true if data are UTC standard with a time shift applied")
    .def("force_t0_shift",&BasicTimeSeries::force_t0_shift,"Force a time shift value to make data shifted UTC in relative time")
    .def("live",&BasicTimeSeries::live,"Return True if the data are marked valid (not dead)")
    .def("dead",&BasicTimeSeries::dead,"Return true if the data are marked bad and should not be used")
    .def("kill",&BasicTimeSeries::kill,"Mark this data object bad = dead")
    .def("set_live",&BasicTimeSeries::set_live,"Undo a kill (mark data ok)")
    .def("dt",&BasicTimeSeries::dt,"Return the sample interval (normally in second)")
    .def("samprate",&BasicTimeSeries::samprate,"Return the sample rate (usually in Hz)")
    .def("time_is_UTC",&BasicTimeSeries::time_is_UTC,"Return true if t0 is a UTC epoch time")
    .def("time_is_relative",&BasicTimeSeries::time_is_relative,"Return true if t0 is not UTC=some relative time standard like shot time")
    .def("npts",&BasicTimeSeries::npts,"Return the number of time samples in this object")
    .def("t0",&BasicTimeSeries::t0,"Return the time of the first sample of data in this time series")
    /*Useful alias for t0 method*/
    .def("starttime",[](const BasicTimeSeries &self){return self.t0();})
    .def("set_dt",&BasicTimeSeries::set_dt,"Set the data time sample interval")
    .def("set_npts",&BasicTimeSeries::set_npts,"Set the number of data samples in this object")
    .def("set_t0",&BasicTimeSeries::set_t0,"Set time of sample 0 (t0) - does not check if consistent with time standard")
    .def_property("npts",[](const BasicTimeSeries &self) {
        return self.npts();
      },[](BasicTimeSeries &self, size_t npts) {
        self.set_npts(npts);
      },"Number of samples in this object")
    .def_property("t0",[](const BasicTimeSeries &self) {
        return self.t0();
      },[](BasicTimeSeries &self, double t0) {
        self.set_t0(t0);
      },"The time of the first sample of data in this object")
    .def_property("dt",[](const BasicTimeSeries &self) {
        return self.dt();
      },[](BasicTimeSeries &self, double dt) {
        self.set_dt(dt);
      },"The sample interval (normally in second)")
    .def_property("live",[](const BasicTimeSeries &self) {
        return self.live();
      },[](BasicTimeSeries &self, bool b) {
        if(b)
          self.set_live();
        else
          self.kill();
      },"True if the data is valid")
    .def_property("tref",[](const BasicTimeSeries &self) {
        return self.timetype();
      },[](BasicTimeSeries &self, TimeReferenceType tref) {
        self.set_tref(tref);
      },"Time reference standard for this data object")
  ;

  /* The following line is necessary for Metadata to be recognized
     if mspasspy.ccore.utility is not imported already. Reference:
     https://pybind11.readthedocs.io/en/stable/advanced/misc.html#partitioning-code-over-multiple-extension-modules*/
  py::module_::import("mspasspy.ccore.utility");

  py::class_<CoreTimeSeries,BasicTimeSeries,Metadata>(m,"_CoreTimeSeries","Defines basic concepts of a scalar time series")
    .def(py::init<>())
    .def(py::init<const CoreTimeSeries&>())
    .def(py::init<const size_t>())
    .def(py::init<const BasicTimeSeries&, const Metadata&>())
    .def("set_dt",&CoreTimeSeries::set_dt,
      "Set data sample interval (overrides BasicTimeSeries virtual method)")
    .def("set_npts",&CoreTimeSeries::set_npts,
      "Set data number of samples (overrides BasicTimeSeries virtual method)")
    .def("sync_npts",&CoreTimeSeries::sync_npts,
      "Sync number of samples with data")
    .def("set_t0",&CoreTimeSeries::set_t0,
      "Set data definition of time of sample 0 (overrides BasicTimeSeries virtual method)")
    .def(py::self += py::self)
    .def(py::self -= py::self)
    .def(py::self + py::self)
    .def(py::self - py::self)
    .def(py::self *= double())
    .def_readwrite("data",&CoreTimeSeries::s,"Actual samples are stored in this data vector")
  ;
  py::class_<CoreSeismogram,BasicTimeSeries,Metadata>(m,"_CoreSeismogram","Defines basic concepts of a three-component seismogram")
    .def(py::init<>())
    .def(py::init<const CoreSeismogram&>())
    .def(py::init<const size_t>())
    .def(py::init<const Metadata&,const bool>(),"Construct from Metadata with read from file option")
    .def(py::init<const std::vector<CoreTimeSeries>&,const unsigned int>())
    .def("set_dt",&CoreSeismogram::set_dt,
      "Set data sample interval (overrides BasicTimeSeries virtual method)")
    .def("set_npts",&CoreSeismogram::set_npts,
      "Set data number of samples (overrides BasicTimeSeries virtual method)")
    .def("sync_npts",&CoreSeismogram::sync_npts,
      "Sync number of samples with data")
    .def("set_t0",&CoreSeismogram::set_t0,
      "Set data definition of time of sample 0 (overrides BasicTimeSeries virtual method)")
    .def("endtime",&CoreSeismogram::endtime,"Return the (computed) end time of a time series")
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
    .def("cardinal",&CoreSeismogram::cardinal,"Returns true if components are cardinal")
    .def("orthogonal",&CoreSeismogram::orthogonal,"Returns true if components are orthogonal")
    .def("free_surface_transformation",&CoreSeismogram::free_surface_transformation,"Apply free surface transformation operator to data")
    .def_property("tmatrix",
      [](const CoreSeismogram &self){
        dmatrix tm = self.get_transformation_matrix();
        return pybind11::module::import("numpy").attr("array")(tm);
      },
      [](CoreSeismogram &self, py::object tm) {
        self.set_transformation_matrix(tm);
      },"3x3 transformation matrix")
    .def(py::self += py::self)
    .def(py::self -= py::self)
    .def(py::self + py::self)
    .def(py::self - py::self)
    .def(py::self *= double())
    /* Place holder for data array.   Probably want this exposed through
    Seismogram api */
    .def_readwrite("data",&CoreSeismogram::u)
  ;

  py::class_<Seismogram,CoreSeismogram,ProcessingHistory>(m,"Seismogram", "mspass three-component seismogram data object")
    .def(py::init<>())
    .def(py::init<const Seismogram&>())
    .def(py::init<const CoreSeismogram&>())
    .def(py::init<const size_t>())
    .def(py::init<const BasicTimeSeries&,const Metadata&>())
    .def(py::init<const Metadata&,bool>())
    .def(py::init<const CoreSeismogram&,const std::string>())
    /* Don't think we really want to expose this to python if we don't need to
    .def(py::init<const BasicTimeSeries&,const Metadata&, const CoreSeismogram,
      const ProcessingHistory&, const ErrorLogger&,
      const bool,const bool, const dmatrix&,const dmatrix&>())
      */
    .def(py::init<const Metadata&,std::string,std::string,std::string,std::string>())
    .def("load_history",&Seismogram::load_history,
       "Load ProcessingHistory from another data object that contains relevant history")
    .def("__sizeof__",[](const Seismogram& self){return self.memory_use();})
    .def(py::pickle(
      [](const Seismogram &self) {
        pybind11::object sbuf;
        sbuf=serialize_metadata_py(self);
        stringstream ssbts;
        ssbts << std::setprecision(17);
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
        //This creates a numpy array alias from the vector container
        //without a move or copy of the data
        size_t u_size = self.u.rows()*self.u.columns();
        if(u_size==0){
          py::array_t<double, py::array::f_style> darr(u_size,NULL);
          return py::make_tuple(sbuf,ssbts.str(),sscorets.str(),
            cardinal, orthogonal,sstm.str(),
            u_size, darr);
        } else {
          py::array_t<double, py::array::f_style> darr(u_size,self.u.get_address(0,0));
          return py::make_tuple(sbuf,ssbts.str(),sscorets.str(),
            cardinal, orthogonal,sstm.str(),
            u_size, darr);
        }
      },
      [](py::tuple t) {
        pybind11::object sbuf=t[0];
        Metadata md=restore_serialized_metadata_py(sbuf);
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
        size_t u_size = t[6].cast<size_t>();
        py::array_t<double, py::array::f_style> darr;
        darr=t[7].cast<py::array_t<double, py::array::f_style>>();
        py::buffer_info info = darr.request();
        if(u_size==0) {
          dmatrix u;
          return Seismogram(bts,md,corets,cardinal,orthogonal,tmatrix,u);
        } else {
          dmatrix u(3, u_size/3);
          memcpy(u.get_address(0,0), info.ptr, sizeof(double) * u_size);
          return Seismogram(bts,md,corets,cardinal,orthogonal,tmatrix,u);
        }
     }
     ))
    ;

    py::class_<TimeSeries,CoreTimeSeries,ProcessingHistory>(m,"TimeSeries","mspass scalar time series data object")
      .def(py::init<>())
      .def(py::init<const TimeSeries&>())
      .def(py::init<const size_t>())
      .def(py::init<const CoreTimeSeries&>())
      .def(py::init<const BasicTimeSeries&,const Metadata&>())
      .def(py::init<const Metadata&>())
      .def(py::init<const CoreTimeSeries&,const std::string>())
      /* Not certain we should have this in the python api.  It is used in pickle interface but doesn't seem
	helpful for python.  Uncomment if this proves false.
      .def(py::init<const BasicTimeSeries&, const Metadata&, const ErrorLogger&, const ProcessingHistory&,
		const std::vector<double>&>())
	*/
      /* this is a python only constructor using a dict. */
      .def(py::init([](py::dict d, py::array_t<double, py::array::f_style | py::array::forcecast> b) {
        py::buffer_info info = b.request();
        if (info.ndim != 1)
          throw MsPASSError("CoreTimeSeries constructor:  Incompatible buffer dimension!", ErrorSeverity::Invalid);
        size_t npts = info.shape[0];
        Metadata md;
        md=py::cast<Metadata>(py::module_::import("mspasspy.ccore.utility").attr("Metadata")(d));
        BasicTimeSeries bts;
        double dt=md.get_double(mspass::seismic::SEISMICMD_dt);
        bts.set_dt(dt);
        double t0=md.get_double(mspass::seismic::SEISMICMD_t0);
        bts.set_t0(t0);
        /* We invoke the BasicTimeSeries method for set_npts which sets the
        internal protected npts attribute of the base class.  We then set
        npts is the metadata.   This trick allows the use of initialization of
        the  std::vector container only once with the push back below.
        Otherwise we could do an initalization zeros followed by insertion.
        This algorithm will be slightly faster. */
        bts.set_npts(npts);
        md.put(mspass::seismic::SEISMICMD_npts,npts);  // don't assume npts is set in metadata
        /* We only support UTC for this constructor assuming it is only used
        to go back and forth from obspy trace objects. */
        bts.set_tref(TimeReferenceType::UTC);
        ProcessingHistory emptyph;
        vector<double> sbuf(npts);
        memcpy(&sbuf[0], info.ptr, npts*sizeof(double));
        auto v = new TimeSeries(bts,md,emptyph,sbuf);
        return v;
      }))
      .def("load_history",&TimeSeries::load_history,
         "Load ProcessingHistory from another data object that contains relevant history")
      .def("__sizeof__",[](const TimeSeries& self){return self.memory_use();})
      // Not sure this constructor needs to be exposed to python
      /*
      .def(py::init<const BasicTimeSeries&,const Metadata&,
        const ProcessingHistory&, const std::vector&)
        */
      .def(py::pickle(
        [](const TimeSeries &self) {
          pybind11::object sbuf;
          sbuf=serialize_metadata_py(self);
          stringstream ssbts;
          ssbts << std::setprecision(17);
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
         pybind11::object sbuf=t[0];
         Metadata md=restore_serialized_metadata_py(sbuf);
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
  py::class_<Ensemble<TimeSeries>,Metadata>(m,"CoreTimeSeriesEnsemble","Gather of scalar time series objects")
    .def(py::init<>())
    .def(py::init<const size_t >())
    .def(py::init<const Metadata&, const size_t>())
    .def(py::init<const Ensemble<TimeSeries>&>())
    .def("update_metadata",&Ensemble<TimeSeries>::update_metadata,"Update the ensemble header (metadata)")
    .def("sync_metadata",py::overload_cast<>(&Ensemble<TimeSeries>::sync_metadata),"Copy ensemble metadata to all members")
    .def("sync_metadata",py::overload_cast<std::vector<std::string>>(&Ensemble<TimeSeries>::sync_metadata),"Copy ensemble metadata to all members")
    // Note member is an std::container - requires py::bind_vector lines at the start of this module defintions
    //    to function properlty
    .def_readwrite("member",&Ensemble<TimeSeries>::member,
            "Vector of TimeSeries objects defining the ensemble")
    /* this small lambda is needed because python has no equivalent of
   a dynamic_cast.  It just returns the ensemble metadata */
    .def("_get_ensemble_md",[](Ensemble<TimeSeries> &self){
        return dynamic_cast<Metadata&>(self);
    })
    .def("__getitem__", [](Ensemble<TimeSeries> &self, const size_t i) {
      return self.member.at(i);
    })
    .def("__getitem__",&Metadata::get_any)
    .def("__setitem__", [](Ensemble<TimeSeries> &self, const size_t i, const TimeSeries ts) {
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
  py::class_<Ensemble<Seismogram>,Metadata>(m,"CoreSeismogramEnsemble","Gather of vector(3c) time series objects")
    .def(py::init<>())
    .def(py::init<const size_t >())
    .def(py::init<const Metadata&, const size_t>())
    .def(py::init<const Ensemble<Seismogram>&>())
    .def("update_metadata",&Ensemble<Seismogram>::update_metadata,"Update the ensemble header (metadata)")
    .def("sync_metadata",py::overload_cast<>(&Ensemble<Seismogram>::sync_metadata),"Copy ensemble metadata to all members")
    .def("sync_metadata",py::overload_cast<std::vector<std::string>>(&Ensemble<Seismogram>::sync_metadata),"Copy ensemble metadata to all members")
    // Note member is an std::container - requires py::bind_vector lines at the start of this module defintions
    //    to function properlty
    .def_readwrite("member",&Ensemble<Seismogram>::member,
            "Vector of Seismogram objects defining the ensemble")
    /* this small lambda is needed because python has no equivalent of
   a dynamic_cast.  It just returns the ensemble metadata */
    .def("_get_ensemble_md",[](Ensemble<Seismogram> &self){
        return dynamic_cast<Metadata&>(self);
    })
    .def("__getitem__", [](Ensemble<Seismogram> &self, const size_t i) {
      return self.member.at(i);
    })
    .def("__getitem__",&Metadata::get_any)
    .def("__setitem__", [](Ensemble<Seismogram> &self, const size_t i, const Seismogram ts) {
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
  py::class_<LoggingEnsemble<Seismogram>, Ensemble<Seismogram> >(m,"SeismogramEnsemble","Gather of vector(3c) time series objects")
    .def(py::init<>())
    .def(py::init<const size_t >())
    .def(py::init<const Metadata&, const size_t>())
    .def(py::init<const Ensemble<Seismogram>&>())
    .def(py::init<const LoggingEnsemble<Seismogram>&>())
    .def("kill",&LoggingEnsemble<Seismogram>::kill,"Mark the entire ensemble dead")
    //.def("live",&LoggingEnsemble<Seismogram>::live,"Return true if the ensemble is marked live")
    .def_property("live",[](const LoggingEnsemble<Seismogram> &self) {
        return self.live();
      },[](LoggingEnsemble<Seismogram> &self, bool b) {
        if(b)
          self.set_live();
        else
          self.kill();
      },"True if the ensemble contains any valid data.  False if empty or all invalid.")
    .def("dead",&LoggingEnsemble<Seismogram>::dead,"Return true if the entire ensemble is marked dead")
    .def("validate",&LoggingEnsemble<Seismogram>::validate,"Test to see if the ensemble has any live members - return true of it does")
    .def("set_live",&LoggingEnsemble<Seismogram>::set_live,"Mark ensemble live but use a validate test first")
    .def("__sizeof__",[](const LoggingEnsemble<Seismogram>& self){return self.memory_use();})
    .def_readwrite("elog",&LoggingEnsemble<Seismogram>::elog,"Error log attached to the ensemble - not the same as member error logs")
    .def(py::pickle(
      [](const LoggingEnsemble<Seismogram> &self) {
        pybind11::gil_scoped_acquire acquire;
        try{
          pybind11::object sbuf;
          sbuf=serialize_metadata_py(self);
          stringstream sselog;
          boost::archive::text_oarchive arelog(sselog);
          arelog << self.elog;
          pybind11::module pickle = pybind11::module::import("pickle");
          pybind11::object dumps = pickle.attr("dumps");
          pybind11::object dbuf = dumps(py::list(pybind11::cast(self.member)));
          bool is_live=self.live();
          pybind11::tuple r_tuple = py::make_tuple(sbuf, sselog.str(), is_live, dbuf);
          pybind11::gil_scoped_release release;
          return r_tuple;
        }catch(...){pybind11::gil_scoped_release release;throw;};
      },
      [](py::tuple t) {
        pybind11::gil_scoped_acquire acquire;
        try{
          pybind11::object sbuf=t[0];
          Metadata md=restore_serialized_metadata_py(sbuf);
          ErrorLogger elog;
          stringstream sselog(t[1].cast<std::string>());
          boost::archive::text_iarchive arelog(sselog);;
          arelog>>elog;
          pybind11::module pickle = pybind11::module::import("pickle");
          pybind11::object loads = pickle.attr("loads");
          /* This algorithm is a horrible memory pig because it has to
          hold both a copy of the monster list of strings of pickled
          data members and the reconstituted C++ data objects (ensemble
          members).  Need to see if we can find a way to handle that
          in a more memory efficient way.
          wangyinz: The new implementation below should be better. */
          py::list dlist = loads(t[3]);
          LoggingEnsemble<Seismogram> result(md, elog, 0);
          pybind11::object member_py = pybind11::module_::import("mspasspy.ccore.seismic").attr("SeismogramVector")(dlist);
          result.member = member_py.cast<vector<Seismogram>>();
          bool is_live = t[2].cast<bool>();
          if(is_live)
            result.set_live();
          else
            /* this kill is not really necessary with the current implmentation
            The constructor we use here set ensmeble dead by default.  For
            a tiny cost we make this more robust by forcing a kill here */
            result.kill();
          pybind11::gil_scoped_release release;
          return result;
        }catch(...){
          pybind11::gil_scoped_release release;
          throw;
        };
      } )
    )
  ;
  py::class_<LoggingEnsemble<TimeSeries>, Ensemble<TimeSeries> >(m,"TimeSeriesEnsemble","Gather of scalar time series objects")
    .def(py::init<>())
    .def(py::init<const size_t >())
    .def(py::init<const Metadata&, const size_t>())
    .def(py::init<const Ensemble<TimeSeries>&>())
    .def(py::init<const LoggingEnsemble<TimeSeries>&>())
    .def("kill",&LoggingEnsemble<TimeSeries>::kill,"Mark the entire ensemble dead")
    //.def("live",&LoggingEnsemble<TimeSeries>::live,"Return true if the ensemble is marked live")
    .def_property("live",[](const LoggingEnsemble<TimeSeries> &self) {
        return self.live();
      },[](LoggingEnsemble<TimeSeries> &self, bool b) {
        if(b)
          self.set_live();
        else
          self.kill();
      },"True if the ensemble contains any valid data.  False if empty or all invalid.")
    .def("dead",&LoggingEnsemble<TimeSeries>::dead,"Return true if the entire ensemble is marked dead")
    .def("validate",&LoggingEnsemble<TimeSeries>::validate,"Test to see if the ensemble has any live members - return true of it does")
    .def("set_live",&LoggingEnsemble<TimeSeries>::set_live,"Mark ensemble live but use a validate test first")
    .def("__sizeof__",[](const LoggingEnsemble<TimeSeries>& self){return self.memory_use();})
    .def_readwrite("elog",&LoggingEnsemble<TimeSeries>::elog,"Error log attached to the ensemble - not the same as member error logs")
    /* This is exactly parallel to the version for a SeismogramEnsemble.
    Only changed Seismogram to TimeSeries everywhere in this section.
    It might be preferable for maintenance to create a template version of
    the key code in Ensemble.h to reduce the redundant code.   Making the
    pickle bindings a template would really be adventure land but would
    be the ideal solution. */
    .def(py::pickle(
      [](const LoggingEnsemble<TimeSeries> &self) {
        pybind11::gil_scoped_acquire acquire;
        try{
          pybind11::object sbuf;
          sbuf=serialize_metadata_py(self);
          stringstream sselog;
          boost::archive::text_oarchive arelog(sselog);
          arelog << self.elog;
          pybind11::module pickle = pybind11::module::import("pickle");
          pybind11::object dumps = pickle.attr("dumps");
          pybind11::object dbuf = dumps(py::list(pybind11::cast(self.member)));
          bool is_live = self.live();
          pybind11::tuple r_tuple = py::make_tuple(sbuf, sselog.str(), is_live, dbuf);
          pybind11::gil_scoped_release release;
          return r_tuple;
        }catch(...){pybind11::gil_scoped_release release;throw;};
      },
      [](py::tuple t) {
        pybind11::gil_scoped_acquire acquire;
        try{
          pybind11::object sbuf=t[0];
          Metadata md=restore_serialized_metadata_py(sbuf);
          ErrorLogger elog;
          stringstream sselog(t[1].cast<std::string>());
          boost::archive::text_iarchive arelog(sselog);;
          arelog>>elog;
          pybind11::module pickle = pybind11::module::import("pickle");
          pybind11::object loads = pickle.attr("loads");
          /* This algorithm is a horrible memory pig because it has to
          hold both a copy of the monster list of strings of pickled
          data members and the reconstituted C++ data objects (ensemble
          members).  Need to see if we can find a way to handle that
          in a more memory efficient way.
          wangyinz: The new implementation below should be better. */
          py::list dlist = loads(t[3]);
          LoggingEnsemble<TimeSeries> result(md, elog, 0);
          pybind11::object member_py = pybind11::module_::import("mspasspy.ccore.seismic").attr("TimeSeriesVector")(dlist);
          result.member = member_py.cast<vector<TimeSeries>>();
          bool is_live = t[2].cast<bool>();
          if(is_live)
            result.set_live();
          else
            /* this kill is not really necessary with the current implmentation
            The constructor we use here set ensmeble dead by default.  For
            a tiny cost we make this more robust by forcing a kill here */
            result.kill();
          pybind11::gil_scoped_release release;
          return result;
        }catch(...){
          pybind11::gil_scoped_release release;
          throw;
        };
      } )
    )
  ;
  py::class_<BasicSpectrum,PyBasicSpectrum>(m,"_BasicSpectrum",
     "Base class for data objects based on Fourier transforms with uniform sampling")
    .def(py::init<>())
    /* We do not need bindings for these base class constuctors.  they
    cause errors with pybind11 and because I see no need for them we don't
    include these bindings.
    .def(py::init<const double, const double>())
    .def(py::init<const BasicSpectrum&>())
    */
    .def("live",&BasicSpectrum::live,"Return True if marked ok, False if data are bad")
    .def("dead",&BasicSpectrum::dead,"Return True if marked bad, False if data are good - negation of live method")
    .def("kill",&BasicSpectrum::kill,"Mark this datum bad (dead)")
    .def("set_live",&BasicSpectrum::set_live,"Mark this datum as good (not dead)")
    .def("df",&BasicSpectrum::df,"Return the frequency bin sample interval")
    .def("f0",&BasicSpectrum::f0,
      "Return frequency of first sample of the vector holding the spectrum (normally 0 but interface allow it to be nonzero)")
    .def("sample_number",&BasicSpectrum::sample_number,"Return vector index position of a specified frequency")
    .def("dt",&BasicSpectrum::dt,"Return parent data sample interval")
    .def("rayleigh",&BasicSpectrum::rayleigh,"Return Rayleigh bin size")
    .def("set_df",&BasicSpectrum::set_df,"Set the frequency bin interval")
    .def("set_f0",&BasicSpectrum::set_f0,
      "Set the frequency defined for first component of vector holding spectrum")
    .def("set_dt",&BasicSpectrum::set_dt,"Set the parent data sample interval")
    .def("set_npts",&BasicSpectrum::set_npts,"Set the number of points of the parent spectrum")
  ;


  py::class_<PowerSpectrum,BasicSpectrum,Metadata>(m,"PowerSpectrum",
                  "Container for power spectrum estimates")
      .def(py::init<>())
      .def(py::init<const Metadata&,const vector<double>&,const double,
        const string,const double, const double, const int>())
      .def(py::init<const PowerSpectrum&>())
      .def("amplitude",&PowerSpectrum::amplitude,
        "Return an std::vector of amplitude values (sqrt of power)")
      .def("power",&PowerSpectrum::power,
        "Return power at a specified frequency using linear interpolation between gridded values")
      .def("frequency",&PowerSpectrum::frequency,"Return frequency linked to given sample number")
      .def("frequencies",&PowerSpectrum::frequencies,"Return an std::vector of ")
      .def("nf",&PowerSpectrum::nf,
        "Return number of frequencies in this spectral estimate")
      .def("Nyquist",&PowerSpectrum::Nyquist,
        "Return Nyquist frequency of this powewr spectrum estimate")
      .def_readwrite("spectrum_type",&PowerSpectrum::spectrum_type,
          "Descriptive name of method used to generate spectrum")
      .def_readwrite("spectrum",&PowerSpectrum::spectrum,
          "Vector containing estimated power spectrum; equally spaced ordered in increasing frequency")
      .def_readwrite("elog",&PowerSpectrum::elog,"Handle to ErrorLogger")
      .def(py::pickle(
        [](const PowerSpectrum& self)
        {
          /* PowerSpectrum inherit Metadata so we have to serialize that*/

          pybind11::object sbuf;
          sbuf=serialize_metadata_py(dynamic_cast<const Metadata&>(self));
          py::array_t<double, py::array::f_style> darr(self.spectrum.size(),
                              &(self.spectrum[0]));
          stringstream ss_elog;
          boost::archive::text_oarchive ar(ss_elog);
          ar << self.elog;
          return py::make_tuple(sbuf,self.df(),self.f0(),self.spectrum_type,
              ss_elog.str(),darr,self.dt(),self.timeseries_npts());
        },
        [](py::tuple t)
        {
          /* Deserialize Metadata*/
          pybind11::object sbuf=t[0];
          Metadata md=restore_serialized_metadata_py(sbuf);
          double df=t[1].cast<double>();
          double f0=t[2].cast<double>();
          string spectrum_type=t[3].cast<std::string>();
          stringstream ss_elog(t[4].cast<std::string>());
          boost::archive::text_iarchive ar(ss_elog);
          ErrorLogger elog;
          ar >> elog;
          py::array_t<double, py::array::f_style> darr;
          darr=t[5].cast<py::array_t<double, py::array::f_style>>();
          py::buffer_info info = darr.request();
          std::vector<double> d;
          d.resize(info.shape[0]);
          memcpy(d.data(), info.ptr, sizeof(double) * d.size());
          double dt=t[6].cast<double>();
          double parent_npts=t[7].cast<double>();
          PowerSpectrum restored(md,d,df,spectrum_type,f0,dt,parent_npts);
          restored.elog=elog;
          return restored;
        }
      ))
    ;

    /*TODO:  This needs a pickle interface.*/
    py::class_<DataGap>(m,"DataGap","Base class for lightweight definition of data gaps")
      .def(py::init<>())
      .def(py::init<const DataGap&>())
      //.def(py::init<std::list<mspass::algorithms::TimeWindow&>())
      /* Cannot get bindings with the next line for this constructor to compile.
      Disabled as the python interface only uses DataGap as a base class for TimeSeriesWGaps
      and I see now reason a python code would need this constructor*/
      //.def(py::init<const std::list<mspass::algorithms::TimeWindow>&>())
      .def("is_gap",&DataGap::is_gap,"Return true if arg0 time is inside a data gap")
      .def("has_gap",py::overload_cast<>(&DataGap::has_gap),"Test if datum has any gaps defined")
      .def("has_gap",py::overload_cast<const mspass::algorithms::TimeWindow>(&DataGap::has_gap),
                 "Test if there is a gap inside a specified time range (defined with TimeWindow object)")
      .def("add_gap",&DataGap::add_gap,"Define a specified time range as a data gap")
      .def("get_gaps",&DataGap::get_gaps,"Return a list of TimeWindows marked as gaps")
      .def("clear_gaps",&DataGap::clear_gaps,"Flush the entire gaps container")
      .def("translate_origin",&DataGap::translate_origin,
		      "Shift time origin by a specified value")
      .def(py::self += py::self)
    ;

    /*TODO:  this needs a pickle interface.*/
    py::class_<TimeSeriesWGaps,TimeSeries,DataGap>(m,"TimeSeriesWGaps","TimeSeries object with gap handling methods")
      .def(py::init<>())
      .def(py::init<const TimeSeries&>())
      .def(py::init<const TimeSeriesWGaps&>())
      .def(py::init<const TimeSeries&,const DataGap&>())
      .def("ator",&TimeSeriesWGaps::ator,"Convert to relative time shifting gaps to match")
      .def("rtoa",py::overload_cast<>(&TimeSeriesWGaps::rtoa),
         "Return to UTC time using time shift defined in earlier ator call")
      .def("rtoa",py::overload_cast<const double>(&TimeSeriesWGaps::rtoa),
                 "Return to UTC time using a specified time shift")
      .def("shift",&TimeSeriesWGaps::shift,"Shift the time reference by a specified constant")
      .def("zero_gaps",&TimeSeriesWGaps::zero_gaps,"Zero the data vector for all sections defined as a gap")
      .def("__sizeof__",[](const TimeSeriesWGaps& self){return self.memory_use();})
    ;

}

} // namespace mspasspy
} // namespace mspass
