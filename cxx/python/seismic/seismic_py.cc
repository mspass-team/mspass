#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <pybind11/operators.h>

#include <boost/archive/text_oarchive.hpp>

#include <mspass/seismic/SlownessVector.h>
#include <mspass/seismic/TimeWindow.h>
#include <mspass/seismic/TimeSeries.h>
#include <mspass/seismic/Seismogram.h>
#include <mspass/seismic/Ensemble.h>
#include <mspass/seismic/PowerSpectrum.h>
#include <mspass/seismic/Taper.h>

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

/* Trampoline class for BasicTaper - used for family of taper classes*/
class PyBasicTaper : public BasicTaper
{
public:
  int apply(TimeSeries& d)
  {
    PYBIND11_OVERLOAD_PURE(
      int,
      BasicTaper,
      TimeSeries&,
      d);
  }
  int apply(Seismogram& d)
  {
    PYBIND11_OVERLOAD_PURE(
      int,
      BasicTaper,
      Seismogram&,
      d);
  }
};

PYBIND11_MODULE(seismic, m) {
  m.attr("__name__") = "mspasspy.ccore.seismic";
  m.doc() = "A submodule for seismic namespace of ccore";

  /* We need one of these for each std::vector container to make them function correctly*/
  py::bind_vector<std::vector<double>>(m, "DoubleVector");
  py::bind_vector<std::vector<TimeSeries>>(m, "TimeSeriesVector");
  py::bind_vector<std::vector<Seismogram>>(m, "SeismogramVector");


  py::class_<SlownessVector>(m,"SlownessVector","Encapsulate concept of slowness vector describing wave propagation")
    .def(py::init<>())
    .def(py::init<const SlownessVector&>())
    .def(py::init<const double, const double, const double>())
    .def("mag",&SlownessVector::mag,"Return the magnitude of the slowness vector")
    .def("azimuth",&SlownessVector::azimuth,"Return the azimuth of propagation defined by this slowness vector")
    .def("baz",&SlownessVector::baz,"Return the so called back azimuth defined by a slowness vector")
    .def_readwrite("ux",&SlownessVector::ux,"Slowness component in the x (Easting) direction")
    .def_readwrite("uy",&SlownessVector::uy,"Slowness component in the y (Northing) direction")
  ;

  py::class_<TimeWindow>(m,"TimeWindow","Simple description of a time window")
    .def(py::init<>(),"Default constructor")
    .def(py::init<const double, const double>(),"Construct from start and end time")
    .def(py::init<const TimeWindow&>(),"Copy constuctor")
    .def("shift",&TimeWindow::shift,"Shift the reference time by a specified number of seconds")
    .def("length",&TimeWindow::length,"Return the size of the window in seconds")
    .def_readwrite("start",&TimeWindow::start,"Start time of the window")
    .def_readwrite("end",&TimeWindow::end,"End time of the window")
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
      },"Whether the data is valid or not")
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
    .def(py::init<const CoreSeismogram&,const std::string>())
    /* Don't think we really want to expose this to python if we don't need to
    .def(py::init<const BasicTimeSeries&,const Metadata&, const CoreSeismogram,
      const ProcessingHistory&, const ErrorLogger&,
      const bool,const bool, const dmatrix&,const dmatrix&>())
      */
    .def(py::init<const Metadata&,std::string,std::string,std::string,std::string>())
    .def("load_history",&Seismogram::load_history,
       "Load ProcessingHistory from another data object that contains relevant history")
    .def(py::pickle(
      [](const Seismogram &self) {
        string sbuf;
        sbuf=serialize_metadata(self);
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
        string sbuf=t[0].cast<std::string>();
        Metadata md;
        md=Metadata(restore_serialized_metadata(sbuf));
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
        double dt=md.get_double("delta");
        bts.set_dt(dt);
        double t0=md.get_double("starttime");
        bts.set_t0(t0);
        /* We invoke the BasicTimeSeries method for set_npts which sets the
        internal protected npts attribute of the base class.  We then set
        npts is the metadata.   This trick allows the use of initialization of
        the  std::vector container only once with the push back below.
        Otherwise we could do an initalization zeros followed by insertion.
        This algorithm will be slightly faster. */
        bts.set_npts(npts);
        md.put("npts",npts);  // don't assume npts is set in metadata
        /* We only support UTC for this constructor assuming it is only used
        to go back and forth from obspy trace objects. */
        bts.set_tref(TimeReferenceType::UTC);
        ProcessingHistory emptyph;
        double *dptr;
        vector<double> sbuf;
        sbuf.reserve(npts);   // A standard efficiency trick for std::vector
        /* Initialize dptr here for clarity instead of putting it inside the
        initialization block of the for loop - clearer to me anyway */
        dptr=(double*)(info.ptr);
        for(size_t i=0;i<npts;++i,++dptr) sbuf.push_back(*dptr);
        auto v = new TimeSeries(bts,md,emptyph,sbuf);
        return v;
      }))
      .def("load_history",&TimeSeries::load_history,
         "Load ProcessingHistory from another data object that contains relevant history")
      // Not sure this constructor needs to be exposed to python
      /*
      .def(py::init<const BasicTimeSeries&,const Metadata&,
        const ProcessingHistory&, const std::vector&)
        */
      .def(py::pickle(
        [](const TimeSeries &self) {
          string sbuf;
          sbuf=serialize_metadata(self);
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
         string sbuf=t[0].cast<std::string>();
         Metadata md;
         md=Metadata(restore_serialized_metadata(sbuf));
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
  py::class_<Ensemble<TimeSeries>,Metadata>(m,"TimeSeriesEnsemble","Gather of scalar time series objects")
    .def(py::init<>())
    .def(py::init<const size_t >())
    .def(py::init<const Metadata&, const size_t>())
    .def(py::init<const Ensemble<TimeSeries>&>())
    .def("update_metadata",&Ensemble<TimeSeries>::update_metadata,"Update the ensemble header (metadata)")
    .def("sync_metadata",&Ensemble<TimeSeries>::sync_metadata,"Copy ensemble metadata to all members")
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
  py::class_<Ensemble<Seismogram>,Metadata>(m,"SeismogramEnsemble","Gather of vector(3c) time series objects")
    .def(py::init<>())
    .def(py::init<const size_t >())
    .def(py::init<const Metadata&, const size_t>())
    .def(py::init<const Ensemble<Seismogram>&>())
    .def("update_metadata",&Ensemble<Seismogram>::update_metadata,"Update the ensemble header (metadata)")
    .def("sync_metadata",&Ensemble<Seismogram>::sync_metadata,"Copy ensemble metadata to all members")
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

  /* This following would be the normal way to expose this class to python, but it generates and
  error for reasons described in this issues page of pybind11:
  https://github.com/pybind/pybind11/issues/633
  I (glp) could not find and immediate solution but use the bandaid solution here of removing the
  Metadata bindings to this class.   For the time being that will not present a problem, but it
  should be fixed long term.   Could, for example, add a "get_metadata" lambda in the pybind11
  code that would dynamic cast the PowerSpectrum and return the Metadata.  I think that would work, but
  it would be better to figure out how to allow Metadata to be used.   Might be as easy as
  putting the PowerSpectrum file in the utility module.  Here is the class binding that
  creates an import error:
    py::class_<PowerSpectrum,Metadata>(m,"PowerSpectrum",
  Here is the one that works but doesn't provide Metadata functionality */
    py::class_<PowerSpectrum>(m,"PowerSpectrum",
                  "Container for power spectrum estimates")
      .def(py::init<>())
      .def(py::init<const Metadata&,const vector<double>&,const double,const string>())
      .def(py::init<const PowerSpectrum&>())
      .def("nf",&PowerSpectrum::nf,"Return number of frequencies in this spectral estimate")
      .def("frequency",&PowerSpectrum::frequency,"Return frequency of sample number of spectrum vector")
      .def("Nyquist",&PowerSpectrum::Nyquist,"Return Nyquist frequency")
      .def("sample_number",&PowerSpectrum::sample_number,"Return sample number of a given frequency")
      .def_readonly("df",&PowerSpectrum::df,"Frequency bin size")
      .def_readonly("spectrum_type",&PowerSpectrum::spectrum_type,
          "Descriptive name of method used to generate spectrum")
      .def_readonly("spectrum",&PowerSpectrum::spectrum,
          "Vector containing estimated power spectrum; equally spaced ordered in increasing frequency")
      .def_readwrite("elog",&PowerSpectrum::elog,"Handle to ErrorLogger")
    ;
    py::class_<BasicTaper,PyBasicTaper>(m,"BasicTaper",
                     "Base class for family of taper algorithms")
      .def(py::init<>())
      /* note sure virtual classes need a definition here */
      /*
      .def("apply",py::overload_cast<TimeSeries&>(&BasicTaper::apply),
         "TimeSeries overload of base class")
      .def("apply",py::overload_cast<Seismogram&>(&BasicTaper::apply))
      */
    ;
    py::class_<LinearTaper,BasicTaper>(m,"LinearTaper",
       "Define a ramp taper function")
      .def(py::init<>())
      .def(py::init<const double, const double, const double, const double>())
      .def("apply",py::overload_cast<TimeSeries&>(&LinearTaper::apply),"Apply taper to a scalar TimeSeries object")
      .def("apply",py::overload_cast<Seismogram&>(&LinearTaper::apply),"Apply taper to a Seismogram (3C) object")
    ;
    py::class_<CosineTaper,BasicTaper>(m,"CosineTaper",
       "Define a taper using a half period cosine function")
      .def(py::init<>())
      .def(py::init<const double, const double, const double, const double>())
      .def("apply",py::overload_cast<TimeSeries&>(&CosineTaper::apply),"Apply taper to a scalar TimeSeries object")
      .def("apply",py::overload_cast<Seismogram&>(&CosineTaper::apply),"Apply taper to a Seismogram (3C) object")
    ;
    py::class_<VectorTaper,BasicTaper>(m,"VectorTaper",
       "Define generic taper function with a parallel vector of weights")
      .def(py::init<>())
      .def(py::init<const std::vector<double>>())
      .def("apply",py::overload_cast<TimeSeries&>(&VectorTaper::apply),
        "Apply taper to a scalar TimeSeries object")
      .def("apply",py::overload_cast<Seismogram&>(&VectorTaper::apply),
         "Apply taper to a Seismogram (3C) object")
    ;
}


} // namespace mspasspy
} // namespace mspass
