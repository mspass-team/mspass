

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass14SlownessVectorE_docstring[] = R"CHIMERA_STRING( 
 Slowness vectors are a seismology concept used to describe wave propagation.
 A slowness vector points in the direction of propagation of a wave with a
 magnitude equal to the slowness (1/velocity) of propagation.  
)CHIMERA_STRING";

constexpr char _ZNK6mspass14SlownessVector3magEv_docstring[] = R"CHIMERA_STRING( Computes the magntitude of the slowness vector.
 Value returned is in units of seconds/kilometer.  
)CHIMERA_STRING";

constexpr char _ZNK6mspass14SlownessVector7azimuthEv_docstring[] = R"CHIMERA_STRING( Returns the propagation direction defined by a slowness vector.
 Azimuth is a direction clockwise from north in the standard geographic
 convention.  Value returned is in radians.
)CHIMERA_STRING";

constexpr char _ZNK6mspass14SlownessVector3bazEv_docstring[] = R"CHIMERA_STRING( Returns the back azimuth direction defined by a slowness vector.
 A back azimuth is 180 degrees away from the direction of propagation and 
 points along the great circle path directed back to the source point 
 from a given position.  The value returned is in radians.
)CHIMERA_STRING";


} // namespace

void _ZN6mspass14SlownessVectorE()
{
    

    ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::SlownessVector >("SlownessVector", _ZN6mspass14SlownessVectorE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[]() -> mspass::SlownessVector * { return new mspass::SlownessVector(); }, ::boost::python::default_call_policies()))
        .def("__init__", ::boost::python::make_constructor(+[](double ux0, double uy0) -> mspass::SlownessVector * { return new mspass::SlownessVector(ux0, uy0); }, ::boost::python::default_call_policies(), (::boost::python::arg("ux0"), ::boost::python::arg("uy0"))))
        .def("__init__", ::boost::python::make_constructor(+[](double ux0, double uy0, double az0) -> mspass::SlownessVector * { return new mspass::SlownessVector(ux0, uy0, az0); }, ::boost::python::default_call_policies(), (::boost::python::arg("ux0"), ::boost::python::arg("uy0"), ::boost::python::arg("az0"))))
        .def("mag", +[](const mspass::SlownessVector *self) -> double { return self->mag(); }, _ZNK6mspass14SlownessVector3magEv_docstring)
        .def("azimuth", +[](const mspass::SlownessVector *self) -> double { return self->azimuth(); }, _ZNK6mspass14SlownessVector7azimuthEv_docstring)
        .def("baz", +[](const mspass::SlownessVector *self) -> double { return self->baz(); }, _ZNK6mspass14SlownessVector3bazEv_docstring)
        .def_readwrite("ux", &mspass::SlownessVector::ux)
        .def_readwrite("uy", &mspass::SlownessVector::uy)
    ;

    
}


