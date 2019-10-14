

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass7dvectorE_docstring[] = R"CHIMERA_STRING( 
A vector is a special case of a matrix with one row or column.   In this 
implementation, however, it always means a column vector.   Hence, it is
possible to multiply a vector x and a matrix A as Ax provided they are 
compatible sizes.  This differs from matlab where row and columns vectors
are sometimes used interchangably.   
)CHIMERA_STRING";


} // namespace

void _ZN6mspass7dvectorE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::dvector, ::boost::python::bases<mspass::dmatrix > >("dvector", _ZN6mspass7dvectorE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[]() -> mspass::dvector * { return new mspass::dvector(); }, ::boost::python::default_call_policies()))
        .def("__init__", ::boost::python::make_constructor(+[](int nrv) -> mspass::dvector * { return new mspass::dvector(nrv); }, ::boost::python::default_call_policies(), (::boost::python::arg("nrv"))))
    ;

    
}


