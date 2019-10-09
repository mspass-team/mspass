

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass18dmatrix_size_errorE_docstring[] = R"CHIMERA_STRING( 
Thrown by a dmatrix when two matrices have a size mismatch.
)CHIMERA_STRING";

constexpr char _ZN6mspass18dmatrix_size_error9log_errorEv_docstring[] = R"CHIMERA_STRING( Writes the error message to standard error.
)CHIMERA_STRING";


} // namespace

void _ZN6mspass18dmatrix_size_errorE()
{
    

    ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::dmatrix_size_error, ::boost::noncopyable, ::boost::python::bases<mspass::MsPASSError > >("dmatrix_size_error", _ZN6mspass18dmatrix_size_errorE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[](const int nr1, const int nc1, const int nr2, const int nc2) -> mspass::dmatrix_size_error * { return new mspass::dmatrix_size_error(nr1, nc1, nr2, nc2); }, ::boost::python::default_call_policies(), (::boost::python::arg("nr1"), ::boost::python::arg("nc1"), ::boost::python::arg("nr2"), ::boost::python::arg("nc2"))))
        .def("log_error", +[](mspass::dmatrix_size_error *self) { self->log_error(); }, _ZN6mspass18dmatrix_size_error9log_errorEv_docstring)
    ;

    
}


