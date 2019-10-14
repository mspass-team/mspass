

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass19dmatrix_index_errorE_docstring[] = R"CHIMERA_STRING( 
 Thrown by a dmatrix if a requested index is outside the bounds
 of the matrix dimension.
)CHIMERA_STRING";

constexpr char _ZN6mspass19dmatrix_index_error9log_errorEv_docstring[] = R"CHIMERA_STRING( Writes the error message to standard error.
)CHIMERA_STRING";


} // namespace

void _ZN6mspass19dmatrix_index_errorE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::dmatrix_index_error, ::boost::noncopyable, ::boost::python::bases<mspass::MsPASSError > >("dmatrix_index_error", _ZN6mspass19dmatrix_index_errorE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[](const int nrmax, const int ncmax, const int ir, const int ic) -> mspass::dmatrix_index_error * { return new mspass::dmatrix_index_error(nrmax, ncmax, ir, ic); }, ::boost::python::default_call_policies(), (::boost::python::arg("nrmax"), ::boost::python::arg("ncmax"), ::boost::python::arg("ir"), ::boost::python::arg("ic"))))
        .def("log_error", +[](mspass::dmatrix_index_error *self) { self->log_error(); }, _ZN6mspass19dmatrix_index_error9log_errorEv_docstring)
    ;

    
}


