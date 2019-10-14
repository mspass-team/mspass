

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass7dmatrixE_docstring[] = R"CHIMERA_STRING( 
This class defines a lightweight, simple double precision matrix.
Provides basic matrix functionality. Note that elements of the
matrix are stored internally in FORTRAN order but using
C style indexing.  That is, all indices begin at 0, not 1 and 
run to size - 1.  Further, FORTRAN order means the elements are
actually ordered in columns as in FORTRAN in a continuous,
logical block of memory.  This allow one to use the BLAS functions
to access the elements of the matrix.  As usual be warned this
is useful for efficiency and speed, but completely circumvents the
bounds checking used by methods in the object.  
 
)CHIMERA_STRING";

constexpr char _ZNK6mspass7dmatrix4rowsEv_docstring[] = R"CHIMERA_STRING( Return number of rows in this matrix. 
)CHIMERA_STRING";

constexpr char _ZNK6mspass7dmatrix7columnsEv_docstring[] = R"CHIMERA_STRING( Return number of columns in this matrix. 
)CHIMERA_STRING";

constexpr char _ZNK6mspass7dmatrix4sizeEv_docstring[] = R"CHIMERA_STRING( 
  This function returns an std::vector with 2 elements with size information.
  first component is rows, second is columns.  This simulates
  the matlab size function. 
)CHIMERA_STRING";

constexpr char _ZN6mspass7dmatrix4zeroEv_docstring[] = R"CHIMERA_STRING( Initialize a matrix to all zeros. 
)CHIMERA_STRING";


} // namespace

void _ZN6mspass7dmatrixE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::dmatrix >("dmatrix", _ZN6mspass7dmatrixE_docstring, boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[]() -> mspass::dmatrix * { return new mspass::dmatrix(); }, ::boost::python::default_call_policies()))
        .def("__init__", ::boost::python::make_constructor(+[](const int nr, const int nc) -> mspass::dmatrix * { return new mspass::dmatrix(nr, nc); }, ::boost::python::default_call_policies(), (::boost::python::arg("nr"), ::boost::python::arg("nc"))))
        .def("rows", +[](const mspass::dmatrix *self) -> int { return self->rows(); }, _ZNK6mspass7dmatrix4rowsEv_docstring)
        .def("columns", +[](const mspass::dmatrix *self) -> int { return self->columns(); }, _ZNK6mspass7dmatrix7columnsEv_docstring)
        .def("size", +[](const mspass::dmatrix *self) -> std::vector<int> { return self->size(); }, _ZNK6mspass7dmatrix4sizeEv_docstring)
        .def("zero", +[](mspass::dmatrix *self) { self->zero(); }, _ZN6mspass7dmatrix4zeroEv_docstring)
    ;

    
}


