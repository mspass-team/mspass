

#include <mspass/mspass.h>
#include <boost/python.hpp>


void _ZN6mspass2trERKNS_7dmatrixE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::def("tr", +[](const mspass::dmatrix & A) -> mspass::dmatrix { return tr(A); }, (::boost::python::arg("A")));

    
}


