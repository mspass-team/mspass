

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {

constexpr char _ZN6mspass13TimeWindowCmpE_docstring[] = R"CHIMERA_STRING( 
// TimeWindow objects are used, among other things, to define real
// or processed induced data gaps.
// The set container requires a weak ordering function like to correctly
// determine if a time is inside a particular time window.
//
)CHIMERA_STRING";


} // namespace

void _ZN6mspass13TimeWindowCmpE()
{
    

    ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::TimeWindowCmp, ::boost::noncopyable >("TimeWindowCmp", _ZN6mspass13TimeWindowCmpE_docstring, boost::python::no_init)
    ;

    
}


