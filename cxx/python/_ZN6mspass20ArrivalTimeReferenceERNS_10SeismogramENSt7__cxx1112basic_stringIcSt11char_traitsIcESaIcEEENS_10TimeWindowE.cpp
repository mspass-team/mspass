

#include <mspass/mspass.h>
#include <boost/python.hpp>


void _ZN6mspass20ArrivalTimeReferenceERNS_10SeismogramENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEENS_10TimeWindowE()
{
    

    // ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    // ::boost::python::scope parent_scope(parent_object);

    ::boost::python::def("ArrivalTimeReference", +[](mspass::Seismogram & din, std::string key, mspass::TimeWindow tw) -> std::shared_ptr<mspass::Seismogram> { return mspass::ArrivalTimeReference(din, key, tw); }, (::boost::python::arg("din"), ::boost::python::arg("key"), ::boost::python::arg("tw")));

    
}


