

#include <mspass/mspass.h>
#include <boost/python.hpp>


namespace {



} // namespace

void _ZN6mspass7LogDataE()
{
    

    ::boost::python::object parent_object(::boost::python::scope().attr("mspass"));
    ::boost::python::scope parent_scope(parent_object);

    ::boost::python::class_<mspass::LogData >("LogData", boost::python::no_init)
        .def("__init__", ::boost::python::make_constructor(+[]() -> mspass::LogData * { return new mspass::LogData(); }, ::boost::python::default_call_policies()))
        .def("__init__", ::boost::python::make_constructor(+[](int jid, std::string alg, mspass::MsPASSError & merr) -> mspass::LogData * { return new mspass::LogData(jid, alg, merr); }, ::boost::python::default_call_policies(), (::boost::python::arg("jid"), ::boost::python::arg("alg"), ::boost::python::arg("merr"))))
        .def_readwrite("job_id", &mspass::LogData::job_id)
        .def_readwrite("p_id", &mspass::LogData::p_id)
        .def_readwrite("algorithm", &mspass::LogData::algorithm)
        .def_readwrite("badness", &mspass::LogData::badness)
        .def_readwrite("message", &mspass::LogData::message)
    ;

    
}


