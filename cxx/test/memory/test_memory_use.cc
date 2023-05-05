#include <iostream>
#include <assert.h>
#include "mspass/utility/MsPASSError.h"
#include "mspass/seismic/Seismogram.h"
#include "mspass/seismic/TimeSeries.h"
#include "mspass/seismic/Ensemble.h"
#include "mspass/utility/memory_constants.h"
#include "mspass/utility/ErrorLogger.h"
#include "mspass/utility/ProcessingHistory.h"

using namespace std;
using namespace mspass::utility;
using namespace mspass::utility::memory_constants;
using namespace mspass::seismic;
int main(int argc, char **argv)
{
    Seismogram s1;
    TimeSeries ts1;
    size_t object_size,mem0,memnow;
    object_size=sizeof(s1);
    cout << "Raw sizeof for default constructed Seismogram="<<object_size<<endl;
    cout << "Size of same computed by memory_use method=" << s1.memory_use()<<endl;
    mem0=s1.memory_use();
    s1.set_npts(1000);
    cout << "Size after setting npts to 1000 - allocates 3*1000 matrix: "
      << s1.memory_use()<<endl;
    memnow = s1.memory_use() - mem0;
    cout << memnow << " "<< 3*s1.npts()*sizeof(double) <<endl;
    assert (memnow == (3*s1.npts()*sizeof(double)));
    s1.set_npts(100);
    cout << "After setting back to 100="<<s1.memory_use()<<endl;
    memnow = s1.memory_use() - mem0;
    assert (memnow == (3*s1.npts()*sizeof(double)));

    memnow=s1.memory_use();
    /* Add a few metadata entries */
    s1.put("foo","bar");
    s1.put("one",1);
    s1.put("two",2.0);
    s1.put("bool",true);
    cout << "Difference after inserting 4 metadata entries="<<s1.memory_use()-memnow<<endl;
    assert ( (s1.memory_use()-memnow) == (MD_AVERAGE_SIZE+KEY_AVERAGE_SIZE)*4);

    object_size=sizeof(ts1);
    mem0 = ts1.memory_use();
    cout << "Raw sizeof for default constructed Seismogram="<<object_size<<endl;
    cout << "Size of same computed by memory_use method=" << ts1.memory_use()<<endl;
    ts1.set_npts(1000);
    cout << "Size after setting npts to 1000: "
      << ts1.memory_use()<<endl;
    memnow = ts1.memory_use()-mem0;
    assert(memnow == ts1.npts()*sizeof(double));
    ts1.set_npts(100);
    cout << "After setting back to 100="<<ts1.memory_use()<<endl;
    memnow = ts1.memory_use()-mem0;
    assert(memnow == ts1.npts()*sizeof(double));

    memnow=ts1.memory_use();
    /* Add a few metadata entries */
    ts1.put("foo","bar");
    ts1.put("one",1);
    ts1.put("two",2.0);
    ts1.put("bool",true);
    cout << "Difference after inserting 4 metadata entries="<<ts1.memory_use()-memnow<<endl;
    assert ( (ts1.memory_use()-memnow) == (MD_AVERAGE_SIZE+KEY_AVERAGE_SIZE)*4);

    /* Now add an error log entry - keep copy of ts1 for comparison */
    TimeSeries ts2(ts1);
    ts2.elog.log_error("testmemory","this is a message",ErrorSeverity::Complaint);
    cout << "Memory change with elog entry="<<ts2.memory_use()-ts1.memory_use()<<endl;
    assert((ts2.memory_use()-ts1.memory_use()) == (ELOG_AVERAGE_SIZE*ts2.elog.size()));
    memnow = ts2.memory_use();
    /* This just initializes - not sure it is necessary but safter to
    be sure new_map method call is successful.*/
    ts2.set_as_origin("testmemory","foo","bar",AtomicType::TIMESERIES);
    ts2.new_map("testmemory","testmap",AtomicType::TIMESERIES);
    cout << "Size of processing history container="<<ts2.number_of_stages()<<endl;
    cout << "Memory change with history record addition="<<ts2.memory_use()-memnow<<endl;
    assert((ts2.memory_use()-memnow) == (HISTORYDATA_AVERAGE_SIZE*ts2.number_of_stages()));

    /* Now test ensembles.   Note these are TimeSeriesEnsemble and
    SeismogramEnsemble in the python api - defined as that in pybind11 code */
    LoggingEnsemble<TimeSeries> tse;
    LoggingEnsemble<Seismogram> ens;
    size_t member_mem;
    tse.member.push_back(ts1);
    tse.member.push_back(ts2);
    tse.set_live();   // not necessary but safer long term
    member_mem = ts1.memory_use()+ts2.memory_use();
    cout << "TimeSeriesEnsemble output of memory_use method="<<tse.memory_use()<<endl;
    memnow = sizeof(tse) + member_mem;
    assert(tse.memory_use()==memnow);
    /* Test adding metadata */
    tse.put("foo","bar");
    assert((tse.memory_use()-memnow)==(MD_AVERAGE_SIZE+KEY_AVERAGE_SIZE));
    /* Now test adding elog entry */
    memnow = tse.memory_use();
    tse.elog.log_error("testmemory","this is a message",ErrorSeverity::Complaint);
    assert((tse.memory_use()-memnow) == ELOG_AVERAGE_SIZE);
    /* similar for seismogram object*/
    ens.member.push_back(s1);
    s1.set_npts(200);
    ens.member.push_back(s1);
    s1.set_npts(500);
    ens.member.push_back(s1);
    ens.set_live();   // not necessary but safer long term
    member_mem=0;
    for(auto p=ens.member.begin();p!=ens.member.end();++p) member_mem += p->memory_use();
    cout << "SeismogramEnsemble output of memory_use method="<<ens.memory_use()<<endl;
    memnow = sizeof(ens) + member_mem;
    assert(ens.memory_use()==memnow);
    /* Test adding metadata */
    ens.put("foo","bar");
    assert((ens.memory_use()-memnow)==(MD_AVERAGE_SIZE+KEY_AVERAGE_SIZE));
    /* Now test adding elog entry */
    memnow = ens.memory_use();
    ens.elog.log_error("testmemory","this is a message",ErrorSeverity::Complaint);
    assert((ens.memory_use()-memnow) == ELOG_AVERAGE_SIZE);

}
