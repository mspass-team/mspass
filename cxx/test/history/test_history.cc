#include <string>
#include <vector>
#include <list>
#include <map>
#include <algorithm>
#include <iostream>
#include <sstream>
#include "mspass/utility/ProcessingHistory.h"

using namespace std;
using namespace mspass::utility;
/* Translates ProcessingStatus to word for printing */
string status_string(const ProcessingStatus stat)
{
  string s;
  switch(stat)
  {
    case ProcessingStatus::RAW:
      s="RAW";
      break;
    case ProcessingStatus::ORIGIN:
      s="ORIGIN";
      break;
    case ProcessingStatus::VOLATILE:
      s="VOLATILE";
      break;
    case ProcessingStatus::SAVED:
      s="SAVED";
      break;
    default:
      s="UNDEFINED";
  };
  return s;
}
string type_string(const AtomicType t)
{
  string s;
  switch(t)
  {
    case AtomicType::SEISMOGRAM:
      s="Seismogram";
      break;
    case AtomicType::TIMESERIES:
      s="TimeSeries";
      break;
    default:
      s="INVALID";
  };
  return s;
}

int print_raw_map(const ProcessingHistory& ph)
{
  multimap<string,NodeData> n=ph.get_nodes();
  cout << "Multimap contents in container order:"<<endl
    << "keyuuid parentuuid status type algorithm algid stage"<<endl;
  multimap<string,NodeData>::const_iterator nptr;
  for(nptr=n.begin();nptr!=n.end();++nptr)
  {
    cout << nptr->first<<" ";
    NodeData nd=nptr->second;
    cout << nd.uuid <<" "
       << status_string(nd.status)<<" "
       << type_string(nd.type)<<" "
       << nd.algorithm<<" "
       << nd.algid<<" "
       << nd.stage
       <<endl;
  }
  return n.size();
}
void print_nodedata(const NodeData nd)
{
  cout << nd.uuid <<" "
     << status_string(nd.status)<<" "
     << type_string(nd.type)<<" "
     << nd.algorithm<<" "
     << nd.algid<<" "
     << nd.stage
     <<endl;
}
int print_history(const ProcessingHistory& ph, const string title)
{
  int ret;
  cout << title<<endl;
  if(ph.is_empty())
  {
    cout << "No history data to print"<<endl;
    return 0;
  }
  cout << "NodeData at head of chain: (uuid,status,type,algorithm,algid,stage)"<<endl;
  NodeData nd=ph.current_nodedata();
  print_nodedata(nd);
  ret=print_raw_map(ph);
  return ret;
}
int main(int argc, char **argv)
{
  try{
    cout << "Trying constructor for base class BasicProcessingHistory"<<endl;
    BasicProcessingHistory bh;
    bh.set_jobname("testjob");
    bh.set_jobid("999");
    assert(bh.jobname() == "testjob");
    assert(bh.jobid() == "999");
    cout << "Trying copy constructor"<<endl;
    BasicProcessingHistory bh2(bh);
    assert(bh2.jobname() == "testjob");
    assert(bh2.jobid() == "999");;
    cout << "Trying default constructor for ProcessingHistory"<<endl;
    ProcessingHistory ph0;
    cout << "Trying constructor with jobname and jobid args"<<endl;
    ProcessingHistory ph("testjob","999");
    assert(bh.jobname() == "testjob");
    assert(bh.jobid() == "999");
    cout<< "Verifying an empty history record will be handled correctly by print_history test function"
      <<endl;
    int ret;   // used repeatedly below for the return from this function
    ret=print_history(ph,"Empty ProcessingHistory data test");
    assert(ret==0);
    cout << "Creating a RAW record"<<endl;
    ph.set_as_origin("fakeinput","0","fakeuuid1",AtomicType::SEISMOGRAM,true);
    cout << "Success:  contents now"<<endl;
    ret=print_history(ph,"Test for new chain created with set_as_origin RAW");
    assert(ret==0);  // 0 because return is the side of the node multimap - here only current set
    /* We do assert here on these getters because all the NodeData values are
    predictable here - they aren't later when newid is called to create random
    uuids*/
    assert(ph.stage() == 0);
    assert(ph.status() == ProcessingStatus::RAW);
    assert(ph.id() == "fakeuuid1");
    /* At the moment there are no getters for algorithm, algid, and mytype.
    We verify them though this method */
    NodeData nd(ph.current_nodedata());
    assert(nd.algorithm == "fakeinput");
    assert(nd.algid == "0");
    assert(nd.type == AtomicType::SEISMOGRAM);
    /* This tests operators for NodeData */
    NodeData nd2=nd;
    assert(nd2 == nd);
    assert( !(nd2 != nd));
    /* this is kind of a trivial test, but does exercise something useful to
    verify */
    assert(ph.number_inputs() == 0);
    /* Will create a fairly extensive chain before doing any assert calls
    to validate the results.  The assert tests are then accumulative and
    will validate something is wrong but will require debugging if there
    are mismatches */
    cout << "Trying new_map method"<<endl;
    // Intentionally test default of volatile for status
    ph.new_map("onetoone","0",AtomicType::SEISMOGRAM);
    cout << "Success - tree should now have one branch"<<endl;
    ret=print_history(ph,"ProcessingHistory with single one-to-one map process");
    assert(ret==1);
    /* First a clean test of new reduction with 4 completely independent
    inputs that simulate the normal expected flow*/
    ProcessingHistory vecph[4];
    for(size_t k=0;k<4;++k)
    {
      stringstream ss;
      ss << "fakeid_"<<k;
      vecph[k].set_as_origin("fakedataset","0",ss.str(),AtomicType::SEISMOGRAM,true);
      vecph[k].new_map("onetoone","0",AtomicType::SEISMOGRAM);
    }
    cout << "Testing new_ensemble_process with 4 independent inputs on clone of previous"<<endl;
    vector<ProcessingHistory*> inps;
    for(size_t k=0;k<4;++k) inps.push_back(&(vecph[k]));
    ProcessingHistory phred(ph);
    phred.new_ensemble_process("testreduce","0",AtomicType::SEISMOGRAM,inps);
    ret=print_history(phred,"new_ensemble_process test with 4 independent inputs");
    assert(ret==8);
    /* Second new_ensemble_process test.  We will used a fixed reference of 4
    copies of ph with different entries with new_map on each. This verifies
    handling of duplicate nodes in input chains copied to current*/
    cout << "Testing new_ensemble_process with 4 modified clones of current history object"<<endl
       << "Note creation also tests copy constructor and operator="<<endl;
    ProcessingHistory ph1(ph);
    ProcessingHistory ph2(ph);
    ProcessingHistory ph3(ph);
    ProcessingHistory ph4;  // use this one to also test operator=
    ph4=ph;
    ret=print_history(ph,"Contents of master to be cloned");
    ph1.new_map("proc1","0",AtomicType::SEISMOGRAM);
    ph2.new_map("proc2","0",AtomicType::SEISMOGRAM);
    ph3.new_map("proc3","0",AtomicType::SEISMOGRAM);
    ph4.new_map("proc4","0",AtomicType::SEISMOGRAM);
    /* inps was used above so we have to clear it */
    inps.clear();
    inps.push_back(&ph1);
    inps.push_back(&ph2);
    inps.push_back(&ph3);
    inps.push_back(&ph4);
    /* Add the reduction to another clone */
    ProcessingHistory phredtest(ph);
    cout << "Copy creations finished"<<endl
      <<"First here is one branch of the tree data - head should show proc2"<<endl;
    ret=print_history(ph2,"proc2 branch for new_ensemble_process test");
    assert(ret==2);
    cout << "Trying new_ensemble_process with 4 inputs"<<endl;
    phredtest.new_ensemble_process("testreduce","0",AtomicType::SEISMOGRAM,inps);
    cout << "Succeeded:"<<endl;
    ret=print_history(phredtest,"Second new_ensemble_process test contents");
    assert(ret==9);
//cout << "Debug - above print returned count="<<ret<<endl;
    cout << "Testing map_as_saved method applied to chain just printed"<<endl;
    phredtest.map_as_saved("testwriter","0",AtomicType::SEISMOGRAM);
    cout << "Completed:"<<endl;
    ret=print_history(phredtest,"new_ensemble_process test 2 with map_as_saved called a end");
//cout << "Debug - after fix returned node count="<<ret<<endl;
    assert(ret==10);
    /* this is a brutal test that may be misleading as it uses two parents
    who are too closely related - kind of pharoah's children.  Note
    because add_many_inputs makes multiple calls to add_one_input this test
    exercises both*/
    phredtest.clear();
    /* Verify clear works as it should */
    ret=print_history(phredtest,"test of clear method - should have no data now");
    assert(ret==0);
    phredtest.new_ensemble_process("testreduce","1",AtomicType::SEISMOGRAM,inps);
    phredtest.add_many_inputs(inps);
    ret=print_history(phredtest,"incestuous add_many(one)_inputs test result");
    /* Note the return here is identical to previous because of the incest
    problem with the above test - duplicates are weeded from the nodes tree
    in new_ensemble_process so no actual results are added.  That behavior may be
    problematic if the user needs to say an input is used more than once in the
    same algorithm but that type of thing is probably better posted in
    some other form.  i.e. some forms of history may need to be preserved by
    storing some object in Metadata.*/
    assert(ret==13);
    //cout << "Return count from print_history for the above="<<ret<<endl;
    cout << "Testing accumulate - test uses same 4 inputs (vecph)- should return a tree of the same size"<<endl;
    phredtest.clear();
    cout << "First test with the container empty on first pass"<<endl;
    for(int k=0;k<4;++k)
    {
      phredtest.accumulate("testaccumulate","algid1",AtomicType::SEISMOGRAM,vecph[k]);
    }
    ret=print_history(phredtest,"testaccumulate result with 4 input");
    cout << "Tree node size="<<ret<<endl;
    assert(ret==8);
    cout << "Testing additional accumulate of same data - i.e. 8 inputs each doubled"<<endl;
    for(int k=0;k<4;++k)
    {
      phredtest.accumulate("testaccumulate","algid1",AtomicType::SEISMOGRAM,vecph[k]);
    }
    ret=print_history(phredtest,"testaccumulate result with 8 input");
    cout << "Tree node size="<<ret<<endl;
    assert(ret==12);
    cout << "Testing additional accumulate with new algorithm name - should yield 4 inputs from autoclear"<<endl;
    /* I think this simulates correctly what reduce will do on startup */
    phredtest=vecph[0];
    // note we intentionally start at 1 here
    for(int k=1;k<4;++k)
    {
      phredtest.accumulate("testaccumulate","algid2",AtomicType::SEISMOGRAM,vecph[k]);
    }
    ret=print_history(phredtest,"testaccumulate2 result");
    cout << "Tree node size="<<ret<<endl;
    assert(ret==8);
    cout << "Testing accumulate with 4 inputs split 2 each on two processes"<<endl;
    ProcessingHistory phred_1(ph);
    ProcessingHistory phred_2(ph);
    phred_1.clear();
    phred_2.clear();
    phred_1.accumulate("testsplit","algid1",AtomicType::SEISMOGRAM,vecph[0]);
    phred_1.accumulate("testsplit","algid1",AtomicType::SEISMOGRAM,vecph[1]);
    phred_2.accumulate("testsplit","algid1",AtomicType::SEISMOGRAM,vecph[2]);
    phred_2.accumulate("testsplit","algid1",AtomicType::SEISMOGRAM,vecph[3]);
    print_history(phred_1,"Simulated process 1 data");
    print_history(phred_2,"Simulated process 2 data");
    phred_1.accumulate("testsplit","algid1",AtomicType::SEISMOGRAM,phred_2);
    print_history(phred_1,"Merge of process and 2 with accumulate");
    cout << "Testing clean_accumulate_uuids"<<endl;
    phred_1.clean_accumulate_uuids();
    print_history(phred_1,"After clean_accumulate_uuids");
  }
  catch(MsPASSError& merr)
  {
    cerr << "MsPASSError caught:"<<endl;
    merr.log_error();
  }
  catch(std::exception& serr)
  {
    cerr << "Something threw this generic std::exception:"<<endl;
    cerr << serr.what();
  }
}
