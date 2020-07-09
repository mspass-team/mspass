#include <map>
#include <set>
#include <list>
#include <algorithm>
#include "mspass/utility/MsPASSError.h"
#include "mspass/utility/ProcessingHistory.h"

using namespace std;
using namespace mspass;
namespace mspass{
/* This is an internal function that returns a string description of the
ProcessingStatus enum class */
string status_to_words(const ProcessingStatus status)
{
  string word;
  switch(status)
  {
    case ProcessingStatus::RAW:
      word=string("RAW");
      break;
    case ProcessingStatus::ORIGIN:
      word=string("ORIGIN");
      break;
    case ProcessingStatus::VOLATILE:
      word=string("VOLATILE");
      break;
    case ProcessingStatus::SAVED:
      word=string("SAVED");
      break;
    default:
      word=string("UNDEFINED");
  };
  return word;
}
/* Start of NodeData implementations - all of these could have probably
been defaulted, but defined here for clarity.  Default constructor
definitely does something different from default */
NodeData::NodeData()
{
  status=ProcessingStatus::UNDEFINED;
  uuid="UNDEFINED";
  type=AtomicType::UNDEFINED;
  stage=-1;   //Invalid value could be used as a hint of uninitialized data
}
NodeData::NodeData(const NodeData& parent)
       : uuid(parent.uuid),algorithm(parent.algorithm),algid(parent.algid)
{
  status=parent.status;
  type=parent.type;
  stage=parent.stage;
}
NodeData& NodeData::operator=(const NodeData& parent)
{
  if(&parent != this)
  {
    status=parent.status;
    type=parent.type;
    stage=parent.stage;
    uuid=parent.uuid;
    algorithm=parent.algorithm;
    algid=parent.algid;
  }
  return *this;
}
/* Start of ProcessingHistory code. */

ProcessingHistory::ProcessingHistory():elog()
{
  current_status=ProcessingStatus::UNDEFINED;
  current_id="UNDEFINED";
  current_stage=-1;  //illegal value that could be used as signal for uninitalized
  mytype=AtomicType::UNDEFINED;
  algorithm="UNDEFINED";
  algid="UNDEFINED";
}
ProcessingHistory::ProcessingHistory(const ProcessingHistory& parent)
  : BasicProcessingHistory(parent),elog(parent.elog),nodes(parent.nodes),
      algorithm(parent.algorithm),algid(parent.algid)
{
  current_status=parent.current_status;
  current_id=parent.current_id;
  current_stage=parent.current_stage;
  mytype=parent.mytype;
}
bool ProcessingHistory::is_raw()
{
  if(current_status==ProcessingStatus::RAW)
    return true;
  else
    return false;
}
bool ProcessingHistory::is_origin()
{
  if(current_status==ProcessingStatus::RAW || current_status==ProcessingStatus::ORIGIN)
    return true;
  else
    return false;
}
bool ProcessingHistory::is_volatile()
{
  if(current_status==ProcessingStatus::VOLATILE)
    return true;
  else
    return false;
}
bool ProcessingHistory::is_saved()
{
  if(current_status==ProcessingStatus::SAVED)
    return true;
  else
    return false;
}
size_t ProcessingHistory::number_of_stages()
{
  return current_stage;
}

/* the next set of methods are the primary methdods for managing the history
data.   A key implementation detail is when data marked current is pushed to
the multimaps that handle the history.  In all cases the model is the data
are pushed to the maps when and only they become a parent.   That means
all the methods named "map" something.    A corollary is that when an object
is an origin the multimaps must be empty. */
/* Note we don't distinguish raw and origin here - rec must define it one
way or the other. */
void ProcessingHistory::set_as_origin(const string alg,const string algid_in,
  const string uuid,const AtomicType typ, bool define_as_raw)
{
  const string base_error("ProcessingHistory::set_as_origin:  ");
  if( nodes.size()>0 )
  {
    elog.log_error(alg+":"+algid_in,
      base_error + "Illegal usage.  History chain was not empty.   Calling clear method and continuing",
       ErrorSeverity::Complaint);
    this->clear();
  }
  if(define_as_raw)
  {
    current_status=ProcessingStatus::RAW;
  }
  else
  {
    current_status=ProcessingStatus::ORIGIN;
  }
  algorithm=alg;
  algid=algid_in;
  current_id=uuid;
  mytype=typ;
  /* Origin/raw are always defined as stage 0 even after a save. */
  current_stage=0;
}
string ProcessingHistory::new_reduction(const string alg,const string algid_in,
  const AtomicType typ,const vector<ProcessingHistory*> parents,
    const bool create_newid)
{
  if(create_newid)
  {
    this->newid();
  }
  /* This works because the get methods used here return a deep copy from
  each parent with their current data pushed to define the base of the
  chain.  Because we are bringing in history from other data we also
  have clear the nodes multimap before inserting parent
  data to avoid duplicates - it would be very error prone to require caller
  to clear before calling this method*/
  this->clear();
  multimap<string,NodeData>::const_iterator nptr;
  multimap<ProcessDefinition,string,AlgorithmCompare>::const_iterator pptr;
  size_t i;
  for(i=0;i<parents.size();++i)
  {
    multimap<string,NodeData> parent_node_data(parents[i]->get_nodes());
    for(nptr=parent_node_data.begin();nptr!=parent_node_data.end();++nptr)
    {
      this->nodes.insert(*nptr);
    }
  }
  /* Now reset the current contents to make it the base of the history tree */
  ++current_stage;
  algorithm=alg;
  algid=algid_in;
  // note this is output type - inputs can be variable and defined by nodes
  mytype=typ;
  current_status=ProcessingStatus::VOLATILE;
  return current_id;
}
/* Companion to new_reduction that appends the history of one datum to the
multimap containers.  It does not alter the current values the new_reduction method
MUST have been called before calling this method or the history chain will
become corrupted.*/
void ProcessingHistory::add_one_input(const ProcessingHistory& data_to_add)
{
  multimap<string,NodeData>::iterator nptr;
  multimap<string,NodeData> newhistory = data_to_add.get_nodes();
  if(newhistory.size()>0)
  for(nptr=newhistory.begin();nptr!=newhistory.end();++nptr)
    this->nodes.insert(*nptr);
}
/* This one also doesn't change the current contents because it is just a
 front end to a loop calling add_one_input for each vector component */
void ProcessingHistory::add_many_inputs(const vector<ProcessingHistory*>& d)
{
  vector<ProcessingHistory*>::const_iterator dptr;
  for(dptr=d.begin();dptr!=d.end();++dptr)
  {
    ProcessingHistory *ptr;
    ptr=(*dptr);
    this->add_one_input(*ptr);
  }
}
/* This method always creates a new id which is the return.  If the user
unintentionally creates a new uuid before calling this method the
history chain will be broken.  We may need to build safeties to prevent that.

for a map we have to make a decision about how to handle the parent copy.
By default we assume we have to make a deep copy - the safest algorithm.
When optional use_parent_history is false we simply append to current
history data - more dangerous but also more efficent.*/
string ProcessingHistory::new_map(const string alg,const string algid_in,
  const AtomicType typ,const ProcessingHistory& parent,
  const ProcessingStatus newstatus,bool use_parent_history)
{
  /* Clear and make a copy of the parent's history when asked */
  if(use_parent_history)
  {
    /* In this mode these getters automatically push the parent's current
    data to the history chain so we don't have to do that. */
    this->clear();
    nodes=parent.get_nodes();
  }
  else
  {
    /* In this case we have to push current data to the history chain */
    NodeData nd;
    nd.status=ProcessingStatus::VOLATILE;
    nd.uuid=current_id;
    nd.type=typ;
    nd.stage=current_stage;
    nd.algorithm=algorithm;
    nd.algid=algid;
    pair<string,NodeData> pn(current_id,nd);
    this->nodes.insert(pn);
  }
  /* We always need a new id here for this object we are handling as the child */
  current_id=this->newid();
  algorithm=alg;
  algid=algid_in;
  current_status=newstatus;   //Probably should default in include file to VOLATILE
  ++current_stage;
  mytype=typ;
  return current_id;
}
/* Note we always trust that the parent history data is ok in this case
assuming this would only be called immediately after a save.*/
string ProcessingHistory::map_as_saved(const string alg,const string algid_in,
  const AtomicType typ)
{
  /* This is essentially pushing current data to the end of the history chain.*/
  NodeData nd;
  nd.status=current_status;
  nd.uuid=current_id;
  nd.type=typ;
  nd.stage=current_stage;
  nd.algorithm=algorithm;
  nd.algid=algid_in;
  pair<string,NodeData> pn(current_id,nd);
  this->nodes.insert(pn);
  /* Now we reset current to define it as the saver.  Then calls to the
  getters for the multimap will properly insert this data as the end of the
  chain.  Note a key difference from new_map is we don't create a new uuid.
  I don't think that will cause an ambiguity, but it might be better to
  just create a new one here - will do it this way unless that proves a problem
  as the equality of the two might be a useful test for other purposes */
  algorithm=alg;
  algid=algid_in;
  current_status=ProcessingStatus::SAVED;
  ++current_stage;
  mytype=typ;
  return current_id;
}
multimap<string,NodeData> ProcessingHistory::get_nodes() const
{
  /* first define a one-to-one map for history chain */
  NodeData nd;
  nd.status=this->current_status;
  nd.uuid=this->current_id;
  nd.type=this->mytype;
  nd.stage=this->current_stage;
  nd.algorithm=algorithm;
  nd.algid=algid;
  pair<string,NodeData> pn(current_id,nd);
  /* Get deep copy of current nodes data and then insert the current data into
  it before returning*/
  multimap<string,NodeData> result(this->nodes);
  result.insert(pn);
  return result;
}

/* This is really just a wrapper around the count method.  We do it
because it is an implementation detail to use a multimap in this form */
int ProcessingHistory::number_inputs(const string testuuid) const
{
  // Return result is int to mesh better with python even though
  // count returns size_t
  int n=nodes.count(testuuid);
  return n;
}
int ProcessingHistory::number_inputs() const
{
  return this->number_inputs(current_id);
}
string ProcessingHistory::newid()
{
  boost::uuids::random_generator gen;
  boost::uuids::uuid uuidval;
  uuidval=gen();
  this->current_id=boost::uuids::to_string(uuidval);
  return current_id;
}
void ProcessingHistory::set_id(const string newid)
{
  this->current_id=newid;
}


list<NodeData> ProcessingHistory::inputs(const std::string id_to_find) const
{
  list<NodeData> result;
  // Return empty list immediately if key not found
  if(nodes.count(id_to_find)<=0) return result;
  /* Note these have to be const_iterators because method is tagged const*/
  multimap<string,NodeData>::const_iterator upper,lower;
  lower=nodes.lower_bound(id_to_find);
  upper=nodes.upper_bound(id_to_find);
  multimap<string,NodeData>::const_iterator mptr;
  for(mptr=lower;mptr!=upper;++mptr)
  {
    result.push_back(mptr->second);
  }
  return result;
};

ProcessingHistory& ProcessingHistory::operator=(const ProcessingHistory& parent)
{
  if(this!=(&parent))
  {
    this->BasicProcessingHistory::operator=(parent);
    nodes=parent.nodes;
    current_status=parent.current_status;
    current_id=parent.current_id;
    current_stage=parent.current_stage;
    mytype=parent.mytype;
    algorithm=parent.algorithm;
    algid=parent.algid;
  }
  return *this;
}
//// End ProcessingHistory methods //////
/* This pair of functions in an earlier version were members of
ProcessingHistory.   They were made functions to reduce unnecessary baggage
in the low level ProcessingHistory object that is a base class of all
atomic data in mspass */
/* This is used for sorting tuple in set below */
typedef std::tuple<int,std::string,std::string> Algdata;
class sort_by_stage
{
public:
  bool operator()(const Algdata A, const Algdata B) const
  {
    int i=std::get<0>(A);
    int j=std::get<0>(B);
    return i<j;
  };
};

/* This function uses a completely different algorithm than the prototype
that was once a method.   It also returns a lsit of tuples while the original
only returned a list of names.  The order of the tuple returned is:
stage : algorithm : algid

Note the list is sorted into ascending order by stage*/
list<Algdata> algorithm_history(const ProcessingHistory& h)
{
  /* We use this set container to sort out unique combinations of the
  tuple of 3 pieces of NodeData that form the ouput.   */
  std::set<Algdata> algset;
  multimap<string,NodeData> hmap=h.get_nodes();
  multimap<string,NodeData>::iterator mptr;
  for(mptr=hmap.begin();mptr!=hmap.end();++mptr)
  {
    NodeData n=mptr->second;  //created only to make this more readable
    Algdata work(n.stage,n.algorithm,n.algid);
    /* Intentionally ignore the return of insert.   We expect
    it to return true and false for different elements */
    algset.insert(work);
  }
  /* This sort is creating a mysterious compilation so will
  temporarily disable it to work on testing main class */
  //std::sort(algset.begin(),algset.end(),sort_by_stage);
  list<Algdata> result;
  set<Algdata>::iterator aptr;
  for(aptr=algset.begin();aptr!=algset.end();++aptr)
  {
    result.push_back(*aptr);
  }
  return result;
}
/* this also uses a completely differnet algorithm than that prototype
that was a method.  This is a simple linear scan pulling all uuids that
match alg and algid.  The original method had a different name
(data_processed_by) that only made sense if the function were a member.
This function does the same thing but has a different name that
hopefully is closer to describing what it does */
list<string> algorithm_outputs(const ProcessingHistory& h, const string alg,
     const string aid)
{
  list<string> result;
  multimap<string,NodeData> hmap=h.get_nodes();
  multimap<string,NodeData>::iterator hptr;
  for(hptr=hmap.begin();hptr!=hmap.end();++hptr)
  {
    NodeData n=hptr->second;
    if( (alg==n.algorithm) && (aid==n.algid)) result.push_back(hptr->first);
  }
  return result;
}

}//End mspass namespace encapsulation
