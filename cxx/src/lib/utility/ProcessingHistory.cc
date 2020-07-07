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
NodeData::NodeData(const NodeData& parent) : uuid(parent.uuid)
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
  current_pdef.algorithm="UNDEFINED";
  current_pdef.id="UNDEFINED";
}
ProcessingHistory::ProcessingHistory(const ProcessingHistory& parent)
  : BasicProcessingHistory(parent),elog(parent.elog),nodes(parent.nodes),
      process_data(parent.process_data),current_pdef(parent.current_pdef)
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
void ProcessingHistory::set_as_origin(const string alg,const string algid,
  const string uuid,const AtomicType typ, bool define_as_raw)
{
  const string base_error("ProcessingHistory::set_as_origin:  ");
  if( (nodes.size()>0) || (process_data.size()>0) )
  {
    elog.log_error(alg+":"+algid,
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
  current_pdef.algorithm=alg;
  current_pdef.id=algid;
  current_id=uuid;
  mytype=typ;
  /* Origin/raw are always defined as stage 0 even after a save. */
  current_stage=0;
}
string ProcessingHistory::new_reduction(const string alg,const string algid,
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
  have clear the nodes and process_data areas before inserting parent
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
    multimap<ProcessDefinition,string,AlgorithmCompare>
          parent_process_data(parents[i]->get_process_data());
    for(pptr=parent_process_data.begin();pptr!=parent_process_data.end();++pptr)
    {
      this->process_data.insert(*pptr);
    }
  }
  /* Now reset the current contents to make it the base of the history tree */
  ++current_stage;
  current_pdef.algorithm=alg;
  current_pdef.id=algid;
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
  multimap<ProcessDefinition,string,AlgorithmCompare>::iterator pptr;
  multimap<string,NodeData> newhistory = data_to_add.get_nodes();
  if(newhistory.size()>0)
  for(nptr=newhistory.begin();nptr!=newhistory.end();++nptr)
    this->nodes.insert(*nptr);
  multimap<ProcessDefinition,string,AlgorithmCompare>
                  newpds=data_to_add.get_process_data();
  for(pptr=newpds.begin();pptr!=newpds.end();++pptr)
    this->process_data.insert(*pptr);
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
string ProcessingHistory::new_map(const string alg,const string algid,
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
    process_data=parent.get_process_data();
  }
  else
  {
    /* In this case we have to push current data to the history chain */
    NodeData nd;
    nd.status=ProcessingStatus::VOLATILE;
    nd.uuid=current_id;
    nd.type=typ;
    nd.stage=current_stage;
    pair<string,NodeData> pn(current_id,nd);
    this->nodes.insert(pn);
    pair<ProcessDefinition,string> ppd(current_pdef,current_id);
    this->process_data.insert(ppd);
  }
  /* We always need a new id here for this object we are handling as the child */
  current_id=this->newid();
  current_pdef=ProcessDefinition(alg,algid);
  current_status=newstatus;   //Probably should default in include file to VOLATILE
  ++current_stage;
  mytype=typ;
  return current_id;
}
/* Note we always trust that the parent history data is ok in this case
assuming this would only be called immediately after a save.*/
string ProcessingHistory::map_as_saved(const string alg,const string algid,
  const AtomicType typ)
{
  /* This is essentially pushing current data to the end of the history chain.*/
  NodeData nd;
  nd.status=current_status;
  nd.uuid=current_id;
  nd.type=typ;
  nd.stage=current_stage;
  pair<string,NodeData> pn(current_id,nd);
  this->nodes.insert(pn);
  pair<ProcessDefinition,string> ppd(current_pdef,current_id);
  this->process_data.insert(ppd);
  /* Now we reset current to define it as the saver.  Then calls to the
  getters for the multimap will properly insert this data as the end of the
  chain.  Note a key difference from new_map is we don't create a new uuid.
  I don't think that will cause an ambiguity, but it might be better to
  just create a new one here - will do it this way unless that proves a problem
  as the equality of the two might be a useful test for other purposes */
  current_pdef=ProcessDefinition(alg,algid);
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
  pair<string,NodeData> pn(current_id,nd);
  /* Get deep copy of current nodes data and then insert the current data into
  it before returning*/
  multimap<string,NodeData> result(this->nodes);
  result.insert(pn);
  return result;
}
multimap<ProcessDefinition,string,AlgorithmCompare>
        ProcessingHistory::get_process_data() const
{
  multimap<ProcessDefinition,string,AlgorithmCompare> result(this->process_data);
  pair<ProcessDefinition,string> ppd(this->current_pdef,current_id);
  result.insert(ppd);
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

list<string> ProcessingHistory::algorithm_history() const
{
  list<string> result;
  /*The multimap has no method to get unique keys. We use an increment
  algorithm because we don't expect the size of the map for each key to
  be huge.  Books recommend using upper_bound to jump forward if the map is
  huge.  */
  multimap<ProcessDefinition,string,AlgorithmCompare>::const_iterator mptr;
  string testid;
  string thisalg;
  size_t ii(0);   //counter
  for(mptr=process_data.begin();mptr!=process_data.end();++mptr)
  {
    if(ii==0)
    {
      testid=mptr->first.id;
      thisalg=mptr->first.algorithm;
    }
    else
    {
      if((mptr->first.id)==testid)
      {
        ++ii;
      }
      else
      {
        /* this is a slow way to do this - creation on each instance, but
        the chatter on the web suggests clearing a stringstring has platform
        dependencies.  Since I don't expect this method to be called millions
        of times this should be a minor issue, but it is definitely an inefficiency.*/
        stringstream ss;
        ss<<testid<<":"<<thisalg<<":"<<ii;
        result.push_back(ss.str());
        testid==mptr->first.id;
        thisalg=mptr->first.algorithm;
        ii=1;   //1 because mptr is currently pointing to next block
      }
    }
  }
  /* Save last block*/
  stringstream ssfinal;
  ssfinal<<testid<<":"<<thisalg<<":"<<ii;
  result.push_back(ssfinal.str());
  return result;
};
list<string> ProcessingHistory::data_processed_by(const string alg,const string algid) const
{
  list<string> result;
  ProcessDefinition test;
  test.algorithm=alg;
  test.id=algid;
  // return empty list immediately if there is no match
  if(process_data.count(test)<=0) return result;

  pair<multimap<ProcessDefinition,string,AlgorithmCompare>::const_iterator,
       multimap<ProcessDefinition,string,AlgorithmCompare>::const_iterator> rng;
  rng=process_data.equal_range(test);
  multimap<ProcessDefinition,string,AlgorithmCompare>::const_iterator mptr;
  for(mptr=rng.first;mptr!=rng.second;++mptr)
  {
    result.push_back(mptr->second);
  }
  return result;
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
    process_data=parent.process_data;
    current_status=parent.current_status;
    current_id=parent.current_id;
    current_stage=parent.current_stage;
    mytype=parent.mytype;
    current_pdef=parent.current_pdef;
  }
  return *this;
}
}//End mspass namespace encapsulation
