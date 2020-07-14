#ifndef _PROCESSING_HISTORY_H_
#define _PROCESSING_HISTORY_H_
#include <string>
#include <list>
#include <vector>
#include <map>
#include <boost/serialization/map.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_serialize.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include "mspass/utility/ErrorLogger.h"
//#include "mspass/seismic/Ensemble.h"
namespace mspass{
/*! This enum class is used to define status of processing of a datum.
We use this mechanism to help keep the history data from creating memory
bloat.   It is alwo helpul to build a linked list of a chain of data
that have to be handled somewhat differently. See documentation for
classes below for further info about how this is used */
enum class ProcessingStatus
{
  RAW,
  ORIGIN,
  VOLATILE,
  SAVED,
  UNDEFINED
};
/*! \brief Atomic data type definition for mspass

MsPASS has the concept of atomic types.  One part of that definition is that
such a class must implement this history mechanism.  This enum class will need
to be expanded if new types are added, but the design goal is to make
extension relatively easy - add the data implementation that inherits
ProcessingHistory and add an entry for that type here.
*/
enum class AtomicType
{
  SEISMOGRAM,
  TIMESERIES,
  UNDEFINED
};
/*! \brief Special definition of uuid for a saved record.

We found in the implementation of this that an issue in the use of uuids and
a multimap to define the history tree was both endpoints of any chain were
problematic.  i.e. the root of the tree (end of the chain) and the leaves.
Leaves defined by two alternative properties:  ProcessingStatus RAW or ORIGIN
or uuid of the key equal to the uuid attribute in the NodeData struct (class).
The head data (= root of the tree in this case) have two cases.  During
normal processing the head is defined by the current_id value and the
NodeData attributes stored in the private area of ProcessingHistory.
We found, however, that SAVED had to be treated specially because of the
design goal where the normal procedure would be to flush the history
record after a save.  To do that and have everything else work we adopted
a solution where map_as_saved writes a special record to the multimap
with the key defined by this const string.   This works correctly only
if a writer immediately clears the history record after it is saved.
If not, duplicate values with this key will appear in the nodes multimap
and the history chain will become ambiguous (two trees emerging from the
same root).*/
const string SAVED_ID_KEY("NODEDATA_AT_SAVE");


/*! Base class defining core concepts.  */
class  BasicProcessingHistory
{
public:
  BasicProcessingHistory()
  {
    jid=std::string();
    jnm=std::string();
  };
  BasicProcessingHistory(const std::string jobname,const std::string jobid)
  {
    jid=jobid;
    jnm=jobname;
  };
  BasicProcessingHistory(const BasicProcessingHistory& parent)
  {
    jid=parent.jid;
    jnm=parent.jnm;
  };

  /*! Return number or processing algorithms applied to produce these data.

  Implementations of this method need to return a count of the number of
  processing steps that have been applied to put the data in the current state.
  This method could be made pure, but for convenience the base class always
  returns 0 since it does not implement the actual history mechanism.
  */
  virtual size_t number_of_stages(){return 0;};
  std::string jobid()
  {
    return jid;
  };
  void set_jobid(const std::string& newjid)
  {
    jid=newjid;
  };
  std::string jobname() const
  {
    return jnm;
  };
  void set_jobname(const std::string jobname)
  {
    jnm=jobname;
  };
  BasicProcessingHistory& operator=(const BasicProcessingHistory& parent)
  {
    if(this!=(&parent))
    {
      jnm=parent.jnm;
      jid=parent.jid;
    }
    return *this;
  }
protected:
  std::string jid;
  std::string jnm;
private:
  friend boost::serialization::access;
    template<class Archive>
       void serialize(Archive& ar,const unsigned int version)
    {
      ar & jid;
      ar & jnm;
    };
};
/*! \brief Holds properties of data used as input to algorithm that created this object.

The implementation here uses a multimap to define parents of each uuid
in a history chain.   This class is used mainly internally for ProcessingHistory
to maintain that data.   It will be visible to C++ programs but will not
be visible in python.  One of these entries is created for each parent
data used to create the current data.
*/
class NodeData
{
public:
  /*! status definition of the parent. */
  mspass::ProcessingStatus status;
  /*! uuid of the parent. */
  std::string uuid;
  /*! This enum can be used to track changes in data type.  */
  mspass::AtomicType type;
  /*! Integer count of the number of processing steps applied to create this parent.*/
  int stage;
  /*! \brief Name of algorithm algorithm applied at this stage.

  We use the concept that every processing algorithm has a name keyword
  that togther with an id and/or instance defines a unique definition of
  the algorithm and a set of input parameters that define the algorithm's
  behavior.  Note this is the algorithm that creates the uuid also
  stored in this struct (class)
  */
  std::string algorithm;
  /*! id string to identify this instance of algorithm.

  Only assumption is that the combination of algorithm and id provide
  a unique specification of a particular instance of an algorithm.  That
  means some algorithm and a particular set of control parameters that
  control the outcome of the algorithm.  In MsPASS this is usually the
  ObjectID string of saved parameters in MongoDB, but users can use any
  method they wish to describe a unique combination of parameters and an
  algorithm implementation. */
  std::string algid;
  /* These standard elements could be defaulted, but we implement them
  explicitly for clarity - implemented in the cc file. */
  NodeData();
  NodeData(const NodeData& parent);
  NodeData& operator=(const NodeData& parent);
  bool operator==(const NodeData& other);
  bool operator!=(const NodeData& other);
private:
  friend boost::serialization::access;
    template<class Archive>
       void serialize(Archive& ar,const unsigned int version)
    {
      ar & status;
      ar & uuid;
      ar & type;
      ar & stage;
      ar & algorithm;
      ar & algid;
    };
};
/*! \brief Lightweight class to preserve procesing chain of atomic objects.

This class is intended to be used as a parent for any data object in
MsPASS that should be considered atomic.   It is designed to completely
preserve the chain of processing algorithms applied to any atomic data
to put it in it's current state.   It is designed to save that information
during processing with the core information that can then be saved to
define the state.  Writers for atomic objects inheriting this class should
arrange to save the data contained in it to history collection in MongoDB.
Note that actually doing the inverse is a different problem that
are expected to be implemented as extesions of this class to be used in
special programs used to reconstrut a data workflow and the processing chain
applied to produce any final output.

The design was complicated by the need to keep the history data from causing
memory bloat.  A careless implementation could be prone to that problem
even for modest chains, but we were particularly worried about iterative
algorithms that could conceivably multiply the size of out of control.
There was also the fundamental problem of dealing with transient versus
data stored in longer term storage instead of just in memory. Our
implementation was simplified by using the concept of a unique id with
a Universal Unique IDentifier. (UUID)  Our history mechanism assumes
each data object has a uuid assigned to it on creation by an implementation
id of the one object this particular record is associated with on
dependent mechanism.  That is, whenever a new object is created in MsPASS using
the history feature one of these records will be created for each data
object that is defined as atomic.   This string defines unique key for
the object it could be connected to with the this pointer.  The parents of
the current object are defined by the inputs data structure below.

In the current implementation id is string representation of a uuid
maintained by each atomic object.  We use a string to maximize flexibility
at a minor cost for storage.

Names used imply the following concepts:
 raw - means the data is new input to mspass (raw data from data center,
   field experiment, or simulation).  That tag means now prior history
   can be reconstructed.
 origin - top-level ancestor of current data.  The top of a processing
   chain is always tagged as an origin.  A top level can also be "raw" but not necessarily.
   In particular, readers that load partially processed data should mark
   the data read as an origin, but not raw.
  stage - all processed data objects that are volatile elements within a
    workflow are defined as a stage.   They are presumed to leave their
    existence known only through ancestory preserved in the processing
    chain.  A stage becomes a potential root only when it is saved by
    a writer where the writer will mark that position as a save.  Considered
    calling this a branch, but that doesn't capture the concept right since
    we require this mechanism to correctly perserve splits into multiple
    outputs. We preserve that cleanly for each data object.  That is, the
    implementation make it easy to reconstruct the history of a single
    final data object, but reconstructing interlinks between objects in an
    overall processing flow will be a challenge.  That was a necessary
    compomise to avoid memory bloat.  The history is properly viewed as
    a tree branching from a single root (the final output) to leaves that
    define all it's parents.

  The concepts of raw, origin, and stage are implemented with the
  enum class defined above called ProcessingStatus.  Each history record
  has that as an attribute, but each call to new_stage updates a copy
  kept inside this object to simplify the python wrappers.


*/

class ProcessingHistory : public BasicProcessingHistory
{
public:
  ErrorLogger elog;
  /*! Default constructor. */
  ProcessingHistory();
  /*! Construct and fill in BasicProcessingHistory job attributes.

  \param jobnm - set as jobname
  \param jid - set as jobid
  */
  ProcessingHistory(const std::string jobnm,const std::string jid);
  /*! Standard copy constructor. */
  ProcessingHistory(const ProcessingHistory& parent);
  /*! Return true if the processing chain is empty.

  This method provides a standard test for an invalid, empty processing chain.
  Constructors except the copy constructor will all put this object in
  an invalid state that will cause this method to return true.  Only if
  the chain is initialized properly with a call to set_as_origin will
  this method return a false. */
  bool is_empty() const;
  /*! Return true if the current data is in state defined as "raw" - see class description*/
  bool is_raw()const;
  /*! Return true if the current data is in state defined as "origin" - see class description*/
  bool is_origin() const;
  /*! Return true if the current data is in state defined as "volatile" - see class description*/
  bool is_volatile() const;
  /*! Return true if the current data is in state defined as "saved" - see class description*/
  bool is_saved() const;
  /*! \brief Return number of processing stages that have been applied to this object.

  One might want to know how many processing steps have been previously applied
  to produce the current data.  For linear algorithms that would be useful
  only in debugging, but for an iterative algorithm it can be essential to
  avoid infinite loops with a loop limit parameter.  This method returns
  how many times something has been done to alter the associated data.
  It returns 0 if the data are raw.

  Important note is that the number return is the number of processing steps
  since the last save.   Because a save operation is assumed to save the
  history chain then flush it there is not easy way at present to keep track
  of the total number of stages.  If we really need this functionality it
  could be easily retrofitted with another private variable that is not
  reset when the clear method is called.
  */
  size_t number_of_stages() override;
  /*! Set to define this as the top origin of a history chain.

  This method should be called when a new object is created to initialize the
  history as an origin.  Note again an origin may be raw but not all origins
  are define as raw.   This interface controls that through the boolean
  define_as_raw (false by default).   python wrappers should define an
  alternate set_as_raw method that calls this method with define_as_raw
  set true.

  It is VERY IMPORTANT to realize that the uuid argument passed to this
  method is if fundamental importance.   That
  string is assumed to be a uuid that can be linked to either a parent
  data object read from storage and/or linked to a history chain saved by
  a prior run.  It becomes the current_id for the data to which this
  object is a parent. This method also always does two things that define how
  the contents can be used.  current_stage is ALWAYS set 0.  We distinguish
  a pure origin from an intermediate save ONLY by the status value saved in
  the history chain. That is, only uuids with status set to RAW are viewed
  as guaranteed to be stored.   A record marked ORIGIN is assumed to passed
  through save operation.   To retrieve the history chain from multiple
  runs the pieces have to be pieced together by history data stored in
  MongoDB.

  The contents of the history data structures should be empty when this
  method is called.  That would be the norm for any constructor except
  those that make a deep copy. If unsure the clear method should be called
  before this method is called.  If it isn't empty it will be cleared anyway
  and a complaint message will be posted to elog.

  \param alg is the algorithm names to assign to the origin node.  This
    would normally be a reader name, but it could be a synthetic generator.
  \param algid is an id designator to uniquely define an instance of algorithm.
    Note that algid must itself be a unique keyword or the history chains
    will get scrambled.
  \param uuid unique if for this data object (see note above)
  \param typ defines the data type (C++ class) "this" points to.  It might
    be possible to determine this dynamically, but a design choice was to
    only allow registered classes through this mechanism.  i.e. the enum
    class typ implements has a finite number of C++ classes it accepts.
    The type must be a child ProcessingHistory.
  \param define_as_raw sets status as RAW if true and ORIGIN otherwise.

  \exception  Never throws an exception BUT this method will post a complaint
  to elog if the history data structures are not empty and it the clear
  method needs to be called internally.
  */
  void set_as_origin(const string alg,const string algid,
    const string uuid,const AtomicType typ, bool define_as_raw=false);
  /*! Define history chain for an algorithm with multiple inputs.

  Use this method to define the history chain for an algorithm that has
  multiple inputs for each output.  Each output needs to call this method
  to build the connections that define how all inputs link to the the
  new data being created by the algorithm that calls this method.   We \
  call it reduction because it loosly matches the idea of reduce in map-reduce,
  but don't take that too literally - this method has nothing directly to do
  with reduce in Spark.

  Normally, it makes sense to have the boolean create_newid true so it is
  guaranteed the current_id is unique.  There is little cost in creating a new
  one if there is any doubt the current_id is not a duplicate.  The false option
  is there only for rare cases where the current id value needs to be preserved.

  Note the vector of data passed is raw pointers for efficiency to avoid
  excessive copying.   For normal use this should not create memory leaks
  but make sure you don't try to free what the pointers point to or problems are
  guaranteed.  It is VERY IMPORTANT to realize that all the pointers are
  presumed to point to the ProcessingHistory component of a set of larger data
  object (Seismogram or TimeSeries).  The parents do not all have be a common
  type as if they have valid history data within them their current type
  will be defined.

  This method ALWAYS marks the status as VOLATILE.

  \param alg is the algorithm names to assign to the origin node.  This
    would normally be name defining the algorithm that makes sense to a human.
  \param algid is an id designator to uniquely define an instance of algorithm.
    Note that algid must itself be a unique keyword or the history chains
    will get scrambled.  alg is mostly carried as baggage to make output
    more easily comprehended without additional lookups.
  \param typ defines the data type (C++ class) the algorithm that is generating
    this data will create.
  \param create_newid is a boolean defining how the current id is handled.
    As described above, if true the method will call newid and set that
     as the current id of this data object.  If false the current value is
     left intact.
  \return a string representation of the uuid of the data to which this
    ProcessingHistory is now attached.
  */
  string new_reduction(const string alg,const string algid,
    const AtomicType typ,const vector<ProcessingHistory*> parents,
      const bool create_newid=true);
  /*! \brief Add one datum as an input for current data.

  This method MUST ONLY be called after a call to new_reduction in the
  situation were additional inputs need to be defined that were not
  available at the time new_reduction was called.   An example might be
  a stack that was created within the scope of "algorithm" and then used in
  some way to create the output data.   In any case it differs fundamentally
  from new_reduction in that it does not touch attributes that define the
  current state of "this".  It simply says this is another input to the
  data "this" contains.

  \param data_to_add is the ProcessingHistory of the data object to be
   defined as input.  Note the type of the data to which it is linked
   will be saved as the base of the input chain from data_to_add.  It can
   be different from the type of "this".
  */
  void add_one_input(const ProcessingHistory& data_to_add);
  /*! \brief Define several data objects as inputs.

  This method acts like add_one_input in that it alters only the inputs
  chain.   In fact it is nothing more than a loop over the components of
  the vector calling add_one_input for each component.

  \param d is the vector of data to define as inputs
  */
  void add_many_inputs(const vector<ProcessingHistory*>& d);
    /*! \brief Define this algorithm as a one-to-one map of same type data.

  Many algorithms define a one-to-one map where each one input data object
  creates one output data object.  This (overloaded) version of this method
  is most appropriate when input and output are the same type and the
  history chain (ProcessingHistory) is what the new algorithm will
  alter to make the result when it finishes.   Use the overloaded
  version with a separate ProcessingHistory copy if the current object's
  data are not correct.   In this algorithm the chain for this algorithm
  is simply appended with new definitions.

  \param alg is the algorithm names to assign to the origin node.  This
    would normally be name defining the algorithm that makes sense to a human.
  \param algid is an id designator to uniquely define an instance of algorithm.
    Note that algid must itself be a unique keyword or the history chains
    will get scrambled.  alg is mostly carried as baggage to make output
    more easily comprehended without additional lookups.
  \param typ defines the data type (C++ class) the algorithm that is generating
    this data will create.
  \param newstatus is how the status marking for the output.  Normal (default)
    would be VOLATILE.  This argument was included mainly for flexibility in
    case we wanted to extend the allowed entries in ProcessingStatus.
  */
  std::string new_map(const std::string alg,const std::string algid,
    const AtomicType typ,
        const ProcessingStatus newstatus=ProcessingStatus::VOLATILE);
  /*! \brief Define this algorithm as a one-to-one map.

  Many algorithms define a one-to-one map where each one input data object
  creates one output data object. This class allows the input and output to
  be different data types requiring only that one input will map to one
  output.  It differs from  the overloaded method with fewer arguments
  in that it should be used if you need to clear and refresh the history
  chain for any reason.   Known examples are creating simulation waveforms
  for testing within a workflow that have no prior history data loaded but
  which clone some properties of another piece of data.   This method should
  be used in any situation where the history chain in the current data is wrong
  but the contents are the linked to some other process chain.   It is
  supplied to cover odd cases, but use will likely be rare.

  \param alg is the algorithm names to assign to the origin node.  This
    would normally be name defining the algorithm that makes sense to a human.
  \param algid is an id designator to uniquely define an instance of algorithm.
    Note that algid must itself be a unique keyword or the history chains
    will get scrambled.  alg is mostly carried as baggage to make output
    more easily comprehended without additional lookups.
  \param typ defines the data type (C++ class) the algorithm that is generating
    this data will create.
  \param data_to_clone is reference to the ProcessingHistory section of a parent
    data object that should be used to override the existing history chain.
  \param newstatus is how the status marking for the output.  Normal (default)
    would be VOLATILE.  This argument was included mainly for flexibility in
    case we wanted to extend the allowed entries in ProcessingStatus.
  */
  std::string new_map(const std::string alg,const std::string algid,
    const AtomicType typ,
      const ProcessingHistory& data_to_clone,
        const ProcessingStatus newstatus=ProcessingStatus::VOLATILE);

  /*! \brief Prepare the current data for saving.

  Saving data is treated as a special form of map operation.   That is because
  a save by our definition is always a one-to-one operation with an index entry
  for each atomic object.  This method pushes a new entry in the history chain
  tagged by the algorithm/algid field for the writer.  It differs from new_map
  in the important sense that the uuid is not changed.   The record this
  sets in the nodes multimap will then have the same uuid for the key as the
  that in NodeData.   That along with the status set SAVED can be used
  downstream to recognize save records.

  It is VERY IMPORTANT for use of this method to realize this method saves
  nothing.  It only preps the history chain data so calls that follow
  will retrieve the right information to reconstruct the full history chain.
  Writers should follow this sequence:
    1.  call map_as_saved with the writer name for algorithm definition
    2.  save the data and history chain to MongoDB.
    3.  be sure you have a copy of the uuid string of the data just saved and
        call the clear method.
    4.  call the set_as_origin method using the uuid saved with the algorithm/id
        the same as used for earlier call to map_as_saved.   This makes the
        put ProcessingHistory in a state identical to that produced by a reader.

  \param alg is the algorithm names to assign to the ouput.  This
    would normally be name defining the writer.
  \param algid is an id designator to uniquely define an instance of algorithm.
    Note that algid must itself be a unique keyword or the history chains
    will get scrambled.  alg is mostly carried as baggage to make output
    more easily comprehended without additional lookups.  Note one model to
    distinguish records of actual save and redefinition of the data as an
    origin (see above) is to use a different id for the call to map_as_saved
    and later call to set_as_origin.  This code doesn't care, but that is
    an implementation detail in how this will work with MongoDB.
  \param typ defines the data type (C++ class) that was just saved.
  */
  std::string map_as_saved(const std::string alg,const std::string algid,
    const AtomicType typ);

  void clear();
  /*! Retrieve the nodes multimap that defines the tree stucture branches.

  This method does more than just get the protected multimap called nodes.
  It copies the map and then pushes the "current" contents to the map
  before returning the copy.  This allows the data defines as current to
  not be pushed into the tree until they are needed.   */
  std::multimap<std::string,mspass::NodeData> get_nodes() const;

  /*! Return the current stage count for this object.

  We maintain a counter of the number of processing steps that have been
  applied to produce this data object.   This simple method returns that
  counter.   With this implementation this is identical to number_of_stages.
  We retain it in the API in the event we want to implement an accumulating
  counter.
  */
  int stage() const
  {
    return current_stage;
  };
  /*! Return the current status definition (an enum). */
  ProcessingStatus status() const
  {
    return current_status;
  };
  /*! Return the id of this object set for this history  chain.

  We maintain the uuid for a data object inside this class.  This method
  fetches the string representation of the uuid of this data object.
  */
  std::string id() const
  {
    return current_id;
  };
  pair<std::string,std::string> create_by() const
  {
    pair<std::string,std::string> result(algorithm,algid);
    return result;
  }
  /*! Return all the attributes of current.

  This is a convenience method strictly for the C++ interface (it too
  nonpythonic to be useful to wrap for python).  It returns a NodeData
  class containing the attributes of the head of the chain.  Like the
  getters above that is needed to save that data. */
  NodeData current_nodedata() const;
  /*! Create a new id.

  This creates a new uuid - how is an implementation detail but here we use
  boost's random number generator uuid generator that has some absurdly small
  probability of generating two equal ids.   It returns the string representation
  of the id created. */
  std::string newid();
  /*! Return the number of inputs used to create current data.

  In a number of contexts it can be useful to know the number of inputs
  defined for the current object.  This returns that count.
  */
  int number_inputs()const;
  /*! Return the number of inputs defined for any data in the process chain.

  This overloaded version of number_inputs asks for the number of inputs
  defined for an arbitrary uuid.  This is useful only if backtracing
  the ancestory of a child.

  \param uuidstr is the uuid string to check in the ancestory record.
  */
  int number_inputs(const std::string uuidstr)const;

  /*! Set the uuid manually.

  It may occasionally be necessary to create a uuid by some other mechanism.
  This allows that, but this method should be used with caution and only if
  you understand the consequences.

  \param newid is string definition to use for the id.
  */
  void set_id(const std::string newid);

  /*! \brief Return a list of data that define the inputs to a give uuids.

  This low level getter returns the NodeData objects that define the inputs
  to the uuid of some piece of data that was used as input at some stage
  for the current object.

  \param id_to_find is the uuid for which input data is desired.

  \return list of NodeData that define the inputs.  Will silently return
    empty list if the key is not found.

  */
  std::list<mspass::NodeData> inputs(const std::string id_to_find) const;

  /*! Assignment operator.  */
  ProcessingHistory& operator=(const ProcessingHistory& parent);
/* We make this protected to simplify expected extensions.  In particular,
the process of reconstructing history is a complicated process we don't
want to add as baggage to regular data.  Hence, tools to reconstruct history
(provenance) are expected to extend this class. */
protected:
  /* This map defines connections of each data object to others.  Key is the
  uuid of a given object and the values (second) associated with
  that key are the inputs used to create the data defined by the key uuid */
  std::multimap<std::string,mspass::NodeData> nodes;
private:
  /*  This set of private variables are the values of attributes for
  the same concepts in the NodeData struct/class.   We break them out as
  single variables because they are not always set lumped together.  Hence
  there are also separate getters and setters for each. */
  ProcessingStatus current_status;
  /* uuid of current data object */
  std::string current_id;
  int current_stage;
  AtomicType mytype;
  std::string algorithm;
  std::string algid;


  friend boost::serialization::access;
  template<class Archive>
       void serialize(Archive& ar,const unsigned int version)
  {
      ar & boost::serialization::base_object<BasicProcessingHistory>(*this);
      ar & nodes;
      ar & current_status;
      ar & current_id;
      ar & current_stage;
      ar & mytype;
      ar & algorithm;
      ar & algid;
      ar & elog;
  };
};
/* function prototypes of helpers */

/*! Append history data from a data object to inputs vector.

This is a convenience template that is a wrapper for add_one_input used
as in some contexts in combination with new_reduction.   It handles
automatically ignoring dead data and the casting operation to ProcessingHistory.
It will only work if d can be dynamically cast to a ProcessingHistory.
That means it will work for Seismogram and TimeSeries objects but not
CoreSeismogram or CoreTimeSeries.

\param d is the data to define as an input.
\param his is the history data to append this record to - IMPORTANT this
  function blindly assumes this function is called ONLY after a call to
  his.new_reduction.
*/

template <typename Tdata>
    void append_input(const Tdata& d, ProcessingHistory& his)
{
  if(d.live())
  {
    ProcessingHistory *ptr=dynamic_cast<const ProcessingHistory*>(&d);
    his.add_one_input(*ptr);
  }
};
/* this pair of functions did things that were methods in an earlier
prototype.  What they do is still useful but making them functions
reduced the baggage in the ProcessingHistory class. */

/*! \brief Return a list of algorithms applied to produce this data.

It is often useful to know just the chain of processes that were applied
to produce a data object without the details of the entire tree of
what inputs where processed by what algorithm.  This function cracks
the history chain and returns just such a list as a chain of tuples.
Each tuple has the structure:  stage, algorithm, algid.   The returned
list is sorted by stage alone.  If multiple algorithms were applied at the
same level (stage) the order of the list will be random in algorithm and algid.

\param h is the history chain to be dumped (normally a dynamic cast from a
  Seismogram or TimeSeries object)
 */
std::list<std::tuple<int,std::string,std::string>>
                           algorithm_history(const ProcessingHistory& h);
/*! \brief Return uuids of all data handled by a given processing algorithm that
are parents of this object.

This method is an extended version of algorithm_history.   It returns a list
of uuids matching the algorithm id passed as an argument.  Note for
interactive data exploration a typical usage would be to call algorithm_history
to alg and algid pair of interest and then call this method to get the uuids with
which it is associated.  For linear workflows the return will be equivalent to all
inputs passed through that algorithm.  For iterative algorithms the list
can be much longer as each pass will be post new uuids for the same
algorithm.

\param alg is the algorithm name to search for. (Note:  ignored in this
  implementation but will make any application more readable.)
\param algid is the id string used to uniquely define a algorithm instance.

\return list of uuids handled by that instance of that algorithm.  Silently
  returns an empty list if there is no match
*/
std::list<std::string> algorithm_outputs(const ProcessingHistory& h,
  const std::string alg, const std::string algid);
} // End mspass namespace
#endif
