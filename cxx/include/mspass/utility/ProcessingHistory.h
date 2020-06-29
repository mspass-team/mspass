#ifndef _PROCESSING_HISTORY_H_
#define _PROCESSING_HISTORY_H_
#include <string>
#include <list>
#include <vector>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include "mspass/utility/ErrorLogger.h"
//#include "mspass/seismic/Ensemble.h"
namespace mspass{
/* This enum class is used to define status of processing of a datum.
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
/*! \brief Contains minimal history data to define a processing stage.

This class is the lowest level data structure used to define a processing
step.   A "step" can be a reader, a writer, or some algorithm that alters or
creates new data.   Readers and writers begin and terminate a processing chain.
algorithms are assumed to input one or many objects of a common type that
contain an instance of ProcessingHistory (defined below).   Algorithms
can emit multiple data objects, but they are expected to tag each one with
an instance of this class/struct.

This class is really a struct.   Python wrappers may want to define
getters and putters to simplify python code.  In particular, the enum
class produces awkward constucts in python so the wrappers will want to
define putters for status that create less obscure code.   They
probably should be the same method names used in ProcessingHistory.
*/
//template <typename Tdata> class ProcessingHistoryRecord
class ProcessingHistoryRecord
{
public:

  /*! \brief Define the status of the current object.

  Data can be defined in a variety of states that impact how we save
  th processing chain.   We use an enum class, ProcessingStatus, to
  define states the history mechanism should handle.   See the
  ProcessingHistory class for more details on how this is used.
  */
  ProcessingStatus status;
  /*! \brief Name of this algorithm.

  We use the concept that every processing algorithm has a name keyword
  that togther with an id and/or instance defines a unique definition of
  the algorithm and a set of input parameters that define the algorithm's
  behavior.
  */
  std::string algorithm;
  /* \brief Defines a particular instance of an algorithm.

  We define a particular instance of an algorithm by either a name
  key in combination with an instance defined by this attribute.
  The instance attribute is a string for flexibility.   When run
  under control of mspass the instance string would normally be the
  ObjectID string representation of the document containing the input
  parameters for this instance of algorithm.  For research prototypes or
  quick and dirty tests it can be just a counter defined manually in
  some input file (e.g.   filter 1, filter 2, filter 3 would define three
  instance of the algorithm with the tag "filer".  */
  std::string instance;
  /*! \brief Unique id that defines the current object.

  Data objects in MsPASS use this class to define the state of each
  object through the chain of processes and data used to produce it.
  To do that right each data object needs a unique id.  This defines the
  id of the one object this particular record is associated with on
  creation.  That is, whenever a new object is created in MsPASS using
  the history feature one of these records will be created for each data
  object that is defined as atomic.   This string defines unique key for
  the object it could be connected to with the this pointer.  The parents of
  the current object are defined by the inputs data structure below.

  In the current implementation id is string representation of a uuid
  maintained by each atomic object.  We use a string because a major
  complication to keep this data structure from consuming large amounts
  of memory is that id can be one of two things:  readers can define this
  id as one of two things:  (1) for data marked RAW id is the ObjectID of the
  parent data in the wf collection, and (2) when set as READER_ORIGIN id
  is the string represntation of the ObjectID of the history record
  created when the parent waveform was saved.  That allows the partially
  processed waveform to be deleted from the database while the history is
  retained.   For the same reason the Metadata components of the raw waveforms
  that define woth the RAW objectid is must also not be deleted or the
  top level origin of any output will be lost.
  */
  std::string id;

  //size_t set_inputs(const mspass::Ensemble<Tdata>& d);
  //size_t set_inputs(const std::vector<Tdata>& d);
  //size_t set_inputs(const Tdata& d);
  /*! \brief Defines all parents of this object.

  We save processing history by a chain (linked list) of sets of these
  objects.  (We don't actually use a set container because we view
  vectors as a more familiar concept to seismologists who might want to
  understand this code.)   The vector contains the records extracted from
  each parent used to construct the object that is at the end of the chain.
  For some algorithms there may be only one input (e.g. time invariant filters)
  while other many have many (e.g. a stack).
  */
  std::vector<std::list<mspass::ProcessingHistoryRecord>> inputs;
  /*! Default constructor.  Creates object with no data. */
  ProcessingHistoryRecord();
  /*! Standard copy constructor.  This copies the uuid set in id. */
  ProcessingHistoryRecord(const ProcessingHistoryRecord& parent);
  ProcessingHistoryRecord& operator=(const ProcessingHistoryRecord& parent);
private:
  friend boost::serialization::access;
    template<class Archive>
       void serialize(Archive& ar,const unsigned int version)
    {
      ar & status;
      ar & algorithm;
      ar & instance;
      ar & id;
      ar & inputs;
    }
};
/*! Procedure to set inputs vector defined by an Ensemble object.

Ensembles are the generalization in MsPaSS of a "gather" as used in
seismic reflection processing.  An Ensemble is just a vector of data objects.
This procedure assumes the member of the ensemble have ProcessingHistory
as a base class so they can access the history mechanism datta in MsPaSS.
The algorithm simply copies all history data from each live member to
the inputs vector of ProcessingHistoryRecord.

Do not use this procedure if the algorithm receiving the Ensemble can
ignore members of the Ensemble and mark them dead.  e.g. a robust
stacker might use a trimmed mean that discards some data without an
explicit kill.   Use the procedure that works on the atomic object, Tdata,
instead in such a situation.

\param rec is the output ProcessingHistoryRecord where history will be
  copied to inputs.  (note the operation is append.  If inputs has
  previous content the new data will be appended)
\param d is Ensemble which is to be defined as input.
*/
/*
template <typename Tdata>
  size_t set_inputs(ProcessingHistoryRecord& rec, const mspass::Ensemble<Tdata>& d)
{
  try{
    vector<Tdata>::const_iterator dptr;
    for(dptr=d.member.begin();dptr!=d.member.end();++dptr)
    {
      // Ignore any data not defined as live
      if(dptr->live())
      {
        list<ProcessingHistoryRecord> h dptr->ProcessingHistory::history();
        // do nothing if the container is empty
        if(h.size()>0)rec.inputs.push_back(h);
      }
    }
    return rec.inputs.size();
  }catch(...){throw;};
};
*/
/*! Append history data from a data object to inputs vector.

ProcessingHistoryRecord use a vector container of a linked list of
other ProcessingHistoryRecord objects to define the inputs to an algorithm.
This procedure appends new data to that vector container defined by d.

\param rec is the output record (normally under construction)
\param d is the data to define as an input.
*/

template <typename Tdata>
    size_t append_input(ProcessingHistoryRecord& rec,const Tdata& d)
{
  if(d.live())
  {
    list<ProcessingHistoryRecord> h;
    h=d.history();
    rec.inputs.push_back(h);
  }
  return rec.inputs.size();
};
/*! Base class defining core concepts.  */
class  BasicProcessingHistory
{
public:
  BasicProcessingHistory()
  {
    jid=std::string();
    jnm=std::string();
  };
  BasicProcessingHistory(const BasicProcessingHistory& parent)
  {
    jid=parent.jid;
    jnm=parent.jnm;
  };
  /*! Return a list of algorithm name tags that have been applied.

  All concrete implementations of this base will need to supply a list of
  name tags that define the algorithms applied to the data to produce the
  current state.  Implementations will need to store more than these names.
  The linked list returned must define the order the algorithms are applied.
  The same algorithm may appear sequentially with or without a change in
  parameters that define it's properties.   Classes that implement this
  virtual will want to deal with such ambiguities.
  */
  //virtual std::list<std::string> algorithms_applied() const =0;
  /*! Return number or processing algorithms applied to produce these data.

  Implementations of this method need to return a count of the number of
  processing steps that have been applied to put the data in the current state.
  Note that computing this number is complicated by the fact that most data
  processing is not completed in a single run, but with multiple "jobs".
  */
  virtual size_t current_stage()=0;
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
applied to produce any final output.   Names imply the following meanings:
 raw - means the data is new input to mspass (raw data from data center,
   field experiment, or simulation).  That tag means now prior history
   can be reconstructed.
 origin - top-level ancestory of current data.  The top of a processing
   chain is always tagged as an origin.  A top level can also be "raw" but not necessarily.
   In particular, readers that load partially processed data should mark
   the data read as an origin, but not raw.
  stage - all processed data objects that are volatile elements within a
    workflow are defined as a stage.   They are presumed to leave their
    existence known only through ancestory preserved in the processing
    chain.  A stage becomes a potential root only when it is saved by
    a writer where the writer will mark that position as a save.  Considered
    calling this a branch, but that doesn't capture the concept right since
    we require this mechanism to have not directly define splits into multiple
    outputs.  Those are possible, but not captured by this algorithm.
    Similarly parent/child might work, but that also has doesn't capture the
    one to one constraint as parents can have multiple children.

  The concepts of raw, origin, and stage are implemented with the
  enum class defined above called ProcessingStatus.  Each history record
  has that as an attribute, but each call to new_stage updates a copy
  kept inside this object to simplify the python wrappers.
*/

class ProcessingHistory : public BasicProcessingHistory
{
public:
  /*! Error log.

  ProcessingHistory "has an" ErrorLogger.   It was a design choice to put
  elog as an attribute of ProcessingHistory rather than as a direct attribute
  of data objects because we assume top level data objects in MsPASS have
  MsPASS features.  This is a common MsPASS feature for handling errors so
  it was convenient to put it here.  It makes sense that errors are part of
  the processing history.
  */
  mspass::ErrorLogger elog;
  /*! Default constructor. */
  ProcessingHistory();
  /*! Standard copy constructor. */
  ProcessingHistory(const ProcessingHistory& parent);
  /*! Return true if the current data is in state defined as "raw" - see class description*/
  bool is_raw();
  /*! Return true if the current data is in state defined as "origin" - see class description*/
  bool is_origin();
  /*! Return true if the current data is in state defined as "volatile" - see class description*/
  bool is_volatile();
  /*! Return true if the current data is in state defined as "saved" - see class description*/
  bool is_saved();
  /*! \brief Return number of processing stages that have been applied to this object.

  One might want to know how many processing steps have been previously applied
  to produce the current data.  For linear algorithms that would be useful
  only in debugging, but for an iterative algorithm it can be essential to
  avoid infinite loops with a loop limit parameter.  This method returns
  how many times something has been done to alter the associated data.
  It returns 0 if the data are raw.
  */
  size_t current_stage() override;
  /*! Set as a top level history origin.

  This method should be called when a new object is created to initialize the
  history as an origin.  Note again an origin may be raw but not all origins
  are define as raw.   The caller should distinguish this subtlety by
  setting the status attribute correctly.

  \param rec defines the input for the first history record.

  \exception Will throw an exception if the rec data does not have the
    status value set as raw or origin.  It will also throw an exception if
    the history chain is not empty when this method is called.  Intermediate
    saves should call clear before calling this method.  An empty condition
    should be a given for readers.
  */
  void set_as_origin(const ProcessingHistoryRecord& rec);
  /*! \brief Define current object as a derived from a processing algorithm.

  All algorithms set up to preserve processing history should call this method
  when a new copy is created before applying that particular algorithm.
  It appends the passed record to the processing history data structure stored
  within this object.

  \param rec defines the defining data for this processing stage.

  \return count of processing level (root is 0 and increments for each
    processing stage (new child)

    */
  size_t new_stage(const ProcessingHistoryRecord& rec);
  /*! \brief Add a record showing this data has been saved to storage.

  Writer should first save data for an object to some external storage
  before calling this method.   The rec passed must have the status
  defined as saved and the id set as an object id or this function will
  throw an exception.  Note a nonfatal error will be posted to the
  elog data (type ErrorLogger that is a member of this class) if the
  id field of rec does not appear to be a MongoDB object id string
  representation. The test used is not robust as it just tests for a
  length matching mongo's implementationd detail of exactly 12.

  \param rec is the data defining the save (objectid is critical)
  \return number of stages in the processing log after rec was posted.
  \exception Will throw a MsPASSError if the status is not marked save  and
    not save rec.  It will also throw a MsPASSError set as a warning if
    the id does not appear to be a MongoDB object id string.   The data in
    rec will still be saved when the warning error is thrown, however, so
    a handler should deal with this appropriately.
  */
  size_t set_as_saved(const ProcessingHistoryRecord& rec);
  list<ProcessingHistoryRecord> history() const
  {
    return this->history_list;
  };
  /*! Clear the history data and initialize top level to rec data.

  In designing this class we recognized early on that the structure was
  subject to serious memory bloat problems when averaging large data sets.
  (see github issue 43 section for the design history).   To reduce memory
  the idea is to save older history in MongoDB and then reset the history
  chain of each object to reduce memory use.  To do that first call the
  history method, save the data returned to MongoDD retaining the objectid of
  the stored document, building a ProcessingHistoryRecord with that id,
  and then calling this method to clear the history chain and set the origin
  as the record just saved.

  WARNING:  normal users should call this method only if the clearly
  understand it's implication.  Without a db store previous history will
  be lost.

  \param rec is the new top level history record after clearing contents.
  */
  void reset(const ProcessingHistoryRecord& rec);
  /*! Just clear the history record.

  This method is dangerous, but is a sharp knife that might be necessary
  sometimes.  It simply clears the history chain.   A call to history,
  clear, and set_as_origin are equivalent to the reset method defined above.
  */
  void clear()
  {
    history_list.clear();
  }
  /*! Assignment operator.  */
  ProcessingHistory& operator=(const ProcessingHistory& parent);
/* We make this protected to simplify expected extensions.  In particular,
the process of reconstructing history is a complicated process we don't
want to add as baggage to regular data.  Hence, tools to reconstruct history
(provenance) are expected to extend this class. */
protected:
  list<ProcessingHistoryRecord> history_list;
private:
  /* this is set undefined when empty and is copied from the latest record
  when new_stage is called */
  ProcessingStatus status;
  friend boost::serialization::access;
    template<class Archive>
       void serialize(Archive& ar,const unsigned int version)
    {
      ar & boost::serialization::base_object<BasicProcessingHistory>(*this);
      ar & status;
      ar & history_list;
    };
};

} // End mspass namespace
#endif
