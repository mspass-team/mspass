#ifndef _MEMORY_CONSTANTS_H_
#define _MEMORY_CONSTANTS_H_
/* This file contains constants used to provide approximate memory use
estimates for data objects.   For arrays and lists of simple types that
is not difficult to do, but for map, multimap, and set containers that
cannot be done without traversing the entire tree.   As assumption in
mspass is that such containers consume small amounts of memory relative to
the time series data and if we commit a small error in estimating memory
use in those containers it will not cause issues.  These constants may
require tuning with experiences.   Initial values set May 2023 by glp
are guesses.
*/
namespace mspass::utility
{
namespace memory_constants
{
/*! Average size of key string used for Metadata. */
const size_t KEY_AVERAGE_SIZE(8);
/*! Average Metadata entry size.  key-value so key size + wag of average size */
const size_t MD_AVERAGE_SIZE(KEY_AVERAGE_SIZE+16);
/*! Average size of history record.
History is stored as a multimap of fixed format records.   The variable is that
the strings in the NodeData class have variable length.  This is a WAG of
nominal size that is a bit conservative.  Made this more conservative as
history is optional and when enabled could cause a memory bloat in some
situations.  Estimate is per NodeData entry,.  NodeData has 6 attributes
so we round up to nearest power of 2 from 6*10*/
const size_t HISTORYDATA_AVERAGE_SIZE(64);
/*! Average error log entry size.

The dominant data for an error log entry is the posted error message.
This is a power of 2 wag*/
const size_t ELOG_AVERAGE_SIZE(128);
/*! Average size of a data gap set entry.  */
const size_t DATA_GAP_AVERAGE_SIZE(2*sizeof(double)+sizeof(size_t));
}
}
#endif
