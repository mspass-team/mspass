#ifndef _KEYWORDS_H_
#define _KEYWORDS_H_
/*! \brief Define metadata keys.

This include file defines a set ofconst std::string values that serve as
keys to Metadata get and put calls.  They are placed in this one file to
easy maintainability of the code base.   This is essential because in MsPaSS
we need to map Metadata keys to and from MongoDB database key names.  If
the schema needs to change due to some external constraint like assimilating
some external code base we need only change the names here.  The comments with
each name are roughly the same as the concept line in the yaml file for the
mspass schema at the time this file was created.   Do not depend on an
exact match, but the idea behind each phrase should not change (the concept).
*/
namespace mspass::seismic{
/*! Number of data samples.*/
const std::string SEISMICMD_npts("npts");
/*! data sample interval in seconds.*/
const std::string SEISMICMD_dt("delta");
/*! Time of first sample of data (epoch time or relative to some other time mark)*/
const std::string SEISMICMD_t0("starttime");
/*! Data sampling frequency in Hz=1/s */
const std::string SEISMICMD_sampling_rate("sampling_rate");
/* In MsPASS Seismogram data fetch receiver metadata from the site collection.
The schema definition prepends "site_" to base names.  Use this set for
gets and puts to Seismogram objects in C++ code. */
/*! latitude of a seismic station/instrument in degrees - Seismogram*/
const std::string SEISMICMD_rlat("site_lat");
/*! longitude of a seismic station/instrument in degrees - Seismogram*/
const std::string SEISMICMD_rlon("site_lon");
/*! elevation of a seismic station/instrument in km (subtract emplacement
depth for borehole instruments) - Seismogram*/
const std::string SEISMICMD_relev("site_elev");
/* In MsPASS TimeSeries data fetch receiver metadata from the site collection.
The schema definition prepends "channel_" to base names.  Use this set for
gets and puts to TimeSeries objects in C++ code. */
/*! latitude of a seismic station/instrument in degrees - TimeSeries*/
const std::string SEISMICMD_clat("channel_lat");
/*! longitude of a seismic station/instrument in degrees - TimeSeries*/
const std::string SEISMICMD_clon("channel_lon");
/*! elevation of a seismic station/instrument in km (subtract emplacement
depth for borehole instruments) - TimeSeries*/
const std::string SEISMICMD_celev("channel_elev");
/*!  Azimuth (in degree) of a seismometer component - horizontal angle*/
const std::string SEISMICMD_hang("channel_hang");
/*!  Inclination from +up (in degree) of a seismometer component - vertical angle*/
const std::string SEISMICMD_vang("channel_vang");

/* In MsPASS source information is retrieved from the source collection.
The schema maps these to names with a "source_" string prepended to the
attribute name.   The coordinate names are common to other collections.
Use this set for gets and puts to fetch source coordinate data.*/
/*! Latitude (in degrees) of the hypocenter of seismic source*/
const std::string SEISMICMD_slat("source_lat");
/*! Longitude (in degrees) of the hypocenter of seismic source*/
const std::string SEISMICMD_slon("source_lon");
/*! Depth (in km) of the hypocenter of seismic source*/
const std::string SEISMICMD_selev("source_elev");
/*! Origin time of the hypocenter of a seismic source.*/
const std::string SEISMICMD_stime("source_time");
/*! External data file name.*/
const std::string SEISMICMD_dfile("dfile");
/*! Directory path to an external file (always used with dfile)*/
const std::string SEISMICMD_dir("dir");
/*! Offset in bytes from beginning of a file to first data sample.*/
const std::string SEISMICMD_foff("foff");
/*! Holds transformation matrix in MsPASS use for Seismogram.  */
const std::string SEISMICMD_tmatrix("tmatrix");
/*! Universal Unique Identifier used for history.  */
const std::string SEISMICMD_uuid("uuid");
/*! Boolean used to tag a history chain as beginning with raw data*/
const std::string SEISMICMD_rawdata("rawdata");
/*! network code (net component of SEED net:sta:chan)*/
const std::string SEISMICMD_net("net");
/*! station code assigned to a spot on Earth (sta component of SEED net:sta:chan)*/
const std::string SEISMICMD_sta("sta");
/*! channel name (e.g. HHZ, BHE, etc.) - normally a SEED channel code*/
const std::string SEISMICMD_chan("chan");
/*! location code assigned to an instrument (loc component of SEED net:sta:chan)*/
const std::string SEISMICMD_loc("loc");
}
#endif
