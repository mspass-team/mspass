/* This file contains functions common to both multitaper classes. */
#ifndef _COMMON_MULTITAPER_H_
#define _COMMON_MULTITAPER_H_
#include "mspass/utility/Metadata.h"
namespace mspass::algorithms::deconvolution{
int ComputeTaperLength(const mspass::utility::Metadata& md);
}
#endif
