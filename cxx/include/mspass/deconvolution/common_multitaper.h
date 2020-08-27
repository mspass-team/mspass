/* This file contains functions common to both multitaper classes. */
#ifndef _COMMON_MULTITAPER_H_
#define _COMMON_MULTITAPER_H_
#include "mspass/utility/Metadata.h"
namespace mspass{
int ComputeTaperLength(const mspass::Metadata& md);
}
#endif
