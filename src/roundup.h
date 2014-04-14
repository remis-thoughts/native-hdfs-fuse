
#ifndef ROUNDUP_H
#define ROUNDUP_H

#include <stdint.h>

static inline
uint32_t roundup(uint32_t num, uint32_t multiple)
{
  return (num + multiple - 1) / multiple;
}

#endif