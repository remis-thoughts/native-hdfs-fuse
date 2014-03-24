
#ifndef MINMAX_H
#define MINMAX_H

#include <stdint.h>

#undef min
#undef max

static inline
uint64_t min(uint64_t a, uint64_t b)
{
  return a < b ? a : b;
}

static inline
uint64_t max(uint64_t a, uint64_t b)
{
  return a > b ? a : b;
}

#endif