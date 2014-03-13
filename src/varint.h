
#ifndef VARINT_H
#define VARINT_H

#include <stdint.h>

uint8_t encode_unsigned_varint(uint8_t * const buffer, const uint64_t value);

uint64_t decode_unsigned_varint(const uint8_t * const data, uint8_t * const decoded_bytes);

#endif