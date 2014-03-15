#include "varint.h"

// https://stackoverflow.com/questions/19758270/read-varint-from-linux-sockets
uint8_t encode_unsigned_varint(uint8_t * const buffer, uint64_t value)
{
  uint8_t encoded = 0;
  do
  {
    uint8_t next_byte = value & 0x7F;
    value >>= 7;
    if (value)
      next_byte |= 0x80;
    buffer[encoded++] = next_byte;
  } while (value);
  return encoded;
}

uint64_t decode_unsigned_varint(const uint8_t * const data, uint8_t * const decoded_bytes)
{
  int i = 0;
  uint64_t decoded_value = 0;
  int shift_amount = 0;

  do
  {
    decoded_value |= (uint64_t)(data[i] & 0x7F) << shift_amount;
    shift_amount += 7;
  } while ( (data[i++] & 0x80) != 0 );

  *decoded_bytes = i;
  return decoded_value;
}