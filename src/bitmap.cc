#include "edgefs/bitmap.h"

namespace edgefs
{

BitMap::BitMap(uint64_t size) : size_(size) {
  uint64_t u8_size = size_ / 8;
  if(size_ % 8 != 0) {
    u8_size++;
  }
  bits_ = new uint8_t[u8_size];
}

void BitMap::Set(uint64_t loc) {
  if(loc > size_) {
    return;
  }
  uint64_t addr = loc / 8;
  uint64_t offset = loc % 8;
  uint8_t tmp = 0b1 << offset;
  bits_[addr] |= tmp;
}

void BitMap::Rel(uint64_t loc) {
  if(loc > size_) {
    return;
  }
  uint64_t addr = loc / 8;
  uint64_t offset = loc % 8;
  uint8_t tmp = 0b11111111 ^ (0b1 << offset);
  bits_[addr] &= tmp;
}

bool BitMap::Get(uint64_t loc) const {
  if(loc > size_) {
    return false;
  }
  uint64_t addr = loc / 8;
  uint64_t offset = loc % 8;
  uint8_t tmp = 0b1 << offset;
  return (bits_[addr] & tmp) == 0 ? false : true;
}

} // namespace edgefs
