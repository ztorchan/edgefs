#ifndef _EDGEFS_BITMAP_H
#define _EDGEFS_BITMAP_H

#include <cstdint>

namespace edgefs
{

class BitMap {
public:
  BitMap(uint64_t size);
  ~BitMap() {
    if(bits_ != nullptr) {
      delete bits_;
      bits_ = nullptr;
    }
  }

  BitMap(const BitMap&) = delete;
  BitMap& operator=(const BitMap&) = delete;

  void Set(uint64_t loc);
  void Rel(uint64_t loc);
  bool Get(uint64_t loc) const;
  uint64_t size() { return size_; }

private:
  uint64_t size_;
  uint8_t* bits_;
};

} // namespace edgefs


#endif