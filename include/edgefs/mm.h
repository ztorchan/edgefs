#ifndef _EDGEFS_MM_H
#define _DEGEFS_MM_H

#include <stdint.h>
#include <vector>
#include <atomic>

#define BLOCKSIZE 4096

namespace edgefs
{

class MManger {
public:
  MManger();
  ~MManger();

  MManger(const MManger&) = delete;
  MManger& operator=(const MManger&) = delete;

  char* Allocate(uint32_t bytes);

  uint32_t MemoryUsage() const {
    return memory_usage_.load(std::memory_order_relaxed);
  }

private:
  char* AllocateFallback(uint32_t bytes);
  char* AllocateNewBlock(uint32_t block_bytes);

  char* alloc_ptr_;
  uint32_t alloc_bytes_remaining_;

  std::vector<char*> blocks_;
  std::atomic<uint32_t> memory_usage_;
};

} // namespace edgefs


#endif 