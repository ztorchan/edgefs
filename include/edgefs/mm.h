#ifndef _EDGEFS_MM_H
#define _EDGEFS_MM_H

#include <cstdint>
#include <ctime>
#include <list>
#include <map>
#include <vector>
#include <atomic>
#include <mutex>

#define MAX_BLOCK_SIZE 16777216   // 16 * 1024 * 1024
#define MAX_FREE_MEM 134217728        // 128 * 1024 * 1024
#define MAX_MEM_USAGE 1073741824  // 1024 * 1024 * 1024

namespace edgefs
{

struct cacheblock {
  char*     b_data;
  uint32_t  b_len;              // data length      
  const uint32_t  b_size;       // block size                
  time_t    b_ctime;            // create time
  time_t    b_atime;            // last access time
  uint32_t  b_acounter;         // total access times
};

class MManger {
public:
  MManger(uint32_t max_free_mem = MAX_FREE_MEM);
  ~MManger();

  MManger(const MManger&) = delete;

  struct cacheblock* Allocate(uint32_t bytes);
  void Free(struct cacheblock* block);

  void SetMaxFreeMem(uint32_t max_free_mem) { max_free_mem_ = max_free_mem; }

  uint32_t MemoryUsage() const { return memory_usage_.load(std::memory_order_relaxed); }

private:
  uint32_t max_free_mem_;
  std::map<uint32_t, std::list<cacheblock*>> allocated_blocks_;
  std::map<uint32_t, std::list<cacheblock*>> free_blocks_;
  std::map<uint32_t, std::mutex> mtxs_;
  std::atomic<uint64_t> memory_usage_;
  std::atomic<uint64_t> allocated_memory_usage_;
};

} // namespace edgefs


#endif 