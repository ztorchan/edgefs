#ifndef _EDGEFS_MM_H
#define _EDGEFS_MM_H

#include <cstdint>
#include <ctime>
#include <list>
#include <map>
#include <vector>
#include <atomic>
#include <mutex>

namespace edgefs
{

struct cacheblock {
  char*     b_data; 
  const uint64_t  b_size;       // block size                
  time_t    b_ctime;            // create time
  time_t    b_atime;            // last access time
  uint64_t  b_acounter;         // total access times
};

class MManger {
public:
  MManger(uint64_t max_mem_usage, uint64_t max_free_mem);
  ~MManger();

  MManger(const MManger&) = delete;

  cacheblock* Allocate(uint64_t bytes);
  void Free(cacheblock* block);

  uint64_t MemoryUsage() const { return memory_usage_.load(std::memory_order_relaxed); }

private:
  const uint64_t max_mem_usage_;
  const uint64_t max_free_mem_;
  std::map<uint64_t, std::list<cacheblock*>> free_blocks_;
  std::map<uint64_t, std::mutex> mtxs_;
  std::atomic<uint64_t> memory_usage_;
  std::atomic<uint64_t> allocated_memory_usage_;
};

} // namespace edgefs


#endif 