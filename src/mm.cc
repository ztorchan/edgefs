#include "edgefs/mm.h"

#include <cassert>
#include <cstring>

namespace edgefs
{

uint32_t tablesize(uint32_t n) {
  if(n == 0) 
    return 1;

  n--;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  return n + 1;
}

struct cacheblock* MManger::Allocate(uint32_t bytes) {
  assert(bytes > 0);
  assert(bytes < MAX_BLOCK_SIZE);
  bytes = tablesize(bytes);
  
  if(MemoryUsage() + bytes > MAX_MEM_USAGE) {
    return nullptr;
  }

  std::list<cacheblock*>& free_blocks = free_blocks_[bytes];
  std::list<cacheblock*>& allocated_blocks = allocated_blocks_[bytes];
  std::unique_lock<std::mutex> lck(mtxs_[bytes]);
  
  struct cacheblock* new_block = nullptr;
  if(free_blocks.empty()) {
    new_block = new cacheblock{
      new char[bytes],
      0,
      bytes,
      time(nullptr),
      time(nullptr),
      0
    };
    memory_usage_ += bytes;
  } else {
    new_block = free_blocks.front();
    free_blocks.pop_front();
    new_block->b_ctime = time(nullptr);
    new_block->b_atime = time(nullptr);
  }
  allocated_blocks.push_back(new_block);
  allocated_memory_usage_ += bytes;
  
  return new_block;
}

void MManger::Free(struct cacheblock* block) {
  uint32_t bytes = block->b_size;
  std::list<cacheblock*>& free_blocks = free_blocks_[bytes];
  std::list<cacheblock*>& allocated_blocks = allocated_blocks_[bytes];
  std::unique_lock<std::mutex> lck(mtxs_[bytes]);

  if(free_blocks.size() >= (MAX_FREE / allocated_blocks_.size() / bytes)) {
    // To many free blocks, free the block
    delete[] block->b_data;
    delete block;
    memory_usage_ -= bytes;
  } else {
    // clean the block and push it to free list
    memset(block->b_data, 0, block->b_size);
    block->b_len = 0;
    block->b_ctime = 0;
    block->b_atime = 0;
    block->b_acounter = 0;
  }
}

} // namespace edgefs
