#include "edgefs/mm.h"

#include <cassert>
#include <cstring>

namespace edgefs
{

uint64_t tablesize(uint64_t n) {
  if(n == 0) 
    return 1;

  n--;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  n |= n >> 32;
  return n + 1;
}

MManger::MManger(uint64_t max_mem_usage, uint64_t max_free_mem) :
  max_mem_usage_(max_mem_usage),
  max_free_mem_(max_free_mem),
  free_blocks_(),
  mtxs_(),
  memory_usage_(0),
  allocated_memory_usage_(0) {}

MManger::~MManger() {
  for(auto& it : free_blocks_) {
    auto& blocks = it.second;
    while(!blocks.empty()) {
      cacheblock* block = blocks.front();
      delete[] block->b_data;
      delete block;
      blocks.pop_front();
    }
  }
}

cacheblock* MManger::Allocate(uint64_t bytes) {
  bytes = tablesize(bytes);

  std::list<cacheblock*>& free_blocks = free_blocks_[bytes];
  std::unique_lock<std::mutex> lck(mtxs_[bytes]);
  
  cacheblock* new_block = nullptr;
  if(free_blocks.empty()) {
    if(MemoryUsage() + bytes > max_mem_usage_) {
      return nullptr;
    }
    char* new_buf = new char[bytes];
    if(new_buf == nullptr) {
      return nullptr;
    }
    new_block = new cacheblock{
      new_buf,
      bytes,
      time(nullptr),
      0,
      0
    };
    memory_usage_ += bytes;
  } else {
    new_block = free_blocks.front();
    free_blocks.pop_front();
    new_block->b_ctime = time(nullptr);
    new_block->b_atime = time(nullptr);
  }
  allocated_memory_usage_ += bytes;
  
  return new_block;
}

void MManger::Free(cacheblock* block) {
  uint32_t bytes = block->b_size;
  std::list<cacheblock*>& free_blocks = free_blocks_[bytes];
  std::unique_lock<std::mutex> lck(mtxs_[bytes]);

  if(free_blocks.size() >= (max_free_mem_ / free_blocks_.size() / bytes)) {
    // To many free blocks, free the block
    delete[] block->b_data;
    delete block;
    memory_usage_ -= bytes;
  } else {
    // clean the block and push it to free list
    memset(block->b_data, 0, block->b_size);
    block->b_ctime = 0;
    block->b_atime = 0;
    block->b_acounter = 0;
  }
  allocated_memory_usage_ -= bytes;
}

} // namespace edgefs
