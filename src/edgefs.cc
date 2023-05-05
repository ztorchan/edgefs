#include <cstdlib>
#include <cstdio>
#include <chrono>
#include <fstream>
#include <sstream>
#include <filesystem>

#include <glog/logging.h>
#include <brpc/server.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/document.h>

#include "edgefs/edgefs.h"
#include "edgefs/bitmap.h"
#include "edgefs/rpc/center_service.pb.h"

#define EDGEFS_PORT 2333

namespace edgefs
{

std::thread* EdgeFS::rpc_thread_ = nullptr;
std::thread* EdgeFS::gc_and_pull_thread_ = nullptr;
struct dentry* EdgeFS::root_dentry_ = nullptr;
std::list<request*> EdgeFS::req_list_;
std::mutex EdgeFS::req_mtx_;
std::condition_variable EdgeFS::req_cv_;
Option EdgeFS::options_;
MManger* EdgeFS::mm_ = nullptr;


void SplitPath(const char *path, std::vector<std::string>& d_names) {
  d_names.clear();
  uint32_t head = 0;
  if(path[head] == '/') {
    head++;
  }

  uint32_t len = 1;
  while(path[head] != 0) {
    while(path[head + len] != '/' && path[head + len] != 0) {
      len++;
    }
    d_names.emplace_back(path + head, len);
    
    head += len;
    if(path[head] == 0) {
      break;
    }
    while(path[head] == '/') {
      head++;
    }
  }
}

void EdgeFS::Init(std::string config_path) {
  options_ = Option(config_path);
  mm_ = new MManger(options_.max_free_cache);

  std::filesystem::remove_all(options_.data_root_path);
  std::filesystem::create_directories(options_.data_root_path);

}

int EdgeFS::getattr(const char *path, struct stat *st) {
  LOG(INFO) << "[getattr] " << path;
  
  // find target dentry
  struct dentry* target_dentry = root_dentry_;
  std::vector<std::string> d_names;
  SplitPath(path, d_names);
  for(const auto& dname : d_names) {
    auto it = target_dentry->d_childs.find(dname);
    if(it == target_dentry->d_childs.end()) {
      return 0;
    }
    target_dentry = it->second;
  }

  // stat
  struct inode* target_inode = target_dentry->d_inode;
  st->st_uid = target_inode->i_uid;
  st->st_gid = target_inode->i_gid;
  st->st_mode = target_inode->i_mode;
  st->st_nlink = target_inode->i_nlink;
  st->st_atime = target_inode->i_atime;
  st->st_mtime = target_inode->i_mtime;
}

int EdgeFS::readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info * fi) {
  LOG(INFO) << "[readdir] " << path;
  
  // find target dentry
  struct dentry* target_dentry = root_dentry_;
  std::vector<std::string> d_names;
  SplitPath(path, d_names);
  for(const auto& dname : d_names) {
    auto it = target_dentry->d_childs.find(dname);
    if(it == target_dentry->d_childs.end()) {
      return 0;
    }
    target_dentry = it->second;
  }
  
  for(const auto& child : target_dentry->d_childs) {
    struct stat st;
    struct inode* target_inode = target_dentry->d_inode;
    st.st_uid = target_inode->i_uid;
    st.st_gid = target_inode->i_gid;
    st.st_mode = target_inode->i_mode;
    st.st_nlink = target_inode->i_nlink;
    st.st_atime = target_inode->i_atime;
    st.st_mtime = target_inode->i_mtime;
    filler(buf, child.second->d_name.c_str(), &st, 0);
  }

  return 0;
}

int EdgeFS::read(const char *path, char *buf, std::size_t size, off_t offset, struct fuse_file_info * fi) {
  LOG(INFO) << "[read] " << path;
  
  // find target dentry
  struct dentry* target_dentry = root_dentry_;
  std::vector<std::string> d_names;
  SplitPath(path, d_names);
  for(const auto& dname : d_names) {
    auto it = target_dentry->d_childs.find(dname);
    if(it == target_dentry->d_childs.end()) {
      // can not find target file, commit a pull request
      std::unique_lock<std::mutex> lck(req_mtx_);
      uint64_t start_chunck = offset / options_.chunck_size;
      uint64_t end_chunck = (offset + size - 1) / options_.chunck_size;
      req_list_.push_back(new request{
        RequestType::PULL,
        time(NULL),
        {
          .r_pr_content = {
            path, 
            options_.chunck_size, 
            start_chunck, 
            end_chunck - start_chunck + 1
          }
        }
      });
      req_cv_.notify_all();
      return 0;
    }
    target_dentry = it->second;
  }

  // successfully find target dentry
  struct inode* target_inode = target_dentry->d_inode;
  if(target_inode->i_state == file_state::INVAILD) {
    
  }
  if(offset >= target_inode->i_len) {
    // offset is greater than file length
    return 0;
  }
  if(offset + size > target_inode->i_len) {
    // size is greater than rest length, change size
    size = target_inode->i_len - offset;
  }
  
  // convert to chunck no and offset
  uint64_t start_chunck_no = offset / target_inode->i_chunck_size;
  uint64_t start_chunck_offset = offset % target_inode->i_chunck_size;              // the first byte
  uint64_t end_chunck_no = (offset + size - 1) / target_inode->i_chunck_size;
  uint64_t end_chunck_offset = (offset + size - 1) % target_inode->i_chunck_size;   // the last byte
  
  //check if all chunck exist
  std::vector<std::pair<uint64_t, uint64_t>> lack_extents;
  if(!check_chuncks_exist(target_inode, start_chunck_no, end_chunck_no, lack_extents)) {
    std::unique_lock<std::mutex> lck(req_mtx_);
    for(const auto& extent : lack_extents) {
      req_list_.push_back(new request{
        RequestType::PULL,
        time(NULL),
        {
          .r_pr_content = {
            path, 
            options_.chunck_size, 
            extent.first, 
            extent.second
          }
        }
      });
    }
    req_cv_.notify_all();
    return 0;
  }

  // all chunck exist, read chunck
  char* cur_buf_ptr = buf;
  uint64_t cur_offset;
  uint64_t cur_size;
  uint64_t total_read_bytes = 0;
  for(uint64_t chunck_no = start_chunck_no; chunck_no <= end_chunck_no; chunck_no++) {
    if(chunck_no == start_chunck_no) {
      cur_offset = start_chunck_offset;
    } else {
      cur_offset = 0;
    }
    if(chunck_no == end_chunck_no) {
      cur_size = end_chunck_offset - cur_offset + 1;
    } else{
      cur_size = target_inode->i_chunck_size - cur_offset;
    }
    
    int read_bytes = read_from_chunck(path, target_inode->i_subinodes[chunck_no], cur_buf_ptr, cur_size, cur_offset);
    if(read_bytes != cur_size) {
      return 0;
    } else {
      cur_buf_ptr += read_bytes;
      total_read_bytes += read_bytes;
    }
  }

  assert(total_read_bytes == size);
  return total_read_bytes;
}

bool EdgeFS::check_chuncks_exist(struct inode* in, uint64_t start_chunck_no, uint64_t end_chunck_no, _OUT std::vector<std::pair<uint64_t, uint64_t>> lack_extents) {
  bool all_exist = true;
  uint64_t now_extent_start_chunck = -1;
  for(uint64_t chunck_no = start_chunck_no; chunck_no <= end_chunck_no; chunck_no++) {
    if(!in->i_chunck_bitmap->Get(chunck_no)) {
      // chunck does not exist
      all_exist = false;
      if(now_extent_start_chunck == -1) {
        now_extent_start_chunck = chunck_no;
      }
    } else {
      // exist
      if(now_extent_start_chunck != -1) {
        lack_extents.push_back({now_extent_start_chunck, chunck_no - now_extent_start_chunck + 1});
        now_extent_start_chunck = -1;
      }
    }
  }

  if(now_extent_start_chunck != -1) {
    lack_extents.push_back({now_extent_start_chunck, end_chunck_no - now_extent_start_chunck + 1});
  }
  return all_exist;
}

int EdgeFS::read_from_chunck(const char *path, struct subinode* subi, char *buf, std::size_t size, off_t offset) {
  // convert to block no and offset
  uint64_t start_block_no = offset / subi->subi_inode->i_block_size;
  uint64_t start_block_offset = offset % subi->subi_inode->i_block_size;              // the first byte
  uint64_t end_block_no = (offset + size - 1) / subi->subi_inode->i_block_size;
  uint64_t end_block_offset = (offset + size - 1) % subi->subi_inode->i_block_size;   // the last byte

  FILE* f_chunck = NULL;
  uint64_t total_read_bytes;
  char* cur_buf_ptr = buf;
  uint64_t cur_offset;
  uint64_t cur_size;
  for(uint64_t block_no = start_block_no; block_no <= end_block_no; block_no++) {
    // current block offset and size
    if(block_no == start_block_no) {
      cur_offset = start_block_offset;
    } else {
      cur_offset = 0;
    }
    if(block_no == end_block_no) {
      cur_size = end_block_offset - cur_offset + 1;
    } else{
      cur_size = subi->subi_inode->i_block_size - cur_offset;
    }
    
    // check cache
    if(subi->subi_block_bitmap->Get(block_no)) {
      memcpy(cur_buf_ptr, (subi->subi_blocks[block_no]->b_data + cur_offset), cur_size);
      subi->subi_blocks[block_no]->b_atime = time(NULL);
      subi->subi_blocks[block_no]->b_acounter++;
    } else {
      // firstly open chunck file
      if(f_chunck == NULL) {
        std::string chunck_data_path = options_.data_root_path + std::string(path) + "/" + std::to_string(subi->subi_no);
        f_chunck = fopen(chunck_data_path.c_str(), "r");
        if(f_chunck == NULL) {
          return 0;
        }
      }
      
      // cache the block 
      cacheblock* new_block = mm_->Allocate(subi->subi_inode->i_block_size);
      if(new_block != nullptr) {
        // successfully allocate a block
        fseek(f_chunck, block_no * subi->subi_inode->i_block_size, SEEK_SET);
        if(fread(new_block->b_data, 1, subi->subi_inode->i_block_size, f_chunck) != subi->subi_inode->i_block_size) {
          mm_->Free(new_block);
          return 0;
        }
        new_block->b_len = subi->subi_inode->i_block_size;
        new_block->b_atime = time(NULL);
        new_block->b_acounter++;
        subi->subi_blocks[block_no] = new_block;
        subi->subi_block_bitmap->Set(block_no);
        memcpy(cur_buf_ptr, new_block->b_data + cur_offset, cur_size);
      } else {
        // fail to allocate a block, seek and read from disk
        fseek(f_chunck, block_no * subi->subi_inode->i_block_size + cur_offset, SEEK_SET);
        if(fread(cur_buf_ptr, 1, cur_size, f_chunck) != cur_size){
          return 0;
        }
      }
    }
    cur_buf_ptr += cur_size;
    total_read_bytes += cur_size;
  }

  assert(total_read_bytes == size);
  if(f_chunck != NULL) {
    fclose(f_chunck);
  }
  return total_read_bytes;
}

void EdgeFS::RPC() {
  brpc::Server edge_server;
  EdgeServiceImpl edge_service(options_.root_path, options_.data_root_path);
  if(edge_server.AddService(&edge_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Fail to add service";
    exit(-1);
  }
  
  butil::EndPoint point = butil::EndPoint(butil::IP_ANY, options_.rpc_port);
  brpc::ServerOptions options;
  if(edge_server.Start(point, &options) != 0) {
    LOG(ERROR) << "Fail to start EdgeRPC";
    exit(-1);
  } else {
    LOG(INFO) << "EdgeRPC Start.";
  }
  edge_server.RunUntilAskedToQuit();
  LOG(INFO) << "EdgeRPC Stop";
}

void EdgeFS::GC_AND_PULL() {
  while(true) {
    std::unique_lock<std::mutex> lck(EdgeFS::req_mtx_);
    EdgeFS::req_cv_.wait(lck, [&] {
      return !EdgeFS::req_list_.empty();
    });

    
  }
}

void EdgeFS::SCAN() {
  while(true) {

  }
}



} // namespace edgefs
