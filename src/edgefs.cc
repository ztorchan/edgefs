#include <cstdlib>
#include <cstdio>
#include <chrono>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <filesystem>

#include <butil/logging.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/document.h>

#include "edgefs/edgefs.h"
#include "edgefs/bitmap.h"
#include "edgefs/rpc/center_service.pb.h"

namespace edgefs
{

std::thread* EdgeFS::rpc_thread_ = nullptr;
std::thread* EdgeFS::gc_and_pull_thread_ = nullptr;
std::thread* EdgeFS::scan_thread_ = nullptr;
dentry* EdgeFS::root_dentry_ = nullptr;
std::list<request*> EdgeFS::req_list_;
std::mutex EdgeFS::req_mtx_;
std::condition_variable EdgeFS::req_cv_;
Option EdgeFS::options_;
MManger* EdgeFS::mm_ = nullptr;

void EdgeFS::Init(std::string config_path) {
  LOG(INFO) << "EdgeFS Initial...";
  LOG(INFO) << "Load configuration from: " << config_path;
  options_ = Option(config_path);
  LOG(INFO) << "Successfully load configuration";
  LOG(INFO) << "Create memory manager";
  mm_ = new MManger(options_.max_cache_size, options_.max_free_cache);

  LOG(INFO) << "Initial data path: " << options_.data_root_path;
  std::filesystem::remove_all(options_.data_root_path);
  std::filesystem::create_directories(options_.data_root_path);
  LOG(INFO) << "EdgeFS initial completed";

  rpc_thread_ = new std::thread(RPC);
  gc_and_pull_thread_ = new std::thread(GC_AND_PULL);
  scan_thread_ = new std::thread(SCAN);
}

int EdgeFS::getattr(const char *path, struct stat *st) {
  LOG(INFO) << "[getattr] " << path;
  
  // find target dentry
  std::vector<std::unique_ptr<std::shared_lock<std::shared_mutex>>> path_lcks;
  dentry* target_dentry = root_dentry_;
  std::vector<std::string> d_names;
  split_path(path, d_names);
  for(const auto& dname : d_names) {
    path_lcks.push_back(std::make_unique<std::shared_lock<std::shared_mutex>>(target_dentry->d_mtx));
    auto it = target_dentry->d_childs.find(dname);
    if(it == target_dentry->d_childs.end()) {
      LOG(INFO) << "Can not find the dentry";
      return -1;
    }
    target_dentry = it->second;
  }

  // stat
  std::shared_lock<std::shared_mutex> target_lck;
  inode* target_inode = target_dentry->d_inode;
  if(target_inode == nullptr) {
    st->st_mode = 0444 | __S_IFDIR;
  } else {
    st->st_mode = target_inode->i_mode;
    st->st_atime = target_inode->i_atime;
    st->st_mtime = target_inode->i_mtime;
  }
  return 0;
}

int EdgeFS::readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info * fi) {
  LOG(INFO) << "[readdir] " << path;
  
  // find target dentry
  std::vector<std::unique_ptr<std::shared_lock<std::shared_mutex>>> path_lcks;
  dentry* target_dentry = root_dentry_;
  std::vector<std::string> d_names;
  split_path(path, d_names);
  for(const auto& dname : d_names) {
    path_lcks.push_back(std::make_unique<std::shared_lock<std::shared_mutex>>(target_dentry->d_mtx));
    auto it = target_dentry->d_childs.find(dname);
    if(it == target_dentry->d_childs.end()) {
      LOG(INFO) << "Can not find the dentry";
      return -1;
    }
    target_dentry = it->second;
  }
  
  std::shared_lock<std::shared_mutex> target_lck;
  for(const auto& child : target_dentry->d_childs) {
    std::shared_lock<std::shared_mutex> child_lck(child.second->d_mtx);
    struct stat st;
    inode* child_inode = child.second->d_inode;
    if(child_inode == nullptr) {
      st.st_mode = 0444 | __S_IFDIR;
    } else {
      st.st_mode = child_inode->i_mode;
      st.st_atime = child_inode->i_atime;
      st.st_mtime = child_inode->i_mtime;
    }
    filler(buf, child.second->d_name.c_str(), &st, 0);
  }

  return 0;
}

int EdgeFS::read(const char *path, char *buf, std::size_t size, off_t offset, struct fuse_file_info * fi) {
  LOG(INFO) << "[read] " << path;
  
  // find target dentry
  std::vector<std::unique_ptr<std::shared_lock<std::shared_mutex>>> path_lcks;
  dentry* target_dentry = root_dentry_;
  std::vector<std::string> d_names;
  split_path(path, d_names);
  for(const auto& dname : d_names) {
    path_lcks.push_back(std::make_unique<std::shared_lock<std::shared_mutex>>(target_dentry->d_mtx));
    auto it = target_dentry->d_childs.find(dname);
    if(it == target_dentry->d_childs.end()) {
      // can not find target file, publish a pull request
      LOG(INFO) << "Can not find the dentry, publish a pull request";
      std::unique_lock<std::mutex> lck(req_mtx_);
      uint64_t start_chunck = offset / options_.chunck_size;
      uint64_t end_chunck = (offset + size - 1) / options_.chunck_size;
      pull_request* new_pr = new pull_request;
      {
        new_pr->r_type = RequestType::PULL;
        new_pr->r_time = time(NULL);
        new_pr->pr_path = path;
        new_pr->pr_chunck_size = options_.chunck_size;
        new_pr->pr_start_chunck = start_chunck;
        new_pr->pr_chunck_num = end_chunck - start_chunck + 1;
        // backward pre pull strategy
        if(new_pr->pr_start_chunck >= options_.backward_pre_pull_chunck_num) {
          new_pr->pr_chunck_num += options_.backward_pre_pull_chunck_num;
          new_pr->pr_start_chunck -= options_.backward_pre_pull_chunck_num;
        } else {
          new_pr->pr_chunck_num += new_pr->pr_start_chunck;
          new_pr->pr_start_chunck = 0;
        }
        // forward pre pull strategy
        new_pr->pr_chunck_num += options_.forward_pre_pull_chunck_num;
      }
      req_list_.push_back(new_pr);
      req_cv_.notify_all();
      return 0;
    }
    target_dentry = it->second;
  }

  // successfully find target dentry
  inode* target_inode = target_dentry->d_inode;
  if(target_inode == nullptr) {
    // it is a directory
    LOG(INFO) << "Given path is a directory";
    return 0;
  }
  if(target_inode->i_state == FileState::INVALID) {
    // file is invaild
    LOG(INFO) << "File has been invaild";
    return 0;
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
  LOG(INFO) << "Read from chunck " << start_chunck_no << ":" << start_chunck_offset 
            << " to chunck " << end_chunck_no << ":" << end_chunck_offset; 

  //check if all chunck exist
  std::vector<std::pair<uint64_t, uint64_t>> lack_extents;
  std::shared_lock<std::shared_mutex> target_den_lck(target_dentry->d_mtx);
  if(!check_chuncks_exist(target_inode, start_chunck_no, end_chunck_no, lack_extents)) {
    std::unique_lock<std::mutex> lck(req_mtx_);
    for(const auto& extent : lack_extents) {
      LOG(INFO) << "Chuncks " << extent.first << " to " << extent.first + extent.second - 1 << "do not exist, "
                << "publish a pull request";
      pull_request* new_pr = new pull_request;
      {
        new_pr->r_type = RequestType::PULL;
        new_pr->r_time = time(NULL);
        new_pr->pr_path = path;
        new_pr->pr_chunck_size = options_.chunck_size;
        new_pr->pr_start_chunck = extent.first;
        new_pr->pr_chunck_num = extent.second;
      }
      req_list_.push_back(new_pr);
    }
    req_cv_.notify_all();
    return 0;
  }

  // all chunck exist, read chunck
  LOG(INFO) << "All chuncks exist";
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
  LOG(INFO) << "Successfully read " << total_read_bytes << " bytes";
  return total_read_bytes;
}

std::string EdgeFS::get_path_from_inode(inode* in) {
  std::string path = "";
  dentry* cur_den = in->i_dentry;
  while(cur_den->d_parent != nullptr) {
    path = std::string("/") + cur_den->d_name + path;
    cur_den = cur_den->d_parent;
  }
  return path;
}

void EdgeFS::split_path(const char *path, std::vector<std::string>& d_names) {
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

bool EdgeFS::check_chuncks_exist(inode* in, uint64_t start_chunck_no, uint64_t end_chunck_no, _OUT std::vector<std::pair<uint64_t, uint64_t>> lack_extents) {
  bool all_exist = true;
  uint64_t now_extent_start_chunck = UINT64_MAX;
  for(uint64_t chunck_no = start_chunck_no; chunck_no <= end_chunck_no; chunck_no++) {
    if(!in->i_chunck_bitmap->Get(chunck_no)) {
      // chunck does not exist
      all_exist = false;
      if(now_extent_start_chunck == UINT64_MAX) {
        now_extent_start_chunck = chunck_no;
      }
    } else {
      // exist
      if(now_extent_start_chunck != UINT64_MAX) {
        lack_extents.push_back({now_extent_start_chunck, chunck_no - now_extent_start_chunck + 1});
        now_extent_start_chunck = UINT64_MAX;
      }
    }
  }

  if(now_extent_start_chunck != UINT64_MAX) {
    lack_extents.push_back({now_extent_start_chunck, end_chunck_no - now_extent_start_chunck + 1});
  }
  return all_exist;
}

int EdgeFS::read_from_chunck(const char *path, subinode* subi, char *buf, std::size_t size, off_t offset) {
  // set chunck to active state
  subi->subi_state = ChunckState::ACTIVE;

  // convert to block no and offset
  uint64_t start_block_no = offset / subi->subi_inode->i_block_size;
  uint64_t start_block_offset = offset % subi->subi_inode->i_block_size;              // the first byte
  uint64_t end_block_no = (offset + size - 1) / subi->subi_inode->i_block_size;
  uint64_t end_block_offset = (offset + size - 1) % subi->subi_inode->i_block_size;   // the last byte

  // contain last block in this chunck
  bool have_last_block_in_file = (subi->subi_no == (subi->subi_inode->i_chunck_bitmap->size() - 1));
  uint64_t last_block_in_file_size = subi->subi_inode->i_len % subi->subi_inode->i_block_size;
  uint64_t last_block_in_chunck = subi->subi_block_bitmap->size() - 1;

  LOG(INFO) << "Read chunck " << subi->subi_no << " blocks " << start_block_no << ":" << start_block_offset
            << " to " << end_block_no << ":" << end_block_offset;

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
    std::unique_lock<std::shared_mutex> blocks_lck(subi->subi_mtx);
    if(subi->subi_block_bitmap->Get(block_no)) {
      // Block has been cached, unlock
      blocks_lck.unlock();
      LOG(INFO) << "Block " << block_no << " has been cached";
      memcpy(cur_buf_ptr, (subi->subi_blocks[block_no]->b_data + cur_offset), cur_size);
      subi->subi_blocks[block_no]->b_atime = time(NULL);
      subi->subi_blocks[block_no]->b_acounter++;
    } else {
      // Block has not been cache, keep lock
      // firstly open chunck file
      if(f_chunck == NULL) {
        std::string chunck_data_path = options_.data_root_path + std::string(path) + "/" + std::to_string(subi->subi_no);
        f_chunck = fopen(chunck_data_path.c_str(), "rb");
        if(f_chunck == NULL) {
          return 0;
        }
      }
      
      // try to cache the block 
      LOG(INFO) << "Try to cache block " << block_no;
      cacheblock* new_block = mm_->Allocate(subi->subi_inode->i_block_size);
      if(new_block != nullptr) {
        // successfully allocate a block
        fseek(f_chunck, block_no * subi->subi_inode->i_block_size, SEEK_SET);
        uint64_t read_size = fread(new_block->b_data, 1, subi->subi_inode->i_block_size, f_chunck);
        if((have_last_block_in_file && block_no == last_block_in_chunck && read_size != last_block_in_file_size)
           || read_size != subi->subi_inode->i_block_size) {
          mm_->Free(new_block);
          return 0;
        }
        new_block->b_atime = time(NULL);
        new_block->b_acounter++;
        subi->subi_blocks[block_no] = new_block;
        subi->subi_block_bitmap->Set(block_no);
        memcpy(cur_buf_ptr, new_block->b_data + cur_offset, cur_size);
        LOG(INFO) << "Successfully cache";
      } else {
        // fail to allocate a block, seek and read from disk
        LOG(INFO) << "Cache failed, directly read from disk";
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

void EdgeFS::gc_extent(inode* in, uint64_t start_chunck_no, uint64_t chunck_num) {
  assert(in != nullptr);
  std::string path = get_path_from_inode(in);
  LOG(INFO) << "GC " << path << " from chunck " << start_chunck_no << " to " << start_chunck_no + chunck_num - 1;
  for(uint64_t chunck_no = start_chunck_no; chunck_no < start_chunck_no + chunck_num; chunck_no++) {
    if(!in->i_chunck_bitmap->Get(chunck_no)) {
      // chunck doesn't exist
      LOG(INFO) << "Chunck " << chunck_no << " does not exist";
      assert(in->i_subinodes.find(chunck_no) == in->i_subinodes.end());
      continue;
    }
    assert(in->i_subinodes.find(chunck_no) != in->i_subinodes.end());
    subinode* subi = in->i_subinodes[chunck_no];
    assert(subi != nullptr);
    if(subi->subi_state == ChunckState::INACTIVE) {
      // Chunck may become active again between from publishing the request to now
      // If chunck is active, do not gc
      assert(subi->subi_block_bitmap->cur_set() == 0);
      assert(subi->subi_blocks.size() == 0);
      std::string chunck_file_path = options_.data_root_path + path + "/" + std::to_string(chunck_no);
      remove(chunck_file_path.c_str());
      delete subi->subi_block_bitmap;
      delete subi;
      in->i_chunck_bitmap->Rel(chunck_no);
      in->i_subinodes.erase(chunck_no);
      LOG(INFO) << "GC chunck " << chunck_no;
    } else {
      LOG(INFO) << "Chunck " << chunck_no << " is active, no need to gc";
    }
  }
}

void EdgeFS::gc_whole_file(inode* in) {
  assert(in != nullptr);
  // delete all exist chuncks 
  std::string path = get_path_from_inode(in);
  LOG(INFO) << "GC whole file: " << path;
  for(auto it = in->i_subinodes.begin(); it != in->i_subinodes.end(); it++) {
    uint64_t chunck_no = it->first;
    subinode* subi = it->second;
    LOG(INFO) << "GC chunck " << chunck_no;
    assert(subi->subi_block_bitmap->cur_set() == 0);
    assert(subi->subi_blocks.size() == 0);
    std::string chunck_file_path = options_.data_root_path + path + "/" + std::to_string(chunck_no);
    remove(chunck_file_path.c_str());
    delete subi->subi_block_bitmap;
    delete subi;
    it->second = nullptr;
  }
}

void EdgeFS::dfs_scan(dentry* cur_den) {
  LOG(INFO) << "Scan dentry: " << cur_den->d_name;
  std::shared_lock<std::shared_mutex> den_lck(cur_den->d_mtx);
  if(cur_den->d_inode != nullptr) {
    LOG(INFO) << "Dentry " << cur_den->d_name << " is a file";
    // dentry is a file, acquire unique lock
    std::unique_lock<std::shared_mutex> lck(cur_den->d_mtx);
    inode* in = cur_den->d_inode;
    if(in->i_state == FileState::INVALID) {
      // File is invaild, no need to scan
      LOG(INFO) << "File " << cur_den->d_name << " is invaild";
      return;
    }
    // scan inactive chunck and block
    std::vector<uint64_t> chuncks_to_gc;
    for(auto chunck_it = in->i_subinodes.begin(); chunck_it != in->i_subinodes.end(); chunck_it++) {
      uint64_t chunck_no = chunck_it->first;
      subinode* subi = chunck_it->second;
      time_t now_time = time(NULL);
      std::unique_lock<std::shared_mutex> blocks_lck(subi->subi_mtx);
      if(now_time - subi->subi_atime < options_.min_chunck_active_time || 
         (now_time - subi->subi_ctime) / (subi->subi_acounter + 1) < options_.chunck_active_access_frequency) {
        // chunck is still active, check cache block 
        LOG(INFO) << "Chunck " << chunck_no << " is still active, check blocks";
        std::vector<uint64_t> block_to_free;
        block_to_free.reserve(subi->subi_blocks.size());
        for(auto block_it = subi->subi_blocks.begin(); block_it != subi->subi_blocks.end(); block_it++) {
          uint64_t block_no = block_it->first;
          cacheblock* block = block_it->second;
          if(now_time - block->b_atime < options_.min_block_active_time || 
             (now_time - block->b_ctime) / (block->b_acounter + 1) < options_.block_active_access_frequency) {
            // block is inactive, free block
            LOG(INFO) << "Chunck " << chunck_no << " block " << block_no << " is inactive, free it";
            mm_->Free(block);
            block_to_free.push_back(block_no);
          } else {
            LOG(INFO) << "Chunck " << chunck_no << " block " << block_no << " is still active";
          }
        }
        for(const uint64_t block_no : block_to_free) {
          // erase all blocks have been free
          subi->subi_block_bitmap->Rel(block_no);
          subi->subi_blocks.erase(block_no);
          assert(subi->subi_block_bitmap->cur_set() == subi->subi_blocks.size());
        }
      } else {
        // chunck is inactive, free all block
        LOG(INFO) << "Chunck " << chunck_no << " is inactive, free all blocks and publish gc request";
        chuncks_to_gc.push_back(chunck_no);
        subi->subi_state = ChunckState::INACTIVE;
        for(auto block_it = subi->subi_blocks.begin(); block_it != subi->subi_blocks.end(); block_it++) {
          mm_->Free(block_it->second);
          subi->subi_block_bitmap->Rel(block_it->first);
          subi->subi_blocks.erase(block_it->first);
        }
        assert(subi->subi_block_bitmap->cur_set() == 0);
        assert(subi->subi_blocks.size() == 0);
      }
    }
    // publish gc requests
    std::string file_path = get_path_from_inode(in);
    uint64_t cur_extent_start_no = UINT64_MAX;
    std::sort(chuncks_to_gc.begin(), chuncks_to_gc.end());
    for(size_t i = 0; i < chuncks_to_gc.size(); i++) {
      if(cur_extent_start_no == UINT64_MAX) {
        // start a new extent
        cur_extent_start_no = chuncks_to_gc[i];
      }
      if((i == chuncks_to_gc.size() - 1) || chuncks_to_gc[i + 1] != chuncks_to_gc[i] + 1) {
        // end a extent and create a extent gc request
        std::unique_lock<std::mutex> req_lck(req_mtx_);
        gc_request* new_gcr = new gc_request;
        {
          new_gcr->r_type = RequestType::GC;
          new_gcr->r_time = time(NULL);
          new_gcr->gcr_path = file_path;
          new_gcr->gcr_start_chunck_no = cur_extent_start_no;
          new_gcr->gcr_chuncks_num = chuncks_to_gc[i] - cur_extent_start_no + 1;
          new_gcr->gcr_reason = GCReason::INACTIVECHUNCK;
        }
        req_list_.push_back(new_gcr);
      }
    }
    req_cv_.notify_all();
  } else {
    // dentry is a directory, acquire shared lock
    LOG(INFO) << "Dentry " << cur_den->d_name << " is a directory";
    std::shared_lock<std::shared_mutex> lck(cur_den->d_mtx);
    for(auto it = cur_den->d_childs.begin(); it != cur_den->d_childs.end(); it++) {
      dfs_scan(it->second);
    }
  }
}

void EdgeFS::RPC() {
  LOG(INFO) << "RPC Server Start";
  brpc::Server edge_server;
  EdgeServiceImpl edge_service;
  if(edge_server.AddService(&edge_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Fail to add service";
    exit(-1);
  }
  
  std::string center_ip_port = options_.center_address + ":" + std::to_string(options_.center_port);
  brpc::ServerOptions options;
  if(edge_server.Start(center_ip_port.c_str(), &options) != 0) {
    LOG(ERROR) << "Fail to start EdgeRPC";
    exit(-1);
  } else {
    LOG(INFO) << "EdgeRPC start.";
  }
  edge_server.RunUntilAskedToQuit();
  LOG(INFO) << "EdgeRPC stop";
}

void EdgeFS::GC_AND_PULL() {
  LOG(INFO) << "GC_AND_PULL Thread Start";
  brpc::Channel rpc_channel;
  brpc::ChannelOptions rpc_options;
  rpc_options.protocol = "baidu_std";
  rpc_options.timeout_ms = 1000;
  rpc_options.max_retry = 3;
  if(rpc_channel.Init(options_.center_address.c_str(), options_.center_port, &rpc_options) != 0) {
    LOG(ERROR) << "Connect to center failed.";
    exit(1);
  }
  CenterService_Stub* stub = new CenterService_Stub(&rpc_channel);
  brpc::Controller cntl; 

  while(true) {
    // wait 
    cntl.Reset();
    std::unique_lock<std::mutex> lck(req_mtx_);
      req_cv_.wait(lck, [&] {
        return !req_list_.empty();
    });

    // get request
    request* cur_req = req_list_.front();
    if(cur_req->r_type == RequestType::PULL) {
      bool need_pull = false;
      bool error_request = false;
      pull_request* cur_pr = reinterpret_cast<pull_request*>(cur_req);
      LOG(INFO) << "Get a pull request, "
                << "[Time] " << cur_pr->r_time
                << "[Path] " << cur_pr->pr_path
                << "[ChunckSize] " << cur_pr->pr_chunck_size
                << "[StartChunck] " << cur_pr->pr_start_chunck
                << "[ChunckNum] " << cur_pr->pr_chunck_num;
      std::vector<std::pair<uint64_t, uint64_t>> split_extents;     // <start_chunck, chunck_num>
      std::vector<std::unique_ptr<std::shared_lock<std::shared_mutex>>> path_lcks;

      // 1. search dentry
      dentry* target_dentry = root_dentry_;
      std::vector<std::string> d_names;
      split_path(cur_pr->pr_path.c_str(), d_names);
      for(size_t i = 0; i < d_names.size(); i++) {
        path_lcks.push_back(std::make_unique<std::shared_lock<std::shared_mutex>>(target_dentry->d_mtx));
        if(target_dentry->d_inode != nullptr) {
          // There is file in the path
          error_request = true;
          break;
        }
        std::string dname = d_names[i];
        auto it = target_dentry->d_childs.find(dname);
        if(it == target_dentry->d_childs.end()) {
          // can not find target file, commit the pull request
          need_pull = true;
          target_dentry = nullptr;
          break;
        }
        target_dentry = it->second;
      }
      if(error_request) {
        LOG(INFO) << "Path is illegal";
        req_list_.pop_front();
        delete cur_pr;
        continue;
      }

      // 2. find dentry, check file state
      if(!need_pull) {
        inode* target_inode = target_dentry->d_inode;
        // check if file is alive
        if(target_inode == nullptr || target_inode->i_state == FileState::INVALID) {
          // (1) it is a directory
          // (2) file is invaild
          // throw the request
          LOG(INFO) << "Target dentry is directory or invaild file, throw the request";
          req_list_.pop_front();
          delete cur_pr;
          continue;
        } else if(target_inode->i_state == FileState::ALIVE) {
          // file is alive, check each chunck
          LOG(INFO) << "File is alive, check each chunck";
          std::shared_lock<std::shared_mutex> tmp_target_den_lck(target_dentry->d_mtx);
          if(cur_pr->pr_start_chunck + cur_pr->pr_chunck_num - 1 > target_inode->i_chunck_bitmap->size() - 1) {
            // request end chunck no exceeds the greatest chunck no in the file
            cur_pr->pr_chunck_num = target_inode->i_chunck_bitmap->size() - cur_pr->pr_start_chunck;
          }
          uint64_t last_lack_chunck = UINT64_MAX;
          for(uint64_t chunck_no = cur_pr->pr_start_chunck; chunck_no < cur_pr->pr_start_chunck + cur_pr->pr_chunck_num; chunck_no++) {
            if(target_inode->i_chunck_bitmap->Get(chunck_no)) {
              // chunck exist
              LOG(INFO) << "Chunck " << chunck_no << " exists";
              if(target_inode->i_subinodes[chunck_no]->subi_state == ChunckState::INACTIVE) {
                // change chunck state to alive
                target_inode->i_subinodes[chunck_no]->subi_state = ChunckState::ACTIVE;
              }
              if(last_lack_chunck != UINT64_MAX) {
                  split_extents.emplace_back(last_lack_chunck, chunck_no - last_lack_chunck);
                  last_lack_chunck = UINT64_MAX;
              }
            } else {
              // chunck doesn't exist, need to pull
              if(last_lack_chunck == UINT64_MAX) {
                last_lack_chunck = chunck_no;
              }
            }
            if(chunck_no == cur_pr->pr_start_chunck + cur_pr->pr_chunck_num - 1 && last_lack_chunck != UINT64_MAX) {
              // The last chunck
              split_extents.emplace_back(last_lack_chunck, chunck_no - last_lack_chunck + 1);
            }
          }
        }
        if(!split_extents.empty()) {
          need_pull = true;
        }
      }

      // need pull
      if(need_pull) {
        if(target_dentry == nullptr) {
          // the whole file doesn't exist, build the dentry and inode
          // getattr from center
          StatRequest rpc_stat_request;
          StatReply rpc_stat_reply;
          rpc_stat_request.set_pr_path(cur_pr->pr_path);
          stub->Stat(&cntl, &rpc_stat_request, &rpc_stat_reply, NULL);
          if(cntl.Failed() || !rpc_stat_reply.ok()) {
            LOG(INFO) << "Get metadata failed";
            req_list_.pop_front();
            delete cur_pr;
            continue;
          }
          if(rpc_stat_reply.mtime() >= cur_pr->r_time) {
            LOG(INFO) << "Target file is newer than request time, throw the pull request";
            req_list_.pop_front();
            delete cur_pr;
            continue;
          }

          // build dentry
          path_lcks.clear();
          target_dentry = root_dentry_;
          for(size_t i = 0; i < d_names.size(); i++) {
            path_lcks.push_back(std::make_unique<std::shared_lock<std::shared_mutex>>(target_dentry->d_mtx));
            auto it = target_dentry->d_childs.find(d_names[i]);
            if(it == target_dentry->d_childs.end()) {
              // release shared lock and acquire unique lock
              path_lcks.back()->unlock();
              std::unique_lock<std::shared_mutex> tmp_unique_lck(target_dentry->d_mtx);
              // build dentry
              LOG(INFO) << "Build dentry: " << d_names[i];
              dentry* new_dentry = new dentry{d_names[i], nullptr, target_dentry, 0, std::shared_mutex(), std::map<std::string, dentry*>()};
              if(i == d_names.size() - 1) {
                // build inode
                LOG(INFO) << "Build file inode";
                inode* new_inode = new inode;
                {
                  new_inode->i_len = rpc_stat_reply.len();
                  new_inode->i_chunck_size = options_.chunck_size;
                  new_inode->i_block_size = options_.block_size;
                  new_inode->i_state = FileState::ALIVE;
                  new_inode->i_mtime = rpc_stat_reply.mtime();
                  new_inode->i_atime = time(NULL);
                  new_inode->i_mode = 0444 | __S_IFREG;
                  new_inode->i_dentry = new_dentry;
                  new_inode->i_subinodes = std::map<uint64_t, subinode*>();
                  uint64_t total_chunck_num = new_inode->i_len / new_inode->i_chunck_size;
                  if(new_inode->i_len % new_inode->i_chunck_size != 0) {
                    total_chunck_num++;
                  }
                  new_inode->i_chunck_bitmap = new BitMap(total_chunck_num);
                }
                new_dentry->d_inode = new_inode;
              }
              target_dentry->d_childs[d_names[i]] = new_dentry;
              target_dentry = new_dentry;
              // check if pull request exceed file size
              if(cur_pr->pr_start_chunck + cur_pr->pr_chunck_num - 1 > target_dentry->d_inode->i_chunck_bitmap->size() - 1) {
                cur_pr->pr_chunck_num = target_dentry->d_inode->i_chunck_bitmap->size() - cur_pr->pr_start_chunck;
              }
              split_extents.emplace_back(cur_pr->pr_start_chunck, cur_pr->pr_chunck_num);
              // release unique lock and acquire shared lock
              tmp_unique_lck.unlock();
              path_lcks.back()->lock();
            } else {
              target_dentry = it->second;
            }
          }
        }

        assert(target_dentry->d_inode != nullptr);
        for(const auto& extent : split_extents) {
          // request each extents
          cntl.Reset();
          LOG(INFO) << "Pull chunck " << extent.first << " to " << extent.first + extent.second - 1;
          PullRequest rpc_pull_request;
          PullReply rpc_pull_reply;
          rpc_pull_request.set_pr_path(cur_pr->pr_path);
          rpc_pull_request.set_pr_time(cur_pr->r_time);
          rpc_pull_request.set_chunck_size(cur_pr->pr_chunck_size);
          rpc_pull_request.set_start_chunck(extent.first);
          rpc_pull_request.set_chunck_size(extent.second);
          stub->PULL(&cntl, &rpc_pull_request, &rpc_pull_reply, NULL);
          if(!cntl.Failed() && rpc_pull_reply.ok()) {
            // rpc request successfully
            LOG(INFO) << "Pull successfully";
            std::unique_lock<std::shared_mutex> target_den_lck(target_dentry->d_mtx);
            for(int chunck_num = 0; chunck_num < rpc_pull_reply.chuncks_size(); chunck_num++) {
              // write each chunck
              uint64_t chunck_no = rpc_pull_reply.chuncks(chunck_num).chunck_no();
              uint64_t cur_chunck_size = rpc_pull_reply.chuncks(chunck_num).data().size();
              assert(!target_dentry->d_inode->i_chunck_bitmap->Get(chunck_no));
              assert(target_dentry->d_inode->i_subinodes.find(chunck_no) == target_dentry->d_inode->i_subinodes.end());
              std::string chunck_file_path = options_.data_root_path + cur_pr->pr_path + "/" + std::to_string(chunck_no);
              FILE* f_chunck = fopen(chunck_file_path.c_str(), "wb");
              if(f_chunck == NULL) {
                LOG(INFO) << "Create chunck " << chunck_no << " file failed";
                continue;
              }
              if(fwrite(rpc_pull_reply.chuncks(chunck_num).data().c_str(), 1, cur_chunck_size, f_chunck) == cur_chunck_size) {
                // write successfully
                subinode* new_subi = new subinode;
                {
                  new_subi->subi_no = chunck_no;
                  new_subi->subi_inode = target_dentry->d_inode;
                  new_subi->subi_ctime = time(NULL);
                  new_subi->subi_atime = time(NULL);
                  new_subi->subi_acounter = 0;
                  new_subi->subi_state = ChunckState::ACTIVE;
                  new_subi->subi_blocks = std::map<uint64_t, cacheblock*>();
                  uint64_t total_block_num = cur_chunck_size / options_.block_size;
                  if(cur_chunck_size % options_.block_size != 0) {
                    total_block_num++;
                  }
                  new_subi->subi_block_bitmap = new BitMap(total_block_num);
                }
                target_dentry->d_inode->i_chunck_bitmap->Set(chunck_no);
                target_dentry->d_inode->i_subinodes[chunck_no] = new_subi;
              } else {
                LOG(INFO) << "Write chunck " << chunck_no << " file failed";
              }
            }
          } else {
            LOG(INFO) << "Pull failed";
          }
        }
      }
      // finish request
      LOG(INFO) << "Finish pull request";
      req_list_.pop_front();
      delete cur_pr;
    } else if(cur_req->r_type == RequestType::GC) {
      gc_request* cur_gcr = reinterpret_cast<gc_request*>(cur_req);
      LOG(INFO) << "Get a pull request, "
          << "[Time] " << cur_gcr->r_time
          << "[Path] " << cur_gcr->gcr_path;
      // 1. search dentry
      dentry* target_dentry = root_dentry_;
      std::vector<std::string> d_names;
      std::vector<std::unique_ptr<std::shared_lock<std::shared_mutex>>> path_lcks;
      split_path(cur_gcr->gcr_path.c_str(), d_names);
      for(const auto& dname : d_names) {
        path_lcks.push_back(std::make_unique<std::shared_lock<std::shared_mutex>>(target_dentry->d_mtx));
        auto it = target_dentry->d_childs.find(dname);
        if(it == target_dentry->d_childs.end()) {
          // can not find target file
          target_dentry = nullptr;
          break;
        }
        target_dentry = it->second;
      }
      if(target_dentry == nullptr || target_dentry->d_inode == nullptr) {
        // (1) dentry doesn't exist
        // (2) dentry is not a file
        // throw the request
        LOG(INFO) << "Dentry does not exist or is a directory";
        req_list_.pop_front();
        delete cur_gcr;
        continue;
      }

      // 2. check the file modify time 
      inode* target_inode = target_dentry->d_inode;
      if(target_inode->i_mtime >= cur_gcr->r_time) {
        LOG(INFO) << "Target file is newer than request time, throw the pull request";
        req_list_.pop_front();
        delete cur_gcr;
        continue;
      }

      // 3. start to gc
      std::unique_lock<std::shared_mutex> target_den_lck(target_dentry->d_mtx);
      if(cur_gcr->gcr_reason == GCReason::INACTIVECHUNCK) {
        gc_extent(target_inode, cur_gcr->gcr_start_chunck_no, cur_gcr->gcr_chuncks_num);
      } else if(cur_gcr->gcr_reason == GCReason::INVAILDFILE) {
        gc_whole_file(target_inode);
      }

      // if file is empty, recursively remove inode and dentry
      if(target_dentry->d_inode->i_chunck_bitmap->cur_set() == 0) {
        LOG(INFO) << "No chunck exist, recursively remove inode and dentry";
        // remove inode
        delete target_dentry->d_inode->i_chunck_bitmap;
        delete target_dentry->d_inode;
        target_dentry->d_inode = nullptr;
        target_den_lck.unlock();
        // recursively delete empty directory
        while(target_dentry != root_dentry_ && target_dentry->d_childs.empty()) {
          assert(target_dentry->d_inode == nullptr);
          LOG(INFO) << "Delete dentry: " << target_dentry->d_name;
          // release parent shared lock and acquire parent unique lock
          dentry* parent_dentry = target_dentry->d_parent;
          path_lcks.pop_back();
          std::unique_lock<std::shared_mutex> parent_dentry_lck(parent_dentry->d_mtx);
          parent_dentry->d_childs.erase(target_dentry->d_name);
          delete target_dentry;
          target_dentry = parent_dentry;
        }
      }
    } else {
      // throw and nothing to do
    }
  }
  delete stub;
}

void EdgeFS::SCAN() {
  LOG(INFO) << "SCAN Thread Start";
  // DFS traverse all dentry
  while(true) {
    std::this_thread::sleep_for(std::chrono::seconds(options_.scan_period));
    LOG(INFO) << "Scan start";
    dfs_scan(root_dentry_);
    LOG(INFO) << "Scan end";
  }
}

} // namespace edgefs
