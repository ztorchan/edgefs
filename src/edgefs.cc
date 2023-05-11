#include <cstdlib>
#include <cstdio>
#include <chrono>
#include <fstream>
#include <sstream>
#include <filesystem>

#include <glog/logging.h>
#include <brpc/channel.h>
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
  if(target_inode == nullptr) {
    st->st_mode = 0444 | __S_IFDIR;
  } else {
    st->st_mode = target_inode->i_mode;
    st->st_atime = target_inode->i_atime;
    st->st_mtime = target_inode->i_mtime;
  }
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
    if(target_inode == nullptr) {
      st.st_mode = 0444 | __S_IFDIR;
    } else {
      st.st_mode = target_inode->i_mode;
      st.st_atime = target_inode->i_atime;
      st.st_mtime = target_inode->i_mtime;
    }
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
      pull_request* new_pr = new pull_request;
      {
        new_pr->r_type = RequestType::PULL;
        new_pr->r_time = time(NULL);
        new_pr->pr_path = path;
        new_pr->pr_chunck_size = options_.chunck_size;
        new_pr->pr_start_chunck = start_chunck;
        new_pr->pr_chunck_num = end_chunck - start_chunck + 1;
      }
      req_list_.push_back(new_pr);
      req_cv_.notify_all();
      return 0;
    }
    target_dentry = it->second;
  }

  // successfully find target dentry
  struct inode* target_inode = target_dentry->d_inode;
  if(target_inode == nullptr) {
    // it is a directory
    return 0;
  }
  if(target_inode->i_state == FileState::INVAILD) {
    // file is invaild
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
  
  //check if all chunck exist
  std::vector<std::pair<uint64_t, uint64_t>> lack_extents;
  if(!check_chuncks_exist(target_inode, start_chunck_no, end_chunck_no, lack_extents)) {
    std::unique_lock<std::mutex> lck(req_mtx_);
    for(const auto& extent : lack_extents) {
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

std::string EdgeFS::get_path_from_inode(struct inode* in) {
  std::string path;
  struct dentry* cur_den = in->i_dentry;
  while(cur_den->d_parent != nullptr) {
    path = std::string("/") + cur_den->d_name + path;
  }
  return path;
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
  // set chunck to active state
  subi->subi_state = ChunckState::ACTIVE;

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

void EdgeFS::gc_extent(struct inode* in, uint64_t start_chunck_no, uint64_t chunck_num) {
  assert(in != nullptr);
  std::string path = get_path_from_inode(in);
  for(uint64_t chunck_no = start_chunck_no; chunck_no < start_chunck_no + chunck_num; chunck_no++) {
    if(!in->i_chunck_bitmap->Get(chunck_no)) {
      // chunck doesn't exist
      assert(in->i_subinodes.find(chunck_no) == in->i_subinodes.end());
      continue;
    }
    assert(in->i_subinodes.find(chunck_no) != in->i_subinodes.end());
    struct subinode* subi = in->i_subinodes[chunck_no];
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
    }
  }
}

void EdgeFS::gc_whole_file(struct inode* in) {
  assert(in != nullptr);
  // delete all exist chuncks 
  std::string path = get_path_from_inode(in);
  for(auto it = in->i_subinodes.begin(); it != in->i_subinodes.end(); it++) {
    uint64_t chunck_no = it->first;
    struct subinode* subi = it->second;
    assert(subi->subi_block_bitmap->cur_set() == 0);
    assert(subi->subi_blocks.size() == 0);
    std::string chunck_file_path = options_.data_root_path + path + "/" + std::to_string(chunck_no);
    remove(chunck_file_path.c_str());
    delete subi->subi_block_bitmap;
    delete subi;
    it->second = nullptr;
  }
  // delete inode
  struct dentry* den = in->i_dentry;
  delete in->i_chunck_bitmap;
  delete in;
  den->d_inode = nullptr;
  // recursively delete empty directory
  while(den != root_dentry_ && !den->d_childs.empty()) {
    assert(den->d_inode == nullptr);
    struct dentry* parent = den->d_parent;
    parent->d_childs.erase(den->d_name);
    delete den;
    den = parent;
  }
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
      pull_request* cur_pr = reinterpret_cast<pull_request*>(cur_req);
      std::vector<std::pair<uint64_t, uint64_t>> split_extents;     // <start_chunck, chunck_num>
      // 1. search dentry
      struct dentry* target_dentry = root_dentry_;
      std::vector<std::string> d_names;
      SplitPath(cur_pr->pr_path.c_str(), d_names);
      for(const auto& dname : d_names) {
        auto it = target_dentry->d_childs.find(dname);
        if(it == target_dentry->d_childs.end()) {
          // can not find target file, commit a pull request
          need_pull = true;
          target_dentry = nullptr;
          split_extents.emplace_back(cur_pr->pr_start_chunck, cur_pr->pr_chunck_num);
          break;
        }
        target_dentry = it->second;
      }

      // 2. find dentry, check file state
      if(!need_pull) {
        struct inode* target_inode = target_dentry->d_inode;
        // check if file is alive
        if(target_inode == nullptr || target_inode->i_state == FileState::INVAILD) {
          // (1) it is a directory
          // (2) file is invaild
          // throw the request
          req_list_.pop_front();
          delete cur_pr;
          continue;
        } else if(target_inode->i_state == FileState::ALIVE) {
          // file is alive, check each chunck
          uint64_t last_lack_chunck = UINT64_MAX;
          for(uint64_t chunck_no = cur_pr->pr_start_chunck; chunck_no < cur_pr->pr_start_chunck + cur_pr->pr_chunck_num; chunck_no++) {
            if(target_inode->i_chunck_bitmap->Get(chunck_no)) {
              // chunck exist
              if(target_inode->i_subinodes[chunck_no]->subi_state == ChunckState::INACTIVE) {
                // change chunck state to alive
                target_inode->i_subinodes[chunck_no]->subi_state = ChunckState::ACTIVE;
              }
              if(last_lack_chunck != UINT64_MAX) {
                  split_extents.emplace_back(last_lack_chunck, chunck_no - last_lack_chunck - 1);
              }
            } else {
              // chunck doesn't exist, need to pull
              if(last_lack_chunck == UINT64_MAX) {
                last_lack_chunck = chunck_no;
              }
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
            req_list_.pop_front();
            delete cur_pr;
            continue;
          }

          // build dentry
          target_dentry = root_dentry_;
          for(size_t i = 0; i < d_names.size(); i++) {
            auto it = target_dentry->d_childs.find(d_names[i]);
            if(it == target_dentry->d_childs.end()) {
              // build dentry
              struct dentry* new_dentry = new dentry{d_names[i], nullptr, target_dentry, std::mutex(), std::map<std::string, struct dentry*>()};
              if(i == d_names.size() - 1) {
                // build inode
                struct inode* new_inode = new inode;
                {
                  new_inode->i_len = rpc_stat_reply.len();
                  new_inode->i_chunck_size = options_.chunck_size;
                  new_inode->i_block_size = options_.block_size;
                  new_inode->i_ref = 0;
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
            } else {
              target_dentry = it->second;
            }
          }
        }

        assert(target_dentry->d_inode != nullptr);
        for(const auto& extent : split_extents) {
          // request each extents
          cntl.Reset();
          PullRequest rpc_pull_request;
          PullReply rpc_pull_reply;
          rpc_pull_request.set_pr_path(cur_pr->pr_path);
          rpc_pull_request.set_pr_time(cur_pr->r_time);
          rpc_pull_request.set_chunck_size(cur_pr->pr_chunck_size);
          rpc_pull_request.set_start_chunck(cur_pr->pr_start_chunck);
          rpc_pull_request.set_chunck_size(cur_pr->pr_chunck_num);
          stub->PULL(&cntl, &rpc_pull_request, &rpc_pull_reply, NULL);
          if(!cntl.Failed() && rpc_pull_reply.ok()) {
            // rpc request successfully
            for(int chunck_num = 0; chunck_num < rpc_pull_reply.chuncks_size(); chunck_num++) {
              // write each chunck
              uint64_t chunck_no = rpc_pull_reply.chuncks(chunck_num).chunck_no();
              assert(!target_dentry->d_inode->i_chunck_bitmap->Get(chunck_no));
              assert(target_dentry->d_inode->i_subinodes.find(chunck_no) == target_dentry->d_inode->i_subinodes.end());
              std::string chunck_file_path = options_.data_root_path + cur_pr->pr_path + "/" + std::to_string(chunck_no);
              FILE* f_chunck = fopen(chunck_file_path.c_str(), "w");
              if(f_chunck == NULL) {
                continue;
              }
              if(fwrite(rpc_pull_reply.chuncks(chunck_num).data().c_str(), 1, options_.chunck_size, f_chunck) == options_.chunck_size) {
                // write successfully
                struct subinode* new_subi = new subinode;
                {
                  new_subi->subi_no = chunck_no;
                  new_subi->subi_inode = target_dentry->d_inode;
                  new_subi->subi_ctime = time(NULL);
                  new_subi->subi_atime = time(NULL);
                  new_subi->subi_acounter = 0;
                  new_subi->subi_state = ChunckState::ACTIVE;
                  new_subi->subi_blocks = std::map<uint64_t, struct cacheblock*>();
                  uint64_t total_block_num = options_.chunck_size / options_.block_size;
                  if(options_.chunck_size % options_.block_size != 0) {
                    total_block_num++;
                  }
                  new_subi->subi_block_bitmap = new BitMap(total_block_num);
                }
                target_dentry->d_inode->i_chunck_bitmap->Set(chunck_no);
                target_dentry->d_inode->i_subinodes[chunck_no] = new_subi;
              }
            }
          }
        }
      }
      // finish request
      req_list_.pop_front();
      delete cur_pr;

    } else if(cur_req->r_type == RequestType::GC) {
      gc_request* cur_gcr = reinterpret_cast<gc_request*>(cur_req);
      // 1. search dentry
      struct dentry* target_dentry = root_dentry_;
      std::vector<std::string> d_names;
      SplitPath(cur_gcr->gcr_path.c_str(), d_names);
      for(const auto& dname : d_names) {
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
        req_list_.pop_front();
        delete cur_gcr;
        continue;
      }

      // 2. check the file modify time 
      struct inode* target_inode = target_dentry->d_inode;
      if(target_inode->i_mtime >= cur_gcr->r_time) {
        req_list_.pop_front();
        delete cur_gcr;
        continue;
      }

      // 3. start to gc
      if(cur_gcr->gcr_reason == GCReason::COLDCHUNCK) {
        gc_extent(target_inode, cur_gcr->gcr_start_chunck_no, cur_gcr->gcr_chuncks_num);
      } else if(cur_gcr->gcr_reason == GCReason::FILEINVAILD) {
        gc_whole_file(target_inode);
      }
    } else {
      // throw and nothing to do
    }
  }
  delete stub;
}

void EdgeFS::SCAN() {
  while(true) {

  }
}



} // namespace edgefs
