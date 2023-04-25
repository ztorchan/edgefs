#include <cstdlib>
#include <chrono>
#include <fstream>
#include <sstream>
#include <filesystem>

#include <glog/logging.h>
#include <brpc/server.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/document.h>

#include "edgefs/edgefs.h"
#include "edgefs/rpc/center_service.pb.h"

#define EDGEFS_PORT 2333

namespace edgefs
{

std::thread* EdgeFS::rpc_thread_ = nullptr;
std::thread* EdgeFS::gc_thread_ = nullptr;
std::thread* EdgeFS::pull_thread_ = nullptr;
struct dentry* EdgeFS::root_dentry_ = nullptr;
std::list<gc_request*> EdgeFS::gc_list_;
std::mutex EdgeFS::gc_mtx_;
std::condition_variable EdgeFS::gc_cv_;
std::list<pull_request*> EdgeFS::pull_list_;
std::mutex EdgeFS::pull_mtx_;
std::condition_variable EdgeFS::pull_cv_;
Option EdgeFS::options_;


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
      std::unique_lock<std::mutex> lck(pull_mtx_);
      pull_list_.push_back(new pull_request{
        path,
        
      });
      return 0;
    }
    target_dentry = it->second;
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

void EdgeFS::GC() {
  while(true) {
    std::unique_lock<std::mutex> lck(EdgeFS::gc_mtx_);
    EdgeFS::gc_cv_.wait(lck, [&] {
      return !EdgeFS::gc_list_.empty();
    });

    
  }
}

void EdgeFS::PULL() {
  while(true) {

  }
}

void EdgeFS::SCAN() {
  while(true) {

  }
}



} // namespace edgefs
