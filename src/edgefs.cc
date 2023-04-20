#include <cstdlib>
#include <glog/logging.h>
#include <brpc/server.h>

#include "edgefs/edgefs.h"
#include "edgefs/rpc/center_service.pb.h"

#define EDGEFS_PORT 2333

namespace edgefs
{

std::thread* EdgeFS::rpc_thread_ = nullptr;
std::thread* EdgeFS::gc_thread_ = nullptr;
std::thread* EdgeFS::pull_thread_ = nullptr;
struct dentry* EdgeFS::root_dentry_ = nullptr;
std::string EdgeFS::center_ipv4_ = std::string();

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

void EdgeFS::Init() {

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

int EdgeFS::readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offser, struct fuse_file_info * fi) {
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

void EdgeFS::RPC() {
  brpc::Server edge_server;
  EdgeServiceImpl edge_service("/edge", "/edgedata");
  if(edge_server.AddService(&edge_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Fail to add service";
    exit(-1);
  }
  
  butil::EndPoint point = butil::EndPoint(butil::IP_ANY, EDGEFS_PORT);
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



} // namespace edgefs
