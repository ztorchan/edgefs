#include <string>

#include <brpc/channel.h>
#include <butil/logging.h>

#include "edgefs/edgefs.h"
#include "edgefs/edgerpc.h"

namespace edgefs
{

void EdgeServiceImpl::Invalid(::google::protobuf::RpcController* controller,
                              const ::edgefs::InvalidFileRequest* request,
                              ::google::protobuf::Empty* response,
                              ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  LOG(INFO) << "Try to invalid file: " << request->path();
  std::vector<std::string> d_names;
  EdgeFS::split_path(request->path().c_str(), d_names);
  dentry* target_dentry = EdgeFS::root_dentry_;
  for(const auto& dname : d_names) {
    auto it = target_dentry->d_childs.find(dname);
    if(it == target_dentry->d_childs.end()) {
      // can not find such file
      LOG(INFO) << "Can not find such file";
      return;
    }
    target_dentry = it->second;
  }
  target_dentry->d_inode->i_state = FileState::INVALID;
  LOG(INFO) << "Invalid file successfully";
  return ;
}

} // namespace edgefs
