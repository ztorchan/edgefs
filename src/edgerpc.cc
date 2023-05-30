#include <string>

#include <brpc/channel.h>
#include <butil/logging.h>
#include <spdlog/spdlog.h>

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
  
  spdlog::info("[invalid] try to invalid file {}", request->path());
  std::vector<std::string> d_names;
  EdgeFS::split_path(request->path().c_str(), d_names);
  std::vector<std::unique_ptr<std::shared_lock<std::shared_mutex>>> path_lcks;
  dentry* target_dentry = EdgeFS::root_dentry_;
  for(const auto& dname : d_names) {
    path_lcks.push_back(std::make_unique<std::shared_lock<std::shared_mutex>>(target_dentry->d_mtx));
    auto it = target_dentry->d_childs->find(dname);
    if(it == target_dentry->d_childs->end()) {
      // can not find such file
      spdlog::info("[invalid] failed to find target dentry");
      return;
    }
    target_dentry = it->second;
  }

  // set file state
  spdlog::info("[invalid] set file state to INVALID");
  std::unique_lock<std::shared_mutex> target_dentry_lck(target_dentry->d_mtx);
  target_dentry->d_inode->i_state = FileState::INVALID;
  target_dentry_lck.unlock();
  
  // publish a gc request
  spdlog::info("[invalid] publish a gc request");
  std::unique_lock<std::mutex> req_list_lck(EdgeFS::req_mtx_);
  gc_request* new_gcr = new gc_request;
  {
    new_gcr->r_type = RequestType::GC;
    new_gcr->r_time = time(NULL);
    new_gcr->gcr_path = request->path();
    new_gcr->gcr_reason = GCReason::INVAILDFILE;
  }
  EdgeFS::req_list_.push_back(new_gcr);
  EdgeFS::req_cv_.notify_all();

  spdlog::info("[invalid] invalid file successfully");
  return ;
}

} // namespace edgefs
