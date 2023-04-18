#include <brpc/channel.h>
#include <glog/logging.h>

#include "edgefs/edgefs.h"
#include "edgefs/edgerpc.h"

namespace edgefs
{

EdgeServiceImpl::EdgeServiceImpl(std::string edgefs_root, std::string edgefs_data_root) : 
  EdgeService(),
  edgefs_root_(edgefs_root),
  edgefs_data_root_(edgefs_data_root) {}

void EdgeServiceImpl::Invalid(::google::protobuf::RpcController* controller,
                              const ::edgefs::InvalidFileRequest* request,
                              ::google::protobuf::Empty* response,
                              ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  

}

} // namespace edgefs
