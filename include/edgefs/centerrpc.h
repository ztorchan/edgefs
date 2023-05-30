#ifndef _EDGEFS_CENTERRPC_H
#define _EDGEFS_CENTERRPC_H

#include <string>

#include "edgefs/rpc/center_service.pb.h"

namespace edgefs
{

class CenterServiceImpl : public CenterService {
public:
  CenterServiceImpl(std::string basic_path) : CenterService(), basic_path_(basic_path) {}
  void Pull(::google::protobuf::RpcController* controller,
            const ::edgefs::PullRequest* request,
            ::edgefs::PullReply* response,
            ::google::protobuf::Closure* done) override;
  void Stat(::google::protobuf::RpcController* controller,
            const ::edgefs::StatRequest* request,
            ::edgefs::StatReply* response,
            ::google::protobuf::Closure* done) override;
  void Readdir(::google::protobuf::RpcController* controller,
               const ::edgefs::ReaddirRequest* request,
               ::edgefs::ReaddirReply* response,
               ::google::protobuf::Closure* done) override;
private:
  std::string basic_path_;
};

} // namespace edgefs


#endif