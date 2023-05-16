#ifndef _EDGEFS_CENTERRPC_H
#define _EDGEFS_CENTERRPC_H

#include "edgefs/rpc/center_service.pb.h"

namespace edgefs
{

class CenterServiceImpl : public CenterService {
public:
  void PULL(::google::protobuf::RpcController* controller,
            const ::edgefs::PullRequest* request,
            ::edgefs::PullReply* response,
            ::google::protobuf::Closure* done) override;
  void Stat(::google::protobuf::RpcController* controller,
            const ::edgefs::StatRequest* request,
            ::edgefs::StatReply* response,
            ::google::protobuf::Closure* done) override;
};

} // namespace edgefs


#endif