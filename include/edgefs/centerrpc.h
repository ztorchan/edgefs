#ifndef _EDGEFS_CENTERRPC_H
#define _EDGEFS_CENTERRPC_H

#include "edgefs/rpc/center_service.pb.h"

namespace edgefs
{

class CenterServiceImpl : public CenterService {
public:
  void PULL(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
            const ::edgefs::PullRequest* request,
            ::edgefs::PullReply* response,
            ::google::protobuf::Closure* done) override;
  void Stat(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
            const ::edgefs::StatRequest* request,
            ::edgefs::StatReply* response,
            ::google::protobuf::Closure* done) override;
}

} // namespace edgefs


#endif