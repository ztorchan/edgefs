#ifndef _EDGEFS_EDGERPC_H
#define _EDGEFS_EDGERPC_H

#include <string>

#include "edgefs/edgefs.h"
#include "edgefs/rpc/edge_service.pb.h"

namespace edgefs
{

class EdgeServiceImpl : public EdgeService {
public:
  void Invalid(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
               const ::edgefs::InvalidFileRequest* request,
               PROTOBUF_NAMESPACE_ID::Empty* response,
               ::google::protobuf::Closure* done) override;
};

} // namespace edgefs

#endif