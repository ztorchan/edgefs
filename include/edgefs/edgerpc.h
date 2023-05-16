#ifndef _EDGEFS_EDGERPC_H
#define _EDGEFS_EDGERPC_H

#include <string>

#include "edgefs/edgefs.h"
#include "edgefs/rpc/edge_service.pb.h"

namespace edgefs
{

class EdgeServiceImpl : public EdgeService {
public:
  void Invalid(::google::protobuf::RpcController* controller,
               const ::edgefs::InvalidFileRequest* request,
               ::google::protobuf::Empty* response,
               ::google::protobuf::Closure* done) override;
};

} // namespace edgefs

#endif