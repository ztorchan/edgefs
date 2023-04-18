#ifndef _EDGEFS_EDGEFSRPC_H
#define _EDGEFS_EDGEFSRPC_H

#include <string>

#include "edgefs/rpc/edge_service.pb.h"

namespace edgefs
{

class EdgeServiceImpl : public EdgeService {
public:
  EdgeServiceImpl(std::string edgefs_root, std::string edgefs_data_root);

  void Invalid(::google::protobuf::RpcController* controller,
               const ::edgefs::InvalidFileRequest* request,
               ::google::protobuf::Empty* response,
               ::google::protobuf::Closure* done) override;

private:
  std::string edgefs_root_;
  std::string edgefs_data_root_;
};


} // namespace edgefs

#endif