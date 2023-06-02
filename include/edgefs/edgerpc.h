#ifndef _EDGEFS_EDGERPC_H
#define _EDGEFS_EDGERPC_H

#include <string>

#include "edgefs/rpc/edge_service.pb.h"

namespace edgefs
{

struct edgefs_inode;

class EdgeChunckStreamReceiver : public brpc::StreamInputHandler {
public:
  EdgeChunckStreamReceiver(std::string fpath, edgefs_inode* fi)
  : fpath_(fpath)
  , fi_(fi)
  , finish_(false)
  , close_(false) {}
  
  int on_received_messages(brpc::StreamId id, 
                           butil::IOBuf *const messages[], 
                           size_t size) override;
  void on_idle_timeout(brpc::StreamId id) override;
  void on_closed(brpc::StreamId id) override;
  bool is_finish() { return finish_; }
  bool is_close() { return close_; }
private:
  const std::string fpath_;
  edgefs_inode* fi_;
  bool finish_;
  bool close_;
};

class EdgeServiceImpl : public EdgeService {
public:
  void Invalid(::google::protobuf::RpcController* controller,
               const ::edgefs::InvalidFileRequest* request,
               ::google::protobuf::Empty* response,
               ::google::protobuf::Closure* done) override;
};

} // namespace edgefs

#endif