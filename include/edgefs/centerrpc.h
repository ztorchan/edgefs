#ifndef _EDGEFS_CENTERRPC_H
#define _EDGEFS_CENTERRPC_H

#include <cstdint>
#include <string>
#include <unordered_map>

#include "edgefs/rpc/center_service.pb.h"

namespace edgefs
{

class CenterChunckStreamReceiver : public brpc::StreamInputHandler {
public:
  CenterChunckStreamReceiver(std::string root_path)
  : root_path_(root_path)
  , sd_mtx_()
  , sd_ctxs_()
  , finish_(false) {}

  int on_received_messages(brpc::StreamId id, 
                           butil::IOBuf *const messages[], 
                           size_t size) override;
  void on_idle_timeout(brpc::StreamId id) override;
  void on_closed(brpc::StreamId id) override;

  void add_stream(brpc::StreamId* sd_ptr, std::string fpath, struct stat st, uint64_t chuncksize) {
    std::unique_lock<std::mutex> sd_lck(sd_mtx_);
    sd_ctxs_[*sd_ptr] = new sd_context{sd_ptr, fpath, st, chuncksize};
  }

  void close_stream(brpc::StreamId sd) { 
    std::unique_lock<std::mutex> sd_lck(sd_mtx_);
    if(sd_ctxs_[sd] != nullptr) {
      delete sd_ctxs_[sd]->sd;
      delete sd_ctxs_[sd];
    }
    sd_ctxs_.erase(sd);
   }

private:
  struct sd_context {
    brpc::StreamId* sd;
    std::string fpath;
    struct stat st;
    uint64_t chuncksize;
  };
  const std::string root_path_;
  std::mutex sd_mtx_;
  std::unordered_map<brpc::StreamId, sd_context*> sd_ctxs_;
  bool finish_;
};

class CenterServiceImpl : public CenterService {
public:
  CenterServiceImpl(std::string root_path) 
  : CenterService()
  , root_path_(root_path)
  , stream_receiver_(root_path_) {}
  
  void Pull(::google::protobuf::RpcController* controller,
            const ::edgefs::PullRequest* request,
            ::edgefs::PullReply* response,
            ::google::protobuf::Closure* done) override;
  void StreamPull(::google::protobuf::RpcController* controller,
                  const ::edgefs::StreamPullRequest* request,
                  ::edgefs::StreamPullReply* response,
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
  std::string root_path_;
  CenterChunckStreamReceiver stream_receiver_;
};

} // namespace edgefs


#endif