#include <cstdlib>
#include <cstdio>
#include <sys/stat.h>
#include <unistd.h>
#include <string>

#include <brpc/channel.h>
#include <glog/logging.h>

#include "edgefs/centerrpc.h"

namespace edgefs
{

void CenterServiceImpl::PULL(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                        const ::edgefs::PullRequest* request,
                        ::edgefs::PullReply* response,
                        ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);
  response->set_ok(false);

  LOG(INFO) << "Pull file: " << request->pr_path() 
            << ", from chunck " << request->start_chunck() 
            << " to " << (request->start_chunck() + request->chuncks_num() - 1)
            << " with size " << request->chunck_size();

  struct stat st;
  if(stat(request->pr_path().c_str(), &st) != 0 || st.st_mtim >= request->pr_time()) {
    LOG(INFO) << "Stat failed";
    return;
  }

  FILE* f = fopen(request->pr_path().c_str(), "rb");
  char* buf = new char[request->chunck_size()];
  if(f == NULL) {
    LOG(INFO) << "Open file failed";
    return;
  } else {
    uint64_t end_chunck_no = st.st_size / request->chunck_size();
    if(st.st_size % request->chunck_size() == 0) {
      // compute last chunck no for special situation
      end_chunck_no--;
    }
    for(uint64_t chunck_no = request->start_chunck(); chunck_no < (request->start_chunck() + request->chuncks_num()); chunck_no++) {
      if(chunck_no > end_chunck_no) {
        break;
      }
      memset(buf, 0, request->chunck_size());
      fseek(f, chunck_no * request->chunck_size(), SEEK_SET);
      uint64_t read_size = fread(buf, 1, request->chunck_size(), f);
      if((chunck_no != end_chunck_no && read_size != request->chunck_size()) 
         || (chunck_no == end_chunck_no && read_size != st.st_size % request->chunck_size())) {
        // read error
        LOG(INFO) << "Read chunck " << chunck_no << " failed";
      } else {
        LOG(INFO) << "Read chunck " << chunck_no << " " << read_size << " bytes";
        response->set_ok(true);
        PullReply_Chunck* new_chunck = response->add_chuncks();
        new_chunck->set_chunck_no(chunck_no);
        new_chunck->set_data(std::string(buf));
      }
    }
  }

  return;
}

void CenterServiceImpl::Stat(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                             const ::edgefs::StatRequest* request,
                             ::edgefs::StatReply* response,
                             ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  LOG(INFO) << "Stat file: " << request->pr_path();
  struct stat st;
  if(stat(request->pr_path().c_str(), &st) == 0) {
    LOG(INFO) << "Stat successfully";
    response->set_ok(true);
    response->set_len(st.st_size);
    response->set_mtime(st.st_mtim);
  } else {
    LOG(INFO) << "Stat failed"
    response->set_ok(false);
  }

  return ;
}

} // namespace edgefs