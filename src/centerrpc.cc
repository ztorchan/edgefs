#include <cstdlib>
#include <cstdio>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>
#include <string>

#include <spdlog/spdlog.h>
#include <brpc/channel.h>
#include <butil/logging.h>

#include "edgefs/centerrpc.h"

namespace edgefs
{

void CenterServiceImpl::Pull(::google::protobuf::RpcController* controller,
                             const ::edgefs::PullRequest* request,
                             ::edgefs::PullReply* response,
                             ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);
  std::string real_path = basic_path_ + request->pr_path();
  response->set_ok(false);

  spdlog::info("[pull] path: {}, startchunck: {}, chuncknum: {}, chuncksize: {}", real_path, request->start_chunck(), request->chuncks_num(), request->chunck_size());

  struct stat st;
  if(stat(real_path.c_str(), &st) != 0 || st.st_mtim.tv_sec >= request->pr_time()) {
    spdlog::info("[pull] pull failed");
    return;
  }

  FILE* f = fopen(real_path.c_str(), "rb");
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
        spdlog::info("[pull] failed to read chunck {}", chunck_no);
      } else {
        spdlog::info("[pull] successfully read chunck {} {} bytes", chunck_no, read_size);
        LOG(INFO) << "Read chunck " << chunck_no << " " << read_size << " bytes";
        response->set_ok(true);
        PullReply_Chunck* new_chunck = response->add_chuncks();
        new_chunck->set_chunck_no(chunck_no);
        new_chunck->set_data(std::string(buf, read_size));
      }
    }
  }

  return;
}

void CenterServiceImpl::Stat(::google::protobuf::RpcController* controller,
                             const ::edgefs::StatRequest* request,
                             ::edgefs::StatReply* response,
                             ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);
  std::string real_path = basic_path_ + request->path();

  spdlog::info("[stat] path: {}", real_path);
  struct stat st;
  if(stat(real_path.c_str(), &st) == 0) {
    spdlog::info("[stat] stat successfully");
    response->set_ok(true);
    response->mutable_st()->set_len(st.st_size);
    response->mutable_st()->set_atime(st.st_atime);
    response->mutable_st()->set_ctime(st.st_ctime);
    response->mutable_st()->set_mtime(st.st_mtime);
    response->mutable_st()->set_mode(st.st_mode);
    response->mutable_st()->set_nlink(st.st_nlink);
  } else {
    spdlog::info("[stat] failed to stat");
    response->set_ok(false);
  }

  return ;
}

void CenterServiceImpl::Readdir(::google::protobuf::RpcController* controller,
                                const ::edgefs::ReaddirRequest* request,
                                ::edgefs::ReaddirReply* response,
                                ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);
  std::string real_path = basic_path_ + request->path();

  spdlog::info("[readdir] path: {}", real_path.c_str());

  response->set_ok(false);
  DIR* dir = opendir(real_path.c_str());
  if(dir == NULL) {
    spdlog::info("[readdir] failed to open directory");
    return ;
  }
  struct dirent* den;
  while((den = readdir(dir)) != NULL) {
    struct stat st;
    std::string den_path = real_path + "/" + den->d_name;
    spdlog::info("[readdir] stat path {}", den_path.c_str());
    if(stat(den_path.c_str(), &st) != 0) {
      spdlog::info("[readdir] failed to stat path {}", den_path.c_str());
      continue;
    }
    auto new_st_with_name = response->add_sts_with_name();
    new_st_with_name->set_name(den->d_name);
    new_st_with_name->mutable_st()->set_atime(st.st_atime);
    new_st_with_name->mutable_st()->set_ctime(st.st_ctime);
    new_st_with_name->mutable_st()->set_len(st.st_size);
    new_st_with_name->mutable_st()->set_mode(st.st_mode);
    new_st_with_name->mutable_st()->set_mtime(st.st_mtime);
    new_st_with_name->mutable_st()->set_nlink(st.st_nlink);
    response->set_ok(true);
  }

  return;
}

} // namespace edgefs