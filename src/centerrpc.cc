#include <cstdlib>
#include <cstdio>
#include <cstring>
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
  spdlog::info("[pull] path: {}, startchunck: {}, chuncknum: {}, chuncksize: {}", request->pr_path(), request->start_chunck(), request->chuncks_num(), request->chunck_size());
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);
  std::string real_path = root_path_ + request->pr_path();
  response->set_ok(false);

  struct stat st;
  if(stat(real_path.c_str(), &st) != 0 || st.st_mtim.tv_sec >= request->pr_time()) {
    spdlog::info("[streampull] failed to find such file");
    return;
  }
  if(!S_ISREG(st.st_mode)) {
    spdlog::info("[streampull] file is not a regular file");
    return;
  }
  if(st.st_mtim.tv_sec >= request->pr_time()) {
    spdlog::info("[streampull] file is newer than request");
    return;
  }

  FILE* f = fopen(real_path.c_str(), "rb");
  char* buf = new char[request->chunck_size()];
  if(f == NULL) {
    spdlog::info("[pull] failed to open");
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
        response->set_ok(true);
        PullReply_Chunck* new_chunck = response->add_chuncks();
        new_chunck->set_chunck_no(chunck_no);
        new_chunck->set_data(std::string(buf, read_size));
      }
    }
  }

  return;
}

void CenterServiceImpl::StreamPull(::google::protobuf::RpcController* controller,
                                   const ::edgefs::StreamPullRequest* request,
                                   ::edgefs::StreamPullReply* response,
                                   ::google::protobuf::Closure* done) {
  spdlog::info("[streampull] start a stream pull, path: {}, chuncksize: {}", request->pr_path(), request->chunck_size());
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);
  std::string real_path = root_path_ + request->pr_path();
  response->set_ok(false);

  struct stat st;
  if(stat(real_path.c_str(), &st) != 0 || st.st_mtim.tv_sec >= request->pr_time()) {
    spdlog::info("[streampull] failed to find such file");
    return;
  }
  if(!S_ISREG(st.st_mode)) {
    spdlog::info("[streampull] file is not a regular file");
    return;
  }
  if(st.st_mtim.tv_sec >= request->pr_time()) {
    spdlog::info("[streampull] file is newer than request");
    return;
  }

  brpc::StreamOptions stream_options;
  brpc::StreamId* sd = new brpc::StreamId;
  stream_options.max_buf_size = 0;
  stream_options.handler = &stream_receiver_;
  if(brpc::StreamAccept(sd, *ctl, &stream_options) != 0) {
    spdlog::info("[streampull] failed to accept stream");
    return ;
  }
  stream_receiver_.add_stream(sd, request->pr_path(), st, request->chunck_size());
  response->set_ok(true);
  return ;
}

void CenterServiceImpl::Stat(::google::protobuf::RpcController* controller,
                             const ::edgefs::StatRequest* request,
                             ::edgefs::StatReply* response,
                             ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);
  std::string real_path = root_path_ + request->path();

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
  std::string real_path = root_path_ + request->path();

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

int CenterChunckStreamReceiver::on_received_messages(brpc::StreamId id, 
                                                     butil::IOBuf *const messages[], 
                                                     size_t size) {
  spdlog::info("[streampull] start pull file {}", sd_ctxs_[id]->fpath);
  std::string real_path = root_path_ + sd_ctxs_[id]->fpath;            // get path
  uint64_t chuncksize = sd_ctxs_[id]->chuncksize;
  struct stat st = sd_ctxs_[id]->st;
  FILE* f = fopen(real_path.c_str(), "rb");
    if(f == NULL) {
      spdlog::info("[streampull] open file failed");
      return 0;
  }
  uint64_t end_chunck_no = st.st_size / chuncksize;
  if(st.st_size % chuncksize == 0) {
    // compute last chunck no for special situation
    end_chunck_no--;
  }

  char* buf = new char[chuncksize];
  memset(buf, 0, chuncksize);
  for(size_t i = 0; i < size && !sd_ctxs_[id]->finish; i++) {
    butil::IOBuf* cur_msg = messages[i];
    uint64_t chunck_no = UINT64_MAX;
    cur_msg->cutn(&chunck_no, sizeof(uint64_t));                      // get chunck no
    if(chunck_no == UINT64_MAX) {
      spdlog::info("[streampull] finish pull");
      butil::IOBuf response;
      response.append(&chunck_no, sizeof(uint64_t));
      brpc::StreamWrite(id, response);
      sd_ctxs_[id]->finish = true;
      break;
    }

    fseek(f, chunck_no * chuncksize, SEEK_SET);
    uint64_t read_size = fread(buf, 1, chuncksize, f);
    if((chunck_no != end_chunck_no && read_size != chuncksize) 
        || (chunck_no == end_chunck_no && read_size != st.st_size % chuncksize)) {
      // read error
      spdlog::info("[streampull] failed to read chunck {}", chunck_no);
    } else {
      spdlog::info("[streampull] successfully read chunck {} {} bytes", chunck_no, read_size);
      butil::IOBuf response;
      response.append(&chunck_no, sizeof(uint64_t));
      response.append(buf, chuncksize);
      brpc::StreamWrite(id, response);
    }
  }
  return 0;
}

void CenterChunckStreamReceiver::on_idle_timeout(brpc::StreamId id) {
  spdlog::info("[streampull] waiting");
}

void CenterChunckStreamReceiver::on_closed(brpc::StreamId id) {
  spdlog::info("[streampull] close stream");
  close_stream(id);
}

} // namespace edgefs