#include <string>

#include <brpc/channel.h>
#include <butil/logging.h>
#include <spdlog/spdlog.h>

#include "edgefs/bitmap.h"
#include "edgefs/edgefs.h"
#include "edgefs/edgerpc.h"

namespace edgefs
{

void EdgeServiceImpl::Invalid(::google::protobuf::RpcController* controller,
                              const ::edgefs::InvalidFileRequest* request,
                              ::google::protobuf::Empty* response,
                              ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);
  
  spdlog::info("[invalid] try to invalid file {}", request->path());
  std::vector<std::string> d_names;
  EdgeFS::split_path(request->path().c_str(), d_names);
  std::vector<std::unique_ptr<std::shared_lock<std::shared_mutex>>> path_lcks;
  edgefs_dentry* target_dentry = EdgeFS::root_dentry_;
  for(const auto& dname : d_names) {
    path_lcks.push_back(std::make_unique<std::shared_lock<std::shared_mutex>>(target_dentry->d_mtx));
    auto it = target_dentry->d_childs->find(dname);
    if(it == target_dentry->d_childs->end()) {
      // can not find such file
      spdlog::info("[invalid] failed to find target dentry");
      return;
    }
    target_dentry = it->second;
  }

  // set file state
  spdlog::info("[invalid] set file state to INVALID");
  std::unique_lock<std::shared_mutex> target_dentry_lck(target_dentry->d_mtx);
  target_dentry->d_inode->i_state = FileState::INVALID;
  target_dentry_lck.unlock();
  
  // publish a gc request
  spdlog::info("[invalid] publish a gc request");
  std::unique_lock<std::mutex> req_list_lck(EdgeFS::req_mtx_);
  gc_request* new_gcr = new gc_request;
  {
    new_gcr->r_type = RequestType::GC;
    new_gcr->r_time = time(NULL);
    new_gcr->gcr_path = request->path();
    new_gcr->gcr_reason = GCReason::INVAILDFILE;
  }
  EdgeFS::req_list_.push_back(new_gcr);
  EdgeFS::req_cv_.notify_all();

  spdlog::info("[invalid] invalid file successfully");
  return ;
}

int EdgeChunckStreamReceiver::on_received_messages(brpc::StreamId id, 
                                                   butil::IOBuf *const messages[], 
                                                   size_t size) {
  for(size_t i = 0; i < size; i++) {
    butil::IOBuf* cur_msg = messages[i];
    uint64_t chunck_no = UINT64_MAX;
    cur_msg->cutn(&chunck_no, sizeof(uint64_t));
    if(chunck_no == UINT64_MAX) {
      finish_ = true;
      spdlog::info("[pullstream] finish pull");
      break;
    }

    assert(!fi_->i_chunck_bitmap->Get(chunck_no));
    assert(fi_->i_subinodes.find(chunck_no) == fi_->i_subinodes.end());
    spdlog::info("[pullstream] successfully pull chunck {}", chunck_no);
    std::unique_lock<std::shared_mutex> target_den_lck(fi_->i_dentry->d_mtx);
    std::string chunck_file_path = EdgeFS::options_.data_root_path + fpath_ + "/" + std::to_string(chunck_no);
    FILE* f_chunck = fopen(chunck_file_path.c_str(), "wb");
    if(f_chunck == NULL) {
      spdlog::info("[pullstream] failed to create chunck {} file", chunck_no);
      continue;
    }

    std::string data = cur_msg->to_string();
    if(fwrite(data.c_str(), 1, data.size(), f_chunck) == data.size()) {
      edgefs_subinode* new_subi = new edgefs_subinode;
      {
        new_subi->subi_no = chunck_no;
        new_subi->subi_inode = fi_;
        new_subi->subi_ctime = time(NULL);
        new_subi->subi_atime = time(NULL);
        new_subi->subi_acounter = 0;
        new_subi->subi_state = ChunckState::ACTIVE;
        new_subi->subi_blocks = std::map<uint64_t, cacheblock*>();
        uint64_t total_block_num = data.size() / EdgeFS::options_.block_size;
        if(data.size() % EdgeFS::options_.block_size != 0) {
          total_block_num++;
        }
        new_subi->subi_block_bitmap = new BitMap(total_block_num);
      }
      fi_->i_chunck_bitmap->Set(chunck_no);
      fi_->i_subinodes[chunck_no] = new_subi;
    } else {
      spdlog::info("[pullstream] failed to write chunck {} file");
    }
  }
  return 0;
}

void EdgeChunckStreamReceiver::on_idle_timeout(brpc::StreamId id) {
  spdlog::info("[streampull] waiting");
}

void EdgeChunckStreamReceiver::on_closed(brpc::StreamId id) {
  spdlog::info("[streampull] close stream");
  close_ = true;
}

} // namespace edgefs
