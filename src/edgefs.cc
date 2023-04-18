#include <cstdlib>
#include <glog/logging.h>
#include <brpc/server.h>

#include "edgefs/edgefs.h"
#include "edgefs/rpc/center_service.pb.h"

#define EDGEFS_PORT 2333

namespace edgefs
{

std::thread* EdgeFS::rpc_thread_ = nullptr;
std::thread* EdgeFS::gc_thread_ = nullptr;
std::thread* EdgeFS::pull_thread_ = nullptr;
struct dentry* EdgeFS::root_dentry_ = nullptr;
std::string EdgeFS::center_ipv4_ = std::string();

void EdgeFS::Init() {

}

void EdgeFS::RPC() {
  brpc::Server edge_server;
  EdgeServiceImpl edge_service("/edge", "/edgedata");
  if(edge_server.AddService(&edge_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Fail to add service";
    exit(-1);
  }
  
  butil::EndPoint point = butil::EndPoint(butil::IP_ANY, EDGEFS_PORT);
  brpc::ServerOptions options;
  if(edge_server.Start(point, &options) != 0) {
    LOG(ERROR) << "Fail to start EdgeRPC";
    exit(-1);
  } else {
    LOG(INFO) << "EdgeRPC Start.";
  }
  edge_server.RunUntilAskedToQuit();
  LOG(INFO) << "EdgeRPC Stop";
}

} // namespace edgefs
