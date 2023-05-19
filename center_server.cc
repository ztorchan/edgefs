#include <brpc/server.h>
#include <brpc/channel.h>
#include <butil/logging.h>

#include "edgefs/centerrpc.h"

int main() {
  brpc::Server center_server;
  edgefs::CenterServiceImpl center_service;
  if(center_server.AddService(&center_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Failed to start Center Service";
    exit(-1);
  }

  butil::EndPoint listen_ep(butil::IP_ANY, 2333);
  brpc::ServerOptions options;
  if(center_server.Start(listen_ep, &options) != 0) {
    LOG(ERROR) << "Fail to start CenterRPC";
    exit(-1);
  } else {
    LOG(INFO) << "CenterRPC start";
  }
  center_server.RunUntilAskedToQuit();
  LOG(INFO) << "CenterRPC stop";
}