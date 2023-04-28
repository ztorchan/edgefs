#ifndef _EDGEFS_OPTION_H
#define _EDGEFS_OPTION_H

#include <cstdlib>
#include <string>
#include <fstream>
#include <sstream>
#include <rapidjson/rapidjson.h>
#include <rapidjson/document.h>

namespace edgefs
{
  
class Option {
public:
  Option() {}
  Option(std::string config_path) {
    std::ifstream ifs(config_path);
    std::stringstream buf;
    buf << ifs.rdbuf();
    std::string config_json(buf.str());
    ifs.close();

    rapidjson::Document doc;
    doc.Parse(config_json.c_str());
    assert(doc.IsObject());

    /* Necessary config */
    // Data Center address
    assert(doc.HasMember("center_address"));
    assert(doc["center_address"].IsString());
    center_address = doc["center_address"].GetString();

    /* Optional config */
    // root path
    if(doc.HasMember("root")) {
      assert(doc["root"].IsString());
      root_path = doc["root"].GetString();
    }

    // data root path
    if(doc.HasMember("data_root")) {
      assert(doc["data_root"].IsString());
      data_root_path = doc["data_root"].GetString();
    }

    // edge rpc port
    if(doc.HasMember("rpc_port")) {
      assert(doc["rpc_port"].IsUint());
      rpc_port = doc["rpc_port"].GetUint();
    }

    // file chunck size
    if(doc.HasMember("chunck_size")) {
      assert(doc["chunck_size"].IsUint64());
      chunck_size = doc["chunck_size"].GetUint64();
    } 

    // file cache block size 
    if(doc.HasMember("block_size")) {
      assert(doc["block_size"].IsUint64());
      block_size = doc["block_size"].GetUint64();
    }

    // max size of total block cache
    if(doc.HasMember("max_cache_size")) {
      assert(doc["max_cache_size"].IsUint());
      max_cache_size = doc["max_cache_size"].GetUint();
    }

    // max size of total free cache hold by mm
    if(doc.HasMember("max_free_cache")) {
      assert(doc["max_free_cache"].IsUint64());
      max_free_cache = doc["max_free_cache"].GetUint64();
    }
  }

public:
  // Root path of EdgeFS (Metadata)
  std::string root_path = "/edge";

  // Root path of EdgeFS data (Data)
  std::string data_root_path = "/edgedata";

  // Data center address(ipv4)
  std::string center_address;

  // Edge RPC listen port
  uint32_t rpc_port = 2333;

  // File chunck size (bytes)
  uint64_t chunck_size = 64 * 1024 * 1024;

  // File cache block size (bytes)
  uint64_t block_size = 4 * 1024 * 1024;

  // Max size of total block cache
  uint32_t max_cache_size = 1024 * 1024 * 1024;

  // Max size of total free cache hold by mm
  uint32_t max_free_cache = 128 * 1024 * 1024;

};

} // namespace edgefs


#endif