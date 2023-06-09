cmake_minimum_required(VERSION 3.16.0)
project(edgefs CXX)

set(CMAKE_CXX_STANDARD 17)

# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -D_FILE_OFFSET_BITS=64")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -D_FILE_OFFSET_BITS=64")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()
if(NOT CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX /usr/local)
endif()

include(FindProtobuf)
file(GLOB_RECURSE PROTO_FILE_LIST ${CMAKE_SOURCE_DIR}/protos/*.proto)
set(PROTO_OUT_DIR ${CMAKE_SOURCE_DIR}/include/edgefs/rpc/)
foreach(PROTO_FILE ${PROTO_FILE_LIST})
  get_filename_component(FILE_NAME ${PROTO_FILE} NAME_WE)
  list(APPEND PROTO_CXX "${CMAKE_SOURCE_DIR}/include/mapreduce/rpc/${FILE_NAME}.pb.cc")
  list(APPEND PROTO_H "${CMAKE_SOURCE_DIR}/include/mapreduce/rpc/${FILE_NAME}.pb.h")
  execute_process(
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} -I${CMAKE_SOURCE_DIR}/protos/ --cpp_out=${PROTO_OUT_DIR} ${PROTO_FILE}
  )
endforeach()

include_directories(${CMAKE_SOURCE_DIR}/include/)
include_directories(${CMAKE_SOURCE_DIR}/third-party/rapidjson/include/)
include_directories(${CMAKE_SOURCE_DIR}/third-party/spdlog/include/)

find_package(gflags REQUIRED)
find_package(OpenSSL)
find_package(Threads)
find_library(BRPC_LIB NAMES libbrpc.a brpc)
find_library(LEVELDB_LIB NAMES leveldb)

set(FS_SRCS
  ${CMAKE_SOURCE_DIR}/include/edgefs/option.h
  ${CMAKE_SOURCE_DIR}/src/mm.cc
  ${CMAKE_SOURCE_DIR}/src/bitmap.cc
  ${CMAKE_SOURCE_DIR}/src/edgefs.cc
  ${CMAKE_SOURCE_DIR}/edgefs_main.cc
)

set(CENTER_SERVER_SRCS
  ${CMAKE_SOURCE_DIR}/center_server.cc
)

set(EDGE_RPC_SRCS
  ${CMAKE_SOURCE_DIR}/include/edgefs/rpc/edge_service.pb.h
  ${CMAKE_SOURCE_DIR}/include/edgefs/rpc/edge_service.pb.cc
  ${CMAKE_SOURCE_DIR}/src/edgerpc.cc
)

set(CENTER_RPC_SRCS
  ${CMAKE_SOURCE_DIR}/include/edgefs/rpc/center_service.pb.h
  ${CMAKE_SOURCE_DIR}/include/edgefs/rpc/center_service.pb.cc
  ${CMAKE_SOURCE_DIR}/src/centerrpc.cc
)

set(LIBS 
  ${BRPC_LIB}
  ${PROTOBUF_LIBRARIES}
  ${OPENSSL_SSL_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${LEVELDB_LIB}
  ${CMAKE_THREAD_LIBS_INIT}
  gflags
  dl
  stdc++fs
  fuse3
)

add_library(edgerpc STATIC ${EDGE_RPC_SRCS})
add_library(centerrpc STATIC ${CENTER_RPC_SRCS})

add_executable(edgefs ${FS_SRCS})
target_link_libraries(edgefs centerrpc edgerpc ${LIBS})

add_executable(centerserver ${CENTER_SERVER_SRCS})
target_link_libraries(centerserver centerrpc ${LIBS})

# add_executable(test test.cc)
# target_link_libraries(test ${LIBS})