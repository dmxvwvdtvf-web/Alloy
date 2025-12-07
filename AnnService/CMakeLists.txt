# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

set(AnnService ${PROJECT_SOURCE_DIR}/AnnService)
set(Zstd ${PROJECT_SOURCE_DIR}/ThirdParty/zstd)
set(ThreadPool ${PROJECT_SOURCE_DIR}/ThirdParty/thread-pool)

include_directories(${AnnService})
include_directories(${Zstd}/lib)
include_directories(${ThreadPool}/include)

# onnxruntime
set(ONNXRUNTIME_ROOT_DIR ${PROJECT_SOURCE_DIR}/ThirdParty/onnxruntime-linux-x64-1.22.0)
if(NOT EXISTS "${ONNXRUNTIME_ROOT_DIR}/lib/libonnxruntime.so")
    message(FATAL_ERROR "ONNX Runtime library not found at ${ONNXRUNTIME_ROOT_DIR}/lib/libonnxruntime.so")
endif()
if(NOT EXISTS "${ONNXRUNTIME_ROOT_DIR}/include/onnxruntime_cxx_api.h")
    message(FATAL_ERROR "ONNX Runtime header not found at ${ONNXRUNTIME_ROOT_DIR}/include/onnxruntime_cxx_api.h")
endif()
add_library(onnxruntime_lib SHARED IMPORTED GLOBAL)
set_target_properties(onnxruntime_lib PROPERTIES
    IMPORTED_LOCATION "${ONNXRUNTIME_ROOT_DIR}/lib/libonnxruntime.so"
)
# 将头文件目录设为 onnxruntime_lib 的 INTERFACE 属性
target_include_directories(onnxruntime_lib INTERFACE "${ONNXRUNTIME_ROOT_DIR}/include")

file(GLOB_RECURSE HDR_FILES ${AnnService}/inc/Core/*.h ${AnnService}/inc/Helper/*.h)
file(GLOB_RECURSE SRC_FILES ${AnnService}/src/Core/*.cpp ${AnnService}/src/Helper/*.cpp)

list(REMOVE_ITEM HDR_FILES
    ${AnnService}/inc/Core/Common/DistanceUtils.h
    ${AnnService}/inc/Core/Common/SIMDUtils.h
    ${AnnService}/inc/Core/Common/InstructionUtils.h
    ${AnnService}/inc/Core/Common/CommonUtils.h
)

list(REMOVE_ITEM SRC_FILES
    ${AnnService}/src/Core/Common/DistanceUtils.cpp
    ${AnnService}/src/Core/Common/SIMDUtils.cpp
    ${AnnService}/src/Core/Common/InstructionUtils.cpp
)

add_library(DistanceUtils STATIC
    inc/Core/Common/DistanceUtils.h
    inc/Core/Common/SIMDUtils.h
    inc/Core/Common/InstructionUtils.h
    inc/Core/Common/CommonUtils.h
    src/Core/Common/DistanceUtils.cpp
    src/Core/Common/SIMDUtils.cpp
    src/Core/Common/InstructionUtils.cpp
)

if(${CMAKE_CXX_COMPILER_ID} STREQUAL "GNU")
    target_compile_options(DistanceUtils PRIVATE -mavx2 -mavx -msse -msse2 -mavx512f -mavx512bw -mavx512dq -fPIC)
endif()

add_library(SPTAGLib SHARED ${SRC_FILES} ${HDR_FILES})
target_link_libraries(SPTAGLib DistanceUtils concurrentqueue libzstd_shared absl::flat_hash_map ${NUMA_LIBRARY} onnxruntime_lib)
add_library(SPTAGLibStatic STATIC ${SRC_FILES} ${HDR_FILES})
target_link_libraries(SPTAGLibStatic DistanceUtils concurrentqueue libzstd_static absl::flat_hash_map ${NUMA_LIBRARY_STATIC} ${JEMALLOC_LIBRARY} onnxruntime_lib)
if(${CMAKE_CXX_COMPILER_ID} STREQUAL "GNU")
    target_compile_options(SPTAGLibStatic PRIVATE -fPIC)
endif()

install(TARGETS SPTAGLib SPTAGLibStatic
    RUNTIME DESTINATION bin
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib)

if(NOT LIBRARYONLY)
    file(GLOB SERVER_HDR_FILES ${AnnService}/inc/Server/*.h ${AnnService}/inc/Socket/*.h)
    file(GLOB SERVER_FILES ${AnnService}/src/Server/*.cpp ${AnnService}/src/Socket/*.cpp)
    add_executable(server ${SERVER_FILES} ${SERVER_HDR_FILES})
    target_link_libraries(server ${Boost_LIBRARIES} SPTAGLibStatic)

    file(GLOB CLIENT_HDR_FILES ${AnnService}/inc/Client/*.h ${AnnService}/inc/Socket/*.h)
    file(GLOB CLIENT_FILES ${AnnService}/src/Client/*.cpp ${AnnService}/src/Socket/*.cpp)
    add_executable(client ${CLIENT_FILES} ${CLIENT_HDR_FILES})
    target_link_libraries(client ${Boost_LIBRARIES} SPTAGLibStatic)

    file(GLOB AGG_HDR_FILES ${AnnService}/inc/Aggregator/*.h ${AnnService}/inc/Socket/*.h ${AnnService}/inc/Server/QueryParser.h)
    file(GLOB AGG_FILES ${AnnService}/src/Aggregator/*.cpp ${AnnService}/src/Socket/*.cpp ${AnnService}/src/Server/QueryParser.cpp)
    add_executable(aggregator ${AGG_FILES} ${AGG_HDR_FILES})
    target_link_libraries(aggregator ${Boost_LIBRARIES} SPTAGLibStatic)

    file(GLOB BUILDER_FILES ${AnnService}/src/IndexBuilder/*.cpp)
    add_executable(indexbuilder ${BUILDER_FILES})
    target_link_libraries(indexbuilder ${Boost_LIBRARIES} SPTAGLibStatic)

    file(GLOB SEARCHER_FILES ${AnnService}/src/IndexSearcher/*.cpp)
    add_executable(indexsearcher ${SEARCHER_FILES})
    target_link_libraries(indexsearcher ${Boost_LIBRARIES} SPTAGLibStatic)

    file(GLOB QUANTIZER_HDR_FILES ${AnnService}/inc/Quantizer/*.h)
    file(GLOB QUANTIZER_FILES ${AnnService}/src/Quantizer/*.cpp)
    add_executable(quantizer ${QUANTIZER_FILES} ${QUANTIZER_HDR_FILES})
    target_link_libraries(quantizer ${Boost_LIBRARIES} SPTAGLibStatic)

    install(TARGETS server client aggregator indexbuilder indexsearcher quantizer
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)
endif()

find_package(MPI)
if(MPI_FOUND)
    message(STATUS "Found MPI.")
    message(STATUS "MPI Include Path: ${MPI_CXX_INCLUDE_PATH}")
    message(STATUS "MPI Libraries: ${MPI_CXX_LIBRARIES}")
    include_directories(${MPI_CXX_INCLUDE_PATH})
    file(GLOB PARTITION_FILES ${AnnService}/src/BalancedDataPartition/*.cpp)
    add_executable(balanceddatapartition ${PARTITION_FILES})
    target_link_libraries(balanceddatapartition SPTAGLibStatic ${Boost_LIBRARIES} ${MPI_CXX_LIBRARIES})
endif()

# Original ssdserving executable
file(GLOB_RECURSE SSD_SERVING_HDR_FILES ${AnnService}/inc/SSDServing/*.h)
file(GLOB_RECURSE SSD_SERVING_FILES ${AnnService}/src/SSDServing/*.cpp)
add_executable(ssdserving ${SSD_SERVING_HDR_FILES} ${SSD_SERVING_FILES})
target_link_libraries(ssdserving SPTAGLibStatic ${Boost_LIBRARIES})
target_compile_definitions(ssdserving PRIVATE _exe)
# for Test
add_library(ssdservingLib ${SSD_SERVING_HDR_FILES} ${SSD_SERVING_FILES})
target_link_libraries(ssdservingLib SPTAGLibStatic ${Boost_LIBRARIES})


# seperation index part

# proto c++ generate
find_package(Protobuf REQUIRED)
protobuf_generate_cpp(PROTO_SRC PROTO_HEADER ${AnnService}/protos/search_pass.proto)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
message(STATUS "proto output: ${PROTO_SRC}")
message(STATUS "proto output: ${PROTO_HEADER}")

# find dependencies
set(BRPC_INSTALL_DIR ${PROJECT_SOURCE_DIR}/ThirdParty/brpc_install)
list(PREPEND CMAKE_PREFIX_PATH ${BRPC_INSTALL_DIR})

# brpc
find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)
find_library(BRPC_LIB NAMES libbrpc.a brpc)
if((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
    message(FATAL_ERROR "Fail to find brpc")
endif()
include_directories(${BRPC_INCLUDE_PATH})
message(STATUS "Using bRPC: ${BRPC_LIB}")

# gflags, brpc relies
find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES gflags libgflags)
if((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIBRARY))
    message(FATAL_ERROR "Fail to find gflags")
endif()
include_directories(${GFLAGS_INCLUDE_PATH})

# leveldb, brpc relies
find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
find_library(LEVELDB_LIB NAMES leveldb)
if((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
    message(FATAL_ERROR "Fail to find leveldb")
endif()
include_directories(${LEVELDB_INCLUDE_PATH})

# openssl, brpc relies
find_package(OpenSSL)
include_directories(${OPENSSL_INCLUDE_DIR})

# brpc needs these dynamic libs
set(DYNAMIC_LIB
    ${CMAKE_THREAD_LIBS_INIT}
    ${GFLAGS_LIBRARY}
    ${PROTOBUF_LIBRARIES}
    ${LEVELDB_LIB}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${OPENSSL_SSL_LIBRARY}
    dl
)

file(GLOB_RECURSE MEMORY_INDEX_CLIENT_FILES ${AnnService}/src/MemoryIndexClient/*.cpp)
file(GLOB_RECURSE SSD_INDEX_SERVER_FILES ${AnnService}/src/SSDIndexServer/*.cpp)

add_executable(MemoryIndexClient
    ${SSD_SERVING_HDR_FILES} ${MEMORY_INDEX_CLIENT_FILES}
    ${PROTO_SRC} ${PROTO_HEADER}
)
add_executable(SSDIndexServer
    ${SSD_SERVING_HDR_FILES} ${SSD_INDEX_SERVER_FILES}
    ${PROTO_SRC} ${PROTO_HEADER}
)

target_link_libraries(MemoryIndexClient SPTAGLibStatic ${BRPC_LIB} ${DYNAMIC_LIB})
target_link_libraries(SSDIndexServer SPTAGLibStatic ${BRPC_LIB} ${DYNAMIC_LIB})

install(TARGETS ssdservingLib ssdserving SSDIndexServer MemoryIndexClient
    RUNTIME DESTINATION bin
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)
