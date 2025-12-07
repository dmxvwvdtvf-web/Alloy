// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/SSDServing/main.h"

#include <chrono>
#include <iostream>

#include "BS_thread_pool.hpp"
#include "inc/Core/Common.h"
#include "inc/Core/Common/TruthSet.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/SSDServing/Rpc.h"
#include "inc/SSDServing/SSDIndex.h"
#include "inc/SSDServing/SSDIndexServer.h"
#include "inc/SSDServing/Utils.h"

using namespace SPTAG;
using namespace SPTAG::SPANN;
using namespace SPTAG::SSDServing;
using namespace SPTAG::SSDServing::SSDIndex;

int main(int argc, char* argv[]) {
    if (argc < 2) throw std::invalid_argument("need configFilePath");

    // parse some param from command, check DEFINE_xxx in brpc!
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    std::map<std::string, std::map<std::string, std::string>> config_map;

    RPC_Method rpc_method = RPC_Method::Callback;
    int num_threads = -1, port = 50051;
    VectorValueType value_type;
    {
        Helper::IniReader iniReader;
        iniReader.LoadIniFile(argv[1]);
        config_map[SEC_BASE] = iniReader.GetParameters(SEC_BASE);
        config_map[SEC_SELECT_HEAD] = iniReader.GetParameters(SEC_SELECT_HEAD);
        config_map[SEC_BUILD_HEAD] = iniReader.GetParameters(SEC_BUILD_HEAD);
        config_map[SEC_BUILD_SSD_INDEX] =
            iniReader.GetParameters(SEC_BUILD_SSD_INDEX);
        config_map[SEC_REDUNDANCY_MERGER] =
            iniReader.GetParameters(SEC_REDUNDANCY_MERGER);
        config_map[SEC_SEPERATED_INDEX] =
            iniReader.GetParameters(SEC_SEPERATED_INDEX);

        value_type = iniReader.GetParameter(SEC_BASE, "ValueType", value_type);
        num_threads = iniReader.GetParameter(SEC_BUILD_SSD_INDEX,
                                             "numberofthreads", num_threads);
        rpc_method = iniReader.GetParameter(SEC_SEPERATED_INDEX, "rpcmethod",
                                            rpc_method);
        port = iniReader.GetParameter(SEC_SEPERATED_INDEX, "port", port);
    }

    std::shared_ptr<VectorIndex> index =
        VectorIndex::CreateInstance(IndexAlgoType::SPANN, value_type);
    if (index == nullptr) {
        throw std::runtime_error("cannot create index!");
    }

    for (auto& sectionKV : config_map) {
        for (auto& KV : sectionKV.second) {
            index->SetParameter(KV.first, KV.second, sectionKV.first);
        }
    }

    index->LoadSSDIndex();

    std::unique_ptr<pass_search_data::RANNService> service = nullptr;
    std::unique_ptr<BS::light_thread_pool> pool = nullptr;

#define DefineVectorValueType(Name, Type)                                     \
    if (value_type == VectorValueType::Name) {                                \
        auto index_ptr = static_cast<SPANN::Index<Type>*>(index.get());       \
        switch (rpc_method) {                                                 \
            case RPC_Method::Callback:                                        \
                pool.reset(new BS::thread_pool(num_threads));                 \
                service.reset(new SearchCallbackImpl(index_ptr, pool.get())); \
                /* TODO: control #polling threads */                          \
                break;                                                        \
            case RPC_Method::Sync:                                            \
            case RPC_Method::ASync:                                           \
                NOT_IMPLEMENTED();                                            \
                break;                                                        \
            default:                                                          \
                throw std::invalid_argument("unknown rpc method");            \
        }                                                                     \
        /* SSD_Index_Search(index_ptr); */                                    \
    }
#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

    brpc::Server server;
    if (server.AddService(service.get(), brpc::SERVER_DOESNT_OWN_SERVICE) !=
        0) {
        throw std::runtime_error("failed to add service");
    }

    LOG_INFO("Server using RPC API: %s\n",
             Helper::Convert::ConvertToString(rpc_method).c_str());
    std::atomic_bool stop_flag = false;
    if (pool != nullptr) {
        pool->detach_task([&stop_flag, pool = pool.get()]() {
            while (not stop_flag.load()) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                auto nt = pool->get_thread_count();
                auto mt = pool->get_tasks_running();
                auto usage = 100. * mt / nt;
                LOG_INFO(
                    "Callback Service: threadpool=%lu, max_running=%lu, "
                    "usage=%.2f%%\n",
                    nt, mt, usage);
            }
        });
    }

    if (server.Start(port, nullptr) != 0) {
        throw std::runtime_error("failed to start server");
    }
    LOG_INFO("Server listening on prot %d with #threads=%d\n", port,
             num_threads);
    server.RunUntilAskedToQuit();
    stop_flag.store(true);
    return 0;
}
