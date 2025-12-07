// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#pragma once
#include <onnxruntime_cxx_api.h>

#include <atomic>
#include <chrono>
#include <climits>
#include <cstdio>
#include <fstream>
#include <limits>
#include <memory>
#include <thread>

#include "inc/Core/Common.h"
#include "inc/Core/Common/DistanceUtils.h"
#include "inc/Core/Common/QueryResultSet.h"
#include "inc/Core/SearchQuery.h"
#include "inc/Core/SPANN/ExtraFullGraphSearcher.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Helper/DiskIO.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/SSDServing/SSDIndex.h"
#include "inc/SSDServing/Utils.h"
#include "Rpc.h"
#include "search_pass.pb.h"

namespace SPTAG {
namespace SSDServing {
namespace SSDIndex {

template <typename ValueType>
void Memory_Index_SearchSequential(SPANN::Index<ValueType>* p_index,
                                   int p_numThreads,
                                   std::vector<QueryResult>& p_results,
                                   std::vector<SPANN::SearchStats>& p_stats,
                                   int p_maxQueryCount,
                                   int p_internalResultNum) {
    const auto& option = *(p_index->GetOptions());
    int numQueries = min(static_cast<int>(p_results.size()), p_maxQueryCount);

    std::vector<std::unique_ptr<Ort::Env>> m_pEnv_list;
    std::vector<std::unique_ptr<Ort::SessionOptions>> m_pSessionOptions_list;
    std::vector<std::unique_ptr<Ort::Session>> m_pSession_list;

    m_pEnv_list.reserve(p_numThreads);
    m_pSessionOptions_list.reserve(p_numThreads);
    m_pSession_list.reserve(p_numThreads);

    if (option.m_searchPruning) {
        for (int i = 0; i < p_numThreads; i++) {
            const std::string modelPath = option.m_modelPath;
            if (modelPath == "") {
                LOG_ERROR(
                    "Model path is empty, please set the model path in "
                    "the options.\n");
                throw std::runtime_error("Model path is empty.");
            }

            m_pEnv_list.push_back(std::make_unique<Ort::Env>(
                ORT_LOGGING_LEVEL_WARNING, "ONNX_Inference"));

            auto session_options = std::make_unique<Ort::SessionOptions>();
            session_options->SetIntraOpNumThreads(1);
            session_options->SetInterOpNumThreads(1);
            session_options->SetGraphOptimizationLevel(
                GraphOptimizationLevel::ORT_ENABLE_ALL);

            // 3. 在构造 Session 时，需要解引用(dereference)智能指针
            // 使用 * 来获取指针指向的对象本身
            m_pSession_list.push_back(std::make_unique<Ort::Session>(
                *m_pEnv_list[i], modelPath.c_str(), *session_options));

            // 将 session_options 移动到列表中以保持所有权
            m_pSessionOptions_list.push_back(std::move(session_options));
        }
    }

    std::vector<QueryResult> internal_results = p_results;

    std::atomic_size_t queriesSent(0);
    std::atomic_bool exit_flag = false;

    std::vector<std::thread> threads;
    threads.reserve(p_numThreads);

    std::thread t_monitor([&]() {
        if (option.m_monitorOutput == "") return;

        LOG_INFO("QPS Monitor will output to file: %s\n",
                 option.m_monitorOutput.c_str());

        auto nio = std::make_shared<Helper::NativeFileIO>();
        SimpleIOHelper sio(nio, option.m_monitorOutput,
                           SimpleIOHelper::WriteOnly, true);
        size_t record = 0;
        char buf[64];
        while (not exit_flag) {
            auto cur = queriesSent.load();
            int len = snprintf(buf, sizeof(buf), "%zu\n", cur - record);
            record = cur;
            sio.f_write_data(buf, len);
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });

    LOG_INFO("Client using RPC method: %s\n",
             Helper::Convert::ConvertToString(option.m_rpcMethod).c_str());

    LOG_INFO("Client using RPC Way: %s\n",
             Helper::Convert::ConvertToString(option.m_rpcWay).c_str());

    std::unique_ptr<SearchClientBase> search_client = nullptr;

    switch (option.m_rpcMethod) {
        case RPC_Method::Sync:
            search_client.reset(new SearchClientSync(option.m_serverAddrList));
            break;
        case RPC_Method::Callback:
            search_client.reset(new SearchClientASync(option.m_serverAddrList));
            break;
        case RPC_Method::ASync:
            NOTIMPLEMENTED();
            break;
        default:
            throw std::runtime_error("unknown rpc method");
    }
    LOG_INFO("Searching: numThread: %d, numQueries: %d.\n", p_numThreads,
             numQueries);

    Utils::StopW sw;
    for (int i = 0; i < p_numThreads; i++) {
        threads.emplace_back([&, i]() {
            // NumaStrategy ns =
            //     (p_index->GetDiskIndex() != nullptr)
            //         ? NumaStrategy::SCATTER
            //         : NumaStrategy::LOCAL;  // Only for SPANN, we need to
            //         avoid
            //                                 // IO threads overlap with search
            //                                 // threads.
            // Helper::SetThreadAffinity(i, threads[i], ns, OrderStrategy::ASC);

            Utils::StopW threadws;
            size_t index = 0;

            while (true) {
                index = queriesSent.fetch_add(1);
                if (index < numQueries) {
                    if (index and index % (uint64_t)(0.1 * numQueries) == 0) {
                        auto ratio = 100 * index / numQueries;
                        LOG_INFO("%d%% queries finished\n", ratio);
                    }

                    auto stat = &p_stats[index];
                    auto session = m_pSession_list[i].get();
                    double startTime = threadws.getElapsedMs();
                    p_index->GetMemoryIndex()->SearchIndex(
                        internal_results[index]);

                    p_index->PruneClusters(internal_results[index],
                                           p_results[index], *stat, session);
                    double headEndTime = threadws.getElapsedMs();

                    stat->m_headLatency = headEndTime - startTime;

                    auto result =
                        reinterpret_cast<COMMON::QueryResultSet<ValueType>*>(
                            &p_results[index]);
                    auto f_handle_response =
                        [startTime, headEndTime, threadws, index, result, stat,
                         topk = option.m_resultNum](
                            const RPCResultList& response_list,
                            const std::vector<brpc::Controller*>
                                controllers) mutable {
                            auto merge_time_point =
                                std::chrono::system_clock::now();
                            auto merge_time_since_epoch =
                                merge_time_point.time_since_epoch();

                            double merge_timestamp_ms =
                                std::chrono::duration<double, std::milli>(
                                    merge_time_since_epoch)
                                    .count();
                            double mergeStart = threadws.getElapsedMs();
                            if (response_list.empty()) {
                                LOG_ERROR("Received empty response\n");
                                return;
                            }
                            result->SetResultNum(topk);
                            result->Reverse();
                            std::unordered_set<SPANNVID> deduper;

                            double ServerAcceptClientTime = 0;
                            double ServerToClientTime = 0;
                            double ServerProcessTime = 0;
                            double network_latency = 0;
                            double max_response_exlatency = 0;
                            double brpc_exlatency = 0;
                            double cur_ServerProcessTime = 0;
                            double ServerPruningTime = 0;
                            double cur_ServerPruningTime = 0;
                            for (uint i = 0; i < response_list.size(); i++) {
                                auto response = response_list[i];
                                if (response->k() != topk) {
                                    continue;
                                    LOG_ERROR(
                                        "Query %lu, topk=%d, "
                                        "response->k()=%d\n",
                                        index, topk, response->k());
                                }

                                cur_ServerProcessTime =
                                    response->serverprocesstime();
                                cur_ServerPruningTime =
                                    response->serverpruningtime();
                                brpc_exlatency =
                                    controllers[i]->latency_us() / 1000.0;

                                if (brpc_exlatency > max_response_exlatency) {
                                    network_latency =
                                        brpc_exlatency - cur_ServerProcessTime;
                                    ServerProcessTime = cur_ServerProcessTime;
                                    ServerPruningTime = cur_ServerPruningTime;
                                    max_response_exlatency = brpc_exlatency;
                                }
                                if (ServerProcessTime == 0) {
                                    LOG_INFO(
                                        "Index: %d has error,with StubId=%d\n",
                                        index, index % 3);
                                }

                                auto vid_list =
                                    reinterpret_cast<const int32_t*>(
                                        response->vids().data());
                                auto dist_list = reinterpret_cast<const float*>(
                                    response->dists().data());
                                for (int topi = 0; topi < response->k();
                                     topi++) {
                                    if (deduper.find(vid_list[topi]) !=
                                        deduper.end())
                                        continue;
                                    result->AddPoint(vid_list[topi],
                                                     dist_list[topi]);
                                    deduper.insert(vid_list[topi]);
                                }
                            }
                            result->SortResult();

                            double exEndTime = threadws.getElapsedMs();

                            stat->m_clientToServerLatency =
                                ServerAcceptClientTime;
                            stat->m_serverToclientLatency =
                                merge_timestamp_ms - ServerToClientTime;
                            stat->m_serverprocessLatency = ServerProcessTime;
                            stat->m_totalNetworkLatency = network_latency;
                            stat->m_exLatency = exEndTime - headEndTime;
                            stat->m_mergeResultLatency = exEndTime - mergeStart;
                            stat->m_totalSearchLatency = exEndTime - startTime;
                        };
                    search_client->CallSearchService(
                        option, internal_results[index], index, option.m_rpcWay,
                        f_handle_response);

                    // in callback mode, rpc is returned immediately, thus
                    // latency above is right, while in sync mode, latency2 is
                    // right, so we record both and save max value
                    auto searchEndTime = threadws.getElapsedMs();
                    auto searchLatency2 = searchEndTime - startTime;
                    if (searchLatency2 > stat->m_totalSearchLatency) {
                        stat->m_totalSearchLatency = searchLatency2;
                    }
                } else {
                    return;
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    double sendingCost = sw.getElapsedSec();

    exit_flag.store(true);
    t_monitor.join();
    search_client->wait_for_async_done();

    LOG_INFO(
        "Finish sending in %.3lf seconds, actuallQPS is %.2lf, query "
        "count %u.\n",
        sendingCost, numQueries / sendingCost,
        static_cast<uint32_t>(numQueries));

    for (int i = 0; i < numQueries; i++) {
        p_results[i].CleanQuantizedTarget();
    }
}

template <typename ValueType>
void Memory_Index_Search(SPANN::Index<ValueType>* p_index) {
    SPANN::Options& p_opts = *(p_index->GetOptions());
    std::string outputFile = p_opts.m_searchResult;
    std::string truthFile = p_opts.m_truthPath;
    std::string warmupFile = p_opts.m_warmupPath;

    if (p_index->m_pQuantizer) {
        p_index->m_pQuantizer->SetEnableADC(p_opts.m_enableADC);
    }

    if (!p_opts.m_logFile.empty()) {
        SetLogger(std::make_shared<Helper::FileLogger>(
            Helper::LogLevel::LL_Info, p_opts.m_logFile.c_str()));
    }
    int numThreads = p_opts.m_iSSDNumberOfThreads;
    int internalResultNum = p_opts.m_searchInternalResultNum;
    int K = p_opts.m_resultNum;
    int truthK = (p_opts.m_truthResultNum <= 0) ? K : p_opts.m_truthResultNum;
    int loop_num = p_opts.m_loopNum;
    /*
    No warm up now. After implementing the first version, we should add
    warm-up here
    */

    if (!warmupFile.empty()) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "Start loading warmup query set...\n");
        std::shared_ptr<Helper::ReaderOptions> queryOptions(
            new Helper::ReaderOptions(p_opts.m_valueType, p_opts.m_dim,
                                      p_opts.m_warmupType,
                                      p_opts.m_warmupDelimiter));
        auto queryReader =
            Helper::VectorSetReader::CreateInstance(queryOptions);
        if (ErrorCode::Success != queryReader->LoadFile(p_opts.m_warmupPath)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Failed to read query file.\n");
            exit(1);
        }
        auto warmupQuerySet = queryReader->GetVectorSet();
        int warmupNumQueries = warmupQuerySet->Count();

        std::vector<QueryResult> warmupResults(
            warmupNumQueries * loop_num,
            QueryResult(NULL, max(K, internalResultNum), false));
        std::vector<SPANN::SearchStats> warmpUpStats(warmupNumQueries *
                                                     loop_num);
        for (int i = 0; i < warmupNumQueries * loop_num; ++i) {
            (*((COMMON::QueryResultSet<ValueType>*)&warmupResults[i]))
                .SetTarget(reinterpret_cast<ValueType*>(
                               warmupQuerySet->GetVector(i % warmupNumQueries)),
                           p_index->m_pQuantizer);
            warmupResults[i].Reset();
        }

        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start warmup...\n");
        Memory_Index_SearchSequential(p_index, numThreads, warmupResults,
                                      warmpUpStats, p_opts.m_queryCountLimit,
                                      internalResultNum);
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Finish warmup...\n");
    }

    LOG_INFO("Start loading QuerySet...\n");
    std::shared_ptr<Helper::ReaderOptions> queryOptions(
        new Helper::ReaderOptions(p_opts.m_valueType, p_opts.m_dim,
                                  p_opts.m_queryType, p_opts.m_queryDelimiter));
    auto queryReader = Helper::VectorSetReader::CreateInstance(queryOptions);
    if (ErrorCode::Success != queryReader->LoadFile(p_opts.m_queryPath)) {
        LOG_ERROR("Failed to read query file.\n");
        exit(1);
    }

    auto querySet = queryReader->GetVectorSet();
    int numQueries = querySet->Count();
    std::vector<QueryResult> results(
        numQueries * loop_num,
        QueryResult(NULL, max(K, internalResultNum), false));
    std::vector<SPANN::SearchStats> stats(numQueries * loop_num);
    for (int i = 0; i < numQueries * loop_num; ++i) {
        (*((COMMON::QueryResultSet<ValueType>*)&results[i]))
            .SetTarget(reinterpret_cast<ValueType*>(
                           querySet->GetVector(i % numQueries)),
                       p_index->m_pQuantizer);
        results[i].Reset();
    }

    LOG_INFO("Start ANN Search...\n");

    Memory_Index_SearchSequential(p_index, numThreads, results, stats,
                                  p_opts.m_queryCountLimit, internalResultNum);

    LOG_INFO("\n");
    LOG_INFO("Finish ANN Search...\n");

    std::shared_ptr<VectorSet> vectorSet;

    if (!p_opts.m_vectorPath.empty() &&
        fileexists(p_opts.m_vectorPath.c_str())) {
        std::shared_ptr<Helper::ReaderOptions> vectorOptions(
            new Helper::ReaderOptions(p_opts.m_valueType, p_opts.m_dim,
                                      p_opts.m_vectorType,
                                      p_opts.m_vectorDelimiter));
        auto vectorReader =
            Helper::VectorSetReader::CreateInstance(vectorOptions);
        if (ErrorCode::Success == vectorReader->LoadFile(p_opts.m_vectorPath)) {
            vectorSet = vectorReader->GetVectorSet();
            if (p_opts.m_distCalcMethod == DistCalcMethod::Cosine)
                vectorSet->Normalize(numThreads);
            LOG_INFO("\n");
            LOG_INFO("Load VectorSet(%d,%d).\n", vectorSet->Count(),
                     vectorSet->Dimension());
        }
    }

    if (p_opts.m_rerank > 0 && vectorSet != nullptr) {
        LOG_INFO("\n");
        LOG_INFO(" Begin rerank...\n");
        for (int i = 0; i < results.size(); i++) {
            for (int j = 0; j < K; j++) {
                if (results[i].GetResult(j)->VID < 0) continue;
                results[i].GetResult(j)->Dist =
                    COMMON::DistanceUtils::ComputeDistance(
                        (const ValueType*)querySet->GetVector(i),
                        (const ValueType*)vectorSet->GetVector(
                            results[i].GetResult(j)->VID),
                        querySet->Dimension(), p_opts.m_distCalcMethod);
            }
            BasicResult* re = results[i].GetResults();
            std::sort(re, re + K, COMMON::Compare);
        }
        K = p_opts.m_rerank;
    }

    float recall = 0, MRR = 0;
    std::vector<std::set<SizeType>> truth;
    if (!truthFile.empty()) {
        LOG_INFO("Start loading TruthFile...\n");

        auto ptr = f_createIO();
        if (ptr == nullptr ||
            !ptr->Initialize(truthFile.c_str(),
                             std::ios::in | std::ios::binary)) {
            LOG_ERROR("Failed open truth file: %s\n", truthFile.c_str());
            exit(1);
        }
        int originalK = truthK;
        COMMON::TruthSet::LoadTruth(ptr, truth, numQueries, originalK, truthK,
                                    p_opts.m_truthType);
        char tmp[4];
        if (ptr->ReadBinary(4, tmp) == 4) {
            LOG_ERROR("Truth number is larger than query number(%d)!\n",
                      numQueries);
        }

        recall = COMMON::TruthSet::CalculateRecall<ValueType>(
            (p_index->GetMemoryIndex()).get(), results, truth, K, truthK,
            querySet, vectorSet, numQueries, nullptr, false, &MRR);
        LOG_INFO("Recall%d@%d: %f MRR@%d: %f\n", truthK, K, recall, K, MRR);
    }

    LOG_INFO("=========================================\n");
    LOG_INFO("Total Pruning Latency Distribution:\n");
    PrintPercentiles<double, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> double {
            return ss.m_pruning_total;
        },
        "%.3lf");
    if (p_opts.m_searchPruning) {
        LOG_INFO("Collect vector info:\n");
        PrintPercentiles<double, SPANN::SearchStats>(
            stats,
            [](const SPANN::SearchStats& ss) -> double {
                return ss.m_pruning_collect_vecinfo;
            },
            "%.3lf");

        LOG_INFO("Compute feature:\n");
        PrintPercentiles<double, SPANN::SearchStats>(
            stats,
            [](const SPANN::SearchStats& ss) -> double {
                return ss.m_pruning_feature_compute;
            },
            "%.3lf");

        LOG_INFO("Model Predict:\n");
        PrintPercentiles<double, SPANN::SearchStats>(
            stats,
            [](const SPANN::SearchStats& ss) -> double {
                return ss.m_pruning_model_predict;
            },
            "%.3lf");

        LOG_INFO("Pruning:\n");
        PrintPercentiles<double, SPANN::SearchStats>(
            stats,
            [](const SPANN::SearchStats& ss) -> double {
                return ss.m_pruning_pruning;
            },
            "%.3lf");
    }
    LOG_INFO("=========================================\n");

    LOG_INFO("\n");
    LOG_INFO("Total Latency Distribution:\n");
    PrintPercentiles<double, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> double {
            return ss.m_totalSearchLatency;
        },
        "%.3lf");

    LOG_INFO("\n");
    LOG_INFO("Head Latency Distribution:\n");
    PrintPercentiles<double, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> double { return ss.m_headLatency; },
        "%.3lf");

    LOG_INFO("\n");
    LOG_INFO("Ex Latency Distribution:\n");
    PrintPercentiles<double, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> double { return ss.m_exLatency; },
        "%.3lf");

    LOG_INFO("\n");
    LOG_INFO("Latency Breakdown:\n");
    LOG_INFO("Client Merge Result:\n");
    PrintPercentiles<double, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> double {
            return ss.m_mergeResultLatency;
        },
        "%.3lf");

    LOG_INFO("\n");

    LOG_INFO("\n");

    LOG_INFO("Total Server Process Latency:\n");
    PrintPercentiles<double, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> double {
            return ss.m_serverprocessLatency;
        },
        "%.3lf");

    LOG_INFO("\n");

    LOG_INFO("Total Network Latency:\n");
    PrintPercentiles<double, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> double {
            return ss.m_totalNetworkLatency;
        },
        "%.3lf");

    LOG_INFO("\n");

    if (!outputFile.empty()) {
        LOG_INFO("Start output to %s\n", outputFile.c_str());
        OutputResult<ValueType>(outputFile, results, K);
    }
}
}  // namespace SSDIndex
}  // namespace SSDServing
}  // namespace SPTAG
