// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#pragma once

#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

#include "BS_thread_pool.hpp"
#include "inc/Core/Common.h"
#include "inc/Core/Common/QueryResultSet.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/SSDServing/SSDIndex.h"
#include "inc/SSDServing/Utils.h"
#include "Rpc.h"

namespace SPTAG {
namespace SSDServing {
namespace SSDIndex {

struct SSDSearchRequest {
    int qid = 0;
    QueryResult* result{nullptr};
    SPANN::SearchStats stats{};
    // nprobe: searchInternalResultNum
    // topk:
    SSDSearchRequest(int p_qid, QueryResult* p_result)
        : qid(p_qid), result(p_result) {}
};

template <typename ValueType>
void SSD_Index_Search(SPANN::Index<ValueType>* p_index) {
    SPANN::Options* opts = p_index->GetOptions();
    if (opts == nullptr) {
        throw std::runtime_error("cannot get index options");
    }

    int numThreads = opts->m_iSSDNumberOfThreads;
    int internalResultNum = opts->m_searchInternalResultNum;
    int K = opts->m_resultNum;
    int truthK = (opts->m_truthResultNum <= 0) ? K : opts->m_truthResultNum;

    LOG_INFO("Start loading QuerySet...\n");
    std::shared_ptr<Helper::ReaderOptions> queryOptions(
        new Helper::ReaderOptions(opts->m_valueType, opts->m_dim,
                                  opts->m_queryType, opts->m_queryDelimiter));
    auto queryReader = Helper::VectorSetReader::CreateInstance(queryOptions);
    if (ErrorCode::Success != queryReader->LoadFile(opts->m_queryPath)) {
        throw std::runtime_error("Failed to read query file.");
    }
    auto querySet = queryReader->GetVectorSet();
    int numQueries = querySet->Count();

    std::vector<QueryResult> results(
        numQueries, QueryResult(NULL, max(K, internalResultNum), false));
    std::vector<SPANN::SearchStats> stats(numQueries);
    for (int i = 0; i < numQueries; ++i) {
        (*((COMMON::QueryResultSet<ValueType>*)&results[i]))
            .SetTarget(reinterpret_cast<ValueType*>(querySet->GetVector(i)),
                       p_index->m_pQuantizer);
        results[i].Reset();
    }

    {
        auto debug_file_name =
            "queryInternalResult" + opts->m_indexDirectory + "_nprobe512.txt";
        auto resultPath =
            std::filesystem::path(opts->m_indexDirectory) / debug_file_name;
        SimpleIOHelper sio(resultPath.string(), SimpleIOHelper::ReadOnly);
        for (int i = 0; i < numQueries; ++i) {
            sio.f_seek_pos(i * 512 * (sizeof(SPANNVID) + sizeof(float)));
            for (int j = 0; j < max(K, internalResultNum); ++j) {
                auto res = results[i].GetResult(j);
                sio.f_read_data(&res->VID, sizeof(res->VID));
                sio.f_read_data(&res->Dist, sizeof(res->Dist));
            }
        }
    }

    BS::thread_pool pool(numThreads);
    std::queue<SSDSearchRequest> finished_queue;
    std::mutex queue_mutex;

    Utils::StopW sw;
    size_t querySent = 0;
    LOG_INFO("Start ANN Search...\n");
    auto f_collect_result = [&]() {
        std::lock_guard l(queue_mutex);
        while (not finished_queue.empty()) {
            auto request = finished_queue.front();
            finished_queue.pop();
            auto qid = request.qid;
            stats[qid] = request.stats;
            results[qid] = *request.result;
            delete request.result;
        }
    };
    while (true) {
        if (querySent == numQueries) {
            break;
            // TODO: terminate
        }

        // check result queue
        f_collect_result();

        {  // get input and submit
            Utils::StopW threadws;
            auto result = new QueryResult(results[querySent]);
            SSDSearchRequest request(querySent, result);
            querySent++;
            pool.detach_task([p_index, request, threadws, &finished_queue,
                              &queue_mutex]() mutable {
                auto startTime = threadws.getElapsedMs();
                p_index->SearchDiskIndex(*request.result, &request.stats);
                auto endTime = threadws.getElapsedMs();

                request.stats.m_exLatency = endTime - startTime;
                request.stats.m_totalSearchLatency = endTime;
                {
                    std::lock_guard l(queue_mutex);
                    finished_queue.push(request);
                }
            });
        }
    }
    pool.wait();
    f_collect_result();
    double sendingCost = sw.getElapsedSec();

    LOG_INFO("Finish ANN Search...\n");

    LOG_INFO(
        "Finish sending in %.3lf seconds, actuallQPS is %.2lf, query "
        "count %u.\n",
        sendingCost, numQueries / sendingCost,
        static_cast<uint32_t>(numQueries));

    // for (int j = 0; j < K; j++) {
    //     LOG_INFO("%d %f\n", results[0].GetResult(j)->VID,
    //              results[0].GetResult(j)->Dist);
    // }

    float recall = 0, MRR = 0;
    std::vector<std::set<SPANNVID>> truth;

    std::string truthFile = opts->m_truthPath;
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
                                    opts->m_truthType);
        char tmp[4];
        if (ptr->ReadBinary(4, tmp) == 4) {
            LOG_ERROR("Truth number is larger than query number(%d)!\n",
                      numQueries);
        }

        recall = COMMON::TruthSet::CalculateRecall<ValueType>(
            (p_index->GetMemoryIndex()).get(), results, truth, K, truthK,
            nullptr, nullptr, numQueries, nullptr, false, &MRR);
        LOG_INFO("Recall%d@%d: %f MRR@%d: %f\n", truthK, K, recall, K, MRR);
    }

    LOG_INFO("\n");
    LOG_INFO("Ex Elements Count:\n");
    PrintPercentiles<double, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> double {
            return ss.m_totalListElementsCount;
        },
        "%.3lf");

    LOG_INFO("\n");
    LOG_INFO("Task Wait Time Distribution:\n");
    PrintPercentiles<double, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> double {
            return ss.m_totalSearchLatency - ss.m_exLatency;
        },
        "%.3lf");

    LOG_INFO("\n");
    LOG_INFO("Ex Latency Distribution:\n");
    PrintPercentiles<double, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> double { return ss.m_exLatency; },
        "%.3lf");

    LOG_INFO("\n");
    LOG_INFO("Total Latency Distribution:\n");
    PrintPercentiles<double, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> double {
            return ss.m_totalSearchLatency;
        },
        "%.3lf");

    LOG_INFO("\n");
    LOG_INFO("Total Disk Page Access Distribution:\n");
    PrintPercentiles<int, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> int {
            return ss.m_diskAccessCount;
        },
        "%4d");

    LOG_INFO("\n");
    LOG_INFO("Total Disk IO Distribution:\n");
    PrintPercentiles<int, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> int { return ss.m_diskIOCount; },
        "%4d");

    std::string outputFile = opts->m_searchResult;
    if (!outputFile.empty()) {
        LOG_INFO("Start output to %s\n", outputFile.c_str());
        OutputResult<ValueType>(outputFile, results, K);
    }

    LOG_INFO("\n");
    LOG_INFO("Recall@%d: %f MRR@%d: %f\n", K, recall, K, MRR);
}
}  // namespace SSDIndex
}  // namespace SSDServing
}  // namespace SPTAG
