// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#pragma once
#include <limits>

#include "inc/Core/Common.h"
#include "inc/Core/Common/DistanceUtils.h"
#include "inc/Core/Common/QueryResultSet.h"
#include "inc/Core/SPANN/ExtraFullGraphSearcher.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/VectorSetReader.h"
#include "inc/SSDServing/Utils.h"

namespace SPTAG {
namespace SSDServing {
namespace SSDIndex {

template <typename ValueType>
ErrorCode OutputResult(const std::string& p_output,
                       std::vector<QueryResult>& p_results, int p_resultNum) {
    if (!p_output.empty()) {
        auto ptr = f_createIO();
        if (ptr == nullptr ||
            !ptr->Initialize(p_output.c_str(),
                             std::ios::binary | std::ios::out)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed create file: %s\n",
                         p_output.c_str());
            return ErrorCode::FailedCreateFile;
        }
        int32_t i32Val = static_cast<int32_t>(p_results.size());
        if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(
                                                 &i32Val)) != sizeof(i32Val)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Fail to write result file!\n");
            return ErrorCode::DiskIOFail;
        }
        i32Val = p_resultNum;
        if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(
                                                 &i32Val)) != sizeof(i32Val)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Fail to write result file!\n");
            return ErrorCode::DiskIOFail;
        }

        float fVal = 0;
        for (size_t i = 0; i < p_results.size(); ++i) {
            for (int j = 0; j < p_resultNum; ++j) {
                i32Val = p_results[i].GetResult(j)->VID;
                if (ptr->WriteBinary(sizeof(i32Val),
                                     reinterpret_cast<char*>(&i32Val)) !=
                    sizeof(i32Val)) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                                 "Fail to write result file!\n");
                    return ErrorCode::DiskIOFail;
                }

                fVal = p_results[i].GetResult(j)->Dist;
                if (ptr->WriteBinary(sizeof(fVal),
                                     reinterpret_cast<char*>(&fVal)) !=
                    sizeof(fVal)) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                                 "Fail to write result file!\n");
                    return ErrorCode::DiskIOFail;
                }
            }
        }
    }
    return ErrorCode::Success;
}

template <typename T, typename V>
void PrintPercentiles(const std::vector<V>& p_values,
                      std::function<T(const V&)> p_get, const char* p_format) {
    double sum = 0;
    std::vector<T> collects;
    collects.reserve(p_values.size());
    for (const auto& v : p_values) {
        T tmp = p_get(v);
        sum += tmp;
        collects.push_back(tmp);
    }

    std::sort(collects.begin(), collects.end());

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                 "Avg\t50tiles\t90tiles\t95tiles\t99tiles\t99.9tiles\tMax\n");

    std::string formatStr("%.3lf");
    for (int i = 1; i < 7; ++i) {
        formatStr += '\t';
        formatStr += p_format;
    }

    formatStr += '\n';

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, formatStr.c_str(),
                 sum / collects.size(),
                 collects[static_cast<size_t>(collects.size() * 0.50)],
                 collects[static_cast<size_t>(collects.size() * 0.90)],
                 collects[static_cast<size_t>(collects.size() * 0.95)],
                 collects[static_cast<size_t>(collects.size() * 0.99)],
                 collects[static_cast<size_t>(collects.size() * 0.999)],
                 collects[static_cast<size_t>(collects.size() - 1)]);
}

template <typename ValueType>
void SearchSequential(SPANN::Index<ValueType>* p_index, int p_numThreads,
                      std::vector<QueryResult>& p_results,
                      std::vector<SPANN::SearchStats>& p_stats,
                      int p_maxQueryCount, int p_internalResultNum) {
    int numQueries = min(static_cast<int>(p_results.size()), p_maxQueryCount);

    std::atomic_size_t queriesSent(0);

    std::vector<std::thread> threads;
    threads.reserve(p_numThreads);
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                 "Searching: numThread: %d, numQueries: %d.\n", p_numThreads,
                 numQueries);

    Utils::StopW sw;

    for (int i = 0; i < p_numThreads; i++) {
        threads.emplace_back([&, i]() {
            NumaStrategy ns =
                (p_index->GetDiskIndex() != nullptr)
                    ? NumaStrategy::SCATTER
                    : NumaStrategy::LOCAL;  // Only for SPANN, we need to avoid
                                            // IO threads overlap with search
                                            // threads.
            Helper::SetThreadAffinity(i, threads[i], ns, OrderStrategy::ASC);

            Utils::StopW threadws;
            size_t index = 0;
            while (true) {
                index = queriesSent.fetch_add(1);
                if (index < numQueries) {
                    if (index and index % (uint64_t)(0.1 * numQueries) == 0) {
                        auto ratio = 100 * index / numQueries;
                        LOG_INFO("%d%% queries finished\n", ratio);
                    }

                    double startTime = threadws.getElapsedMs();
                    p_index->GetMemoryIndex()->SearchIndex(p_results[index]);
                    double endTime = threadws.getElapsedMs();
                    p_index->SearchDiskIndex(p_results[index],
                                             &(p_stats[index]));
                    double exEndTime = threadws.getElapsedMs();

                    p_stats[index].m_exLatency = exEndTime - endTime;
                    p_stats[index].m_totalSearchLatency = exEndTime - startTime;
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

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                 "Finish sending in %.3lf seconds, actuallQPS is %.2lf, query "
                 "count %u.\n",
                 sendingCost, numQueries / sendingCost,
                 static_cast<uint32_t>(numQueries));

    for (int i = 0; i < numQueries; i++) {
        p_results[i].CleanQuantizedTarget();
    }
}

template <typename ValueType>
void Search(SPANN::Index<ValueType>* p_index) {
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
            p_opts.m_loopNum * warmupNumQueries,
            QueryResult(NULL, max(K, internalResultNum), false));
        std::vector<SPANN::SearchStats> warmpUpStats(p_opts.m_loopNum *
                                                     warmupNumQueries);
        for (int i = 0; i < p_opts.m_loopNum * warmupNumQueries; ++i) {
            (*((COMMON::QueryResultSet<ValueType>*)&warmupResults[i]))
                .SetTarget(reinterpret_cast<ValueType*>(
                               warmupQuerySet->GetVector(i % warmupNumQueries)),
                           p_index->m_pQuantizer);
            warmupResults[i].Reset();
        }

        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start warmup...\n");
        SearchSequential(p_index, numThreads, warmupResults, warmpUpStats,
                         p_opts.m_queryCountLimit, internalResultNum);
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Finish warmup...\n");
    }

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start loading QuerySet...\n");
    std::shared_ptr<Helper::ReaderOptions> queryOptions(
        new Helper::ReaderOptions(p_opts.m_valueType, p_opts.m_dim,
                                  p_opts.m_queryType, p_opts.m_queryDelimiter));
    auto queryReader = Helper::VectorSetReader::CreateInstance(queryOptions);
    if (ErrorCode::Success != queryReader->LoadFile(p_opts.m_queryPath)) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                     "Failed to read query file.\n");
        exit(1);
    }
    auto querySet = queryReader->GetVectorSet();
    int numQueries = querySet->Count();

    // FIXME:做测试
    // numQueries = 100;
    std::vector<QueryResult> results(
        p_opts.m_loopNum * numQueries,
        QueryResult(NULL, max(K, internalResultNum), false));
    std::vector<SPANN::SearchStats> stats(p_opts.m_loopNum * numQueries,
                                          p_opts.m_numMachine);
    // {  // FIXME: temporary data
    //     if (p_opts.m_searchPruning) {
    //         SimpleIOHelper sio(
    //             "/home/bosen/workspace/SPMerge/SPANN-Merge/DegradedRead/data/"
    //             "sift1b-r8-[416-513-4]-diffnprobe-predict-0.9-with1nodeclush."
    //             "bin",
    //             SimpleIOHelper::ReadOnly);

    //         const uint start = 416, stop = 513, step = 4;
    //         const uint idx = (p_opts.m_searchInternalResultNum - start) /
    //         step; const uint cnt = (stop - start) / step + 1; if
    //         ((p_opts.m_searchInternalResultNum - start) % step != 0) {
    //             LOG_ERROR("nprobe=%u, start=%u, step=%u\n",
    //                       p_opts.m_searchInternalResultNum, start, step);
    //             throw std::invalid_argument("");
    //         }

    //         std::vector<std::vector<uint8_t>> external_freq_predicted(
    //             numQueries);
    //         for (auto& freq_vec : external_freq_predicted) {
    //             freq_vec.resize(cnt);
    //             sio.f_read_data(freq_vec.data(), sizeof(uint8_t) * cnt);
    //         };
    //         for (int i = 0; i < numQueries; ++i) {
    //             stats[i].m_freq = external_freq_predicted[i][idx];
    //         }
    //     }
    // }

    // {
    //     std::string filename =
    //         "/home/bosen/workspace/SPMerge/SPANN-Merge/DegradedRead/data/"
    //         "nprobe416_cidlist.bin";
    //     std::vector<std::vector<int32_t>> total_cid_list;
    //     std::ifstream file(filename, std::ios::binary);

    //     if (!file) {
    //         std::cerr << "无法打开文件: " << filename << std::endl;
    //     }

    //     // 读取集合数量
    //     int32_t set_count;
    //     file.read(reinterpret_cast<char*>(&set_count), sizeof(set_count));

    //     for (int i = 0; i < set_count; i++) {
    //         // 读取当前集合的元素数量
    //         int32_t element_count;
    //         file.read(reinterpret_cast<char*>(&element_count),
    //                   sizeof(element_count));

    //         std::vector<int32_t> current_set;

    //         // 读取当前集合的所有元素
    //         for (int j = 0; j < element_count; j++) {
    //             int32_t element;
    //             file.read(reinterpret_cast<char*>(&element),
    //             sizeof(element)); current_set.push_back(element);
    //         }

    //         total_cid_list.push_back(current_set);
    //     }
    //     for (int i = 0; i < numQueries; ++i) {
    //         stats[i].m_cid_list = total_cid_list[i];
    //     }
    // }

    for (int i = 0; i < p_opts.m_loopNum * numQueries; ++i) {
        (*((COMMON::QueryResultSet<ValueType>*)&results[i]))
            .SetTarget(reinterpret_cast<ValueType*>(
                           querySet->GetVector(i % numQueries)),
                       p_index->m_pQuantizer);
        results[i].Reset();
    }
    // {
    //     auto opts = &p_opts;
    //     auto debug_file_name =
    //         "queryInternalResult" + opts->m_indexDirectory +
    //         "_nprobe512.txt";
    //     auto resultPath =
    //         std::filesystem::path(opts->m_indexDirectory) / debug_file_name;
    //     SimpleIOHelper sio(resultPath.string(), SimpleIOHelper::ReadOnly);
    //     for (int i = 0; i < numQueries; ++i) {
    //         sio.f_seek_pos(i * 512 * (sizeof(SizeType) + sizeof(float)));
    //         for (int j = 0; j < max(K, internalResultNum); ++j) {
    //             auto res = results[i].GetResult(j);
    //             sio.f_read_data(&res->VID, sizeof(res->VID));
    //             sio.f_read_data(&res->Dist, sizeof(res->Dist));
    //         }
    //     }
    // }

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start ANN Search...\n");

    SearchSequential(p_index, numThreads, results, stats,
                     p_opts.m_queryCountLimit, internalResultNum);
    {
        std::string load_balance_output_file = "load-balance.txt";
        std::ofstream ofs(load_balance_output_file);
        for (const auto& stat : stats) {
            for (auto cnt : stat.m_fileAcessedCnt) {
                ofs << cnt << "\t";
            }
            ofs << std::endl;
        }
        LOG_INFO("load balance trace outputed to file %s\n",
                 load_balance_output_file.c_str());
    }

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Finish ANN Search...\n");

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
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Load VectorSet(%d,%d).\n",
                         vectorSet->Count(), vectorSet->Dimension());
        }
    }

    if (p_opts.m_rerank > 0 && vectorSet != nullptr) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, " Begin rerank...\n");
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
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start loading TruthFile...\n");

        auto ptr = f_createIO();
        if (ptr == nullptr ||
            !ptr->Initialize(truthFile.c_str(),
                             std::ios::in | std::ios::binary)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Failed open truth file: %s\n", truthFile.c_str());
            exit(1);
        }
        int originalK = truthK;
        COMMON::TruthSet::LoadTruth(ptr, truth, numQueries, originalK, truthK,
                                    p_opts.m_truthType);
        char tmp[4];
        if (ptr->ReadBinary(4, tmp) == 4) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Truth number is larger than query number(%d)!\n",
                         numQueries);
        }

        recall = COMMON::TruthSet::CalculateRecall<ValueType>(
            (p_index->GetMemoryIndex()).get(), results, truth, K, truthK,
            querySet, vectorSet, numQueries, nullptr, false, &MRR);
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Recall%d@%d: %f MRR@%d: %f\n",
                     truthK, K, recall, K, MRR);
    }

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\n");
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Ex Elements Count:\n");
    PrintPercentiles<double, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> double {
            return ss.m_totalListElementsCount;
        },
        "%.3lf");

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\n");
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Head Latency Distribution:\n");
    PrintPercentiles<double, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> double {
            return ss.m_totalSearchLatency - ss.m_exLatency;
        },
        "%.3lf");

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\n");
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Ex Latency Distribution:\n");
    PrintPercentiles<double, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> double { return ss.m_exLatency; },
        "%.3lf");

    {
        for (int i = 0; i < stats.size(); i++) {
            Helper::EventTimer timer;
            timer.merge_from_other(stats[i].m_pruningTimer);
            auto pruningtime =
                timer.as_ms(timer.get_duration("0. prune clusters")) * 1.0;
            stats[i].m_purningLatency = pruningtime;
        }
    }

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\n");
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Pruning Latency Distribution:\n");
    PrintPercentiles<double, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> double {
            return ss.m_purningLatency;
        },
        "%.3lf");

    {
        Helper::EventTimer timer;
        for (const auto& s : stats) {
            timer.merge_from_other(s.m_pruningTimer);
        }
        for (auto& [k, v] : timer.get_timePoints_()) {
            v.duration /= numQueries;
        }
        timer.log_all_event_duration(true);
    }

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\n");
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Total Latency Distribution:\n");
    PrintPercentiles<double, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> double {
            return ss.m_totalSearchLatency;
        },
        "%.3lf");

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\n");
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                 "Total Disk Page Access Distribution:\n");
    PrintPercentiles<int, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> int {
            return ss.m_diskAccessCount;
        },
        "%4d");

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\n");
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Total Disk IO Distribution:\n");
    PrintPercentiles<int, SPANN::SearchStats>(
        stats,
        [](const SPANN::SearchStats& ss) -> int { return ss.m_diskIOCount; },
        "%4d");

    // SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "");

    if (!outputFile.empty()) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start output to %s\n",
                     outputFile.c_str());
        OutputResult<ValueType>(outputFile, results, K);
    }

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "\n");
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Recall@%d: %f MRR@%d: %f\n", K,
                 recall, K, MRR);

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "");

    if (p_opts.m_recall_analysis) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start recall analysis...\n");

        std::shared_ptr<VectorIndex> headIndex = p_index->GetMemoryIndex();
        SizeType sampleSize = numQueries < 100 ? numQueries : 100;
        SizeType sampleK = headIndex->GetNumSamples() < 1000
                               ? headIndex->GetNumSamples()
                               : 1000;
        float sampleE = 1e-6f;

        std::vector<SizeType> samples(sampleSize, 0);
        std::vector<float> queryHeadRecalls(sampleSize, 0);
        std::vector<float> truthRecalls(sampleSize, 0);
        std::vector<int> shouldSelect(sampleSize, 0);
        std::vector<int> shouldSelectLong(sampleSize, 0);
        std::vector<int> nearQueryHeads(sampleSize, 0);
        std::vector<int> annNotFound(sampleSize, 0);
        std::vector<int> rngRule(sampleSize, 0);
        std::vector<int> postingCut(sampleSize, 0);
        for (int i = 0; i < sampleSize; i++)
            samples[i] = COMMON::Utils::rand(numQueries);

#pragma omp parallel for schedule(dynamic)
        for (int i = 0; i < sampleSize; i++) {
            COMMON::QueryResultSet<ValueType> queryANNHeads(
                (const ValueType*)(querySet->GetVector(samples[i])),
                max(K, internalResultNum));
            headIndex->SearchIndex(queryANNHeads);
            float queryANNHeadsLongestDist =
                queryANNHeads.GetResult(internalResultNum - 1)->Dist;

            COMMON::QueryResultSet<ValueType> queryBFHeads(
                (const ValueType*)(querySet->GetVector(samples[i])),
                max(sampleK, internalResultNum));
            for (SizeType y = 0; y < headIndex->GetNumSamples(); y++) {
                float dist = headIndex->ComputeDistance(
                    queryBFHeads.GetQuantizedTarget(), headIndex->GetSample(y));
                queryBFHeads.AddPoint(y, dist);
            }
            queryBFHeads.SortResult();

            {
                std::vector<bool> visited(internalResultNum, false);
                for (SizeType y = 0; y < internalResultNum; y++) {
                    for (SizeType z = 0; z < internalResultNum; z++) {
                        if (visited[z]) continue;

                        if (fabs(queryANNHeads.GetResult(z)->Dist -
                                 queryBFHeads.GetResult(y)->Dist) < sampleE) {
                            queryHeadRecalls[i] += 1;
                            visited[z] = true;
                            break;
                        }
                    }
                }
            }

            std::map<int, std::set<int>> tmpFound;  // headID->truths
            p_index->DebugSearchDiskIndex(queryBFHeads, internalResultNum,
                                          sampleK, nullptr, &truth[samples[i]],
                                          &tmpFound);

            for (SizeType z = 0; z < K; z++) {
                truthRecalls[i] +=
                    truth[samples[i]].count(queryBFHeads.GetResult(z)->VID);
            }

            for (SizeType z = 0; z < K; z++) {
                truth[samples[i]].erase(results[samples[i]].GetResult(z)->VID);
            }

            for (std::map<int, std::set<int>>::iterator it = tmpFound.begin();
                 it != tmpFound.end(); it++) {
                float q2truthposting =
                    headIndex->ComputeDistance(querySet->GetVector(samples[i]),
                                               headIndex->GetSample(it->first));
                for (auto vid : it->second) {
                    if (!truth[samples[i]].count(vid)) continue;

                    if (q2truthposting < queryANNHeadsLongestDist)
                        shouldSelect[i] += 1;
                    else {
                        shouldSelectLong[i] += 1;

                        std::set<int> nearQuerySelectedHeads;
                        float v2vhead = headIndex->ComputeDistance(
                            vectorSet->GetVector(vid),
                            headIndex->GetSample(it->first));
                        for (SizeType z = 0; z < internalResultNum; z++) {
                            if (queryANNHeads.GetResult(z)->VID < 0) break;
                            float v2qhead = headIndex->ComputeDistance(
                                vectorSet->GetVector(vid),
                                headIndex->GetSample(
                                    queryANNHeads.GetResult(z)->VID));
                            if (v2qhead < v2vhead) {
                                nearQuerySelectedHeads.insert(
                                    queryANNHeads.GetResult(z)->VID);
                            }
                        }
                        if (nearQuerySelectedHeads.size() == 0) continue;

                        nearQueryHeads[i] += 1;

                        COMMON::QueryResultSet<ValueType> annTruthHead(
                            (const ValueType*)(vectorSet->GetVector(vid)),
                            p_opts.m_debugBuildInternalResultNum);
                        headIndex->SearchIndex(annTruthHead);

                        bool found = false;
                        for (SizeType z = 0; z < annTruthHead.GetResultNum();
                             z++) {
                            if (nearQuerySelectedHeads.count(
                                    annTruthHead.GetResult(z)->VID)) {
                                found = true;
                                break;
                            }
                        }

                        if (!found) {
                            annNotFound[i] += 1;
                            continue;
                        }

                        // RNG rule and posting cut
                        std::set<int> replicas;
                        for (SizeType z = 0;
                             z < annTruthHead.GetResultNum() &&
                             replicas.size() < p_opts.m_replicaCount;
                             z++) {
                            BasicResult* item = annTruthHead.GetResult(z);
                            if (item->VID < 0) break;

                            bool good = true;
                            for (auto r : replicas) {
                                if (p_opts.m_rngFactor *
                                        headIndex->ComputeDistance(
                                            headIndex->GetSample(r),
                                            headIndex->GetSample(item->VID)) <
                                    item->Dist) {
                                    good = false;
                                    break;
                                }
                            }
                            if (good) replicas.insert(item->VID);
                        }

                        found = false;
                        for (auto r : nearQuerySelectedHeads) {
                            if (replicas.count(r)) {
                                found = true;
                                break;
                            }
                        }

                        if (found)
                            postingCut[i] += 1;
                        else
                            rngRule[i] += 1;
                    }
                }
            }
        }
        float headacc = 0, truthacc = 0, shorter = 0, longer = 0, lost = 0,
              buildNearQueryHeads = 0, buildAnnNotFound = 0, buildRNGRule = 0,
              buildPostingCut = 0;
        for (int i = 0; i < sampleSize; i++) {
            headacc += queryHeadRecalls[i];
            truthacc += truthRecalls[i];

            lost += shouldSelect[i] + shouldSelectLong[i];
            shorter += shouldSelect[i];
            longer += shouldSelectLong[i];

            buildNearQueryHeads += nearQueryHeads[i];
            buildAnnNotFound += annNotFound[i];
            buildRNGRule += rngRule[i];
            buildPostingCut += postingCut[i];
        }

        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Query head recall @%d:%f.\n",
                     internalResultNum,
                     headacc / sampleSize / internalResultNum);
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "BF top %d postings truth recall @%d:%f.\n", sampleK,
                     truthK, truthacc / sampleSize / truthK);

        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "Percent of truths in postings have shorter distance than "
                     "query selected heads: %f percent\n",
                     shorter / lost * 100);
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "Percent of truths in postings have longer distance than "
                     "query selected heads: %f percent\n",
                     longer / lost * 100);

        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "\tPercent of truths no shorter distance in query "
                     "selected heads: %f percent\n",
                     (longer - buildNearQueryHeads) / lost * 100);
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "\tPercent of truths exists shorter distance in query "
                     "selected heads: %f percent\n",
                     buildNearQueryHeads / lost * 100);

        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "\t\tRNG rule ANN search loss: %f percent\n",
                     buildAnnNotFound / lost * 100);
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "\t\tPosting cut loss: %f percent\n",
                     buildPostingCut / lost * 100);
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "\t\tRNG rule loss: %f percent\n",
                     buildRNGRule / lost * 100);
    }
}
}  // namespace SSDIndex
}  // namespace SSDServing
}  // namespace SPTAG
