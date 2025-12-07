

#pragma once

#include <omp.h>

#include <algorithm>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <memory>
#include <stdexcept>

#include "absl/container/flat_hash_set.h"
#include "inc/Core/Common.h"
#include "inc/Core/Common/TruthSet.h"
#include "inc/Core/SearchQuery.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Helper/DiskIO.h"
#include "inc/Helper/StringConvert.h"
#include "inc/SSDServing/Utils.h"
#include "tqdm.hpp"

namespace SPTAG {
namespace SPMerge {

template <typename ValueType>
inline void FeedModelCore(SPANN::Index<ValueType>* p_index,
                          QueryResult& p_query, const std::set<SizeType>& truth,
                          std::vector<double>& feature, uint8_t& freq_best) {
    const auto& option = *(p_index->GetOptions());
    const auto nprobe = option.m_searchInternalResultNum;
    const auto& cluster_content = p_index->GetMergerResult().cluster_content;

    // search
    p_index->GetMemoryIndex()->SearchIndex(p_query);

    // train data
    std::vector<SPMerge::VecFreqInfo> vec_info_storage;
    std::vector<double> len_list;
    absl::flat_hash_map<SPMerge::VectorID, int> vec_info;
    p_index->CollectInfo(p_query, vec_info_storage, vec_info, len_list);
    p_index->GetFeature(vec_info_storage, len_list, feature);

    // truth vectors from ex-index
    absl::flat_hash_set<int> ex_truth;
    for (auto [vid, _] : vec_info) {
        if (truth.count(vid)) {
            ex_truth.insert(vid);
        }
    }

    // decrease from max-1
    int freq_limit = 0;
    for (const auto& info : vec_info_storage) {
        if (info.freq > freq_limit) {
            freq_limit = info.freq;
        }
    }
    freq_limit -= 1;

    std::vector<bool> send_flag(nprobe, false);
    // search for freq best
    while (true) {
        if (freq_limit < 0) {
            throw std::runtime_error("unexpected freq_limit < 0");
        }
        std::fill(send_flag.begin(), send_flag.end(), false);

        // selected clusters
        for (const auto& info : vec_info_storage) {
            if (info.freq > freq_limit) {
                send_flag[info.first_idx] = true;
            }
        }

        // recall
        absl::flat_hash_set<int> local_truth;
        for (int i = 0; i < nprobe; i++) {
            if (not send_flag[i]) continue;
            const auto vid_list = cluster_content.at(p_query.GetResult(i)->VID);
            for (auto vid : vid_list) {
                if (ex_truth.contains(vid)) {
                    local_truth.insert(vid);
                }
            }
        }
        if (local_truth.size() == ex_truth.size()) {
            freq_best = freq_limit;
            break;
        }

        freq_limit -= 1;
    }
}

template <typename ValueType>
void FeedModelBatch(SPANN::Index<ValueType>* p_index) {
    auto& option = *(p_index->GetOptions());

    // check params
    int nprobe_start = option.m_nprobeRange[0];
    int nprobe_stop = option.m_nprobeRange[1];
    int nprobe_step = option.m_nprobeRange[2];
    int nprobe_count = (nprobe_stop - nprobe_start) / nprobe_step + 1;
    LOG_INFO("read nprobe range: [%d, %d], step=%d, count=%d\n", nprobe_start,
             nprobe_stop, nprobe_step, nprobe_count);

    LOG_INFO("Loading QuerySet...\n");
    std::shared_ptr<Helper::ReaderOptions> queryOptions(
        new Helper::ReaderOptions(option.m_valueType, option.m_dim,
                                  option.m_queryType, option.m_queryDelimiter));
    auto queryReader = Helper::VectorSetReader::CreateInstance(queryOptions);
    if (ErrorCode::Success != queryReader->LoadFile(option.m_queryPath)) {
        LOG_ERROR("Failed to read query file.\n");
        exit(1);
    }
    auto querySet = queryReader->GetVectorSet();
    int numQueries = querySet->Count();

    LOG_INFO("Loading truth set...\n");
    std::vector<std::set<SizeType>> truth;
    {
        int originalK;
        SimpleIOHelper truth_io(option.m_truthPath, SimpleIOHelper::ReadOnly);
        COMMON::TruthSet::LoadTruth(truth_io.m_ptr, truth, numQueries,
                                    originalK, option.m_resultNum,
                                    option.m_truthType);
        LOG_INFO("TruthSet K=%d, cut to %d\n", originalK, option.m_resultNum);
    }
    if (truth.size() != numQueries) {
        LOG_ERROR("Loaded %d queries, but %d truth\n", numQueries,
                  truth.size());
        throw std::logic_error("should check truth and query file");
    }

    // data bufffer
    const auto N_FEATURES = p_index->GetSPMergeFeatureDim();
    const auto FEATURE_COUNT = numQueries * nprobe_count * N_FEATURES;
    const auto FREQ_COUNT = numQueries * nprobe_count;
    std::unique_ptr<double[]> feature_buffer(new double[FEATURE_COUNT]);
    std::unique_ptr<uint8_t[]> freq_buffer(new uint8_t[FREQ_COUNT]);

    // search result
    std::vector<QueryResult> results(
        numQueries,
        QueryResult(nullptr, max(option.m_resultNum, nprobe_stop), false));
    for (int i = 0; i < numQueries; ++i) {
        results[i].SetTarget(querySet->GetVector(i));
        results[i].Reset();
    }

    SSDServing::Utils::StopW sw;
    int nprobe_idx = 0;

    auto bar = tq::trange(nprobe_count);
    bar.set_prefix("calculating model data");
    for (int nprobe = nprobe_start; nprobe <= nprobe_stop;
         nprobe += nprobe_step) {
        // LOG_INFO("Searching with nprobe %d...\n", nprobe);
        option.m_searchInternalResultNum = nprobe;

#pragma omp parallel for
        for (int qid = 0; qid < numQueries; qid++) {
            auto& result = results[qid];
            result.SetResultNum(nprobe);

            std::vector<double> feature;
            uint8_t freq_best;
            FeedModelCore(p_index, result, truth[qid], feature, freq_best);

            // write buffer
            auto feature_ptr = feature_buffer.get() +
                               qid * nprobe_count * N_FEATURES +
                               nprobe_idx * N_FEATURES;
            memcpy(feature_ptr, feature.data(), N_FEATURES * sizeof(double));
            freq_buffer[qid * nprobe_count + nprobe_idx] = freq_best;
            result.Reset();
        }

        nprobe_idx++;
        bar.update();
    }
    results.clear();
    bar.manually_set_progress(1);
    bar.update();
    LOG_INFO("Searching elapesed %.f seconds\n", sw.getElapsedSec());

    // output
    auto dataset = std::filesystem::path(option.m_indexDirectory).filename();
    char buf[64];
    sprintf(
        buf, "%s-nprobe[%d-%d-%d]-q%d-f%d-err[%s]-", dataset.c_str(),
        nprobe_start, nprobe_stop, nprobe_step, numQueries,
        p_index->GetSPMergeFeatureDim(),
        Helper::Convert::ConvertToString(option.m_errorMachineList).c_str());
    auto prefix = std::string(buf);
    auto feature_filename = prefix + "feature.bin";
    auto freq_filename = prefix + "freq.bin";

    SimpleIOHelper feature_io(feature_filename, SimpleIOHelper::WriteOnly,
                              true);
    SimpleIOHelper freq_io(freq_filename, SimpleIOHelper::WriteOnly, true);

    feature_io.f_write_data(feature_buffer.get(),
                            FEATURE_COUNT * sizeof(double));
    freq_io.f_write_data(freq_buffer.get(), FREQ_COUNT * sizeof(uint8_t));

    LOG_INFO("✓ %s\n", feature_filename.c_str());
    LOG_INFO("✓ %s\n", freq_filename.c_str());
}
}  // namespace SPMerge
}  // namespace SPTAG