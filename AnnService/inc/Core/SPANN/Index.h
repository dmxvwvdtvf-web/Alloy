// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SPANN_INDEX_H_
#define _SPTAG_SPANN_INDEX_H_

#include <sys/sendfile.h>

#include <atomic>
#include <boost/mpl/not.hpp>
#include <cstdint>
#include <functional>
#include <future>
#include <unordered_map>

#include "absl/container/flat_hash_map.h"
#include "blockingconcurrentqueue.h"
#include "IExtraSearcher.h"
#include "inc/Core/Common.h"
#include "inc/Core/Common/BKTree.h"
#include "inc/Core/Common/CommonUtils.h"
#include "inc/Core/Common/DistanceUtils.h"
#include "inc/Core/Common/IQuantizer.h"
#include "inc/Core/SPANN/ExtraFullGraphSearcher.h"
#include "inc/Core/SPANN/IExtraSearcher.h"
#include "inc/Core/SPMerge.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/Timer.h"
#include "inc/Helper/VectorSetReader.h"
#include "Options.h"

namespace SPTAG {

namespace Helper {
class IniReader;
}  // namespace Helper

namespace SPANN {
template <typename T>
class SPANNResultIterator;

struct HelperContext {
    const Helper::vector_recovery<SPANNCID>* postingOrderInIndex;
    const ListInfoMapType* listInfoMap;

    size_t ALIGNMENT;
    size_t perVectorSize;
    size_t vecInfoSize;

    // deduplication for vector collection
    size_t target_count;
    bool flip_flag = false;
    std::vector<std::atomic_bool> visited;

    // buffer for vectorset
    using WriterQueueItem = char*;
    using ReaderQueueItem = std::pair<char*, size_t>;
    moodycamel::BlockingConcurrentQueue<WriterQueueItem> writer_buffer_queue;
    moodycamel::BlockingConcurrentQueue<ReaderQueueItem> reader_buffer_queue;

    // for send worker
    size_t small_buffer_size;

    // below are for collect worker
    std::atomic_size_t finished = 0;
    size_t io_read_size;
    SimpleIOHelper index_file_io;
    // for atomic get resources
    std::atomic_size_t area_idx = 0;
    std::vector<std::pair<size_t, size_t>> area_list;

    struct AreaInfo {
        size_t cidx1, cidx2;
        uint64_t area_start;
        uint64_t area_size;
    };
    bool get_area(AreaInfo& ai) {
        auto aidx = area_idx.fetch_add(1);
        if (aidx >= area_list.size()) return false;

        auto [cidx1, cidx2] = area_list[aidx];
        auto cid1 = postingOrderInIndex->at(cidx1);
        auto cid2 = postingOrderInIndex->at(cidx2);
        const auto& listinfo1 = listInfoMap->at(cid1);
        const auto& listinfo2 = listInfoMap->at(cid2);

        auto start = listinfo1.listOffset + listinfo1.pageOffset;

        // output
        ai.cidx1 = cidx1;
        ai.cidx2 = cidx2;
        ai.area_start = listinfo1.listOffset + listinfo1.pageOffset;
        ai.area_size = listinfo2.listOffset + listinfo2.pageOffset +
                       listinfo2.listTotalBytes - ai.area_start;

        uint16_t area_padding = start % ALIGNMENT;
        ai.area_start -= area_padding;
        ai.area_size += area_padding;
        ai.area_size = ROUND_UP(ai.area_size, ALIGNMENT);

        assert(ai.area_start % ALIGNMENT == 0);
        assert(ai.area_size % ALIGNMENT == 0);

        return true;
    }
    bool check_and_set(SPMerge::VectorID vid) {
        uint64_t idx = vid;
        bool expected = flip_flag;
        bool target = !expected;
        if (visited[idx].compare_exchange_strong(expected, target)) {
            finished++;
            return true;
        }
        return false;
    }
    bool task_done() { return finished == target_count; }
};

template <typename T>
class Index : public VectorIndex {
   private:
    std::shared_ptr<VectorIndex> m_index;
    std::shared_ptr<std::uint64_t> m_vectorTranslateMap;
    std::unordered_map<std::string, std::string> m_headParameters;

    std::shared_ptr<IExtraSearcher> m_extraSearcher;

    Options m_options;

    std::function<float(const T*, const T*, DimensionType)> m_fComputeDistance;
    int m_iBaseSquare;
    std::unique_ptr<SPTAG::COMMON::IWorkSpaceFactory<ExtraWorkSpace>>
        m_workSpaceFactory;

   public:
    Index() {
        m_workSpaceFactory = std::make_unique<
            SPTAG::COMMON::ThreadLocalWorkSpaceFactory<ExtraWorkSpace>>();
        m_fComputeDistance =
            std::function<float(const T*, const T*, DimensionType)>(
                COMMON::DistanceCalcSelector<T>(m_options.m_distCalcMethod));
        m_iBaseSquare =
            (m_options.m_distCalcMethod == DistCalcMethod::Cosine)
                ? COMMON::Utils::GetBase<T>() * COMMON::Utils::GetBase<T>()
                : 1;
    }

    ~Index() {}

    virtual void LoadSSDIndex();
    virtual void LoadHeadIndex();
    virtual void LoadPatchMeta();

    // prune clusters, after the call, `result` contains cids to be further
    // searched, `local` contains pruned vectors (cid tranfered to vid)
    virtual void PruneClusters(QueryResult& result, QueryResult& local,
                               SearchStats& stat, Ort::Session* session);
    virtual void NormalPrunig(QueryResult& result, QueryResult& local,
                              SearchStats& stat);
    virtual void FrequencyPrunig(QueryResult& result, QueryResult& local,
                                 SearchStats& stat, Ort::Session* session);
    inline void CollectInfo(
        QueryResult& result,
        std::vector<SPMerge::VecFreqInfo>& vec_info_storage,
        absl::flat_hash_map<SPMerge::VectorID, int>& vec_info,
        std::vector<double>& len_list);
    inline size_t GetSPMergeFeatureDim() { return 12; }

    template <typename ModelDataType>
    inline void GetFeature(
        const std::vector<SPMerge::VecFreqInfo>& vec_info_storage,
        const std::vector<double>& len_list,
        std::vector<ModelDataType>& features) {
        const int N_FEATURES = GetSPMergeFeatureDim();

        features.resize(N_FEATURES, 0);
        for (const auto& info : vec_info_storage) {
            features[info.freq - 1] += 1.0f;
        }
        for (int i = 0; i < 8; ++i) {  // 只归一化频率部分
            features[i] /= static_cast<ModelDataType>(vec_info_storage.size());
        }

        double sum_len_list = 0.0;
        double mean_len_list = 0.0;
        double min_len_list = 0.0;
        double max_len_list = 0.0;
        double std_len_list = 0.0;
        if (len_list.size() > 0) {
            sum_len_list =
                std::accumulate(len_list.begin(), len_list.end(), 0.0);
            mean_len_list = sum_len_list / len_list.size();
            min_len_list = *std::min_element(len_list.begin(), len_list.end());
            max_len_list = *std::max_element(len_list.begin(), len_list.end());
            double sq_sum = 0.0;
            for (const double val : len_list) {
                sq_sum += (val - mean_len_list) * (val - mean_len_list);
            }
            std_len_list = std::sqrt(sq_sum / len_list.size());
        }

        features[8] = mean_len_list;
        features[9] = min_len_list;
        features[10] = max_len_list;
        features[11] = std_len_list;
    }

    inline std::shared_ptr<VectorIndex> GetMemoryIndex() { return m_index; }
    inline std::shared_ptr<IExtraSearcher> GetDiskIndex() {
        return m_extraSearcher;
    }
    inline Options* GetOptions() { return &m_options; }

    inline SizeType GetNumSamples() const { return m_options.m_vectorSize; }
    inline DimensionType GetFeatureDim() const {
        return m_pQuantizer ? m_pQuantizer->ReconstructDim()
                            : m_index->GetFeatureDim();
    }

    inline int GetCurrMaxCheck() const { return m_options.m_maxCheck; }
    inline int GetNumThreads() const { return m_options.m_iSSDNumberOfThreads; }
    inline DistCalcMethod GetDistCalcMethod() const {
        return m_options.m_distCalcMethod;
    }
    inline IndexAlgoType GetIndexAlgoType() const {
        return IndexAlgoType::SPANN;
    }
    inline VectorValueType GetVectorValueType() const {
        return GetEnumValueType<T>();
    }

    void SetQuantizer(std::shared_ptr<SPTAG::COMMON::IQuantizer> quantizer);

    inline float AccurateDistance(const void* pX, const void* pY) const {
        if (m_options.m_distCalcMethod == DistCalcMethod::L2)
            return m_fComputeDistance((const T*)pX, (const T*)pY,
                                      m_options.m_dim);

        float xy =
            m_iBaseSquare -
            m_fComputeDistance((const T*)pX, (const T*)pY, m_options.m_dim);
        float xx =
            m_iBaseSquare -
            m_fComputeDistance((const T*)pX, (const T*)pX, m_options.m_dim);
        float yy =
            m_iBaseSquare -
            m_fComputeDistance((const T*)pY, (const T*)pY, m_options.m_dim);
        return 1.0f - xy / (sqrt(xx) * sqrt(yy));
    }
    inline float ComputeDistance(const void* pX, const void* pY) const {
        return m_fComputeDistance((const T*)pX, (const T*)pY, m_options.m_dim);
    }
    inline float GetDistance(const void* target, const SizeType idx) const {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                     "GetDistance NOT SUPPORT FOR SPANN");
        return -1;
    }
    inline bool ContainSample(const SizeType idx) const {
        return idx < m_options.m_vectorSize;
    }

    std::shared_ptr<std::vector<std::uint64_t>> BufferSize() const {
        std::shared_ptr<std::vector<std::uint64_t>> buffersize(
            new std::vector<std::uint64_t>);
        auto headIndexBufferSize = m_index->BufferSize();
        buffersize->insert(buffersize->end(), headIndexBufferSize->begin(),
                           headIndexBufferSize->end());
        buffersize->push_back(sizeof(long long) * m_index->GetNumSamples());
        return std::move(buffersize);
    }

    std::shared_ptr<std::vector<std::string>> GetIndexFiles() const {
        std::shared_ptr<std::vector<std::string>> files(
            new std::vector<std::string>);
        auto headfiles = m_index->GetIndexFiles();
        for (auto file : *headfiles) {
            files->push_back(m_options.m_headIndexFolder + FolderSep + file);
        }
        files->push_back(m_options.m_headIDFile);
        return std::move(files);
    }

    ErrorCode SaveConfig(std::shared_ptr<Helper::DiskIO> p_configout);
    ErrorCode SaveIndexData(
        const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams);

    ErrorCode LoadConfig(Helper::IniReader& p_reader);
    ErrorCode LoadIndexData(
        const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams);
    ErrorCode LoadIndexDataFromMemory(
        const std::vector<ByteArray>& p_indexBlobs);

    ErrorCode BuildIndex(const void* p_data, SizeType p_vectorNum,
                         DimensionType p_dimension, bool p_normalized = false,
                         bool p_shareOwnership = false);
    ErrorCode BuildIndex(bool p_normalized = false);
    ErrorCode SearchIndex(QueryResult& p_query,
                          bool p_searchDeleted = false) const;

    std::shared_ptr<ResultIterator> GetIterator(
        const void* p_target, bool p_searchDeleted = false) const;
    ErrorCode SearchIndexIterativeNext(QueryResult& p_results,
                                       COMMON::WorkSpace* workSpace, int batch,
                                       int& resultCount, bool p_isFirst,
                                       bool p_searchDeleted = false) const;
    ErrorCode SearchIndexIterativeEnd(
        std::unique_ptr<COMMON::WorkSpace> workSpace) const;
    ErrorCode SearchIndexIterativeEnd(
        std::unique_ptr<SPANN::ExtraWorkSpace> extraWorkspace) const;
    bool SearchIndexIterativeFromNeareast(QueryResult& p_query,
                                          COMMON::WorkSpace* p_space,
                                          bool p_isFirst,
                                          bool p_searchDeleted = false) const;
    std::unique_ptr<COMMON::WorkSpace> RentWorkSpace(int batch) const;
    ErrorCode SearchIndexIterative(QueryResult& p_headQuery,
                                   QueryResult& p_query,
                                   COMMON::WorkSpace* p_indexWorkspace,
                                   ExtraWorkSpace* p_extraWorkspace,
                                   int p_batch, int& resultCount,
                                   bool first) const;

    ErrorCode SearchIndexWithFilter(
        QueryResult& p_query, std::function<bool(const ByteArray&)> filterFunc,
        int maxCheck = 0, bool p_searchDeleted = false) const;

    // search disk index without any pruning
    ErrorCode SearchDiskIndexPurely(QueryResult& p_query,
                                    SearchStats* p_stats = nullptr) const;
    ErrorCode SearchDiskIndex(QueryResult& p_query,
                              SearchStats* p_stats = nullptr) const;
    bool SearchDiskIndexIterative(QueryResult& p_headQuery,
                                  QueryResult& p_query,
                                  ExtraWorkSpace* extraWorkspace) const;
    ErrorCode DebugSearchDiskIndex(
        QueryResult& p_query, int p_subInternalResultNum,
        int p_internalResultNum, SearchStats* p_stats = nullptr,
        std::set<int>* truth = nullptr,
        std::map<int, std::set<int>>* found = nullptr) const;
    ErrorCode UpdateIndex();

    virtual SPMerge::MergerResult& GetMergerResult() {
        return m_extraSearcher->GetMergerResult();
    }
    virtual void SetRecoveryMode(bool recovery) {
        m_extraSearcher->SetRecoveryMode(recovery);
    }
    ErrorCode SetParameter(const char* p_param, const char* p_value,
                           const char* p_section = nullptr);
    std::string GetParameter(const char* p_param,
                             const char* p_section = nullptr) const;

    inline const void* GetSample(const SizeType idx) const { return nullptr; }
    inline SizeType GetNumDeleted() const { return 0; }
    inline bool NeedRefine() const { return false; }

    ErrorCode RefineSearchIndex(QueryResult& p_query,
                                bool p_searchDeleted = false) const {
        return ErrorCode::Undefined;
    }
    ErrorCode SearchTree(QueryResult& p_query) const {
        return ErrorCode::Undefined;
    }
    ErrorCode AddIndex(const void* p_data, SizeType p_vectorNum,
                       DimensionType p_dimension,
                       std::shared_ptr<MetadataSet> p_metadataSet,
                       bool p_withMetaIndex = false,
                       bool p_normalized = false) {
        return ErrorCode::Undefined;
    }
    ErrorCode DeleteIndex(const void* p_vectors, SizeType p_vectorNum) {
        return ErrorCode::Undefined;
    }
    ErrorCode DeleteIndex(const SizeType& p_id) { return ErrorCode::Undefined; }
    ErrorCode RefineIndex(
        const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams,
        IAbortOperation* p_abort) {
        return ErrorCode::Undefined;
    }
    ErrorCode RefineIndex(std::shared_ptr<VectorIndex>& p_newIndex) {
        return ErrorCode::Undefined;
    }
    ErrorCode SetWorkSpaceFactory(
        std::unique_ptr<
            SPTAG::COMMON::IWorkSpaceFactory<SPTAG::COMMON::IWorkSpace>>
            up_workSpaceFactory) {
        SPTAG::COMMON::IWorkSpaceFactory<SPTAG::COMMON::IWorkSpace>*
            raw_generic_ptr = up_workSpaceFactory.release();
        if (!raw_generic_ptr) return ErrorCode::Fail;

        SPTAG::COMMON::IWorkSpaceFactory<ExtraWorkSpace>* raw_specialized_ptr =
            dynamic_cast<SPTAG::COMMON::IWorkSpaceFactory<ExtraWorkSpace>*>(
                raw_generic_ptr);
        if (!raw_specialized_ptr) {
            // If it is of type SPTAG::COMMON::WorkSpace, we should pass on to
            // child index
            if (!m_index) {
                delete raw_generic_ptr;
                return ErrorCode::Fail;
            } else {
                return m_index->SetWorkSpaceFactory(
                    std::unique_ptr<SPTAG::COMMON::IWorkSpaceFactory<
                        SPTAG::COMMON::IWorkSpace>>(raw_generic_ptr));
            }

        } else {
            m_workSpaceFactory = std::unique_ptr<
                SPTAG::COMMON::IWorkSpaceFactory<ExtraWorkSpace>>(
                raw_specialized_ptr);
            return ErrorCode::Success;
        }
    }

    SizeType GetGlobalVID(SizeType vid) {
        return static_cast<SizeType>((m_vectorTranslateMap.get())[vid]);
    }

    void GuideDiskIO(HelperContext& context,
                     std::unique_ptr<char[]>& vector_buffer,
                     const size_t NET_IO_SIZE = 1024 * 1024,
                     const size_t DISK_IO_SIZE = 1024 * 1024,
                     const size_t ALIGNMENT = 512);
    void InitContextDeduper(HelperContext& context,
                            const SPMerge::VectorIDList target_vid_list);
    void f_send_worker(HelperContext& ctx, int sockfd);
    size_t f_collect_worker(HelperContext& ctx);
    void SplitVIDTask(SPMerge::ClusterIDList& primary_task_cids,
                      SPMerge::VectorIDList& primary_task_vids,
                      SPMerge::ClusterIDList& secondary_task_cids,
                      SPMerge::VectorIDList& secondary_task_vids,
                      std::vector<std::atomic_bool>& need_secondary_vids,
                      SPMerge::MachineID fail_mid, void* pool);
    virtual bool HelpRecovery(SPMerge::MachineID fail_mid, std::string fail_ip,
                              int fail_port, void* pool);
    virtual bool PrimaryHelpRecovery(SPMerge::MachineID fail_mid,
                                     std::string fail_ip, int fail_port,
                                     std::string secondary_ip,
                                     int secondary_port, void* pool);
    virtual bool SecondHelpRecovery(int recv_port, std::string fail_ip,
                                    int fail_port, void* pool);
    virtual bool ReceiveAndRecovery(int port);
    virtual void LoadAndRecovery(SPMerge::MachineID healthy_mid,
                                 SPMerge::MachineID fail_mid);
    virtual void Deinit() { m_extraSearcher->DeInit(); }
    virtual SPMerge::ClusterID GetNumPosting() const;
    ErrorCode GetPostingDebug(SizeType vid, std::vector<SizeType>& VIDs,
                              std::shared_ptr<VectorSet>& vecs);
    virtual ErrorCode GetPostingList(
        const SPMerge::ClusterIDList& cids,
        SPMerge::ClusterContentList& cluster_content,
        std::function<void(uint64_t, void*)> f_handle_vector = nullptr) const;

    virtual void ClusterAssignmentToOrder(
        SPMerge::MergerResult& mr,
        std::shared_ptr<Helper::VectorSetReader>& p_reader) const {
        m_extraSearcher->ClusterAssignmentToOrder(mr, p_reader);
    }
    virtual void SaveMergerResultFiles(
        const SPMerge::MergerResult& mr,
        std::shared_ptr<Helper::VectorSetReader>& p_reader) const {
        m_extraSearcher->SaveMergerResultFiles(mr, p_reader);
    }

   private:
    bool CheckHeadIndexType();
    void SelectHeadAdjustOptions(int p_vectorCount);
    int SelectHeadDynamicallyInternal(
        const std::shared_ptr<COMMON::BKTree> p_tree, int p_nodeID,
        const Options& p_opts, std::vector<int>& p_selected);
    void SelectHeadDynamically(const std::shared_ptr<COMMON::BKTree> p_tree,
                               int p_vectorCount, std::vector<int>& p_selected);

    template <typename InternalDataType>
    bool SelectHeadInternal(std::shared_ptr<Helper::VectorSetReader>& p_reader);

    ErrorCode BuildIndexInternal(
        std::shared_ptr<Helper::VectorSetReader>& p_reader);
};
}  // namespace SPANN
}  // namespace SPTAG

#endif  // _SPTAG_SPANN_INDEX_H_
