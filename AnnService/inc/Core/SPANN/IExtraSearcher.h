// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SPANN_IEXTRASEARCHER_H_
#define _SPTAG_SPANN_IEXTRASEARCHER_H_

#include <onnxruntime_cxx_api.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "inc/Core/Common.h"
#include "inc/Core/SPMerge.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/AsyncFileReader.h"
#include "inc/Helper/MemoryManager.h"
#include "inc/Helper/Timer.h"
#include "inc/Helper/VectorSetReader.h"
#include "Options.h"

namespace SPTAG {
namespace SPANN {

using SPANNVID = int;  // FIXME: make it SPMerge::VectorID!
using SPANNCID = int;  // FIXME: make it SPMerge::ClusetID!

struct SearchStats {
    SearchStats(size_t p_numFile = 0)
        : m_totalListElementsCount(0),
          m_diskIOCount(0),
          m_diskAccessCount(0),
          m_totalSearchLatency(0),
          m_headLatency(0),
          m_exLatency(0),
          m_mergeResultLatency(0),
          m_serverSearchLatency(0),
          m_serverLatency(0),
          m_clientToServerLatency(0),
          m_serverToclientLatency(0),
          m_serverprocessLatency(0),
          m_totalNetworkLatency(0),
          m_fileAcessedCnt(p_numFile, 0) {}

    // NOTE: deprecated
    // int m_check;
    // int m_exCheck;
    // double m_totalLatency;
    // double m_asyncLatency0;
    // double m_asyncLatency1;
    // double m_asyncLatency2;
    // double m_queueLatency;
    // double m_sleepLatency;

    // disk access stats
    int m_totalListElementsCount;
    int m_diskIOCount;
    int m_diskAccessCount;

    // core latency
    double m_totalSearchLatency;
    double m_headLatency;
    double m_exLatency;

    // pruning breakdown
    double m_pruning_collect_vecinfo;
    double m_pruning_feature_compute;
    double m_pruning_model_predict;
    double m_pruning_pruning;
    double m_pruning_total;

    // for tmp test
    double m_mergeResultLatency;
    double m_serverSearchLatency;
    double m_serverLatency;
    double m_clientToServerLatency;
    double m_serverprocessLatency;
    double m_serverToclientLatency;
    double m_totalNetworkLatency;

    int m_threadID;

    // for freq pruning (param pass and timer)
    int m_freq = -1;
    double m_purningLatency{0};
    Helper::EventTimer m_pruningTimer{};

    // for load balance
    std::vector<uint> m_fileAcessedCnt;
    bool record_access_to_file(size_t fileid) {
        if (fileid >= m_fileAcessedCnt.size()) return false;
        m_fileAcessedCnt[fileid]++;
        return true;
    }
};

template <typename T>
class PageBuffer {
   public:
    PageBuffer() : m_pageBufferSize(0) {}

    void ReservePageBuffer(std::size_t p_size) {
        if (m_pageBufferSize < p_size) {
            m_pageBufferSize = p_size;
            m_pageBuffer.reset(
                static_cast<T*>(PAGE_ALLOC(sizeof(T) * m_pageBufferSize)),
                [=](T* ptr) { PAGE_FREE(ptr); });
        }
    }

    T* GetBuffer() { return m_pageBuffer.get(); }

    std::size_t GetPageSize() { return m_pageBufferSize; }

   private:
    std::shared_ptr<T> m_pageBuffer;

    std::size_t m_pageBufferSize;
};

struct ExtraWorkSpace : public SPTAG::COMMON::IWorkSpace {
    ExtraWorkSpace() {}

    ~ExtraWorkSpace() { g_spaceCount--; }

    ExtraWorkSpace(ExtraWorkSpace& other) {
        Initialize(other.m_deduper.MaxCheck(),
                   other.m_deduper.HashTableExponent(),
                   (int)other.m_pageBuffers.size(),
                   (int)(other.m_pageBuffers[0].GetPageSize()),
                   other.m_enableDataCompression);
    }

    void Initialize(int p_maxCheck, int p_hashExp, int p_internalResultNum,
                    int p_maxPages, bool enableDataCompression) {
        m_postingIDs.reserve(p_internalResultNum);
        m_deduper.Init(p_maxCheck, p_hashExp);
        m_processIocp.reset(p_internalResultNum);
        m_pageBuffers.resize(p_internalResultNum);
        for (int pi = 0; pi < p_internalResultNum; pi++) {
            m_pageBuffers[pi].ReservePageBuffer(p_maxPages);
        }
        m_diskRequests.resize(p_internalResultNum);
        for (int pi = 0; pi < p_internalResultNum; pi++) {
            m_diskRequests[pi].m_extension = m_processIocp.handle();
        }
        m_enableDataCompression = enableDataCompression;
        if (enableDataCompression) {
            m_decompressBuffer.ReservePageBuffer(p_maxPages);
        }
        m_spaceID = g_spaceCount++;
        m_relaxedMono = false;
    }

    void Initialize(va_list& arg) {
        int maxCheck = va_arg(arg, int);
        int hashExp = va_arg(arg, int);
        int internalResultNum = va_arg(arg, int);
        int maxPages = va_arg(arg, int);
        bool enableDataCompression = bool(va_arg(arg, int));
        Initialize(maxCheck, hashExp, internalResultNum, maxPages,
                   enableDataCompression);
    }

    void Clear(int p_internalResultNum, int p_maxPages,
               bool enableDataCompression) {
        if (p_internalResultNum > m_pageBuffers.size()) {
            m_postingIDs.reserve(p_internalResultNum);
            m_processIocp.reset(p_internalResultNum);
            m_pageBuffers.resize(p_internalResultNum);
            for (int pi = 0; pi < p_internalResultNum; pi++) {
                m_pageBuffers[pi].ReservePageBuffer(p_maxPages);
            }
            m_diskRequests.resize(p_internalResultNum);
            for (int pi = 0; pi < p_internalResultNum; pi++) {
                m_diskRequests[pi].m_extension = m_processIocp.handle();
            }
        } else if (p_maxPages > m_pageBuffers[0].GetPageSize()) {
            for (int pi = 0; pi < m_pageBuffers.size(); pi++)
                m_pageBuffers[pi].ReservePageBuffer(p_maxPages);
        }

        m_enableDataCompression = enableDataCompression;
        if (enableDataCompression) {
            m_decompressBuffer.ReservePageBuffer(p_maxPages);
        }
    }

    static void Reset() { g_spaceCount = 0; }

    std::vector<SPANNCID> m_postingIDs;

    COMMON::OptHashPosVector m_deduper;

    Helper::RequestQueue m_processIocp;

    std::vector<PageBuffer<std::uint8_t>> m_pageBuffers;

    bool m_enableDataCompression;
    PageBuffer<std::uint8_t> m_decompressBuffer;

    std::vector<Helper::AsyncReadRequest> m_diskRequests;

    int m_spaceID;

    uint32_t m_pi;

    int m_offset;

    bool m_loadPosting;

    bool m_relaxedMono;

    int m_loadedPostingNum;

    static std::atomic_int g_spaceCount;
};

struct ListInfo;
using ListInfosType = std::vector<
    ListInfo,
    Helper::TrackerAllocator<ListInfo, Helper::MEMORY_SOURCE::SPANN_META>>;

using ListInfoMapType = std::unordered_map<
    SPMerge::ClusterID, ListInfo, std::hash<SPMerge::ClusterID>,
    std::equal_to<SPMerge::ClusterID>,
    Helper::TrackerAllocator<std::pair<const SPMerge::ClusterID, ListInfo>,
                             Helper::MEMORY_SOURCE::SPANN_META>>;

class IExtraSearcher {
   public:
    IExtraSearcher() {}

    virtual ~IExtraSearcher() {}

    virtual bool LoadIndex(Options& p_options) = 0;

    virtual void SearchIndex(ExtraWorkSpace* p_exWorkSpace,
                             QueryResult& p_queryResults,
                             std::shared_ptr<VectorIndex> p_index,
                             SearchStats* p_stats,
                             std::set<int>* truth = nullptr,
                             std::map<int, std::set<int>>* found = nullptr) = 0;

    virtual bool SearchIterativeNext(ExtraWorkSpace* p_exWorkSpace,
                                     QueryResult& p_queryResults,
                                     std::shared_ptr<VectorIndex> p_index) = 0;

    virtual void SearchIndexWithoutParsing(ExtraWorkSpace* p_exWorkSpace) = 0;

    virtual bool SearchNextInPosting(ExtraWorkSpace* p_exWorkSpace,
                                     QueryResult& p_queryResults,
                                     std::shared_ptr<VectorIndex>& p_index) = 0;

    virtual bool BuildIndex(std::shared_ptr<Helper::VectorSetReader>& p_reader,
                            std::shared_ptr<VectorIndex> p_index,
                            Options& p_opt) = 0;

    virtual bool CheckValidPosting(SizeType postingID) = 0;

    virtual ErrorCode GetPostingDebug(ExtraWorkSpace* p_exWorkSpace,
                                      std::shared_ptr<VectorIndex> p_index,
                                      SizeType vid, std::vector<SizeType>& VIDs,
                                      std::shared_ptr<VectorSet>& vecs) = 0;

    virtual void SetRecoveryMode(bool recovery) { NOT_IMPLEMENTED(); }
    virtual SPMerge::MergerResult& GetMergerResult() { NOT_IMPLEMENTED(); }
    virtual const ListInfosType& GetListInfo() const { NOT_IMPLEMENTED(); }
    virtual const ListInfoMapType& GetListInfoMap() const { NOT_IMPLEMENTED(); }
    virtual ErrorCode GetPostingList(
        ExtraWorkSpace* p_exWorkSpace, std::shared_ptr<VectorIndex> p_index,
        const SPMerge::ClusterIDList& cid_list,
        SPMerge::ClusterContentList& cluster_content,
        std::function<void(uint64_t, void*)> f_handle_vector) const = 0;

    virtual void DeInit(bool keep_mr = false) { NOT_IMPLEMENTED(); }
    virtual Helper::vector_recovery<SPANNCID>& GetPostingOrder() {
        NOT_IMPLEMENTED();
    }

    virtual void ClusterAssignmentToOrder(
        SPMerge::MergerResult& mr,
        std::shared_ptr<Helper::VectorSetReader>& p_reader,
        std::shared_ptr<VectorIndex> p_headIndex = nullptr) const {
        NOT_IMPLEMENTED();
    }
    virtual void SaveMergerResultFiles(
        const SPMerge::MergerResult& mr,
        std::shared_ptr<Helper::VectorSetReader>& p_reader,
        std::shared_ptr<VectorIndex> p_headIndex = nullptr) const {
        NOT_IMPLEMENTED();
    }
    virtual void LoadAndRecovery(SPMerge::MachineID healthy_mid,
                                 SPMerge::MachineID fail_mid) {
        NOT_IMPLEMENTED();
    }
    virtual size_t GetNumNodes() const { NOT_IMPLEMENTED(); }
};
}  // namespace SPANN
}  // namespace SPTAG

#endif  // _SPTAG_SPANN_IEXTRASEARCHER_H_
