// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SPANN_EXTRASEARCHER_H_
#define _SPTAG_SPANN_EXTRASEARCHER_H_

#include <atomic>
#include <climits>
#include <cmath>
#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <thread>
#include <unordered_map>

#include "Compressor.h"
#include "IExtraSearcher.h"
#include "inc/Core/Common.h"
#include "inc/Core/Common/SIMDUtils.h"
#include "inc/Core/Common/TruthSet.h"
#include "inc/Core/SPMerge.h"
#include "inc/Helper/AsyncFileReader.h"
#include "inc/Helper/Concurrent.h"
#include "inc/Helper/DiskIO.h"
#include "inc/Helper/MemoryManager.h"
#include "inc/Helper/VectorSetReader.h"

#undef BLOCK_SIZE
#include "concurrentqueue.h"

namespace SPTAG {
namespace SPANN {

extern std::function<std::shared_ptr<Helper::DiskIO>(void)> f_createAsyncIO;

struct Selection {
    std::string m_tmpfile;
    size_t m_totalsize;
    size_t m_start;
    size_t m_end;
    std::vector<Edge> m_selections;
    static EdgeCompare g_edgeComparer;

    Selection(size_t totalsize, std::string tmpdir)
        : m_tmpfile(tmpdir + FolderSep + "selection_tmp"),
          m_totalsize(totalsize),
          m_start(0),
          m_end(totalsize) {
        remove(m_tmpfile.c_str());
        m_selections.resize(totalsize);
    }

    ErrorCode SaveBatch() {
        auto f_out = f_createIO();
        if (f_out == nullptr ||
            !f_out->Initialize(
                m_tmpfile.c_str(),
                std::ios::out | std::ios::binary |
                    (fileexists(m_tmpfile.c_str()) ? std::ios::in : 0))) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Cannot open %s to save selection for batching!\n",
                         m_tmpfile.c_str());
            return ErrorCode::FailedOpenFile;
        }
        if (f_out->WriteBinary(sizeof(Edge) * (m_end - m_start),
                               (const char*)m_selections.data(),
                               sizeof(Edge) * m_start) !=
            sizeof(Edge) * (m_end - m_start)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot write to %s!\n",
                         m_tmpfile.c_str());
            return ErrorCode::DiskIOFail;
        }
        std::vector<Edge> batch_selection;
        m_selections.swap(batch_selection);
        m_start = m_end = 0;
        return ErrorCode::Success;
    }

    ErrorCode LoadBatch(size_t start, size_t end) {
        auto f_in = f_createIO();
        if (f_in == nullptr ||
            !f_in->Initialize(m_tmpfile.c_str(),
                              std::ios::in | std::ios::binary)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Cannot open %s to load selection batch!\n",
                         m_tmpfile.c_str());
            return ErrorCode::FailedOpenFile;
        }

        size_t readsize = end - start;
        m_selections.resize(readsize);
        if (f_in->ReadBinary(readsize * sizeof(Edge),
                             (char*)m_selections.data(),
                             start * sizeof(Edge)) != readsize * sizeof(Edge)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Cannot read from %s! start:%zu size:%zu\n",
                         m_tmpfile.c_str(), start, readsize);
            return ErrorCode::DiskIOFail;
        }
        m_start = start;
        m_end = end;
        return ErrorCode::Success;
    }

    size_t lower_bound(SizeType node) {
        auto ptr = std::lower_bound(m_selections.begin(), m_selections.end(),
                                    node, g_edgeComparer);
        return m_start + (ptr - m_selections.begin());
    }

    Edge& operator[](size_t offset) {
        if (offset < m_start || offset >= m_end) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Error read offset in selections:%zu\n", offset);
        }
        return m_selections[offset - m_start];
    }
};

#define DecompressPosting()                                                    \
    {                                                                          \
        p_postingListFullData =                                                \
            (char*)p_exWorkSpace->m_decompressBuffer.GetBuffer();              \
        if (listInfo->listEleCount != 0) {                                     \
            std::size_t sizePostingListFullData;                               \
            try {                                                              \
                sizePostingListFullData = m_pCompressor->Decompress(           \
                    buffer + listInfo->pageOffset, listInfo->listTotalBytes,   \
                    p_postingListFullData,                                     \
                    listInfo->listEleCount * m_vectorInfoSize,                 \
                    m_enableDictTraining);                                     \
            } catch (std::runtime_error & err) {                               \
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,                       \
                             "Decompress postingList %d  failed! %s, \n",      \
                             listInfo - m_listInfos.data(), err.what());       \
                return;                                                        \
            }                                                                  \
            if (sizePostingListFullData !=                                     \
                listInfo->listEleCount * m_vectorInfoSize) {                   \
                SPTAGLIB_LOG(                                                  \
                    Helper::LogLevel::LL_Error,                                \
                    "PostingList %d decompressed size not match! %zu, %d, \n", \
                    listInfo - m_listInfos.data(), sizePostingListFullData,    \
                    listInfo->listEleCount * m_vectorInfoSize);                \
                return;                                                        \
            }                                                                  \
        }                                                                      \
    }

#define DecompressPostingIterative()                                         \
    {                                                                        \
        p_postingListFullData =                                              \
            (char*)p_exWorkSpace->m_decompressBuffer.GetBuffer();            \
        if (listInfo->listEleCount != 0) {                                   \
            std::size_t sizePostingListFullData;                             \
            try {                                                            \
                sizePostingListFullData = m_pCompressor->Decompress(         \
                    buffer + listInfo->pageOffset, listInfo->listTotalBytes, \
                    p_postingListFullData,                                   \
                    listInfo->listEleCount * m_vectorInfoSize,               \
                    m_enableDictTraining);                                   \
                if (sizePostingListFullData !=                               \
                    listInfo->listEleCount * m_vectorInfoSize) {             \
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,                 \
                                 "PostingList %d decompressed size not "     \
                                 "match! %zu, %d, \n",                       \
                                 listInfo - m_listInfos.data(),              \
                                 sizePostingListFullData,                    \
                                 listInfo->listEleCount * m_vectorInfoSize); \
                }                                                            \
            } catch (std::runtime_error & err) {                             \
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,                     \
                             "Decompress postingList %d  failed! %s, \n",    \
                             listInfo - m_listInfos.data(), err.what());     \
            }                                                                \
        }                                                                    \
    }

#define ProcessPosting()                                                       \
    for (int i = 0; i < listInfo->listEleCount; i++) {                         \
        uint64_t offsetVectorID, offsetVector;                                 \
        (this->*m_parsePosting)(offsetVectorID, offsetVector, i,               \
                                listInfo->listEleCount);                       \
        int vectorID =                                                         \
            *(reinterpret_cast<int*>(p_postingListFullData + offsetVectorID)); \
        if (p_exWorkSpace->m_deduper.CheckAndSet(vectorID)) continue;          \
        (this->*m_parseEncoding)(                                              \
            p_index, listInfo,                                                 \
            (ValueType*)(p_postingListFullData + offsetVector));               \
        auto distance2leaf =                                                   \
            p_index->ComputeDistance(queryResults.GetQuantizedTarget(),        \
                                     p_postingListFullData + offsetVector);    \
        queryResults.AddPoint(vectorID, distance2leaf);                        \
    }

#define ProcessPostingOffset()                                                 \
    while (p_exWorkSpace->m_offset < listInfo->listEleCount) {                 \
        uint64_t offsetVectorID, offsetVector;                                 \
        (this->*m_parsePosting)(offsetVectorID, offsetVector,                  \
                                p_exWorkSpace->m_offset,                       \
                                listInfo->listEleCount);                       \
        p_exWorkSpace->m_offset++;                                             \
        int vectorID =                                                         \
            *(reinterpret_cast<int*>(p_postingListFullData + offsetVectorID)); \
        if (p_exWorkSpace->m_deduper.CheckAndSet(vectorID)) continue;          \
        (this->*m_parseEncoding)(                                              \
            p_index, listInfo,                                                 \
            (ValueType*)(p_postingListFullData + offsetVector));               \
        auto distance2leaf =                                                   \
            p_index->ComputeDistance(queryResults.GetQuantizedTarget(),        \
                                     p_postingListFullData + offsetVector);    \
        queryResults.AddPoint(vectorID, distance2leaf);                        \
        foundResult = true;                                                    \
        break;                                                                 \
    }                                                                          \
    if (p_exWorkSpace->m_offset == listInfo->listEleCount) {                   \
        p_exWorkSpace->m_pi++;                                                 \
        p_exWorkSpace->m_offset = 0;                                           \
    }

#pragma pack(push, 1)
struct PatchMetadata {
    size_t n_clusters;       // #clusters assigned to this machine
    size_t n_vectors;        // #vectors in patch
    size_t n_headvec;        // #head vectors, NOTE: we only guarantee
                             // n_headvec+n_clusvec=N_DATA
    size_t n_clusvec;        // #clustor vectors
    size_t n_machine;        // total num of machine
    size_t n_totalClusters;  // total clusters stored in cluster content
    size_t perVectorSize;    // bytes each vector

    // The 2 sections below save the cluster ids assigned to each machine, **in
    // the order of postingOrderInIndex**.
    // - first, save current machine's
    // - then save other machines'
    size_t postingOrderStart;       // cluster order in cur machine's index file
    size_t clusterAssignmentStart;  // for recovery: other machines' info

    // format as below:
    // - offset (relative to clusterContentStart) for each machine (uin64[])
    // - cluster content for each machine
    //   - MetaClusterContent
    //   - vid_list (SPMerge::VectorID[])
    size_t clusterContentStart;  // for recovery: all cluster's content

    size_t vectorIDStart;  // vector ids in patch, first self, then orthers

    size_t vectorContentStart;  // vector content in cur machine's patch

    void print() const {
        LOG_INFO("PatchMetadata {\n");
        LOG_INFO("  n_clusters=%llu\n", n_clusters);
        LOG_INFO("  n_vectors=%llu\n", n_vectors);
        LOG_INFO("  n_headvec=%llu\n", n_headvec);
        LOG_INFO("  n_clusvec=%llu\n", n_clusvec);
        LOG_INFO("  n_machine=%llu\n", n_machine);
        LOG_INFO("  n_totalClusters=%llu\n", n_totalClusters);
        LOG_INFO("  perVectorSize=%llu\n", perVectorSize);

        LOG_INFO("  postingOrderStart=%llu\n", postingOrderStart);
        LOG_INFO("  clusterAssignmentStart=%llu\n", clusterAssignmentStart);
        LOG_INFO("  clusterContentStart=%llu\n", clusterContentStart);
        LOG_INFO("  vectorIDStart=%llu\n", vectorIDStart);
        LOG_INFO("  vectorContentStart=%llu\n", vectorContentStart);
        LOG_INFO("}\n");
    }

    // info for cluster assignment
    struct MetaClusterAssignment {
        SPMerge::MachineID mid;
        size_t n_clusters;
    };
    // info for cluster content
    struct MetaClusterContent {
        SPMerge::ClusterID cid;
        size_t n_vecs;
    };
    // info for others' patch
    struct MetaPatch {
        SPMerge::MachineID mid;
        size_t n_vecs;
    };
};
#pragma pack(pop)

struct ListInfo {
    SPMerge::MachineID mid = -1;  // indicate which machine the posting is on
    SPANNCID cid = -1;
    std::size_t listTotalBytes = 0;

    int listEleCount = 0;

    std::uint16_t listPageCount = 0;

    std::uint64_t listOffset = 0;  // page start bytes

    std::uint16_t pageOffset = 0;  // start offset in the page
};

struct PostingOutputInfo {
    size_t bytes;
    int size;                // output
    int page_num;            // output
    std::uint16_t page_off;  // output
    std::uint16_t paddings;
};

struct ClusterContentSelection {
    uint64_t off;
    size_t bytes;
    size_t n_clusters;
};

template <typename ValueType>
class ExtraFullGraphSearcher : public IExtraSearcher {
    template <typename T>
    friend class Index;

   public:
    explicit ExtraFullGraphSearcher(Options& p_options) : m_options(p_options) {
        Helper::MemoryTracker::init();
        m_enableDeltaEncoding = false;
        m_enablePostingListRearrange = false;
        m_enableDataCompression = false;
        m_enableDictTraining = false;
        m_numNodes = m_options.m_numMachine * m_options.m_numSplit;
        LOG_INFO("%u nodes in total: %u(virtual-replication) * %u(split)\n",
                 m_numNodes, m_options.m_numMachine, m_options.m_numSplit);
        if (m_options.m_curMid >= m_numNodes and
            m_options.m_curMid != SPMerge::InvalidMID) {
            LOG_ERROR("mid=%u > numNodes=%u\n", m_options.m_curMid, m_numNodes);
            throw std::logic_error("");
        }
    }

    virtual ~ExtraFullGraphSearcher() {}

    // receive healthy machine's metadata, then load index from memory
    void ReceiveMetadataStage1(int sockfd, SPMerge::MachineID healthy_mid);
    void ReceiveMetadataStage2(int sockfd, SPMerge::MachineID healthy_mid);
    void LoadIndexStage1(SPMerge::MachineID healthy_mid,
                         std::promise<void>* prm_load1_basic_done);
    void LoadIndexStage2(SPMerge::MachineID healthy_mid);
    // compute posting order (on fail machine), alloc memory,
    // recovery index&patch metadata, and compute vector destination
    void PrepareIndexStorage(SPMerge::MachineID healthy_mid,
                             const size_t AreaSize,
                             const size_t ALIGNMENT = 512);
    void PrepareVectorOffset(SPMerge::MachineID healthy_mid);
    void RecoverMetadata(SPMerge::MachineID healthy_mid);
    // receive sent data and recover
    size_t NetWorkReader(SPMerge::MachineID healthy_mid, int sockfd,
                         size_t buffer_size);
    void VectorCollector(int tid, SPMerge::MachineID healthy_mid);
    size_t VectorDataWriter(SPMerge::MachineID healthy_mid, int tid,
                            const size_t AreaSize);

    void LoadAndRecovery(SPMerge::MachineID healthy_mid,
                         SPMerge::MachineID fail_mid);

    virtual bool LoadIndex(Options& p_opt) {
        if (&p_opt != &m_options) {
            LOG_WARN("unexpected option, what is it?\n");
        }

        int page_limit_by_vecLimit =
            PAGE_COUNT(p_opt.m_postingVectorLimit *
                       (p_opt.m_dim * sizeof(ValueType) + sizeof(SPANNVID)));
        p_opt.m_searchPostingPageLimit = p_opt.m_postingPageLimit =
            max(p_opt.m_searchPostingPageLimit, page_limit_by_vecLimit);
        LOG_INFO("Load index with posting page limit:%d\n",
                 p_opt.m_searchPostingPageLimit);

        LoadingHeadInfo(p_opt);

        m_enableDeltaEncoding = p_opt.m_enableDeltaEncoding;
        m_enablePostingListRearrange = p_opt.m_enablePostingListRearrange;
        m_enableDataCompression = p_opt.m_enableDataCompression;
        m_enableDictTraining = p_opt.m_enableDictTraining;

        if (m_enablePostingListRearrange)
            m_parsePosting =
                &ExtraFullGraphSearcher<ValueType>::ParsePostingListRearrange;
        else
            m_parsePosting =
                &ExtraFullGraphSearcher<ValueType>::ParsePostingList;
        if (m_enableDeltaEncoding)
            m_parseEncoding =
                &ExtraFullGraphSearcher<ValueType>::ParseDeltaEncoding;
        else
            m_parseEncoding = &ExtraFullGraphSearcher<ValueType>::ParseEncoding;

        m_listPerFile = static_cast<int>(
            (m_totalListCount + m_indexFiles.size() - 1) / m_indexFiles.size());

#ifndef _MSC_VER
        Helper::AIOTimeout.tv_nsec = p_opt.m_iotimeout * 1000;
#endif
        Helper::MemoryTracker::checkStats("after load index");
        return true;
    }

    virtual void SearchIndex(ExtraWorkSpace* p_exWorkSpace,
                             QueryResult& p_queryResults,
                             std::shared_ptr<VectorIndex> p_index,
                             SearchStats* p_stats, std::set<int>* truth,
                             std::map<int, std::set<int>>* found) {
        if (p_stats)
            p_stats->m_pruningTimer.event_start("search disk index", false);
        uint32_t postingListCount =
            static_cast<uint32_t>(p_exWorkSpace->m_postingIDs.size());

        COMMON::QueryResultSet<ValueType>& queryResults =
            *((COMMON::QueryResultSet<ValueType>*)&p_queryResults);

        // auto debug_file_name =
        //     "query2posting_" + m_options.m_indexDirectory + "_nprobe" +
        //     std::to_string(m_options.m_searchInternalResultNum) + ".txt";
        // // LOG_INFO("file: %s\n", debug_file_name.c_str());
        // std::ofstream ofs(debug_file_name, std::ios::app);
        // for (auto cid : p_exWorkSpace->m_postingIDs) {
        //     ofs << cid << " ";
        // }
        // ofs << std::endl;
        // return;

        int diskRead = 0;
        int diskIO = 0;
        int listElements = 0;

#if defined(ASYNC_READ) && !defined(BATCH_READ)
        int unprocessed = 0;
#endif
        auto f_pruning = [this, p_exWorkSpace, p_stats](uint freq_limit = 1) {
            const auto& ori_cid_list = p_exWorkSpace->m_postingIDs;
            const auto& cluster_content = m_recovery_mr->cluster_content;
            std::unordered_set<SPMerge::ClusterID> pruned_cid_set;

            struct VecInfo {
                uint freq{1};
                SPMerge::ClusterID first_cid{SPMerge::InvalidCID};

                VecInfo() = default;
                VecInfo(uint f, SPMerge::ClusterID cid)
                    : freq(f), first_cid(cid) {}
            };
            std::unordered_map<SPMerge::VectorID, VecInfo> vec_info;
            vec_info.reserve(ori_cid_list.size() *
                             m_options.m_postingVectorLimit);

            if (p_stats)
                p_stats->m_pruningTimer.event_start("0.1 traverse vid_list",
                                                    false);
            for (auto cid : ori_cid_list) {
                for (auto vid : cluster_content.at(cid)) {
                    auto [it, inserted] = vec_info.try_emplace(vid, 1, cid);
                    if (not inserted) {
                        it->second.freq++;
                    }
                }
            }
            if (p_stats)
                p_stats->m_pruningTimer.event_stop("0.1 traverse vid_list");

            if (p_stats)
                p_stats->m_pruningTimer.event_start("0.2 collect cid", false);
            for (const auto& [vid, info] : vec_info) {
                if (info.freq > freq_limit) {
                    pruned_cid_set.insert(info.first_cid);
                }
            }

            if (p_stats) p_stats->m_pruningTimer.event_stop("0.2 collect cid");
            return std::move(pruned_cid_set);
        };
        if (p_stats)
            p_stats->m_pruningTimer.event_start("0. prune clusters", false);
        std::vector<SPANNCID> pruned_cid_list;
        if (m_options.m_searchPruning) {
            uint freq = 1;
            if (p_stats and p_stats->m_freq >= 0) freq = p_stats->m_freq;
            auto cid_set = f_pruning(freq);
            // for (uint i = 0; i < postingListCount / 3; i++) {
            //     cid_set.insert(p_exWorkSpace->m_postingIDs[i]);
            // }
            if (p_stats)
                p_stats->m_pruningTimer.event_start("0.3 set to vector", false);
            pruned_cid_list.assign(cid_set.begin(), cid_set.end());
            if (p_stats)
                p_stats->m_pruningTimer.event_stop("0.3 set to vector");
            postingListCount = pruned_cid_list.size();
        }
        const auto& cid_list = m_options.m_searchPruning
                                   ? pruned_cid_list
                                   : p_exWorkSpace->m_postingIDs;
        if (p_stats) p_stats->m_pruningTimer.event_stop("0. prune clusters");

        if (p_stats)
            p_stats->m_pruningTimer.event_start("1. fill requests", false);
        for (uint32_t pi = 0; pi < postingListCount; ++pi) {
            auto curPostingID = cid_list[pi];
            ListInfo* listInfo = nullptr;

            int fileid = -1;
            if (m_options.m_loadMergerResult) {
                listInfo = &m_listInfoMap.at(curPostingID);
                fileid = listInfo->mid;
                if (p_stats) {
                    p_stats->record_access_to_file(fileid);
                }
            } else {
                listInfo = &m_listInfos[curPostingID];
                fileid = m_oneContext ? 0 : curPostingID / m_listPerFile;
            }

#ifndef BATCH_READ
            Helper::DiskIO* indexFile = m_indexFiles[fileid].get();
#endif

            diskRead += listInfo->listPageCount;
            diskIO += 1;
            listElements += listInfo->listEleCount;

            size_t totalBytes =
                (static_cast<size_t>(listInfo->listPageCount) << PageSizeEx);
            char* buffer =
                (char*)((p_exWorkSpace->m_pageBuffers[pi]).GetBuffer());

#ifdef ASYNC_READ
            auto& request = p_exWorkSpace->m_diskRequests[pi];
            request.m_offset = listInfo->listOffset;
            request.m_readSize = totalBytes;
            request.m_buffer = buffer;
            request.m_status = (fileid << 16) | p_exWorkSpace->m_spaceID;
            request.m_payload = (void*)listInfo;
            request.m_success = false;

#ifdef BATCH_READ  // async batch read
            request.m_callback = [&p_exWorkSpace, &queryResults, &p_index,
                                  &request, this](bool success) {
                char* buffer = request.m_buffer;
                ListInfo* listInfo = (ListInfo*)(request.m_payload);

                // decompress posting list
                char* p_postingListFullData = buffer + listInfo->pageOffset;
                if (m_enableDataCompression) {
                    DecompressPosting();
                }

                ProcessPosting();
            };
#else  // async read
            request.m_callback = [&p_exWorkSpace, &request](bool success) {
                p_exWorkSpace->m_processIocp.push(&request);
            };

            ++unprocessed;
            if (!(indexFile->ReadFileAsync(request))) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to read file!\n");
                unprocessed--;
            }
#endif
#else  // sync read
            auto numRead =
                indexFile->ReadBinary(totalBytes, buffer, listInfo->listOffset);
            if (numRead != totalBytes) {
                SPTAGLIB_LOG(
                    Helper::LogLevel::LL_Error,
                    "File %s read bytes, expected: %zu, acutal: %llu.\n",
                    m_extraFullGraphFile.c_str(), totalBytes, numRead);
                throw std::runtime_error("File read mismatch");
            }
            // decompress posting list
            char* p_postingListFullData = buffer + listInfo->pageOffset;
            if (m_enableDataCompression) {
                DecompressPosting();
            }

            ProcessPosting();
#endif
        }
        if (p_stats) p_stats->m_pruningTimer.event_stop("1. fill requests");

#ifdef ASYNC_READ
#ifdef BATCH_READ
        if (p_stats)
            p_stats->m_pruningTimer.event_start("2. batch read file", false);
        BatchReadFileAsync(m_indexFiles, (p_exWorkSpace->m_diskRequests).data(),
                           postingListCount);
        if (p_stats) p_stats->m_pruningTimer.event_stop("2. batch read file");
#else
        while (unprocessed > 0) {
            Helper::AsyncReadRequest* request;
            if (!(p_exWorkSpace->m_processIocp.pop(request))) break;

            --unprocessed;
            char* buffer = request->m_buffer;
            ListInfo* listInfo = static_cast<ListInfo*>(request->m_payload);
            // decompress posting list
            char* p_postingListFullData = buffer + listInfo->pageOffset;
            if (m_enableDataCompression) {
                DecompressPosting();
            }

            ProcessPosting();
        }
#endif
#endif
        if (p_stats)
            p_stats->m_pruningTimer.event_start("3. truth and so on", false);
        if (truth) {
            for (uint32_t pi = 0; pi < postingListCount; ++pi) {
                auto curPostingID = p_exWorkSpace->m_postingIDs[pi];

                ListInfo* listInfo = nullptr;
                if (m_options.m_loadMergerResult) {
                    listInfo = &m_listInfoMap.at(curPostingID);
                } else {
                    listInfo = &m_listInfos[curPostingID];
                }

                char* buffer =
                    (char*)((p_exWorkSpace->m_pageBuffers[pi]).GetBuffer());

                char* p_postingListFullData = buffer + listInfo->pageOffset;
                if (m_enableDataCompression) {
                    p_postingListFullData =
                        (char*)p_exWorkSpace->m_decompressBuffer.GetBuffer();
                    if (listInfo->listEleCount != 0) {
                        try {
                            m_pCompressor->Decompress(
                                buffer + listInfo->pageOffset,
                                listInfo->listTotalBytes, p_postingListFullData,
                                listInfo->listEleCount * m_vectorInfoSize,
                                m_enableDictTraining);
                        } catch (std::runtime_error& err) {
                            SPTAGLIB_LOG(
                                Helper::LogLevel::LL_Error,
                                "Decompress postingList %d  failed! %s, \n",
                                curPostingID, err.what());
                            continue;
                        }
                    }
                }

                for (size_t i = 0; i < listInfo->listEleCount; ++i) {
                    uint64_t offsetVectorID =
                        m_enablePostingListRearrange
                            ? (m_vectorInfoSize - sizeof(SPANNVID)) *
                                      listInfo->listEleCount +
                                  sizeof(SPANNVID) * i
                            : m_vectorInfoSize * i;
                    int vectorID = *(reinterpret_cast<int*>(
                        p_postingListFullData + offsetVectorID));
                    if (truth && truth->count(vectorID))
                        (*found)[curPostingID].insert(vectorID);
                }
            }
        }

        if (p_stats) {
            p_stats->m_totalListElementsCount = listElements;
            p_stats->m_diskIOCount = diskIO;
            p_stats->m_diskAccessCount = diskRead;
        }

        if (p_stats) {
            p_stats->m_pruningTimer.event_stop("3. truth and so on");
            p_stats->m_pruningTimer.event_stop("search disk index");
        }
    }

    virtual void SearchIndexWithoutParsing(ExtraWorkSpace* p_exWorkSpace) {
        if (m_options.m_loadMergerResult) {
            LOG_ERROR("not implemented\n");
            throw std::logic_error("");
        }
        const uint32_t postingListCount =
            static_cast<uint32_t>(p_exWorkSpace->m_postingIDs.size());

        int diskRead = 0;
        int diskIO = 0;
        int listElements = 0;

#if defined(ASYNC_READ) && !defined(BATCH_READ)
        int unprocessed = 0;
#endif

        for (uint32_t pi = 0; pi < postingListCount; ++pi) {
            auto curPostingID = p_exWorkSpace->m_postingIDs[pi];
            ListInfo* listInfo = &(m_listInfos[curPostingID]);
            int fileid = m_oneContext ? 0 : curPostingID / m_listPerFile;

#ifndef BATCH_READ
            Helper::DiskIO* indexFile = m_indexFiles[fileid].get();
#endif

            diskRead += listInfo->listPageCount;
            diskIO += 1;
            listElements += listInfo->listEleCount;

            size_t totalBytes =
                (static_cast<size_t>(listInfo->listPageCount) << PageSizeEx);
            char* buffer =
                (char*)((p_exWorkSpace->m_pageBuffers[pi]).GetBuffer());

#ifdef ASYNC_READ
            auto& request = p_exWorkSpace->m_diskRequests[pi];
            request.m_offset = listInfo->listOffset;
            request.m_readSize = totalBytes;
            request.m_buffer = buffer;
            request.m_status = (fileid << 16) | p_exWorkSpace->m_spaceID;
            request.m_payload = (void*)listInfo;
            request.m_success = false;

#ifdef BATCH_READ  // async batch read
            request.m_callback = [this](bool success){
                        //char* buffer = request.m_buffer;
                        //ListInfo* listInfo = (ListInfo*)(request.m_payload);

                        // decompress posting list
                        /*
                        char* p_postingListFullData = buffer + listInfo->pageOffset;
                        if (m_enableDataCompression)
                        {
                            DecompressPosting();
                        }

                        ProcessPosting();
                        */ };
#else  // async read
            request.m_callback = [&p_exWorkSpace, &request](bool success) {
                p_exWorkSpace->m_processIocp.push(&request);
            };

            ++unprocessed;
            if (!(indexFile->ReadFileAsync(request))) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to read file!\n");
                unprocessed--;
            }
#endif
#else  // sync read
            auto numRead =
                indexFile->ReadBinary(totalBytes, buffer, listInfo->listOffset);
            if (numRead != totalBytes) {
                SPTAGLIB_LOG(
                    Helper::LogLevel::LL_Error,
                    "File %s read bytes, expected: %zu, acutal: %llu.\n",
                    m_extraFullGraphFile.c_str(), totalBytes, numRead);
                throw std::runtime_error("File read mismatch");
            }
            // decompress posting list
            /*
            char* p_postingListFullData = buffer + listInfo->pageOffset;
            if (m_enableDataCompression)
            {
                DecompressPosting();
            }

            ProcessPosting();
            */
#endif
        }

#ifdef ASYNC_READ
#ifdef BATCH_READ
        BatchReadFileAsync(m_indexFiles, (p_exWorkSpace->m_diskRequests).data(),
                           postingListCount);
#else
        while (unprocessed > 0) {
            Helper::AsyncReadRequest* request;
            if (!(p_exWorkSpace->m_processIocp.pop(request))) break;

            --unprocessed;
            char* buffer = request->m_buffer;
            ListInfo* listInfo = static_cast<ListInfo*>(request->m_payload);
            // decompress posting list
            /*
            char* p_postingListFullData = buffer + listInfo->pageOffset;
            if (m_enableDataCompression)
            {
                DecompressPosting();
            }

            ProcessPosting();
            */
        }
#endif
#endif
    }

    virtual bool SearchNextInPosting(ExtraWorkSpace* p_exWorkSpace,
                                     QueryResult& p_queryResults,
                                     std::shared_ptr<VectorIndex>& p_index) {
        COMMON::QueryResultSet<ValueType>& queryResults =
            *((COMMON::QueryResultSet<ValueType>*)&p_queryResults);
        bool foundResult = false;
        while (!foundResult &&
               p_exWorkSpace->m_pi < p_exWorkSpace->m_postingIDs.size()) {
            char* buffer =
                (char*)((p_exWorkSpace->m_pageBuffers[p_exWorkSpace->m_pi])
                            .GetBuffer());
            ListInfo* listInfo = static_cast<ListInfo*>(
                p_exWorkSpace->m_diskRequests[p_exWorkSpace->m_pi].m_payload);
            // decompress posting list
            char* p_postingListFullData = buffer + listInfo->pageOffset;
            if (m_enableDataCompression && p_exWorkSpace->m_offset == 0) {
                DecompressPostingIterative();
            }
            ProcessPostingOffset();
        }
        return !(p_exWorkSpace->m_pi == p_exWorkSpace->m_postingIDs.size());
    }

    virtual bool SearchIterativeNext(ExtraWorkSpace* p_exWorkSpace,
                                     QueryResult& p_query,
                                     std::shared_ptr<VectorIndex> p_index) {
        if (p_exWorkSpace->m_loadPosting) {
            SearchIndexWithoutParsing(p_exWorkSpace);
            p_exWorkSpace->m_pi = 0;
            p_exWorkSpace->m_offset = 0;
            p_exWorkSpace->m_loadPosting = false;
        }

        return SearchNextInPosting(p_exWorkSpace, p_query, p_index);
    }

    std::string GetPostingListFullData(
        int postingListId, size_t p_postingListSize, Selection& p_selections,
        std::shared_ptr<VectorSet> p_fullVectors,
        bool p_enableDeltaEncoding = false,
        bool p_enablePostingListRearrange = false,
        const ValueType* headVector = nullptr) const {
        std::string postingListFullData("");
        std::string vectors("");
        std::string vectorIDs("");
        size_t selectIdx = p_selections.lower_bound(postingListId);
        // iterate over all the vectors in the posting list
        for (int i = 0; i < p_postingListSize; ++i) {
            if (p_selections[selectIdx].node != postingListId) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Selection ID NOT MATCH! node:%d offset:%zu\n",
                             postingListId, selectIdx);
                throw std::runtime_error("Selection ID mismatch");
            }
            std::string vectorID("");
            std::string vector("");

            SPANNVID vid = p_selections[selectIdx++].tonode;
            vectorID.append(reinterpret_cast<char*>(&vid), sizeof(vid));

            ValueType* p_vector =
                reinterpret_cast<ValueType*>(p_fullVectors->GetVector(vid));
            if (p_enableDeltaEncoding) {
                DimensionType n = p_fullVectors->Dimension();
                std::vector<ValueType> p_vector_delta(n);
                for (auto j = 0; j < n; j++) {
                    p_vector_delta[j] = p_vector[j] - headVector[j];
                }
                vector.append(reinterpret_cast<char*>(&p_vector_delta[0]),
                              p_fullVectors->PerVectorDataSize());
            } else {
                vector.append(reinterpret_cast<char*>(p_vector),
                              p_fullVectors->PerVectorDataSize());
            }

            if (p_enablePostingListRearrange) {
                vectorIDs += vectorID;
                vectors += vector;
            } else {
                postingListFullData += (vectorID + vector);
            }
        }
        if (p_enablePostingListRearrange) {
            return vectors + vectorIDs;
        }
        return postingListFullData;
    }

    bool BuildIndex(std::shared_ptr<Helper::VectorSetReader>& p_reader,
                    std::shared_ptr<VectorIndex> p_headIndex, Options& p_opt) {
        std::string outputFile =
            p_opt.m_indexDirectory + FolderSep + p_opt.m_ssdIndex;
        if (outputFile.empty()) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Output file can't be empty!\n");
            return false;
        }

        int numThreads = p_opt.m_iSSDNumberOfThreads;
        int candidateNum = p_opt.m_internalResultNum;
        std::unordered_set<SizeType> headVectorIDS;
        if (p_opt.m_headIDFile.empty()) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Not found VectorIDTranslate!\n");
            return false;
        }

        {
            auto ptr = SPTAG::f_createIO();
            if (ptr == nullptr ||
                !ptr->Initialize(
                    (p_opt.m_indexDirectory + FolderSep + p_opt.m_headIDFile)
                        .c_str(),
                    std::ios::binary | std::ios::in)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "failed open VectorIDTranslate: %s\n",
                             p_opt.m_headIDFile.c_str());
                return false;
            }

            std::uint64_t vid;
            while (ptr->ReadBinary(sizeof(vid), reinterpret_cast<char*>(
                                                    &vid)) == sizeof(vid)) {
                headVectorIDS.insert(static_cast<SizeType>(vid));
            }
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Loaded %u Vector IDs\n",
                         static_cast<uint32_t>(headVectorIDS.size()));
        }

        SizeType fullCount = 0;
        size_t vectorInfoSize = 0;
        {
            auto fullVectors = p_reader->GetVectorSet();
            fullCount = fullVectors->Count();
            vectorInfoSize =
                fullVectors->PerVectorDataSize() + sizeof(SPANNVID);
        }

        Selection selections(
            static_cast<size_t>(fullCount) * p_opt.m_replicaCount,
            p_opt.m_tmpdir);
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "Full vector count:%d Edge bytes:%llu selection size:%zu, "
                     "capacity size:%zu\n",
                     fullCount, sizeof(Edge), selections.m_selections.size(),
                     selections.m_selections.capacity());
        std::vector<std::atomic_int> replicaCount(fullCount);
        std::vector<std::atomic_int> postingListSize(
            p_headIndex->GetNumSamples());
        for (auto& pls : postingListSize) pls = 0;
        std::unordered_set<SizeType> emptySet;
        SizeType batchSize =
            (fullCount + p_opt.m_batches - 1) / p_opt.m_batches;

        auto t1 = std::chrono::high_resolution_clock::now();
        if (p_opt.m_batches > 1) {
            if (selections.SaveBatch() != ErrorCode::Success) {
                return false;
            }
        }
        {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                         "Preparation done, start candidate searching.\n");
            SizeType sampleSize = p_opt.m_samples;
            std::vector<SizeType> samples(sampleSize, 0);
            for (int i = 0; i < p_opt.m_batches; i++) {
                SizeType start = i * batchSize;
                SizeType end = min(start + batchSize, fullCount);
                auto fullVectors = p_reader->GetVectorSet(start, end);
                if (p_opt.m_distCalcMethod == DistCalcMethod::Cosine &&
                    !p_reader->IsNormalized() && !p_headIndex->m_pQuantizer)
                    fullVectors->Normalize(p_opt.m_iSSDNumberOfThreads);

                if (p_opt.m_batches > 1) {
                    if (selections.LoadBatch(
                            static_cast<size_t>(start) * p_opt.m_replicaCount,
                            static_cast<size_t>(end) * p_opt.m_replicaCount) !=
                        ErrorCode::Success) {
                        return false;
                    }
                    emptySet.clear();
                    for (auto vid : headVectorIDS) {
                        if (vid >= start && vid < end)
                            emptySet.insert(vid - start);
                    }
                } else {
                    emptySet = headVectorIDS;
                }

                int sampleNum = 0;
                for (int j = start; j < end && sampleNum < sampleSize; j++) {
                    if (headVectorIDS.count(j) == 0)
                        samples[sampleNum++] = j - start;
                }

                float acc = 0;
#pragma omp parallel for schedule(dynamic)
                for (int j = 0; j < sampleNum; j++) {
                    COMMON::Utils::atomic_float_add(
                        &acc,
                        COMMON::TruthSet::CalculateRecall(
                            p_headIndex.get(),
                            fullVectors->GetVector(samples[j]), candidateNum));
                }
                acc = acc / sampleNum;
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                             "Batch %d vector(%d,%d) loaded with %d vectors "
                             "(%zu) HeadIndex acc @%d:%f.\n",
                             i, start, end, fullVectors->Count(),
                             selections.m_selections.size(), candidateNum, acc);

                p_headIndex->ApproximateRNG(
                    fullVectors, emptySet, candidateNum,
                    selections.m_selections.data(), p_opt.m_replicaCount,
                    numThreads, p_opt.m_gpuSSDNumTrees, p_opt.m_gpuSSDLeafSize,
                    p_opt.m_rngFactor, p_opt.m_numGPUs);
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Batch %d finished!\n",
                             i);

                for (SizeType j = start; j < end; j++) {
                    replicaCount[j] = 0;
                    size_t vecOffset = j * (size_t)p_opt.m_replicaCount;
                    if (headVectorIDS.count(j) == 0) {
                        for (int resNum = 0;
                             resNum < p_opt.m_replicaCount &&
                             selections[vecOffset + resNum].node != INT_MAX;
                             resNum++) {
                            ++postingListSize[selections[vecOffset + resNum]
                                                  .node];
                            selections[vecOffset + resNum].tonode = j;
                            ++replicaCount[j];
                        }
                    }
                }

                if (p_opt.m_batches > 1) {
                    if (selections.SaveBatch() != ErrorCode::Success) {
                        return false;
                    }
                }
            }
        }
        auto t2 = std::chrono::high_resolution_clock::now();
        SPTAGLIB_LOG(
            Helper::LogLevel::LL_Info,
            "Searching replicas ended. Search Time: %.2lf mins\n",
            ((double)std::chrono::duration_cast<std::chrono::seconds>(t2 - t1)
                 .count()) /
                60.0);

        if (p_opt.m_batches > 1) {
            if (selections.LoadBatch(
                    0, static_cast<size_t>(fullCount) * p_opt.m_replicaCount) !=
                ErrorCode::Success) {
                return false;
            }
        }

        // Sort results either in CPU or GPU
        VectorIndex::SortSelections(&selections.m_selections);

        auto t3 = std::chrono::high_resolution_clock::now();
        SPTAGLIB_LOG(
            Helper::LogLevel::LL_Info, "Time to sort selections:%.2lf sec.\n",
            ((double)std::chrono::duration_cast<std::chrono::seconds>(t3 - t2)
                 .count()) +
                ((double)std::chrono::duration_cast<std::chrono::milliseconds>(
                     t3 - t2)
                     .count()) /
                    1000);

        int postingSizeLimit = INT_MAX;
        if (p_opt.m_postingPageLimit > 0) {
            p_opt.m_postingPageLimit = max(
                p_opt.m_postingPageLimit,
                static_cast<int>((p_opt.m_postingVectorLimit * vectorInfoSize +
                                  PageSize - 1) /
                                 PageSize));
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                         "Build index with posting page limit:%d\n",
                         p_opt.m_postingPageLimit);
            postingSizeLimit = static_cast<int>(p_opt.m_postingPageLimit *
                                                PageSize / vectorInfoSize);
        }

        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Posting size limit: %d\n",
                     postingSizeLimit);

        {
            std::vector<int> replicaCountDist(p_opt.m_replicaCount + 1, 0);
            for (int i = 0; i < replicaCount.size(); ++i) {
                if (headVectorIDS.count(i) > 0) continue;
                ++replicaCountDist[replicaCount[i]];
            }

            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Before Posting Cut:\n");
            for (int i = 0; i < replicaCountDist.size(); ++i) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                             "Replica Count Dist: %d, %d\n", i,
                             replicaCountDist[i]);
            }
        }

#pragma omp parallel for schedule(dynamic)
        for (int i = 0; i < postingListSize.size(); ++i) {
            if (postingListSize[i] <= postingSizeLimit) continue;

            std::size_t selectIdx =
                std::lower_bound(selections.m_selections.begin(),
                                 selections.m_selections.end(), i,
                                 Selection::g_edgeComparer) -
                selections.m_selections.begin();

            for (size_t dropID = postingSizeLimit; dropID < postingListSize[i];
                 ++dropID) {
                int tonode = selections.m_selections[selectIdx + dropID].tonode;
                --replicaCount[tonode];
            }
            postingListSize[i] = postingSizeLimit;
        }

        if (p_opt.m_outputEmptyReplicaID) {
            std::vector<int> replicaCountDist(p_opt.m_replicaCount + 1, 0);
            auto ptr = SPTAG::f_createIO();
            if (ptr == nullptr ||
                !ptr->Initialize("EmptyReplicaID.bin",
                                 std::ios::binary | std::ios::out)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Fail to create EmptyReplicaID.bin!\n");
                return false;
            }
            for (int i = 0; i < replicaCount.size(); ++i) {
                if (headVectorIDS.count(i) > 0) continue;

                ++replicaCountDist[replicaCount[i]];

                if (replicaCount[i] < 2) {
                    long long vid = i;
                    if (ptr->WriteBinary(sizeof(vid),
                                         reinterpret_cast<char*>(&vid)) !=
                        sizeof(vid)) {
                        SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                                     "Failt to write EmptyReplicaID.bin!");
                        return false;
                    }
                }
            }

            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "After Posting Cut:\n");
            for (int i = 0; i < replicaCountDist.size(); ++i) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                             "Replica Count Dist: %d, %d\n", i,
                             replicaCountDist[i]);
            }
        }

        auto t4 = std::chrono::high_resolution_clock::now();
        SPTAGLIB_LOG(
            SPTAG::Helper::LogLevel::LL_Info,
            "Time to perform posting cut:%.2lf sec.\n",
            ((double)std::chrono::duration_cast<std::chrono::seconds>(t4 - t3)
                 .count()) +
                ((double)std::chrono::duration_cast<std::chrono::milliseconds>(
                     t4 - t3)
                     .count()) /
                    1000);

        // number of posting lists per file
        size_t postingFileSize =
            (postingListSize.size() + p_opt.m_ssdIndexFileNum - 1) /
            p_opt.m_ssdIndexFileNum;
        std::vector<size_t> selectionsBatchOffset(p_opt.m_ssdIndexFileNum + 1,
                                                  0);
        for (int i = 0; i < p_opt.m_ssdIndexFileNum; i++) {
            size_t curPostingListEnd =
                min(postingListSize.size(), (i + 1) * postingFileSize);
            selectionsBatchOffset[i + 1] =
                std::lower_bound(selections.m_selections.begin(),
                                 selections.m_selections.end(),
                                 (SizeType)curPostingListEnd,
                                 Selection::g_edgeComparer) -
                selections.m_selections.begin();
        }

        if (p_opt.m_ssdIndexFileNum > 1) {
            if (selections.SaveBatch() != ErrorCode::Success) {
                return false;
            }
        }

        auto fullVectors = p_reader->GetVectorSet();
        if (p_opt.m_distCalcMethod == DistCalcMethod::Cosine &&
            !p_reader->IsNormalized() && !p_headIndex->m_pQuantizer)
            fullVectors->Normalize(p_opt.m_iSSDNumberOfThreads);

        // iterate over files
        for (int i = 0; i < p_opt.m_ssdIndexFileNum; i++) {
            size_t curPostingListOffSet = i * postingFileSize;
            size_t curPostingListEnd =
                min(postingListSize.size(), (i + 1) * postingFileSize);
            // postingListSize: number of vectors in the posting list, type
            // vector<int>
            std::vector<int> curPostingListSizes(
                postingListSize.begin() + curPostingListOffSet,
                postingListSize.begin() + curPostingListEnd);

            std::vector<size_t> curPostingListBytes(curPostingListSizes.size());

            if (p_opt.m_ssdIndexFileNum > 1) {
                if (selections.LoadBatch(selectionsBatchOffset[i],
                                         selectionsBatchOffset[i + 1]) !=
                    ErrorCode::Success) {
                    return false;
                }
            }
            // create compressor
            if (p_opt.m_enableDataCompression && i == 0) {
                m_pCompressor = std::make_unique<Compressor>(
                    p_opt.m_zstdCompressLevel, p_opt.m_dictBufferCapacity);
                // train dict
                if (p_opt.m_enableDictTraining) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                                 "Training dictionary...\n");
                    std::string samplesBuffer("");
                    std::vector<size_t> samplesSizes;
                    for (int j = 0; j < curPostingListSizes.size(); j++) {
                        if (curPostingListSizes[j] == 0) {
                            continue;
                        }
                        ValueType* headVector = nullptr;
                        if (p_opt.m_enableDeltaEncoding) {
                            headVector = (ValueType*)p_headIndex->GetSample(j);
                        }
                        std::string postingListFullData =
                            GetPostingListFullData(
                                j, curPostingListSizes[j], selections,
                                fullVectors, p_opt.m_enableDeltaEncoding,
                                p_opt.m_enablePostingListRearrange, headVector);

                        samplesBuffer += postingListFullData;
                        samplesSizes.push_back(postingListFullData.size());
                        if (samplesBuffer.size() >
                            p_opt.m_minDictTraingBufferSize)
                            break;
                    }
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                                 "Using the first %zu postingLists to train "
                                 "dictionary... \n",
                                 samplesSizes.size());
                    std::size_t dictSize = m_pCompressor->TrainDict(
                        samplesBuffer, &samplesSizes[0],
                        (unsigned int)samplesSizes.size());
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                                 "Dictionary trained, dictionary size: %zu \n",
                                 dictSize);
                }
            }

            if (p_opt.m_enableDataCompression) {
                SPTAGLIB_LOG(
                    Helper::LogLevel::LL_Info,
                    "Getting compressed size of each posting list...\n");
#pragma omp parallel for schedule(dynamic)
                for (int j = 0; j < curPostingListSizes.size(); j++) {
                    SizeType postingListId = j + (SizeType)curPostingListOffSet;
                    // do not compress if no data
                    if (postingListSize[postingListId] == 0) {
                        curPostingListBytes[j] = 0;
                        continue;
                    }
                    ValueType* headVector = nullptr;
                    if (p_opt.m_enableDeltaEncoding) {
                        headVector =
                            (ValueType*)p_headIndex->GetSample(postingListId);
                    }
                    std::string postingListFullData = GetPostingListFullData(
                        postingListId, postingListSize[postingListId],
                        selections, fullVectors, p_opt.m_enableDeltaEncoding,
                        p_opt.m_enablePostingListRearrange, headVector);
                    size_t sizeToCompress =
                        postingListSize[postingListId] * vectorInfoSize;
                    if (sizeToCompress != postingListFullData.size()) {
                        SPTAGLIB_LOG(
                            Helper::LogLevel::LL_Error,
                            "Size to compress NOT MATCH! PostingListFullData "
                            "size: %zu sizeToCompress: %zu \n",
                            postingListFullData.size(), sizeToCompress);
                    }
                    curPostingListBytes[j] = m_pCompressor->GetCompressedSize(
                        postingListFullData, p_opt.m_enableDictTraining);
                    if (postingListId % 10000 == 0 ||
                        curPostingListBytes[j] >
                            static_cast<uint64_t>(p_opt.m_postingPageLimit) *
                                PageSize) {
                        SPTAGLIB_LOG(
                            Helper::LogLevel::LL_Info,
                            "Posting list %d/%d, compressed size: %d, "
                            "compression ratio: %.4f\n",
                            postingListId, postingListSize.size(),
                            curPostingListBytes[j],
                            curPostingListBytes[j] / float(sizeToCompress));
                    }
                }
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                             "Getted compressed size for all the %d posting "
                             "lists in SSD Index file %d.\n",
                             curPostingListBytes.size(), i);
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                             "Mean compressed size: %.4f \n",
                             std::accumulate(curPostingListBytes.begin(),
                                             curPostingListBytes.end(), 0.0) /
                                 curPostingListBytes.size());
                SPTAGLIB_LOG(
                    Helper::LogLevel::LL_Info,
                    "Mean compression ratio: %.4f \n",
                    std::accumulate(curPostingListBytes.begin(),
                                    curPostingListBytes.end(), 0.0) /
                        (std::accumulate(curPostingListSizes.begin(),
                                         curPostingListSizes.end(), 0.0) *
                         vectorInfoSize));
            } else {
                for (int j = 0; j < curPostingListSizes.size(); j++) {
                    curPostingListBytes[j] =
                        curPostingListSizes[j] * vectorInfoSize;
                }
            }

            std::unique_ptr<int[]> postPageNum;
            std::unique_ptr<std::uint16_t[]> postPageOffset;
            std::vector<SPANNCID> postingOrderInIndex;
            SelectPostingOffset(curPostingListBytes, postPageNum,
                                postPageOffset, postingOrderInIndex);

            OutputSSDIndexFile(
                (i == 0) ? outputFile : outputFile + "_" + std::to_string(i),
                p_opt.m_enableDeltaEncoding, p_opt.m_enablePostingListRearrange,
                p_opt.m_enableDataCompression, p_opt.m_enableDictTraining,
                vectorInfoSize, curPostingListSizes, curPostingListBytes,
                p_headIndex, selections, postPageNum, postPageOffset,
                postingOrderInIndex, fullVectors, curPostingListOffSet);
        }

        auto t5 = std::chrono::high_resolution_clock::now();
        auto elapsedSeconds =
            std::chrono::duration_cast<std::chrono::seconds>(t5 - t1).count();
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "Total used time: %.2lf minutes (about %.2lf hours).\n",
                     elapsedSeconds / 60.0, elapsedSeconds / 3600.0);

        return true;
    }

    virtual bool CheckValidPosting(SizeType postingID) {
        if (not m_options.m_loadMergerResult) {
            return m_listInfos[postingID].listEleCount != 0;
        }
        if (m_listInfoMap.count(postingID)) {
            if (m_listInfoMap[postingID].listEleCount == 0) {
                LOG_ERROR("unexpected zero-sized cluster (cid=%u)\n",
                          postingID);
                throw std::runtime_error("");
            }
            return true;
        } else {
            return false;
        }
    }

    virtual void DeInit(bool keep_mr) { Reset(); }
    virtual Helper::vector_recovery<SPANNCID>& GetPostingOrder() {
        return m_postingOrderInIndex;
    }
    virtual size_t GetNumNodes() const { return m_numNodes; }
    virtual ErrorCode GetPostingList(
        ExtraWorkSpace* p_exWorkSpace, std::shared_ptr<VectorIndex> p_index,
        const SPMerge::ClusterIDList& cid_list,
        SPMerge::ClusterContentList& cluster_content,
        std::function<void(uint64_t, void*)> f_handle_vector) const;

    virtual ErrorCode GetPostingDebug(ExtraWorkSpace* p_exWorkSpace,
                                      std::shared_ptr<VectorIndex> p_index,
                                      SizeType vid, std::vector<SizeType>& VIDs,
                                      std::shared_ptr<VectorSet>& vecs) {
        if (m_options.m_loadMergerResult) {
            LOG_ERROR("not implemented\n");
            throw std::logic_error("");
        }
        VIDs.clear();

        SizeType curPostingID = vid;
        ListInfo* listInfo = &(m_listInfos[curPostingID]);
        VIDs.resize(listInfo->listEleCount);
        ByteArray vector_array = ByteArray::Alloc(
            sizeof(ValueType) * listInfo->listEleCount * m_iDataDimension);
        vecs.reset(
            new BasicVectorSet(vector_array, GetEnumValueType<ValueType>(),
                               m_iDataDimension, listInfo->listEleCount));

        int fileid = m_oneContext ? 0 : curPostingID / m_listPerFile;

#ifndef BATCH_READ
        Helper::DiskIO* indexFile = m_indexFiles[fileid].get();
#endif

        size_t totalBytes =
            (static_cast<size_t>(listInfo->listPageCount) << PageSizeEx);
        char* buffer = (char*)((p_exWorkSpace->m_pageBuffers[0]).GetBuffer());

#ifdef ASYNC_READ
        auto& request = p_exWorkSpace->m_diskRequests[0];
        request.m_offset = listInfo->listOffset;
        request.m_readSize = totalBytes;
        request.m_buffer = buffer;
        request.m_status = (fileid << 16) | p_exWorkSpace->m_spaceID;
        request.m_payload = (void*)listInfo;
        request.m_success = false;

#ifdef BATCH_READ  // async batch read
        request.m_callback = [&p_exWorkSpace, &vecs, &VIDs, &p_index, &request,
                              this](bool success) {
            char* buffer = request.m_buffer;
            ListInfo* listInfo = (ListInfo*)(request.m_payload);

            // decompress posting list
            char* p_postingListFullData = buffer + listInfo->pageOffset;
            if (m_enableDataCompression) {
                DecompressPosting();
            }

            for (int i = 0; i < listInfo->listEleCount; i++) {
                uint64_t offsetVectorID, offsetVector;
                (this->*m_parsePosting)(offsetVectorID, offsetVector, i,
                                        listInfo->listEleCount);
                int vectorID = *(reinterpret_cast<int*>(p_postingListFullData +
                                                        offsetVectorID));
                (this->*m_parseEncoding)(
                    p_index, listInfo,
                    (ValueType*)(p_postingListFullData + offsetVector));
                VIDs[i] = vectorID;
                auto outVec = vecs->GetVector(i);
                memcpy(outVec, (void*)(p_postingListFullData + offsetVector),
                       sizeof(ValueType) * m_iDataDimension);
            }
        };
#else  // async read
        request.m_callback = [&p_exWorkSpace, &request](bool success) {
            p_exWorkSpace->m_processIocp.push(&request);
        };

        ++unprocessed;
        if (!(indexFile->ReadFileAsync(request))) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed to read file!\n");
            unprocessed--;
        }
#endif
#else  // sync read
        auto numRead =
            indexFile->ReadBinary(totalBytes, buffer, listInfo->listOffset);
        if (numRead != totalBytes) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "File %s read bytes, expected: %zu, acutal: %llu.\n",
                         m_extraFullGraphFile.c_str(), totalBytes, numRead);
            throw std::runtime_error("File read mismatch");
        }
        // decompress posting list
        char* p_postingListFullData = buffer + listInfo->pageOffset;
        if (m_enableDataCompression) {
            DecompressPosting();
        }

        for (int i = 0; i < listInfo->listEleCount; i++) {
            uint64_t offsetVectorID, offsetVector;
            (this->*m_parsePosting)(offsetVectorID, offsetVector, i,
                                    listInfo->listEleCount);
            int vectorID = *(
                reinterpret_cast<int*>(p_postingListFullData + offsetVectorID));
            (this->*m_parseEncoding)(
                p_index, listInfo,
                (ValueType*)(p_postingListFullData + offsetVector));
            VIDs[i] = vectorID;
            auto outVec = vecs->GetVector(i);
            memcpy(outVec, (void*)(p_postingListFullData + offsetVector),
                   sizeof(ValueType) * m_iDataDimension);
        }
#endif
#ifdef ASYNC_READ
#ifdef BATCH_READ
        BatchReadFileAsync(m_indexFiles, (p_exWorkSpace->m_diskRequests).data(),
                           1);
#else
        while (unprocessed > 0) {
            Helper::AsyncReadRequest* request;
            if (!(p_exWorkSpace->m_processIocp.pop(request))) break;

            --unprocessed;
            char* buffer = request->m_buffer;
            ListInfo* listInfo = static_cast<ListInfo*>(request->m_payload);
            // decompress posting list
            char* p_postingListFullData = buffer + listInfo->pageOffset;
            if (m_enableDataCompression) {
                DecompressPosting();
            }

            ProcessPosting();
        }
#endif
#endif
        return ErrorCode::Success;
    }

    virtual void SetRecoveryMode(bool recovery) { m_recoveryMode = recovery; }
    virtual SPMerge::MergerResult& GetMergerResult() { return *m_recovery_mr; }
    virtual const ListInfosType& GetListInfo() const { return m_listInfos; }
    virtual const ListInfoMapType& GetListInfoMap() const {
        return m_listInfoMap;
    }

    virtual void ClusterAssignmentToOrder(
        SPMerge::MergerResult& mr,
        std::shared_ptr<Helper::VectorSetReader>& p_reader,
        std::shared_ptr<VectorIndex> p_headIndex = nullptr) const;
    virtual void SaveMergerResultFiles(
        const SPMerge::MergerResult& mr,
        std::shared_ptr<Helper::VectorSetReader>& p_reader,
        std::shared_ptr<VectorIndex> p_headIndex = nullptr) const;

   private:
    void LAR_PM_PLUS(SPMerge::MachineID healthy_mid,
                     SPMerge::MachineID fail_mid);
    // Below are all outdated codes.
    // Should pay attention to:
    // - now cluster assignment is in postingInIndex order, so order compute
    //   cost may be saved
    // -
    void LAR_PM(SPMerge::MachineID healthy_mid, SPMerge::MachineID fail_mid);
    void LAR_CR(SPMerge::MachineID healthy_mid, SPMerge::MachineID fail_mid);
    void LAR_SL(SPMerge::MachineID healthy_mid, SPMerge::MachineID fail_mid);
    void LAR_GL(SPMerge::MachineID healthy_mid, SPMerge::MachineID fail_mid);
    void LAR_SC(SPMerge::MachineID healthy_mid, SPMerge::MachineID fail_mid);
    void LAR_SW(SPMerge::MachineID healthy_mid, SPMerge::MachineID fail_mid);
    void Reset(bool keep_mr = false) {
        m_listInfoMap.clear();
        m_patchMeta.clear();
        m_listInfos.clear();
        m_patchFiles.clear();
        m_indexFiles.clear();
        m_pCompressor.reset();
        m_vectorInfoSize = m_iDataDimension = m_totalListCount = m_listPerFile =
            0;

        // recovery
        if (not keep_mr) m_recovery_mr.reset();
        m_recovery_pbuf_vec.reset();
        m_postingOrderInIndex.clear();
        // for pure memory recovery
        m_pmData.Reset();
    }
    // @mid:
    // - InvalidMID: default load all
    // - otherwise: load exactly mid
    // @n_clusters: #clusters on machine mid
    ClusterContentSelection SelectClusterContent(const uint64_t* sub_off,
                                                 const PatchMetadata& p_meta,
                                                 size_t n_clusters,
                                                 SPMerge::MachineID mid);
    void LoadingHeadInfo(Options& p_opt);
    // @cc_load_which_mid:
    // - InvalidMID: dafault load all
    // - otherwise: load exactly cc_load_which_mid
    void LoadingPatchMeta(
        const SPMerge::MachineID mid,
        std::vector<SPANNCID>& p_postingOrderInIndex,
        SPMerge::MachineID cc_load_which_mid = SPMerge::InvalidMID);
    void LoadingPatchFullData(
        const SPMerge::MachineID mid,
        std::vector<SPANNCID>& p_postingOrderInIndex,
        SPMerge::MachineID cc_load_which_mid = SPMerge::InvalidMID,
        std::promise<void>* prm_patch_meta_done = nullptr,
        std::promise<void>* prm_assignment_done = nullptr,
        std::promise<void>* prm_patch_buff_done = nullptr,
        std::promise<void>* prm_cluster_content_done = nullptr);

    // load cluster assignment into mr.cluster_assignment[mid]
    void LoadingClusterAssignment(const SPMerge::MachineID mid);
    // load patch into mr.patch[mid]
    void LoadingPatchVIDs(const SPMerge::MachineID mid);
    // scan cluster content belonging to machine `mid`, for each vid, if not
    // visited, set it and add the vector & cluster into list
    void CheckClusterContent(const SPMerge::MachineID mid,
                             const bool target_value,
                             std::vector<std::atomic_bool>& visited,
                             SPMerge::ClusterIDList& cid_list_output,
                             SPMerge::VectorIDList& vid_list_output, void* pool,
                             std::future<void>* when_to_load);
    // given a memory area, load cluster content from it
    void LoadingClusterContent(const char* buffer, size_t totalClusters);

    int LoadingHeadInfo(
        const std::string& p_file, int p_postingPageLimit,
        const SPMerge::MachineID mid = -1,
        const std::vector<SPANNCID>* p_postingOrderInIndex = nullptr) {
        std::shared_ptr<Helper::DiskIO> ptr;
        if (m_recoveryMode and m_pmData.healthy_ibuf != nullptr) {
            ptr.reset(new Helper::SimpleBufferIO);
            ptr->Initialize(m_pmData.healthy_ibuf.get(), 0,
                            m_pmData.healthy_isize);
        } else {
            ptr = SPTAG::f_createIO();
            if (ptr == nullptr ||
                !ptr->Initialize(p_file.c_str(),
                                 std::ios::binary | std::ios::in)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to open file: %s\n", p_file.c_str());
                throw std::runtime_error("Failed open file in LoadingHeadInfo");
            }
        }
        m_pCompressor = std::make_unique<Compressor>();  // no need compress
                                                         // level to decompress

        SPANNCID m_listCount;
        SPANNVID m_totalDocumentCount;
        int m_listPageOffset;

        if (ptr->ReadBinary(sizeof(m_listCount),
                            reinterpret_cast<char*>(&m_listCount)) !=
            sizeof(m_listCount)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Failed to read head info file!\n");
            throw std::runtime_error("Failed read file in LoadingHeadInfo");
        }
        if (ptr->ReadBinary(sizeof(m_totalDocumentCount),
                            reinterpret_cast<char*>(&m_totalDocumentCount)) !=
            sizeof(m_totalDocumentCount)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Failed to read head info file!\n");
            throw std::runtime_error("Failed read file in LoadingHeadInfo");
        }
        if (ptr->ReadBinary(sizeof(m_iDataDimension),
                            reinterpret_cast<char*>(&m_iDataDimension)) !=
            sizeof(m_iDataDimension)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Failed to read head info file!\n");
            throw std::runtime_error("Failed read file in LoadingHeadInfo");
        }
        if (ptr->ReadBinary(sizeof(m_listPageOffset),
                            reinterpret_cast<char*>(&m_listPageOffset)) !=
            sizeof(m_listPageOffset)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Failed to read head info file!\n");
            throw std::runtime_error("Failed read file in LoadingHeadInfo");
        }

        if (m_vectorInfoSize == 0)
            m_vectorInfoSize =
                m_iDataDimension * sizeof(ValueType) + sizeof(SPANNVID);
        else if (m_vectorInfoSize !=
                 m_iDataDimension * sizeof(ValueType) + sizeof(SPANNVID)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Failed to read head info file! DataDimension and "
                         "ValueType are not match!\n");
            throw std::runtime_error(
                "DataDimension and ValueType don't match in "
                "LoadingHeadInfo");
        }

        size_t totalListCount = m_listInfos.size();
        if (m_options.m_loadMergerResult) {
        } else {
            m_listInfos.resize(totalListCount + m_listCount);
        }
        size_t totalListElementCount = 0;

        std::map<int, int> pageCountDist;

        size_t biglistCount = 0;
        size_t biglistElementCount = 0;
        int pageNum;
        ListInfo* infoPtr;

        auto f_read_listinfo = [&]() {
            if (m_enableDataCompression) {
                if (ptr->ReadBinary(
                        sizeof(infoPtr->listTotalBytes),
                        reinterpret_cast<char*>(&(infoPtr->listTotalBytes))) !=
                    sizeof(infoPtr->listTotalBytes)) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                                 "Failed to read head info file!\n");
                    throw std::runtime_error(
                        "Failed read file in LoadingHeadInfo");
                }
            }
            if (ptr->ReadBinary(sizeof(pageNum),
                                reinterpret_cast<char*>(&(pageNum))) !=
                sizeof(pageNum)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to read head info file!\n");
                throw std::runtime_error("Failed read file in LoadingHeadInfo");
            }
            if (ptr->ReadBinary(
                    sizeof(infoPtr->pageOffset),
                    reinterpret_cast<char*>(&(infoPtr->pageOffset))) !=
                sizeof(infoPtr->pageOffset)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to read head info file!\n");
                throw std::runtime_error("Failed read file in LoadingHeadInfo");
            }
            if (ptr->ReadBinary(
                    sizeof(infoPtr->listEleCount),
                    reinterpret_cast<char*>(&(infoPtr->listEleCount))) !=
                sizeof(infoPtr->listEleCount)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to read head info file!\n");
                throw std::runtime_error("Failed read file in LoadingHeadInfo");
            }
            if (ptr->ReadBinary(
                    sizeof(infoPtr->listPageCount),
                    reinterpret_cast<char*>(&(infoPtr->listPageCount))) !=
                sizeof(infoPtr->listPageCount)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to read head info file!\n");
                throw std::runtime_error("Failed read file in LoadingHeadInfo");
            }
            infoPtr->listOffset =
                (static_cast<uint64_t>(m_listPageOffset + pageNum)
                 << PageSizeEx);
            if (!m_enableDataCompression) {
                infoPtr->listTotalBytes =
                    infoPtr->listEleCount * m_vectorInfoSize;
                infoPtr->listEleCount =
                    min(infoPtr->listEleCount,
                        (min(static_cast<int>(infoPtr->listPageCount),
                             p_postingPageLimit)
                         << PageSizeEx) /
                            m_vectorInfoSize);
                infoPtr->listPageCount = static_cast<std::uint16_t>(
                    ceil((m_vectorInfoSize * infoPtr->listEleCount +
                          infoPtr->pageOffset) *
                         1.0 / (1 << PageSizeEx)));
            }
            totalListElementCount += infoPtr->listEleCount;
            int pageCount = infoPtr->listPageCount;

            if (pageCount > 1) {
                ++biglistCount;
                biglistElementCount += infoPtr->listEleCount;
            }

            if (pageCountDist.count(pageCount) == 0) {
                pageCountDist[pageCount] = 1;
            } else {
                pageCountDist[pageCount] += 1;
            }
        };
        if (m_options.m_loadMergerResult) {
            ListInfo info;
            infoPtr = &info;
            m_listInfoMap.reserve(p_postingOrderInIndex->size());
            for (SPMerge::ClusterID cid : *p_postingOrderInIndex) {
                info.cid = cid;
                info.mid = mid;
                f_read_listinfo();
                m_listInfoMap.insert(std::make_pair(cid, info));
                if (infoPtr->listEleCount == 0) {
                    LOG_ERROR("cluster %u listEleCount==0\n", cid);
                }
            }
        } else {
            for (int i = 0; i < m_listCount; ++i) {
                infoPtr = &(m_listInfos[totalListCount + i]);
                infoPtr->cid = totalListCount + i;
                f_read_listinfo();
            }
        }

        if (m_enableDataCompression && m_enableDictTraining) {
            size_t dictBufferSize;
            if (ptr->ReadBinary(sizeof(size_t),
                                reinterpret_cast<char*>(&dictBufferSize)) !=
                sizeof(dictBufferSize)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to read head info file!\n");
                throw std::runtime_error("Failed read file in LoadingHeadInfo");
            }
            char* dictBuffer = new char[dictBufferSize];
            if (ptr->ReadBinary(dictBufferSize, dictBuffer) != dictBufferSize) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to read head info file!\n");
                throw std::runtime_error("Failed read file in LoadingHeadInfo");
            }
            try {
                m_pCompressor->SetDictBuffer(
                    std::string(dictBuffer, dictBufferSize));
            } catch (std::runtime_error& err) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to read head info file: %s \n",
                             err.what());
                throw std::runtime_error("Failed read file in LoadingHeadInfo");
            }
            delete[] dictBuffer;
        }

        LOG_INFO(
            "Finish reading header info, list count %d, total doc "
            "count %d, dimension %d, list page offset %d.\n",
            m_listCount, m_totalDocumentCount, m_iDataDimension,
            m_listPageOffset);

        LOG_INFO("Big page (>4K): list count %zu, total element count %zu.\n",
                 biglistCount, biglistElementCount);

        LOG_INFO("Total Element Count: %llu\n", totalListElementCount);

        for (auto& ele : pageCountDist) {
            LOG_INFO("Page Count Dist: %d %d\n", ele.first, ele.second);
        }

        return m_listCount;
    }

    inline void ParsePostingListRearrange(uint64_t& offsetVectorID,
                                          uint64_t& offsetVector, int i,
                                          int eleCount) const {
        offsetVectorID = (m_vectorInfoSize - sizeof(SPANNVID)) * eleCount +
                         sizeof(SPANNVID) * i;
        offsetVector = (m_vectorInfoSize - sizeof(SPANNVID)) * i;
    }

    inline void ParsePostingList(uint64_t& offsetVectorID,
                                 uint64_t& offsetVector, int i,
                                 int eleCount) const {
        offsetVectorID = m_vectorInfoSize * i;
        offsetVector = offsetVectorID + sizeof(SPANNVID);
    }

    inline void ParseDeltaEncoding(std::shared_ptr<VectorIndex>& p_index,
                                   ListInfo* p_info, ValueType* vector) const {
        ValueType* headVector = (ValueType*)p_index->GetSample(
            (SizeType)(p_info - m_listInfos.data()));
        COMMON::SIMDUtils::ComputeSum(vector, headVector, m_iDataDimension);
    }

    inline void ParseEncoding(std::shared_ptr<VectorIndex>& p_index,
                              ListInfo* p_info, ValueType* vector) const {}

    void SelectPostingOffset(const std::vector<size_t>& p_postingListBytes,
                             std::unique_ptr<int[]>& p_postPageNum,
                             std::unique_ptr<std::uint16_t[]>& p_postPageOffset,
                             std::vector<SPANNCID>& p_postingOrderInIndex) {
        p_postPageNum.reset(new int[p_postingListBytes.size()]);
        p_postPageOffset.reset(new std::uint16_t[p_postingListBytes.size()]);

        struct PageModWithID {
            SPANNCID id;

            std::uint16_t rest;
        };

        struct PageModeWithIDCmp {
            bool operator()(const PageModWithID& a,
                            const PageModWithID& b) const {
                return a.rest == b.rest ? a.id < b.id : a.rest > b.rest;
            }
        };

        std::set<PageModWithID, PageModeWithIDCmp> listRestSize;

        p_postingOrderInIndex.clear();
        p_postingOrderInIndex.reserve(p_postingListBytes.size());

        PageModWithID listInfo;
        for (size_t i = 0; i < p_postingListBytes.size(); ++i) {
            if (p_postingListBytes[i] == 0) {
                continue;
            }

            listInfo.id = static_cast<int>(i);
            listInfo.rest =
                static_cast<std::uint16_t>(p_postingListBytes[i] % PageSize);

            listRestSize.insert(listInfo);
        }

        listInfo.id = -1;

        int currPageNum = 0;
        std::uint16_t currOffset = 0;

        while (!listRestSize.empty()) {
            listInfo.rest = PageSize - currOffset;
            auto iter =
                listRestSize.lower_bound(listInfo);  // avoid page-crossing
            if (iter == listRestSize.end() ||
                (listInfo.rest != PageSize && iter->rest == 0)) {
                ++currPageNum;
                currOffset = 0;
            } else {
                p_postPageNum[iter->id] = currPageNum;
                p_postPageOffset[iter->id] = currOffset;

                p_postingOrderInIndex.push_back(iter->id);

                currOffset += iter->rest;
                if (currOffset > PageSize) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                                 "Crossing extra pages\n");
                    throw std::runtime_error("Read too many pages");
                }

                if (currOffset == PageSize) {
                    ++currPageNum;
                    currOffset = 0;
                }

                currPageNum +=
                    static_cast<int>(p_postingListBytes[iter->id] / PageSize);

                listRestSize.erase(iter);
            }
        }

        SPTAGLIB_LOG(
            Helper::LogLevel::LL_Info,
            "TotalPageNumbers: %d, IndexSize: %llu\n", currPageNum,
            static_cast<uint64_t>(currPageNum) * PageSize + currOffset);
    }
    static inline uint64_t GetIndexMetaSize(size_t n_clusters) {
        uint64_t size = 0;
        size += sizeof(int) * 2 + sizeof(SPANNCID) + sizeof(SPANNVID);
        size +=
            (sizeof(int) + sizeof(uint16_t) + sizeof(int) + sizeof(uint16_t)) *
            n_clusters;
        size = PAGE_ROUND_UP(size);
        return size;
    }
    inline uint64_t GetPatchMetaSize(const SPMerge::MachineID cur_mid,
                                     const SPMerge::MergerResult& mr,
                                     size_t n_clusters,
                                     size_t cc_total_bytes = 0) {
        uint64_t size = 0;
        const auto& my_patch = mr.patch[cur_mid];
        const auto n_machine = mr.patch.size();

        size_t order_storage_bytes = n_clusters * sizeof(SPANNCID);

        if (not is_primary_node(cur_mid)) {
            return PAGE_ROUND_UP(sizeof(PatchMetadata)) +
                   PAGE_ROUND_UP(order_storage_bytes);
        }

        size_t cluster_assignment_bytes = 0;
        for (SPMerge::MachineID mid = 0; mid < n_machine; mid++) {
            if (mid == cur_mid) continue;
            auto& cid_list = mr.cluster_assignment[mid];
            cluster_assignment_bytes +=
                sizeof(PatchMetadata::MetaClusterAssignment);
            cluster_assignment_bytes +=
                cid_list.size() * sizeof(SPMerge::ClusterID);
        }

        size_t cluster_content_bytes = 0;
        if (cc_total_bytes == 0) {
            cluster_content_bytes += n_machine * sizeof(uint64_t);
            for (auto& [_, vid_list] : mr.cluster_content) {
                cluster_content_bytes +=
                    sizeof(PatchMetadata::MetaClusterContent);
                cluster_content_bytes +=
                    vid_list.size() * sizeof(SPMerge::VectorID);
            }
        } else {
            cluster_content_bytes = cc_total_bytes;
        }

        size_t vid_list_bytes = my_patch.size() * sizeof(SPMerge::VectorID);
        for (SPMerge::MachineID mid = 0; mid < n_machine; mid++) {
            if (mid == cur_mid) continue;
            auto& vid_list = mr.patch[mid];
            vid_list_bytes += sizeof(PatchMetadata::MetaPatch);
            vid_list_bytes += vid_list.size() * sizeof(SPMerge::VectorID);
        }

        size = PAGE_ROUND_UP(sizeof(PatchMetadata)) +
               PAGE_ROUND_UP(order_storage_bytes) +
               PAGE_ROUND_UP(cluster_assignment_bytes) +
               PAGE_ROUND_UP(cluster_content_bytes) +
               PAGE_ROUND_UP(vid_list_bytes);

        return size;
    }

    /// @brief generate storage order for posting list, rule: big posting first
    /// @return final page number of vector data  (start from 0)
    uint64_t GetPostingOrder(
        const SPMerge::ClusterIDList& cid_list,
        std::unordered_map<SPMerge::ClusterID, PostingOutputInfo>&
            p_postingInfo,
        std::vector<SPANNCID>& p_postingOrderInIndex) const;
    void OutputMachineIndexFile(
        const std::string& p_outputFile, size_t p_spacePerVector,
        const std::unordered_map<SPMerge::ClusterID, PostingOutputInfo>&
            p_postingInfo,
        const std::vector<SPANNCID>& p_postingOrderInIndex,
        std::shared_ptr<VectorIndex> p_headIndex,
        std::shared_ptr<VectorSet> p_fullVectors,
        Selection& p_postingSelections) const;
    void OutputMachineIndexMeta(
        const std::unordered_map<SPMerge::ClusterID, PostingOutputInfo>&
            p_postingInfo,
        const std::vector<SPANNCID>& p_postingOrderInIndex,
        const SPANNVID n_vectors, const int vector_dim, SimpleIOHelper& soh,
        bool multithread = false) const;
    void OutputMachineIndexListInfo(
        const std::unordered_map<SPMerge::ClusterID, PostingOutputInfo>&
            p_postingInfo,
        const std::vector<SPANNCID>& p_postingOrderInIndex, SimpleIOHelper& soh,
        bool multithread = false) const;
    void OutputMachinePatchFile(
        const SPMerge::MachineID mid, const SPMerge::MergerResult& mr,
        const std::vector<SPANNCID>& p_postingOrderInIndex,
        std::shared_ptr<VectorSet> p_fullVectors) const;

    void OutputMachinePatchMeta(
        const SPMerge::MachineID mid, const SPMerge::MergerResult& mr,
        const std::vector<SPANNCID>& p_postingOrderInIndex,
        const size_t per_vec_size, PatchMetadata& p_meta, SimpleIOHelper& soh,
        bool multithread = false,
        std::vector<uint64_t>* cc_off = nullptr) const;

    void OutputSSDIndexFile(
        const std::string& p_outputFile, bool p_enableDeltaEncoding,
        bool p_enablePostingListRearrange, bool p_enableDataCompression,
        bool p_enableDictTraining, size_t p_spacePerVector,
        const std::vector<int>& p_postingListSizes,
        const std::vector<size_t>& p_postingListBytes,
        std::shared_ptr<VectorIndex> p_headIndex,
        Selection& p_postingSelections,
        const std::unique_ptr<int[]>& p_postPageNum,
        const std::unique_ptr<std::uint16_t[]>& p_postPageOffset,
        const std::vector<SPANNCID>& p_postingOrderInIndex,
        std::shared_ptr<VectorSet> p_fullVectors,
        size_t p_postingListOffset) const {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start output...\n");

        auto t1 = std::chrono::high_resolution_clock::now();

        auto ptr = SPTAG::f_createIO();
        int retry = 3;
        // open file
        while (retry > 0 &&
               (ptr == nullptr ||
                !ptr->Initialize(p_outputFile.c_str(),
                                 std::ios::binary | std::ios::out))) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Failed open file %s, retrying...\n",
                         p_outputFile.c_str());
            retry--;
        }

        if (ptr == nullptr ||
            !ptr->Initialize(p_outputFile.c_str(),
                             std::ios::binary | std::ios::out)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Failed open file %s\n",
                         p_outputFile.c_str());
            throw std::runtime_error("Failed to open file for SSD index save");
        }
        // meta size of global info
        std::uint64_t listOffset =
            sizeof(int) * 2 + sizeof(SPANNCID) + sizeof(SPANNVID);

        // meta size of the posting lists
        listOffset += (sizeof(int) + sizeof(std::uint16_t) + sizeof(int) +
                       sizeof(std::uint16_t)) *
                      p_postingListSizes.size();
        // write listTotalBytes only when enabled data compression
        if (p_enableDataCompression) {
            listOffset += sizeof(size_t) * p_postingListSizes.size();
        }

        // compression dict
        if (p_enableDataCompression && p_enableDictTraining) {
            listOffset += sizeof(size_t);
            listOffset += m_pCompressor->GetDictBuffer().size();
        }

        std::unique_ptr<char[]> paddingVals(new char[PageSize]);
        memset(paddingVals.get(), 0, sizeof(char) * PageSize);
        // paddingSize: bytes left in the last page
        std::uint64_t paddingSize = PageSize - (listOffset % PageSize);
        if (paddingSize == PageSize) {
            paddingSize = 0;
        } else {
            listOffset += paddingSize;
        }

        // Number of posting lists
        auto n_clusters = static_cast<SPANNCID>(p_postingListSizes.size());
        if (ptr->WriteBinary(sizeof(n_clusters),
                             reinterpret_cast<char*>(&n_clusters)) !=
            sizeof(n_clusters)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Failed to write SSDIndex File!");
            throw std::runtime_error("Failed to write SSDIndex File");
        }

        // Number of vectors
        auto n_vectors = static_cast<SPANNVID>(p_fullVectors->Count());
        if (ptr->WriteBinary(sizeof(n_vectors),
                             reinterpret_cast<char*>(&n_vectors)) !=
            sizeof(n_vectors)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Failed to write SSDIndex File!");
            throw std::runtime_error("Failed to write SSDIndex File");
        }

        // Vector dimension
        auto i32Val = static_cast<int>(p_fullVectors->Dimension());
        if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(
                                                 &i32Val)) != sizeof(i32Val)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Failed to write SSDIndex File!");
            throw std::runtime_error("Failed to write SSDIndex File");
        }

        // Page offset of list content section
        i32Val = static_cast<int>(listOffset / PageSize);
        if (ptr->WriteBinary(sizeof(i32Val), reinterpret_cast<char*>(
                                                 &i32Val)) != sizeof(i32Val)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Failed to write SSDIndex File!");
            throw std::runtime_error("Failed to write SSDIndex File");
        }

        // Meta of each posting list
        for (int i = 0; i < p_postingListSizes.size(); ++i) {
            size_t postingListByte = 0;
            int pageNum = 0;  // starting page number
            std::uint16_t pageOffset = 0;
            int listEleCount = 0;
            std::uint16_t listPageCount = 0;

            if (p_postingListSizes[i] > 0) {
                pageNum = p_postPageNum[i];
                pageOffset = static_cast<std::uint16_t>(p_postPageOffset[i]);
                listEleCount = static_cast<int>(p_postingListSizes[i]);
                postingListByte = p_postingListBytes[i];
                listPageCount =
                    static_cast<std::uint16_t>(postingListByte / PageSize);
                if (0 != (postingListByte % PageSize)) {
                    ++listPageCount;
                }
            }
            // Total bytes of the posting list, write only when enabled data
            // compression
            if (p_enableDataCompression &&
                ptr->WriteBinary(sizeof(postingListByte),
                                 reinterpret_cast<char*>(&postingListByte)) !=
                    sizeof(postingListByte)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to write SSDIndex File!");
                throw std::runtime_error("Failed to write SSDIndex File");
            }
            // Page number of the posting list
            if (ptr->WriteBinary(sizeof(pageNum),
                                 reinterpret_cast<char*>(&pageNum)) !=
                sizeof(pageNum)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to write SSDIndex File!");
                throw std::runtime_error("Failed to write SSDIndex File");
            }
            // Page offset
            if (ptr->WriteBinary(sizeof(pageOffset),
                                 reinterpret_cast<char*>(&pageOffset)) !=
                sizeof(pageOffset)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to write SSDIndex File!");
                throw std::runtime_error("Failed to write SSDIndex File");
            }
            // Number of vectors in the posting list
            if (ptr->WriteBinary(sizeof(listEleCount),
                                 reinterpret_cast<char*>(&listEleCount)) !=
                sizeof(listEleCount)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to write SSDIndex File!");
                throw std::runtime_error("Failed to write SSDIndex File");
            }
            // Page count of the posting list
            if (ptr->WriteBinary(sizeof(listPageCount),
                                 reinterpret_cast<char*>(&listPageCount)) !=
                sizeof(listPageCount)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to write SSDIndex File!");
                throw std::runtime_error("Failed to write SSDIndex File");
            }
        }
        // compression dict
        if (p_enableDataCompression && p_enableDictTraining) {
            std::string dictBuffer = m_pCompressor->GetDictBuffer();
            // dict size
            size_t dictBufferSize = dictBuffer.size();
            if (ptr->WriteBinary(sizeof(size_t),
                                 reinterpret_cast<char*>(&dictBufferSize)) !=
                sizeof(dictBufferSize)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to write SSDIndex File!");
                throw std::runtime_error("Failed to write SSDIndex File");
            }
            // dict
            if (ptr->WriteBinary(dictBuffer.size(),
                                 const_cast<char*>(dictBuffer.data())) !=
                dictBuffer.size()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to write SSDIndex File!");
                throw std::runtime_error("Failed to write SSDIndex File");
            }
        }

        // Write padding vals
        if (paddingSize > 0) {
            if (ptr->WriteBinary(paddingSize,
                                 reinterpret_cast<char*>(paddingVals.get())) !=
                paddingSize) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to write SSDIndex File!");
                throw std::runtime_error("Failed to write SSDIndex File");
            }
        }

        if (static_cast<uint64_t>(ptr->TellP()) != listOffset) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "List offset not match!\n");
            throw std::runtime_error("List offset mismatch");
        }

        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "SubIndex Size: %llu bytes, %llu MBytes\n", listOffset,
                     listOffset >> 20);

        listOffset = 0;

        std::uint64_t paddedSize = 0;
        // iterate over all the posting lists
        for (auto id : p_postingOrderInIndex) {
            std::uint64_t targetOffset =
                static_cast<uint64_t>(p_postPageNum[id]) * PageSize +
                p_postPageOffset[id];
            if (targetOffset < listOffset) {
                SPTAGLIB_LOG(
                    Helper::LogLevel::LL_Info,
                    "List offset not match, targetOffset < listOffset!\n");
                throw std::runtime_error("List offset mismatch");
            }
            // write padding vals before the posting list
            if (targetOffset > listOffset) {
                if (targetOffset - listOffset > PageSize) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                                 "Padding size greater than page size!\n");
                    throw std::runtime_error(
                        "Padding size mismatch with page size");
                }

                if (ptr->WriteBinary(
                        targetOffset - listOffset,
                        reinterpret_cast<char*>(paddingVals.get())) !=
                    targetOffset - listOffset) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                                 "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }

                paddedSize += targetOffset - listOffset;

                listOffset = targetOffset;
            }

            if (p_postingListSizes[id] == 0) {
                continue;
            }
            int postingListId = id + (SPANNCID)p_postingListOffset;
            // get posting list full content and write it at once
            ValueType* headVector = nullptr;
            if (p_enableDeltaEncoding) {
                headVector = (ValueType*)p_headIndex->GetSample(postingListId);
            }
            std::string postingListFullData = GetPostingListFullData(
                postingListId, p_postingListSizes[id], p_postingSelections,
                p_fullVectors, p_enableDeltaEncoding,
                p_enablePostingListRearrange, headVector);
            size_t postingListFullSize =
                p_postingListSizes[id] * p_spacePerVector;
            if (postingListFullSize != postingListFullData.size()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "posting list full data size NOT MATCH! "
                             "postingListFullData.size(): %zu "
                             "postingListFullSize: %zu \n",
                             postingListFullData.size(), postingListFullSize);
                throw std::runtime_error("Posting list full size mismatch");
            }
            if (p_enableDataCompression) {
                std::string compressedData = m_pCompressor->Compress(
                    postingListFullData, p_enableDictTraining);
                size_t compressedSize = compressedData.size();
                if (compressedSize != p_postingListBytes[id]) {
                    SPTAGLIB_LOG(
                        Helper::LogLevel::LL_Error,
                        "Compressed size NOT MATCH! compressed size:%zu, "
                        "pre-calculated compressed size:%zu\n",
                        compressedSize, p_postingListBytes[id]);
                    throw std::runtime_error("Compression size mismatch");
                }
                if (ptr->WriteBinary(compressedSize, compressedData.data()) !=
                    compressedSize) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                                 "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
                listOffset += compressedSize;
            } else {
                if (ptr->WriteBinary(postingListFullSize,
                                     postingListFullData.data()) !=
                    postingListFullSize) {
                    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                                 "Failed to write SSDIndex File!");
                    throw std::runtime_error("Failed to write SSDIndex File");
                }
                listOffset += postingListFullSize;
            }
        }

        paddingSize = PageSize - (listOffset % PageSize);
        if (paddingSize == PageSize) {
            paddingSize = 0;
        } else {
            listOffset += paddingSize;
            paddedSize += paddingSize;
        }

        if (paddingSize > 0) {
            if (ptr->WriteBinary(paddingSize,
                                 reinterpret_cast<char*>(paddingVals.get())) !=
                paddingSize) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to write SSDIndex File!");
                throw std::runtime_error("Failed to write SSDIndex File");
            }
        }

        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "Padded Size: %llu, final total size: %llu.\n", paddedSize,
                     listOffset);

        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Output done...\n");
        auto t2 = std::chrono::high_resolution_clock::now();
        SPTAGLIB_LOG(
            Helper::LogLevel::LL_Info, "Time to write results:%.2lf sec.\n",
            ((double)std::chrono::duration_cast<std::chrono::seconds>(t2 - t1)
                 .count()) +
                ((double)std::chrono::duration_cast<std::chrono::milliseconds>(
                     t2 - t1)
                     .count()) /
                    1000);
    }

    std::string get_file_name_from_fmt(const std::string& fmt, uint mid,
                                       bool recovery) const;
    std::string get_index_file_name(uint mid, bool recovery = false) const {
        return get_file_name_from_fmt(m_options.m_indexFileFormat, mid,
                                      recovery);
    }
    std::string get_ec_metadata_file_name(uint mid,
                                          bool recovery = false) const {
        return (std::filesystem::path(m_options.m_savePath) /
                "m_ListInfoMap.bin")
            .string();
    }
    std::string get_patch_file_name(uint mid, bool recovery = false) const {
        return get_file_name_from_fmt(m_options.m_patchFileFormat, mid,
                                      recovery);
    }
    bool is_primary_node(SPMerge::MachineID mid) const {
        return mid % m_options.m_numSplit == 0;
    }
    bool split_exist() const { return m_options.m_numSplit > 1; }

   private:
    std::string m_extraFullGraphFile;

    ListInfoMapType m_listInfoMap;
    ListInfosType m_listInfos;  // cluster with cid is placed in position cid
    bool m_oneContext;

    std::vector<PatchMetadata,
                Helper::TrackerAllocator<PatchMetadata,
                                         Helper::MEMORY_SOURCE::SPMergeMeta>>
        m_patchMeta;
    std::vector<std::shared_ptr<Helper::DiskIO>> m_patchFiles;
    std::vector<std::shared_ptr<Helper::DiskIO>> m_indexFiles;

    // for recovery
    bool m_recoveryMode = false;
    std::unique_ptr<SPMerge::MergerResult> m_recovery_mr;
    Helper::tracked_buffer m_recovery_pbuf_vec;
    Helper::vector_recovery<SPANNCID> m_postingOrderInIndex;

    // for pruning
    std::unordered_set<SPMerge::ClusterID> m_error_machine_cids;
    std::unordered_set<SPMerge::ClusterID> m_cur_machine_cids;
    // for pure memory recovery
    struct PureMemoryInfo {
        void Reset() {
            // 1.
            healthy_isize = healthy_psize = 0;
            healthy_ibuf.reset();
            healthy_pbuf.reset();
            healthy_pbuf2.reset();
            ccbuf.reset();
            cc_off.clear();
            cc_size.clear();
            // 2.
            postingInfo.clear();
            area_list.clear();
            prm_fail_pmeta_bytes = std::promise<uint64_t>();
            vector_offset.clear();
            // 3.
            vectorset.reset();
            area_idx = 0;
        }
        // 1. receive from healthy machine
        size_t healthy_isize, healthy_psize;
        std::unique_ptr<char[]> healthy_ibuf, healthy_pbuf, healthy_pbuf2;
        std::unique_ptr<char[]> ccbuf;
        std::vector<uint64_t> cc_off, cc_size;
        // 2 compute order, then alloc and compute dest
        std::unordered_map<SPMerge::ClusterID, PostingOutputInfo> postingInfo;
        struct AreaInfo {
            size_t cidx1{0}, cidx2{0};
            uint64_t area_start{0};  // relative to
            uint64_t area_size{0};
        };
        std::vector<AreaInfo> area_list;
        std::promise<uint64_t> prm_fail_pmeta_bytes;
        std::unordered_map<SPMerge::VectorID, uint64_t> vector_offset;
        // 3. write index data in memory
        std::unique_ptr<char[]> vectorset;
        std::atomic_size_t area_idx;

        using NetWorkReaderQueueItem = char*;
        using VectorCollectorQueueItem = std::pair<char*, size_t>;
        moodycamel::ConcurrentQueue<NetWorkReaderQueueItem> net_reader_queue;
        moodycamel::ConcurrentQueue<VectorCollectorQueueItem> collector_queue;

    } m_pmData;

    std::unique_ptr<Compressor> m_pCompressor;
    bool m_enableDeltaEncoding;
    bool m_enablePostingListRearrange;
    bool m_enableDataCompression;
    bool m_enableDictTraining;

    // reference to SPANN::Index::m_options, theoretically the same value
    // for `m_enablexxx` variables above can be got from here too
    Options& m_options;
    SPMerge::MachineID m_numNodes;

    void (ExtraFullGraphSearcher<ValueType>::*m_parsePosting)(uint64_t&,
                                                              uint64_t&, int,
                                                              int) const;
    void (ExtraFullGraphSearcher<ValueType>::*m_parseEncoding)(
        std::shared_ptr<VectorIndex>&, ListInfo*, ValueType*) const;

    int m_vectorInfoSize = 0;
    int m_iDataDimension = 0;

    int m_totalListCount = 0;

    int m_listPerFile = 0;
};
}  // namespace SPANN
}  // namespace SPTAG

#endif  // _SPTAG_SPANN_EXTRASEARCHER_H_