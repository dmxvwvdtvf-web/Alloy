#include "inc/Core/SPANN/ExtraFullGraphSearcher.h"

#include <dirent.h>
#include <fcntl.h>
#include <linux/falloc.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <numa.h>
#include <sys/sendfile.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <regex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "BS_thread_pool.hpp"
#include "inc/Core/Common.h"
#include "inc/Core/SPANN/IExtraSearcher.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Core/SPANN/RecoveryPacket.h"
#include "inc/Core/SPMerge.h"
#include "inc/Helper/DiskIO.h"
#include "inc/Helper/MemoryManager.h"
#include "inc/Helper/SocketWrapper.h"
#include "inc/Helper/Timer.h"
#include "inc/Helper/VectorSetReaders/ReocveryReader.h"
#include "inc/SSDServing/SSDIndex.h"
#include "inc/SSDServing/Utils.h"

#undef BLOCK_SIZE
#include "blockingconcurrentqueue.h"
#include "concurrentqueue.h"

using namespace SPTAG;
using namespace SPTAG::SPANN;
using namespace SPTAG::Helper;

using moodycamel::BlockingConcurrentQueue;
using moodycamel::ConcurrentQueue;

#define DefineVectorValueType(Name, Type) \
    template class ExtraFullGraphSearcher<Type>;
#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

static inline std::shared_ptr<Helper::NativeFileIO> f_new_native_io() {
    return std::make_shared<Helper::NativeFileIO>();
}
static inline std::shared_ptr<Helper::ChunkedBufferIO>
f_new_chunked_buffer_io() {
    return std::make_shared<Helper::ChunkedBufferIO>();
}
static inline std::shared_ptr<Helper::SimpleBufferIO> f_new_simple_buffer_io() {
    return std::make_shared<Helper::SimpleBufferIO>();
}

using Helper::f_read_wrapper;

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::ReceiveMetadataStage1(
    int sockfd, SPMerge::MachineID healthy_mid) {
    Reset();

    SSDServing::Utils::StopW sw;
    size_t bytes = 0;

    PacketHeader header;
    f_read_wrapper(sockfd, &header, sizeof(header));
    if (header.type != RecoveryPacketType::PatchData and header.idx != 0) {
        LOG_ERROR("received header with type=%u, idx=%llu\n", header.type,
                  header.idx);
        throw std::runtime_error("");
    }
    LOG_INFO("creating %s buffer for patch stage1\n",
             bytesToString(header.n).c_str());
    m_pmData.healthy_pbuf.reset(new char[header.n]);
    if (m_pmData.healthy_pbuf == nullptr) {
        LOG_ERROR("failed to create patch buffer\n");
        throw std::runtime_error("");
    }
    f_read_wrapper(sockfd, m_pmData.healthy_pbuf.get(), header.n);
    m_pmData.healthy_psize = header.n;

    bytes = sizeof(header) + header.n;
    auto rate = 1000 * bytes / static_cast<uint64_t>(sw.getElapsedMs());
    LOG_INFO("<Network Bandwidth> patch stage1: %s/s\n",
             bytesToString(rate).c_str());
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::LoadIndexStage1(
    SPMerge::MachineID healthy_mid, std::promise<void>* prm_load1_basic_done) {
    SPMerge::MachineID fail_mid = m_options.m_curMid;
    EventTimer timer;
    timer.event_start("LoadIndexStage1");
    // trival items
    {
        int page_limit_by_vecLimit = PAGE_COUNT(
            m_options.m_postingVectorLimit *
            (m_options.m_dim * sizeof(ValueType) + sizeof(SPANNVID)));
        m_options.m_searchPostingPageLimit = m_options.m_postingPageLimit =
            max(m_options.m_searchPostingPageLimit, page_limit_by_vecLimit);
        LOG_INFO("Load index with posting page limit:%d\n",
                 m_options.m_searchPostingPageLimit);

        m_enableDeltaEncoding = m_options.m_enableDeltaEncoding;
        m_enablePostingListRearrange = m_options.m_enablePostingListRearrange;
        m_enableDataCompression = m_options.m_enableDataCompression;
        m_enableDictTraining = m_options.m_enableDictTraining;

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
    }

    m_patchMeta.resize(m_numNodes);

    // 1. correct pmeta offset after cluster content,
    // cause we only sent 1/3 in stage1
    PatchMetadata p_meta_bk;
    memcpy(&p_meta_bk, m_pmData.healthy_pbuf.get(), sizeof(PatchMetadata));
    if (p_meta_bk.n_machine != m_numNodes) {
        LOG_ERROR("pmeta.n_machine=%llu, m_numNodes=%u\n", p_meta_bk.n_machine,
                  m_numNodes);
        throw std::runtime_error("");
    }
    // how many bytes are ignored
    size_t stage2_bytes = 0;
    auto content_sub_off = reinterpret_cast<uint64_t*>(
        m_pmData.healthy_pbuf.get() + p_meta_bk.clusterContentStart);
    {
        m_pmData.cc_off.resize(m_numNodes);
        memcpy(m_pmData.cc_off.data(), content_sub_off,
               m_numNodes * sizeof(uint64_t));
    }
    size_t cc_bytes_total = m_numNodes * sizeof(uint64_t);
    for (SPMerge::MachineID mid = 0; mid < p_meta_bk.n_machine; mid++) {
        size_t content_size;
        if (mid != p_meta_bk.n_machine - 1) {
            content_size = content_sub_off[mid + 1] - content_sub_off[mid];
        } else {
            content_size = p_meta_bk.vectorIDStart -
                           p_meta_bk.clusterContentStart - content_sub_off[mid];
        }
        content_sub_off[mid] -= stage2_bytes;
        if (mid != fail_mid) {
            stage2_bytes += content_size;
        }
        m_pmData.cc_size.push_back(content_size);
        cc_bytes_total += content_size;
    }
    {
        m_pmData.cc_off.push_back(cc_bytes_total);
    }
    // correct info
    auto p_meta = reinterpret_cast<PatchMetadata*>(m_pmData.healthy_pbuf.get());
    p_meta->vectorIDStart -= stage2_bytes;
    p_meta->vectorContentStart -= stage2_bytes;

    // 2. trival info again
    m_vectorInfoSize = p_meta->perVectorSize + sizeof(SPANNVID);

    // 3. load healthy patch data
    std::promise<void> prm_patch_meta_done;
    std::promise<void> prm_assignment_done;
    std::promise<void> prm_patch_buff_done;
    std::promise<void> prm_cluster_content_done;

    std::thread t_load1_core([&]() {
        std::vector<SPANNCID> postingOrderInIndex;
        timer.event_start("- LoadingPatchFullData");
        LoadingPatchFullData(healthy_mid, postingOrderInIndex, fail_mid,
                             &prm_patch_meta_done, &prm_assignment_done,
                             &prm_patch_buff_done, &prm_cluster_content_done);
        timer.event_stop("- LoadingPatchFullData");
    });

    if (prm_load1_basic_done) {
        prm_patch_meta_done.get_future().wait();
        prm_patch_buff_done.get_future().wait();
        prm_load1_basic_done->set_value();
    }

    {  // select my cc into separated buffer
        auto size = m_pmData.cc_size[fail_mid];
        LOG_INFO("Allocating %s buffer for cluster content (failed only)",
                 bytesToString(size).c_str());
        m_pmData.ccbuf.reset(new char[m_pmData.cc_size[fail_mid]]);
        memcpy(m_pmData.ccbuf.get(),
               m_pmData.healthy_pbuf.get() + p_meta_bk.clusterContentStart +
                   content_sub_off[fail_mid],
               size);
    }

    t_load1_core.join();

    m_pmData.healthy_pbuf.reset();

    timer.event_stop("LoadIndexStage1");
    timer.log_all_event_duration();
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::ReceiveMetadataStage2(
    int sockfd, SPMerge::MachineID healthy_mid) {
    SSDServing::Utils::StopW sw;
    size_t bytes = 0;
    {
        PacketHeader header;
        f_read_wrapper(sockfd, &header, sizeof(header));
        if (header.type != RecoveryPacketType::PatchData and header.idx != 0) {
            LOG_ERROR("received header with type=%u, idx=%llu\n", header.type,
                      header.idx);
            throw std::runtime_error("");
        }

        LOG_INFO("creating %s buffer for patch stage2\n",
                 bytesToString(header.n).c_str());

        m_pmData.healthy_pbuf2.reset(new char[header.n]);
        if (m_pmData.healthy_pbuf2 == nullptr) {
            LOG_ERROR("failed to create patch buffer\n");
            throw std::runtime_error("");
        }

        f_read_wrapper(sockfd, m_pmData.healthy_pbuf2.get(), header.n);

        // m_pmData.healthy_psize = header.n;
        bytes = sizeof(header) + header.n;
    }

    auto rate = 1000 * bytes / (size_t)sw.getElapsedMs();
    LOG_INFO("<Network Bandwidth> patch stage2: %s/s\n",
             bytesToString(rate).c_str());
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::LoadIndexStage2(
    SPMerge::MachineID healthy_mid) {
    EventTimer timer;
    timer.event_start("LoadIndexStage2");

    auto cluster_left = m_patchMeta[healthy_mid].n_totalClusters -
                        m_recovery_mr->cluster_content.size();

    LoadingClusterContent(m_pmData.healthy_pbuf2.get(), cluster_left);
    m_pmData.healthy_pbuf2.reset();

    timer.event_stop("LoadIndexStage2");
    timer.log_all_event_duration();
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::PrepareIndexStorage(
    SPMerge::MachineID healthy_mid, const size_t AreaSize,
    const size_t ALIGNMENT) {
    EventTimer timer;
    SPMerge::MachineID fail_mid = m_options.m_curMid;
    const auto& p_meta = m_patchMeta[healthy_mid];
    const auto& mr = *m_recovery_mr;
    const size_t vectorInfoSize = p_meta.perVectorSize + sizeof(SPANNVID);
    const auto numPostingOnFail = mr.cluster_assignment[fail_mid].size();
    const auto fail_imeta_bytes = GetIndexMetaSize(numPostingOnFail);

    timer.event_start("PrepareIndexStorage");

    std::vector<size_t> cluster_size;
    {
        const auto& cluster_content = mr.cluster_content;
        assert(numPostingOnFail == cluster_content.size());
        cluster_size.resize(cluster_content.size());
#pragma omp parallel for
        for (size_t cidx = 0; cidx < numPostingOnFail; cidx++) {
            auto cid = mr.cluster_assignment[fail_mid][cidx];
            cluster_size[cidx] = cluster_content.at(cid).size();
        }
    }

    // calc posting order
    timer.event_start("_1 scan");
    auto& postingInfo = m_pmData.postingInfo;
    auto& area_list = m_pmData.area_list;
    const auto numPostingPerArea =
        AreaSize / (m_options.m_postingPageLimit << PageSizeEx);
    // variable used for computing `postingInfo`
    postingInfo.reserve(numPostingOnFail);
    int currPageNum = 0;
    uint16_t currPageOff = 0;
    // variable used for collecting area info
    uint64_t currOffset = fail_imeta_bytes;
    area_list.reserve(numPostingOnFail / numPostingPerArea);
    typename PureMemoryInfo::AreaInfo area;
    area.cidx2 = -1;
    area.area_start = currOffset;
    auto f_push_area = [&]() {
        assert(area.area_size != 0);
        assert(area.area_size <= AreaSize);
        assert(area.cidx2 != -1);
        area_list.push_back(area);
        area.cidx1 = area.cidx2 + 1;
        area.cidx2 = -1;
        area.area_start += area.area_size;
        area.area_size = 0;
    };
    auto f_update_area = [&](size_t cidx) {
        if (currOffset - area.area_start > AreaSize) f_push_area();
        if (currOffset - area.area_start > AreaSize) {
            LOG_ERROR(
                "cidx=%lu, curOffset=%llu, area.cidx1=%lu, "
                "area.area_start=%lu, AreaSize=%lu\n",
                cidx, currOffset, area.cidx1, area.area_start, AreaSize);
            // TODO: modify AreaSize to fit special case
            NOT_IMPLEMENTED();
        }
        if (currOffset % ALIGNMENT == 0) {
            area.cidx2 = cidx - 1;
            area.area_size = currOffset - area.area_start;
        }
    };
    for (size_t cidx = 0; cidx < numPostingOnFail; cidx++) {
        const auto cid = mr.cluster_assignment[fail_mid][cidx];
        PostingOutputInfo info;
        info.size = cluster_size[cidx];
        info.bytes = cluster_size[cidx] * vectorInfoSize;
        if (currPageOff + info.bytes % PageSize > PageSize) {
            info.paddings = PageSize - currPageOff;
            currPageNum++;
            currPageOff = 0;
            currOffset += info.paddings;
        } else {
            info.paddings = 0;
        }
        info.page_num = currPageNum;
        info.page_off = currPageOff;

        postingInfo.emplace(cid, info);

        f_update_area(cidx);

        // update
        currPageNum += (info.bytes >> PageSizeEx);
        currPageOff += (info.bytes % PageSize);
        currOffset += info.bytes;

        if (currPageOff == PageSize) {
            ++currPageNum;
            currPageOff = 0;
        }
        if (currPageOff > PageSize) {
            throw std::logic_error("currOffset > PageSize");
        }
    }
    uint64_t ibuf_page_cnt = currPageOff == 0 ? currPageNum : currPageNum + 1;
    const auto fail_idata_bytes = ibuf_page_cnt << PageSizeEx;
    currOffset = fail_imeta_bytes + fail_idata_bytes;
    f_update_area(numPostingOnFail);
    f_push_area();

    timer.event_stop("_1 scan");
    timer.event_stop("PrepareIndexStorage");
    timer.log_all_event_duration();

    LOG_INFO("Disk IO Size Distribution:\n");
    SPTAG::SSDServing::SSDIndex::PrintPercentiles<
        size_t, typename PureMemoryInfo::AreaInfo>(
        area_list,
        [](const typename PureMemoryInfo::AreaInfo& ai) -> size_t {
            return ai.area_size;
        },
        "%lu");
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::PrepareVectorOffset(
    SPMerge::MachineID healthy_mid) {
    EventTimer timer;
    SPMerge::MachineID fail_mid = m_options.m_curMid;
    const auto& p_meta = m_patchMeta[healthy_mid];
    const auto& mr = *m_recovery_mr;
    const auto numPostingOnFail = mr.cluster_assignment[fail_mid].size();

    const auto& cluster_content = mr.cluster_content;

    timer.event_start("PrepareVectorOffset");
    {
        auto cluster0_size = cluster_content.begin()->second.size();
        m_pmData.vector_offset.clear();
        m_pmData.vector_offset.reserve(numPostingOnFail * cluster0_size);
    }
    for (auto& [_, vid_list] : cluster_content) {
        for (auto vid : vid_list) {
            m_pmData.vector_offset.emplace(vid, 0);
        }
    }
    uint64_t off = 0;
    for (auto& [_, local_offset] : m_pmData.vector_offset) {
        local_offset = off;
        off += p_meta.perVectorSize;
    }

    timer.event_stop("PrepareVectorOffset");
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::RecoverMetadata(
    SPMerge::MachineID healthy_mid) {
    Helper::EventTimer timer;
    timer.event_start("RecoverMetadata");

    SPMerge::MachineID fail_mid = m_options.m_curMid;
    const auto& mr = *m_recovery_mr;
    const auto& p_meta = m_patchMeta[healthy_mid];
    const auto totalVec = p_meta.n_clusvec + p_meta.n_headvec;
    const auto& my_ass = GetMergerResult().cluster_assignment[fail_mid];

    const auto fail_imeta_bytes = GetIndexMetaSize(my_ass.size());
    const auto fail_pmeta_bytes = GetPatchMetaSize(fail_mid, mr, my_ass.size(),
                                                   m_pmData.cc_off[m_numNodes]);
    LOG_INFO(
        "Allocating %s buffer for patch meta, and %s buffer for index meta\n",
        bytesToString(fail_pmeta_bytes).c_str(),
        bytesToString(fail_imeta_bytes).c_str());

    m_pmData.prm_fail_pmeta_bytes.set_value(fail_pmeta_bytes);

    timer.event_start("1. Allocate meta buffer");
    auto fail_ibuf_meta_io = f_new_chunked_buffer_io();
    auto fail_pbuf_meta_io = f_new_chunked_buffer_io();
    fail_ibuf_meta_io->Initialize(fail_imeta_bytes);
    fail_pbuf_meta_io->Initialize(fail_pmeta_bytes);
    timer.event_stop("1. Allocate meta buffer");

    std::vector<SPANNCID> postingOrderInIndex(my_ass.begin(), my_ass.end());
    SimpleIOHelper sio_i, sio_p;
    sio_i.init_with_ext_io(fail_ibuf_meta_io, SimpleIOHelper::ReadWrite);
    sio_p.init_with_ext_io(fail_pbuf_meta_io, SimpleIOHelper::ReadWrite);

    timer.event_start("2. Recover metadata in memory");
    auto& my_pmeta = m_patchMeta[fail_mid];
    OutputMachineIndexMeta(m_pmData.postingInfo, postingOrderInIndex, totalVec,
                           m_options.m_dim, sio_i, true);
    OutputMachinePatchMeta(fail_mid, mr, postingOrderInIndex,
                           p_meta.perVectorSize, my_pmeta, sio_p, true,
                           &m_pmData.cc_off);
    if (is_primary_node(fail_mid)) {
        // we copy cluster content buffer directly, instead of loading
        for (SPMerge::MachineID mid = 0; mid < m_numNodes; mid++) {
            auto suboff = m_pmData.cc_off[mid];
            char* src = nullptr;
            if (mid == fail_mid) {
                src = m_pmData.ccbuf.get();
            } else {
                src = m_pmData.healthy_pbuf2.get();
                src += suboff - sizeof(uint64_t) * m_numNodes;
                if (mid > fail_mid) src -= m_pmData.cc_size[fail_mid];
            }
            auto off = my_pmeta.clusterContentStart + suboff;
            auto size = m_pmData.cc_size[mid];
            fail_pbuf_meta_io->WriteBinaryDirect(src, size, off);
        }
    }
    // NOTE: We do not load all clusters now (for bothn pnode and snode),
    // so cluster_content.size() != total_clusters
    my_pmeta.n_totalClusters = p_meta.n_totalClusters;
    fail_pbuf_meta_io->WriteBinaryDirect(
        reinterpret_cast<const char*>(&my_pmeta), sizeof(PatchMetadata), 0);

    my_pmeta.print();
    timer.event_stop("2. Recover metadata in memory");

    timer.event_start("3. Output metadata");
    SSDServing::Utils::StopW sw;
    auto fail_index_file = get_index_file_name(fail_mid, true);
    auto fail_patch_file = get_patch_file_name(fail_mid, true);
    SimpleIOHelper fail_index(f_new_native_io(), fail_index_file,
                              SimpleIOHelper::ReadWrite, false, false, true);
    SimpleIOHelper fail_patch(f_new_native_io(), fail_patch_file,
                              SimpleIOHelper::ReadWrite, false, false, true);
    fail_ibuf_meta_io->Output(fail_index);
    fail_pbuf_meta_io->Output(fail_patch);
    fail_index.close();
    fail_patch.close();
    timer.event_stop("3. Output metadata");

    auto bytes = fail_pmeta_bytes + fail_imeta_bytes;
    auto dur = sw.getElapsedMs();
    auto rate = static_cast<size_t>(1000 * bytes / dur);
    LOG_INFO("<Disk Write Bandwidth> Patch Metadata: %s/s\n",
             bytesToString(rate).c_str());

    fail_ibuf_meta_io->ShutDown();
    fail_pbuf_meta_io->ShutDown();
    m_pmData.healthy_pbuf2.reset();
    m_pmData.ccbuf.reset();

    timer.event_stop("RecoverMetadata");
    timer.log_all_event_duration();
}

template <typename ValueType>
size_t ExtraFullGraphSearcher<ValueType>::NetWorkReader(
    SPMerge::MachineID healthy_mid, int sockfd, size_t buffer_size) {
    {
        PacketHeader header;
        header.type = RecoveryPacketType::IAmReady;
        f_write_wrapper(sockfd, &header, sizeof(header));
    }

    const auto& p_meta = m_patchMeta[healthy_mid];
    const size_t vectorInfoSize = p_meta.perVectorSize + sizeof(SPANNVID);

    size_t read_bytes = 0;

    // HelpInfo3
    size_t target_count = 0;
    SPMerge::MachineID src_mid = 0;
    {
        PacketHeader header;
        f_read_wrapper(sockfd, &header, sizeof(header));
        read_bytes += sizeof(header);
        if (header.type != RecoveryPacketType::HelpInfo) {
            LOG_ERROR("Received header with type=%u\n", header.type);
            throw std::runtime_error("");
        }
        src_mid = header.idx;
        target_count = header.n;
        LOG_INFO("ready to receive %lu vectors from node %u, port=%d\n",
                 target_count, src_mid, sockfd);
    }

    const size_t CheckGranularity = 0.2 * target_count;
    size_t received_cnt = 0;
    size_t check_target = CheckGranularity;

    // receive and parse index data from remote
    while (received_cnt < target_count) {
        PacketHeader header;
        f_read_wrapper(sockfd, &header, sizeof(header));
        if (header.type != RecoveryPacketType::IndexData or
            header.sub_size % vectorInfoSize != 0) {
            LOG_ERROR("received packet header type=%u, size=%llu\n",
                      header.type, header.sub_size);
            throw std::runtime_error("");
        }

        if (header.sub_size > buffer_size) {
            LOG_ERROR(
                "received packet header with size=%llu, > buffer_size=%llu\n",
                header.sub_size, buffer_size);
            throw std::logic_error("");
        }

        char* buffer = nullptr;
        while (not m_pmData.net_reader_queue.try_dequeue(buffer)) continue;

        f_read_wrapper(sockfd, buffer, header.sub_size);
        read_bytes += header.sub_size + sizeof(header);

        typename PureMemoryInfo::VectorCollectorQueueItem item{buffer,
                                                               header.sub_size};
        m_pmData.collector_queue.enqueue(item);

        received_cnt += header.sub_size / vectorInfoSize;
        if (received_cnt >= check_target) {
            float ratio = 100. * received_cnt / target_count;
            LOG_INFO("[from m%u] %.1f%% received\n", src_mid, ratio);
            check_target =
                (received_cnt / CheckGranularity + 1) * CheckGranularity;
        }
    }
    return read_bytes;
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::VectorCollector(
    int tid, SPMerge::MachineID healthy_mid) {
    const auto& p_meta = m_patchMeta[healthy_mid];
    const auto& mr = *m_recovery_mr;
    const size_t vectorInfoSize = p_meta.perVectorSize + sizeof(SPANNVID);

    const SPMerge::MachineID fail_mid = m_options.m_curMid;
    const auto is_primary = is_primary_node(fail_mid);

    size_t skipped_invalid = 0;

    if (not m_pmData.vector_offset.empty()) NOT_IMPLEMENTED();

    auto f_handle_vector = [&](SPANNVID vid, char* src_ptr,
                               bool invalid_ok = false) {
        uint64_t offset_in_buffer = 0;
        offset_in_buffer = (size_t)vid * p_meta.perVectorSize;
        // if (is_primary) {  // primary node receives full vectorset
        //     offset_in_buffer = (size_t)vid * p_meta.perVectorSize;
        // } else {  // secondary node using map: vid->offset
        //     auto iter = m_pmData.vector_offset.find(vid);
        //     if (iter == m_pmData.vector_offset.end()) {
        //         if (invalid_ok) {
        //             skipped_invalid++;
        //             return;
        //         } else {
        //             LOG_ERROR("Received vector {}, which is not needed\n",
        //             vid); throw std::runtime_error("");
        //         }
        //     } else {
        //         offset_in_buffer = iter->second;
        //     }
        // }
        auto dest_ptr = m_pmData.vectorset.get() + offset_in_buffer;
        memcpy(dest_ptr, src_ptr, p_meta.perVectorSize);
    };

    // handle patch
    if (tid == 0) {
        auto patch_count = mr.patch[healthy_mid].size();
        for (size_t i = 0; i < patch_count; i++) {
            auto vid = mr.patch[healthy_mid][i];
            auto src_ptr = m_recovery_pbuf_vec.get() + i * p_meta.perVectorSize;
            f_handle_vector(vid, src_ptr, true);
        }
        if (not m_pmData.vector_offset.empty()) {
            auto useful_patch_count = patch_count - skipped_invalid;
            LOG_INFO("In m%u's patch buffer, useful vectors: %llu/%llu\n",
                     healthy_mid, useful_patch_count, patch_count);
        }
    }

    // parse index data from remote
    while (true) {
        typename PureMemoryInfo::VectorCollectorQueueItem item;
        if (not m_pmData.collector_queue.try_dequeue(item)) continue;

        auto [srcbuf, bufsize] = item;
        if (srcbuf == nullptr and bufsize == 0) break;

        size_t vcount = bufsize / vectorInfoSize;

        uint64_t offset_vid, offset_vec;
        for (int i = 0; i < vcount; i++) {
            (this->*m_parsePosting)(offset_vid, offset_vec, i, vcount);
            auto vid = *reinterpret_cast<SPANNVID*>(srcbuf + offset_vid);
            auto src_ptr = srcbuf + offset_vec;
            f_handle_vector(vid, src_ptr);
        }
        m_pmData.net_reader_queue.enqueue(srcbuf);
    }
}

template <typename ValueType>
size_t ExtraFullGraphSearcher<ValueType>::VectorDataWriter(
    SPMerge::MachineID healthy_mid, int tid, const size_t AreaSize) {
    const SPMerge::MachineID fail_mid = m_options.m_curMid;
    const auto& p_meta = m_patchMeta[healthy_mid];
    const auto& mr = *m_recovery_mr;
    const auto numPatchVecOnFail = mr.patch[fail_mid].size();
    const auto numPostingOnFail = mr.cluster_assignment[fail_mid].size();
    const size_t vectorInfoSize = p_meta.perVectorSize + sizeof(SPANNVID);
    const size_t fail_imeta_bytes = GetIndexMetaSize(numPostingOnFail);
    const bool is_primary = is_primary_node(fail_mid);

    if (not m_pmData.vector_offset.empty()) NOT_IMPLEMENTED();

    auto fail_index_file = get_index_file_name(fail_mid, true);
    SimpleIOHelper fail_index(f_new_native_io(), fail_index_file,
                              SimpleIOHelper::ReadWrite, false, false, true);
    auto f_get_vector_ptr = [&](SPMerge::VectorID vid) {
        uint64_t offset = 0;
        offset = (size_t)vid * p_meta.perVectorSize;
        // if (is_primary) {
        //     offset = (size_t)vid * p_meta.perVectorSize;
        // } else {
        //     offset = m_pmData.vector_offset.at(vid);
        // }
        return m_pmData.vectorset.get() + offset;
    };

    std::unique_ptr<char, void (*)(char*)> buffer(
        static_cast<char*>(PAGE_ALLOC(AreaSize)),
        [](char* ptr) { PAGE_FREE(ptr); });

    size_t written_bytes = 0;

    // recover patch (only for primary node)
    if (is_primary and tid == 0) {
        auto pdata_bytes =
            PAGE_ROUND_UP(numPatchVecOnFail * p_meta.perVectorSize);
        std::unique_ptr<char, void (*)(char*)> pbuf(
            static_cast<char*>(PAGE_ALLOC(pdata_bytes)),
            [](char* ptr) { PAGE_FREE(ptr); });
        auto ptr = pbuf.get();
        memset(ptr, 0, pdata_bytes);
        for (auto vid : mr.patch[fail_mid]) {
            memcpy(ptr, f_get_vector_ptr(vid), p_meta.perVectorSize);
            ptr += p_meta.perVectorSize;
        }

        auto fail_patch_file = get_patch_file_name(fail_mid, true);
        SimpleIOHelper fail_patch(f_new_native_io(), fail_patch_file,
                                  SimpleIOHelper::ReadWrite, false, false,
                                  true);
        auto fail_pmeta_bytes =
            m_pmData.prm_fail_pmeta_bytes.get_future().get();
        fail_patch.f_write_data(pbuf.get(), pdata_bytes, fail_pmeta_bytes);
        written_bytes += pdata_bytes;
    }

    while (true) {
        auto area_idx = m_pmData.area_idx.fetch_add(1);
        if (area_idx >= m_pmData.area_list.size()) break;
        memset(buffer.get(), 0, AreaSize);
        const auto& area = m_pmData.area_list[area_idx];
        for (size_t cidx = area.cidx1; cidx <= area.cidx2; cidx++) {
            const auto cid = mr.cluster_assignment[fail_mid][cidx];
            const auto& listinfo = m_pmData.postingInfo.at(cid);
            // calculate offset
            auto cluster_offset =
                fail_imeta_bytes +
                (static_cast<uint64_t>(listinfo.page_num) << PageSizeEx) +
                listinfo.page_off;
            assert(area.area_start <= cluster_offset and
                   cluster_offset < area.area_start + area.area_size);
            auto offset_in_buffer = cluster_offset - area.area_start;
            // copy data in memory
            char* ptr = buffer.get() + offset_in_buffer;
            for (SPANNVID vid : mr.cluster_content.at(cid)) {
                memcpy(ptr, &vid, sizeof(vid));
                memcpy(ptr + sizeof(vid), f_get_vector_ptr(vid),
                       p_meta.perVectorSize);
                ptr += vectorInfoSize;
            }
        }
        fail_index.f_write_data(buffer.get(), area.area_size, area.area_start);
        written_bytes += area.area_size;
    }

    return written_bytes;
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::LoadAndRecovery(
    SPMerge::MachineID healthy_mid, SPMerge::MachineID fail_mid) {
    if (healthy_mid >= m_options.m_numMachine or
        fail_mid >= m_options.m_numMachine or fail_mid == healthy_mid) {
        LOG_ERROR("invalid parameter, healthy_mid=%u and faile_mid=%u\n",
                  healthy_mid, fail_mid);
        throw std::invalid_argument("");
    }
    if (m_enableDataCompression or m_enableDictTraining) {
        NOT_IMPLEMENTED();
    }
    Reset();
    clear_page_cache();
    MemoryTracker::init();
    m_recoveryMode = true;
    switch (m_options.m_recoveryMethod) {
        case SPMerge::RecoveryMethod::PureMemory:
            LAR_PM_PLUS(healthy_mid, fail_mid);
            // LAR_PM(healthy_mid, fail_mid);
            break;
        case SPMerge::RecoveryMethod::StupidLoad:
            LAR_SL(healthy_mid, fail_mid);
            break;
        case SPMerge::RecoveryMethod::GreedyLoad:
            LAR_GL(healthy_mid, fail_mid);
            break;
        case SPMerge::RecoveryMethod::ScanGather:
            LAR_SC(healthy_mid, fail_mid);
            break;
        case SPMerge::RecoveryMethod::ScanWrite:
            LAR_SW(healthy_mid, fail_mid);
            break;
        case SPMerge::RecoveryMethod::ConcuRecov:
            LAR_CR(healthy_mid, fail_mid);
            break;
        default:
            throw std::invalid_argument("bad recovery method!");
    }
    m_recoveryMode = false;
}

struct RCCommonData {
    static int NW;
    static std::function<void(uint64_t&, uint64_t&, int, int)> g_parsePosting;

    // index recovery info
    static const SPMerge::VectorIDList* patch_vid_list;
    static std::unique_ptr<char[]> failed_patch_buffer;
    static std::promise<void> patch_data_ready;

    // patch recovery info
    static std::atomic_uint64_t index_final_offset;

    static void reset() {
        NW = 0;
        g_parsePosting = nullptr;
        patch_vid_list = nullptr;
        failed_patch_buffer.reset();
        patch_data_ready = std::promise<void>();
        index_final_offset = 0;
    }

    static inline bool f_execute_cmd(const std::string& cmd) {
        int ret = system(cmd.c_str());

        if (ret == -1) {
            LOG_ERROR("system(%s) failed, error: %s\n", cmd.c_str(),
                      strerror(errno));
            return false;
        }

        if (not WIFEXITED(ret)) {
            LOG_ERROR(
                "system(%s) failed, child process didn't exit normally "
                "(ret=%d)\n",
                cmd.c_str(), ret);
            return false;
        }

        if (int retcode = WEXITSTATUS(ret); retcode != 0) {
            LOG_ERROR("cmd(%s) exited with code %d\n", cmd.c_str(), retcode);
            return false;
        }

        return true;
    }
};
int RCCommonData::NW;
std::function<void(uint64_t&, uint64_t&, int, int)>
    RCCommonData::g_parsePosting;
const SPMerge::VectorIDList* RCCommonData::patch_vid_list;
std::unique_ptr<char[]> RCCommonData::failed_patch_buffer;
std::promise<void> RCCommonData::patch_data_ready;
std::atomic_uint64_t RCCommonData::index_final_offset;

struct LoadBuffer {
    using Queue = BlockingConcurrentQueue<LoadBuffer*>;

    static std::unique_ptr<LoadBuffer[]> buffer_arr;
    static void reset() { buffer_arr.reset(); }

    std::unique_ptr<char[]> data;
    std::atomic_int state;
    std::unordered_map<SPMerge::VectorID, void*> vec_pos;

    LoadBuffer() = default;

    void init(int NPage, int s) {
        data.reset(new char[NPage << PageSizeEx]);
        vec_pos.clear();
        state = s;
    }
};
std::unique_ptr<LoadBuffer[]> LoadBuffer::buffer_arr = nullptr;

struct RecoveryWorkerReader {
    // data sync
    static LoadBuffer::Queue rqueue;

    // healthy info
    static std::string index_filename;
    static const ListInfoMapType* p_listInfoMap;
    static const Helper::vector_recovery<SPANNCID>* p_postingOrder;
    static std::atomic<SPMerge::ClusterID> cluster_idx;
    static std::unique_ptr<std::atomic_bool[]> visited;

    // timer
    static std::vector<EventTimer> timer_arr;

    static void reset(int NR = 0) {
        rqueue = LoadBuffer::Queue(0);
        index_filename.clear();
        p_listInfoMap = nullptr;
        p_postingOrder = nullptr;
        cluster_idx = 0;
        visited.reset();

        timer_arr = std::vector<EventTimer>(NR);
        for (auto& timer : timer_arr) timer.clear();
    }

    static void reader_func(int tid);
};
LoadBuffer::Queue RecoveryWorkerReader::rqueue;
std::string RecoveryWorkerReader::index_filename;
const ListInfoMapType* RecoveryWorkerReader::p_listInfoMap;
const Helper::vector_recovery<SPANNCID>* RecoveryWorkerReader::p_postingOrder;
std::atomic<SPMerge::ClusterID> RecoveryWorkerReader::cluster_idx;
std::unique_ptr<std::atomic_bool[]> RecoveryWorkerReader::visited;
std::vector<EventTimer> RecoveryWorkerReader::timer_arr;

struct RecoveryWorkerWriter {
    // data sync
    static std::unique_ptr<LoadBuffer::Queue[]> wqueue_arr;

    // failed info
    static std::string index_filename;
    static std::size_t per_vector_size;  // donot count vid
    static std::vector<SPANNCID> posting_order;
    static std::unordered_map<SPMerge::ClusterID, PostingOutputInfo>
        posting_info;

    // sync info
    static std::vector<std::promise<void>> finished_flags;

    // timer
    static std::vector<EventTimer> timer_arr;

    static const SPMerge::ClusterContentList* p_clusterContent;
    static size_t p_listOffset;

    static void reset(int NW = 0) {
        wqueue_arr.reset();
        index_filename.clear();
        per_vector_size = 0;
        posting_order.clear();
        posting_info.clear();
        p_clusterContent = nullptr;
        p_listOffset = 0;

        // NOTE: writer-0 needs to wait for index meta to finish,
        // so there are NW+1 promise
        RecoveryWorkerWriter::finished_flags.clear();
        for (int i = 0; i < NW + 1; i++) {
            RecoveryWorkerWriter::finished_flags.emplace_back();
        }

        timer_arr = std::vector<EventTimer>(NW);
        for (auto& timer : timer_arr) timer.clear();
    }

    static void writer_func(int tid);
    static bool f_concat_index_files_all(bool rmsrc = true) {
        const auto& dest = index_filename;
        auto fmt = f_get_local_index_name_fmt();
        std::regex pattern("%d");
        std::string replacement =
            "{0.." + std::to_string(RCCommonData::NW) + "}";
        std::string src = std::regex_replace(fmt, pattern, replacement);
        std::string cmd = "cat " + src + " >> " + dest;
        if (not RCCommonData::f_execute_cmd(cmd)) return false;
        if (rmsrc) {
            for (int i = 0; i < RCCommonData::NW; i++) {
                std::filesystem::remove(f_get_local_index_name(i));
            }
        }
        return true;
    }

   private:
    struct VectorDest {
        SPMerge::ClusterID cid;
        uint64_t offset_vid;
        uint64_t offset_vec;
    };
    static void f_set_final_pos(uint64_t new_v) {
        auto& index_final_offset = RCCommonData::index_final_offset;
        auto old = index_final_offset.load();
        while (old < new_v) {
            if (index_final_offset.compare_exchange_weak(old, new_v)) {
                break;
            }
        }
    };
    static inline uint64_t f_get_start_page(size_t idx) {
        if (idx == -1 or idx == posting_order.size()) {
            return -1;
        }
        return posting_info.at(posting_order[idx]).page_num;
    };
    static inline uint64_t f_get_end_page(size_t idx) {
        if (idx == -1 or idx == posting_order.size()) {
            return -1;
        }
        const auto& info = posting_info.at(posting_order[idx]);
        return info.page_num + PAGE_COUNT(info.page_off + info.bytes) - 1;
    };
    static void f_split_task(int tid, size_t& start, size_t& end) {
        // coarse-grained split by cluster-id
        const auto& NW = RCCommonData::NW;
        size_t block_size = (posting_order.size() + NW - 1) / NW;
        start = tid * block_size;
        end = min((tid + 1) * block_size, posting_order.size()) - 1;
        // LOG_DEBUG("tid=%d, start=%llu, end=%llu\n", tid, start, end);

        // correct cid range to meet page-level exclusion
        while (f_get_start_page(start) == f_get_end_page(start - 1)) start--;
        while (f_get_end_page(end) == f_get_start_page(end + 1)) end--;
    }
    static std::string f_get_local_index_name_fmt() {
        return index_filename + "%d";
    }
    static std::string f_get_local_index_name(int tid) {
        static char buffer[256];
        auto fmt = f_get_local_index_name_fmt();
        snprintf(buffer, sizeof(buffer), fmt.c_str(), tid);
        return std::string(buffer);
    }

    static bool f_concat_index_file(int tid, bool rmsrc = true) {
        SimpleIOHelper dest(f_new_native_io(), index_filename,
                            SimpleIOHelper::ReadWrite);
        lseek(dest.m_ptr->GetFD(), 0, SEEK_END);

        auto local_index = f_get_local_index_name(tid);
        auto src_size = std::filesystem::file_size(local_index);
        SimpleIOHelper src(f_new_native_io(), local_index,
                           SimpleIOHelper::ReadOnly);

        auto ret = copy_file_range(src.m_ptr->GetFD(), nullptr,
                                   dest.m_ptr->GetFD(), nullptr, src_size, 0);
        if (ret == -1) {
            LOG_ERROR("copy_file_range failed, error: %s\n", strerror(errno));
            return false;
        } else if (ret != src_size) {
            LOG_ERROR(
                "copy_file_range copied only %llu bytes (%llu expected)\n", ret,
                src_size);
            return false;
        }

        if (rmsrc) std::filesystem::remove(local_index);
        return true;
    }
};
std::unique_ptr<LoadBuffer::Queue[]> RecoveryWorkerWriter::wqueue_arr;
std::string RecoveryWorkerWriter::index_filename;
std::size_t RecoveryWorkerWriter::per_vector_size;  // donot count vid
std::vector<SPANNCID> RecoveryWorkerWriter::posting_order;
std::unordered_map<SPMerge::ClusterID, PostingOutputInfo>
    RecoveryWorkerWriter::posting_info;
const SPMerge::ClusterContentList* RecoveryWorkerWriter::p_clusterContent;
size_t RecoveryWorkerWriter::p_listOffset;
std::vector<std::promise<void>> RecoveryWorkerWriter::finished_flags;
std::vector<EventTimer> RecoveryWorkerWriter::timer_arr;

void RecoveryWorkerReader::reader_func(int tid) {
    auto& timer = timer_arr[tid];

    timer.event_start("[reader] total", false);

    SimpleIOHelper sio(f_new_native_io(), index_filename,
                       SimpleIOHelper::ReadOnly);
    size_t nbuffer = 0, nvector = 0;
    LoadBuffer* lb_ptr = nullptr;
    while (true) {
        // check load buffer pointer
        if (lb_ptr == nullptr) {
            rqueue.wait_dequeue(lb_ptr);
        }
        if (auto state = lb_ptr->state.load(); state != -1) {
            LOG_ERROR("reader get load buffer with state=%d\n", state);
            throw std::runtime_error("");
        }
        lb_ptr->vec_pos.clear();

        // read cluster
        auto idx = cluster_idx.fetch_add(1);
        if (idx >= p_postingOrder->size()) {
            // NOTE: reader finish
            LOG_DEBUG(
                "reader-%d exited with idx=%lu, #buffers=%llu, "
                "#vectors=%llu\n",
                tid, idx, nbuffer, nvector);
            timer.event_stop("[reader] total");
            return;
        }
        auto cid = p_postingOrder->at(idx);
        const auto& listinfo = p_listInfoMap->at(cid);
        auto read_offset = listinfo.listOffset;
        size_t totalBytes =
            (static_cast<size_t>(listinfo.listPageCount) << PageSizeEx);
        sio.f_read_data(lb_ptr->data.get(), totalBytes, read_offset);

        // parse vectors in buffer
        uint64_t offset_vid;
        uint64_t offset_vec;
        for (int i = 0; i < listinfo.listEleCount; i++) {
            RCCommonData::g_parsePosting(offset_vid, offset_vec, i,
                                         listinfo.listEleCount);

            auto start_ptr = lb_ptr->data.get() + listinfo.pageOffset;
            auto vid = *reinterpret_cast<int*>(start_ptr + offset_vid);
            if (visited[vid].exchange(true)) {
                continue;  // vector touched before
            }
            lb_ptr->vec_pos.emplace(vid, start_ptr + offset_vec);
        }

        // enqueue writer queue
        if (lb_ptr->vec_pos.empty()) {
            // reuse the buffer
        } else {
            lb_ptr->state = RCCommonData::NW;
            nvector += lb_ptr->vec_pos.size();
            for (int i = 0; i < RCCommonData::NW; i++) {
                auto success =
                    RecoveryWorkerWriter::wqueue_arr[i].enqueue(lb_ptr);
                if (not success) {
                    LOG_ERROR("reader %d failed to enqueue wqueue-%d\n", tid,
                              i);
                    throw std::runtime_error("");
                }
            }
            lb_ptr = nullptr;
            nbuffer++;
        }
    }
}

void RecoveryWorkerWriter::writer_func(int tid) {
    auto& my_queue = wqueue_arr[tid];
    auto& timer = timer_arr[tid];

    timer.event_start("[writer] total", false);

    // 1. split area
    timer.event_start("[writer] 1. split task", false);
    size_t cid_idx_start, cid_idx_end;
    f_split_task(tid, cid_idx_start, cid_idx_end);
    uint64_t start_byte, total_byte;
    {
        uint64_t p1 = f_get_start_page(cid_idx_start);
        uint64_t p2 = f_get_end_page(cid_idx_end);
        start_byte = p_listOffset + (p1 << PageSizeEx);
        total_byte = (p2 - p1 + 1) << PageSizeEx;
        LOG_INFO(
            "writer-%d cid range: %llu~%llu, page range: %llu~%llu, size: "
            "%s\n",
            tid, cid_idx_start, cid_idx_end, p1, p2,
            bytesToString(total_byte).c_str());
        for (auto idx : {cid_idx_start, cid_idx_end}) {
            auto cid = posting_order[idx];
            const auto& info = posting_info.at(cid);
            LOG_DEBUG(
                "idx %llu, cluster %lu: page_num=%llu, page_off=%llu, "
                "bytes=%llu, "
                "page_count=%llu\n",
                idx, cid, info.page_num, info.page_off, info.bytes,
                PAGE_COUNT(info.page_off + info.bytes));
        }
    }
    timer.event_stop("[writer] 1. split task");

    std::string local_index_name = f_get_local_index_name(tid);
    SimpleIOHelper sio(f_new_native_io(), local_index_name,
                       SimpleIOHelper::ReadWrite, true);
    fallocate(sio.m_ptr->GetFD(), 0, sio.m_stat.written_bytes, total_byte);

    // 2. calculate vector output position
    timer.event_start("[writer] 2. calculate vector offset", false);
    std::unordered_map<SPMerge::VectorID, std::vector<VectorDest>> vec_dest;
    std::unordered_map<SPMerge::VectorID, void*> pvec_dest;
    auto vecInfoSize = per_vector_size + sizeof(SPANNVID);
    for (auto idx = cid_idx_start; idx <= cid_idx_end; idx++) {
        SPMerge::ClusterID cid = posting_order[idx];

        uint16_t i = 0;
        const auto& listinfo = posting_info.at(cid);
        for (auto vid : p_clusterContent->at(cid)) {
            uint64_t offset_vid;
            uint64_t offset_vec;
            RCCommonData::g_parsePosting(offset_vid, offset_vec, i,
                                         listinfo.size);
            auto postingStart =
                p_listOffset +
                (static_cast<uint64_t>(listinfo.page_num) << PageSizeEx) +
                listinfo.page_off;

            VectorDest info{
                .cid = cid,
                .offset_vid = postingStart + offset_vid,
                .offset_vec = postingStart + offset_vec,
            };
            if (offset_vid + sizeof(SPANNVID) != offset_vec) {
                LOG_ERROR("offset mismatch, vid=%llu, vec=%llu\n", offset_vid,
                          offset_vec);
                throw std::runtime_error("");
            }
            f_set_final_pos(info.offset_vid + vecInfoSize);

            vec_dest[vid].push_back(info);
            i++;
        }
    }
    // the last writer is responsible for patch
    if (tid == RCCommonData::NW - 1) {
        auto pbuf = RCCommonData::failed_patch_buffer.get();
        auto& pvlist = *RCCommonData::patch_vid_list;
        pvec_dest.reserve(pvlist.size());
        for (size_t idx = 0; idx < pvlist.size(); idx++) {
            auto vid = pvlist[idx];
            pvec_dest[vid] = pbuf + idx * per_vector_size;
        }
    }
    timer.event_stop("[writer] 2. calculate vector offset");

    // 3. start write
    timer.event_start("[writer] 3. wait and write", false);
    LoadBuffer* lb_ptr;
    std::unique_ptr<char[]> vec_buf(new char[vecInfoSize]);
    size_t left1 = vec_dest.size(), left2 = pvec_dest.size();
    while (left1 or left2) {
        my_queue.wait_dequeue(lb_ptr);
        if (auto state = lb_ptr->state.load(); state < 0) {
            LOG_ERROR("writer get load buffer with state=%d\n", state);
            throw std::runtime_error("");
        }
        for (auto [vid, ptr] : lb_ptr->vec_pos) {
            if (vec_dest.count(vid) != 0) {
                memcpy(vec_buf.get(), &vid, sizeof(SPANNVID));
                memcpy(vec_buf.get() + sizeof(SPANNVID), ptr, per_vector_size);
                for (const auto& dest : vec_dest[vid]) {
                    sio.f_write_data(vec_buf.get(), vecInfoSize,
                                     dest.offset_vid - start_byte);
#ifdef DEBUG
                    if (dest.offset_vid - start_byte > total_byte) {
                        LOG_ERROR(
                            "worker-%d, start=%llu, vid=%llu, "
                            "vector-off=%llu, rel-off=%llu \n",
                            tid, start_byte, vid, dest.offset_vid,
                            dest.offset_vid - start_byte);
                    }
#endif
                }
                left1--;
            }
            // the last writer is responsible for patch
            if (tid == RCCommonData::NW - 1 and pvec_dest.count(vid)) {
                memcpy(pvec_dest[vid], ptr, per_vector_size);
                if (--left2 == 0) {
                    RCCommonData::patch_data_ready.set_value();
                }
            }
        }
        if (lb_ptr->state.operator--() == 0) {
            lb_ptr->state = -1;
            auto success = RecoveryWorkerReader::rqueue.enqueue(lb_ptr);
            if (not success) {
                LOG_ERROR("worker %d failed to enqueue\n", tid);
                throw std::runtime_error("");
            }
        }
    }
    timer.event_stop("[writer] 3. wait and write");

    // concat local result to final one
    timer.event_start("[writer] 4. merge result", false);
    finished_flags[tid].get_future().wait();
    f_concat_index_file(tid);
    finished_flags[tid + 1].set_value();
    timer.event_stop("[writer] 4. merge result");

    // fsync(sio.m_ptr->GetFD());
    LOG_DEBUG("writer-%d exited\n", tid);

    timer.event_stop("[writer] total");
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::LAR_CR(SPMerge::MachineID healthy_mid,
                                               SPMerge::MachineID fail_mid) {
    if (m_enableDeltaEncoding or m_enablePostingListRearrange) {
        NOT_IMPLEMENTED();
    }
    EventTimer timer;
    std::string load_recovery_name = "load from " +
                                     std::to_string(healthy_mid) +
                                     " to recovery " + std::to_string(fail_mid);
    timer.event_start(load_recovery_name);

    // place at first
    const int NW = m_options.m_numWriterThreads;
    const int NR = m_options.m_numReaderThreads;
    RCCommonData::reset();
    RecoveryWorkerReader::reset(NR);
    RecoveryWorkerWriter::reset(NW);

    m_options.m_curMid = healthy_mid;

    timer.event_start("1. Load Index");
    LoadIndex(m_options);  // load metadata, and load patch file
    timer.event_stop("1. Load Index");

    timer.event_start("2. Launch Workers");
    timer.event_start("2.1 Necessary computing task");
    const auto& p_meta = m_patchMeta[healthy_mid];
    const auto& mr = *m_recovery_mr;
    const auto& cluster_content = mr.cluster_content;
    const size_t vectorInfoSize = p_meta.perVectorSize + sizeof(SPANNVID);
    const auto totalVec = p_meta.n_clusvec + p_meta.n_headvec;
    // failed files
    auto fail_index_file = get_index_file_name(fail_mid, true);
    auto fail_patch_file = get_patch_file_name(fail_mid, true);
    // FIXME: chekc open mode
    SimpleIOHelper fail_index(f_new_native_io(), fail_index_file,
                              SimpleIOHelper::ReadWrite, true);
    SimpleIOHelper fail_patch(f_new_native_io(), fail_patch_file,
                              SimpleIOHelper::WriteOnly, true);

    // cluster output position
    auto& postingInfo = RecoveryWorkerWriter::posting_info;
    for (auto cid : mr.cluster_assignment[fail_mid]) {
        const auto& content = cluster_content.at(cid);
        PostingOutputInfo info;
        info.size = content.size();
        info.bytes = content.size() * vectorInfoSize;
        postingInfo.emplace(cid, info);
    }
    auto& postingOrderInIndex = RecoveryWorkerWriter::posting_order;
    auto napge = GetPostingOrder(mr.cluster_assignment[fail_mid], postingInfo,
                                 postingOrderInIndex);
    // fallocate(fail_index.m_ptr->GetFD(), 0,
    // fail_index.m_stat.written_bytes,
    //           (napge + 1) << PageSizeEx);
    // fsync(fail_index.m_ptr->GetFD());
    timer.event_stop("2.1 Necessary computing task");

    timer.event_start("2.2 Data init and thread launch");
    // global common data init
    RCCommonData::NW = NW;
    RCCommonData::g_parsePosting = [this](uint64_t& offset_vid,
                                          uint64_t& offset_vec, int i,
                                          int listEleCount) {
        (this->*m_parsePosting)(offset_vid, offset_vec, i, listEleCount);
    };
    RCCommonData::patch_vid_list = &mr.patch[fail_mid];
    auto fail_patch_bytes = mr.patch[fail_mid].size() * p_meta.perVectorSize;
    auto fpbuf = new char[fail_patch_bytes];
    RCCommonData::failed_patch_buffer.reset(fpbuf);

    // load buffer init
    LoadBuffer::buffer_arr.reset(new LoadBuffer[m_options.m_numLoadBuffers]);
    for (int i = 0; i < m_options.m_numLoadBuffers; i++) {
        LoadBuffer::buffer_arr[i].init(m_options.m_postingPageLimit, -1);
    }
    LOG_INFO("created %d load buffer, static memory size: %s\n",
             m_options.m_numLoadBuffers,
             bytesToString(m_options.m_numLoadBuffers * 1ull *
                           (m_options.m_postingPageLimit << PageSizeEx))
                 .c_str());

    // reader data init
    RecoveryWorkerReader::index_filename = get_index_file_name(healthy_mid);
    RecoveryWorkerReader::rqueue =
        LoadBuffer::Queue(m_options.m_numLoadBuffers);
    for (int i = 1; i < m_options.m_numLoadBuffers; i++) {
        auto lb_ptr = &LoadBuffer::buffer_arr[i];
        auto success = RecoveryWorkerReader::rqueue.enqueue(lb_ptr);
        if (not success) {
            LOG_ERROR("main thread enqueue rqueue (buffer-%d) failed\n", i);
            throw std::runtime_error("");
        }
    }
    RecoveryWorkerReader::p_listInfoMap = &m_listInfoMap;
    RecoveryWorkerReader::p_postingOrder = &m_postingOrderInIndex;
    RecoveryWorkerReader::visited.reset(new std::atomic_bool[totalVec]{false});

    // writer data init
    RecoveryWorkerWriter::wqueue_arr.reset(new LoadBuffer::Queue[NW]);
    RecoveryWorkerWriter::index_filename = fail_index_file;
    RecoveryWorkerWriter::per_vector_size = p_meta.perVectorSize;
    RecoveryWorkerWriter::p_clusterContent = &cluster_content;
    // NOTE: when written to multi files, each writer use relative offset,
    // so p_listOffset is not necessary
    RecoveryWorkerWriter::p_listOffset = fail_index.m_stat.written_bytes;

    {
        auto lb_ptr = &LoadBuffer::buffer_arr[0];
        lb_ptr->state = NW;
        auto vec_ptr = m_recovery_pbuf_vec.get();
        for (auto vid : mr.patch[healthy_mid]) {
            lb_ptr->vec_pos.emplace(vid, vec_ptr);
            vec_ptr += p_meta.perVectorSize;
        }
        for (int i = 0; i < NW; i++) {
            auto success = RecoveryWorkerWriter::wqueue_arr[i].enqueue(lb_ptr);
            if (not success) {
                LOG_ERROR("main thread enqueue wqueue-%d failed\n", i);
                throw std::runtime_error("");
            }
        }
    }

    // launch readers & writers
    std::vector<std::thread> reader_threads, writer_threads;
    for (int i = 0; i < m_options.m_numReaderThreads; i++) {
        reader_threads.emplace_back(RecoveryWorkerReader::reader_func, i);
    }
    for (int i = 0; i < NW; i++) {
        writer_threads.emplace_back(RecoveryWorkerWriter::writer_func, i);
    }
    timer.event_stop("2.2 Data init and thread launch");
    timer.event_stop("2. Launch Workers");

    timer.event_start("3. Wait Workers");

    timer.event_start("3.(overlapped) Metadata recovery");
    // index meta
    OutputMachineIndexMeta(postingInfo, postingOrderInIndex, totalVec,
                           m_iDataDimension, fail_index);
    RecoveryWorkerWriter::finished_flags[0].set_value();
    fail_index.close();

    // patch meta
    PatchMetadata fail_meta;
    OutputMachinePatchMeta(fail_mid, mr, postingOrderInIndex,
                           p_meta.perVectorSize, fail_meta, fail_patch);
    timer.event_stop("3.(overlapped) Metadata recovery");

    // wait for patch data
    timer.event_start("3.(overlapped) Patch vector recovery");
    RCCommonData::patch_data_ready.get_future().wait();
    fail_patch.f_write_data(fpbuf, fail_patch_bytes);
    fail_patch.f_write_padding();
    fail_patch.close();
    timer.event_stop("3.(overlapped) Patch vector recovery");

    for (auto& t : reader_threads) t.join();
    for (auto& t : writer_threads) t.join();
    // RecoveryWorkerWriter::f_concat_index_files_all();

    sync();

    timer.event_stop("3. Wait Workers");

    timer.event_stop(load_recovery_name);

    timer.log_all_event_duration();

    // EventTimer::generate_average_timer(RecoveryWorkerReader::timer_arr)
    //     .log_all_event_duration_and_clear();
    // EventTimer::generate_average_timer(RecoveryWorkerWriter::timer_arr)
    //     .log_all_event_duration_and_clear();
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::LAR_GL(SPMerge::MachineID healthy_mid,
                                               SPMerge::MachineID fail_mid) {
    if (m_enableDeltaEncoding) {
        NOT_IMPLEMENTED();
    }
    EventTimer timer;
    std::string load_recovery_name = "load from " +
                                     std::to_string(healthy_mid) +
                                     " to recovery " + std::to_string(fail_mid);
    timer.event_start(load_recovery_name);

    m_options.m_curMid = healthy_mid;

    // 1. read from disk
    timer.event_start("1. Load Index");
    LoadIndex(m_options);  // load metadata, and load patch file
    timer.event_stop("1. Load Index");

    // 2. prepare data
    timer.event_start("2. Prepare Data");
    const auto& p_meta = m_patchMeta[healthy_mid];
    const auto& mr = *m_recovery_mr;
    const auto& my_clusters = mr.cluster_assignment[healthy_mid];
    const auto& cluster_content = mr.cluster_content;

    // analyze vector position
    timer.event_start("2.1");
    const auto totalVec = p_meta.n_clusvec + p_meta.n_headvec;
    Helper::vector_recovery<SPMerge::VectorPosition> vec_pos(totalVec);
    char* ptr = m_recovery_pbuf_vec.get();
    for (auto vid : mr.patch[healthy_mid]) {
        vec_pos[vid].where = SPMerge::VectorPosition::Memory;
        vec_pos[vid].mem_pos.ptr = ptr;
        ptr += p_meta.perVectorSize;
    }
    for (auto cid : my_clusters) {
        for (auto vid : cluster_content.at(cid)) {
            if (vec_pos[vid].where == SPMerge::VectorPosition::Unavailable) {
                vec_pos[vid].where = SPMerge::VectorPosition::Disk;
                vec_pos[vid].disk_pos.cid = cid;
                vec_pos[vid].disk_pos.list_info = &m_listInfoMap.at(cid);
            }
        }
    }
    timer.event_stop("2.1");

    // analyze who need vector, and other cluster info
    timer.event_start("2.2");
    Helper::vector_recovery<Helper::unordered_set_recovery<SPMerge::ClusterID>>
        who_need(totalVec);
    // NOTE: This data structure is used for output, no need to track
    std::unordered_map<SPMerge::ClusterID, PostingOutputInfo> postingInfo;
    const size_t vectorInfoSize = p_meta.perVectorSize + sizeof(SPANNVID);
    for (auto cid : mr.cluster_assignment[fail_mid]) {
        const auto& content = cluster_content.at(cid);
        for (auto vid : content) {
            who_need[vid].insert(cid);
        }
        PostingOutputInfo info;
        info.size = content.size();
        info.bytes = content.size() * vectorInfoSize;
        postingInfo.emplace(cid, info);
    }
    timer.event_stop("2.2");

    // fail patch info
    timer.event_start("2.3");
    auto fail_patch_bytes = mr.patch[fail_mid].size() * p_meta.perVectorSize;
    Helper::tracked_buffer fail_pbuf(
        Helper::make_tracked_buffer(fail_patch_bytes));
    Helper::unordered_map_recovery<SPMerge::VectorID, void*>
        vec_pos_in_fail_patch;
    ptr = fail_pbuf.get();
    for (auto vid : mr.patch[fail_mid]) {
        if (vec_pos[vid].where == SPMerge::VectorPosition::Memory) {
            memcpy(ptr, vec_pos[vid].mem_pos.ptr, p_meta.perVectorSize);
        } else {
            who_need[vid].insert(SPMerge::InvalidCID);  // Invalid means patch
            vec_pos_in_fail_patch.emplace(vid, ptr);
        }
        ptr += p_meta.perVectorSize;
    }
    timer.event_stop("2.3");

    std::vector<SPANNCID> postingOrderInIndex;  // data for output
    GetPostingOrder(mr.cluster_assignment[fail_mid], postingInfo,
                    postingOrderInIndex);

    timer.event_stop("2. Prepare Data");

    timer.event_start("3. Recover Files");
    // open file
    auto my_index_file = get_index_file_name(healthy_mid);
    auto fail_index_file = get_index_file_name(fail_mid, true);
    auto fail_patch_file = get_patch_file_name(fail_mid, true);
    SimpleIOHelper my_index(my_index_file, SimpleIOHelper::ReadOnly);
    SimpleIOHelper fail_index(fail_index_file, SimpleIOHelper::WriteOnly);
    SimpleIOHelper fail_patch(fail_patch_file, SimpleIOHelper::WriteOnly);

    // output metadata first
    timer.event_start("3.1 write files");
    OutputMachineIndexMeta(postingInfo, postingOrderInIndex, totalVec,
                           m_iDataDimension, fail_index);
    PatchMetadata fail_meta;
    OutputMachinePatchMeta(fail_mid, mr, postingOrderInIndex,
                           p_meta.perVectorSize, fail_meta, fail_patch);
    timer.event_stop("3.1 write files");

    // buffer for disk load
    Helper::tracked_buffer load_buffer(Helper::make_tracked_buffer(
        m_options.m_postingPageLimit << PageSizeEx));
    size_t diskIO = 0;
    auto f_read_posting_sync = [this, &vec_pos, &timer, &load_buffer, &my_index,
                                &diskIO, &p_meta](const ListInfo& list_info) {
        timer.event_start("3.2 load cluster from disk", false);
        auto totalBytes =
            (static_cast<size_t>(list_info.listPageCount) << PageSizeEx);
        auto offset = list_info.listOffset;
        my_index.f_read_data(load_buffer.get(), totalBytes, offset);
        timer.event_stop("3.2 load cluster from disk");
        diskIO++;
        void* start_ptr = load_buffer.get() + list_info.pageOffset;

        for (int i = 0; i < list_info.listEleCount; i++) {
            uint64_t offset_vid;
            uint64_t offset_vec;

            (this->*m_parsePosting)(offset_vid, offset_vec, i,
                                    list_info.listEleCount);
            auto vid = *reinterpret_cast<SPANNVID*>(start_ptr + offset_vid);
            auto& pos = vec_pos[vid];
            if (pos.where == SPMerge::VectorPosition::Disk) {
                pos.where = SPMerge::VectorPosition::Loaded;
                pos.load_pos.ptr =
                    Helper::make_tracked_buffer(p_meta.perVectorSize);
                timer.event_start("3.3 memory copies", false);
                memcpy(pos.load_pos.ptr.get(), start_ptr + offset_vec,
                       p_meta.perVectorSize);
                timer.event_stop("3.3 memory copies");
            }
        }
    };

    // buffer for disk output
    Helper::tracked_buffer output_buffer(Helper::make_tracked_buffer(
        m_options.m_postingPageLimit << PageSizeEx));
    const size_t list_start = fail_index.m_stat.written_bytes;
    uint64_t listOffset = 0;

    // recover index file
    for (auto cid : postingOrderInIndex) {  // NOTE: maybe optimized
        const auto& info = postingInfo.at(cid);
        uint64_t targetOffset =
            static_cast<uint64_t>(info.page_num) * PageSize + info.page_off;
        // write padding vals before the posting list
        if (targetOffset > listOffset) {
            timer.event_start("3.1 write files", false);
            fail_index.f_write_padding(list_start + targetOffset);
            timer.event_stop("3.1 write files");
            listOffset = targetOffset;
        }

        size_t idx = 0;
        for (auto vid : cluster_content.at(cid)) {
            uint64_t offset_vid;
            uint64_t offset_vec;
            (this->*m_parsePosting)(offset_vid, offset_vec, idx++, info.size);
            auto ptr_vid = output_buffer.get() + offset_vid;
            auto ptr_vec = output_buffer.get() + offset_vec;

            SPANNVID svid = vid;
            void* src_ptr{nullptr};

            switch (vec_pos[vid].where) {
                case SPMerge::VectorPosition::Disk:
                    f_read_posting_sync(*vec_pos[vid].disk_pos.list_info);
                case SPMerge::VectorPosition::Loaded:
                    src_ptr = vec_pos[vid].load_pos.ptr.get();
                    break;
                case SPMerge::VectorPosition::Memory:
                    src_ptr = vec_pos[vid].mem_pos.ptr;
                    break;
                case SPMerge::VectorPosition::Outdated:
                    LOG_ERROR("vector %llu is already outdated!\n", vid);
                default:
                    throw std::runtime_error("Should not reach here!");
            }

            timer.event_start("3.3 memory copies", false);
            memcpy(ptr_vid, &svid, sizeof(SPANNVID));
            memcpy(ptr_vec, src_ptr, p_meta.perVectorSize);
            timer.event_stop("3.3 memory copies");

            // maintain infos
            timer.event_start("3.4 maintain data structures", false);
            if (who_need[vid].erase(cid) == 0) {
                LOG_ERROR("cid=%lu not exist in who_need[vid=%llu]\n", cid,
                          vid);
                throw std::runtime_error("");
            }
            if (who_need[vid].size() == 0) {
                if (vec_pos[vid].where == SPMerge::VectorPosition::Loaded) {
                    vec_pos[vid].where = SPMerge::VectorPosition::Outdated;
                    vec_pos[vid].load_pos.ptr.reset();
                }
            }
            timer.event_stop("3.4 maintain data structures");
        }

        timer.event_start("3.1 write files", false);
        fail_index.f_write_data(output_buffer.get(), info.bytes);
        timer.event_stop("3.1 write files");
        listOffset += info.bytes;
    }
    timer.event_start("3.1 write files", false);
    fail_index.f_write_padding();
    timer.event_stop("3.1 write files");

    // recover patch file
    size_t index_diskIO = diskIO;
    size_t lonely_vectors = 0;
    diskIO = 0;
    for (auto vid : mr.patch[fail_mid]) {
        auto& pos = vec_pos[vid];
        auto& cid_list = who_need[vid];
        // data checks
        if (cid_list.size() == 0) continue;
        if (cid_list.size() != 1) {
            LOG_ERROR(
                "After index file output, "
                "vid=%llu is needed by %llu clusters (expected 1)\n",
                vid, cid_list.size());
            throw std::runtime_error("");
        }
        if (*cid_list.begin() != SPMerge::InvalidCID) {
            LOG_ERROR(
                "After index file output, "
                "vid=%llu is needed by the cluster %lu (expected -1)\n",
                vid, *cid_list.begin());
            throw std::runtime_error("");
        }
        switch (pos.where) {
            case SPMerge::VectorPosition::Disk:
                f_read_posting_sync(*pos.disk_pos.list_info);
            case SPMerge::VectorPosition::Loaded:
                lonely_vectors++;
                break;
            default:
                LOG_ERROR("Should not reach here, pos.where=%d!\n", pos.where);
                throw std::runtime_error("");
        }
        // memory copy
        timer.event_start("3.3 memory copies", false);
        memcpy(vec_pos_in_fail_patch.at(vid), pos.load_pos.ptr.get(),
               p_meta.perVectorSize);
        timer.event_stop("3.3 memory copies");

        // deinit
        timer.event_start("3.4 maintain data structures", false);
        cid_list.clear();
        pos.load_pos.ptr.reset();
        pos.where = SPMerge::VectorPosition::Outdated;
        vec_pos_in_fail_patch.erase(vid);
        timer.event_stop("3.4 maintain data structures");
    }
    if (not vec_pos_in_fail_patch.empty()) {
        throw std::runtime_error(
            "Patch construction is done, why are you not empty?");
    }

    timer.event_start("3.1 write files", false);
    fail_patch.f_write_data(fail_pbuf.get(), fail_patch_bytes);
    fail_patch.f_write_padding();
    fail_index.close();
    fail_patch.close();
    timer.event_stop("3.1 write files");

    timer.event_stop("3. Recover Files");

    timer.event_stop(load_recovery_name);
    LOG_INFO(
        "Recovery done, disk read during index recovery=%llu, "
        "disk read during patch recovery=%llu, #lonely-vectors=%llu\n",
        index_diskIO, diskIO, lonely_vectors);
    LOG_INFO("Recovered index file: %s\n", fail_index_file.c_str());
    LOG_INFO("Recovered patch file: %s\n", fail_patch_file.c_str());
    timer.log_all_event_duration();
    Helper::MemoryTracker::dumpCurrentStats();
    Helper::MemoryTracker::dumpPeakStats();
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::LAR_SL(SPMerge::MachineID healthy_mid,
                                               SPMerge::MachineID fail_mid) {
    EventTimer timer;
    std::string load_recovery_name = "load from " +
                                     std::to_string(healthy_mid) +
                                     " to recovery " + std::to_string(fail_mid);
    timer.event_start(load_recovery_name);

    m_options.m_curMid = healthy_mid;

    // 1. read from disk
    timer.event_start("1. Disk Read");
    LoadIndex(m_options);  // load metadata, and load patch file
    timer.event_stop("1. Disk Read");

    // 2. prepare data
    timer.event_start("2. Prepare Data");
    const auto& p_meta = m_patchMeta[healthy_mid];
    const auto& mr = *m_recovery_mr;
    const auto& my_clusters = mr.cluster_assignment[healthy_mid];
    const auto& cluster_content = mr.cluster_content;

    auto f_parsePosting = [this](uint64_t& offsetVectorID,
                                 uint64_t& offsetVector, int i, int eleCount) {
        (this->*m_parsePosting)(offsetVectorID, offsetVector, i, eleCount);
    };
    auto vectorset = std::make_shared<RecoveryStupidVectorset>(
        GetEnumValueType<ValueType>(), m_iDataDimension, p_meta.n_headvec,
        p_meta.n_clusvec, get_index_file_name(healthy_mid),
        m_options.m_postingPageLimit, f_parsePosting);
    std::shared_ptr<VectorSetReader> reader(new RecoveryReader(vectorset));
    // fill content in vector set
    auto& vec_pos = *reinterpret_cast<std::vector<SPMerge::VectorPosition>*>(
        vectorset->GetData());
    // iterate patch
    char* ptr = m_recovery_pbuf_vec.get();
    for (auto vid : mr.patch[healthy_mid]) {
        vec_pos[vid].where = SPMerge::VectorPosition::Memory;
        vec_pos[vid].mem_pos.ptr = ptr;
        ptr += p_meta.perVectorSize;
    }
    for (auto cid : my_clusters) {
        for (auto vid : cluster_content.at(cid)) {
            if (vec_pos[vid].where == SPMerge::VectorPosition::Unavailable) {
                vec_pos[vid].where = SPMerge::VectorPosition::Disk;
                vec_pos[vid].disk_pos.cid = cid;
                vec_pos[vid].disk_pos.list_info = &m_listInfoMap.at(cid);
            }
        }
    }
    timer.event_stop("2. Prepare Data");

    // output
    timer.event_start("3. Output Recovery File");
    m_options.m_curMid = fail_mid;
    SaveMergerResultFiles(mr, reader);
    timer.event_stop("3. Output Recovery File");

    timer.event_stop(load_recovery_name);
    timer.log_all_event_duration();
    Helper::MemoryTracker::dumpCurrentStats();
    Helper::MemoryTracker::dumpPeakStats();
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::LAR_SW(SPMerge::MachineID healthy_mid,
                                               SPMerge::MachineID fail_mid) {
    if (m_enableDeltaEncoding or m_enablePostingListRearrange) {
        NOT_IMPLEMENTED();
    }
    EventTimer timer;
    std::string load_recovery_name = "load from " +
                                     std::to_string(healthy_mid) +
                                     " to recovery " + std::to_string(fail_mid);
    timer.event_start(load_recovery_name);

    m_options.m_curMid = healthy_mid;

    // 1. read from disk
    timer.event_start("1. Load Index");
    LoadIndex(m_options);  // load metadata, and load patch file
    timer.event_stop("1. Load Index");

    // 2. prepare data
    timer.event_start("2. Recover Metadata");
    const auto& p_meta = m_patchMeta[healthy_mid];
    const auto& mr = *m_recovery_mr;
    const auto& cluster_content = mr.cluster_content;
    // my index file
    auto my_index_file = get_index_file_name(healthy_mid);
    SimpleIOHelper my_index(my_index_file, SimpleIOHelper::ReadOnly);
    // failed files
    auto fail_index_file = get_index_file_name(fail_mid, true);
    auto fail_patch_file = get_patch_file_name(fail_mid, true);
    SimpleIOHelper fail_index(fail_index_file, SimpleIOHelper::ReadWrite, true);
    SimpleIOHelper fail_patch(fail_patch_file, SimpleIOHelper::WriteOnly);

    // cluster output position
    const size_t vectorInfoSize = p_meta.perVectorSize + sizeof(SPANNVID);
    const auto totalVec = p_meta.n_clusvec + p_meta.n_headvec;
    std::unordered_map<SPMerge::ClusterID, PostingOutputInfo> postingInfo;
    for (auto cid : mr.cluster_assignment[fail_mid]) {
        const auto& content = cluster_content.at(cid);
        PostingOutputInfo info;
        info.size = content.size();
        info.bytes = content.size() * vectorInfoSize;
        postingInfo.emplace(cid, info);
    }
    std::vector<SPANNCID> postingOrderInIndex;
    GetPostingOrder(mr.cluster_assignment[fail_mid], postingInfo,
                    postingOrderInIndex);
    // MemoryTracker::checkStats("after calc posting order");

    // output metadata first
    OutputMachineIndexMeta(postingInfo, postingOrderInIndex, totalVec,
                           m_iDataDimension, fail_index);
    PatchMetadata fail_meta;
    OutputMachinePatchMeta(fail_mid, mr, postingOrderInIndex,
                           p_meta.perVectorSize, fail_meta, fail_patch);
    // MemoryTracker::checkStats("after recover meta");

    free_space(postingOrderInIndex);
    // MemoryTracker::checkStats("after free posting order");

    timer.event_stop("2. Recover Metadata");

    timer.event_start("3. Recover Data");
    const auto listOffset = fail_index.m_stat.written_bytes;
    // analyze vector destination
    std::atomic<uint64_t> index_final_vec_pos = 0;
    auto f_set_final_pos = [&index_final_vec_pos](uint64_t new_v) {
        auto old = index_final_vec_pos.load();
        while (old < new_v) {
            if (index_final_vec_pos.compare_exchange_weak(old, new_v)) {
                break;
            }
        }
    };
    struct VectorDest {
        SPMerge::ClusterID cid;
        uint64_t offset_vid;
        uint64_t offset_vec;
    };
    std::atomic<uint64_t> dest_mem_usage{0}, thread_mem_usage{0};
    Helper::vector_recovery<Helper::vector_recovery<VectorDest>> vector_dest;
    {
        // JemallocProfiler::Initialize();

        timer.event_start("3.1 calculating vector destination");

        std::unique_ptr<ExternalMemUsageGuard> thread_mem_guard(
            new ExternalMemUsageGuard(thread_mem_usage,
                                      Helper::MEMORY_SOURCE::SPMergeRecovery));
        auto num_thread = m_options.m_recoveryNumThreads;  // FIXME: oom risk
        timer.event_start("3.1.1 thread local data init");
        std::vector<std::atomic_int8_t> vec_count(totalVec);
        std::vector<
            std::unordered_map<SPMerge::VectorID, std::vector<VectorDest>>>
            local_vector_dest(num_thread);
        // MemoryTracker::checkStats("before calc vector position");
        timer.event_stop("3.1.1 thread local data init");

        timer.event_start("3.1.2 vector for clusters");
#pragma omp parallel for num_threads(num_thread)
        for (auto cid : mr.cluster_assignment[fail_mid]) {
            uint16_t i = 0;
            const auto& listinfo = postingInfo.at(cid);
            for (auto vid : cluster_content.at(cid)) {
                uint64_t offset_vid;
                uint64_t offset_vec;
                (this->*m_parsePosting)(offset_vid, offset_vec, i,
                                        listinfo.size);
                auto postingStart =
                    listOffset +
                    (static_cast<uint64_t>(listinfo.page_num) << PageSizeEx) +
                    listinfo.page_off;

                VectorDest info{
                    .cid = cid,
                    .offset_vid = postingStart + offset_vid,
                    .offset_vec = postingStart + offset_vec,
                };
                f_set_final_pos(info.offset_vec);

                local_vector_dest[omp_get_thread_num()][vid].push_back(info);
                vec_count[vid]++;
                i++;
            }
        }
        timer.event_stop("3.1.2 vector for clusters");

        // MemoryTracker::checkStats("during calc vector position");

        timer.event_start("3.1.3 vector for patch");
#pragma omp parallel for num_threads(num_thread)
        for (size_t idx = 0; idx < mr.patch[fail_mid].size(); idx++) {
            auto vid = mr.patch[fail_mid][idx];
            VectorDest info{
                .cid = SPMerge::InvalidCID,
                .offset_vec = idx * p_meta.perVectorSize
                // + fail_meta.vectorContentStart
                ,
            };
            local_vector_dest[omp_get_thread_num()][vid].push_back(info);
            vec_count[vid]++;
        }
        timer.event_stop("3.1.3 vector for patch");
        thread_mem_guard.reset();
        free_space(postingInfo);

        // MemoryTracker::checkStats("start merge threads' result");

        // JemallocProfiler::DumpProfile("during.snapshot");

        Helper::MemoryUsageGuard mem_guard(dest_mem_usage);
        vector_dest.resize(totalVec);
        timer.event_start("3.1.4 merge result");
#pragma omp parallel for
        for (SPMerge::VectorID vid = 0; vid < totalVec; vid++) {
            auto& dst = vector_dest[vid];
            if (vec_count[vid] == 0) continue;
            dst.reserve(vec_count[vid]);
            for (auto thread = 0; thread < num_thread; thread++) {
                auto& map = local_vector_dest[thread];
                if (map.find(vid) == map.end()) continue;
                auto& src = map.at(vid);
                auto beg = dst.size();
                dst.resize(beg + src.size());
                memcpy(dst.data() + beg, src.data(),
                       src.size() * sizeof(VectorDest));
                free_space(src);
            }
        }
        // JemallocProfiler::DumpProfile("before-free.snapshot1");

        free_space(vec_count);
        free_space(local_vector_dest);
        // mallctl("arena.0.purge_all", NULL, NULL, NULL, 0);

        // JemallocProfiler::DumpProfile("after-free.snapshot1");

        // JemallocProfiler::StopProfiling();

        timer.event_stop("3.1.4 merge result");
        timer.event_stop("3.1 calculating vector destination");
        // MemoryTracker::checkStats("calc vector position done");
    }
    LOG_INFO(
        "vector destination memory usage: %s, "
        "thread local memory usage: %s\n",
        bytesToString(dest_mem_usage.load()).c_str(),
        bytesToString(thread_mem_usage.load()).c_str());
    // timer.log_all_event_duration();

    // allocate buffers
    Helper::tracked_buffer load_buffer(Helper::make_tracked_buffer(
        m_options.m_postingPageLimit << PageSizeEx));
    Helper::tracked_buffer fail_pbuffer(Helper::make_tracked_buffer(
        mr.patch[fail_mid].size() * p_meta.perVectorSize));
    Helper::vector_recovery<bool> vector_touched(totalVec);

    Helper::tracked_buffer output_buffer(
        Helper::make_tracked_buffer(1024 * vectorInfoSize));
    auto const output_buf_end = output_buffer.get() + 1024 * vectorInfoSize;
    MemoryTracker::checkStats("after alloc static buffer");

    // buffer to fully utilize memory
    struct OutputPair {
        uint64_t offset;
        void* vec_ptr;
        SPMerge::VectorID vid;
        bool operator<(const OutputPair& other) {
            return offset < other.offset;
        }
    };

    const auto memAvailable = Helper::MemoryTracker::getFreeMemorySize();
    LOG_INFO("Current memory usage: %s, memory available:%s\n",
             bytesToString(MemoryTracker::getUsedMemorySize()).c_str(),
             bytesToString(memAvailable).c_str());
    const size_t replicaPerMachine =
        std::ceil(1. * m_options.m_replicaCount / m_options.m_numMachine);
    const size_t nvecInPatch = mr.patch[healthy_mid].size();
    size_t NVecInBuffer = m_options.m_numVecBuffers;
    // if not configured, we calculate it by maxmizing memroy usage
    if (NVecInBuffer == 0) {
        const auto safetyFactor = 0.8;
        const size_t memPairPerVec = sizeof(OutputPair) * replicaPerMachine;
        const size_t memPerVecInBuffer =  //
            p_meta.perVectorSize          // vector buffer
            + memPairPerVec               // output pairs
            ;

        size_t left = 1;
        size_t right = memAvailable / memPerVecInBuffer;

        while (left < right) {
            size_t mid = (left + right + 1) / 2;
            size_t memory_needed =             //
                mid * memPerVecInBuffer        //
                + nvecInPatch * memPairPerVec  //
                // + mid * logn * memPairPerVec   //
                ;

            if (memory_needed <= memAvailable * safetyFactor) {
                left = mid;
            } else {
                right = mid - 1;
            }
        }
        NVecInBuffer = std::min(p_meta.n_clusvec, left);
    }

    size_t buffer_size = NVecInBuffer * p_meta.perVectorSize;
    Helper::tracked_buffer vector_buffer(
        Helper::make_tracked_buffer(buffer_size));
    auto const vec_buf_end = vector_buffer.get() + buffer_size;
    MemoryTracker::checkStats("after alloc vector buffer");

    const auto nPairLimit = (NVecInBuffer + nvecInPatch) * replicaPerMachine;
    Helper::vector_recovery<OutputPair> output_pairs;
    output_pairs.reserve(nPairLimit);
    LOG_INFO(
        "%lu vectors in buffer, and %lu pair reserved, "
        "after buffer create, current memory usage: %s\n",
        NVecInBuffer, nPairLimit,
        bytesToString(MemoryTracker::getUsedMemorySize()).c_str());
    MemoryTracker::checkStats("after reserve pair buffer");

    auto ifile = get_index_file_name(healthy_mid);
    SimpleIOHelper ihelper(ifile, SimpleIOHelper::ReadOnly);

    auto f_handle_patch = [&fail_pbuffer, &vector_dest, &p_meta](
                              SPMerge::VectorID vid, void* ptr) {
        if (vector_dest[vid][0].cid != SPMerge::InvalidCID) return false;
        if (vector_dest[vid].size() != 1) {
            LOG_ERROR(
                "vid=%llu has %lu destinations, "
                "expected 1 (patch)\n",
                vid, vector_dest[vid].size());
            throw std::runtime_error("");
        }
        memcpy(fail_pbuffer.get() + vector_dest[vid][0].offset_vec, ptr,
               p_meta.perVectorSize);
        return true;
    };

    auto f_prepare_output_pairs = [&output_pairs, &vector_dest, &nPairLimit](
                                      SPMerge::VectorID vid, void* ptr) {
        for (auto dest : vector_dest[vid]) {
            OutputPair op{
                .offset = dest.offset_vid,
                .vec_ptr = ptr,
                .vid = vid,
            };
            if (output_pairs.size() == nPairLimit) {
                throw std::runtime_error("pairs vector is full!");
            }
            output_pairs.push_back(op);
        }
    };

    // return if the vector is a needed by a cluster (on fail machine)
    auto f_handle_a_vector = [&vector_touched, &f_handle_patch,
                              &f_prepare_output_pairs, &p_meta, &vec_buf_end](
                                 SPMerge::VectorID vid, void* vec_ptr,
                                 void* buffer = nullptr) {
        if (vector_touched[vid]) return false;
        vector_touched[vid] = true;
        if (f_handle_patch(vid, vec_ptr)) {
            return false;
        }
        // if buffer end is reached, stop copying
        if (buffer != nullptr and buffer < vec_buf_end) {
            memcpy(buffer, vec_ptr, p_meta.perVectorSize);
            vec_ptr = buffer;
        }
        f_prepare_output_pairs(vid, vec_ptr);
        return true;
    };

    size_t continousIO = 0, diskIO = 0;
    auto f_output_data = [&fail_index, &timer, &diskIO, &continousIO,
                          &output_buffer, &output_buf_end, &output_pairs,
                          &p_meta, &vectorInfoSize]() {
        // for (const auto& pair : output_pairs) {
        // const uint batchsize = output_pairs.size();
        size_t idx = 0;
        while (idx < output_pairs.size()) {
            auto ptr = output_buffer.get();
            size_t sub_idx = idx;
            auto prev_offset = output_pairs[idx].offset - vectorInfoSize;
            for (; sub_idx < output_pairs.size(); sub_idx++) {
                const auto& pair = output_pairs[sub_idx];
                if (pair.offset - prev_offset != vectorInfoSize) {
                    break;
                }
                prev_offset = pair.offset;
                // copy data
                SPANNVID svid = pair.vid;
                memcpy(ptr, &svid, sizeof(svid));
                ptr += sizeof(svid);
                memcpy(ptr, pair.vec_ptr, p_meta.perVectorSize);
                ptr += p_meta.perVectorSize;
                continousIO++;
                if (ptr == output_buf_end) break;
            }

            diskIO++;
            timer.event_start("3.2 write data to disk", false);
            fail_index.f_write_data(output_buffer.get(),
                                    (sub_idx - idx) * vectorInfoSize,
                                    output_pairs[idx].offset);
            timer.event_stop("3.2 write data to disk");
            idx = sub_idx;
        }
    };

    // vectors in patch file
    LOG_INFO("Handling vectors in patch\n");
    auto vec_ptr = m_recovery_pbuf_vec.get();
    for (auto vid : mr.patch[healthy_mid]) {
        f_handle_a_vector(vid, vec_ptr);
        vec_ptr += p_meta.perVectorSize;
    }

    // vectors in index file
    LOG_INFO("Handling vectors from index file\n");
    vec_ptr = vector_buffer.get();
    for (auto cid : m_postingOrderInIndex) {  // TODO: readahead explicitly
        const auto& listinfo = m_listInfoMap.at(cid);
        auto read_offset = listinfo.listOffset;
        size_t totalBytes =
            (static_cast<size_t>(listinfo.listPageCount) << PageSizeEx);
        timer.event_start("3.3 load cluster from disks", false);
        ihelper.f_read_data(load_buffer.get(), totalBytes, read_offset);
        timer.event_stop("3.3 load cluster from disks");
        uint64_t offset_vid;
        uint64_t offset_vec;
        for (int i = 0; i < listinfo.listEleCount; i++) {
            (this->*m_parsePosting)(offset_vid, offset_vec, i,
                                    listinfo.listEleCount);

            auto start_ptr = load_buffer.get() + listinfo.pageOffset;
            auto vid = *reinterpret_cast<int*>(start_ptr + offset_vid);

            if (f_handle_a_vector(vid, start_ptr + offset_vec, vec_ptr)) {
                vec_ptr += p_meta.perVectorSize;
            }
        }
        if (vec_ptr >= vec_buf_end) {
            timer.event_start("3.4 sort output order", false);
            std::sort(output_pairs.begin(), output_pairs.end());
            timer.event_stop("3.4 sort output order");
            f_output_data();
            vec_ptr = vector_buffer.get();
            output_pairs.clear();
        }
    }

    LOG_INFO("Handling vectors left\n");
    if (vec_ptr != vector_buffer.get()) {
        // MemoryTracker::checkStats("batch output for left vectors");
        timer.event_start("3.4 sort output order", false);
        std::sort(output_pairs.begin(), output_pairs.end());
        timer.event_stop("3.4 sort output order");
        f_output_data();
        vec_ptr = vector_buffer.get();
        output_pairs.clear();
    }

    // MemoryTracker::checkStats("index data output done");
    auto index_end = index_final_vec_pos + p_meta.perVectorSize;
    fail_index.f_write_padding_at(index_end);
    fail_index.close();
    // MemoryTracker::checkStats("index file close");
    fail_patch.f_write_data(fail_pbuffer.get(),
                            mr.patch[fail_mid].size() * p_meta.perVectorSize);
    fail_patch.f_write_padding();
    fail_patch.close();
    // MemoryTracker::checkStats("patch file close");
    sync();
    // MemoryTracker::checkStats("after sync");
    timer.event_stop("3.2 write data to disk");
    timer.event_stop("3. Recover Data");
    timer.event_stop(load_recovery_name);
    LOG_INFO("Recovered index file: %s\n", fail_index_file.c_str());
    LOG_INFO("Recovered patch file: %s\n", fail_patch_file.c_str());
    LOG_INFO("DiskIO: %llu, ContinousIO: %llu\n", diskIO, continousIO);
    timer.log_all_event_duration();
    Helper::MemoryTracker::dumpCurrentStats();
    Helper::MemoryTracker::dumpPeakStats();
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::LAR_SC(SPMerge::MachineID healthy_mid,
                                               SPMerge::MachineID fail_mid) {
    EventTimer timer;
    std::string load_recovery_name = "load from " +
                                     std::to_string(healthy_mid) +
                                     " to recovery " + std::to_string(fail_mid);
    timer.event_start(load_recovery_name);

    m_options.m_curMid = healthy_mid;

    // 1. read from disk
    timer.event_start("1. Load Index");
    // load metadata, and load patch file
    LoadIndex(m_options);
    timer.event_stop("1. Load Index");

    // 2. prepare data
    timer.event_start("2. Prepare Data");
    const auto& p_meta = m_patchMeta[healthy_mid];
    auto vectorset = std::make_shared<RecoveryInMemVectorset>(
        GetEnumValueType<ValueType>(), m_iDataDimension, p_meta.n_headvec,
        p_meta.n_clusvec);
    auto vec_postion = reinterpret_cast<void**>(vectorset->GetData());

    // read patch first
    auto vec_ptr = m_recovery_pbuf_vec.get();
    for (auto vid : m_recovery_mr->patch[healthy_mid]) {
        vec_postion[vid] = vec_ptr;
        vec_ptr += p_meta.perVectorSize;
    }

    // scan index file
    auto ifile = get_index_file_name(healthy_mid);
    SimpleIOHelper ihelper(ifile, SimpleIOHelper::ReadOnly);
    Helper::tracked_buffer ibuf(Helper::make_tracked_buffer(
        (p_meta.n_clusvec - m_recovery_mr->patch[healthy_mid].size()) *
        p_meta.perVectorSize));
    Helper::tracked_buffer load_buffer(Helper::make_tracked_buffer(
        m_options.m_postingPageLimit << PageSizeEx));

    auto ptr = ibuf.get();
    for (auto cid : m_postingOrderInIndex) {
        const auto& listinfo = m_listInfoMap.at(cid);
        auto read_offset = listinfo.listOffset;
        size_t totalBytes =
            (static_cast<size_t>(listinfo.listPageCount) << PageSizeEx);

        ihelper.f_read_data(load_buffer.get(), totalBytes, read_offset);
        uint64_t offset_vid;
        uint64_t offset_vec;
        for (int i = 0; i < listinfo.listEleCount; i++) {
            (this->*m_parsePosting)(offset_vid, offset_vec, i,
                                    listinfo.listEleCount);

            auto start_ptr = load_buffer.get() + listinfo.pageOffset;
            auto vid = *reinterpret_cast<int*>(start_ptr + offset_vid);
            if (vec_postion[vid] == nullptr) {
                memcpy(ptr, start_ptr + offset_vec, p_meta.perVectorSize);
                vec_postion[vid] = ptr;
                ptr += p_meta.perVectorSize;
            }
        }
    }
    timer.event_stop("2. Prepare Data");

    // 3. output
    timer.event_start("3. Output Recovery File");
    m_options.m_curMid = fail_mid;
    std::shared_ptr<VectorSetReader> reader(new RecoveryReader(vectorset));
    SaveMergerResultFiles(*m_recovery_mr, reader);
    timer.event_stop("3. Output Recovery File");

    timer.event_stop(load_recovery_name);
    timer.log_all_event_duration();
    Helper::MemoryTracker::dumpCurrentStats();
    Helper::MemoryTracker::dumpPeakStats();
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::LAR_PM_PLUS(
    SPMerge::MachineID healthy_mid, SPMerge::MachineID fail_mid) {
    EventTimer timer;
    std::string load_recovery_name = "load from " +
                                     std::to_string(healthy_mid) +
                                     " to recovery " + std::to_string(fail_mid);
    m_options.m_curMid = healthy_mid;

    timer.event_start("0. Read files into memory");
    auto healthy_ifile = get_index_file_name(healthy_mid);
    auto healthy_pfile = get_patch_file_name(healthy_mid);
    m_pmData.healthy_isize = std::filesystem::file_size(healthy_ifile);
    m_pmData.healthy_psize = std::filesystem::file_size(healthy_pfile);
    m_pmData.healthy_ibuf.reset(new char[m_pmData.healthy_isize]);
    m_pmData.healthy_pbuf.reset(new char[m_pmData.healthy_psize]);
    SimpleIOHelper ihelper(healthy_ifile, SimpleIOHelper::ReadOnly);
    SimpleIOHelper phelper(healthy_pfile, SimpleIOHelper::ReadOnly);
    ihelper.f_read_data(m_pmData.healthy_ibuf.get(), m_pmData.healthy_isize);
    phelper.f_read_data(m_pmData.healthy_pbuf.get(), m_pmData.healthy_psize);
    timer.event_stop("0. Read files into memory");

    timer.event_start(load_recovery_name);

    // load metadata, and load patch file
    timer.event_start("1. Load index from memory");
    LoadIndex(m_options);
    timer.event_stop("1. Load index from memory");

    // read whole index file into memory

    // 2. recovery
    timer.event_start("2. Recovery in memory");
    const auto& p_meta = m_patchMeta[healthy_mid];
    const auto& mr = *m_recovery_mr;
    const auto& cluster_content = mr.cluster_content;
    const size_t vectorInfoSize = p_meta.perVectorSize + sizeof(SPANNVID);
    const auto totalVec = p_meta.n_clusvec + p_meta.n_headvec;
    const auto numThreads = m_options.m_recoveryNumThreads;
    const auto numPatchVecOnFail = mr.patch[fail_mid].size();
    const auto numPostingOnFail = mr.cluster_assignment[fail_mid].size();

    // calc posting order
    timer.event_start("2.1 Compute posting order");
    std::unordered_map<SPMerge::ClusterID, PostingOutputInfo> postingInfo;
    for (auto cid : mr.cluster_assignment[fail_mid]) {
        const auto& content = cluster_content.at(cid);
        PostingOutputInfo info;
        info.size = content.size();
        info.bytes = content.size() * vectorInfoSize;
        postingInfo.emplace(cid, info);
    }
    std::vector<SPANNCID> postingOrderInIndex;
    uint64_t final_page = GetPostingOrder(mr.cluster_assignment[fail_mid],
                                          postingInfo, postingOrderInIndex);
    timer.event_stop("2.1 Compute posting order");

    // allocating buffer
    const auto fail_ibuf_meta_bytes = GetIndexMetaSize(numPostingOnFail);
    const auto fail_ibuf_data_bytes = (final_page + 1) << PageSizeEx;
    const auto fail_pbuf_meta_bytes =
        GetPatchMetaSize(fail_mid, mr, numPostingOnFail);
    const auto fail_pbuf_data_bytes = numPatchVecOnFail * p_meta.perVectorSize;
    const auto fail_ibuf_total_bytes =
        fail_ibuf_meta_bytes + fail_ibuf_data_bytes;
    const auto fail_pbuf_total_bytes =
        fail_pbuf_meta_bytes + PAGE_ROUND_UP(fail_pbuf_data_bytes);

    auto fail_ibuf_io = f_new_chunked_buffer_io();
    auto fail_pbuf_io = f_new_chunked_buffer_io();
    std::promise<void> data_alloc_done;
    std::thread t_alloc_then_write_meta([&]() {
        timer.event_start("2.2 (overlapped) Data alloc");
        fail_ibuf_io->Initialize(fail_ibuf_total_bytes);
        fail_pbuf_io->Initialize(fail_pbuf_total_bytes);
        timer.event_stop("2.2 (overlapped) Data alloc");

        data_alloc_done.set_value();

        PatchMetadata patch_meta;
        SimpleIOHelper sio_i, sio_p;
        sio_i.init_with_ext_io(fail_ibuf_io, SimpleIOHelper::ReadWrite);
        sio_p.init_with_ext_io(fail_pbuf_io, SimpleIOHelper::ReadWrite);
        timer.event_start("2.3 (overlapped) Recovery index metadata");
        OutputMachineIndexMeta(postingInfo, postingOrderInIndex, totalVec,
                               m_iDataDimension, sio_i, true);
        timer.event_stop("2.3 (overlapped) Recovery index metadata");
        timer.event_start("2.3 (overlapped) Recovery patch metadata");
        OutputMachinePatchMeta(fail_mid, mr, postingOrderInIndex,
                               p_meta.perVectorSize, patch_meta, sio_p, true);
        timer.event_stop("2.3 (overlapped) Recovery patch metadata");
    });

    // analyze vector destination
    timer.event_start("2.2 Compute vector destination");
    struct VecDest {
        std::vector<uint64_t> offset_list;
        bool is_patch{false};
    };
    std::vector<VecDest> vector_dest(totalVec);
    {
        std::vector<std::mutex> lock_arr(totalVec);

#pragma omp parallel for
        for (auto cid : mr.cluster_assignment[fail_mid]) {
            uint16_t i = 0;
            const auto& listinfo = postingInfo.at(cid);
            for (auto vid : cluster_content.at(cid)) {
                uint64_t offset_vid;
                uint64_t offset_vec;
                (this->*m_parsePosting)(offset_vid, offset_vec, i,
                                        listinfo.size);
                if (offset_vid + sizeof(SPANNVID) != offset_vec) {
                    LOG_ERROR("offset mismatch, vid=%llu, vec=%llu\n",
                              offset_vid, offset_vec);
                    throw std::runtime_error("");
                }
                auto offset =
                    fail_ibuf_meta_bytes +
                    (static_cast<uint64_t>(listinfo.page_num) << PageSizeEx) +
                    listinfo.page_off + offset_vid;
                {
                    std::lock_guard guard(lock_arr[vid]);
                    vector_dest[vid].offset_list.push_back(offset);
                }
                i++;
            }
        }

#pragma omp parallel for
        for (size_t idx = 0; idx < mr.patch[fail_mid].size(); idx++) {
            auto vid = mr.patch[fail_mid][idx];
            auto offset = fail_pbuf_meta_bytes + idx * p_meta.perVectorSize;
            // no need to lock
            vector_dest[vid].is_patch = true;
            vector_dest[vid].offset_list.push_back(offset);
        }
    }
    timer.event_stop("2.2 Compute vector destination");

    data_alloc_done.get_future().wait();

    // write vector data into memory bufferr
    timer.event_start("2.3 Write data in memory");
    std::vector<std::atomic_bool> visited(totalVec);
    std::atomic_uint64_t visited_cnt = 0;
    auto f_handle_vector = [&](SPANNVID vid, char* src_ptr) {
        if (visited[vid].exchange(true)) {
            return;  // vector touched before
        }
        const auto& dest = vector_dest[vid];
        if (dest.is_patch) {
            assert(dest.offset_list.size() == 1);
            fail_pbuf_io->WriteBinaryDirect(src_ptr, p_meta.perVectorSize,
                                            dest.offset_list[0]);
        } else {
            for (auto offset : dest.offset_list) {
                auto vid_ptr = reinterpret_cast<const char*>(&vid);
                fail_ibuf_io->WriteBinaryDirect(vid_ptr, sizeof(SPANNVID),
                                                offset);
                fail_ibuf_io->WriteBinaryDirect(src_ptr, p_meta.perVectorSize,
                                                offset + sizeof(SPANNVID));
            }
        }
        visited_cnt++;
    };
#pragma omp parallel for
    for (auto cid : m_postingOrderInIndex) {
        const auto& listinfo = m_listInfoMap.at(cid);
        char* posting_ptr = m_pmData.healthy_ibuf.get() + listinfo.listOffset +
                            listinfo.pageOffset;
        // parse vectors in buffer
        uint64_t offset_vid;
        uint64_t offset_vec;
        for (int i = 0; i < listinfo.listEleCount; i++) {
            (this->*m_parsePosting)(offset_vid, offset_vec, i,
                                    listinfo.listEleCount);
            auto vid = *reinterpret_cast<SPANNVID*>(posting_ptr + offset_vid);
            auto src_ptr = posting_ptr + offset_vec;
            f_handle_vector(vid, src_ptr);
        }
    }
#pragma omp parallel for
    for (size_t i = 0; i < mr.patch[healthy_mid].size(); i++) {
        auto vid = mr.patch[healthy_mid][i];
        auto src_ptr = m_recovery_pbuf_vec.get() + i * p_meta.perVectorSize;
        f_handle_vector(vid, src_ptr);
    }

    if (visited_cnt.load() != p_meta.n_clusvec) {
        LOG_ERROR("Not all vectors are visited: %llu/%llu\n",
                  visited_cnt.load(), p_meta.n_clusvec);
        throw std::runtime_error("");
    }
    timer.event_stop("2.3 Write data in memory");

    t_alloc_then_write_meta.join();

    timer.event_stop("2. Recovery in memory");

    // 3. output
    timer.event_start("3. Output Recovery Files");
    auto fail_index_file = get_index_file_name(fail_mid, true);
    auto fail_patch_file = get_patch_file_name(fail_mid, true);
    SimpleIOHelper fail_index(f_new_native_io(), fail_index_file,
                              SimpleIOHelper::WriteOnly, true);
    SimpleIOHelper fail_patch(f_new_native_io(), fail_patch_file,
                              SimpleIOHelper::WriteOnly, true);
    auto ibuf_sum = fail_ibuf_io->report();
    auto pbuf_sum = fail_pbuf_io->report();
    for (auto& info : ibuf_sum) fail_index.f_write_data(info.ptr, info.left);
    for (auto& info : pbuf_sum) fail_patch.f_write_data(info.ptr, info.left);
    fsync(fail_index.m_ptr->GetFD());
    fsync(fail_patch.m_ptr->GetFD());
    fail_ibuf_io->ShutDown();
    fail_pbuf_io->ShutDown();
    fail_index.close();
    fail_patch.close();
    timer.event_stop("3. Output Recovery Files");

    timer.event_stop(load_recovery_name);
    timer.log_all_event_duration();
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::LAR_PM(SPMerge::MachineID healthy_mid,
                                               SPMerge::MachineID fail_mid) {
    EventTimer timer;
    std::string load_recovery_name = "load from " +
                                     std::to_string(healthy_mid) +
                                     " to recovery " + std::to_string(fail_mid);
    timer.event_start(load_recovery_name);

    m_options.m_curMid = healthy_mid;

    // 1. read from disk
    timer.event_start("1. Disk Read");
    // load metadata, and load patch file
    LoadIndex(m_options);
    // read whole index file into memory
    auto ifile = get_index_file_name(healthy_mid);
    auto isize = std::filesystem::file_size(ifile);
    auto ibuf = make_tracked_buffer(isize);
    SimpleIOHelper ihelper(ifile, SimpleIOHelper::ReadOnly);
    ihelper.f_read_data(ibuf.get(), isize);
    timer.event_stop("1. Disk Read");

    // 2. prepare data
    timer.event_start("2. Prepare Data");
    const auto& p_meta = m_patchMeta[healthy_mid];
    auto vectorset = std::make_shared<RecoveryInMemVectorset>(
        GetEnumValueType<ValueType>(), m_iDataDimension, p_meta.n_headvec,
        p_meta.n_clusvec);
    std::shared_ptr<VectorSetReader> reader(new RecoveryReader(vectorset));
    // fill content in vector set
    auto vec_postion = reinterpret_cast<void**>(vectorset->GetData());
    // read patch first
    auto vec_ptr = m_recovery_pbuf_vec.get();
    for (auto vid : m_recovery_mr->patch[healthy_mid]) {
        vec_postion[vid] = vec_ptr;
        vec_ptr += p_meta.perVectorSize;
    }
    // iterate all clusters
    for (const auto& [_, listinfo] : m_listInfoMap) {
        auto start_ptr = ibuf.get() + listinfo.listOffset + listinfo.pageOffset;
        uint64_t offset_vid;
        uint64_t offset_vec;
        for (int i = 0; i < listinfo.listEleCount; i++) {
            (this->*m_parsePosting)(offset_vid, offset_vec, i,
                                    listinfo.listEleCount);
            auto vid = *reinterpret_cast<int*>(start_ptr + offset_vid);
            vec_postion[vid] = start_ptr + offset_vec;
        }
    }
    timer.event_stop("2. Prepare Data");

    // 3. output
    timer.event_start("3. Output Recovery File");
    m_options.m_curMid = fail_mid;
    SaveMergerResultFiles(*m_recovery_mr, reader);
    timer.event_stop("3. Output Recovery File");

    timer.event_stop(load_recovery_name);
    timer.log_all_event_duration();
    Helper::MemoryTracker::dumpCurrentStats();
    Helper::MemoryTracker::dumpPeakStats();
}

struct ClusterStorageSize {
    SPMerge::ClusterID cid;
    uint16_t rest;
};

struct CssCmp {
    bool operator()(const ClusterStorageSize& a,
                    const ClusterStorageSize& b) const {
        return a.rest == b.rest ? a.cid < b.cid : a.rest > b.rest;
    }
};

template <typename T>
uint64_t ExtraFullGraphSearcher<T>::GetPostingOrder(
    const SPMerge::ClusterIDList& cid_list,
    std::unordered_map<SPMerge::ClusterID, PostingOutputInfo>& p_postingInfo,
    std::vector<SPANNCID>& p_postingOrderInIndex) const {
    if (cid_list.size() != p_postingInfo.size()) {
        LOG_ERROR("not consistent cluster size: %lu vs %lu\n", cid_list.size(),
                  p_postingInfo.size());
        throw std::invalid_argument("");
    }

    p_postingOrderInIndex.clear();
    p_postingOrderInIndex.reserve(cid_list.size());

    ClusterStorageSize listInfo;
    std::set<ClusterStorageSize, CssCmp> listRestSize;
    for (auto cid : cid_list) {
        auto cluster_bytes = p_postingInfo.at(cid).bytes;
        auto cluster_pages = static_cast<uint16_t>(cluster_bytes % PageSize);
        if (cluster_bytes == 0) {
            LOG_ERROR("unexpected zero sized cluster (cid=%u)!\n", cid);
            throw std::runtime_error("");
        }
        listInfo.cid = cid;
        listInfo.rest = cluster_pages;
        listRestSize.insert(listInfo);
    }

    listInfo.cid = -1;

    int currPageNum = 0;
    uint16_t curPadding = 0;
    uint16_t currOffset = 0;
    while (!listRestSize.empty()) {
        listInfo.rest = PageSize - currOffset;
        auto iter = listRestSize.lower_bound(listInfo);  // avoid page-crossing
        if (iter == listRestSize.end() ||
            (listInfo.rest != PageSize && iter->rest == 0)) {
            curPadding = PageSize - currOffset;
            ++currPageNum;
            currOffset = 0;
        } else {
            auto& info = p_postingInfo.at(iter->cid);
            p_postingOrderInIndex.push_back(iter->cid);
            info.paddings = curPadding;
            info.page_num = currPageNum;
            info.page_off = currOffset;

            curPadding = 0;
            currOffset += iter->rest;
            if (currOffset > PageSize) {
                LOG_ERROR("Crossing extra pages\n");
                throw std::runtime_error("Read too many pages");
            }
            if (currOffset == PageSize) {
                ++currPageNum;
                currOffset = 0;
            }
            currPageNum += static_cast<int>(info.bytes / PageSize);
            listRestSize.erase(iter);
        }
    }

    auto index_size =
        static_cast<uint64_t>(currPageNum) * PageSize + currOffset;
    LOG_INFO("TotalPageNumbers: %d, IndexSize: %s\n", currPageNum,
             bytesToString(index_size).c_str());
    return currPageNum;
}

template <typename T>
std::string ExtraFullGraphSearcher<T>::get_file_name_from_fmt(
    const std::string& fmt, uint mid, bool recovery) const {
    char buffer[64];
    sprintf(buffer, fmt.c_str(), mid);
    auto output_dir = std::filesystem::path(recovery ? m_options.m_recoveryPath
                                                     : m_options.m_savePath);
    auto path = output_dir / std::string(buffer);
    return path.string();
}

template <typename T>
void ExtraFullGraphSearcher<T>::ClusterAssignmentToOrder(
    SPMerge::MergerResult& mr,
    std::shared_ptr<Helper::VectorSetReader>& p_reader,
    std::shared_ptr<VectorIndex> p_headIndex) const {
    if (mr.cluster_assignment.size() != m_numNodes or
        mr.patch.size() != m_numNodes) {
        throw std::invalid_argument("param size not correct!");
    }
    if (m_enableDeltaEncoding and p_headIndex == nullptr) {
        throw std::invalid_argument(
            "delta encoding enabled, but p_headIndex==nullptr");
    }

    auto fullVectors = p_reader->GetVectorSet();
    size_t vectorInfoSize = fullVectors->PerVectorDataSize() + sizeof(SPANNVID);

    // compute data ahead
#pragma omp parallel for num_threads(m_numNodes)
    for (SPMerge::MachineID mid = 0; mid < m_numNodes; mid++) {
        std::unordered_map<SPMerge::ClusterID, PostingOutputInfo> postingInfo;
        for (auto cid : mr.cluster_assignment[mid]) {
            auto cluster_size = mr.cluster_content.at(cid).size();
            PostingOutputInfo info;
            info.size = cluster_size;
            info.bytes = cluster_size * vectorInfoSize;
            postingInfo.emplace(cid, info);
        }

        std::vector<SPANNCID> postingOrderInIndex;
        GetPostingOrder(mr.cluster_assignment[mid], postingInfo,
                        postingOrderInIndex);
        // NOTE: now each cluster_assignment is in the order of
        // postingOrderInIndex
        SPMerge::ClusterIDList ordered_cid_list(postingOrderInIndex.begin(),
                                                postingOrderInIndex.end());
        mr.cluster_assignment[mid].swap(ordered_cid_list);
    }
}

template <typename T>
void ExtraFullGraphSearcher<T>::SaveMergerResultFiles(
    const SPMerge::MergerResult& mr,
    std::shared_ptr<Helper::VectorSetReader>& p_reader,
    std::shared_ptr<VectorIndex> p_headIndex) const {
    if (mr.cluster_assignment.size() != m_numNodes or
        mr.patch.size() != m_numNodes) {
        throw std::invalid_argument("param size not correct!");
    }
    if (m_enableDeltaEncoding and p_headIndex == nullptr) {
        throw std::invalid_argument(
            "delta encoding enabled, but p_headIndex==nullptr");
    }

    auto fullVectors = p_reader->GetVectorSet();
    size_t vectorInfoSize = fullVectors->PerVectorDataSize() + sizeof(SPANNVID);

    // ECMeta
    SimpleIOHelper listinfo_soh(
        (std::filesystem::path(m_options.m_savePath) / "m_ListInfoMap.bin")
            .string(),
        SimpleIOHelper::WriteOnly);
    size_t total_clusters = 0;
    for (SPMerge::MachineID mid = 0; mid < m_numNodes; mid++) {
        total_clusters += mr.cluster_assignment[mid].size();
    }
    size_t global_header_size = sizeof(size_t) + sizeof(size_t) +
                                sizeof(SPMerge::MachineID) +
                                sizeof(int) * m_numNodes;
    // 每个记录的大小: page_begin + cid + page_num + page_off + size +
    // listPageCount
    const size_t record_size = sizeof(int) + sizeof(SPANNCID) + sizeof(int) +
                               sizeof(uint16_t) + sizeof(int) +
                               sizeof(uint16_t);
    const size_t total_records_size = total_clusters * record_size;
    size_t content_start_offset = global_header_size + total_records_size;
    // 写入全局头部
    listinfo_soh.f_write_data(&total_clusters, sizeof(total_clusters));
    listinfo_soh.f_write_data(&content_start_offset,
                              sizeof(content_start_offset));
    listinfo_soh.f_write_data(&m_numNodes, sizeof(m_numNodes));

    std::vector<std::unordered_map<SPMerge::ClusterID, PostingOutputInfo>>
        postingInfo_List;
    std::vector<std::vector<SPANNCID>> postingOrderInIndex_List;

    for (SPMerge::MachineID mid = 0; mid < m_numNodes; mid++) {
        if (m_options.m_curMid != SPMerge::InvalidMID and
            mid != m_options.m_curMid) {
            continue;
        }
        size_t n_vecs = 0;
        // size_t n_clusters = mr.cluster_assignment[mid].size();
        std::unordered_map<SPMerge::ClusterID, PostingOutputInfo> postingInfo;
        for (auto cid : mr.cluster_assignment[mid]) {
            auto cluster_size = mr.cluster_content.at(cid).size();
            PostingOutputInfo info;
            info.size = cluster_size;
            info.bytes = cluster_size * vectorInfoSize;
            postingInfo.emplace(cid, info);
            n_vecs += cluster_size;
        }

        Selection selections{n_vecs, "/tmp"};
        {
            size_t idx = 0;
            // NOTE: now cluster_assignment is ordered by storage order, but we
            // need numeric order in selections
            auto ass_cpy = mr.cluster_assignment[mid];
            std::sort(ass_cpy.begin(), ass_cpy.end());
            for (auto cid : ass_cpy) {
                for (auto vid : mr.cluster_content.at(cid)) {
                    selections.m_selections[idx].node = cid;
                    selections.m_selections[idx].tonode = vid;
                    idx++;
                }
            }

            if (idx != n_vecs) {
                LOG_ERROR(
                    "how many vectors on machine %u? nvecs=%llu, "
                    "itered:%llu\n",
                    mid, n_vecs, idx);
            }
        }

        LOG_INFO("\n");

        std::vector<SPANNCID> postingOrderInIndex;
        GetPostingOrder(mr.cluster_assignment[mid], postingInfo,
                        postingOrderInIndex);
        {  // ECMeta
            auto n_clusters = static_cast<SPANNCID>(postingInfo.size());
            uint64_t listOffset = GetIndexMetaSize(n_clusters);
            int page_begin = static_cast<int>(listOffset / PageSize);
            listinfo_soh.f_write_data(&page_begin, sizeof(page_begin));
            postingInfo_List.push_back(postingInfo);
            postingOrderInIndex_List.push_back(postingOrderInIndex);
        }
        OutputMachineIndexFile(get_index_file_name(mid, m_recoveryMode),
                               vectorInfoSize, postingInfo, postingOrderInIndex,
                               p_headIndex, fullVectors, selections);
        OutputMachinePatchFile(mid, mr, postingOrderInIndex, fullVectors);
    }

    for (SPMerge::MachineID mid = 0; mid < m_numNodes; mid++) {
        OutputMachineIndexListInfo(postingInfo_List[mid],
                                   postingOrderInIndex_List[mid], listinfo_soh);
    }
    if (listinfo_soh.m_stat.written_bytes != content_start_offset) {
        LOG_ERROR(
            "Final size mismatch: written_bytes=%llu, "
            "expected_offset=%llu\n",
            listinfo_soh.m_stat.written_bytes, content_start_offset);
        throw std::runtime_error("Final file size mismatch.");
    }

    LOG_INFO(
        "Successfully wrote combined index. Metadata "
        "Size: %s\n",
        bytesToString(content_start_offset).c_str());
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::OutputMachineIndexListInfo(
    const std::unordered_map<SPMerge::ClusterID, PostingOutputInfo>&
        p_postingInfo,
    const std::vector<SPANNCID>& p_postingOrderInIndex, SimpleIOHelper& soh,
    bool multithread) const {
    {
        auto n_clusters = static_cast<SPANNCID>(p_postingInfo.size());
        uint64_t listOffset = GetIndexMetaSize(n_clusters);
        int page_begin = static_cast<int>(listOffset / PageSize);
        LOG_INFO("page_begin=%d\n", page_begin);
        for (auto cid : p_postingOrderInIndex) {
            const auto& info = p_postingInfo.at(cid);
            if (info.size == 0) {
                LOG_ERROR("unexpected zero sized cluster (cid=%u)\n", cid);
                throw std::runtime_error("Zero sized cluster");
            }

            // write cid
            soh.f_write_data(&page_begin, sizeof(page_begin));
            soh.f_write_data(&cid, sizeof(cid));
            // write info
            int page_num_begin = info.page_num + page_begin;
            soh.f_write_data(&page_num_begin, sizeof(page_num_begin));
            soh.f_write_data(&info.page_off, sizeof(info.page_off));
            soh.f_write_data(&info.size, sizeof(info.size));
            uint16_t listPageCount = PAGE_COUNT(info.bytes);
            soh.f_write_data(&listPageCount, sizeof(listPageCount));
        }
    }
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::OutputMachineIndexMeta(
    const std::unordered_map<SPMerge::ClusterID, PostingOutputInfo>&
        p_postingInfo,
    const std::vector<SPANNCID>& p_postingOrderInIndex,
    const SPANNVID n_vectors, const int vector_dim, SimpleIOHelper& soh,
    bool multithread) const {
    // meta size of global info
    auto n_clusters = static_cast<SPANNCID>(p_postingInfo.size());
    uint64_t listOffset = GetIndexMetaSize(n_clusters);

    auto chunked_fio =
        std::dynamic_pointer_cast<Helper::ChunkedBufferIO>(soh.m_ptr);
    multithread = multithread and chunked_fio != nullptr;

    // Number of posting lists
    soh.f_write_data(&n_clusters, sizeof(n_clusters));

    // Number of vectors
    soh.f_write_data(&n_vectors, sizeof(n_vectors));

    // Vector dimension
    soh.f_write_data(&vector_dim, sizeof(vector_dim));

    // Page offset of list content section
    auto i32Val = static_cast<int>(listOffset / PageSize);
    soh.f_write_data(&i32Val, sizeof(i32Val));

    // Meta of each posting list
    if (multithread) {
        LOG_INFO(
            "using multi thread to recovery index metadata (meta of each "
            "posting list)\n");
        auto perPostingSize =
            (sizeof(int) + sizeof(uint16_t) + sizeof(int) + sizeof(uint16_t));
        auto start_pos = chunked_fio->TellP();
#pragma omp parallel for
        for (size_t idx = 0; idx < p_postingOrderInIndex.size(); idx++) {
            auto cid = p_postingOrderInIndex[idx];
            const auto& info = p_postingInfo.at(cid);
            if (info.size == 0) {
                LOG_ERROR("unexpected zero sized cluster (cid=%u)\n", cid);
                throw std::runtime_error("");
            }

            auto offset = start_pos + idx * perPostingSize;

            chunked_fio->WriteBinaryDirect(
                reinterpret_cast<const char*>(&info.page_num),
                sizeof(info.page_num), offset);
            offset += sizeof(info.page_num);

            chunked_fio->WriteBinaryDirect(
                reinterpret_cast<const char*>(&info.page_off),
                sizeof(info.page_off), offset);
            offset += sizeof(info.page_off);

            chunked_fio->WriteBinaryDirect(
                reinterpret_cast<const char*>(&info.size), sizeof(info.size),
                offset);
            offset += sizeof(info.size);

            uint16_t listPageCount = PAGE_COUNT(info.bytes);
            chunked_fio->WriteBinaryDirect(
                reinterpret_cast<const char*>(&listPageCount),
                sizeof(listPageCount), offset);
        }
        auto cur_off =
            start_pos + perPostingSize * p_postingOrderInIndex.size();
        soh.f_seek_pos(cur_off);
        soh.m_stat.written_bytes = cur_off;
    } else {
        for (auto cid : p_postingOrderInIndex) {
            const auto& info = p_postingInfo.at(cid);
            if (info.size == 0) {
                LOG_ERROR("unexpected zero sized cluster (cid=%u)\n", cid);
                throw std::runtime_error("");
            }

            // Page number of the posting list
            soh.f_write_data(&info.page_num, sizeof(info.page_num));

            // Page offset
            soh.f_write_data(&info.page_off, sizeof(info.page_off));

            // Number of vectors in the posting list
            soh.f_write_data(&info.size, sizeof(info.size));

            // Page count of the posting list
            uint16_t listPageCount = PAGE_COUNT(info.bytes);
            soh.f_write_data(&listPageCount, sizeof(listPageCount));
        }
    }

    // Write padding vals
    soh.f_write_padding();
    if (soh.m_stat.written_bytes != listOffset) {
        LOG_ERROR("mismatch: written_bytes=%llu, listOffset=%llu\n",
                  soh.m_stat.written_bytes, listOffset);
        throw std::runtime_error("");
    }

    LOG_INFO("SubIndex Size: %s\n", bytesToString(listOffset).c_str());
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::OutputMachineIndexFile(
    const std::string& p_outputFile, size_t p_spacePerVector,
    const std::unordered_map<SPMerge::ClusterID, PostingOutputInfo>&
        p_postingInfo,
    const std::vector<SPANNCID>& p_postingOrderInIndex,
    std::shared_ptr<VectorIndex> p_headIndex,
    std::shared_ptr<VectorSet> p_fullVectors,
    Selection& p_postingSelections) const {
    EventTimer timer;
    timer.event_start("write machine index file");

    SimpleIOHelper soh(p_outputFile, SimpleIOHelper::WriteOnly);

    OutputMachineIndexMeta(p_postingInfo, p_postingOrderInIndex,
                           p_fullVectors->Count(), p_fullVectors->Dimension(),
                           soh);

    size_t list_start = soh.m_stat.written_bytes;
    uint64_t listOffset = 0;

    // iterate over all the posting lists
    for (auto cid : p_postingOrderInIndex) {
        const auto& info = p_postingInfo.at(cid);
        uint64_t targetOffset =
            static_cast<uint64_t>(info.page_num) * PageSize + info.page_off;
        if (targetOffset < listOffset) {
            throw std::runtime_error(
                "List offset not match, targetOffset < listOffset!");
        }
        // write padding vals before the posting list
        if (targetOffset > listOffset) {
            if (targetOffset - listOffset != info.paddings) {
                LOG_ERROR(
                    "info.paddings=%u, but is targetOffset-listOffset=%u\n",
                    info.paddings, targetOffset - listOffset);
                throw std::runtime_error("");
            }
            soh.f_write_padding(list_start + targetOffset);
            listOffset = targetOffset;
        }

        // get posting list full content and write it at once
        ValueType* headVector = nullptr;
        if (m_enableDeltaEncoding) {
            headVector = (ValueType*)p_headIndex->GetSample(cid);
        }
        std::string postingListFullData = GetPostingListFullData(
            cid, info.size, p_postingSelections, p_fullVectors,
            m_enableDeltaEncoding, m_enablePostingListRearrange, headVector);
        size_t postingListFullSize =
            static_cast<size_t>(info.size) * p_spacePerVector;
        if (postingListFullSize != postingListFullData.size()) {
            LOG_ERROR(
                "posting list full data size NOT MATCH! "
                "postingListFullData.size(): %zu "
                "postingListFullSize: %zu \n",
                postingListFullData.size(), postingListFullSize);
            throw std::runtime_error("Posting list full size mismatch");
        }
        soh.f_write_data(postingListFullData.data(), postingListFullSize);
        listOffset += postingListFullSize;
    }

    soh.f_write_padding();

    timer.event_stop("write machine index file");
    timer.log_all_event_duration_and_clear();
    LOG_INFO("Machine file saved to %s\n",
             std::filesystem::absolute(p_outputFile).c_str());
    LOG_INFO("Padded Size: %s, final total size: %s.\n",
             bytesToString(soh.m_stat.padding_total_bytes).c_str(),
             bytesToString(soh.m_stat.written_bytes).c_str());
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::OutputMachinePatchMeta(
    const SPMerge::MachineID cur_mid, const SPMerge::MergerResult& mr,
    const std::vector<SPANNCID>& p_postingOrderInIndex,
    const size_t per_vec_size, PatchMetadata& p_meta, SimpleIOHelper& soh,
    bool multithread, std::vector<uint64_t>* cc_off) const {
    const auto is_primary = is_primary_node(cur_mid);
    const auto& my_patch = mr.patch[cur_mid];

    auto chunked_fio =
        std::dynamic_pointer_cast<Helper::ChunkedBufferIO>(soh.m_ptr);
    multithread = multithread and chunked_fio != nullptr;

    size_t order_storage_bytes =
        p_postingOrderInIndex.size() * sizeof(SPANNCID);
    size_t cluster_assignment_bytes = 0;

    struct ContentOutInfo {
        SPMerge::ClusterID cid;
        uint64_t sub_offset;  // offset in content area
        const SPMerge::VectorIDList* vid_list;
        ContentOutInfo(SPMerge::ClusterID ci, uint64_t off,
                       const SPMerge::VectorIDList* ptr)
            : cid(ci), sub_offset(off), vid_list(ptr) {}
    };
    std::vector<ContentOutInfo> content_info;
    content_info.reserve(mr.cluster_content.size());
    std::vector<uint64_t> cluster_content_offset_each_machine(m_numNodes, 0);
    size_t cluster_content_bytes = m_numNodes * sizeof(uint64_t);
    size_t vid_list_bytes = my_patch.size() * sizeof(SPMerge::VectorID);

    if (is_primary) {
        // calc cluster assignment bytes
        for (SPMerge::MachineID mid = 0; mid < m_numNodes; mid++) {
            if (mid == cur_mid) continue;
            auto& cid_list = mr.cluster_assignment[mid];
            cluster_assignment_bytes +=
                sizeof(PatchMetadata::MetaClusterAssignment);
            cluster_assignment_bytes +=
                cid_list.size() * sizeof(SPMerge::ClusterID);
        }
        // calc cluster content bytes
        if (cc_off == nullptr) {
            for (SPMerge::MachineID mid = 0; mid < m_numNodes; mid++) {
                cluster_content_offset_each_machine[mid] =
                    cluster_content_bytes;
                // TODO: parallel for
                for (auto cid : mr.cluster_assignment[mid]) {
                    const auto& vid_list = mr.cluster_content.at(cid);
                    content_info.emplace_back(cid, cluster_content_bytes,
                                              &vid_list);
                    cluster_content_bytes +=
                        sizeof(PatchMetadata::MetaClusterContent);
                    cluster_content_bytes +=
                        vid_list.size() * sizeof(SPMerge::VectorID);
                }
            }
        } else {
            // in special case, we handle output for cluster content outside
            if (cc_off->size() != m_numNodes + 1) throw std::logic_error("");
            memcpy(cluster_content_offset_each_machine.data(), cc_off->data(),
                   sizeof(uint64_t) * m_numNodes);
            cluster_content_bytes = cc_off->at(m_numNodes);
        }
        // calc patch vid list bytes
        for (SPMerge::MachineID mid = 0; mid < m_numNodes; mid++) {
            if (mid == cur_mid) continue;
            auto& vid_list = mr.patch[mid];
            vid_list_bytes += sizeof(PatchMetadata::MetaPatch);
            vid_list_bytes += vid_list.size() * sizeof(SPMerge::VectorID);
        }
    }

    memset(&p_meta, 0, sizeof(p_meta));
    p_meta.n_clusters = p_postingOrderInIndex.size();
    p_meta.n_vectors = my_patch.size();
    p_meta.n_headvec = mr.head_count;
    p_meta.n_clusvec = mr.cluster_vector_count;
    p_meta.n_machine = m_numNodes;
    p_meta.n_totalClusters = mr.cluster_content.size();
    p_meta.perVectorSize = per_vec_size;
    p_meta.postingOrderStart = PAGE_ROUND_UP(sizeof(PatchMetadata));

    auto postingOrderEnd =
        p_meta.postingOrderStart + PAGE_ROUND_UP(order_storage_bytes);
    if (is_primary) {
        p_meta.clusterAssignmentStart = postingOrderEnd;
        p_meta.clusterContentStart = p_meta.clusterAssignmentStart +
                                     PAGE_ROUND_UP(cluster_assignment_bytes);
        p_meta.vectorIDStart =
            p_meta.clusterContentStart + PAGE_ROUND_UP(cluster_content_bytes);
        p_meta.vectorContentStart =
            p_meta.vectorIDStart + PAGE_ROUND_UP(vid_list_bytes);
    }

    // Metadata
    soh.f_write_data(&p_meta, sizeof(p_meta));
    soh.f_write_padding(p_meta.postingOrderStart);

    // cluster order
    soh.f_write_data(p_postingOrderInIndex.data(), order_storage_bytes);
    soh.f_write_padding(postingOrderEnd);

    if (not is_primary) return;

    // cluster assignment
    for (SPMerge::MachineID mid = 0; mid < m_numNodes; mid++) {
        if (mid == cur_mid) continue;
        auto& cid_list = mr.cluster_assignment[mid];

        PatchMetadata::MetaClusterAssignment meta{
            .mid = mid,
            .n_clusters = cid_list.size(),
        };

        soh.f_write_data(&meta, sizeof(meta));
        soh.f_write_data(cid_list.data(),
                         cid_list.size() * sizeof(SPMerge::ClusterID));
    }
    soh.f_write_padding(p_meta.clusterContentStart);

    // cluster content
    soh.f_write_data(cluster_content_offset_each_machine.data(),
                     m_numNodes * sizeof(uint64_t));
    if (cc_off == nullptr) {  // normal output
        if (multithread) {
            LOG_INFO(
                "using multi thread to recovery patch metadata (cluster "
                "content)\n");
#pragma omp parallel for
            for (const auto& info : content_info) {
                PatchMetadata::MetaClusterContent meta{
                    .cid = info.cid,
                    .n_vecs = info.vid_list->size(),
                };
                uint64_t meta_off =
                    p_meta.clusterContentStart + info.sub_offset;
                chunked_fio->WriteBinaryDirect(
                    reinterpret_cast<const char*>(&meta), sizeof(meta),
                    meta_off);

                uint64_t list_off = meta_off + sizeof(meta);
                chunked_fio->WriteBinaryDirect(
                    reinterpret_cast<const char*>(info.vid_list->data()),
                    info.vid_list->size() * sizeof(SPMerge::VectorID),
                    list_off);
            }
            auto cur_off = p_meta.clusterContentStart + cluster_content_bytes;
            soh.f_seek_pos(cur_off);
            soh.m_stat.written_bytes = cur_off;
        } else {
            for (const auto& info : content_info) {
                PatchMetadata::MetaClusterContent meta{
                    .cid = info.cid,
                    .n_vecs = info.vid_list->size(),
                };
                soh.f_write_data(&meta, sizeof(meta));
                soh.f_write_data(
                    info.vid_list->data(),
                    info.vid_list->size() * sizeof(SPMerge::VectorID));
            }
        }
    } else {  // skip output for cluster content
        LOG_INFO(
            "cc_off!=nullptr, outside function is responsible for output of "
            "cluster content\n");
        LOG_INFO("cluster_content_bytes=%llu\n", cluster_content_bytes);
        auto cur_off = p_meta.clusterContentStart + cluster_content_bytes;
        soh.f_seek_pos(cur_off);
        soh.m_stat.written_bytes = cur_off;
    }
    soh.f_write_padding(p_meta.vectorIDStart);

    // vector ids (cur machine)
    soh.f_write_data(my_patch.data(),
                     my_patch.size() * sizeof(SPMerge::VectorID));
    for (SPMerge::MachineID mid = 0; mid < m_numNodes; mid++) {
        if (mid == cur_mid) continue;
        auto& vid_list = mr.patch[mid];

        PatchMetadata::MetaPatch meta{
            .mid = mid,
            .n_vecs = vid_list.size(),
        };

        soh.f_write_data(&meta, sizeof(meta));
        soh.f_write_data(vid_list.data(),
                         vid_list.size() * sizeof(SPMerge::VectorID));
    }
    soh.f_write_padding(p_meta.vectorContentStart);

    size_t vector_bytes = my_patch.size() * per_vec_size;
    LOG_INFO(
        "Patch Info: posting order=%s, cluster assignment=%s, cluster "
        "content=%s, patch vids=%s, patch vectors=%s\n",
        bytesToString(order_storage_bytes).c_str(),
        bytesToString(cluster_assignment_bytes).c_str(),
        bytesToString(cluster_content_bytes).c_str(),
        bytesToString(vid_list_bytes).c_str(),
        bytesToString(vector_bytes).c_str());
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::OutputMachinePatchFile(
    const SPMerge::MachineID cur_mid, const SPMerge::MergerResult& mr,
    const std::vector<SPANNCID>& p_postingOrderInIndex,
    std::shared_ptr<VectorSet> p_fullVectors) const {
    auto filename = get_patch_file_name(cur_mid, m_recoveryMode);
    const auto& my_patch = mr.patch[cur_mid];

    EventTimer timer;
    timer.event_start("write machine patch file");

    SimpleIOHelper soh(filename, SimpleIOHelper::WriteOnly);

    PatchMetadata p_meta;
    OutputMachinePatchMeta(cur_mid, mr, p_postingOrderInIndex,
                           p_fullVectors->PerVectorDataSize(), p_meta, soh);

    if (is_primary_node(cur_mid)) {
        size_t vector_bytes =
            my_patch.size() * p_fullVectors->PerVectorDataSize();

        // vector content
        std::string vectors;
        for (auto vid : my_patch) {
            auto p_vector = p_fullVectors->GetVector(vid);
            vectors.append(reinterpret_cast<char*>(p_vector),
                           p_fullVectors->PerVectorDataSize());
        }
        soh.f_write_data(vectors.data(), vector_bytes);
        soh.f_write_padding(PAGE_ROUND_UP(soh.m_stat.written_bytes));
    }

    timer.event_stop("write machine patch file");
    timer.log_all_event_duration_and_clear();
    LOG_INFO("patch file saved to %s\n",
             std::filesystem::absolute(filename).c_str());
    LOG_INFO("padded_size: %s, final total size: %s.\n",
             bytesToString(soh.m_stat.padding_total_bytes).c_str(),
             bytesToString(soh.m_stat.written_bytes).c_str());
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::LoadingHeadInfo(Options& p_opt) {
    EventTimer timer;
    timer.event_start("Load index");
    auto f_open = [&p_opt](std::shared_ptr<Helper::DiskIO> handler,
                           std::string filename) {
        if (handler == nullptr ||
            !handler->Initialize(filename.c_str(),
                                 std::ios::binary | std::ios::in,
                                 p_opt.m_searchInternalResultNum, 2, 2,
                                 p_opt.m_iSSDNumberOfThreads)) {
            LOG_ERROR("Cannot open file: %s!\n", filename.c_str());
            throw std::runtime_error("");
        }
    };

    if (m_options.m_loadMergerResult) {
        LOG_INFO("using merger result, curMid is set to %u\n",
                 m_options.m_curMid);

        m_patchFiles.resize(m_numNodes);
        m_indexFiles.resize(m_numNodes);
        m_patchMeta.resize(m_numNodes);
        for (SPMerge::MachineID mid = 0; mid < m_numNodes; mid++) {
            // default is load all machine files, but on recovery, load only
            // the healthy's
            if (m_options.m_curMid != SPMerge::InvalidMID and
                mid != m_options.m_curMid) {
                LOG_INFO("skip loading machine %u files\n", mid);
                continue;
            }

            auto ifilename = get_index_file_name(mid);
            auto pfilename = get_patch_file_name(mid);
            if (not m_recoveryMode) {
                auto ihandler = f_createAsyncIO();
                auto phandler = f_createAsyncIO();
                f_open(ihandler, ifilename);
                f_open(phandler, pfilename);
                m_indexFiles[mid] = ihandler;
                m_patchFiles[mid] = phandler;
            }

            std::vector<SPANNCID> postingOrderInIndex;
            if (m_recoveryMode) {
                timer.event_start("LoadingPatchFullData");
                LoadingPatchFullData(mid, postingOrderInIndex);
                timer.event_stop("LoadingPatchFullData");
            } else {
                timer.event_start("LoadingPatchMeta");
                LoadingPatchMeta(mid, postingOrderInIndex);
                timer.event_stop("LoadingPatchMeta");
            }

            timer.event_start("SPANN LoadingHeadInfo");
            m_totalListCount +=
                LoadingHeadInfo(ifilename, p_opt.m_searchPostingPageLimit, mid,
                                &postingOrderInIndex);
            timer.event_stop("SPANN LoadingHeadInfo");

            if (m_options.m_curMid != SPMerge::InvalidMID) {
                m_postingOrderInIndex.assign(postingOrderInIndex.begin(),
                                             postingOrderInIndex.end());
            }
        }
    } else {
        timer.event_start("SPANN LoadingHeadInfo");
        m_extraFullGraphFile =
            p_opt.m_indexDirectory + FolderSep + p_opt.m_ssdIndex;
        std::string curFile = m_extraFullGraphFile;
        do {
            auto curIndexFile = f_createAsyncIO();
            f_open(curIndexFile, curFile);
            m_indexFiles.emplace_back(curIndexFile);
            m_totalListCount +=
                LoadingHeadInfo(curFile, p_opt.m_searchPostingPageLimit);

            curFile = m_extraFullGraphFile + "_" +
                      std::to_string(m_indexFiles.size());
        } while (fileexists(curFile.c_str()));
        timer.event_stop("SPANN LoadingHeadInfo");
    }
    m_oneContext = (m_indexFiles.size() == 1);
    timer.event_stop("Load index");
    timer.log_all_event_duration();
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::LoadingClusterContent(
    const char* buffer, size_t totalClusters) {
    EventTimer timer;
    std::string name = "load " + std::to_string(totalClusters) + " clusters";
    timer.event_start(name, false);

    timer.event_start("1. Sequential init", false);
    struct ClusterInfo {
        const char* src;
        SPMerge::ClusterID cid;
        size_t n_vecs;
    };
    BlockingConcurrentQueue<ClusterInfo> queue;

    std::thread t_init_map([&]() {
        m_recovery_mr->cluster_content.reserve(
            m_recovery_mr->cluster_content.size() + totalClusters);
        ClusterInfo data;
        while (true) {
            queue.wait_dequeue(data);
            if (data.cid == SPMerge::InvalidCID) break;
            m_recovery_mr->cluster_content.emplace(data.cid,
                                                   SPMerge::VectorIDList());
        }
        LOG_INFO("cluster content map init done\n");
    });

    std::vector<ClusterInfo> all_data(totalClusters);

    // thread to enqueue
    const char* ptr = buffer;
    for (size_t i = 0; i < totalClusters; i++) {
        auto meta =
            reinterpret_cast<const PatchMetadata::MetaClusterContent*>(ptr);

        ClusterInfo data{ptr + sizeof(*meta), meta->cid, meta->n_vecs};
        all_data[i] = data;
        queue.enqueue(data);

        assert(meta->n_vecs != 0);
        size_t list_size = meta->n_vecs * sizeof(SPMerge::VectorID);
        ptr += sizeof(*meta) + list_size;
    }
    queue.enqueue(ClusterInfo{nullptr, SPMerge::InvalidCID, 0});
    LOG_INFO("all cluster content data enqueued\n");

    t_init_map.join();
    timer.event_stop("1. Sequential init");

    timer.event_start("2. Parallel fill data", false);
#pragma omp parallel for
    for (uint i = 0; i < totalClusters; i++) {
        ClusterInfo& data = all_data[i];
        auto& vid_list = m_recovery_mr->cluster_content[data.cid];
        vid_list.resize(data.n_vecs);
        size_t list_size = data.n_vecs * sizeof(SPMerge::VectorID);
        memcpy(vid_list.data(), data.src, list_size);
    }
    timer.event_stop("2. Parallel fill data");

    // assert(m_recovery_mr->cluster_content.size() == totalClusters);
    timer.event_stop(name);
    timer.log_all_event_duration();
}

template <typename ValueType>
ClusterContentSelection ExtraFullGraphSearcher<ValueType>::SelectClusterContent(
    const uint64_t* sub_off, const PatchMetadata& p_meta, size_t n_clusters,
    SPMerge::MachineID mid) {
    ClusterContentSelection s;
    s.off = p_meta.clusterContentStart + p_meta.n_machine * sizeof(uint64_t);
    if (mid == SPMerge::InvalidMID) {
        s.bytes = p_meta.vectorIDStart - s.off;
        s.n_clusters = p_meta.n_totalClusters;
    } else {
        s.off = p_meta.clusterContentStart + sub_off[mid];
        s.bytes = mid == p_meta.n_machine - 1 ? p_meta.vectorIDStart - s.off
                                              : sub_off[mid + 1] - sub_off[mid];
        s.n_clusters = n_clusters;
    }
    return s;
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::CheckClusterContent(
    const SPMerge::MachineID mid, const bool target_value,
    std::vector<std::atomic_bool>& visited,
    SPMerge::ClusterIDList& cid_list_output,
    SPMerge::VectorIDList& vid_list_output, void* pool,
    std::future<void>* when_to_load) {
    EventTimer timer;
    std::string name = "check m" + std::to_string(mid) + "'s clusters";
    timer.event_start(name, false);

    auto thread_pool = reinterpret_cast<BS::light_thread_pool*>(pool);

    const auto my_mid = m_options.m_curMid;
    if (my_mid == SPMerge::InvalidMID) NOT_IMPLEMENTED();

    const auto& p_meta = m_patchMeta[my_mid];

    vid_list_output.clear();
    cid_list_output.clear();
    vid_list_output.reserve(p_meta.n_clusvec);
    cid_list_output.reserve(p_meta.n_clusters);

    auto pfilename = get_patch_file_name(my_mid);
    SimpleIOHelper sih(pfilename, SimpleIOHelper::ReadOnly);

    struct ClusterInfo {
        const char* src;
        SPMerge::ClusterID cid;
        size_t n_vecs;
    };

    timer.event_start("1. Sequential init", false);
    // calc disk area
    std::vector<uint64_t> sub_off(p_meta.n_machine);
    sih.f_read_data(sub_off.data(), p_meta.n_machine * sizeof(uint64_t),
                    p_meta.clusterContentStart);
    auto s = SelectClusterContent(sub_off.data(), p_meta, 0, mid);
    // read disk
    std::unique_ptr<char[]> buffer(new char[s.bytes]);
    sih.f_read_data(buffer.get(), s.bytes, s.off);
    // collect info for multi thread
    std::vector<ClusterInfo> all_data;
    all_data.reserve(p_meta.n_clusters);
    uint64_t offset = 0;
    while (offset < s.bytes) {
        const char* ptr = buffer.get() + offset;
        auto meta =
            reinterpret_cast<const PatchMetadata::MetaClusterContent*>(ptr);
        ClusterInfo data{ptr + sizeof(*meta), meta->cid, meta->n_vecs};
        all_data.push_back(data);
        assert(meta->n_vecs != 0);
        size_t list_size = meta->n_vecs * sizeof(SPMerge::VectorID);
        offset += sizeof(*meta) + list_size;
    }
    if (offset != s.bytes) {
        LOG_ERROR("offset=%llu, s.bytes=%llu\n", offset, s.bytes);
    }
    timer.event_stop("1. Sequential init");

    timer.event_start("2. Wait for wake-up", false);
    if (when_to_load) when_to_load->wait();
    timer.event_stop("2. Wait for wake-up");

    timer.event_start("3. Parallel check", false);

    const size_t num_threads = m_options.m_recoveryNumThreads;
    std::vector<SPMerge::ClusterIDList> cid_list_local(num_threads);
    std::vector<SPMerge::VectorIDList> vid_list_local(num_threads);
    std::vector<std::future<void>> future_list(num_threads);
    std::atomic_size_t global_idx = 0;

    for (int j = 0; j < num_threads; j++) {
        auto future = thread_pool->submit_task([&, tid = j]() {
            auto& cid_list = cid_list_local[tid];
            auto& vid_list = vid_list_local[tid];
            while (true) {
                auto idx = global_idx.fetch_add(1);
                if (idx >= all_data.size()) break;
                const auto& data = all_data[idx];
                auto vid_arr =
                    reinterpret_cast<const SPMerge::VectorID*>(data.src);
                bool send = false;
                for (size_t i = 0; i < data.n_vecs; i++) {
                    auto vid = vid_arr[i];
                    if (visited[vid].exchange(target_value,
                                              std::memory_order_relaxed) !=
                        target_value) {
                        send = true;
                        vid_list.push_back(vid);
                    }
                }
                if (send) cid_list.push_back(data.cid);
            }
        });
        future_list[j] = std::move(future);
    }
    for (auto& future : future_list) future.wait();

    timer.event_stop("3. Parallel check");

    timer.event_start("4. Merge list", false);
    for (int i = 0; i < num_threads; ++i) {
        cid_list_output.insert(cid_list_output.end(), cid_list_local[i].begin(),
                               cid_list_local[i].end());
        vid_list_output.insert(vid_list_output.end(), vid_list_local[i].begin(),
                               vid_list_local[i].end());
    }
    timer.event_stop("4. Merge list");

    timer.event_stop(name);
    timer.log_all_event_duration();
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::LoadingPatchVIDs(
    const SPMerge::MachineID mid) {
    const auto my_mid = m_options.m_curMid;
    if (my_mid == SPMerge::InvalidMID) NOT_IMPLEMENTED();
    auto mr = m_recovery_mr.get();
    auto& patch = mr->patch[mid];
    if (not patch.empty()) {
        LOG_WARN("patch for m%u is loaded before, skipping it", mid);
        return;
    }
    auto pfilename = get_patch_file_name(my_mid);
    SimpleIOHelper sih(pfilename, SimpleIOHelper::ReadOnly);
    const auto& p_meta = m_patchMeta[my_mid];

    auto off = p_meta.vectorIDStart;
    auto my_patch_list_bytes = p_meta.n_vectors * sizeof(SPMerge::VectorID);
    if (mid == my_mid) {
        patch.resize(p_meta.n_vectors);
        sih.f_read_data(patch.data(), my_patch_list_bytes, off);
        return;
    }
    off += my_patch_list_bytes;
    for (SPMerge::MachineID check_mid = 0; check_mid < m_numNodes;
         check_mid++) {
        if (check_mid == my_mid) continue;
        // read meta
        PatchMetadata::MetaPatch meta;
        sih.f_read_data(&meta, sizeof(meta), off);
        if (meta.mid != check_mid) {
            LOG_ERROR("mismatch mid, read:%u but expected: %u\n", meta.mid,
                      check_mid);
            throw std::runtime_error("");
        }
        // content
        off += sizeof(meta);
        auto vid_list_bytes = meta.n_vecs * sizeof(SPMerge::VectorID);
        if (check_mid == mid) {
            patch.resize(meta.n_vecs);
            sih.f_read_data(patch.data(), vid_list_bytes, off);
            return;
        }
        off += vid_list_bytes;
    }
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::LoadingPatchMeta(
    const SPMerge::MachineID mid, std::vector<SPANNCID>& p_postingOrderInIndex,
    SPMerge::MachineID cc_load_which_mid) {
    auto pfilename = get_patch_file_name(mid);
    SimpleIOHelper sih(pfilename, SimpleIOHelper::ReadOnly);
    auto& p_meta = m_patchMeta[mid];
    sih.f_read_data(&p_meta, sizeof(PatchMetadata));
    if (p_meta.n_machine != m_numNodes) {
        LOG_ERROR("n_machine=%u, m_numNodes=%u\n", p_meta.n_machine,
                  m_numNodes);
        throw std::logic_error("");
    }

    p_postingOrderInIndex.clear();
    p_postingOrderInIndex.resize(p_meta.n_clusters);
    size_t order_storage_bytes = p_meta.n_clusters * sizeof(SPANNCID);
    sih.f_read_data(p_postingOrderInIndex.data(), order_storage_bytes,
                    p_meta.postingOrderStart);
    if (m_recovery_mr == nullptr) {
        m_recovery_mr.reset(new SPMerge::MergerResult);
    }
    auto mr = m_recovery_mr.get();
    mr->cluster_assignment.resize(p_meta.n_machine);
    mr->patch.resize(p_meta.n_machine);
    mr->cluster_assignment.resize(p_meta.n_machine);
    return;

    if (not is_primary_node(mid)) return;

    if (mr->cluster_assignment.size() == p_meta.n_machine) {
        LOG_INFO(
            "cluster assignment is already loaded completedly, "
            "avoid loading agin\n");
    } else {
        // cluster assignment: self
        mr->cluster_assignment.resize(p_meta.n_machine);
        mr->cluster_assignment[mid].assign(p_postingOrderInIndex.begin(),
                                           p_postingOrderInIndex.end());
        // cluster assignment: others
        auto off = p_meta.clusterAssignmentStart;
        for (SPMerge::MachineID other_mid = 0; other_mid < m_numNodes;
             other_mid++) {
            if (other_mid == mid) continue;

            // read meta
            PatchMetadata::MetaClusterAssignment meta;
            sih.f_read_data(&meta, sizeof(meta), off);
            if (meta.mid != other_mid) {
                LOG_ERROR("mismatch mid, read:%u but expected: %u\n", meta.mid,
                          other_mid);
                throw std::runtime_error("");
            }
            // content
            off += sizeof(meta);
            auto cid_list_bytes = meta.n_clusters * sizeof(SPMerge::ClusterID);
            mr->cluster_assignment[other_mid].resize(meta.n_clusters);
            sih.f_read_data(mr->cluster_assignment[other_mid].data(),
                            cid_list_bytes, off);
            off += cid_list_bytes;
        }
        // 把故障机器作为参数传入
        if (m_options.m_errorMachineList.empty()) {
            LOG_INFO("All Machine Healthy\n");
        } else {
            if (m_error_machine_cids.empty()) {
                for (auto err_id : m_options.m_errorMachineList) {
                    LOG_INFO("Error Machine IDs: %d\n", err_id);
                    const auto& cluster_assignment_error =
                        mr->cluster_assignment[err_id];
                    m_error_machine_cids.insert(
                        cluster_assignment_error.begin(),
                        cluster_assignment_error.end());
                }
            }
        }
    }

    if (mr->cluster_content.size() == p_meta.n_totalClusters) {
        LOG_INFO(
            "cluster content is already loaded completedly (%lu clusters), "
            "avoid loading agin\n",
            p_meta.n_totalClusters);
    } else {
        // read cluster content for freq pruning
        std::vector<uint64_t> sub_off(p_meta.n_machine);
        sih.f_read_data(sub_off.data(), p_meta.n_machine * sizeof(uint64_t),
                        p_meta.clusterContentStart);

        if (cc_load_which_mid != mid and
            cc_load_which_mid != SPMerge::InvalidMID) {
            NOT_IMPLEMENTED();
        }

        auto s = SelectClusterContent(sub_off.data(), p_meta, p_meta.n_clusters,
                                      cc_load_which_mid);
        std::unique_ptr<char[]> buffer(new char[s.bytes]);
        sih.f_read_data(buffer.get(), s.bytes, s.off);
        LoadingClusterContent(buffer.get(), s.n_clusters);
    }
}

template <typename ValueType>
void ExtraFullGraphSearcher<ValueType>::LoadingPatchFullData(
    const SPMerge::MachineID mid, std::vector<SPANNCID>& p_postingOrderInIndex,
    SPMerge::MachineID cc_load_which_mid,
    std::promise<void>* prm_patch_meta_done,
    std::promise<void>* prm_assignment_done,
    std::promise<void>* prm_patch_buff_done,
    std::promise<void>* prm_cluster_content_done) {
    // should not load full patch from a secondary node
    if (not is_primary_node(mid)) NOT_IMPLEMENTED();

    // load full patch into memory
    char* ptr = nullptr;
    Helper::tracked_buffer buffer;
    if (m_pmData.healthy_pbuf != nullptr) {
        ptr = m_pmData.healthy_pbuf.get();
    } else {
        SimpleIOHelper sih;
        auto pfilename = get_patch_file_name(mid);
        sih.normal_init(pfilename, SimpleIOHelper::ReadOnly);
        auto psize = std::filesystem::file_size(pfilename);
        buffer = make_tracked_buffer(psize);
        ptr = buffer.get();
        sih.f_read_data(ptr, psize);
    }

    // 1. regular data
    auto& p_meta = m_patchMeta[mid];
    memcpy(&p_meta, ptr, sizeof(PatchMetadata));
    if (prm_patch_meta_done) prm_patch_meta_done->set_value();

    p_postingOrderInIndex.clear();
    p_postingOrderInIndex.resize(p_meta.n_clusters);
    size_t order_storage_bytes = p_meta.n_clusters * sizeof(SPANNCID);
    memcpy(p_postingOrderInIndex.data(), ptr + p_meta.postingOrderStart,
           order_storage_bytes);

    // LoadingPatchFullData is only called on recovery mode,
    // so we can imagine m_recovery_mr is null before the call
    m_recovery_mr.reset(new SPMerge::MergerResult);
    auto mr = m_recovery_mr.get();
    mr->head_count = p_meta.n_headvec;
    mr->cluster_vector_count = p_meta.n_clusvec;

    // 2. patch
    // patch vid
    auto vid_start_ptr = ptr + p_meta.vectorIDStart;
    auto my_patch_list_bytes = p_meta.n_vectors * sizeof(SPMerge::VectorID);
    mr->patch.resize(m_numNodes);
    mr->patch[mid].resize(p_meta.n_vectors);
    memcpy(mr->patch[mid].data(), vid_start_ptr, my_patch_list_bytes);
    vid_start_ptr += my_patch_list_bytes;
    for (SPMerge::MachineID other_mid = 0; other_mid < m_numNodes;
         other_mid++) {
        if (other_mid == mid) continue;
        // read meta
        auto meta = reinterpret_cast<PatchMetadata::MetaPatch*>(vid_start_ptr);
        if (meta->mid != other_mid) {
            LOG_ERROR("mismatch mid, read:%u but expected: %u\n", meta->mid,
                      other_mid);
            throw std::runtime_error("");
        }
        // content
        vid_start_ptr += sizeof(PatchMetadata::MetaPatch);
        auto vid_list_bytes = meta->n_vecs * sizeof(SPMerge::VectorID);
        mr->patch[other_mid].resize(meta->n_vecs);
        // FIXME: for secondary node, vid_list_bytes=0
        memcpy(mr->patch[other_mid].data(), vid_start_ptr, vid_list_bytes);
        vid_start_ptr += vid_list_bytes;
    }
    // patch buff
    size_t vec_size = mr->patch[mid].size() * p_meta.perVectorSize;
    m_recovery_pbuf_vec = make_tracked_buffer(vec_size);
    memcpy(m_recovery_pbuf_vec.get(), ptr + p_meta.vectorContentStart,
           vec_size);
    if (prm_patch_buff_done) prm_patch_buff_done->set_value();

    // 3. cluster assginment
    // cluster assignment: self
    mr->cluster_assignment.resize(m_numNodes);
    SPMerge::ClusterIDList tmp_vec(p_postingOrderInIndex.begin(),
                                   p_postingOrderInIndex.end());
    mr->cluster_assignment[mid].swap(tmp_vec);

    // cluster assignment: others
    auto ass_start_ptr = ptr + p_meta.clusterAssignmentStart;
    for (SPMerge::MachineID other_mid = 0; other_mid < m_numNodes;
         other_mid++) {
        if (other_mid == mid) continue;
        // read meta
        auto meta = reinterpret_cast<PatchMetadata::MetaClusterAssignment*>(
            ass_start_ptr);
        if (meta->mid != other_mid) {
            LOG_ERROR("mismatch mid, read:%u but expected: %u\n", meta->mid,
                      other_mid);
            throw std::runtime_error("");
        }
        // content
        ass_start_ptr += sizeof(PatchMetadata::MetaClusterAssignment);
        auto cid_list_bytes = meta->n_clusters * sizeof(SPMerge::ClusterID);
        mr->cluster_assignment[other_mid].resize(meta->n_clusters);
        memcpy(mr->cluster_assignment[other_mid].data(), ass_start_ptr,
               cid_list_bytes);
        ass_start_ptr += cid_list_bytes;
    }
    if (prm_assignment_done) prm_assignment_done->set_value();

    // 4. cluster content
    size_t numClusterOnMID = 0;
    if (cc_load_which_mid != SPMerge::InvalidMID) {
        numClusterOnMID = mr->cluster_assignment[cc_load_which_mid].size();
    }
    auto s = SelectClusterContent(
        reinterpret_cast<uint64_t*>(ptr + p_meta.clusterContentStart), p_meta,
        numClusterOnMID, cc_load_which_mid);
    LoadingClusterContent(ptr + s.off, s.n_clusters);
    if (prm_cluster_content_done) prm_cluster_content_done->set_value();
}

template <typename ValueType>
ErrorCode ExtraFullGraphSearcher<ValueType>::GetPostingList(
    ExtraWorkSpace* p_exWorkSpace, std::shared_ptr<VectorIndex> p_index,
    const SPMerge::ClusterIDList& cids,
    SPMerge::ClusterContentList& cluster_content,
    std::function<void(uint64_t, void*)> f_handle_vector) const {
    if (m_enableDataCompression) throw std::invalid_argument("not implemented");
    if (m_options.m_loadMergerResult) throw std::logic_error("not implemented");

    size_t idx = 0;
    for (auto cid : cids) {
        auto listInfo = &m_listInfos[cid];

        cluster_content[cid] = {};
        int fileid = m_oneContext ? 0 : cid / m_listPerFile;

        size_t totalBytes =
            (static_cast<size_t>(listInfo->listPageCount) << PageSizeEx);
        char* buffer = (char*)((p_exWorkSpace->m_pageBuffers[idx]).GetBuffer());

        auto& request = p_exWorkSpace->m_diskRequests[idx];
        request.m_offset = listInfo->listOffset;
        request.m_readSize = totalBytes;
        request.m_buffer = buffer;
        request.m_status = (fileid << 16) | p_exWorkSpace->m_spaceID;
        request.m_payload = (void*)listInfo;
        request.m_success = false;

        request.m_callback = [&cluster_content, f_handle_vector, cid, idx,
                              p_exWorkSpace, p_index, this](bool success) {
            auto& vid_list = cluster_content.at(cid);
            auto& request = p_exWorkSpace->m_diskRequests[idx];
            char* buffer = request.m_buffer;
            auto listInfo =
                reinterpret_cast<const ListInfo*>(request.m_payload);
            char* p_postingListFullData = buffer + listInfo->pageOffset;

            vid_list.reserve(listInfo->listEleCount);
            for (int i = 0; i < listInfo->listEleCount; i++) {
                uint64_t offsetVectorID, offsetVector;
                (this->*m_parsePosting)(offsetVectorID, offsetVector, i,
                                        listInfo->listEleCount);
                int vectorID = *(reinterpret_cast<int*>(p_postingListFullData +
                                                        offsetVectorID));

                vid_list.push_back(vectorID);
                if (f_handle_vector) {
                    auto vec_ptr = p_postingListFullData + offsetVector;
                    f_handle_vector(vectorID, vec_ptr);
                }
            }
        };
        idx++;
    }
    BatchReadFileAsync(m_indexFiles, (p_exWorkSpace->m_diskRequests).data(),
                       cids.size());
    return ErrorCode::Success;
}