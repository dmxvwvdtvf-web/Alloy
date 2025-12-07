// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/mman.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include "blockingconcurrentqueue.h"
#include "BS_thread_pool.hpp"
#include "inc/Core/Common.h"
#include "inc/Core/Common/QueryResultSet.h"
#include "inc/Core/MetadataSet.h"
#include "inc/Core/ResultIterator.h"
#include "inc/Core/SPANN/ExtraFullGraphSearcher.h"
#include "inc/Core/SPANN/IExtraSearcher.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Core/SPANN/RecoveryPacket.h"
#include "inc/Core/SPANN/SPANNResultIterator.h"
#include "inc/Core/SPANN/VoidIndex.h"
#include "inc/Core/SPMerge.h"
#include "inc/Helper/DiskIO.h"
#include "inc/Helper/MemoryManager.h"
#include "inc/Helper/SocketWrapper.h"
#include "inc/Helper/Timer.h"
#include "inc/Helper/VectorSetReaders/MemoryReader.h"
#include "inc/SSDServing/Utils.h"

#pragma warning(disable : 4242)  // '=' : conversion from 'int' to 'short',
                                 // possible loss of data
#pragma warning(disable : 4244)  // '=' : conversion from 'int' to 'short',
                                 // possible loss of data
#pragma warning(disable : 4127)  // conditional expression is constant

namespace SPTAG {
template <typename T>
thread_local std::unique_ptr<T>
    COMMON::ThreadLocalWorkSpaceFactory<T>::m_workspace;
namespace SPANN {
std::atomic_int ExtraWorkSpace::g_spaceCount(0);
EdgeCompare Selection::g_edgeComparer;

std::function<std::shared_ptr<Helper::DiskIO>(void)> f_createAsyncIO =
    []() -> std::shared_ptr<Helper::DiskIO> {
    return std::shared_ptr<Helper::DiskIO>(new Helper::AsyncFileIO());
};

using Helper::f_read_wrapper;
using Helper::f_sendfile64_wrapper;
using Helper::f_write_wrapper;

using moodycamel::BlockingConcurrentQueue;

// Struct For Pnode->Snode
struct TaskMapHeader {
    uint32_t num_clusters;  // secondary node需要遍历多少个cid
    uint64_t total_vids;    // secondary node需要发送多少个vid
    size_t totalVec;
    size_t perVectorSize;
    size_t vecInfoSize;
};

// TODO: now we read whole index file, need to optionally read specific
// clusters.
template <typename T>
void Index<T>::GuideDiskIO(HelperContext& context,
                           std::unique_ptr<char[]>& vector_buffer,
                           const size_t NET_IO_SIZE, const size_t DISK_IO_SIZE,
                           const size_t ALIGNMENT) {
    auto disk_index =
        dynamic_cast<ExtraFullGraphSearcher<T>*>(m_extraSearcher.get());
    const auto my_id = m_options.m_curMid;
    const auto& p_meta = disk_index->m_patchMeta[my_id];
    const size_t vecInfoSize = p_meta.perVectorSize + sizeof(SPANNVID);
    const auto ifname = disk_index->get_index_file_name(my_id);
    const auto pfname = disk_index->get_patch_file_name(my_id);
    const auto index_size = std::filesystem::file_size(ifname);

    // fill context
    context.postingOrderInIndex = &disk_index->m_postingOrderInIndex;
    context.listInfoMap = &disk_index->m_listInfoMap;
    //
    context.ALIGNMENT = ALIGNMENT;
    context.perVectorSize = p_meta.perVectorSize;
    context.vecInfoSize = vecInfoSize;
    //
    context.small_buffer_size = NET_IO_SIZE;
    context.io_read_size = DISK_IO_SIZE;
    // use pread to avoid multi thread read conflict
    context.index_file_io.normal_init(std::make_shared<Helper::NativeFileIO>(),
                                      ifname, SimpleIOHelper::ReadOnly, false,
                                      false, true);

    // vectorset buffer
    const float NetworkBandWidth = 1.1;
    const float DiskReadBandWidth = 6.1;
    size_t buffersize = index_size / DiskReadBandWidth * NetworkBandWidth;
    buffersize = ROUND_UP(buffersize, context.small_buffer_size);
    vector_buffer.reset(new char[buffersize]);
    LOG_INFO("Allocated %s vectorset buffer\n",
             bytesToString(buffersize).c_str());
    // enqueue
    char* ptr = vector_buffer.get();
    while (ptr < vector_buffer.get() + buffersize) {
        context.writer_buffer_queue.enqueue(ptr);
        ptr += context.small_buffer_size;
    }

    // split area, 128KB
    std::pair<size_t, size_t> area(0, 0);
    size_t byteStart =
        context.listInfoMap->at(context.postingOrderInIndex->at(0)).listOffset;
    assert(byteStart % context.ALIGNMENT == 0);
    assert(context.io_read_size % context.ALIGNMENT == 0);
    for (size_t i = 0; i < context.postingOrderInIndex->size(); i++) {
        auto cid = context.postingOrderInIndex->at(i);
        const auto& listinfo = context.listInfoMap->at(cid);
        auto curStart = listinfo.listOffset + listinfo.pageOffset;
        auto curEnd = curStart + listinfo.listTotalBytes;
        if (curEnd - byteStart > context.io_read_size) {
            area.second = i - 1;
            context.area_list.push_back(area);
            area.first = i;
            byteStart = curStart;
        }
    }
    // handle final one
    size_t final_idx = context.postingOrderInIndex->size() - 1;
    const auto& final_list_info =
        context.listInfoMap->at(context.postingOrderInIndex->at(final_idx));
    assert(final_list_info.listOffset + final_list_info.pageOffset +
               final_list_info.listTotalBytes - byteStart <=
           context.io_read_size);
    area.second = final_idx;
    context.area_list.push_back(area);
}

template <typename T>
void Index<T>::InitContextDeduper(HelperContext& context,
                                  const SPMerge::VectorIDList target_vid_list) {
    auto disk_index =
        dynamic_cast<ExtraFullGraphSearcher<T>*>(m_extraSearcher.get());
    const auto my_id = m_options.m_curMid;
    const auto& p_meta = disk_index->m_patchMeta[my_id];
    const auto totalVec = p_meta.n_clusvec + p_meta.n_headvec;
    if (context.visited.size() == 0) {
        context.visited = decltype(context.visited)(totalVec);
    } else {
        LOG_WARN(
            "The visited array is non-empty(%llu/%llu) on initialization, all "
            "entries are expected to be false\n",
            context.visited.size(), totalVec);
    }

    // infomation for target vid list
    if (target_vid_list.size() == 0) {
        context.target_count = p_meta.n_clusvec - p_meta.n_vectors;
    } else {
        context.target_count = target_vid_list.size();
        context.flip_flag = true;
#pragma omp parallel for num_threads(m_options.m_recoveryNumThreads)
        for (auto vid : target_vid_list) {
            context.visited[vid].store(true, std::memory_order_relaxed);
        }
    }
}

template <typename T>
void Index<T>::SplitVIDTask(SPMerge::ClusterIDList& primary_task_cids,
                            SPMerge::VectorIDList& primary_task_vids,
                            SPMerge::ClusterIDList& secondary_task_cids,
                            SPMerge::VectorIDList& secondary_task_vids,
                            std::vector<std::atomic_bool>& need_secondary_vids,
                            SPMerge::MachineID fnode_id, void* pool) {
    Helper::EventTimer timer;

    timer.event_start("SplitVIDTask");
    auto thread_pool = reinterpret_cast<BS::light_thread_pool*>(pool);
    auto disk_index =
        dynamic_cast<ExtraFullGraphSearcher<T>*>(m_extraSearcher.get());
    const auto pnode_id = m_options.m_curMid;
    const auto& p_meta = disk_index->m_patchMeta[pnode_id];
    const auto totalVec = p_meta.n_clusvec + p_meta.n_headvec;
    const auto& mr = disk_index->m_recovery_mr;
    // FIXME: avoid hard code
    const SPMerge::MachineID snode_id = pnode_id + 1;

    need_secondary_vids = std::vector<std::atomic_bool>(totalVec);
    thread_pool->detach_task([&]() {
        const size_t page_size = 4096;
        const size_t stride = page_size / sizeof(T);
        for (size_t idx = 0; idx < totalVec; idx += stride) {
            [[maybe_unused]] volatile bool dummy =
                need_secondary_vids[idx].load(std::memory_order_relaxed);
        }
    });

    // 1.
    std::promise<void> prm_fnode_checked;
    auto future_fnode_checked = prm_fnode_checked.get_future();
    auto future_fcc_checked = thread_pool->submit_task([&]() {
        SPMerge::ClusterIDList cid_list;
        SPMerge::VectorIDList vid_list;
        disk_index->CheckClusterContent(fnode_id, true, need_secondary_vids,
                                        cid_list, vid_list, pool, nullptr);
    });
    auto future_fpatch_checked = thread_pool->submit_task([&] {
        disk_index->LoadingPatchVIDs(fnode_id);
        thread_pool
            ->submit_loop(0, mr->patch[fnode_id].size(),
                          [&](size_t i) {
                              const auto vid = mr->patch[fnode_id][i];
                              need_secondary_vids[vid].store(
                                  true, std::memory_order_relaxed);
                          })
            .wait();
    });

    // 2.
    auto future_pcc_checked = thread_pool->submit_task([&]() {
        disk_index->CheckClusterContent(pnode_id, false, need_secondary_vids,
                                        primary_task_cids, primary_task_vids,
                                        pool, &future_fnode_checked);
    });
    // 3.
    auto future_scc_checked = thread_pool->submit_task([&]() {
        disk_index->CheckClusterContent(
            snode_id, false, need_secondary_vids, secondary_task_cids,
            secondary_task_vids, pool, &future_pcc_checked);
    });
    auto future_ppatch_checked = thread_pool->submit_task([&] {
        disk_index->LoadingPatchVIDs(pnode_id);
        thread_pool
            ->submit_loop(0, mr->patch[pnode_id].size(),
                          [&](size_t i) {
                              const auto vid = mr->patch[pnode_id][i];
                              need_secondary_vids[vid].store(
                                  false, std::memory_order_relaxed);
                          })
            .wait();
    });

    const size_t num_threads = m_options.m_recoveryNumThreads;
    LOG_INFO("using %llu threads to split vid task\n", num_threads);

    timer.event_start("1. check fnode vectors");
    future_fpatch_checked.wait();
    future_fcc_checked.wait();
    prm_fnode_checked.set_value();
    timer.event_stop("1. check fnode vectors");

    timer.event_start("2. check pnode vectors");
    future_pcc_checked.wait();
    timer.event_stop("2. check pnode vectors");

    timer.event_start("3. check snode vectors");
    future_scc_checked.wait();
    timer.event_stop("3. check snode vectors");

    future_ppatch_checked.wait();

    timer.event_stop("SplitVIDTask");
    timer.log_all_event_duration();
}

template <typename T>
void Index<T>::f_send_worker(HelperContext& ctx, int sockfd) {
    {
        SSDServing::Utils::StopW sw;
        PacketHeader header;
        f_read_wrapper(sockfd, &header, sizeof(header));
        if (header.type != RecoveryPacketType::IAmReady) {
            LOG_ERROR("received header with type=%u, idx=%llu\n", header.type,
                      header.idx);
        }
        LOG_INFO("send vectors to fail node waiting time: %.1f ms\n",
                 sw.getElapsedMs());
    }
    const auto my_id = m_options.m_curMid;
    const int ON = 1, OFF = 0;

    size_t vector_sent = 0;

    setsockopt(sockfd, IPPROTO_TCP, TCP_CORK, &ON, sizeof(ON));

    // HelpInfo3
    // tell receiver transfer size
    PacketHeader header;
    header.type = RecoveryPacketType::HelpInfo;
    header.idx = my_id;
    header.n = ctx.target_count;
    f_write_wrapper(sockfd, &header, sizeof(header));

    const size_t CheckGranularity = 0.1 * ctx.target_count;
    size_t check_target = CheckGranularity;

    HelperContext::ReaderQueueItem item;
    while (vector_sent < ctx.target_count) {
        ctx.reader_buffer_queue.wait_dequeue(item);
        auto [ptr, size] = item;
        assert(size % ctx.vecInfoSize == 0);
        assert(size <= ctx.small_buffer_size);

        PacketHeader header;
        header.type = RecoveryPacketType::IndexData;
        header.sub_size = size;
        f_write_wrapper(sockfd, &header, sizeof(header));
        f_write_wrapper(sockfd, ptr, size);

        ctx.writer_buffer_queue.enqueue(ptr);

        vector_sent += size / ctx.vecInfoSize;

        if (vector_sent >= check_target) {
            float ratio = 100. * vector_sent / ctx.target_count;
            LOG_INFO("sent %.1f%%\n", ratio);
            check_target =
                (vector_sent / CheckGranularity + 1) * CheckGranularity;
        }
    }
    // NOTE: no need for now!
    // tell receiver done
    // header.type = RecoveryPacketType::IAmReady;
    // f_write_wrapper(sockfd, &header, sizeof(header));
    setsockopt(sockfd, IPPROTO_TCP, TCP_CORK, &OFF, sizeof(OFF));
    if (not ctx.task_done()) {
        LOG_ERROR(
            "vectors sent=%llu, but ctx.task_done()=false, ctx.finished=%llu\n",
            vector_sent, ctx.finished.load());
        throw std::logic_error("");
    }
    LOG_INFO("vector data all sent\n");
}

template <typename T>
size_t Index<T>::f_collect_worker(HelperContext& ctx) {
    std::unique_ptr<char, void (*)(char*)> load_buffer(
        static_cast<char*>(PAGE_ALLOC(ctx.io_read_size + ctx.ALIGNMENT)),
        [](char* ptr) { PAGE_FREE(ptr); });

    // buffer to collect vectors, dequeue from writer_buffer_queue
    char* local_vec_buffer = nullptr;
    const size_t lvb_capacity = ctx.small_buffer_size / ctx.vecInfoSize;
    size_t lvb_idx = 0;

    ctx.writer_buffer_queue.wait_dequeue(local_vec_buffer);

    auto f_clear_local_buffer = [&]() {
        if (lvb_idx == 0) return;
        assert(lvb_idx <= lvb_capacity);
        HelperContext::ReaderQueueItem ritem{local_vec_buffer,
                                             lvb_idx * ctx.vecInfoSize};
        ctx.reader_buffer_queue.enqueue(ritem);
        ctx.writer_buffer_queue.wait_dequeue(local_vec_buffer);
        lvb_idx = 0;
    };

    // load and output
    size_t bytes = 0;
    while (not ctx.task_done()) {
        HelperContext::AreaInfo ai;
        // get area info
        if (not ctx.get_area(ai)) break;
        // read area
        assert(ai.area_size <= ctx.io_read_size + ctx.ALIGNMENT);
        ctx.index_file_io.f_read_data(load_buffer.get(), ai.area_size,
                                      ai.area_start);
        bytes += ai.area_size;
        // collect vectors
        for (auto cidx = ai.cidx1; cidx <= ai.cidx2; cidx++) {
            auto cid = ctx.postingOrderInIndex->at(cidx);
            const auto& listinfo = ctx.listInfoMap->at(cid);
            auto off_in_buffer =
                listinfo.listOffset + listinfo.pageOffset - ai.area_start;
            for (int i = 0; i < listinfo.listEleCount; i++) {
                uint64_t offset_vid = off_in_buffer + i * ctx.vecInfoSize;
                uint64_t offset_vec = offset_vid + sizeof(SPANNVID);
                auto vid = *reinterpret_cast<SPANNVID*>(load_buffer.get() +
                                                        offset_vid);
                auto src_ptr = load_buffer.get() + offset_vec;

                if (not ctx.check_and_set(vid)) continue;

                if (lvb_idx == lvb_capacity) f_clear_local_buffer();

                char* dest_ptr =
                    local_vec_buffer + (lvb_idx++) * ctx.vecInfoSize;
                memcpy(dest_ptr, &vid, sizeof(vid));
                memcpy(dest_ptr + sizeof(vid), src_ptr, ctx.perVectorSize);
            }
        }
    }
    f_clear_local_buffer();
    return bytes;
}

template <typename T>
bool Index<T>::HelpRecovery(SPMerge::MachineID fail_mid, std::string fail_ip,
                            int fail_port, void* pool) {
    Helper::EventTimer timer;
    timer.event_start("HelpRecovery");

    bool del_pool = false;
    if (pool == nullptr) {
        pool = new BS::thread_pool;
        del_pool = true;
    }
    auto threadpool = reinterpret_cast<BS::light_thread_pool*>(pool);

    auto disk_index =
        dynamic_cast<ExtraFullGraphSearcher<T>*>(m_extraSearcher.get());
    if (disk_index == nullptr) {
        throw std::runtime_error("disk index is not ExtraFullGraphSearcher");
    }
    if (not m_options.m_loadMergerResult) {
        throw std::logic_error("should use merger result to recover");
    }
    if (m_options.m_enablePostingListRearrange or
        m_options.m_enableDeltaEncoding or m_options.m_enableDataCompression) {
        NOT_IMPLEMENTED();
    }

    timer.event_start("0. Prepare connection");

    Helper::SocketManager sm;

    const auto my_id = m_options.m_curMid;
    const auto& p_meta = disk_index->m_patchMeta[my_id];
    const size_t vecInfoSize = p_meta.perVectorSize + sizeof(SPANNVID);
    const auto totalVec = p_meta.n_clusvec + p_meta.n_headvec;
    const int ON = 1, OFF = 0;

    const auto ifname = disk_index->get_index_file_name(my_id);
    const auto pfname = disk_index->get_patch_file_name(my_id);
    const auto patch_size = std::filesystem::file_size(pfname);
    const auto index_size = std::filesystem::file_size(ifname);

    const int pfd = open(pfname.c_str(), O_RDONLY);
    if (pfd < 0) {
        throw std::runtime_error("failed to open index or patch file");
    }

    SimpleIOHelper pio(pfname, SimpleIOHelper::ReadOnly);

    HelperContext context;
    std::unique_ptr<char[]> vector_buffer;
    auto prepare_vector_collection = threadpool->submit_task([&]() {
        timer.event_start("(background) Prepare context");
        GuideDiskIO(context, vector_buffer);
        InitContextDeduper(context, {});
        timer.event_stop("(background) Prepare context");
    });

    int sockfd = sm.f_connect_wrapper(fail_ip, fail_port);

    timer.event_stop("0. Prepare connection");

    timer.event_start("1. Send metadata");
    // send info HelpInfo1
    LOG_INFO("sending help info\n");
    {
        PacketHeader header;
        header.type = RecoveryPacketType::HelpInfo;
        header.idx = my_id;
        header.sub_size = patch_size;
        header.n = index_size;
        f_write_wrapper(sockfd, &header, sizeof(header));
    }

    timer.event_stop("1. Send metadata");

    // send patch data
    timer.event_start("2. Send patch data");
    std::vector<uint64_t> content_sub_off(p_meta.n_machine);
    std::vector<size_t> content_sub_size(p_meta.n_machine);
    pio.f_read_data(content_sub_off.data(), p_meta.n_machine * sizeof(uint64_t),
                    p_meta.clusterContentStart);
    size_t stage2_bytes = 0;
    for (SPMerge::MachineID mid = 0; mid < p_meta.n_machine; mid++) {
        if (mid != p_meta.n_machine - 1) {
            content_sub_size[mid] =
                content_sub_off[mid + 1] - content_sub_off[mid];
        } else {
            content_sub_size[mid] = p_meta.vectorIDStart -
                                    p_meta.clusterContentStart -
                                    content_sub_off[mid];
        }
        if (mid != fail_mid) stage2_bytes += content_sub_size[mid];
    }
    size_t stage1_bytes = patch_size - stage2_bytes;

    timer.event_start("2.1 Stage1");
    LOG_INFO("sending patch data stage1 %s\n",
             bytesToString(stage1_bytes).c_str());
    {
        PacketHeader header;
        header.type = RecoveryPacketType::PatchData;
        header.n = stage1_bytes;
        header.sub_size = stage1_bytes;

        off64_t off2 = p_meta.clusterContentStart + content_sub_off[fail_mid];
        off64_t off3 = p_meta.vectorIDStart;

        size_t bytes1 =
            p_meta.clusterContentStart + p_meta.n_machine * sizeof(uint64_t);
        size_t bytes2 = content_sub_size[fail_mid];
        size_t bytes3 = patch_size - off3;

        if (bytes1 + bytes2 + bytes3 != stage1_bytes) {
            LOG_ERROR("Stage1 bytes not match, expected: %llu, real: %llu\n",
                      stage1_bytes, bytes1 + bytes2 + bytes3);
            throw std::logic_error("");
        }

        setsockopt(sockfd, IPPROTO_TCP, TCP_CORK, &ON, sizeof(ON));
        f_write_wrapper(sockfd, &header, sizeof(header));  //  header
        f_sendfile64_wrapper(sockfd, pfd, bytes1);  // data before content
        f_sendfile64_wrapper(sockfd, pfd, bytes2, &off2);  // content data
        f_sendfile64_wrapper(sockfd, pfd, bytes3,
                             &off3);  // data after content
        setsockopt(sockfd, IPPROTO_TCP, TCP_CORK, &OFF, sizeof(OFF));
    }
    timer.event_stop("2.1 Stage1");

    timer.event_start("2.2 Stage2");
    LOG_INFO("sending patch data stage2 %s\n",
             bytesToString(stage2_bytes).c_str());
    {
        PacketHeader header;
        header.type = RecoveryPacketType::PatchData;
        header.n = stage2_bytes;
        header.sub_size = stage2_bytes;

        setsockopt(sockfd, IPPROTO_TCP, TCP_CORK, &ON, sizeof(ON));
        f_write_wrapper(sockfd, &header, sizeof(header));
        for (SPMerge::MachineID mid = 0; mid < p_meta.n_machine; mid++) {
            if (mid == fail_mid) continue;
            off64_t off = p_meta.clusterContentStart + content_sub_off[mid];
            f_sendfile64_wrapper(sockfd, pfd, content_sub_size[mid], &off);
        }
        setsockopt(sockfd, IPPROTO_TCP, TCP_CORK, &OFF, sizeof(OFF));
    }
    timer.event_stop("2.2 Stage2");
    timer.event_stop("2. Send patch data");

    SSDServing::Utils::StopW sw;
    prepare_vector_collection.wait();
    LOG_INFO("context init waiting time: %f ms\n", sw.getElapsedMs());

    {  // HelpInfo2
        PacketHeader header;
        header.type = RecoveryPacketType::HelpInfo;
        header.idx = 0;
        header.sub_size = context.small_buffer_size;
        header.n = context.target_count;
        f_write_wrapper(sockfd, &header, sizeof(header));
    }

    // send data
    timer.event_start("3. Send vector data");
    LOG_INFO("sending cluster vectors %s\n",
             bytesToString(context.target_count * vecInfoSize).c_str());
    auto t1 = sw.getElapsedMs();
    // wirter
    std::atomic_size_t read_bytes = 0;
    std::vector<std::future<void>> future_vec(m_options.m_numReaderThreads);
    for (int i = 0; i < m_options.m_numReaderThreads; i++) {
        auto future = threadpool->submit_task([&]() {
            auto bytes = f_collect_worker(context);
            read_bytes += bytes;
        });
        future_vec[i] = std::move(future);
    }
    LOG_INFO("Launched %lu threads to read vector data\n",
             m_options.m_numReaderThreads);

    // sender
    f_send_worker(context, sockfd);
    for (auto& future : future_vec) future.wait();
    {  // check disk read rate
        auto dur = sw.getElapsedMs() - t1;
        auto rate = static_cast<size_t>(1000 * read_bytes / dur);
        LOG_INFO("<Disk Read Bandwidth> Collect Vector: %s/s\n",
                 bytesToString(rate).c_str());
    }
    timer.event_stop("3. Send vector data");

    // send translatemap
    timer.event_start("4. Send trasnlate map");
    const auto trans_size = std::filesystem::file_size(
        std::filesystem::path(m_options.m_indexDirectory) /
        m_options.m_headIDFile);
    LOG_INFO("sending translate map %s\n", bytesToString(trans_size).c_str());
    {
        PacketHeader header;
        header.type = RecoveryPacketType::Translate;
        header.n = trans_size / sizeof(uint64_t);
        header.sub_size = trans_size;

        setsockopt(sockfd, IPPROTO_TCP, TCP_CORK, &ON, sizeof(ON));
        f_write_wrapper(sockfd, &header, sizeof(header));
        f_write_wrapper(sockfd, m_vectorTranslateMap.get(), trans_size);
        setsockopt(sockfd, IPPROTO_TCP, TCP_CORK, &OFF, sizeof(OFF));
    }
    timer.event_stop("4. Send trasnlate map");

    LOG_INFO("Recovery data already sent\n");
    {
        PacketHeader header;
        f_read_wrapper(sockfd, &header, sizeof(header));
        if (header.type != RecoveryPacketType::IAmReady) {
            LOG_ERROR("on exit, received header with type=%u, idx=%llu\n",
                      header.type, header.idx);
        }
    }

    if (del_pool) delete threadpool;

    timer.event_stop("HelpRecovery");

    timer.log_all_event_duration();
    return true;
}

template <typename T>
bool Index<T>::PrimaryHelpRecovery(SPMerge::MachineID fail_mid,
                                   std::string fail_ip, int fail_port,
                                   std::string secondary_ip, int secondary_port,
                                   void* pool) {
    Helper::EventTimer timer;
    timer.add_lock();
    timer.event_start("HelpRecovery");
    bool del_pool = false;
    if (pool == nullptr) {
        pool = new BS::thread_pool;
        del_pool = true;
    }
    auto threadpool = reinterpret_cast<BS::light_thread_pool*>(pool);
    auto disk_index =
        dynamic_cast<ExtraFullGraphSearcher<T>*>(m_extraSearcher.get());
    if (disk_index == nullptr) {
        throw std::runtime_error("disk index is not ExtraFullGraphSearcher");
    }
    if (not m_options.m_loadMergerResult) {
        throw std::logic_error("should use merger result to recover");
    }
    if (m_options.m_enablePostingListRearrange or
        m_options.m_enableDeltaEncoding or m_options.m_enableDataCompression) {
        NOT_IMPLEMENTED();
    }
    timer.event_start("0. Prepare connection");

    Helper::SocketManager sm;

    const auto my_id = m_options.m_curMid;
    const auto& p_meta = disk_index->m_patchMeta[my_id];
    const size_t vecInfoSize = p_meta.perVectorSize + sizeof(SPANNVID);
    const auto totalVec = p_meta.n_clusvec + p_meta.n_headvec;
    const int ON = 1, OFF = 0;

    const auto ifname = disk_index->get_index_file_name(my_id);
    const auto pfname = disk_index->get_patch_file_name(my_id);
    const auto patch_size = std::filesystem::file_size(pfname);
    const auto index_size = std::filesystem::file_size(ifname);

    const int pfd = open(pfname.c_str(), O_RDONLY);
    if (pfd < 0) {
        throw std::runtime_error("failed to open index or patch file");
    }

    SimpleIOHelper pio(pfname, SimpleIOHelper::ReadOnly);

    int fail_sockfd = sm.f_connect_wrapper(fail_ip, fail_port);

    timer.event_stop("0. Prepare connection");

    SPMerge::ClusterIDList primary_task_cids, secondary_task_cids;
    SPMerge::VectorIDList primary_task_vids, secondary_task_vids;
    std::vector<std::atomic_bool> need_secondary_vids;
    auto future_split_vid_task = threadpool->submit_task([&]() {
        timer.event_start("(background) split vid task");
        SplitVIDTask(primary_task_cids, primary_task_vids, secondary_task_cids,
                     secondary_task_vids, need_secondary_vids, fail_mid,
                     threadpool);
        timer.event_stop("(background) split vid task");
        LOG_INFO("Primary Node to send: %llu cids, %llu vids\n",
                 primary_task_cids.size(), primary_task_vids.size());
        LOG_INFO("Secondary Node to send %llu cids, %llu vids\n",
                 secondary_task_cids.size(), secondary_task_vids.size());
    });
    threadpool->detach_task([&]() {
        future_split_vid_task.wait();
        timer.event_start("(background) lead snode");
        int second_sockfd = sm.f_connect_wrapper(secondary_ip, secondary_port);
        TaskMapHeader header;
        header.num_clusters = secondary_task_cids.size();
        header.total_vids = secondary_task_vids.size();
        header.totalVec = totalVec;
        header.perVectorSize = p_meta.perVectorSize;
        header.vecInfoSize = vecInfoSize;
        size_t total_bytes_cids =
            header.num_clusters * sizeof(SPMerge::ClusterID);
        size_t total_bytes_vids = header.total_vids * sizeof(SPMerge::VectorID);

        f_write_wrapper(second_sockfd, &header, sizeof(header));
        f_write_wrapper(second_sockfd, secondary_task_cids.data(),
                        total_bytes_cids);
        f_write_wrapper(second_sockfd, secondary_task_vids.data(),
                        total_bytes_vids);

        LOG_INFO("Network IO between pnode and snode: %s\n",
                 bytesToString(total_bytes_cids + total_bytes_vids).c_str());
        timer.event_stop("(background) lead snode");
    });

    HelperContext context;
    std::unique_ptr<char[]> vector_buffer;
    auto future_guid_disk_io = threadpool->submit_task([&]() {
        timer.event_start("(background) guide disk IO");
        GuideDiskIO(context, vector_buffer);
        timer.event_stop("(background) guide disk IO");
    });
    auto future_init_deduper = threadpool->submit_task([&]() {
        future_split_vid_task.wait();

        SSDServing::Utils::StopW sw;
        threadpool
            ->submit_loop(0, need_secondary_vids.size(),
                          [&](size_t idx) {
                              need_secondary_vids[idx].store(
                                  false, std::memory_order_relaxed);
                          })
            .wait();
        context.visited.swap(need_secondary_vids);
        LOG_INFO("fill vector<atomic_bool> time: %f ms\n", sw.getElapsedMs());

        timer.event_start("(background) init deduper");
        InitContextDeduper(context, primary_task_vids);
        timer.event_stop("(background) init deduper");
    });

    {  // send HelpInfo1
        PacketHeader header;
        header.type = RecoveryPacketType::HelpInfo;
        header.idx = my_id;
        header.sub_size = patch_size;
        header.n = index_size;
        f_write_wrapper(fail_sockfd, &header, sizeof(header));
    }

    // send patch data
    timer.event_start("1. Send patch stage 1");
    std::vector<uint64_t> content_sub_off(p_meta.n_machine);
    std::vector<size_t> content_sub_size(p_meta.n_machine);
    pio.f_read_data(content_sub_off.data(), p_meta.n_machine * sizeof(uint64_t),
                    p_meta.clusterContentStart);
    size_t stage2_bytes = 0;
    for (SPMerge::MachineID mid = 0; mid < p_meta.n_machine; mid++) {
        if (mid != p_meta.n_machine - 1) {
            content_sub_size[mid] =
                content_sub_off[mid + 1] - content_sub_off[mid];
        } else {
            content_sub_size[mid] = p_meta.vectorIDStart -
                                    p_meta.clusterContentStart -
                                    content_sub_off[mid];
        }
        if (mid != fail_mid) stage2_bytes += content_sub_size[mid];
    }
    size_t stage1_bytes = patch_size - stage2_bytes;
    LOG_INFO("sending patch data stage1 %s\n",
             bytesToString(stage1_bytes).c_str());
    {
        PacketHeader header;
        header.type = RecoveryPacketType::PatchData;
        header.n = stage1_bytes;
        header.sub_size = stage1_bytes;

        off64_t off2 = p_meta.clusterContentStart + content_sub_off[fail_mid];
        off64_t off3 = p_meta.vectorIDStart;

        size_t bytes1 =
            p_meta.clusterContentStart + p_meta.n_machine * sizeof(uint64_t);
        size_t bytes2 = content_sub_size[fail_mid];
        size_t bytes3 = patch_size - off3;

        if (bytes1 + bytes2 + bytes3 != stage1_bytes) {
            LOG_ERROR("Stage1 bytes not match, expected: %llu, real: %llu\n",
                      stage1_bytes, bytes1 + bytes2 + bytes3);
            throw std::logic_error("");
        }

        setsockopt(fail_sockfd, IPPROTO_TCP, TCP_CORK, &ON, sizeof(ON));
        f_write_wrapper(fail_sockfd, &header, sizeof(header));  //  header
        f_sendfile64_wrapper(fail_sockfd, pfd, bytes1);  // data before content
        f_sendfile64_wrapper(fail_sockfd, pfd, bytes2, &off2);  // content data
        f_sendfile64_wrapper(fail_sockfd, pfd, bytes3,
                             &off3);  // data after content
        setsockopt(fail_sockfd, IPPROTO_TCP, TCP_CORK, &OFF, sizeof(OFF));
    }
    timer.event_stop("1. Send patch stage 1");

    timer.event_start("2. Send patch stage 2");
    if (disk_index->is_primary_node(fail_mid)) {
        LOG_INFO("sending patch data stage2 %s\n",
                 bytesToString(stage2_bytes).c_str());
        PacketHeader header;
        header.type = RecoveryPacketType::PatchData;
        header.n = stage2_bytes;
        header.sub_size = stage2_bytes;

        setsockopt(fail_sockfd, IPPROTO_TCP, TCP_CORK, &ON, sizeof(ON));
        f_write_wrapper(fail_sockfd, &header, sizeof(header));
        for (SPMerge::MachineID mid = 0; mid < p_meta.n_machine; mid++) {
            if (mid == fail_mid) continue;
            off64_t off = p_meta.clusterContentStart + content_sub_off[mid];
            f_sendfile64_wrapper(fail_sockfd, pfd, content_sub_size[mid], &off);
        }
        setsockopt(fail_sockfd, IPPROTO_TCP, TCP_CORK, &OFF, sizeof(OFF));
    }
    timer.event_stop("2. Send patch stage 2");

    SSDServing::Utils::StopW sw;
    future_guid_disk_io.wait();
    future_split_vid_task.wait();
    LOG_INFO("HelpInfo2 waiting time: %f ms\n", sw.getElapsedMs());
    {  // HelpInfo2
        PacketHeader header;
        header.type = RecoveryPacketType::HelpInfo;
        header.idx = 1;  // TODO: support multiple resuces
        header.sub_size = context.small_buffer_size;
        header.n = primary_task_vids.size() + secondary_task_vids.size();
        f_write_wrapper(fail_sockfd, &header, sizeof(header));
    }

    auto t0 = sw.getElapsedMs();
    future_init_deduper.wait();
    LOG_INFO("context init waiting time: %f ms\n", sw.getElapsedMs() - t0);

    // send data
    timer.event_start("3. Send vector data");
    LOG_INFO("sending cluster vectors %s\n",
             bytesToString(context.target_count * vecInfoSize).c_str());
    auto t1 = sw.getElapsedMs();
    std::atomic_size_t read_bytes = 0;
    // collector
    std::vector<std::future<void>> future_vec(m_options.m_numReaderThreads);
    for (int i = 0; i < m_options.m_numReaderThreads; i++) {
        auto future = threadpool->submit_task([&]() {
            auto bytes = f_collect_worker(context);
            read_bytes += bytes;
        });
        future_vec[i] = std::move(future);
    }
    LOG_INFO("Launched %lu threads to read vector data\n",
             m_options.m_numReaderThreads);
    // sender
    f_send_worker(context, fail_sockfd);
    for (auto& future : future_vec) future.wait();
    {  // check disk read rate
        auto dur = sw.getElapsedMs() - t1;
        auto rate = static_cast<size_t>(1000 * read_bytes / dur);
        LOG_INFO("<Disk Read Bandwidth> Collect Vector: %s/s\n",
                 bytesToString(rate).c_str());
    }
    {  // disk scan percentage
        float ratio = 100. * context.area_idx.load() / context.area_list.size();
        LOG_INFO("Disk scan percentage: %.1f\n", ratio);
    }
    timer.event_stop("3. Send vector data");

    // send translatemap
    timer.event_start("4. Send trasnlate map");
    const auto trans_size = std::filesystem::file_size(
        std::filesystem::path(m_options.m_indexDirectory) /
        m_options.m_headIDFile);
    LOG_INFO("sending translate map %s\n", bytesToString(trans_size).c_str());
    {
        PacketHeader header;
        header.type = RecoveryPacketType::Translate;
        header.n = trans_size / sizeof(uint64_t);
        header.sub_size = trans_size;

        setsockopt(fail_sockfd, IPPROTO_TCP, TCP_CORK, &ON, sizeof(ON));
        f_write_wrapper(fail_sockfd, &header, sizeof(header));
        f_write_wrapper(fail_sockfd, m_vectorTranslateMap.get(), trans_size);
        setsockopt(fail_sockfd, IPPROTO_TCP, TCP_CORK, &OFF, sizeof(OFF));
    }
    timer.event_stop("4. Send trasnlate map");

    LOG_INFO("Recovery data already sent\n");
    {
        PacketHeader header;
        f_read_wrapper(fail_sockfd, &header, sizeof(header));
        if (header.type != RecoveryPacketType::IAmReady) {
            LOG_ERROR("on exit, received header with type=%u, idx=%llu\n",
                      header.type, header.idx);
        }
    }

    if (del_pool) delete threadpool;

    timer.event_stop("HelpRecovery");

    timer.log_all_event_duration();
    return true;
}

template <typename T>
bool Index<T>::SecondHelpRecovery(int recv_port, std::string fail_ip,
                                  int fail_port, void* pool) {
    Helper::EventTimer timer;
    timer.add_lock();

    bool del_pool = false;
    if (pool == nullptr) {
        pool = new BS::thread_pool;
        del_pool = true;
    }
    auto threadpool = reinterpret_cast<BS::light_thread_pool*>(pool);

    timer.event_start("0.(excluded) Wait connection");

    auto disk_index =
        dynamic_cast<ExtraFullGraphSearcher<T>*>(m_extraSearcher.get());
    if (disk_index == nullptr) {
        throw std::runtime_error("disk index is not ExtraFullGraphSearcher");
    }
    if (not m_options.m_loadMergerResult) {
        throw std::logic_error("should use merger result to recover");
    }
    if (m_options.m_enablePostingListRearrange or
        m_options.m_enableDeltaEncoding or m_options.m_enableDataCompression) {
        NOT_IMPLEMENTED();
    }

    Helper::SocketManager sm;
    sm.create_listen_socket(recv_port);
    int rescue_socket = sm.f_accept_wrapper();
    timer.event_stop("0.(excluded) Wait connection");

    timer.event_start("Snode Recovery");

    timer.event_start("1. Receive pnode data");
    TaskMapHeader header;
    f_read_wrapper(rescue_socket, &header, sizeof(header));
    size_t total_bytes_cids = header.num_clusters * sizeof(SPMerge::ClusterID);
    size_t total_bytes_vids = header.total_vids * sizeof(SPMerge::VectorID);
    SPMerge::ClusterIDList secondary_process_cids(header.num_clusters);
    SPMerge::VectorIDList secondary_process_vids(header.total_vids);
    f_read_wrapper(rescue_socket, secondary_process_cids.data(),
                   total_bytes_cids);
    f_read_wrapper(rescue_socket, secondary_process_vids.data(),
                   total_bytes_vids);
    timer.event_stop("1. Receive pnode data");

    LOG_INFO("Received %llu cids and %llu vids\n",
             secondary_process_cids.size(), secondary_process_vids.size());

    timer.event_start("2. Compute IO tasks");
    HelperContext context;
    std::unique_ptr<char[]> vector_buffer;
    GuideDiskIO(context, vector_buffer);
    InitContextDeduper(context, secondary_process_vids);
    timer.event_stop("2. Compute IO tasks");

    timer.event_start("3. Send data to fail node");

    // launch collector in advance, no need to wait for connection
    SSDServing::Utils::StopW sw;
    std::atomic_size_t read_bytes = 0;
    std::vector<std::future<void>> future_vec(m_options.m_numReaderThreads);
    for (int i = 0; i < m_options.m_numReaderThreads; i++) {
        auto future = threadpool->submit_task([&]() {
            auto bytes = f_collect_worker(context);
            read_bytes += bytes;
        });
        future_vec[i] = std::move(future);
    }
    LOG_INFO("Launched %lu threads to read vector data\n",
             m_options.m_numReaderThreads);

    int fail_socket = sm.f_connect_wrapper(fail_ip, fail_port);

    LOG_INFO("sending cluster vectors %s\n",
             bytesToString(context.target_count * header.vecInfoSize).c_str());
    // sender
    f_send_worker(context, fail_socket);
    for (auto& future : future_vec) future.wait();
    {  // check disk read rate
        auto dur = sw.getElapsedMs();
        auto rate = static_cast<size_t>(1000 * read_bytes / dur);
        LOG_INFO("<Disk Read Bandwidth> Collect Vector: %s/s\n",
                 bytesToString(rate).c_str());
    }
    {  // disk scan percentage
        float ratio = 100. * context.area_idx.load() / context.area_list.size();
        LOG_INFO("Disk scan percentage: %.1f\n", ratio);
    }
    timer.event_stop("3. Send data to fail node");

    timer.event_stop("Snode Recovery");

    if (del_pool) delete threadpool;
    timer.log_all_event_duration();
    return true;
}

template <typename T>
bool Index<T>::ReceiveAndRecovery(int recv_port) {
    Helper::EventTimer timer;
    timer.add_lock();

    timer.event_start("(excluded) Prepare and wait connection");

    if (m_extraSearcher == nullptr) {
        m_extraSearcher.reset(new ExtraFullGraphSearcher<T>(m_options));
    }
    auto disk_index =
        dynamic_cast<ExtraFullGraphSearcher<T>*>(m_extraSearcher.get());
    if (disk_index == nullptr) {
        throw std::runtime_error("disk index is not ExtraFullGraphSearcher");
    }

    Helper::SocketManager sm;
    sm.create_listen_socket(recv_port);

    timer.event_stop("(excluded) Prepare and wait connection");

    int rescue_socket = sm.f_accept_wrapper();

    timer.event_start("Recovery");

    // receive HelpInfo1
    PacketHeader header;
    f_read_wrapper(rescue_socket, &header, sizeof(header));
    if (header.type != RecoveryPacketType::HelpInfo) {
        LOG_ERROR("Received header with type=%u\n", header.type);
        throw std::runtime_error("");
    }

    SPMerge::MachineID fail_mid = m_options.m_curMid;
    SPMerge::MachineID healthy_mid = header.idx;
    LOG_INFO("Primary rescue machine %u\n", healthy_mid);

    const bool is_primary = disk_index->is_primary_node(fail_mid);
    const bool split_exist = disk_index->split_exist();

    auto fail_index_file = disk_index->get_index_file_name(fail_mid, true);
    auto fail_patch_file = disk_index->get_patch_file_name(fail_mid, true);

    std::vector<std::thread> background_threads;
    background_threads.emplace_back([&, isize = header.n,
                                     psize = header.sub_size]() {
        LOG_INFO(
            "fallocating %s size for index file and %s size for patch file\n",
            bytesToString(isize).c_str(), bytesToString(psize).c_str());
        timer.event_start("(background) fallocate index file", false);
        SimpleIOHelper sio_i(std::make_shared<Helper::NativeFileIO>(),
                             fail_index_file, SimpleIOHelper::WriteOnly);
        SimpleIOHelper sio_p(std::make_shared<Helper::NativeFileIO>(),
                             fail_patch_file, SimpleIOHelper::WriteOnly);
        int ifd = sio_i.m_ptr->GetFD();
        int pfd = sio_p.m_ptr->GetFD();
        if (ftruncate(ifd, 0) != 0 or ftruncate(pfd, 0) != 0) {
            throw std::runtime_error("ftruncate failed");
        }
        fallocate(ifd, FALLOC_FL_KEEP_SIZE, 0, isize);
        if (is_primary) {
            fallocate(pfd, FALLOC_FL_KEEP_SIZE, 0, psize);
        }
        timer.event_stop("(background) fallocate index file");
    });

    const size_t AreaSize = 1024 * 1024;
    std::promise<void> prm_stage1_received;  // receive stage1 data
    std::promise<void> prm_load1_basic_done, prm_load1_done;  // load stage1
    std::promise<void> prm_vector_offset,
        prm_prepare_done;  // prepare
    auto future_load1_done = prm_load1_done.get_future().share();
    auto future_stage1_done = prm_prepare_done.get_future().share();
    background_threads.emplace_back([&]() {
        prm_stage1_received.get_future().wait();

        timer.event_start("1.2 Load index stage1");
        disk_index->LoadIndexStage1(healthy_mid, &prm_load1_basic_done);
        timer.event_stop("1.2 Load index stage1");

        prm_load1_done.set_value();
    });

    // - On common cases (no split), full vectorset will be received, so we will
    //   se an array[totalVec] to hold vectors.
    // - On case with split, #vectors will be small (≈75% for 3*2), actually we
    //   should use a map (vid->ptr/offset) with a suitable sized array,
    //   instead of allocating a large array which wastes memory.
    // But on case of 6 nodes (#vectors=75% for each), allocating a large array
    // with 25% memory wasted is still acceptable.
    const bool force_large_array = true;
    std::atomic_int stage1_remainig_work =
        (force_large_array or not split_exist) ? 1 : 2;
    auto f_stage1_work_done = [&]() {
        if (stage1_remainig_work.operator--() == 0) {
            prm_prepare_done.set_value();
            timer.event_stop("1. Stage1");
        }
    };
    background_threads.emplace_back([&]() {
        future_load1_done.wait();
        // prepare recoverying
        timer.event_start("1.3 Prepare for index storage");
        disk_index->PrepareIndexStorage(healthy_mid, AreaSize);
        timer.event_stop("1.3 Prepare for index storage");
        f_stage1_work_done();
    });
    background_threads.emplace_back([&]() {
        if (force_large_array or not split_exist) return;
        future_load1_done.wait();
        // prepare vid order
        timer.event_start("1.4 Prepare for vid->off map");
        disk_index->PrepareVectorOffset(healthy_mid);
        timer.event_stop("1.4 Prepare for vid->off map");
        prm_vector_offset.set_value();
        f_stage1_work_done();
    });

    std::promise<void> prm_stage2_received, prm_stage2_done;
    background_threads.emplace_back([&]() {
        if (is_primary) prm_stage2_received.get_future().wait();

        timer.event_start("2.2 Load index stage2");
        // if (is_primary) disk_index->LoadIndexStage2(healthy_mid);
        timer.event_stop("2.2 Load index stage2");
        // prm_load2_done.set_value();

        SSDServing::Utils::StopW sw;
        // wait for m_pmData.postingInfo
        future_stage1_done.wait();  // TODO: wait inside RecoverMetadata()
        // should be around zero
        LOG_INFO("recover metadata waiting time: %f ms\n", sw.getElapsedMs());

        timer.event_start("2.3 Recover metadata");
        disk_index->RecoverMetadata(healthy_mid);
        timer.event_stop("2.3 Recover metadata");
        prm_stage2_done.set_value();

        timer.event_stop("2. Stage2");
    });

    // Patch Stage1
    timer.event_start("1. Stage1");
    timer.event_start("1.1 Receive metadata stage1");
    disk_index->ReceiveMetadataStage1(rescue_socket, healthy_mid);
    timer.event_stop("1.1 Receive metadata stage1");
    prm_stage1_received.set_value();

    // Patch Stage2
    timer.event_start("2. Stage2");
    timer.event_start("2.1 Receive metadata stage2");
    if (is_primary) {
        disk_index->ReceiveMetadataStage2(rescue_socket, healthy_mid);
    }
    timer.event_stop("2.1 Receive metadata stage2");
    prm_stage2_received.set_value();

    // HelpInfo2
    f_read_wrapper(rescue_socket, &header, sizeof(header));
    if (header.type != RecoveryPacketType::HelpInfo) {
        LOG_ERROR("Received header with type=%u\n", header.type);
        throw std::runtime_error("");
    }
    const size_t additional_rescue_cnt = header.idx;
    const size_t receive_buffer_size = header.sub_size;
    const size_t coming_vector_cnt = header.n;
    LOG_INFO(
        "HelpInfo2 received, %lu additonal connections will be accepted, and "
        "%llu vectors will be sent\n",
        additional_rescue_cnt, coming_vector_cnt);

    // Vector Set
    timer.event_start("3. Receive vectorset");
    {
        SSDServing::Utils::StopW sw;
        prm_load1_basic_done.get_future().wait();
        if (not force_large_array and split_exist) {
            prm_vector_offset.get_future().wait();
        }
        auto t1 = sw.getElapsedMs();
        // should be around zero
        LOG_INFO("Receive vectorset waiting time: %f ms\n", t1);

        const auto& p_meta = disk_index->m_patchMeta[healthy_mid];
        const auto totalVec = p_meta.n_clusvec + p_meta.n_headvec;

        // vector buffer
        disk_index->m_pmData.vectorset.reset(
            new char[totalVec * p_meta.perVectorSize]);

        // parser threads
        std::vector<std::thread> collector_threads;
        std::unique_ptr<char[]> receive_buffer(
            new char[m_options.m_numReaderThreads * receive_buffer_size]);
        for (int i = 0; i < m_options.m_numReaderThreads; i++) {
            collector_threads.emplace_back(
                [&, i]() { disk_index->VectorCollector(i, healthy_mid); });
            char* ptr =
                receive_buffer.get() + (uint64_t)i * receive_buffer_size;
            disk_index->m_pmData.net_reader_queue.enqueue(ptr);
        }
        LOG_INFO("Launched %lu threads to parse vector data\n",
                 m_options.m_numReaderThreads);

        // receiver threads
        std::vector<std::thread> vector_receive_threads;

        // prepare for receive vector set
        std::atomic_size_t received_vectorset_bytes_total = 0;
        auto f_launch_receiver = [&](int sockfd) {
            vector_receive_threads.emplace_back([&, sockfd]() {
                int local_fd = sockfd;  // NOTE:
                if (local_fd == -1) local_fd = sm.f_accept_wrapper();
                auto bytes = disk_index->NetWorkReader(healthy_mid, local_fd,
                                                       receive_buffer_size);
                received_vectorset_bytes_total += bytes;
            });
        };
        f_launch_receiver(rescue_socket);
        for (int i = 0; i < additional_rescue_cnt; i++) f_launch_receiver(-1);
        for (auto& t : vector_receive_threads) t.join();

        auto dur = sw.getElapsedMs() - t1;
        auto bytes = received_vectorset_bytes_total.load();
        auto rate = static_cast<size_t>(1000 * bytes / dur);
        LOG_INFO("<Network Bandwidth> index data: %s/s\n",
                 bytesToString(rate).c_str());

        for (int i = 0; i < m_options.m_numReaderThreads; i++) {
            disk_index->m_pmData.collector_queue.enqueue({nullptr, 0});
        }
        for (auto& t : collector_threads) t.join();
    }
    timer.event_stop("3. Receive vectorset");

    // translate map
    std::promise<void> prm_translate_map_received;
    background_threads.emplace_back([&]() {
        // receive translate map
        timer.event_start("(background) receive and recover translate map");
        f_read_wrapper(rescue_socket, &header, sizeof(header));
        if (header.type != RecoveryPacketType::Translate) {
            LOG_ERROR("Received header with type=%u\n", header.type);
            throw std::runtime_error("");
        }
        m_vectorTranslateMap.reset(new std::uint64_t[header.n],
                                   std::default_delete<std::uint64_t[]>());
        f_read_wrapper(rescue_socket, m_vectorTranslateMap.get(),
                       header.sub_size);

        // output translatemp
        auto indexDir = std::filesystem::path(m_options.m_recoveryPath);
        auto headIDPath = indexDir / m_options.m_headIDFile;
        SimpleIOHelper sio(std::make_shared<Helper::NativeFileIO>(), headIDPath,
                           SimpleIOHelper::WriteOnly, true);
        sio.f_write_data(m_vectorTranslateMap.get(), header.sub_size);
        fsync(sio.m_ptr->GetFD());
        LOG_INFO("✓ %s\n", headIDPath.c_str());
        timer.event_stop("(background) receive and recover translate map");
        prm_translate_map_received.set_value();
    });

    // Write vector data
    timer.event_start("4. Write vector data to disk");
    {
        // SSDServing::Utils::StopW sw;
        // prm_load2_done.get_future().wait();
        // auto t1 = sw.getElapsedMs();
        // // should be around zero
        // LOG_INFO("write vector data waiting time: %f ms\n", t1);

        SSDServing::Utils::StopW sw;
        std::vector<std::thread> recovery_threads;
        std::atomic_size_t written_bytes = 0;

        for (size_t i = 0; i < m_options.m_numWriterThreads; i++) {
            recovery_threads.emplace_back([&, i]() {
                written_bytes +=
                    disk_index->VectorDataWriter(healthy_mid, i, AreaSize);
            });
        }
        LOG_INFO("Launched %lu threads to write vector data\n",
                 m_options.m_numWriterThreads);

        for (auto& t : recovery_threads) t.join();

        LOG_INFO("✓ %s\n", fail_index_file.c_str());
        LOG_INFO("✓ %s\n", fail_patch_file.c_str());

        auto dur = sw.getElapsedMs();
        auto rate = static_cast<size_t>(1000 * written_bytes / dur);
        LOG_INFO("<Disk Write Bandwidth> index data: %s/s\n",
                 bytesToString(rate).c_str());
    }
    timer.event_stop("4. Write vector data to disk");

    prm_translate_map_received.get_future().wait();
    header.type = RecoveryPacketType::IAmReady;
    f_write_wrapper(rescue_socket, &header, sizeof(header));

    future_stage1_done.wait();
    prm_stage2_done.get_future().wait();

    timer.event_stop("Recovery");

    timer.event_start("5. Wait background threads");
    sm.close_all_socket();
    disk_index->m_pmData.vector_offset.clear();
    disk_index->m_pmData.postingInfo.clear();
    disk_index->m_pmData.area_list.clear();
    disk_index->m_pmData.vectorset.reset();
    for (auto& t : background_threads) t.join();
    timer.event_stop("5. Wait background threads");

    timer.log_all_event_duration();
    return true;
}

template <typename T>
void Index<T>::LoadAndRecovery(SPMerge::MachineID healthy_mid,
                               SPMerge::MachineID fail_mid) {
    if (m_extraSearcher == nullptr) {
        m_extraSearcher.reset(new ExtraFullGraphSearcher<T>(m_options));
    }
    m_extraSearcher->LoadAndRecovery(healthy_mid, fail_mid);
}

template <typename T>
SPMerge::ClusterID Index<T>::GetNumPosting() const {
    return m_extraSearcher->GetListInfo().size();
}

template <typename T>
bool Index<T>::CheckHeadIndexType() {
    SPTAG::VectorValueType v1 = m_index->GetVectorValueType(),
                           v2 = GetEnumValueType<T>();
    if (v1 != v2) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                     "Head index and vectors don't have the same value types, "
                     "which are %s %s\n",
                     SPTAG::Helper::Convert::ConvertToString(v1).c_str(),
                     SPTAG::Helper::Convert::ConvertToString(v2).c_str());
        if (!m_pQuantizer) return false;
    }
    return true;
}

template <typename T>
void Index<T>::SetQuantizer(
    std::shared_ptr<SPTAG::COMMON::IQuantizer> quantizer) {
    m_pQuantizer = quantizer;
    if (m_pQuantizer) {
        m_fComputeDistance =
            m_pQuantizer->DistanceCalcSelector<T>(m_options.m_distCalcMethod);
        m_iBaseSquare = (m_options.m_distCalcMethod == DistCalcMethod::Cosine)
                            ? m_pQuantizer->GetBase() * m_pQuantizer->GetBase()
                            : 1;
    } else {
        m_fComputeDistance =
            COMMON::DistanceCalcSelector<T>(m_options.m_distCalcMethod);
        m_iBaseSquare = (m_options.m_distCalcMethod == DistCalcMethod::Cosine)
                            ? COMMON::Utils::GetBase<std::uint8_t>() *
                                  COMMON::Utils::GetBase<std::uint8_t>()
                            : 1;
    }
    if (m_index) {
        m_index->SetQuantizer(quantizer);
    }
}

template <typename T>
ErrorCode Index<T>::LoadConfig(Helper::IniReader& p_reader) {
    IndexAlgoType algoType = p_reader.GetParameter("Base", "IndexAlgoType",
                                                   IndexAlgoType::Undefined);
    VectorValueType valueType =
        p_reader.GetParameter("Base", "ValueType", VectorValueType::Undefined);
    if ((m_index = CreateInstance(algoType, valueType)) == nullptr)
        return ErrorCode::FailedParseValue;

    std::string sections[] = {"Base", "SelectHead", "BuildHead",
                              "BuildSSDIndex"};
    for (int i = 0; i < 4; i++) {
        auto parameters = p_reader.GetParameters(sections[i].c_str());
        for (auto iter = parameters.begin(); iter != parameters.end(); iter++) {
            SetParameter(iter->first.c_str(), iter->second.c_str(),
                         sections[i].c_str());
        }
    }

    if (m_pQuantizer) {
        m_pQuantizer->SetEnableADC(m_options.m_enableADC);
    }

    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::LoadIndexDataFromMemory(
    const std::vector<ByteArray>& p_indexBlobs) {
    m_index->SetQuantizer(m_pQuantizer);
    if (m_index->LoadIndexDataFromMemory(p_indexBlobs) != ErrorCode::Success)
        return ErrorCode::Fail;

    m_index->SetParameter("NumberOfThreads",
                          std::to_string(m_options.m_iSSDNumberOfThreads));
    // m_index->SetParameter("MaxCheck",
    // std::to_string(m_options.m_maxCheck));
    // m_index->SetParameter("HashTableExponent",
    // std::to_string(m_options.m_hashExp));
    m_index->UpdateIndex();
    m_index->SetReady(true);

    if (m_pQuantizer) {
        m_extraSearcher.reset(
            new ExtraFullGraphSearcher<std::uint8_t>(m_options));
    } else {
        m_extraSearcher.reset(new ExtraFullGraphSearcher<T>(m_options));
    }

    if (!m_extraSearcher->LoadIndex(m_options)) return ErrorCode::Fail;

    m_vectorTranslateMap.reset((std::uint64_t*)(p_indexBlobs.back().Data()),
                               [=](std::uint64_t* ptr) {});

    omp_set_num_threads(m_options.m_iSSDNumberOfThreads);
    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::LoadIndexData(
    const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams) {
    m_index->SetQuantizer(m_pQuantizer);
    if (m_index->LoadIndexData(p_indexStreams) != ErrorCode::Success)
        return ErrorCode::Fail;

    m_index->SetParameter("NumberOfThreads",
                          std::to_string(m_options.m_iSSDNumberOfThreads));
    // m_index->SetParameter("MaxCheck",
    // std::to_string(m_options.m_maxCheck));
    // m_index->SetParameter("HashTableExponent",
    // std::to_string(m_options.m_hashExp));
    m_index->UpdateIndex();
    m_index->SetReady(true);

    if (m_pQuantizer) {
        m_extraSearcher.reset(
            new ExtraFullGraphSearcher<std::uint8_t>(m_options));
    } else {
        m_extraSearcher.reset(new ExtraFullGraphSearcher<T>(m_options));
    }

    if (!m_extraSearcher->LoadIndex(m_options)) return ErrorCode::Fail;

    m_vectorTranslateMap.reset(new std::uint64_t[m_index->GetNumSamples()],
                               std::default_delete<std::uint64_t[]>());
    IOBINARY(p_indexStreams[m_index->GetIndexFiles()->size()], ReadBinary,
             sizeof(std::uint64_t) * m_index->GetNumSamples(),
             reinterpret_cast<char*>(m_vectorTranslateMap.get()));

    omp_set_num_threads(m_options.m_iSSDNumberOfThreads);
    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::SaveConfig(std::shared_ptr<Helper::DiskIO> p_configOut) {
    IOSTRING(p_configOut, WriteString, "[Base]\n");
#define DefineBasicParameter(VarName, VarType, DefaultValue, RepresentStr) \
    IOSTRING(p_configOut, WriteString,                                     \
             (RepresentStr + std::string("=") +                            \
              SPTAG::Helper::Convert::ConvertToString(m_options.VarName) + \
              std::string("\n"))                                           \
                 .c_str());

#include "inc/Core/SPANN/ParameterDefinitionList.h"
#undef DefineBasicParameter

    IOSTRING(p_configOut, WriteString, "[SelectHead]\n");
#define DefineSelectHeadParameter(VarName, VarType, DefaultValue,          \
                                  RepresentStr)                            \
    IOSTRING(p_configOut, WriteString,                                     \
             (RepresentStr + std::string("=") +                            \
              SPTAG::Helper::Convert::ConvertToString(m_options.VarName) + \
              std::string("\n"))                                           \
                 .c_str());

#include "inc/Core/SPANN/ParameterDefinitionList.h"
#undef DefineSelectHeadParameter

    IOSTRING(p_configOut, WriteString, "[BuildHead]\n");
#define DefineBuildHeadParameter(VarName, VarType, DefaultValue, RepresentStr) \
    IOSTRING(p_configOut, WriteString,                                         \
             (RepresentStr + std::string("=") +                                \
              SPTAG::Helper::Convert::ConvertToString(m_options.VarName) +     \
              std::string("\n"))                                               \
                 .c_str());

#include "inc/Core/SPANN/ParameterDefinitionList.h"
#undef DefineBuildHeadParameter

    m_index->SaveConfig(p_configOut);

    Helper::Convert::ConvertStringTo<int>(
        m_index->GetParameter("HashTableExponent").c_str(),
        m_options.m_hashExp);
    IOSTRING(p_configOut, WriteString, "[BuildSSDIndex]\n");
#define DefineSSDParameter(VarName, VarType, DefaultValue, RepresentStr)   \
    IOSTRING(p_configOut, WriteString,                                     \
             (RepresentStr + std::string("=") +                            \
              SPTAG::Helper::Convert::ConvertToString(m_options.VarName) + \
              std::string("\n"))                                           \
                 .c_str());

#include "inc/Core/SPANN/ParameterDefinitionList.h"
#undef DefineSSDParameter

    IOSTRING(p_configOut, WriteString, "\n");
    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::SaveIndexData(
    const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams) {
    if (m_index == nullptr || m_vectorTranslateMap == nullptr)
        return ErrorCode::EmptyIndex;

    ErrorCode ret;
    if ((ret = m_index->SaveIndexData(p_indexStreams)) != ErrorCode::Success)
        return ret;

    IOBINARY(p_indexStreams[m_index->GetIndexFiles()->size()], WriteBinary,
             sizeof(std::uint64_t) * m_index->GetNumSamples(),
             (char*)(m_vectorTranslateMap.get()));
    return ErrorCode::Success;
}

#pragma region K-NN search

template <typename T>
ErrorCode Index<T>::SearchIndex(QueryResult& p_query,
                                bool p_searchDeleted) const {
    if (!m_bReady) return ErrorCode::EmptyIndex;

    COMMON::QueryResultSet<T>* p_queryResults;
    if (p_query.GetResultNum() >= m_options.m_searchInternalResultNum)
        p_queryResults = (COMMON::QueryResultSet<T>*)&p_query;
    else
        p_queryResults = new COMMON::QueryResultSet<T>(
            (const T*)p_query.GetTarget(), m_options.m_searchInternalResultNum);

    m_index->SearchIndex(*p_queryResults);

    if (m_extraSearcher != nullptr) {
        auto workSpace = m_workSpaceFactory->GetWorkSpace();
        if (!workSpace) {
            workSpace.reset(new ExtraWorkSpace());
            workSpace->Initialize(m_options.m_maxCheck, m_options.m_hashExp,
                                  m_options.m_searchInternalResultNum,
                                  max(m_options.m_postingPageLimit,
                                      m_options.m_searchPostingPageLimit + 1)
                                      << PageSizeEx,
                                  m_options.m_enableDataCompression);
        } else {
            workSpace->Clear(m_options.m_searchInternalResultNum,
                             max(m_options.m_postingPageLimit,
                                 m_options.m_searchPostingPageLimit + 1)
                                 << PageSizeEx,
                             m_options.m_enableDataCompression);
        }
        workSpace->m_deduper.clear();
        workSpace->m_postingIDs.clear();

        float limitDist =
            p_queryResults->GetResult(0)->Dist * m_options.m_maxDistRatio;
        for (int i = 0; i < p_queryResults->GetResultNum(); ++i) {
            auto res = p_queryResults->GetResult(i);
            if (res->VID == -1) break;

            auto postingID = res->VID;
            res->VID =
                static_cast<SizeType>((m_vectorTranslateMap.get())[res->VID]);
            if (res->VID == MaxSize) {
                res->VID = -1;
                res->Dist = MaxDist;
            }

            // Don't do disk reads for irrelevant pages
            if (workSpace->m_postingIDs.size() >=
                    m_options.m_searchInternalResultNum ||
                (limitDist > 0.1 && res->Dist > limitDist) ||
                !m_extraSearcher->CheckValidPosting(postingID))
                continue;
            workSpace->m_postingIDs.emplace_back(postingID);
        }

        p_queryResults->Reverse();
        m_extraSearcher->SearchIndex(workSpace.get(), *p_queryResults, m_index,
                                     nullptr);
        m_workSpaceFactory->ReturnWorkSpace(std::move(workSpace));
        p_queryResults->SortResult();
    }

    if (p_query.GetResultNum() < m_options.m_searchInternalResultNum) {
        std::copy(p_queryResults->GetResults(),
                  p_queryResults->GetResults() + p_query.GetResultNum(),
                  p_query.GetResults());
        delete p_queryResults;
    }

    if (p_query.WithMeta() && nullptr != m_pMetadata) {
        for (int i = 0; i < p_query.GetResultNum(); ++i) {
            SizeType result = p_query.GetResult(i)->VID;
            p_query.SetMetadata(i, (result < 0)
                                       ? ByteArray::c_empty
                                       : m_pMetadata->GetMetadataCopy(result));
        }
    }
    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::SearchIndexIterative(QueryResult& p_headQuery,
                                         QueryResult& p_query,
                                         COMMON::WorkSpace* p_indexWorkspace,
                                         ExtraWorkSpace* p_extraWorkspace,
                                         int p_batch, int& resultCount,
                                         bool first) const {
    if (!m_bReady) return ErrorCode::EmptyIndex;

    COMMON::QueryResultSet<T>* p_headQueryResults =
        (COMMON::QueryResultSet<T>*)&p_headQuery;
    COMMON::QueryResultSet<T>* p_queryResults =
        (COMMON::QueryResultSet<T>*)&p_query;

    if (first) {
        p_headQueryResults->Reset();
        m_index->SearchIndexIterativeFromNeareast(*p_headQueryResults,
                                                  p_indexWorkspace, true);
        p_extraWorkspace->m_loadPosting = true;
    }

    bool continueSearch = true;
    resultCount = 0;
    while (continueSearch && resultCount < p_batch) {
        bool found =
            SearchDiskIndexIterative(p_headQuery, p_query, p_extraWorkspace);
        p_extraWorkspace->m_loadPosting = false;
        if (!found) {
            p_headQueryResults->Reset();
            continueSearch = m_index->SearchIndexIterativeFromNeareast(
                *p_headQueryResults, p_indexWorkspace, false);
            p_extraWorkspace->m_loadPosting = true;
        } else
            resultCount++;
    }
    p_queryResults->SortResult();

    if (p_query.WithMeta() && nullptr != m_pMetadata) {
        for (int i = 0; i < resultCount; ++i) {
            SizeType result = p_query.GetResult(i)->VID;
            p_query.SetMetadata(i, (result < 0)
                                       ? ByteArray::c_empty
                                       : m_pMetadata->GetMetadataCopy(result));
        }
    }
    return ErrorCode::Success;
}

template <typename T>
std::shared_ptr<ResultIterator> Index<T>::GetIterator(
    const void* p_target, bool p_searchDeleted) const {
    if (!m_bReady) return nullptr;
    auto extraWorkspace = m_workSpaceFactory->GetWorkSpace();
    if (!extraWorkspace) {
        extraWorkspace.reset(new ExtraWorkSpace());
        extraWorkspace->Initialize(m_options.m_maxCheck, m_options.m_hashExp,
                                   m_options.m_searchInternalResultNum,
                                   max(m_options.m_postingPageLimit,
                                       m_options.m_searchPostingPageLimit + 1)
                                       << PageSizeEx,
                                   m_options.m_enableDataCompression);
    }
    extraWorkspace->m_relaxedMono = false;
    extraWorkspace->m_loadedPostingNum = 0;
    extraWorkspace->m_deduper.clear();
    extraWorkspace->m_postingIDs.clear();
    std::shared_ptr<ResultIterator> resultIterator =
        std::make_shared<SPANNResultIterator<T>>(this, m_index.get(), p_target,
                                                 std::move(extraWorkspace),
                                                 m_options.m_headBatch);
    return resultIterator;
}

template <typename T>
ErrorCode Index<T>::SearchIndexIterativeNext(QueryResult& p_query,
                                             COMMON::WorkSpace* workSpace,
                                             int p_batch, int& resultCount,
                                             bool p_isFirst,
                                             bool p_searchDeleted) const {
    SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "ITERATIVE NOT SUPPORT FOR SPANN");
    return ErrorCode::Undefined;
}

template <typename T>
ErrorCode Index<T>::SearchIndexIterativeEnd(
    std::unique_ptr<COMMON::WorkSpace> space) const {
    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                 "SearchIndexIterativeEnd NOT SUPPORT FOR SPANN");
    return ErrorCode::Fail;
}

template <typename T>
ErrorCode Index<T>::SearchIndexIterativeEnd(
    std::unique_ptr<SPANN::ExtraWorkSpace> extraWorkspace) const {
    if (!m_bReady) return ErrorCode::EmptyIndex;

    if (extraWorkspace != nullptr)
        m_workSpaceFactory->ReturnWorkSpace(std::move(extraWorkspace));

    return ErrorCode::Success;
}

template <typename T>
bool Index<T>::SearchIndexIterativeFromNeareast(QueryResult& p_query,
                                                COMMON::WorkSpace* p_space,
                                                bool p_isFirst,
                                                bool p_searchDeleted) const {
    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                 "SearchIndexIterativeFromNeareast NOT SUPPORT FOR SPANN");
    return false;
}

template <typename T>
ErrorCode Index<T>::SearchIndexWithFilter(
    QueryResult& p_query, std::function<bool(const ByteArray&)> filterFunc,
    int maxCheck, bool p_searchDeleted) const {
    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                 "Not Support Filter on SPANN Index!\n");
    return ErrorCode::Fail;
}

template <typename T>
ErrorCode Index<T>::SearchDiskIndexPurely(QueryResult& p_query,
                                          SearchStats* p_stats) const {
    if (nullptr == m_extraSearcher) return ErrorCode::EmptyIndex;

    COMMON::QueryResultSet<T>* p_queryResults =
        (COMMON::QueryResultSet<T>*)&p_query;

    auto workSpace = m_workSpaceFactory->GetWorkSpace();
    if (!workSpace) {
        workSpace.reset(new ExtraWorkSpace());
        workSpace->Initialize(m_options.m_maxCheck, m_options.m_hashExp,
                              m_options.m_searchInternalResultNum,
                              max(m_options.m_postingPageLimit,
                                  m_options.m_searchPostingPageLimit + 1)
                                  << PageSizeEx,
                              m_options.m_enableDataCompression);
    } else {
        workSpace->Clear(m_options.m_searchInternalResultNum,
                         max(m_options.m_postingPageLimit,
                             m_options.m_searchPostingPageLimit + 1)
                             << PageSizeEx,
                         m_options.m_enableDataCompression);
    }
    workSpace->m_deduper.clear();
    workSpace->m_postingIDs.clear();

    for (int i = 0; i < p_queryResults->GetResultNum(); ++i) {
        auto res = p_queryResults->GetResult(i);
        if (res->VID == -1) break;
        if (m_extraSearcher->CheckValidPosting(res->VID)) {
            workSpace->m_postingIDs.emplace_back(res->VID);
        }
        res->VID =
            static_cast<SizeType>((m_vectorTranslateMap.get())[res->VID]);
        if (res->VID == MaxSize) {
            res->VID = -1;
            res->Dist = MaxDist;
        }
    }
    p_queryResults->Reverse();
    m_extraSearcher->SearchIndex(workSpace.get(), *p_queryResults, m_index,
                                 p_stats);
    m_workSpaceFactory->ReturnWorkSpace(std::move(workSpace));
    p_queryResults->SortResult();
    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::SearchDiskIndex(QueryResult& p_query,
                                    SearchStats* p_stats) const {
    if (nullptr == m_extraSearcher) return ErrorCode::EmptyIndex;

    COMMON::QueryResultSet<T>* p_queryResults =
        (COMMON::QueryResultSet<T>*)&p_query;

    auto workSpace = m_workSpaceFactory->GetWorkSpace();
    if (!workSpace) {
        workSpace.reset(new ExtraWorkSpace());
        workSpace->Initialize(m_options.m_maxCheck, m_options.m_hashExp,
                              m_options.m_searchInternalResultNum,
                              max(m_options.m_postingPageLimit,
                                  m_options.m_searchPostingPageLimit + 1)
                                  << PageSizeEx,
                              m_options.m_enableDataCompression);
    } else {
        workSpace->Clear(m_options.m_searchInternalResultNum,
                         max(m_options.m_postingPageLimit,
                             m_options.m_searchPostingPageLimit + 1)
                             << PageSizeEx,
                         m_options.m_enableDataCompression);
    }
    workSpace->m_deduper.clear();
    workSpace->m_postingIDs.clear();

    // {  // NOTE: for debug
    //     auto debug_file_name =
    //         "queryInternalResult" + m_options.m_indexDirectory +
    //         "_nprobe" +
    //         std::to_string(m_options.m_searchInternalResultNum) + ".txt";
    //     SimpleIOHelper sio(debug_file_name, SimpleIOHelper::ReadWrite,
    //     false, true); for (int i = 0; i <
    //     m_options.m_searchInternalResultNum; ++i)
    //     {
    //         auto res = p_queryResults->GetResult(i);
    //         if (res == nullptr) {
    //             throw std::runtime_error("res==nullptr");
    //         }
    //         sio.f_write_data(&res->VID, sizeof(res->VID));
    //         sio.f_write_data(&res->Dist, sizeof(res->Dist));
    //     }
    //     return ErrorCode::Success;
    // }

    float limitDist =
        p_queryResults->GetResult(0)->Dist * m_options.m_maxDistRatio;
    int i = 0;
    for (; i < m_options.m_searchInternalResultNum; ++i) {
        auto res = p_queryResults->GetResult(i);
        if (res->VID == -1 || (limitDist > 0.1 && res->Dist > limitDist)) break;
        if (m_extraSearcher->CheckValidPosting(res->VID)) {
            workSpace->m_postingIDs.emplace_back(res->VID);
        }
        res->VID =
            static_cast<SizeType>((m_vectorTranslateMap.get())[res->VID]);
        if (res->VID == MaxSize) {
            res->VID = -1;
            res->Dist = MaxDist;
        }
    }

    for (; i < p_queryResults->GetResultNum(); ++i) {
        auto res = p_queryResults->GetResult(i);
        if (res->VID == -1) break;
        res->VID =
            static_cast<SizeType>((m_vectorTranslateMap.get())[res->VID]);
        if (res->VID == MaxSize) {
            res->VID = -1;
            res->Dist = MaxDist;
        }
    }

    p_queryResults->Reverse();
    m_extraSearcher->SearchIndex(workSpace.get(), *p_queryResults, m_index,
                                 p_stats);
    m_workSpaceFactory->ReturnWorkSpace(std::move(workSpace));
    p_queryResults->SortResult();
    return ErrorCode::Success;
}

template <typename T>
bool Index<T>::SearchDiskIndexIterative(QueryResult& p_headQuery,
                                        QueryResult& p_query,
                                        ExtraWorkSpace* extraWorkspace) const {
    if (extraWorkspace->m_loadPosting) {
        COMMON::QueryResultSet<T>* p__headQueryResults =
            (COMMON::QueryResultSet<T>*)&p_headQuery;
        // std::shared_ptr<ExtraWorkSpace> workSpace =
        // m_workSpacePool->Rent(); workSpace->m_deduper.clear();
        extraWorkspace->m_postingIDs.clear();

        // float limitDist = p_queryResults->GetResult(0)->Dist *
        // m_options.m_maxDistRatio;

        for (int i = 0; i < p__headQueryResults->GetResultNum(); ++i) {
            auto res = p__headQueryResults->GetResult(i);
            // break or continue
            if (res->VID == -1) break;
            if (m_extraSearcher->CheckValidPosting(res->VID)) {
                extraWorkspace->m_postingIDs.emplace_back(res->VID);
            }
            res->VID =
                static_cast<SizeType>((m_vectorTranslateMap.get())[res->VID]);
            if (res->VID == MaxSize) {
                res->VID = -1;
                res->Dist = MaxDist;
            }
        }
        if (extraWorkspace->m_loadedPostingNum >=
            m_options.m_searchInternalResultNum)
            extraWorkspace->m_relaxedMono = true;
        extraWorkspace->m_loadedPostingNum +=
            (int)(extraWorkspace->m_postingIDs.size());
    }

    return m_extraSearcher->SearchIterativeNext(extraWorkspace, p_query,
                                                m_index);
}

template <typename T>
std::unique_ptr<COMMON::WorkSpace> Index<T>::RentWorkSpace(int batch) const {
    SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                 "RentWorkSpace NOT SUPPORT FOR SPANN");
    return nullptr;
}

template <typename T>
ErrorCode Index<T>::DebugSearchDiskIndex(
    QueryResult& p_query, int p_subInternalResultNum, int p_internalResultNum,
    SearchStats* p_stats, std::set<int>* truth,
    std::map<int, std::set<int>>* found) const {
    if (nullptr == m_extraSearcher) return ErrorCode::EmptyIndex;

    COMMON::QueryResultSet<T> newResults(
        *((COMMON::QueryResultSet<T>*)&p_query));
    for (int i = 0; i < newResults.GetResultNum(); ++i) {
        auto res = newResults.GetResult(i);
        if (res->VID == -1) break;

        auto global_VID =
            static_cast<SizeType>((m_vectorTranslateMap.get())[res->VID]);
        if (truth && truth->count(global_VID))
            (*found)[res->VID].insert(global_VID);
        res->VID = global_VID;
        if (res->VID == MaxSize) {
            res->VID = -1;
            res->Dist = MaxDist;
        }
    }
    newResults.Reverse();

    auto workSpace = m_workSpaceFactory->GetWorkSpace();
    if (!workSpace) {
        workSpace.reset(new ExtraWorkSpace());
        workSpace->Initialize(m_options.m_maxCheck, m_options.m_hashExp,
                              m_options.m_searchInternalResultNum,
                              max(m_options.m_postingPageLimit,
                                  m_options.m_searchPostingPageLimit + 1)
                                  << PageSizeEx,
                              m_options.m_enableDataCompression);
    } else {
        workSpace->Clear(m_options.m_searchInternalResultNum,
                         max(m_options.m_postingPageLimit,
                             m_options.m_searchPostingPageLimit + 1)
                             << PageSizeEx,
                         m_options.m_enableDataCompression);
    }
    workSpace->m_deduper.clear();

    int partitions = (p_internalResultNum + p_subInternalResultNum - 1) /
                     p_subInternalResultNum;
    float limitDist = p_query.GetResult(0)->Dist * m_options.m_maxDistRatio;
    for (SizeType p = 0; p < partitions; p++) {
        int subInternalResultNum =
            min(p_subInternalResultNum,
                p_internalResultNum - p_subInternalResultNum * p);

        workSpace->m_postingIDs.clear();

        for (int i = p * p_subInternalResultNum;
             i < p * p_subInternalResultNum + subInternalResultNum; i++) {
            auto res = p_query.GetResult(i);
            if (res->VID == -1 || (limitDist > 0.1 && res->Dist > limitDist))
                break;
            if (!m_extraSearcher->CheckValidPosting(res->VID)) continue;
            workSpace->m_postingIDs.emplace_back(res->VID);
        }

        m_extraSearcher->SearchIndex(workSpace.get(), newResults, m_index,
                                     p_stats, truth, found);
    }
    m_workSpaceFactory->ReturnWorkSpace(std::move(workSpace));
    newResults.SortResult();
    std::copy(newResults.GetResults(),
              newResults.GetResults() + newResults.GetResultNum(),
              p_query.GetResults());
    return ErrorCode::Success;
}
#pragma endregion

template <typename T>
ErrorCode Index<T>::GetPostingDebug(SizeType vid, std::vector<SizeType>& VIDs,
                                    std::shared_ptr<VectorSet>& vecs) {
    VIDs.clear();
    if (!m_extraSearcher) return ErrorCode::EmptyIndex;
    if (!m_extraSearcher->CheckValidPosting(vid)) return ErrorCode::Fail;

    auto workSpace = m_workSpaceFactory->GetWorkSpace();
    if (!workSpace) {
        workSpace.reset(new ExtraWorkSpace());
        workSpace->Initialize(m_options.m_maxCheck, m_options.m_hashExp,
                              m_options.m_searchInternalResultNum,
                              max(m_options.m_postingPageLimit,
                                  m_options.m_searchPostingPageLimit + 1)
                                  << PageSizeEx,
                              m_options.m_enableDataCompression);
    } else {
        workSpace->Clear(m_options.m_searchInternalResultNum,
                         max(m_options.m_postingPageLimit,
                             m_options.m_searchPostingPageLimit + 1)
                             << PageSizeEx,
                         m_options.m_enableDataCompression);
    }
    workSpace->m_deduper.clear();

    auto out = m_extraSearcher->GetPostingDebug(workSpace.get(), m_index, vid,
                                                VIDs, vecs);
    m_workSpaceFactory->ReturnWorkSpace(std::move(workSpace));
    return out;
}

template <typename T>
ErrorCode Index<T>::GetPostingList(
    const SPMerge::ClusterIDList& cid_list,
    SPMerge::ClusterContentList& cluster_content,
    std::function<void(uint64_t, void*)> f_handle_vector) const {
    if (!m_extraSearcher) return ErrorCode::EmptyIndex;

    auto workSpace = m_workSpaceFactory->GetWorkSpace();
    if (!workSpace) {
        workSpace.reset(new ExtraWorkSpace());
        workSpace->Initialize(m_options.m_maxCheck, m_options.m_hashExp,
                              cid_list.size(),
                              max(m_options.m_postingPageLimit,
                                  m_options.m_searchPostingPageLimit + 1)
                                  << PageSizeEx,
                              m_options.m_enableDataCompression);
    } else {
        workSpace->Clear(cid_list.size(),
                         max(m_options.m_postingPageLimit,
                             m_options.m_searchPostingPageLimit + 1)
                             << PageSizeEx,
                         m_options.m_enableDataCompression);
    }
    workSpace->m_deduper.clear();

    auto out = m_extraSearcher->GetPostingList(
        workSpace.get(), m_index, cid_list, cluster_content, f_handle_vector);
    m_workSpaceFactory->ReturnWorkSpace(std::move(workSpace));
    return out;
}

template <typename T>
void Index<T>::SelectHeadAdjustOptions(int p_vectorCount) {
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Begin Adjust Parameters...\n");

    if (m_options.m_headVectorCount != 0)
        m_options.m_ratio = m_options.m_headVectorCount * 1.0 / p_vectorCount;
    int headCnt =
        static_cast<int>(std::round(m_options.m_ratio * p_vectorCount));
    if (headCnt == 0) {
        for (double minCnt = 1; headCnt == 0; minCnt += 0.2) {
            m_options.m_ratio = minCnt / p_vectorCount;
            headCnt =
                static_cast<int>(std::round(m_options.m_ratio * p_vectorCount));
        }

        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "Setting requires to select none vectors as head, "
                     "adjusted it to %d vectors\n",
                     headCnt);
    }

    if (m_options.m_iBKTKmeansK > headCnt) {
        m_options.m_iBKTKmeansK = headCnt;
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "Setting of cluster number is less than head count, "
                     "adjust it to %d\n",
                     headCnt);
    }

    if (m_options.m_selectThreshold == 0) {
        m_options.m_selectThreshold =
            min(p_vectorCount - 1, static_cast<int>(1 / m_options.m_ratio));
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Set SelectThreshold to %d\n",
                     m_options.m_selectThreshold);
    }

    if (m_options.m_splitThreshold == 0) {
        m_options.m_splitThreshold =
            min(p_vectorCount - 1,
                static_cast<int>(m_options.m_selectThreshold * 2));
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Set SplitThreshold to %d\n",
                     m_options.m_splitThreshold);
    }

    if (m_options.m_splitFactor == 0) {
        m_options.m_splitFactor =
            min(p_vectorCount - 1,
                static_cast<int>(std::round(1 / m_options.m_ratio) + 0.5));
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Set SplitFactor to %d\n",
                     m_options.m_splitFactor);
    }
}

template <typename T>
int Index<T>::SelectHeadDynamicallyInternal(
    const std::shared_ptr<COMMON::BKTree> p_tree, int p_nodeID,
    const Options& p_opts, std::vector<int>& p_selected) {
    typedef std::pair<int, int> CSPair;
    std::vector<CSPair> children;
    int childrenSize = 1;
    const auto& node = (*p_tree)[p_nodeID];
    if (node.childStart >= 0) {
        children.reserve(node.childEnd - node.childStart);
        for (int i = node.childStart; i < node.childEnd; ++i) {
            int cs =
                SelectHeadDynamicallyInternal(p_tree, i, p_opts, p_selected);
            if (cs > 0) {
                children.emplace_back(i, cs);
                childrenSize += cs;
            }
        }
    }

    if (childrenSize >= p_opts.m_selectThreshold) {
        if (node.centerid < (*p_tree)[0].centerid) {
            p_selected.push_back(node.centerid);
        }

        if (childrenSize > p_opts.m_splitThreshold) {
            std::sort(children.begin(), children.end(),
                      [](const CSPair& a, const CSPair& b) {
                          return a.second > b.second;
                      });

            size_t selectCnt = static_cast<size_t>(
                std::ceil(childrenSize * 1.0 / p_opts.m_splitFactor) + 0.5);
            // if (selectCnt > 1) selectCnt -= 1;
            for (size_t i = 0; i < selectCnt && i < children.size(); ++i) {
                p_selected.push_back((*p_tree)[children[i].first].centerid);
            }
        }

        return 0;
    }

    return childrenSize;
}

template <typename T>
void Index<T>::SelectHeadDynamically(
    const std::shared_ptr<COMMON::BKTree> p_tree, int p_vectorCount,
    std::vector<int>& p_selected) {
    p_selected.clear();
    p_selected.reserve(p_vectorCount);

    if (static_cast<int>(std::round(m_options.m_ratio * p_vectorCount)) >=
        p_vectorCount) {
        for (int i = 0; i < p_vectorCount; ++i) {
            p_selected.push_back(i);
        }

        return;
    }
    Options opts = m_options;

    int selectThreshold = m_options.m_selectThreshold;
    int splitThreshold = m_options.m_splitThreshold;

    double minDiff = 100;
    for (int select = 2; select <= m_options.m_selectThreshold; ++select) {
        opts.m_selectThreshold = select;
        opts.m_splitThreshold = m_options.m_splitThreshold;

        int l = m_options.m_splitFactor;
        int r = m_options.m_splitThreshold;

        while (l < r - 1) {
            opts.m_splitThreshold = (l + r) / 2;
            p_selected.clear();

            SelectHeadDynamicallyInternal(p_tree, 0, opts, p_selected);
            std::sort(p_selected.begin(), p_selected.end());
            p_selected.erase(std::unique(p_selected.begin(), p_selected.end()),
                             p_selected.end());

            double diff =
                static_cast<double>(p_selected.size()) / p_vectorCount -
                m_options.m_ratio;

            SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                         "Select Threshold: %d, Split Threshold: %d, diff: "
                         "%.2lf%%.\n",
                         opts.m_selectThreshold, opts.m_splitThreshold,
                         diff * 100.0);

            if (minDiff > fabs(diff)) {
                minDiff = fabs(diff);

                selectThreshold = opts.m_selectThreshold;
                splitThreshold = opts.m_splitThreshold;
            }

            if (diff > 0) {
                l = (l + r) / 2;
            } else {
                r = (l + r) / 2;
            }
        }
    }

    opts.m_selectThreshold = selectThreshold;
    opts.m_splitThreshold = splitThreshold;

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                 "Final Select Threshold: %d, Split Threshold: %d.\n",
                 opts.m_selectThreshold, opts.m_splitThreshold);

    p_selected.clear();
    SelectHeadDynamicallyInternal(p_tree, 0, opts, p_selected);
    std::sort(p_selected.begin(), p_selected.end());
    p_selected.erase(std::unique(p_selected.begin(), p_selected.end()),
                     p_selected.end());
}

template <typename T>
template <typename InternalDataType>
bool Index<T>::SelectHeadInternal(
    std::shared_ptr<Helper::VectorSetReader>& p_reader) {
    std::shared_ptr<VectorSet> vectorset = p_reader->GetVectorSet();
    if (m_options.m_distCalcMethod == DistCalcMethod::Cosine &&
        !p_reader->IsNormalized())
        vectorset->Normalize(m_options.m_iSelectHeadNumberOfThreads);
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Begin initial data (%d,%d)...\n",
                 vectorset->Count(), vectorset->Dimension());

    COMMON::Dataset<InternalDataType> data(
        vectorset->Count(), vectorset->Dimension(), vectorset->Count(),
        vectorset->Count() + 1, (InternalDataType*)vectorset->GetData());

    auto t1 = std::chrono::high_resolution_clock::now();
    SelectHeadAdjustOptions(data.R());
    std::vector<int> selected;
    if (data.R() == 1) {
        selected.push_back(0);
    } else if (Helper::StrUtils::StrEqualIgnoreCase(
                   m_options.m_selectType.c_str(), "Random")) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "Start generating Random head.\n");
        selected.resize(data.R());
        for (int i = 0; i < data.R(); i++) selected[i] = i;
        std::shuffle(selected.begin(), selected.end(), rg);
        int headCnt =
            static_cast<int>(std::round(m_options.m_ratio * data.R()));
        selected.resize(headCnt);
    } else if (Helper::StrUtils::StrEqualIgnoreCase(
                   m_options.m_selectType.c_str(), "BKT")) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start generating BKT.\n");
        std::shared_ptr<COMMON::BKTree> bkt =
            std::make_shared<COMMON::BKTree>();
        bkt->m_iBKTKmeansK = m_options.m_iBKTKmeansK;
        bkt->m_iBKTLeafSize = m_options.m_iBKTLeafSize;
        bkt->m_iSamples = m_options.m_iSamples;
        bkt->m_iTreeNumber = m_options.m_iTreeNumber;
        bkt->m_fBalanceFactor = m_options.m_fBalanceFactor;
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Start invoking BuildTrees.\n");
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "BKTKmeansK: %d, BKTLeafSize: %d, Samples: %d, "
                     "BKTLambdaFactor:%f TreeNumber: %d, ThreadNum: %d.\n",
                     bkt->m_iBKTKmeansK, bkt->m_iBKTLeafSize, bkt->m_iSamples,
                     bkt->m_fBalanceFactor, bkt->m_iTreeNumber,
                     m_options.m_iSelectHeadNumberOfThreads);

        bkt->BuildTrees<InternalDataType>(
            data, m_options.m_distCalcMethod,
            m_options.m_iSelectHeadNumberOfThreads, nullptr, nullptr, true);
        auto t2 = std::chrono::high_resolution_clock::now();
        double elapsedSeconds =
            std::chrono::duration_cast<std::chrono::seconds>(t2 - t1).count();
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "End invoking BuildTrees.\n");
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "Invoking BuildTrees used time: %.2lf minutes (about "
                     "%.2lf hours).\n",
                     elapsedSeconds / 60.0, elapsedSeconds / 3600.0);

        if (m_options.m_saveBKT) {
            std::stringstream bktFileNameBuilder;
            bktFileNameBuilder
                << m_options.m_vectorPath << ".bkt." << m_options.m_iBKTKmeansK
                << "_" << m_options.m_iBKTLeafSize << "_"
                << m_options.m_iTreeNumber << "_" << m_options.m_iSamples << "_"
                << static_cast<int>(m_options.m_distCalcMethod) << ".bin";
            bkt->SaveTrees(bktFileNameBuilder.str());
        }
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Finish generating BKT.\n");

        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "Start selecting nodes...Select Head Dynamically...\n");
        SelectHeadDynamically(bkt, data.R(), selected);

        if (selected.empty()) {
            SPTAGLIB_LOG(
                Helper::LogLevel::LL_Error,
                "Can't select any vector as head with current settings\n");
            return false;
        }
    }

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                 "Seleted Nodes: %u, about %.2lf%% of total.\n",
                 static_cast<unsigned int>(selected.size()),
                 selected.size() * 100.0 / data.R());

    if (!m_options.m_noOutput) {
        std::sort(selected.begin(), selected.end());

        std::shared_ptr<Helper::DiskIO> output = SPTAG::f_createIO(),
                                        outputIDs = SPTAG::f_createIO();
        if (output == nullptr || outputIDs == nullptr ||
            !output->Initialize((m_options.m_indexDirectory + FolderSep +
                                 m_options.m_headVectorFile)
                                    .c_str(),
                                std::ios::binary | std::ios::out) ||
            !outputIDs->Initialize((m_options.m_indexDirectory + FolderSep +
                                    m_options.m_headIDFile)
                                       .c_str(),
                                   std::ios::binary | std::ios::out)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Failed to create output file:%s %s\n",
                         (m_options.m_indexDirectory + FolderSep +
                          m_options.m_headVectorFile)
                             .c_str(),
                         (m_options.m_indexDirectory + FolderSep +
                          m_options.m_headIDFile)
                             .c_str());
            return false;
        }

        SizeType val = static_cast<SizeType>(selected.size());
        if (output->WriteBinary(sizeof(val), reinterpret_cast<char*>(&val)) !=
            sizeof(val)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Failed to write output file!\n");
            return false;
        }
        DimensionType dt = data.C();
        if (output->WriteBinary(sizeof(dt), reinterpret_cast<char*>(&dt)) !=
            sizeof(dt)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Failed to write output file!\n");
            return false;
        }

        for (int i = 0; i < selected.size(); i++) {
            uint64_t vid = static_cast<uint64_t>(selected[i]);
            if (outputIDs->WriteBinary(sizeof(vid), reinterpret_cast<char*>(
                                                        &vid)) != sizeof(vid)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to write output file!\n");
                return false;
            }

            if (output->WriteBinary(sizeof(InternalDataType) * data.C(),
                                    (char*)(data[vid])) !=
                sizeof(InternalDataType) * data.C()) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to write output file!\n");
                return false;
            }
        }
    }
    auto t3 = std::chrono::high_resolution_clock::now();
    double elapsedSeconds =
        std::chrono::duration_cast<std::chrono::seconds>(t3 - t1).count();
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                 "Total used time: %.2lf minutes (about %.2lf hours).\n",
                 elapsedSeconds / 60.0, elapsedSeconds / 3600.0);
    return true;
}

template <typename T>
ErrorCode Index<T>::BuildIndexInternal(
    std::shared_ptr<Helper::VectorSetReader>& p_reader) {
    if (!m_options.m_indexDirectory.empty()) {
        if (!direxists(m_options.m_indexDirectory.c_str())) {
            mkdir(m_options.m_indexDirectory.c_str());
        }
    }

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Begin Select Head...\n");
    auto t1 = std::chrono::high_resolution_clock::now();
    if (m_options.m_selectHead) {
        omp_set_num_threads(m_options.m_iSelectHeadNumberOfThreads);
        bool success = false;
        if (m_pQuantizer) {
            success = SelectHeadInternal<std::uint8_t>(p_reader);
        } else {
            success = SelectHeadInternal<T>(p_reader);
        }
        if (!success) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "SelectHead Failed!\n");
            return ErrorCode::Fail;
        }
    }
    auto t2 = std::chrono::high_resolution_clock::now();
    double selectHeadTime =
        std::chrono::duration_cast<std::chrono::seconds>(t2 - t1).count();
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "select head time: %.2lfs\n",
                 selectHeadTime);

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Begin Build Head...\n");
    if (m_options.m_buildHead) {
        auto valueType = m_pQuantizer ? SPTAG::VectorValueType::UInt8
                                      : m_options.m_valueType;
        auto dims =
            m_pQuantizer ? m_pQuantizer->GetNumSubvectors() : m_options.m_dim;

        m_index = SPTAG::VectorIndex::CreateInstance(m_options.m_indexAlgoType,
                                                     valueType);
        m_index->SetParameter("DistCalcMethod",
                              SPTAG::Helper::Convert::ConvertToString(
                                  m_options.m_distCalcMethod));
        m_index->SetQuantizer(m_pQuantizer);
        for (const auto& iter : m_headParameters) {
            m_index->SetParameter(iter.first.c_str(), iter.second.c_str());
        }

        std::shared_ptr<Helper::ReaderOptions> vectorOptions(
            new Helper::ReaderOptions(valueType, dims,
                                      VectorFileType::DEFAULT));
        auto vectorReader =
            Helper::VectorSetReader::CreateInstance(vectorOptions);
        if (ErrorCode::Success !=
            vectorReader->LoadFile(m_options.m_indexDirectory + FolderSep +
                                   m_options.m_headVectorFile)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Failed to read head vector file.\n");
            return ErrorCode::Fail;
        }
        {
            auto headvectorset = vectorReader->GetVectorSet();
            if (m_index->BuildIndex(headvectorset, nullptr, false, true,
                                    true) != ErrorCode::Success) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to build head index.\n");
                return ErrorCode::Fail;
            }
            m_index->SetQuantizerFileName(m_options.m_quantizerFilePath.substr(
                m_options.m_quantizerFilePath.find_last_of("/\\") + 1));
            if (m_index->SaveIndex(m_options.m_indexDirectory + FolderSep +
                                   m_options.m_headIndexFolder) !=
                ErrorCode::Success) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to save head index.\n");
                return ErrorCode::Fail;
            }
        }
        m_index.reset();
        if (LoadIndex(m_options.m_indexDirectory + FolderSep +
                          m_options.m_headIndexFolder,
                      m_index) != ErrorCode::Success) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Cannot load head index from %s!\n",
                         (m_options.m_indexDirectory + FolderSep +
                          m_options.m_headIndexFolder)
                             .c_str());
        }
    }
    auto t3 = std::chrono::high_resolution_clock::now();
    double buildHeadTime =
        std::chrono::duration_cast<std::chrono::seconds>(t3 - t2).count();
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                 "select head time: %.2lfs build head time: %.2lfs\n",
                 selectHeadTime, buildHeadTime);

    SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Begin Build SSDIndex...\n");
    if (m_options.m_enableSSD) {
        omp_set_num_threads(m_options.m_iSSDNumberOfThreads);

        if (m_index == nullptr &&
            LoadIndex(m_options.m_indexDirectory + FolderSep +
                          m_options.m_headIndexFolder,
                      m_index) != ErrorCode::Success) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Cannot load head index from %s!\n",
                         (m_options.m_indexDirectory + FolderSep +
                          m_options.m_headIndexFolder)
                             .c_str());
            return ErrorCode::Fail;
        }
        m_index->SetQuantizer(m_pQuantizer);
        if (!CheckHeadIndexType()) return ErrorCode::Fail;

        m_index->SetParameter("NumberOfThreads",
                              std::to_string(m_options.m_iSSDNumberOfThreads));
        m_index->SetParameter("MaxCheck", std::to_string(m_options.m_maxCheck));
        m_index->SetParameter("HashTableExponent",
                              std::to_string(m_options.m_hashExp));
        m_index->UpdateIndex();

        if (m_pQuantizer) {
            m_extraSearcher.reset(
                new ExtraFullGraphSearcher<std::uint8_t>(m_options));
        } else {
            m_extraSearcher.reset(new ExtraFullGraphSearcher<T>(m_options));
        }

        if (m_options.m_buildSsdIndex) {
            if (!m_options.m_excludehead) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                             "Include all vectors into SSD index...\n");
                std::shared_ptr<Helper::DiskIO> ptr = SPTAG::f_createIO();
                if (ptr == nullptr ||
                    !ptr->Initialize((m_options.m_indexDirectory + FolderSep +
                                      m_options.m_headIDFile)
                                         .c_str(),
                                     std::ios::binary | std::ios::out)) {
                    SPTAGLIB_LOG(
                        Helper::LogLevel::LL_Error,
                        "Failed to open headIDFile file:%s for overwrite\n",
                        (m_options.m_indexDirectory + FolderSep +
                         m_options.m_headIDFile)
                            .c_str());
                    return ErrorCode::Fail;
                }
                std::uint64_t vid = (std::uint64_t)MaxSize;
                for (int i = 0; i < m_index->GetNumSamples(); i++) {
                    IOBINARY(ptr, WriteBinary, sizeof(std::uint64_t),
                             (char*)(&vid));
                }
            }

            if (!m_extraSearcher->BuildIndex(p_reader, m_index, m_options)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "BuildSSDIndex Failed!\n");
                return ErrorCode::Fail;
            }
        }
        if (!m_extraSearcher->LoadIndex(m_options)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error, "Cannot Load SSDIndex!\n");
            if (m_options.m_buildSsdIndex) {
                return ErrorCode::Fail;
            } else {
                m_extraSearcher.reset();
            }
        }

        if (m_extraSearcher != nullptr) {
            m_vectorTranslateMap.reset(
                new std::uint64_t[m_index->GetNumSamples()],
                std::default_delete<std::uint64_t[]>());
            std::shared_ptr<Helper::DiskIO> ptr = SPTAG::f_createIO();
            if (ptr == nullptr ||
                !ptr->Initialize((m_options.m_indexDirectory + FolderSep +
                                  m_options.m_headIDFile)
                                     .c_str(),
                                 std::ios::binary | std::ios::in)) {
                SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                             "Failed to open headIDFile file:%s\n",
                             (m_options.m_indexDirectory + FolderSep +
                              m_options.m_headIDFile)
                                 .c_str());
                return ErrorCode::Fail;
            }
            IOBINARY(ptr, ReadBinary,
                     sizeof(std::uint64_t) * m_index->GetNumSamples(),
                     (char*)(m_vectorTranslateMap.get()));
        }
    }
    auto t4 = std::chrono::high_resolution_clock::now();
    double buildSSDTime =
        std::chrono::duration_cast<std::chrono::seconds>(t4 - t3).count();
    SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                 "select head time: %.2lfs build head time: %.2lfs build ssd "
                 "time: %.2lfs\n",
                 selectHeadTime, buildHeadTime, buildSSDTime);

    if (m_options.m_deleteHeadVectors) {
        if (fileexists((m_options.m_indexDirectory + FolderSep +
                        m_options.m_headVectorFile)
                           .c_str()) &&
            remove((m_options.m_indexDirectory + FolderSep +
                    m_options.m_headVectorFile)
                       .c_str()) != 0) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Warning,
                         "Head vector file can't be removed.\n");
        }
    }

    m_bReady = true;
    return ErrorCode::Success;
}
template <typename T>
ErrorCode Index<T>::BuildIndex(bool p_normalized) {
    SPTAG::VectorValueType valueType =
        m_pQuantizer ? SPTAG::VectorValueType::UInt8 : m_options.m_valueType;
    SizeType dim =
        m_pQuantizer ? m_pQuantizer->GetNumSubvectors() : m_options.m_dim;
    std::shared_ptr<Helper::ReaderOptions> vectorOptions(
        new Helper::ReaderOptions(
            valueType, dim, m_options.m_vectorType, m_options.m_vectorDelimiter,
            m_options.m_iSSDNumberOfThreads, p_normalized));
    auto vectorReader = Helper::VectorSetReader::CreateInstance(vectorOptions);
    if (m_options.m_vectorPath.empty()) {
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info,
                     "Vector file is empty. Skipping loading.\n");
    } else {
        if (ErrorCode::Success !=
            vectorReader->LoadFile(m_options.m_vectorPath)) {
            SPTAGLIB_LOG(Helper::LogLevel::LL_Error,
                         "Failed to read vector file.\n");
            return ErrorCode::Fail;
        }
        m_options.m_vectorSize = vectorReader->GetVectorSet()->Count();
    }

    return BuildIndexInternal(vectorReader);
}

template <typename T>
void Index<T>::LoadSSDIndex() {
    m_extraSearcher.reset(new ExtraFullGraphSearcher<T>(m_options));
    if (!m_extraSearcher->LoadIndex(m_options)) {
        throw std::runtime_error("Cannot load SSD index!");
    }
    LOG_INFO("SSD index loaded.\n");

    m_index.reset(new VoidIndex<T>(this));

    auto indexDir = std::filesystem::path(m_options.m_indexDirectory);
    auto headIDPath = indexDir / m_options.m_headIDFile;
    auto filesize = std::filesystem::file_size(headIDPath);
    auto numSamples = filesize / sizeof(uint64_t);

    m_vectorTranslateMap.reset(new std::uint64_t[numSamples],
                               std::default_delete<std::uint64_t[]>());
    SimpleIOHelper sio(headIDPath, SimpleIOHelper::ReadOnly);
    sio.f_read_data(m_vectorTranslateMap.get(), filesize);
    LOG_INFO("%d head vid loaded.\n", numSamples);
}
template <typename T>
void Index<T>::LoadHeadIndex() {
    auto head_index_dir =
        m_options.m_indexDirectory + FolderSep + m_options.m_headIndexFolder;
    if (LoadIndex(head_index_dir, m_index) != ErrorCode::Success) {
        LOG_ERROR("Cannot load head index from %s!\n", head_index_dir.c_str());
        throw std::runtime_error("");
    }
    if (!CheckHeadIndexType()) throw std::runtime_error("");

    m_index->SetParameter("NumberOfThreads",
                          std::to_string(m_options.m_iSSDNumberOfThreads));
    m_index->SetParameter("MaxCheck", std::to_string(m_options.m_maxCheck));
    m_index->SetParameter("HashTableExponent",
                          std::to_string(m_options.m_hashExp));
    m_index->UpdateIndex();

    auto indexDir = std::filesystem::path(m_options.m_indexDirectory);
    auto headIDPath = indexDir / m_options.m_headIDFile;
    auto filesize = std::filesystem::file_size(headIDPath);
    auto numSamples = filesize / sizeof(uint64_t);

    LOG_INFO("loading translate map: %s...\n", headIDPath.c_str());
    m_vectorTranslateMap.reset(new std::uint64_t[numSamples],
                               std::default_delete<std::uint64_t[]>());
    SimpleIOHelper sio(headIDPath, SimpleIOHelper::ReadOnly);
    sio.f_read_data(m_vectorTranslateMap.get(), filesize);
    LOG_INFO("%d head vid loaded.\n", numSamples);
}
template <typename T>
void Index<T>::LoadPatchMeta() {
    auto disk_index = new ExtraFullGraphSearcher<T>(m_options);
    m_extraSearcher.reset(disk_index);

    LOG_INFO("loading patch meta...\n");
    disk_index->m_patchMeta.resize(m_options.m_numMachine);
    std::vector<SPANNCID> tmp_vec;
    disk_index->LoadingPatchMeta(m_options.m_curMid, tmp_vec);
}

template <typename T>
void Index<T>::PruneClusters(QueryResult& p_query, QueryResult& local,
                             SearchStats& stat, Ort::Session* session) {
    // split result to 2 part
    if (m_options.m_searchPruning) {
        FrequencyPrunig(p_query, local, stat, session);
    } else {
        NormalPrunig(p_query, local, stat);
    }

    assert(p_query.GetResultNum() <= m_options.m_searchInternalResultNum);

    // transfer cid to vid
    int local_idx = 0;
    for (int i = 0; i < local.GetResultNum(); i++) {
        auto cid = local.GetResult(i)->VID;
        auto dist = local.GetResult(i)->Dist;
        auto vid = static_cast<SizeType>((m_vectorTranslateMap.get())[cid]);
        if (vid == MaxSize) continue;
        local.SetResult(local_idx++, vid, dist);
    }
    local.SetResultNum(local_idx);
}

template <typename T>
void Index<T>::NormalPrunig(QueryResult& p_query, QueryResult& local,
                            SearchStats& stat) {
    SSDServing::Utils::StopW sw;
    const auto result_num = p_query.GetResultNum();
    const auto nprobe = m_options.m_searchInternalResultNum;
    float limitDist = p_query.GetResult(0)->Dist * m_options.m_maxDistRatio;
    int local_idx = 0, send_idx = 0;
    for (int i = 0; i < result_num; i++) {
        auto cid = p_query.GetResult(i)->VID;
        auto dist = p_query.GetResult(i)->Dist;
        if (cid == -1) break;
        if (send_idx == nprobe or (limitDist > 0.1 and dist > limitDist)) {
            local.SetResult(local_idx++, cid, dist);
        } else {
            p_query.SetResult(send_idx++, cid, dist);
        }
    }
    p_query.SetResultNum(send_idx);
    local.SetResultNum(local_idx);

    stat.m_pruning_total = sw.getElapsedMs();
}

template <typename T>
void Index<T>::CollectInfo(
    QueryResult& p_query, std::vector<SPMerge::VecFreqInfo>& vec_info_storage,
    absl::flat_hash_map<SPMerge::VectorID, int>& vec_info,
    std::vector<double>& len_list) {
    NOT_IMPLEMENTED();  // now we don't load error_machine_cids by default
    auto disk_index =
        static_cast<ExtraFullGraphSearcher<T>*>(m_extraSearcher.get());

    const auto result_num = p_query.GetResultNum();
    const auto nprobe = m_options.m_searchInternalResultNum;
    const auto& cluster_content = disk_index->GetMergerResult().cluster_content;
    const auto& error_machine_cids = disk_index->m_error_machine_cids;

    vec_info_storage.resize(result_num * m_options.m_postingVectorLimit);
    vec_info.reserve(result_num * m_options.m_postingVectorLimit * .7);
    len_list.reserve(result_num);

    int storage_idx = 0;
    // normal prune
    float limitDist = p_query.GetResult(0)->Dist * m_options.m_maxDistRatio;
    // traverse
    int cidx = 0;
    for (int idx = 0; idx < result_num and cidx < nprobe; idx++) {
        auto cid = p_query.GetResult(idx)->VID;
        auto dist = p_query.GetResult(idx)->Dist;

        if (error_machine_cids.count(cid)) continue;

        if (cid == -1) break;
        if (limitDist > 0.1 && dist > limitDist) break;

        cidx++;

        auto iter = cluster_content.find(cid);
        // may not exist, e.g. empty cluster
        if (iter == cluster_content.end()) {
            continue;
        }
        const auto& vid_list = iter->second;
        len_list.push_back(vid_list.size());
        for (auto vid : vid_list) {
            auto [it, inserted] = vec_info.try_emplace(vid, storage_idx);
            if (inserted) {
                vec_info_storage[storage_idx].first_idx = idx;
                storage_idx++;
            } else {
                vec_info_storage[it->second].freq++;
            }
        }
    }
    vec_info_storage.resize(storage_idx);
}

template <typename T>
void Index<T>::FrequencyPrunig(QueryResult& p_query, QueryResult& local,
                               SearchStats& stat, Ort::Session* session) {
    SSDServing::Utils::StopW sw;

    const auto result_num = p_query.GetResultNum();
    std::vector<bool> send_flag(result_num, false);

    auto t1 = sw.getElapsedMs();

    absl::flat_hash_map<SPMerge::VectorID, int> vec_info;
    std::vector<SPMerge::VecFreqInfo> vec_info_storage;
    std::vector<double> len_list;
    CollectInfo(p_query, vec_info_storage, vec_info, len_list);

    auto t2 = sw.getElapsedMs();

    std::vector<float> features;
    GetFeature(vec_info_storage, len_list, features);

    auto t3 = sw.getElapsedMs();

    // predict freq_limit
    uint8_t freq_limit = 0;

    std::string m_inputName;
    std::string m_outputName;

    std::vector<float> m_predictionBuffer;
    Ort::Value m_inputTensor{nullptr};
    // 获取输入输出名
    Ort::AllocatorWithDefaultOptions allocator;
    m_inputName = session->GetInputNameAllocated(0, allocator).get();
    m_outputName = session->GetOutputNameAllocated(0, allocator).get();

    // 初始化用于预测的、可重用的缓冲区
    auto symbolic_shape =
        session->GetInputTypeInfo(0).GetTensorTypeAndShapeInfo().GetShape();

    // 检查以确保模型至少有2个维度 (batch, features)
    if (symbolic_shape.size() < 2 || symbolic_shape.back() < 0) {
        throw std::runtime_error("模型输入形状不符合预期，无法确定特征数量。");
    }
    // 获取特征数（通常是最后一个维度）
    size_t numFeatures = symbolic_shape.back();

    m_predictionBuffer.resize(numFeatures);

    std::vector<int64_t> concrete_shape = {1,
                                           static_cast<int64_t>(numFeatures)};

    Ort::MemoryInfo memoryInfo =
        Ort::MemoryInfo::CreateCpu(OrtArenaAllocator, OrtMemTypeDefault);
    m_inputTensor = Ort::Value::CreateTensor<float>(
        memoryInfo, m_predictionBuffer.data(), m_predictionBuffer.size(),
        concrete_shape.data(), concrete_shape.size());
    std::memcpy(m_predictionBuffer.data(), features.data(),
                features.size() * sizeof(float));
    const char* input_names[] = {m_inputName.c_str()};
    const char* output_names[] = {m_outputName.c_str()};

    auto outputTensors = session->Run(Ort::RunOptions{nullptr}, input_names,
                                      &m_inputTensor, 1, output_names, 1);
    float prediction_float = *outputTensors[0].GetTensorMutableData<float>();
    float decimal_part = prediction_float - std::floor(prediction_float);
    freq_limit = (decimal_part < 0.9f)
                     ? static_cast<int>(std::floor(prediction_float))
                     : static_cast<int>(std::ceil(prediction_float));

    auto t4 = sw.getElapsedMs();

    // do pruning
    for (const auto& info : vec_info_storage) {
        if (info.freq > freq_limit) {
            send_flag[info.first_idx] = true;
        }
    }
    int local_idx = 0, send_idx = 0;
    for (int i = 0; i < result_num; i++) {
        auto cid = p_query.GetResult(i)->VID;
        auto dist = p_query.GetResult(i)->Dist;
        if (cid == -1) break;
        if (not send_flag[i]) {
            local.SetResult(local_idx++, cid, dist);
        } else {
            p_query.SetResult(send_idx++, cid, dist);
        }
    }
    p_query.SetResultNum(send_idx);
    local.SetResultNum(local_idx);

    auto t5 = sw.getElapsedMs();

    stat.m_pruning_collect_vecinfo = t2 - t1;
    stat.m_pruning_feature_compute = t3 - t2;
    stat.m_pruning_model_predict = t4 - t3;
    stat.m_pruning_pruning = t5 - t4;
    stat.m_pruning_total = t5;

    return;
}

template <typename T>
ErrorCode Index<T>::BuildIndex(const void* p_data, SizeType p_vectorNum,
                               DimensionType p_dimension, bool p_normalized,
                               bool p_shareOwnership) {
    if (p_data == nullptr || p_vectorNum == 0 || p_dimension == 0)
        return ErrorCode::EmptyData;

    std::shared_ptr<VectorSet> vectorSet;
    if (p_shareOwnership) {
        vectorSet.reset(new BasicVectorSet(
            ByteArray((std::uint8_t*)p_data,
                      sizeof(T) * p_vectorNum * p_dimension, false),
            GetEnumValueType<T>(), p_dimension, p_vectorNum));
    } else {
        ByteArray arr = ByteArray::Alloc(sizeof(T) * p_vectorNum * p_dimension);
        memcpy(arr.Data(), p_data, sizeof(T) * p_vectorNum * p_dimension);
        vectorSet.reset(new BasicVectorSet(arr, GetEnumValueType<T>(),
                                           p_dimension, p_vectorNum));
    }

    if (m_options.m_distCalcMethod == DistCalcMethod::Cosine && !p_normalized) {
        vectorSet->Normalize(m_options.m_iSSDNumberOfThreads);
    }
    SPTAG::VectorValueType valueType =
        m_pQuantizer ? SPTAG::VectorValueType::UInt8 : m_options.m_valueType;
    std::shared_ptr<Helper::VectorSetReader> vectorReader(
        new Helper::MemoryVectorReader(
            std::make_shared<Helper::ReaderOptions>(
                valueType, p_dimension, VectorFileType::DEFAULT,
                m_options.m_vectorDelimiter, m_options.m_iSSDNumberOfThreads,
                true),
            vectorSet));

    m_options.m_valueType = GetEnumValueType<T>();
    m_options.m_dim = p_dimension;
    m_options.m_vectorSize = p_vectorNum;
    return BuildIndexInternal(vectorReader);
}

template <typename T>
ErrorCode Index<T>::UpdateIndex() {
    omp_set_num_threads(m_options.m_iSSDNumberOfThreads);
    m_index->SetParameter("NumberOfThreads",
                          std::to_string(m_options.m_iSSDNumberOfThreads));
    // m_index->SetParameter("MaxCheck",
    // std::to_string(m_options.m_maxCheck));
    // m_index->SetParameter("HashTableExponent",
    // std::to_string(m_options.m_hashExp));
    m_index->UpdateIndex();
    return ErrorCode::Success;
}

template <typename T>
ErrorCode Index<T>::SetParameter(const char* p_param, const char* p_value,
                                 const char* p_section) {
    if (SPTAG::Helper::StrUtils::StrEqualIgnoreCase(p_section, "BuildHead") &&
        !SPTAG::Helper::StrUtils::StrEqualIgnoreCase(p_param, "isExecute")) {
        if (m_index != nullptr)
            return m_index->SetParameter(p_param, p_value);
        else
            m_headParameters[p_param] = p_value;
    } else {
        m_options.SetParameter(p_section, p_param, p_value);
    }
    if (SPTAG::Helper::StrUtils::StrEqualIgnoreCase(p_param,
                                                    "DistCalcMethod")) {
        if (m_pQuantizer) {
            m_fComputeDistance = m_pQuantizer->DistanceCalcSelector<T>(
                m_options.m_distCalcMethod);
            m_iBaseSquare =
                (m_options.m_distCalcMethod == DistCalcMethod::Cosine)
                    ? m_pQuantizer->GetBase() * m_pQuantizer->GetBase()
                    : 1;
        } else {
            m_fComputeDistance =
                COMMON::DistanceCalcSelector<T>(m_options.m_distCalcMethod);
            m_iBaseSquare =
                (m_options.m_distCalcMethod == DistCalcMethod::Cosine)
                    ? COMMON::Utils::GetBase<T>() * COMMON::Utils::GetBase<T>()
                    : 1;
        }
    }
    return ErrorCode::Success;
}

template <typename T>
std::string Index<T>::GetParameter(const char* p_param,
                                   const char* p_section) const {
    if (SPTAG::Helper::StrUtils::StrEqualIgnoreCase(p_section, "BuildHead") &&
        !SPTAG::Helper::StrUtils::StrEqualIgnoreCase(p_param, "isExecute")) {
        if (m_index != nullptr)
            return m_index->GetParameter(p_param);
        else {
            auto iter = m_headParameters.find(p_param);
            if (iter != m_headParameters.end()) return iter->second;
            return "Undefined!";
        }
    } else {
        return m_options.GetParameter(p_section, p_param);
    }
}
}  // namespace SPANN
}  // namespace SPTAG

#define DefineVectorValueType(Name, Type) \
    template class SPTAG::SPANN::Index<Type>;

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType
