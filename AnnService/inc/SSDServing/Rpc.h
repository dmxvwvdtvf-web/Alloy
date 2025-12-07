// Network connection for MemoryIndexClient and SSDIndexServer

#pragma once

#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <vector>

#include "inc/Core/SearchQuery.h"
#include "inc/Core/SPANN/Index.h"

// FIXME: there exists a compilation bug, brpc use `BLOCK_SIZE` as variable
// name, but `BLOCK_SIZE` is defined somewhere...
#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif

#include "brpc/callback.h"
#include "brpc/channel.h"
#include "brpc/http_status_code.h"
#include "brpc/server.h"
#include "BS_thread_pool.hpp"
#include "inc/Core/Common.h"
#include "inc/Core/Common/QueryResultSet.h"
#include "inc/Core/SPANN/IExtraSearcher.h"
#include "inc/Core/SPANN/Options.h"
#include "inc/SSDServing/Utils.h"
#include "search_pass.pb.h"

using pass_search_data::QueryHeadResult;
using pass_search_data::QuerySearchResult;
using pass_search_data::RANNService;
using pass_search_data::RANNService_Stub;

using RPCResultList = std::vector<QuerySearchResult*>;

using SPTAG::SPANN::SPANNVID;

namespace SPTAG {
namespace SSDServing {
namespace SSDIndex {

class SearchClientBase {
   public:
    SearchClientBase(const std::vector<std::string>& addr_list) {
        channel_list.reset(new brpc::Channel[addr_list.size()]);
        for (uint i = 0; i < addr_list.size(); i++) {
            auto& channel = channel_list[i];
            auto addr = addr_list[i].c_str();
            if (channel.Init(addr, nullptr) != 0) {
                LOG_ERROR("Fail to initialize channel to %s\n", addr);
                throw std::runtime_error("");
            }
            auto stub = std::make_unique<RANNService_Stub>(&channel);
            stub_list.push_back(std::move(stub));
            LOG_INFO("Channel to %s created\n", addr);
        }
    }

    virtual void wait_for_async_done() {}

    virtual bool CallSearchService(
        const SPANN::Options& p_opts, const QueryResult& p_query, int qid,
        SPTAG::SSDServing::RPC_Way rpc_way,
        std::function<void(const RPCResultList&,
                           const std::vector<brpc::Controller*>)>
            f_handle_response) = 0;

   protected:
    static bool f_check_status(brpc::Controller* cntl) {
        if (cntl->Failed()) {
            LOG_ERROR("RPC failed with error code %d: %s\n", cntl->ErrorCode(),
                      cntl->ErrorText().c_str());
        }
        return not cntl->Failed();
    }

    static std::unique_ptr<QueryHeadResult> InitializeRequest(
        const SPANN::Options& p_opts, const QueryResult& p_query, int qid) {
        auto request = std::make_unique<QueryHeadResult>();
        auto typesize = GetValueTypeSize(p_opts.m_valueType);
        auto dim = p_opts.m_dim;
        auto nprobe = p_query.GetResultNum();
        auto K = p_opts.m_resultNum;

        // auto cid_list_bytes = m_cid_list.size() * sizeof(int32_t);
        auto query_bytes = dim * typesize;
        auto vids_bytes = nprobe * sizeof(SPANNVID);
        auto dists_bytes = nprobe * sizeof(float);

        // 传递target
        const void* target = p_query.GetTarget();
        request->set_m_target(reinterpret_cast<const char*>(target),
                              query_bytes);

        // 传递VID和Dists
        std::vector<SPANNVID> vids;
        std::vector<float> dists;
        vids.reserve(nprobe);
        dists.reserve(nprobe);
        for (int i = 0; i < nprobe; ++i) {
            auto res = p_query.GetResult(i);
            vids.push_back(res->VID);
            dists.push_back(res->Dist);
        }
        request->set_vids(reinterpret_cast<const char*>(vids.data()),
                          vids_bytes);
        request->set_dists(reinterpret_cast<const char*>(dists.data()),
                           dists_bytes);
        request->set_nprobe(nprobe);
        request->set_k(K);
        request->set_qid(qid);
        // request->set_m_cid_list(reinterpret_cast<const
        // char*>(m_cid_list.data()),
        //                   cid_list_bytes);
        // request->set_m_cid_list_size(m_cid_list.size());
        return request;
    }

    // split 1 query to `split_cnt` sub queries
    static std::vector<std::shared_ptr<QueryHeadResult>>
    InitializeRequestWithSplit(const SPANN::Options& p_opts,
                               const QueryResult& p_query, int qid,
                               int split_cnt) {
        auto start_server = balance_number++;

        auto typesize = GetValueTypeSize(p_opts.m_valueType);
        auto dim = p_opts.m_dim;
        auto query_bytes = dim * typesize;

        const void* target = p_query.GetTarget();

        auto nprobe = p_query.GetResultNum();
        auto K = p_opts.m_resultNum;

        std::vector<std::shared_ptr<QueryHeadResult>> request_vec(split_cnt);
        for (auto& request : request_vec) request.reset(new QueryHeadResult);

        std::vector<std::vector<SPANNVID>> vid_list_vec(split_cnt);
        std::vector<std::vector<float>> dist_list_vec(split_cnt);

        int avg_nprobe = nprobe / split_cnt;
        for (auto& vid_list : vid_list_vec) vid_list.reserve(avg_nprobe);
        for (auto& dist_list : dist_list_vec) dist_list.reserve(avg_nprobe);

        // split 1 query to `split_cnt` sub queries
        for (int i = 0; i < nprobe; i++) {
            auto res = p_query.GetResult(i);
            // avoid skewed rpc pattern: server 0 under greater pressure
            auto server_id = (i + start_server) % split_cnt;
            vid_list_vec[server_id].push_back(res->VID);
            dist_list_vec[server_id].push_back(res->Dist);
        }

        // fill requests
        for (int i = 0; i < split_cnt; i++) {
            auto& request = request_vec[i];
            const auto& vid_list = vid_list_vec[i];
            const auto& dist_list = dist_list_vec[i];

            int sub_nprobe = vid_list.size();
            assert(vid_list.size() == dist_list.size());

            auto vids_bytes = sub_nprobe * sizeof(SPANNVID);
            auto dists_bytes = sub_nprobe * sizeof(float);

            request->set_vids(reinterpret_cast<const char*>(vid_list.data()),
                              vids_bytes);
            request->set_dists(reinterpret_cast<const char*>(dist_list.data()),
                               dists_bytes);
            request->set_nprobe(sub_nprobe);
            request->set_k(K);
            request->set_qid(qid);
            request->set_m_target(reinterpret_cast<const char*>(target),
                                  query_bytes);
        }

        return request_vec;
    }

    static thread_local uint32_t balance_number;
    std::unique_ptr<brpc::Channel[]> channel_list;
    std::vector<std::unique_ptr<RANNService_Stub>> stub_list;
};
inline thread_local uint32_t SearchClientBase::balance_number = 0;

class SearchClientSync : public SearchClientBase {
   public:
    SearchClientSync(const std::vector<std::string>& addr_list)
        : SearchClientBase(addr_list) {}

    virtual bool CallSearchService(
        const SPANN::Options& p_opts, const QueryResult& p_query, int qid,
        SPTAG::SSDServing::RPC_Way rpc_way,
        std::function<void(const RPCResultList&,
                           const std::vector<brpc::Controller*>)>
            f_handle_response) override {
        bool success = true;

        RPCResultList responses;
        std::vector<brpc::Controller*> controllers;
        for (uint i = 0; i < stub_list.size(); i++) {
            responses.push_back(new QuerySearchResult);
            controllers.push_back(new brpc::Controller);
        }

        auto time_point = std::chrono::system_clock::now();
        auto time_since_epoch = time_point.time_since_epoch();
        double timestamp_to_send_ms =
            std::chrono::duration<double, std::milli>(time_since_epoch).count();

        // initialize request
        std::unique_ptr<QueryHeadResult> request_holder;
        std::vector<std::shared_ptr<QueryHeadResult>> multi_request;
        std::vector<std::shared_ptr<QueryHeadResult>> search_request(
            stub_list.size());
        switch (rpc_way) {
            case RPC_Way::Replication:
            case RPC_Way::LoadBalance: {
                request_holder = InitializeRequest(p_opts, p_query, qid);
                request_holder->set_clienttoservertime(timestamp_to_send_ms);
                break;
            }
            case RPC_Way::SplitQuery: {
                // num machine * split
                // multi_request = InitializeRequestWithSplit(p_opts, p_query,
                // qid,
                //                                            stub_list.size());
                multi_request = InitializeRequestWithSplit(p_opts, p_query, qid,
                                                           p_opts.m_numMachine);
                for (int i = 0; i < stub_list.size(); i++) {
                    int request_id = i / p_opts.m_numSplit;
                    search_request[i] = multi_request[request_id];
                }
                for (auto& request : search_request) {
                    request->set_clienttoservertime(timestamp_to_send_ms);
                }
                break;
            }
            default:
                throw std::invalid_argument("unknown grpc way");
        }

        // /* serial rpc calls, bad performance */
        // for (uint i = 0; i < stub_list.size(); i++) {
        //     stub_list[i]->DiskSearch(controllers[i], request_holder.get(),
        //                             responses[i], nullptr);
        // }

        /* parallel rpc calls */
        int q_machine = qid % stub_list.size();
        switch (rpc_way) {
            case RPC_Way::Replication: {
                for (int i = 0; i < stub_list.size(); i++) {
                    stub_list[i]->DiskSearch(controllers[i],
                                             request_holder.get(), responses[i],
                                             brpc::DoNothing());
                }
                break;
            }
            case RPC_Way::LoadBalance: {
                stub_list[q_machine]->DiskSearch(
                    controllers[q_machine], request_holder.get(),
                    responses[q_machine], brpc::DoNothing());
                break;
            }
            case RPC_Way::SplitQuery: {
                for (int i = 0; i < stub_list.size(); i++) {
                    stub_list[i]->DiskSearch(controllers[i],
                                             search_request[i].get(),
                                             responses[i], brpc::DoNothing());
                }
                break;
            }
            default:
                throw std::invalid_argument("unknown grpc way");
        }

        // wait rpc
        switch (rpc_way) {
            case RPC_Way::Replication:
            case RPC_Way::SplitQuery:
                for (uint i = 0; i < controllers.size(); i++) {
                    brpc::Join(controllers[i]->call_id());
                    if (not f_check_status(controllers[i])) {
                        success = false;
                    }
                }
                break;
            case RPC_Way::LoadBalance:
                brpc::Join(controllers[q_machine]->call_id());
                if (not f_check_status(controllers[q_machine])) {
                    success = false;
                }
                break;
            default:
                throw std::invalid_argument("unknown grpc way");
        }

        if (success) {
            f_handle_response(responses, controllers);
        }
        for (auto ptr : responses) delete ptr;
        for (auto ptr : controllers) delete ptr;
        return success;
    }
};

template <typename F>
class LambdaClosure : public google::protobuf::Closure {
   public:
    explicit LambdaClosure(F&& f) : func(std::forward<F>(f)) {}

    void Run() override {
        func();
        delete this;
    }

   private:
    F func;
};

template <typename F>
google::protobuf::Closure* MakeClosure(F&& f) {
    return new LambdaClosure<F>(std::forward<F>(f));
}

class SearchClientASync : public SearchClientBase {
   public:
    SearchClientASync(const std::vector<std::string>& addr_list)
        : SearchClientBase(addr_list) {
        // TODO: fit to variable length topk
        NOT_IMPLEMENTED();
    }

    void wait_for_async_done() override {
        while (async_waiting.load() != 0) {
        }
    }

    virtual bool CallSearchService(
        const SPANN::Options& p_opts, const QueryResult& p_query, int qid,
        SPTAG::SSDServing::RPC_Way rpc_way,
        std::function<void(const RPCResultList&,
                           const std::vector<brpc::Controller*>)>
            f_handle_response) override {
        async_waiting++;

        bool success = true;
        auto request = InitializeRequest(p_opts, p_query, qid).release();
        RPCResultList responses;
        std::vector<brpc::Controller*> controllers;
        for (uint i = 0; i < stub_list.size(); i++) {
            responses.push_back(new QuerySearchResult);
            controllers.push_back(new brpc::Controller);
        }

        auto rpc_finished = new std::atomic_int8_t(0);

        for (uint i = 0; i < stub_list.size(); i++) {
            auto cntl = controllers[i];
            stub_list[i]->DiskSearch(
                controllers[i], request, responses[i],
                MakeClosure([this, request, responses, f_handle_response, cntl,
                             controllers, rpc_finished,
                             rpc_cnt = stub_list.size()]() {
                    f_check_status(cntl);
                    if (rpc_finished->operator++() == rpc_cnt) {
                        f_handle_response(responses, controllers);
                        // free memory
                        delete request;
                        for (auto ptr : responses) delete ptr;
                        delete rpc_finished;
                        async_waiting--;
                    }
                    delete cntl;
                }));
        }
        return success;
    }

   private:
    std::atomic_int async_waiting = 0;
};

template <typename ValueType>
class SearchCallbackImpl final : public RANNService {
   public:
    explicit SearchCallbackImpl(SPANN::Index<ValueType>* p_index,
                                BS::light_thread_pool* pool)
        : m_index(p_index), m_pool(pool) {}

    virtual void DiskSearch(::google::protobuf::RpcController* controller,
                            const QueryHeadResult* request,
                            QuerySearchResult* response,
                            ::google::protobuf::Closure* done) override {
        m_pool->detach_task([=]() mutable {
            // auto server_time_point = std::chrono::system_clock::now();
            // auto server_time_since_epoch =
            // server_time_point.time_since_epoch();

            // double sw0_start_time = std::chrono::duration<double,
            // std::milli>(
            //                             server_time_since_epoch)
            //                             .count();
            Utils::StopW threadws;
            brpc::ClosureGuard done_guard(done);
            auto cntl = static_cast<brpc::Controller*>(controller);
            // return;
            const std::string& target_payload = request->m_target();
            const std::string& vids_payload = request->vids();
            const std::string& dists_payload = request->dists();
            const int nprobe = request->nprobe();
            const int topk = request->k();
            const int qid = request->qid();

            // parameter check
            if (nprobe > m_index->GetOptions()->m_searchInternalResultNum) {
                cntl->SetFailed(
                    "received nprobe > server config, overflow risk");
                return;
            }
            if (topk != m_index->GetOptions()->m_resultNum) {
                cntl->SetFailed("different topk not implemented");
                return;
            }
            if (vids_payload.size() != nprobe * sizeof(int32_t) ||
                dists_payload.size() != nprobe * sizeof(float)) {
                cntl->SetFailed("Data size mismatch");
                return;
            }

            // fill `result`
            QueryResult result(target_payload.data(), max(nprobe, topk), false);
            auto vids_data =
                reinterpret_cast<const int32_t*>(vids_payload.data());
            auto dists_data =
                reinterpret_cast<const float*>(dists_payload.data());
            for (int i = 0; i < nprobe; i++) {
                result.SetResult(i, vids_data[i], dists_data[i]);
            }
            // `stats`
            SPANN::SearchStats stats;

            // search
            m_index->SearchDiskIndexPurely(result, &stats);

            // prepare response data
            std::vector<SPANNVID> res_vids(topk);
            std::vector<float> res_dists(topk);
            for (int i = 0; i < topk; i++) {
                res_vids[i] = result.GetResult(i)->VID;
                res_dists[i] = result.GetResult(i)->Dist;
            }

            auto res_vids_size = topk * sizeof(SPANNVID);
            auto res_dists_size = topk * sizeof(float);
            response->set_vids(res_vids.data(), res_vids_size);
            response->set_dists(res_dists.data(), res_dists_size);
            response->set_k(topk);
            double endTime = threadws.getElapsedMs();
            response->set_serverprocesstime(endTime);
        });
    }

    size_t GetMaxRunningThreads() const { return m_maxRunningThreads.load(); }
    void LogThreadPoolUsage() {
        auto nt = m_pool->get_thread_count();
        auto mt = m_maxRunningThreads.load();
        auto usage = 100. * mt / nt;
        LOG_INFO(
            "Callback Service: threadpool=%lu, max_running=%lu, "
            "usage=%.2f%%\n",
            nt, mt, usage);
    }

   private:
    void UpdateMaxRunningThreads(size_t new_val) {
        size_t old_val = m_maxRunningThreads.load();
        while (
            old_val < new_val &&
            not m_maxRunningThreads.compare_exchange_weak(old_val, new_val)) {
        }
        if (old_val < new_val) LogThreadPoolUsage();
    }

   private:
    SPANN::Index<ValueType>* m_index;
    BS::light_thread_pool* m_pool;
    std::atomic_size_t m_maxRunningThreads{0};
};

}  // namespace SSDIndex
}  // namespace SSDServing
}  // namespace SPTAG