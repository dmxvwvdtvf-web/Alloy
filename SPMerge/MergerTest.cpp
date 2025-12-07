#include <cassert>
#include <memory>
#include <string>

#include "inc/Core/Common.h"
#include "inc/Core/DefinitionList.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Core/SPMerge.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/Timer.h"
#include "inc/Helper/VectorSetReaders/ReocveryReader.h"
#include "inc/SSDServing/main.h"
#include "Merger.h"
#include "tqdm.hpp"

using namespace SPTAG;
using namespace SPTAG::Helper;
using namespace SPTAG::SSDServing;
using namespace SPTAG::SPMerge;
using namespace SPTAG::SPANN;

int main(int argc, char* argv[]) {
    if (argc < 2) throw std::invalid_argument("need configFilePath");

    std::map<std::string, std::map<std::string, std::string>> config_map;
    VectorValueType value_type;
    MachineID N_MACHINE;
    VectorID N_DATA;
    uint BATCH_SIZE;

    {
        Helper::IniReader iniReader;
        iniReader.LoadIniFile(argv[1]);
        config_map[SEC_BASE] = iniReader.GetParameters(SEC_BASE);
        config_map[SEC_SELECT_HEAD] = iniReader.GetParameters(SEC_SELECT_HEAD);
        config_map[SEC_BUILD_HEAD] = iniReader.GetParameters(SEC_BUILD_HEAD);
        config_map[SEC_BUILD_SSD_INDEX] =
            iniReader.GetParameters(SEC_BUILD_SSD_INDEX);
        config_map[SEC_REDUNDANCY_MERGER] =
            iniReader.GetParameters(SEC_REDUNDANCY_MERGER);
        config_map[SEC_REDUNDANCY_MERGER].at("loadmergerresult") = "false";

        value_type = iniReader.GetParameter(SEC_BASE, "ValueType", value_type);
        bool buildSSD =
            iniReader.GetParameter(SEC_BUILD_SSD_INDEX, "isExecute", false);
        N_MACHINE = iniReader.GetParameter(SEC_REDUNDANCY_MERGER, "NumMachine",
                                           N_MACHINE);
        N_DATA = iniReader.GetParameter(SEC_BASE, "N_DATA", N_DATA);
        BATCH_SIZE = iniReader.GetParameter(SEC_REDUNDANCY_MERGER,
                                            "MergerLoadBatchSize", BATCH_SIZE);

        for (auto& KV : iniReader.GetParameters(SEC_SEARCH_SSD_INDEX)) {
            std::string param = KV.first, value = KV.second;
            if (buildSSD && Helper::StrUtils::StrEqualIgnoreCase(
                                param.c_str(), "BuildSsdIndex"))
                continue;
            if (buildSSD && Helper::StrUtils::StrEqualIgnoreCase(param.c_str(),
                                                                 "isExecute"))
                continue;
            if (Helper::StrUtils::StrEqualIgnoreCase(param.c_str(),
                                                     "PostingPageLimit"))
                param = "SearchPostingPageLimit";
            if (Helper::StrUtils::StrEqualIgnoreCase(param.c_str(),
                                                     "InternalResultNum"))
                param = "SearchInternalResultNum";
            config_map[SEC_BUILD_SSD_INDEX][param] = value;
        }
    }

    std::shared_ptr<VectorIndex> index =
        VectorIndex::CreateInstance(IndexAlgoType::SPANN, value_type);
    if (index == nullptr) {
        LOG_ERROR("Cannot create Index with ValueType %s!\n",
                  config_map[SEC_BASE]["ValueType"].c_str());
        throw std::runtime_error("");
    }

    for (auto& sectionKV : config_map) {
        for (auto& KV : sectionKV.second) {
            index->SetParameter(KV.first, KV.second, sectionKV.first);
        }
    }
    SPANN::Options* option = nullptr;

#define DefineVectorValueType(Name, Type)                               \
    if (value_type == VectorValueType::Name) {                          \
        auto index_ptr = static_cast<SPANN::Index<Type>*>(index.get()); \
        option = index_ptr->GetOptions();                               \
    }
#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

    if (option == nullptr) {
        throw std::runtime_error("cannot get options");
    }

    EventTimer timer;

    timer.event_start("Merger Assignment");
    timer.event_start("1. SPANN index load");
    if (index->BuildIndex() != ErrorCode::Success) {
        throw std::runtime_error("Failed to build index.");
    }
    timer.event_stop("1. SPANN index load");

    timer.event_start("2. Prepare data");
    ClusterID n_cluster = index->GetNumPosting();
    MergerResult mr;
    VectorIDList all_vid_list;
    std::unique_ptr<bool> is_cluster_vector(new bool[N_DATA]{});

    auto t_collect_cluster =
        tq::trange((n_cluster + BATCH_SIZE - 1) / BATCH_SIZE);
    t_collect_cluster.set_prefix(
        "colllecting cluster content" +
        (BATCH_SIZE > 1 ? " in batchsize=" + std::to_string(BATCH_SIZE)
                        : "from file"));

    const size_t perVectorSize =
        option->m_dim * GetValueTypeSize(option->m_valueType);

    // collect vector set

    auto clusvec_buffer = std::make_unique<char[]>(N_DATA * perVectorSize);
    auto vectorset = std::make_shared<RecoveryInMemVectorset>(
        option->m_valueType, option->m_dim, 0, N_DATA);
    auto vec_postion = reinterpret_cast<void**>(vectorset->GetData());

    auto ptr = clusvec_buffer.get();
    auto f_handle_vector = [perVectorSize, &ptr, &vec_postion](uint64_t vid,
                                                               void* vec_ptr) {
        if (vec_postion[vid] == nullptr) {
            memcpy(ptr, vec_ptr, perVectorSize);
            vec_postion[vid] = ptr;
            ptr += perVectorSize;
        }
    };

    for (ClusterID cid = 0; cid < n_cluster; cid += BATCH_SIZE) {
        ClusterIDList cid_list;
        for (ClusterID i = cid; i < cid + BATCH_SIZE and i < n_cluster; i++) {
            cid_list.push_back(i);
        }
        ClusterContentList cluster_content;
        auto ret =
            index->GetPostingList(cid_list, cluster_content, f_handle_vector);
        if (ret == ErrorCode::Success) {
            mr.cluster_content.merge(cluster_content);
        } else {
            throw std::runtime_error("get posting list failed");
        }
        t_collect_cluster.update();
    }
    t_collect_cluster.manually_set_progress(1);
    t_collect_cluster.update();

    all_vid_list.reserve(N_DATA);
    auto t_check_vid = tq::tqdm(mr.cluster_content);
    t_check_vid.set_prefix("check cluster vectors");
    for (const auto& [_, vid_list] : t_check_vid) {
        for (auto vid : vid_list) {
            is_cluster_vector.get()[vid] = true;
        }
    }

    auto t_collect_vid = tq::trange(N_DATA);
    t_collect_vid.set_prefix("collect cluster vector vids");
    for (auto vid : t_collect_vid) {
        if (is_cluster_vector.get()[vid]) {
            all_vid_list.push_back(vid);
        }
    };

    std::vector<ClusterID> empty_cid_list;
    for (const auto& [cid, vid_list] : mr.cluster_content) {
        if (vid_list.empty()) {
            empty_cid_list.push_back(cid);
        }
    }
    for (auto cid : empty_cid_list) {
        mr.cluster_content.erase(cid);
    }
    LOG_INFO("removed %lu empty clusters to be assigned\n",
             empty_cid_list.size());
    timer.event_stop("2. Prepare data");

    mr.cluster_vector_count = all_vid_list.size();
    // NOTE: actually none of the equations below holds:
    // 1. head_count==#clusters
    // 2. head_count==#non_empty_clusters
    // 3. head_count+#cluster_vectors==N_DATA
    // This is because build step `posting-cut` may cut some vectors out of
    // cluster-vectors (making #cluster_vectors not correct). And similar
    // problem for head vectors.
    // So we can ONLY guarantee equation 3...
    mr.head_count = N_DATA - all_vid_list.size();
    if (ptr != &clusvec_buffer[mr.cluster_vector_count * perVectorSize]) {
        LOG_ERROR("After data load, ptr=%p, but expected=%p\n", ptr,
                  &clusvec_buffer[mr.cluster_vector_count]);
        throw std::runtime_error("");
    }

    Merger merger(N_MACHINE, option->m_numSplit, mr.cluster_content.size(),
                  all_vid_list.size(), mr.head_count);

    // vector assignment
    VectorAssignment vector_assignment;
    VectorCount vector_count;
    timer.event_start("3. Cluster assignment");
    merger.assign(mr.cluster_content, mr.cluster_assignment, vector_assignment,
                  vector_count);
    timer.event_stop("3. Cluster assignment");

    for (MachineID mid = 0; mid < N_MACHINE; mid++) {
        VectorID mi_nvec = vector_count[mid];
        ClusterID mi_nclus = mr.cluster_assignment[mid].size();
        printf("Machine %u has %u clusters, %u vecs, coverage %.1f%%\n", mid,
               mi_nclus, mi_nvec, 100. * mi_nvec / all_vid_list.size());
    }

    // split nodes
    VectorCount parent_vector_count = vector_count;

    timer.event_start("4. Split nodes");
    merger.split(mr.cluster_content, mr.cluster_assignment, vector_assignment,
                 vector_count);
    timer.event_stop("4. Split nodes");

    for (MachineID mid = 0; mid < N_MACHINE * option->m_numSplit; mid++) {
        VectorID mi_nvec = vector_count[mid];
        ClusterID mi_nclus = mr.cluster_assignment[mid].size();
        printf("Machine %u has %u clusters, %u vecs, coverage %.1f%%\n", mid,
               mi_nclus, mi_nvec, 100. * mi_nvec / all_vid_list.size());
    }

    // vector patch
    timer.event_start("5. Vector patch");
    merger.patch(vector_assignment, all_vid_list, vector_count, mr.patch);
    timer.event_stop("5. Vector patch");

    for (MachineID mid = 0; mid < N_MACHINE * option->m_numSplit; mid++) {
        VectorID mi_npatch = mr.patch[mid].size();
        printf("Machine %u has %u(%.1f%%) vecs in patches\n", mid, mi_npatch,
               100. * mi_npatch / all_vid_list.size());
    }

    timer.event_start("5. Output Files");
    std::shared_ptr<Helper::VectorSetReader> reader(
        new Helper::RecoveryReader(vectorset));
    index->ClusterAssignmentToOrder(mr, reader);
    index->SaveMergerResultFiles(mr, reader);
    timer.event_stop("5. Output Files");
    timer.event_stop("Merger Assignment");

    timer.log_all_event_duration();

    return 0;
}