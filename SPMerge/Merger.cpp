#include "Merger.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <queue>
#include <unordered_map>

#include "inc/Core/Common.h"
#include "inc/Core/SPMerge.h"
#include "tqdm.hpp"

using namespace SPTAG::SPMerge;

// greedy algo., vector impl
void Merger::assign(const ClusterContentList& cluster_content,
                    ClusterAssignment& cluster_assignment,
                    VectorAssignment& vector_assignment,
                    VectorCount& vector_count) {
    assert(cluster_content.size() == n_clusters);
    // initialization
    cluster_assignment.resize(n_machines);
    vector_assignment.resize(n_machines);
    vector_count.resize(n_machines, 0);
    for (MachineID i = 0; i < n_machines; ++i) {
        cluster_assignment[i].clear();
        cluster_assignment[i].reserve(cluster_content.size() / n_machines);
        vector_assignment[i].clear();
        vector_assignment[i].resize(n_vectors + n_headvec, false);
    }
    ClusterIDSet empty_clusters;
    ClusterIDList assigned_cluster_list;

    // Now cluster content uses unordered_map, cid order is random, but we
    // expect deterministic result
    assigned_cluster_list.reserve(cluster_content.size());
    for (const auto& [cid, ci_content] : cluster_content) {
        if (ci_content.empty()) {
            // printf("Cluster %u has no content, skipping.\n", cid);
            empty_clusters.insert(cid);
        } else {
            assigned_cluster_list.push_back(cid);
        }
    }

    std::sort(assigned_cluster_list.begin(), assigned_cluster_list.end());

    auto t = tq::tqdm(assigned_cluster_list);
    t.set_prefix("calculating cluster assignments");
    for (auto cid : t) {
        const auto& ci_content = cluster_content.at(cid);
        VectorID max_update_nvec = 0;
        MachineID best_machine = 0;
        for (MachineID mid = 0; mid < n_machines; mid++) {
            VectorID n_diff = 0;
            for (auto vecid : ci_content) {
                if (not vector_assignment[mid][vecid]) {
                    n_diff++;
                }
            }
            if (n_diff > max_update_nvec) {
                max_update_nvec = n_diff;
                best_machine = mid;
            } else if (n_diff == max_update_nvec) {
                if (vector_count[best_machine] > vector_count[mid]) {
                    best_machine = mid;
                }
            }
        }

        // update assignments
        cluster_assignment[best_machine].push_back(cid);
        for (auto vecid : ci_content) {
            if (not vector_assignment[best_machine][vecid]) {
                vector_assignment[best_machine][vecid] = true;
                vector_count[best_machine]++;
            }
        }
    }
    if (empty_clusters.size()) {
        LOG_INFO("%lu clusters are empty, skipped\n", empty_clusters.size());
    }
    return;
}

void Merger::split(const ClusterContentList& cluster_content,
                   ClusterAssignment& cluster_assignment,
                   VectorAssignment& vector_assignment,
                   VectorCount& vector_count) {
    if (n_split <= 1) return;
    assert(cluster_content.size() == n_clusters);

    // initialization
    ClusterAssignment new_cluster_assignment;
    new_cluster_assignment.resize(n_machines * n_split);
    vector_assignment.resize(n_machines * n_split);
    for (MachineID mid = 0; mid < n_machines * n_split; ++mid) {
        new_cluster_assignment[mid].clear();
        new_cluster_assignment[mid].reserve(n_clusters / n_machines / n_split);
        vector_assignment[mid].clear();
        vector_assignment[mid].resize(n_vectors + n_headvec, false);
    }
    vector_count.clear();
    vector_count.resize(n_machines * n_split, 0);

    split_even(cluster_content, cluster_assignment, new_cluster_assignment,
               vector_assignment, vector_count);
    cluster_assignment.swap(new_cluster_assignment);
}

struct ClusterGain {
    ClusterID cid;
    size_t gain;
    size_t version = 0;

    ClusterGain(ClusterID cid, size_t gain, size_t ver = 0)
        : cid(cid), gain(gain), version(ver) {}
    bool operator<(const ClusterGain& other) {
        if (gain == other.gain) return cid < other.cid;
        return gain < other.gain;
    }
};

void Merger::split_even(const ClusterContentList& cluster_content,
                        const ClusterAssignment& cluster_assignment,
                        ClusterAssignment& new_cluster_assignment,
                        VectorAssignment& vector_assignment,
                        VectorCount& vector_count) {
#pragma omp parallel for
    for (MachineID parent_id = 0; parent_id < n_machines; ++parent_id) {
        // MachineID i  = 0;
        auto t = tq::tqdm(cluster_assignment[parent_id]);
        t.set_prefix("Re-assign");
        for (auto cid : t) {
            const auto& ci_content = cluster_content.at(cid);
            VectorID max_update_nvec = 0;
            MachineID best_machine = parent_id * n_split;
            for (MachineID mid = parent_id * n_split;
                 mid < parent_id * n_split + n_split; mid++) {
                VectorID n_diff = 0;
                for (auto vecid : ci_content) {
                    if (not vector_assignment[mid][vecid]) {
                        n_diff++;
                    }
                }
                if (n_diff > max_update_nvec) {
                    max_update_nvec = n_diff;
                    best_machine = mid;
                } else if (n_diff == max_update_nvec) {
                    if (vector_count[best_machine] > vector_count[mid]) {
                        best_machine = mid;
                    }
                }
            }
            MachineID assign_mid = best_machine;
            // MachineID assign_mid = parent_id*n_split + i%n_split;
            new_cluster_assignment[assign_mid].push_back(cid);
            for (auto vid : cluster_content.at(cid)) {
                if (not vector_assignment[assign_mid][vid]) {
                    vector_assignment[assign_mid][vid] = true;
                    vector_count[assign_mid]++;
                }
            }

            // i++;
        }
    }
}

void Merger::split_greedy(const ClusterContentList& cluster_content,
                          const ClusterAssignment& cluster_assignment,
                          ClusterAssignment& new_cluster_assignment,
                          VectorAssignment& vector_assignment,
                          VectorCount& vector_count) {
#pragma omp parallel for
    for (MachineID parent_id = 0; parent_id < n_machines; ++parent_id) {
        const auto& cid_list = cluster_assignment[parent_id];

        std::queue<ClusterGain> cluster_queue;
        for (auto cid : cid_list) {
            cluster_queue.emplace(cid, cluster_content.at(cid).size());
        }

        // assign to primary node as many vectors as possible
        MachineID primary_mid = parent_id * n_split;
        // greedy select with CELF
        while (new_cluster_assignment[primary_mid].size() <=
               cid_list.size() / n_split) {
            // LOG_INFO("machine %u, #clusters=%llu\n", primary_mid,
            //          new_cluster_assignment[primary_mid].size());
            size_t cur_ver = new_cluster_assignment[primary_mid].size();

            ClusterGain cg = cluster_queue.front();
            cluster_queue.pop();
            while (cg.version != cur_ver) {
                const auto& vid_list = cluster_content.at(cg.cid);
                cg.gain = 0;
                for (auto vid : vid_list) {
                    if (not vector_assignment[primary_mid][vid]) {
                        cg.gain++;
                    }
                }
                cg.version = cur_ver;
                cluster_queue.push(cg);
                cg = cluster_queue.front();
                cluster_queue.pop();
            }
            new_cluster_assignment[primary_mid].push_back(cg.cid);
            vector_count[primary_mid] += cg.gain;
            for (auto vid : cluster_content.at(cg.cid)) {
                vector_assignment[primary_mid][vid] = true;
            }
        }

        // assign to left machines round-robin
        size_t idx = 0;
        while (not cluster_queue.empty()) {
            MachineID mid = (primary_mid + 1) + (idx++ % (n_split - 1));

            ClusterGain cg = cluster_queue.front();
            cluster_queue.pop();

            new_cluster_assignment[mid].push_back(cg.cid);
            for (auto vid : cluster_content.at(cg.cid)) {
                if (not vector_assignment[mid][vid]) {
                    vector_assignment[mid][vid] = true;
                    vector_count[mid]++;
                }
            }
        }
    }
}

void Merger::split_horizontal(const ClusterContentList& cluster_content,
                              const ClusterAssignment& cluster_assignment,
                              ClusterAssignment& new_cluster_assignment,
                              VectorAssignment& vector_assignment,
                              VectorCount& vector_count) {
    // assign to each sub-node round-robin
#pragma omp parallel for
    for (MachineID parent_id = 0; parent_id < n_machines; ++parent_id) {
        const auto& cid_list = cluster_assignment[parent_id];
        for (size_t idx = 0; idx < cid_list.size(); idx++) {
            MachineID select_mid = (parent_id * n_split) + (idx % n_split);
            auto cid = cid_list[idx];

            new_cluster_assignment[select_mid].push_back(cid);
            for (auto vid : cluster_content.at(cid)) {
                if (not vector_assignment[select_mid][vid]) {
                    vector_assignment[select_mid][vid] = true;
                    vector_count[select_mid]++;
                }
            }
        }
    }
}

void Merger::patch(const VectorAssignment& vector_assignment,
                   const VectorIDList& all_vid_list,
                   const VectorCount& vector_count, Patch& patch) {
    if (n_split <= 1) {
        patch_vr_only(vector_assignment, all_vid_list, vector_count, patch);
    } else {
        patch_with_split(vector_assignment, all_vid_list, vector_count, patch);
    }
}

void Merger::patch_vr_only(const VectorAssignment& vector_assignment,
                           const VectorIDList& all_vid_list,
                           const VectorCount& vector_count, Patch& patch) {
    // initialization
    patch.clear();
    patch.resize(n_machines);
    for (MachineID mid = 0; mid < n_machines; mid++) {
        VectorIDList vecs;
        vecs.reserve(all_vid_list.size() - vector_count[mid]);
        for (auto vid : all_vid_list) {
            if (not vector_assignment[mid][vid]) {
                vecs.push_back(vid);
            }
        }
        patch[mid] = std::move(vecs);
    }
}

void Merger::patch_with_split(const VectorAssignment& vector_assignment,
                              const VectorIDList& all_vid_list,
                              const VectorCount& vector_count, Patch& patch) {
    // initialization
    patch.clear();
    patch.resize(n_machines * n_split);

    // patch with whole replication granularity,
    // save patch vid list to primary node
    for (MachineID primary_mid = 0; primary_mid < n_machines * n_split;
         primary_mid += n_split) {
        VectorIDList vecs;
        vecs.reserve(all_vid_list.size() - vector_count[primary_mid]);
        for (auto vid : all_vid_list) {
            bool exist = false;
            for (int split_idx = 0; split_idx < n_split; split_idx++) {
                MachineID sub_mid = primary_mid + split_idx;
                if (vector_assignment[sub_mid][vid]) {
                    exist = true;
                    break;
                }
            }
            if (not exist) vecs.push_back(vid);
        }

        patch[primary_mid] = std::move(vecs);
    }
}