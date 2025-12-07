#pragma once

#include "inc/Core/Common.h"
#include "inc/Core/SPMerge.h"

namespace SPTAG {
namespace SPMerge {

class Merger {
    MachineID n_machines = 0;  // #virtual-replication, logical node
    MachineID n_split = 0;  // #sub-nodes, split from each virtual-replication
    ClusterID n_clusters = 0;
    VectorID n_vectors = 0;
    VectorID n_headvec = 0;

   public:
    Merger(MachineID machine_count, MachineID split_count,
           ClusterID cluster_count, VectorID vector_count,
           VectorID headvec_count)
        //    std::string output_dir, std::string original_file)
        : n_machines(machine_count),
          n_split(split_count),
          n_clusters(cluster_count),
          n_vectors(vector_count),
          n_headvec(headvec_count) {
        if (n_split == 0) NOT_IMPLEMENTED();
    }

    /// @brief invoked by upper node at initialization stage
    /// @param cluster_content each cluster's content, a set of vector IDs
    /// @param cluster_assignment what clusters does a machine have
    /// @param vector_assignment what vectors does a machine have
    /// @param vector_count how many vectors does a machine have
    void assign(const ClusterContentList& cluster_content,
                ClusterAssignment& cluster_assignment,
                VectorAssignment& vector_assignment, VectorCount& vector_count);

    void split(const ClusterContentList& cluster_content,
               ClusterAssignment& cluster_assignment,
               VectorAssignment& vector_assignment, VectorCount& vector_count);

    void patch(const VectorAssignment& vector_assignment,
               const VectorIDList& all_vid_list,
               const VectorCount& vector_count, Patch& patch);

   private:
    // no sub-nodes, patch each virtual-replication
    void patch_vr_only(const VectorAssignment& vector_assignment,
                       const VectorIDList& all_vid_list,
                       const VectorCount& vector_count, Patch& patch);
    void split_even(const ClusterContentList& cluster_content,
                    const ClusterAssignment& cluster_assignment,
                    ClusterAssignment& new_cluster_assignment,
                    VectorAssignment& vector_assignment,
                    VectorCount& vector_count);

    // patch primary node only
    void patch_with_split(const VectorAssignment& vector_assignment,
                          const VectorIDList& all_vid_list,
                          const VectorCount& vector_count, Patch& patch);

    void split_greedy(const ClusterContentList& cluster_content,
                      const ClusterAssignment& cluster_assignment,
                      ClusterAssignment& new_cluster_assignment,
                      VectorAssignment& vector_assignment,
                      VectorCount& vector_count);
    void split_horizontal(const ClusterContentList& cluster_content,
                          const ClusterAssignment& cluster_assignment,
                          ClusterAssignment& new_cluster_assignment,
                          VectorAssignment& vector_assignment,
                          VectorCount& vector_count);
};
}  // namespace SPMerge
}  // namespace SPTAG