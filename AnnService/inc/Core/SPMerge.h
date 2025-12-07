#pragma once

#include <bitset>
#include <cstdint>
#include <functional>
#include <memory>
#include <set>
#include <unordered_map>
#include <vector>

#include "inc/Helper/MemoryManager.h"

namespace SPTAG {
namespace SPANN {
struct ListInfo;
}

namespace SPMerge {

using MachineID = uint8_t;
using ClusterID = uint32_t;
using VectorID = uint32_t;

// used during assignment
using ClusterIDSet = std::set<ClusterID>;
using VectorIDSet = std::set<VectorID>;
using VectorAssignment = std::vector<std::vector<bool>>;
using VectorCount = std::vector<VectorID>;

///

// used for data storage, vector is more friendly

using ClusterIDList = std::vector<
    ClusterID,
    Helper::TrackerAllocator<ClusterID, Helper::MEMORY_SOURCE::SPMergeCIDList>>;

using VectorIDList = std::vector<
    VectorID,
    Helper::TrackerAllocator<VectorID, Helper::MEMORY_SOURCE::SPMergeVIDList>>;

using ClusterContentList = std::unordered_map<
    ClusterID, VectorIDList, std::hash<ClusterID>, std::equal_to<ClusterID>,
    Helper::TrackerAllocator<std::pair<const ClusterID, VectorIDList>,
                             Helper::MEMORY_SOURCE::SPMergeCContent>>;

using ClusterAssignment =
    std::vector<ClusterIDList,
                Helper::TrackerAllocator<
                    ClusterIDList, Helper::MEMORY_SOURCE::SPMergeCAssign>>;

using Patch =
    std::vector<VectorIDList,
                Helper::TrackerAllocator<VectorIDList,
                                         Helper::MEMORY_SOURCE::SPMergePatch>>;

const MachineID InvalidMID = ~static_cast<MachineID>(0);
const ClusterID InvalidCID = ~static_cast<ClusterID>(0);

struct MergerResult {
    SPMerge::VectorID cluster_vector_count;
    SPMerge::VectorID head_count;
    ClusterAssignment cluster_assignment;
    ClusterContentList cluster_content;
    Patch patch;
};

enum class RecoveryMethod {
    PureMemory,
    StupidLoad,
    GreedyLoad,
    ScanGather,
    ConcuRecov,
    ScanWrite,
};

struct VectorPosition {
    enum { Disk, Memory, Loaded, Outdated, Unavailable } where{Unavailable};
    struct {
        SPMerge::ClusterID cid;
        const SPTAG::SPANN::ListInfo* list_info;
    } disk_pos;
    struct {
        void* ptr;
    } mem_pos;
    struct {
        Helper::tracked_buffer ptr;
    } load_pos;
};

struct VecFreqInfo {
    uint freq{1};
    int first_idx{-1};  // first cluster idx, not cid

    VecFreqInfo() = default;
    VecFreqInfo(uint f, int i) : freq(f), first_idx(i) {}
};

}  // namespace SPMerge
}  // namespace SPTAG