#include "inc/Helper/MemoryManager.h"

#include <sys/vfs.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <string>

#include "inc/Core/Common.h"
#include "inc/Helper/Logging.h"

namespace SPTAG::Helper {

using MM = MemoryTracker;
using MS = MEMORY_SOURCE;

static constexpr uint64_t SYSTEM_MEMORY_THRESHOLD_MAX = 2 * GB;

thread_local uint64_t MM::local_used_memory_size_ = 0;
std::atomic<uint64_t> MM::used_memory_size_ = 0;
std::atomic<uint64_t> MM::peak_memory_size_ = 0;
std::atomic<uint64_t> MM::used_memory_by_source_[MS::MAX_SOURCES];
std::atomic<uint64_t> MM::peak_memory_by_source_[MS::MAX_SOURCES];
uint64_t MM::max_memory_size_ = 0;
uint64_t MM::system_memory_available_ = 0;
uint64_t MM::system_used_memory_ = 0;
uint64_t MM::reserved_memory_ = 0 * GB;

const char *MS_NAMES[MS::MAX_SOURCES] = {
    "UNKNOWN",
    "SPANN Metadata",
    "SPMerge Runtime Metadata",
    "SPMerge Recovery Buffer",
    "SPMerge VID List",
    "SPMerge CID List",
    "SPMerge Cluster Content",
    "SPMerge Cluster Assignment",
    "SPMerge Patch",
};

// FIXME: so ugly,,,
uint64_t get_cgroup_memory(std::string s, int ver) {
    std::ifstream cgroup_file("/proc/self/cgroup");
    if (!cgroup_file) return 0;

    std::string cgroup_path;
    if (ver == 1) {
        std::string line;
        while (std::getline(cgroup_file, line)) {
            auto pos = line.find("memory:");
            if (pos != std::string::npos) {
                cgroup_path = line.substr(pos + 7);
                break;
            }
        }
    } else if (ver == 2) {
        std::string line;
        std::getline(cgroup_file, line);
        if (line.rfind("0::", 0) != 0) return 0;
        cgroup_path = line.substr(3);
    }

    if (cgroup_path.empty()) return 0;

    // path to memory.max/memory.current
    std::string full_cgroup_mount;
    if (ver == 1) {
        full_cgroup_mount =
            "/sys/fs/cgroup/memory" + cgroup_path + "/memory." + s;
    } else if (ver == 2) {
        full_cgroup_mount = "/sys/fs/cgroup" + cgroup_path + "/memory." + s;
    }

    std::ifstream ifs(full_cgroup_mount);
    if (!ifs) return 0;

    std::string value;
    std::getline(ifs, value);
    if (value == "max" or value == "9223372036854771712") return 0;
    uint64_t bytes = std::stoull(value);
    return bytes;
}

uint64_t getMemoryInfo(std::string_view expected) {
    std::ifstream meminfo("/proc/meminfo");
    std::string line;
    while (std::getline(meminfo, line)) {
        std::istringstream iss(line);
        std::string key;
        uint64_t value;
        iss >> key >> value;
        if (key == expected) {
            return value * KB;
        }
    }
    return 0;
}

uint64_t get_proc_vmrss() {
    // if (auto size = get_cgroup_memory("current")) {
    //     return size;
    // }

    std::string line;
    std::ifstream status_file("/proc/self/status");
    uint64_t vmrss = 0;

    while (std::getline(status_file, line)) {
        if (line.find("VmRSS:") != std::string::npos) {
            sscanf(line.c_str(), "VmRSS: %lu", &vmrss);
            break;
        }
    }
    return vmrss * 1 * KB;
}

ExternalMemUsageGuard::ExternalMemUsageGuard(std::atomic<uint64_t> &usage,
                                             MEMORY_SOURCE src)
    : usage(usage), src(src), initial_state(get_proc_vmrss()) {
    if (initial_state == 0) {
        LOG_ERROR("get proc vm size = 0 KB\n");
    }
}

ExternalMemUsageGuard::~ExternalMemUsageGuard() {
    auto current = get_proc_vmrss();
    if (current < initial_state) {
        LOG_ERROR("initial state=%s, current=%s\n",
                  bytesToString(initial_state).c_str(),
                  bytesToString(current).c_str());
        return;
    }
    usage = current - initial_state;
    MemoryTracker::addExternalUsed(usage, src);
}

static const uint64_t CGROUP2_SUPER_MAGIC = 0x63677270;

void MM::init() {
    for (int i = 0; i < MAX_SOURCES; ++i) {
        used_memory_by_source_[i] = 0;
        peak_memory_by_source_[i] = 0;
    };
    used_memory_size_ = 0;
    local_used_memory_size_ = 0;
    peak_memory_size_ = 0;

    int cgroup_ver = 1;
    struct statfs s;
    statfs("/sys/fs/cgroup", &s);
    if (s.f_type == CGROUP2_SUPER_MAGIC) cgroup_ver = 2;

    auto cg_max = cgroup_ver == 2 ? get_cgroup_memory("max", 2)
                                  : get_cgroup_memory("limit_in_bytes", 1);
    auto sys_mem_total = getMemoryInfo("MemTotal:");
    // auto sys_mem_free = getMemoryInfo("MemAvailable:");

    if (cg_max) {
        max_memory_size_ = cg_max;
        system_used_memory_ = cgroup_ver == 2
                                  ? get_cgroup_memory("current", 2)
                                  : get_cgroup_memory("usage_in_bytes", 1);
        system_memory_available_ =
            cg_max - system_used_memory_ - reserved_memory_;
    } else {
        max_memory_size_ = sys_mem_total;
        system_memory_available_ = getMemoryInfo("MemAvailable:");
        system_used_memory_ = max_memory_size_ - system_memory_available_;
        system_memory_available_ -= reserved_memory_;
    }

    LOG_INFO("memory total=%s= %s(used by sys) + %s(reserved) + %s(free)\n",
             bytesToString(max_memory_size_).c_str(),
             bytesToString(system_used_memory_).c_str(),
             bytesToString(reserved_memory_).c_str(),
             bytesToString(system_memory_available_).c_str());
    checkStats("at init");
}

void MM::checkStats(std::string desc) {
    auto self_used = MM::getUsedMemorySize();
    auto sys_used = system_used_memory_;
    auto self_reported = self_used;
    auto real_used = get_proc_vmrss();
    uint64_t diff, threshold;
    if (self_reported < real_used) {
        diff = real_used - self_reported;
        threshold = 0.1 * real_used;
    } else {
        diff = self_reported - real_used;
        threshold = 0.1 * real_used;
    }
    if (diff > threshold) {
        LOG_WARN(
            "<%s> Memory status mismatch: recorded is %s(%s+%s), "
            "but real is %s\n",
            desc.c_str(), bytesToString(self_reported).c_str(),
            bytesToString(sys_used).c_str(), bytesToString(self_used).c_str(),
            bytesToString(real_used).c_str());
        // after the program starts up, we don't know whether the
        // (uncatched) memory change is caused by memory fragmentation or
        // system
        self_reported < real_used ? addUsed(diff) : subUsed(diff);
    } else {
        LOG_INFO("<%s> Memory status ok, used: %s\n", desc.c_str(),
                 bytesToString(self_reported).c_str());
    }
    dumpCurrentStats();
}

void MM::dumpCurrentStats() {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    auto total = used_memory_size_.load();
    oss << "[TOTAL=" << bytesToString(total);
    for (int i = 0; i < MS::MAX_SOURCES; ++i) {
        auto used = used_memory_by_source_[i].load();
        if (used == 0) {
            continue;
        }
        oss << ", " << MS_NAMES[i] << "(" << 100. * used / total << "%)"
            << "=" << bytesToString(used);
    }
    oss << "]";

    LOG_INFO("Memory Current Usage: %s\n", oss.str().c_str());
}

void MM::dumpPeakStats() {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    auto total = peak_memory_size_.load();
    oss << "[TOTAL=" << bytesToString(total);
    for (int i = 0; i < MS::MAX_SOURCES; ++i) {
        auto used = peak_memory_by_source_[i].load();
        if (used == 0) {
            continue;
        }
        oss << ", " << MS_NAMES[i] << "(" << 100. * used / total << "%)"
            << "=" << bytesToString(used);
    }
    oss << "]";

    LOG_INFO("Memory Peak Usage: %s\n", oss.str().c_str());
}

void *MM::alloc(uint64_t size, int source) {
    void *ptr = malloc(size);
    size = malloc_usable_size(ptr);
    addUsed(size, source);
    return ptr;
}

void *MM::aligned_alloc(uint64_t alignment, uint64_t size, int source) {
    void *ptr = ::aligned_alloc(alignment, size);
    size = malloc_usable_size(ptr);
    addUsed(size, source);
    return ptr;
}

int MM::memalign(void **ptr, int alignment, uint64_t size, int source) {
    int ret = posix_memalign(ptr, alignment, size);
    if (ret != 0) {
        return ret;
    }
    size = malloc_usable_size(*ptr);
    addUsed(size, source);
    return 0;
}

void MM::memalign_free(void *ptr, int source) {
    uint64_t size = malloc_usable_size(ptr);
    subUsed(size, source);
    ::free(ptr);
}

void *MM::realloc(void *ptr, uint64_t size, int source) {
    uint64_t old_alloc_size = malloc_usable_size(ptr);
    subUsed(old_alloc_size, source);
    void *nptr = ::realloc(ptr, size);
    uint64_t new_alloc_size = malloc_usable_size(nptr);
    addUsed(new_alloc_size, source);
    return nptr;
}

void MM::free(void *ptr, int source) {
    uint64_t size = malloc_usable_size(ptr);
    subUsed(size, source);
    ::free(ptr);
}

tracked_buffer make_tracked_buffer(uint64_t size, MEMORY_SOURCE src) {
    auto ptr = static_cast<char *>(MemoryTracker::alloc(size, src));
    return tracked_buffer(ptr, [src](char *p) { MemoryTracker::free(p, src); });
}

}  // namespace SPTAG::Helper