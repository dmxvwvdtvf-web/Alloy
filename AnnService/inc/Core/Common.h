// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_CORE_COMMONDEFS_H_
#define _SPTAG_CORE_COMMONDEFS_H_

#include <ios>
#include <mutex>
#include <stdexcept>
#ifdef DEBUG
#define IF_DEBUG(statement) statement
#define IF_NDEBUG(statement)
#else
#define IF_DEBUG(statement)
#define IF_NDEBUG(statement) statement
#endif

#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

#include "inc/Helper/DiskIO.h"
#include "inc/Helper/Logging.h"

#ifndef _MSC_VER
#include <dirent.h>
#include <stdio.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstring>

#if defined(__INTEL_COMPILER)
#include <malloc.h>
#else
#include <mm_malloc.h>
#endif

#define FolderSep '/'

inline bool direxists(const char* path) {
    struct stat info;
    return stat(path, &info) == 0 && (info.st_mode & S_IFDIR);
}
inline bool fileexists(const char* path) {
    struct stat info;
    return stat(path, &info) == 0 && (info.st_mode & S_IFDIR) == 0;
}

inline std::string bytesToString(size_t bytes) {
    const size_t GB = 1024 * 1024 * 1024;
    const size_t MB = 1024 * 1024;
    const size_t KB = 1024;

    std::stringstream ss;

    if (bytes >= GB) {
        ss << bytes / GB << "GB ";
        bytes %= GB;
    }
    if (bytes >= MB) {
        ss << bytes / MB << "MB ";
        bytes %= MB;
    }
    if (bytes >= KB) {
        ss << bytes / KB << "KB ";
        bytes %= KB;
    }
    if (bytes) {
        ss << bytes << "B";
    }

    if (ss.str().empty()) {
        ss << "0";
    }
    return ss.str();
}

template <class T>
inline T min(T a, T b) {
    return a < b ? a : b;
}
template <class T>
inline T max(T a, T b) {
    return a > b ? a : b;
}

#ifndef _rotl
#define _rotl(x, n) (((x) << (n)) | ((x) >> (32 - (n))))
#endif

#define mkdir(a) mkdir(a, ACCESSPERMS)
#define InterlockedCompareExchange(a, b, c) __sync_val_compare_and_swap(a, c, b)
#define InterlockedExchange8(a, b) __sync_lock_test_and_set(a, b)
#define Sleep(a) usleep(a * 1000)
#define strtok_s(a, b, c) strtok_r(a, b, c)

#else

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif  // !WIN32_LEAN_AND_MEAN

#include <malloc.h>
#include <Psapi.h>
#include <Windows.h>

#define FolderSep '\\'

inline bool direxists(const TCHAR* path) {
    auto dwAttr = GetFileAttributes(path);
    return (dwAttr != INVALID_FILE_ATTRIBUTES) &&
           (dwAttr & FILE_ATTRIBUTE_DIRECTORY);
}
inline bool fileexists(const TCHAR* path) {
    auto dwAttr = GetFileAttributes(path);
    return (dwAttr != INVALID_FILE_ATTRIBUTES) &&
           (dwAttr & FILE_ATTRIBUTE_DIRECTORY) == 0;
}
#define mkdir(a) CreateDirectory(a, NULL)

#ifndef max
#define max(a, b) (((a) > (b)) ? (a) : (b))
#endif

#ifndef min
#define min(a, b) (((a) < (b)) ? (a) : (b))
#endif

#endif

namespace SPTAG {
#if (__cplusplus < 201703L)
#define ALIGN_ALLOC(size) _mm_malloc(size, 32)
#define ALIGN_FREE(ptr) _mm_free(ptr)
#define PAGE_ALLOC(size) _mm_malloc(size, 512)
#define PAGE_FREE(ptr) _mm_free(ptr)
#else
#define ALIGN_ALLOC(size) ::operator new(size, (std::align_val_t)32)
#define ALIGN_FREE(ptr) ::operator delete(ptr, (std::align_val_t)32)
#define PAGE_ALLOC(size) ::operator new(size, (std::align_val_t)512)
#define PAGE_FREE(ptr) ::operator delete(ptr, (std::align_val_t)512)
#endif

// FIXME: use int32 to represent vector-id and cluster-id is bad!
typedef std::int32_t SizeType;
typedef std::int32_t DimensionType;

const SizeType MaxSize = (std::numeric_limits<SizeType>::max)();
const float MinDist = (std::numeric_limits<float>::min)();
const float MaxDist = (std::numeric_limits<float>::max)() / 10;
const float Epsilon = 0.000001f;
const std::uint16_t PageSize = 4096;
const int PageSizeEx = 12;

#define ROUND_UP(x, align) ((x + align - 1) & (~(align - 1)))
#define PAGE_ROUND_UP(x) ROUND_UP(x, PageSize)
#define PAGE_COUNT(x) (PAGE_ROUND_UP(x) / PageSize)

extern std::mt19937 rg;
inline std::mt19937 get_rg_local(uint i = 0) {
    unsigned int rand_number =
        std::chrono::steady_clock::now().time_since_epoch().count();
    rand_number += i;
    return std::mt19937(rand_number);
}
extern std::shared_ptr<Helper::DiskIO> (*f_createIO)();

#define IOBINARY(ptr, func, bytes, ...) \
    if (ptr->func(bytes, __VA_ARGS__) != bytes) return ErrorCode::DiskIOFail
#define IOSTRING(ptr, func, ...) \
    if (ptr->func(__VA_ARGS__) == 0) return ErrorCode::DiskIOFail

extern Helper::LoggerHolder& GetLoggerHolder();
extern std::shared_ptr<Helper::Logger> GetLogger();
extern void SetLogger(std::shared_ptr<Helper::Logger>);

#define SPTAGLIB_LOG(l, ...)                                           \
    GetLogger()->Logging("SPTAG", l, __FILE__, __LINE__, __FUNCTION__, \
                         __VA_ARGS__)
#define LOG_ERROR(...) SPTAGLIB_LOG(Helper::LogLevel::LL_Error, __VA_ARGS__)
#define LOG_INFO(...) SPTAGLIB_LOG(Helper::LogLevel::LL_Info, __VA_ARGS__)
#define LOG_DEBUG(...) SPTAGLIB_LOG(Helper::LogLevel::LL_Debug, __VA_ARGS__)
#define LOG_WARN(...) SPTAGLIB_LOG(Helper::LogLevel::LL_Warning, __VA_ARGS__)

#define NOT_IMPLEMENTED()                                                      \
    {                                                                          \
        LOG_ERROR(                                                             \
            "This function (%s) is not implemented (probably shouldn't be)\n", \
            __FUNCTION__);                                                     \
        throw std::logic_error("");                                            \
    }  // namespace SPTAG

inline void clear_page_cache() {
    int ret = system("sh -c 'echo 1 > /proc/sys/vm/drop_caches'");
    if (ret != 0) {
        LOG_ERROR("Failed to clear page cache (return=%d)!\n", ret);
    }
}

struct SimpleIOHelper {
    static std::shared_ptr<char> m_padding_vals;
    static std::once_flag init_flag;

    std::shared_ptr<Helper::DiskIO> m_ptr = nullptr;
    enum IOMode {
        Null = 0,
        ReadOnly = 1 << 0,
        WriteOnly = 1 << 1,
        ReadWrite = 3
    } m_mode = Null;

    struct {
        size_t written_bytes{0};
        size_t padding_total_bytes{0};
    } m_stat{};

    explicit SimpleIOHelper() { init_paddings_once(); }
    explicit SimpleIOHelper(const std::string& output_file, IOMode mode,
                            bool trunc = false, bool append = false)
        : m_ptr(SPTAG::f_createIO()), m_mode(mode) {
        init_fileIO(output_file, mode, append, trunc, false);
        init_paddings_once();
    }
    explicit SimpleIOHelper(std::shared_ptr<Helper::DiskIO> ptr,
                            const std::string& output_file, IOMode mode,
                            bool trunc = false, bool append = false,
                            bool direct = false)
        : m_mode(mode) {
        m_ptr = ptr;
        init_fileIO(output_file, mode, append, trunc, direct);
        init_paddings_once();
    }
    void normal_init(const std::string& output_file, IOMode mode,
                     bool trunc = false, bool append = false) {
        m_ptr = SPTAG::f_createIO();
        m_mode = mode;
        init_fileIO(output_file, mode, append, trunc, false);
        init_paddings_once();
    }
    void normal_init(std::shared_ptr<Helper::DiskIO> ptr,
                     const std::string& output_file, IOMode mode,
                     bool trunc = false, bool append = false,
                     bool direct = false) {
        m_ptr = ptr;
        m_mode = mode;
        init_fileIO(output_file, mode, append, trunc, direct);
        init_paddings_once();
    }
    void init_with_ext_io(std::shared_ptr<Helper::DiskIO> ptr, IOMode mode) {
        m_ptr = ptr;
        m_mode = mode;
        init_paddings_once();
    }
    void f_write_data(const void* data_ptr, size_t size,
                      uint64_t offset = UINT64_MAX) {
        assert_can_write();
        if (offset != UINT64_MAX) assert_can_read();
        auto ptr_reinterpreted = reinterpret_cast<const char*>(data_ptr);
        while (size != 0) {
            auto ret = m_ptr->WriteBinary(size, ptr_reinterpreted, offset);
            if (ret == 0) {
                throw std::runtime_error("Failed to write SSD file, ret=0\n");
            }
            size -= ret;
            ptr_reinterpreted += ret;
            if (offset != UINT64_MAX) offset += ret;
            m_stat.written_bytes += ret;
        }
    };
    void f_write_padding_at(size_t start_byte,
                            size_t target_byte = UINT64_MAX) {
        assert_can_write();
        if (target_byte == UINT64_MAX) {
            target_byte = PAGE_ROUND_UP(start_byte);
        }
        size_t padding_size = target_byte - start_byte;
        if (padding_size >= PageSize) {
            LOG_ERROR("padding size is %llu, >PageSize\n", padding_size);
            throw std::invalid_argument("");
        }
        auto ret =
            m_ptr->WriteBinary(padding_size, m_padding_vals.get(), start_byte);
        if (ret != padding_size) {
            LOG_ERROR("Failed to write SSD file, ret=%llu!\n", ret);
            throw std::runtime_error("");
        }
        m_stat.written_bytes += padding_size;
        m_stat.padding_total_bytes += padding_size;
    };
    void f_write_padding(size_t target_byte = UINT64_MAX) {
        assert_can_write();
        if (target_byte == UINT64_MAX) {
            target_byte = PAGE_ROUND_UP(m_stat.written_bytes);
        }
        if (m_stat.written_bytes == target_byte) return;
        size_t padding_size = target_byte - m_stat.written_bytes;
        if (padding_size >= PageSize) {
            LOG_ERROR("padding size is %llu, >PageSize\n", padding_size);
            throw std::invalid_argument("");
        }
        auto ret = m_ptr->WriteBinary(padding_size, m_padding_vals.get());
        if (ret != padding_size) {
            LOG_ERROR("Failed to write SSD file, ret=%llu!\n", ret);
            throw std::runtime_error("");
        }
        m_stat.written_bytes += padding_size;
        m_stat.padding_total_bytes += padding_size;
    };
    void f_read_data(void* data_ptr, size_t size,
                     uint64_t offset = UINT64_MAX) const {
        assert_can_read();
        auto ret =
            m_ptr->ReadBinary(size, reinterpret_cast<char*>(data_ptr), offset);
        if (ret != size) {
            LOG_ERROR("Failed to read SSD file, ret=%llu!\n", ret);
            throw std::runtime_error("");
        }
    }
    void f_seek_pos(uint64_t offset) {
        assert_can_read();
        m_ptr->SeekP(offset);
    }
    void close() {
        m_ptr.reset();
        m_mode = Null;
    }

   private:
    void assert_can_write() const {
        if (not(m_mode & WriteOnly)) {
            LOG_ERROR("cannot write, m_mode=%u\n", m_mode);
            throw std::logic_error("");
        }
    }
    void assert_can_read() const {
        if (not(m_mode & ReadOnly)) {
            LOG_ERROR("cannot read, m_mode=%u\n", m_mode);
            throw std::logic_error("");
        }
    }
    void init_fileIO(const std::string& output_file, int mode, bool append,
                     bool trunc, bool direct, int retry = 3) {
        bool is_sfio =
            std::dynamic_pointer_cast<Helper::SimpleFileIO>(m_ptr) != nullptr;
        bool is_nfio =
            std::dynamic_pointer_cast<Helper::NativeFileIO>(m_ptr) != nullptr;
        if (not(is_sfio or is_nfio)) {
            throw std::runtime_error(
                "ptr is not a SimpleFileIO nor NativeFileIO!");
        }
        // open file
        int open_mode;
        switch (mode) {
            case ReadOnly:
                open_mode = std::ios::binary | std::ios::in;
                if (trunc or append) {
                    LOG_WARN(
                        "in ReadOnly mode, should not set trunc or append\n");
                }
                break;
            case WriteOnly:
                open_mode = std::ios::binary | std::ios::out;
                if (append) open_mode |= std::ios::app;
                if (trunc) open_mode |= std::ios::trunc;
                break;
            case ReadWrite:
                open_mode = std::ios::binary | std::ios::in | std::ios::out;
                if (append and trunc) {
                    throw std::logic_error("append and trunc both set!");
                }
                if (append) open_mode |= std::ios::app;
                if (trunc) open_mode |= std::ios::trunc;
                break;
            default:
                LOG_ERROR("unknown mode: %u\n", mode);
                throw std::invalid_argument("");
        }
        if (is_nfio and direct) {
            open_mode |= Helper::extended_flags::direct;
        }
        while (retry > 0 &&
               !m_ptr->Initialize(output_file.c_str(), open_mode)) {
            LOG_ERROR("Failed open file %s, retrying...\n",
                      output_file.c_str());
            auto parent_dir = std::filesystem::path(output_file).parent_path();
            if (not std::filesystem::exists(parent_dir)) {
                std::filesystem::create_directories(parent_dir);
            }
            retry--;
        }
    }
    static void init_paddings_once() {
        std::call_once(init_flag, []() {
            m_padding_vals.reset(static_cast<char*>(PAGE_ALLOC(PageSize)),
                                 [](char* ptr) { PAGE_FREE(ptr); });
            memset(m_padding_vals.get(), 0, PageSize);
        });
    }
};

inline std::shared_ptr<char> SimpleIOHelper::m_padding_vals = nullptr;
inline std::once_flag SimpleIOHelper::init_flag;

class MyException : public std::exception {
   private:
    std::string Exp;

   public:
    MyException(std::string e) { Exp = e; }
#ifdef _MSC_VER
    const char* what() const { return Exp.c_str(); }
#else
    const char* what() const noexcept { return Exp.c_str(); }
#endif
};

enum class ErrorCode : std::uint16_t {
#define DefineErrorCode(Name, Value) Name = Value,
#include "DefinitionList.h"
#undef DefineErrorCode

    Undefined
};
static_assert(static_cast<std::uint16_t>(ErrorCode::Undefined) != 0,
              "Empty ErrorCode!");

enum class DistCalcMethod : std::uint8_t {
#define DefineDistCalcMethod(Name) Name,
#include "DefinitionList.h"
#undef DefineDistCalcMethod

    Undefined
};
static_assert(static_cast<std::uint8_t>(DistCalcMethod::Undefined) != 0,
              "Empty DistCalcMethod!");

enum class VectorValueType : std::uint8_t {
#define DefineVectorValueType(Name, Type) Name,
#include "DefinitionList.h"
#undef DefineVectorValueType

    Undefined
};
static_assert(static_cast<std::uint8_t>(VectorValueType::Undefined) != 0,
              "Empty VectorValueType!");

enum class IndexAlgoType : std::uint8_t {
#define DefineIndexAlgo(Name) Name,
#include "DefinitionList.h"
#undef DefineIndexAlgo

    Undefined
};
static_assert(static_cast<std::uint8_t>(IndexAlgoType::Undefined) != 0,
              "Empty IndexAlgoType!");

enum class VectorFileType : std::uint8_t {
#define DefineVectorFileType(Name) Name,
#include "DefinitionList.h"
#undef DefineVectorFileType

    Undefined
};
static_assert(static_cast<std::uint8_t>(VectorFileType::Undefined) != 0,
              "Empty VectorFileType!");

enum class TruthFileType : std::uint8_t {
#define DefineTruthFileType(Name) Name,
#include "DefinitionList.h"
#undef DefineTruthFileType

    Undefined
};
static_assert(static_cast<std::uint8_t>(TruthFileType::Undefined) != 0,
              "Empty TruthFileType!");

template <typename T>
constexpr VectorValueType GetEnumValueType() {
    return VectorValueType::Undefined;
}

#define DefineVectorValueType(Name, Type)                \
    template <>                                          \
    constexpr VectorValueType GetEnumValueType<Type>() { \
        return VectorValueType::Name;                    \
    }

#include "DefinitionList.h"
#undef DefineVectorValueType

inline std::size_t GetValueTypeSize(VectorValueType p_valueType) {
    switch (p_valueType) {
#define DefineVectorValueType(Name, Type) \
    case VectorValueType::Name:           \
        return sizeof(Type);

#include "DefinitionList.h"
#undef DefineVectorValueType

        default:
            break;
    }

    return 0;
}

enum class QuantizerType : std::uint8_t {
#define DefineQuantizerType(Name, Type) Name,
#include "DefinitionList.h"
#undef DefineQuantizerType

    Undefined
};
static_assert(static_cast<std::uint8_t>(QuantizerType::Undefined) != 0,
              "Empty QuantizerType!");

enum class NumaStrategy : std::uint8_t {
#define DefineNumaStrategy(Name) Name,
#include "DefinitionList.h"
#undef DefineNumaStrategy

    Undefined
};
static_assert(static_cast<std::uint8_t>(NumaStrategy::Undefined) != 0,
              "Empty NumaStrategy!");

enum class OrderStrategy : std::uint8_t {
#define DefineOrderStrategy(Name) Name,
#include "DefinitionList.h"
#undef DefineOrderStrategy

    Undefined
};
static_assert(static_cast<std::uint8_t>(OrderStrategy::Undefined) != 0,
              "Empty OrderStrategy!");

}  // namespace SPTAG

#endif  // _SPTAG_CORE_COMMONDEFS_H_
