#include "inc/Helper/DiskIO.h"

#include <omp.h>

#include <cassert>
#include <cstdint>
#include <cstring>

#include "inc/Core/Common.h"

namespace SPTAG {
namespace Helper {

bool ChunkedBufferIO::Initialize(std::uint64_t totalSize, int threads,
                                 size_t chunkSize) {
    m_totalSize = totalSize;
    m_chunkSize = chunkSize;
    m_pos = 0;

    size_t n_chunks = totalSize / chunkSize;
    size_t remainder = totalSize % chunkSize;

    m_chunks.resize(n_chunks + (remainder != 0 ? 1 : 0));

    if (threads == -1) threads = omp_get_max_threads();

#pragma omp parallel for if (threads > 1) num_threads(threads)
    for (int i = 0; i < n_chunks; i++) {
        m_chunks[i].reset(static_cast<char*>(PAGE_ALLOC(chunkSize)),
                          [](char* ptr) { PAGE_FREE(ptr); });
        memset(m_chunks[i].get(), 0, chunkSize);
    }
    if (remainder > 0) {
        m_chunks[n_chunks].reset(static_cast<char*>(PAGE_ALLOC(remainder)),
                                 [](char* ptr) { PAGE_FREE(ptr); });
        memset(m_chunks[n_chunks].get(), 0, remainder);
    }
    return true;
};

void ChunkedBufferIO::Output(SimpleIOHelper& sio, uint64_t start_offset,
                             int num_threads, size_t bs) {
    const auto ALIGNMENT = 512;
    assert(start_offset % ALIGNMENT == 0);
    assert(bs % ALIGNMENT == 0);

#pragma omp parallel for num_threads(num_threads)
    for (auto& info : report()) {
        assert(reinterpret_cast<uint64_t>(info.ptr) % ALIGNMENT == 0);
        assert(info.left % ALIGNMENT == 0);
        assert(info.off % ALIGNMENT == 0);
        // sio.f_write_data(info.ptr, info.left, start_offset + info.off);

        uint64_t sub_off = 0;
        for (size_t i = 0; i < info.left / bs; i++) {
            sio.f_write_data(info.ptr + sub_off, bs,
                             start_offset + info.off + sub_off);
            sub_off += bs;
        }
        if (sub_off != info.left) {
            sio.f_write_data(info.ptr + sub_off, info.left - sub_off,
                             start_offset + info.off + sub_off);
        }
    }
}

}  // namespace Helper

}  // namespace SPTAG