// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Core/VectorSet.h"

#include "inc/Core/Common/CommonUtils.h"
#include "inc/Core/SPANN/ExtraFullGraphSearcher.h"

using namespace SPTAG;
using SPANN::SPANNVID;

VectorSet::VectorSet() {}

VectorSet::~VectorSet() {}

void* RecoveryStupidVectorset::GetVector(SizeType p_vectorID) const {
    const auto& info = m_vectorPosition[p_vectorID];
    switch (info.where) {
        case SPMerge::VectorPosition::Memory:
            return info.mem_pos.ptr;
        case SPMerge::VectorPosition::Disk: {
            auto& list_info = *info.disk_pos.list_info;
            if (info.disk_pos.cid != m_loadHelper->cid_in_buffer) {
                m_loadHelper->timer.event_start("load cluster from disk",
                                                false);
                auto totalBytes = (static_cast<size_t>(list_info.listPageCount)
                                   << PageSizeEx);
                auto offset = list_info.listOffset;
                m_loadHelper->sih.f_read_data(m_loadHelper->buffer.get(),
                                              totalBytes, offset);
                m_loadHelper->timer.event_stop("load cluster from disk");
                m_loadHelper->cid_in_buffer = info.disk_pos.cid;
                m_loadHelper->n_diskIO++;
            }
            void* start_ptr = m_loadHelper->buffer.get() + list_info.pageOffset;
            uint64_t offset_vid;
            uint64_t offset_vec;
            m_loadHelper->timer.event_start("find vector in mem", false);
            m_loadHelper->n_parse++;
            for (int i = 0; i < list_info.listEleCount; i++) {
                m_loadHelper->f_parsePosting(offset_vid, offset_vec, i,
                                             list_info.listEleCount);
                auto vid = *reinterpret_cast<SPANNVID*>(start_ptr + offset_vid);
                if (vid == p_vectorID) {
                    m_loadHelper->timer.event_stop("find vector in mem");
                    return start_ptr + offset_vec;
                }
            }
        }
        default:
            LOG_ERROR("Vector with vid=%llu cannot be found!\n", p_vectorID);
            exit(-1);
    }
}

BasicVectorSet::BasicVectorSet(const ByteArray& p_bytesArray,
                               VectorValueType p_valueType,
                               DimensionType p_dimension,
                               SizeType p_vectorCount)
    : m_data(p_bytesArray),
      m_valueType(p_valueType),
      m_dimension(p_dimension),
      m_vectorCount(p_vectorCount),
      m_perVectorDataSize(
          static_cast<SizeType>(p_dimension * GetValueTypeSize(p_valueType))) {}

BasicVectorSet::~BasicVectorSet() {}

VectorValueType BasicVectorSet::GetValueType() const { return m_valueType; }

void* BasicVectorSet::GetVector(SizeType p_vectorID) const {
    if (p_vectorID < 0 || p_vectorID >= m_vectorCount) {
        return nullptr;
    }

    return reinterpret_cast<void*>(m_data.Data() +
                                   ((size_t)p_vectorID) * m_perVectorDataSize);
}

void* BasicVectorSet::GetData() {
    return reinterpret_cast<void*>(m_data.Data());
}

DimensionType BasicVectorSet::Dimension() const { return m_dimension; }

SizeType BasicVectorSet::Count() const { return m_vectorCount; }

bool BasicVectorSet::Available() const { return m_data.Data() != nullptr; }

ErrorCode BasicVectorSet::Save(const std::string& p_vectorFile) const {
    auto fp = SPTAG::f_createIO();
    if (fp == nullptr ||
        !fp->Initialize(p_vectorFile.c_str(), std::ios::binary | std::ios::out))
        return ErrorCode::FailedOpenFile;

    IOBINARY(fp, WriteBinary, sizeof(SizeType), (char*)&m_vectorCount);
    IOBINARY(fp, WriteBinary, sizeof(DimensionType), (char*)&m_dimension);
    IOBINARY(fp, WriteBinary, m_data.Length(), (char*)m_data.Data());
    return ErrorCode::Success;
}

ErrorCode BasicVectorSet::AppendSave(const std::string& p_vectorFile) const {
    auto append = fileexists(p_vectorFile.c_str());

    SizeType count;
    SizeType dim;

    // Get count based on already written results
    if (append) {
        auto fp_read = SPTAG::f_createIO();
        if (fp_read == nullptr ||
            !fp_read->Initialize(p_vectorFile.c_str(),
                                 std::ios::binary | std::ios::in))
            return ErrorCode::FailedOpenFile;
        IOBINARY(fp_read, ReadBinary, sizeof(SizeType), (char*)&count);
        IOBINARY(fp_read, ReadBinary, sizeof(DimensionType), (char*)&dim);
        if (dim != m_dimension) {
            return ErrorCode::DimensionSizeMismatch;
        }
        count += m_vectorCount;
    } else {
        count = m_vectorCount;
        dim = m_dimension;
    }

    // Update count header
    {
        auto fp_write = SPTAG::f_createIO();
        if (fp_write == nullptr ||
            !fp_write->Initialize(
                p_vectorFile.c_str(),
                std::ios::binary | std::ios::out | (append ? std::ios::in : 0)))
            return ErrorCode::FailedOpenFile;
        IOBINARY(fp_write, WriteBinary, sizeof(SizeType), (char*)&count);
        IOBINARY(fp_write, WriteBinary, sizeof(DimensionType), (char*)&dim);
    }

    // Write new vectors to end of file
    {
        auto fp_append = SPTAG::f_createIO();
        if (fp_append == nullptr ||
            !fp_append->Initialize(p_vectorFile.c_str(), std::ios::binary |
                                                             std::ios::out |
                                                             std::ios::app))
            return ErrorCode::FailedOpenFile;
        IOBINARY(fp_append, WriteBinary, m_data.Length(), (char*)m_data.Data());
    }

    return ErrorCode::Success;
}

SizeType BasicVectorSet::PerVectorDataSize() const {
    return (SizeType)m_perVectorDataSize;
}

void BasicVectorSet::Normalize(int p_threads) {
    switch (m_valueType) {
#define DefineVectorValueType(Name, Type)                                   \
    case SPTAG::VectorValueType::Name:                                      \
        SPTAG::COMMON::Utils::BatchNormalize<Type>(                         \
            reinterpret_cast<Type*>(m_data.Data()), m_vectorCount,          \
            m_dimension, SPTAG::COMMON::Utils::GetBase<Type>(), p_threads); \
        break;

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType
        default:
            break;
    }
}