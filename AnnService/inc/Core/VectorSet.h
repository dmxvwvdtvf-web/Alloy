// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_VECTORSET_H_
#define _SPTAG_VECTORSET_H_

#include "CommonDataStructure.h"
#include "inc/Core/Common.h"
#include "inc/Core/SPMerge.h"
#include "inc/Helper/MemoryManager.h"
#include "inc/Helper/Timer.h"

namespace SPTAG {

class VectorSet {
   public:
    VectorSet();

    virtual ~VectorSet();

    virtual VectorValueType GetValueType() const = 0;

    virtual void* GetVector(SizeType p_vectorID) const = 0;

    virtual void* GetData() = 0;

    virtual DimensionType Dimension() const = 0;

    virtual SizeType Count() const = 0;

    virtual bool Available() const = 0;

    virtual ErrorCode Save(const std::string& p_vectorFile) const = 0;

    virtual ErrorCode AppendSave(const std::string& p_vectorFile) const = 0;

    virtual SizeType PerVectorDataSize() const = 0;

    virtual void Normalize(int p_threads) = 0;
};

class BasicVectorSet : public VectorSet {
   public:
    BasicVectorSet(const ByteArray& p_bytesArray, VectorValueType p_valueType,
                   DimensionType p_dimension, SizeType p_vectorCount);

    virtual ~BasicVectorSet();

    virtual VectorValueType GetValueType() const;

    virtual void* GetVector(SizeType p_vectorID) const;

    virtual void* GetData();

    virtual DimensionType Dimension() const;

    virtual SizeType Count() const;

    virtual bool Available() const;

    virtual ErrorCode Save(const std::string& p_vectorFile) const;

    virtual ErrorCode AppendSave(const std::string& p_vectorFile) const;

    virtual SizeType PerVectorDataSize() const;

    virtual void Normalize(int p_threads);

   private:
    ByteArray m_data;

    VectorValueType m_valueType;

    DimensionType m_dimension;

    SizeType m_vectorCount;

    size_t m_perVectorDataSize;
};

class RecoveryInMemVectorset : public VectorSet {
   public:
    explicit RecoveryInMemVectorset(VectorValueType p_valueType,
                                    DimensionType p_dimension, size_t num_head,
                                    size_t num_vec)
        : m_valueType(p_valueType),
          m_dimension(p_dimension),
          m_perVectorDataSize(static_cast<SizeType>(
              p_dimension * GetValueTypeSize(p_valueType))),
          m_numHead(num_head),
          m_numVec(num_vec),
          m_VID2PtrMap(num_head + num_vec, nullptr) {}

    virtual ~RecoveryInMemVectorset() = default;

    virtual VectorValueType GetValueType() const { return m_valueType; }

    virtual void* GetVector(SizeType p_vectorID) const {
        if (m_VID2PtrMap.at(p_vectorID) == nullptr) {
            LOG_ERROR("Vector with vid=%llu cannot be found!\n", p_vectorID);
            throw std::invalid_argument("");
        }
        return m_VID2PtrMap.at(p_vectorID);
    }

    virtual DimensionType Dimension() const { return m_dimension; }

    virtual SizeType Count() const { return m_numHead + m_numVec; }

    virtual SizeType PerVectorDataSize() const { return m_perVectorDataSize; }

    virtual bool Available() const { return true; }

    virtual void* GetData() { return static_cast<void*>(m_VID2PtrMap.data()); }

    virtual ErrorCode Save(const std::string& p_vectorFile) const {
        NOT_IMPLEMENTED();
    }

    virtual ErrorCode AppendSave(const std::string& p_vectorFile) const {
        NOT_IMPLEMENTED();
    }

    virtual void Normalize(int p_threads) { NOT_IMPLEMENTED(); }

   private:
    VectorValueType m_valueType;

    DimensionType m_dimension;

    size_t m_perVectorDataSize;
    size_t m_numHead;
    size_t m_numVec;

    Helper::vector_recovery<void*> m_VID2PtrMap;
};

class RecoveryStupidVectorset : public VectorSet {
   public:
    struct LoadHelpers {
        SimpleIOHelper sih;
        Helper::tracked_buffer buffer;
        SPMerge::ClusterID cid_in_buffer;
        size_t n_diskIO{0};
        size_t n_parse{0};
        Helper::EventTimer timer;
        std::function<void(uint64_t&, uint64_t&, int, int)> f_parsePosting;

        LoadHelpers(
            std::string filename, size_t max_list_pages,
            std::function<void(uint64_t&, uint64_t&, int, int)> f_parsePosting)
            : sih(filename, SimpleIOHelper::ReadOnly),
              buffer(Helper::make_tracked_buffer(max_list_pages << PageSizeEx)),
              f_parsePosting(f_parsePosting) {}
    };

    explicit RecoveryStupidVectorset(
        VectorValueType p_valueType, DimensionType p_dimension, size_t num_head,
        size_t num_vec, std::string filename, size_t max_list_pages,
        std::function<void(uint64_t&, uint64_t&, int, int)> f_parsePosting)
        : m_valueType(p_valueType),
          m_dimension(p_dimension),
          m_perVectorDataSize(static_cast<SizeType>(
              p_dimension * GetValueTypeSize(p_valueType))),
          m_numHead(num_head),
          m_numVec(num_vec),
          m_vectorPosition(num_head + num_vec),
          m_loadHelper(
              new LoadHelpers(filename, max_list_pages, f_parsePosting)) {
        LOG_INFO("%s: initialize disk load buffer with %lu pages\n",
                 __FUNCTION__, max_list_pages);
    }

    virtual ~RecoveryStupidVectorset() {
        LOG_INFO("%s: %llu disk IO and %llu parsing!\n", __FUNCTION__,
                 m_loadHelper->n_diskIO, m_loadHelper->n_parse);
        m_loadHelper->timer.log_all_event_duration_and_clear();
    }

    virtual VectorValueType GetValueType() const { return m_valueType; }

    virtual void* GetVector(SizeType p_vectorID) const;

    virtual DimensionType Dimension() const { return m_dimension; }

    virtual SizeType Count() const { return m_numHead + m_numVec; }

    virtual SizeType PerVectorDataSize() const { return m_perVectorDataSize; }

    virtual bool Available() const { return true; }

    virtual void* GetData() { return static_cast<void*>(&m_vectorPosition); }

    virtual ErrorCode Save(const std::string& p_vectorFile) const {
        NOT_IMPLEMENTED();
    }

    virtual ErrorCode AppendSave(const std::string& p_vectorFile) const {
        NOT_IMPLEMENTED();
    }

    virtual void Normalize(int p_threads) { NOT_IMPLEMENTED(); }

   private:
    VectorValueType m_valueType;

    DimensionType m_dimension;

    size_t m_perVectorDataSize;
    size_t m_numHead;
    size_t m_numVec;

    Helper::vector_recovery<SPMerge::VectorPosition> m_vectorPosition;

    std::unique_ptr<LoadHelpers> m_loadHelper;
};

}  // namespace SPTAG

#endif  // _SPTAG_VECTORSET_H_
