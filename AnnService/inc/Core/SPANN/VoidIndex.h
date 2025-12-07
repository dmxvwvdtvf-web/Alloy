#pragma once

#include <functional>
#include <shared_mutex>

#include "inc/Core/Common.h"
#include "inc/Core/Common/BKTree.h"
#include "inc/Core/Common/CommonUtils.h"
#include "inc/Core/Common/Dataset.h"
#include "inc/Core/Common/DistanceUtils.h"
#include "inc/Core/Common/IQuantizer.h"
#include "inc/Core/Common/Labelset.h"
#include "inc/Core/Common/QueryResultSet.h"
#include "inc/Core/Common/RelativeNeighborhoodGraph.h"
#include "inc/Core/Common/WorkSpace.h"
#include "inc/Core/Common/WorkSpacePool.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/ThreadPool.h"

namespace SPTAG {

namespace SPANN {
template <typename T>
class VoidIndex : public VectorIndex {
   private:
    VectorIndex* m_parent{nullptr};

   public:
    VoidIndex(VectorIndex* parent) : m_parent(parent) {}

    inline float ComputeDistance(const void* pX, const void* pY) const {
        return m_parent->ComputeDistance(pX, pY);
    }

    virtual ErrorCode BuildIndex(const void* p_data, SizeType p_vectorNum,
                                 DimensionType p_dimension,
                                 bool p_normalized = false,
                                 bool p_shareOwnership = false) {
        NOT_IMPLEMENTED();
    }

    virtual ErrorCode AddIndex(const void* p_data, SizeType p_vectorNum,
                               DimensionType p_dimension,
                               std::shared_ptr<MetadataSet> p_metadataSet,
                               bool p_withMetaIndex = false,
                               bool p_normalized = false) {
        NOT_IMPLEMENTED();
    }

    virtual ErrorCode DeleteIndex(const void* p_vectors, SizeType p_vectorNum) {
        NOT_IMPLEMENTED();
    }

    virtual ErrorCode SearchIndex(QueryResult& p_results,
                                  bool p_searchDeleted = false) const {
        NOT_IMPLEMENTED();
    }

    virtual std::shared_ptr<ResultIterator> GetIterator(
        const void* p_target, bool p_searchDeleted = false) const {
        NOT_IMPLEMENTED();
    }

    virtual ErrorCode SearchIndexIterativeNext(QueryResult& p_query,
                                               COMMON::WorkSpace* workSpace,
                                               int p_batch, int& resultCount,
                                               bool p_isFirst,
                                               bool p_searchDeleted) const {
        NOT_IMPLEMENTED();
    }

    virtual ErrorCode SearchIndexIterativeEnd(
        std::unique_ptr<COMMON::WorkSpace> workSpace) const {
        NOT_IMPLEMENTED();
    }

    virtual bool SearchIndexIterativeFromNeareast(
        QueryResult& p_query, COMMON::WorkSpace* p_space, bool p_isFirst,
        bool p_searchDeleted = false) const {
        NOT_IMPLEMENTED();
    }

    virtual std::unique_ptr<COMMON::WorkSpace> RentWorkSpace(int batch) const {
        NOT_IMPLEMENTED();
    }

    virtual ErrorCode RefineSearchIndex(QueryResult& p_query,
                                        bool p_searchDeleted = false) const {
        NOT_IMPLEMENTED();
    }

    virtual ErrorCode SearchIndexWithFilter(
        QueryResult& p_query, std::function<bool(const ByteArray&)> filterFunc,
        int maxCheck = 0, bool p_searchDeleted = false) const {
        NOT_IMPLEMENTED();
    }

    virtual ErrorCode SearchTree(QueryResult& p_query) const {
        NOT_IMPLEMENTED();
    }

    virtual ErrorCode RefineIndex(std::shared_ptr<VectorIndex>& p_newIndex) {
        NOT_IMPLEMENTED();
    }

    virtual float AccurateDistance(const void* pX, const void* pY) const {
        NOT_IMPLEMENTED();
    }
    virtual float GetDistance(const void* target, const SizeType idx) const {
        NOT_IMPLEMENTED();
    }
    virtual const void* GetSample(const SizeType idx) const {
        NOT_IMPLEMENTED();
    }
    virtual bool ContainSample(const SizeType idx) const { NOT_IMPLEMENTED(); }
    virtual bool NeedRefine() const { NOT_IMPLEMENTED(); };

    virtual DimensionType GetFeatureDim() const { NOT_IMPLEMENTED(); }
    virtual SizeType GetNumSamples() const { NOT_IMPLEMENTED(); }
    virtual SizeType GetNumDeleted() const { NOT_IMPLEMENTED(); }

    virtual DistCalcMethod GetDistCalcMethod() const { NOT_IMPLEMENTED(); }
    virtual IndexAlgoType GetIndexAlgoType() const { NOT_IMPLEMENTED(); }
    virtual VectorValueType GetVectorValueType() const { NOT_IMPLEMENTED(); }

    virtual std::string GetParameter(const char* p_param,
                                     const char* p_section = nullptr) const {
        NOT_IMPLEMENTED();
    }
    virtual ErrorCode SetParameter(const char* p_param, const char* p_value,
                                   const char* p_section = nullptr) {
        NOT_IMPLEMENTED();
    }
    virtual ErrorCode UpdateIndex() { NOT_IMPLEMENTED(); }

    virtual void SetQuantizer(
        std::shared_ptr<SPTAG::COMMON::IQuantizer> quantizer) {
        NOT_IMPLEMENTED();
    }
    virtual std::shared_ptr<std::vector<std::uint64_t>> BufferSize() const {
        NOT_IMPLEMENTED();
    }

    virtual std::shared_ptr<std::vector<std::string>> GetIndexFiles() const {
        NOT_IMPLEMENTED();
    }

    virtual ErrorCode SaveConfig(std::shared_ptr<Helper::DiskIO> p_configout) {
        NOT_IMPLEMENTED();
    }

    virtual ErrorCode SaveIndexData(
        const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams) {
        NOT_IMPLEMENTED();
    }

    virtual ErrorCode LoadConfig(Helper::IniReader& p_reader) {
        NOT_IMPLEMENTED();
    }

    virtual ErrorCode LoadIndexData(
        const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams) {
        NOT_IMPLEMENTED();
    }

    virtual ErrorCode LoadIndexDataFromMemory(
        const std::vector<ByteArray>& p_indexBlobs) {
        NOT_IMPLEMENTED();
    }
    virtual ErrorCode DeleteIndex(const SizeType& p_id) { NOT_IMPLEMENTED(); }

    virtual ErrorCode RefineIndex(
        const std::vector<std::shared_ptr<Helper::DiskIO>>& p_indexStreams,
        IAbortOperation* p_abort) {
        NOT_IMPLEMENTED();
    }

    virtual ErrorCode SetWorkSpaceFactory(
        std::unique_ptr<
            SPTAG::COMMON::IWorkSpaceFactory<SPTAG::COMMON::IWorkSpace>>
            up_workSpaceFactory) {
        NOT_IMPLEMENTED();
    }
};
}  // namespace SPANN
}  // namespace SPTAG
