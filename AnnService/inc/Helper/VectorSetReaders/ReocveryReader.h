#pragma once

#include <functional>

#include "inc/Core/SPMerge.h"
#include "inc/Helper/VectorSetReader.h"

namespace SPTAG {
namespace Helper {

class RecoveryReader : public VectorSetReader {
   public:
    explicit RecoveryReader(std::shared_ptr<VectorSet> vectorset)
        : VectorSetReader(nullptr), vectorset(vectorset) {}

    virtual ~RecoveryReader() = default;

    virtual ErrorCode LoadFile(const std::string &p_filePaths) {
        return ErrorCode::Success;
    };

    virtual std::shared_ptr<VectorSet> GetVectorSet(SizeType start = 0,
                                                    SizeType end = -1) const {
        return vectorset;
    }

    virtual std::shared_ptr<MetadataSet> GetMetadataSet() const {
        NOT_IMPLEMENTED();
    }

   private:
    std::shared_ptr<VectorSet> vectorset;
};

}  // namespace Helper
}  // namespace SPTAG