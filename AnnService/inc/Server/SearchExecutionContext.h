// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SERVER_SEARCHEXECUTIONCONTEXT_H_
#define _SPTAG_SERVER_SEARCHEXECUTIONCONTEXT_H_

#include <memory>
#include <string>
#include <vector>

#include "inc/Core/SearchQuery.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Socket/RemoteSearchQuery.h"
#include "QueryParser.h"
#include "ServiceSettings.h"

namespace SPTAG {
namespace Service {

typedef Socket::IndexSearchResult SearchResult;

class SearchExecutionContext {
   public:
    SearchExecutionContext(
        const std::shared_ptr<const ServiceSettings>& p_serviceSettings);

    ~SearchExecutionContext();

    ErrorCode ParseQuery(const std::string& p_query);

    ErrorCode ExtractOption();

    ErrorCode ExtractVector(VectorValueType p_targetType);

    void AddResults(std::string p_indexName, QueryResult& p_results);

    std::vector<SearchResult>& GetResults();

    const std::vector<SearchResult>& GetResults() const;

    const ByteArray& GetVector() const;

    const std::vector<std::string>& GetSelectedIndexNames() const;

    const SizeType GetVectorDimension() const;

    const std::vector<QueryParser::OptionPair>& GetOptions() const;

    const SizeType GetResultNum() const;

    const bool GetExtractMetadata() const;

   private:
    const std::shared_ptr<const ServiceSettings> c_serviceSettings;

    QueryParser m_queryParser;

    std::vector<std::string> m_indexNames;

    ByteArray m_vector;

    SizeType m_vectorDimension;

    std::vector<SearchResult> m_results;

    VectorValueType m_inputValueType;

    bool m_extractMetadata;

    SizeType m_resultNum;
};

}  // namespace Service
}  // namespace SPTAG

#endif  // _SPTAG_SERVER_SEARCHEXECUTIONCONTEXT_H_
