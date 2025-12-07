// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SPANN_OPTIONS_H_
#define _SPTAG_SPANN_OPTIONS_H_

#include <string>

#include "inc/Core/Common.h"
#include "inc/Core/SPMerge.h"
#include "inc/Helper/CommonHelper.h"
#include "inc/Helper/StringConvert.h"
#include "inc/SSDServing/main.h"

namespace SPTAG {
namespace SPANN {

class Options {
   public:
    VectorValueType m_valueType;
    DistCalcMethod m_distCalcMethod;
    IndexAlgoType m_indexAlgoType;
    DimensionType m_dim;
    std::string m_vectorPath;
    VectorFileType m_vectorType;
    SizeType m_vectorSize;          // Optional on condition
    std::string m_vectorDelimiter;  // Optional on condition
    std::string m_queryPath;
    VectorFileType m_queryType;
    SizeType m_querySize;          // Optional on condition
    std::string m_queryDelimiter;  // Optional on condition
    std::string m_warmupPath;
    VectorFileType m_warmupType;
    SizeType m_warmupSize;          // Optional on condition
    std::string m_warmupDelimiter;  // Optional on condition
    std::string m_truthPath;
    TruthFileType m_truthType;
    bool m_generateTruth;
    std::string m_indexDirectory;
    std::string m_headIDFile;
    std::string m_headVectorFile;
    std::string m_headIndexFolder;
    std::string m_deleteIDFile;
    std::string m_ssdIndex;
    bool m_deleteHeadVectors;
    int m_ssdIndexFileNum;
    std::string m_quantizerFilePath;

    // Section 2: for selecting head
    bool m_selectHead;
    int m_iTreeNumber;
    int m_iBKTKmeansK;
    int m_iBKTLeafSize;
    int m_iSamples;
    float m_fBalanceFactor;
    int m_iSelectHeadNumberOfThreads;
    bool m_saveBKT;
    // analyze
    bool m_analyzeOnly;
    bool m_calcStd;
    bool m_selectDynamically;
    bool m_noOutput;
    // selection factors
    int m_selectThreshold;
    int m_splitFactor;
    int m_splitThreshold;
    int m_maxRandomTryCount;
    double m_ratio;
    int m_headVectorCount;
    bool m_recursiveCheckSmallCluster;
    bool m_printSizeCount;
    std::string m_selectType;
    // Dataset constructor args
    int m_datasetRowsInBlock;
    int m_datasetCapacity;

    // Section 3: for build head
    bool m_buildHead;

    // Section 4: for build ssd and search ssd
    bool m_enableSSD;
    bool m_buildSsdIndex;
    int m_iSSDNumberOfThreads;
    bool m_enableDeltaEncoding;
    bool m_enablePostingListRearrange;
    bool m_enableDataCompression;
    bool m_enableDictTraining;
    int m_minDictTraingBufferSize;
    int m_dictBufferCapacity;
    int m_zstdCompressLevel;

    // Building
    int m_replicaCount;
    int m_postingPageLimit;
    int m_internalResultNum;
    bool m_outputEmptyReplicaID;
    int m_batches;
    std::string m_tmpdir;
    float m_rngFactor;
    int m_samples;
    bool m_excludehead;
    int m_postingVectorLimit;

    // GPU building
    int m_gpuSSDNumTrees;
    int m_gpuSSDLeafSize;
    int m_numGPUs;

    // Searching
    std::string m_searchResult;
    std::string m_logFile;
    int m_qpsLimit;
    int m_resultNum;
    int m_truthResultNum;
    int m_queryCountLimit;
    int m_maxCheck;
    int m_hashExp;
    float m_maxDistRatio;
    int m_ioThreads;
    int m_searchPostingPageLimit;
    int m_searchInternalResultNum;
    int m_rerank;
    bool m_recall_analysis;
    int m_debugBuildInternalResultNum;
    bool m_enableADC;
    int m_iotimeout;
    bool m_searchPruning;
    std::string m_monitorOutput;
    int m_loopNum;
    std::string m_modelPath;
    std::vector<int> m_errorMachineList;
    // Iterative
    int m_headBatch;

    // Section: Redundancy Merger
    bool m_enableMerger;
    bool m_loadMergerResult;
    SPMerge::MachineID m_curMid;
    SPMerge::MachineID m_numMachine;
    SPMerge::MachineID m_numSplit;
    SPMerge::RecoveryMethod m_recoveryMethod;
    std::string m_savePath;
    std::string m_recoveryPath;
    std::string m_indexFileFormat;
    std::string m_patchFileFormat;
    // ScanWrite
    int m_recoveryNumThreads;
    size_t m_numVecBuffers;
    // parallel recovery
    int m_numLoadBuffers;
    int m_numReaderThreads;
    int m_numWriterThreads;
    // feed model
    std::vector<int> m_nprobeRange;

    // Section: Seperated Index
    SSDServing::RPC_Method m_rpcMethod;
    SSDServing::RPC_Way m_rpcWay;
    // for server
    int m_listeningPort;
    // for client
    std::vector<std::string> m_serverAddrList;

    Options() {
#define DefineBasicParameter(VarName, VarType, DefaultValue, RepresentStr) \
    VarName = DefaultValue;

#define DefineSelectHeadParameter(VarName, VarType, DefaultValue, \
                                  RepresentStr)                   \
    VarName = DefaultValue;

#define DefineBuildHeadParameter(VarName, VarType, DefaultValue, RepresentStr) \
    VarName = DefaultValue;

#define DefineSSDParameter(VarName, VarType, DefaultValue, RepresentStr) \
    VarName = DefaultValue;

#define DefineMergerParameter(VarName, VarType, DefaultValue, RepresentStr) \
    VarName = DefaultValue;

#define DefineSeperatedIndexParameter(VarName, VarType, DefaultValue, \
                                      RepresentStr)                   \
    VarName = DefaultValue;

#include "inc/Core/SPANN/ParameterDefinitionList.h"

#undef DefineBasicParameter
#undef DefineSelectHeadParameter
#undef DefineBuildHeadParameter
#undef DefineSSDParameter
#undef DefineMergerParameter
    }

    ~Options() {}

    ErrorCode SetParameter(const char* p_section, const char* p_param,
                           const char* p_value) {
        if (nullptr == p_section || nullptr == p_param || nullptr == p_value)
            return ErrorCode::Fail;

        if (Helper::StrUtils::StrEqualIgnoreCase(p_section, "Base")) {
#define DefineBasicParameter(VarName, VarType, DefaultValue, RepresentStr)    \
    if (Helper::StrUtils::StrEqualIgnoreCase(p_param, RepresentStr)) {        \
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Setting %s with value %s\n", \
                     RepresentStr, p_value);                                  \
        VarType tmp;                                                          \
        if (Helper::Convert::ConvertStringTo<VarType>(p_value, tmp)) {        \
            VarName = tmp;                                                    \
        }                                                                     \
    } else

#include "inc/Core/SPANN/ParameterDefinitionList.h"
#undef DefineBasicParameter

            ;
        } else if (Helper::StrUtils::StrEqualIgnoreCase(p_section,
                                                        "SelectHead")) {
#define DefineSelectHeadParameter(VarName, VarType, DefaultValue,             \
                                  RepresentStr)                               \
    if (Helper::StrUtils::StrEqualIgnoreCase(p_param, RepresentStr)) {        \
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Setting %s with value %s\n", \
                     RepresentStr, p_value);                                  \
        VarType tmp;                                                          \
        if (Helper::Convert::ConvertStringTo<VarType>(p_value, tmp)) {        \
            VarName = tmp;                                                    \
        }                                                                     \
    } else

#include "inc/Core/SPANN/ParameterDefinitionList.h"
#undef DefineSelectHeadParameter

            ;
        } else if (Helper::StrUtils::StrEqualIgnoreCase(p_section,
                                                        "BuildHead")) {
#define DefineBuildHeadParameter(VarName, VarType, DefaultValue, RepresentStr) \
    if (Helper::StrUtils::StrEqualIgnoreCase(p_param, RepresentStr)) {         \
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Setting %s with value %s\n",  \
                     RepresentStr, p_value);                                   \
        VarType tmp;                                                           \
        if (Helper::Convert::ConvertStringTo<VarType>(p_value, tmp)) {         \
            VarName = tmp;                                                     \
        }                                                                      \
    } else

#include "inc/Core/SPANN/ParameterDefinitionList.h"
#undef DefineBuildHeadParameter

            ;
        } else if (Helper::StrUtils::StrEqualIgnoreCase(p_section,
                                                        "BuildSSDIndex")) {
#define DefineSSDParameter(VarName, VarType, DefaultValue, RepresentStr)      \
    if (Helper::StrUtils::StrEqualIgnoreCase(p_param, RepresentStr)) {        \
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Setting %s with value %s\n", \
                     RepresentStr, p_value);                                  \
        VarType tmp;                                                          \
        if (Helper::Convert::ConvertStringTo<VarType>(p_value, tmp)) {        \
            VarName = tmp;                                                    \
        }                                                                     \
    } else

#include "inc/Core/SPANN/ParameterDefinitionList.h"
#undef DefineSSDParameter

            ;
        } else if (Helper::StrUtils::StrEqualIgnoreCase(p_section,
                                                        "RedundancyMerger")) {
#define DefineMergerParameter(VarName, VarType, DefaultValue, RepresentStr)   \
    if (Helper::StrUtils::StrEqualIgnoreCase(p_param, RepresentStr)) {        \
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Setting %s with value %s\n", \
                     RepresentStr, p_value);                                  \
        VarType tmp;                                                          \
        if (Helper::Convert::ConvertStringTo<VarType>(p_value, tmp)) {        \
            VarName = tmp;                                                    \
        }                                                                     \
    } else

#include "inc/Core/SPANN/ParameterDefinitionList.h"
#undef DefineMergerParameter
            ;
        } else if (Helper::StrUtils::StrEqualIgnoreCase(p_section,
                                                        "SeperatedIndex")) {
#define DefineSeperatedIndexParameter(VarName, VarType, DefaultValue,         \
                                      RepresentStr)                           \
    if (Helper::StrUtils::StrEqualIgnoreCase(p_param, RepresentStr)) {        \
        SPTAGLIB_LOG(Helper::LogLevel::LL_Info, "Setting %s with value %s\n", \
                     RepresentStr, p_value);                                  \
        VarType tmp;                                                          \
        if (Helper::Convert::ConvertStringTo<VarType>(p_value, tmp)) {        \
            VarName = tmp;                                                    \
        }                                                                     \
    } else

#include "inc/Core/SPANN/ParameterDefinitionList.h"
#undef DefineSeperatedIndexParameter
            ;
        } else {
            LOG_ERROR("unknown section: %s\n", p_section);
        }
        return ErrorCode::Success;
    }

    std::string GetParameter(const char* p_section, const char* p_param) const {
        if (nullptr == p_section || nullptr == p_param) return std::string();

        if (Helper::StrUtils::StrEqualIgnoreCase(p_section, "Base")) {
#define DefineBasicParameter(VarName, VarType, DefaultValue, RepresentStr) \
    if (Helper::StrUtils::StrEqualIgnoreCase(p_param, RepresentStr)) {     \
        return SPTAG::Helper::Convert::ConvertToString(VarName);           \
    } else

#include "inc/Core/SPANN/ParameterDefinitionList.h"
#undef DefineBasicParameter

            ;
        } else if (Helper::StrUtils::StrEqualIgnoreCase(p_section,
                                                        "SelectHead")) {
#define DefineSelectHeadParameter(VarName, VarType, DefaultValue,      \
                                  RepresentStr)                        \
    if (Helper::StrUtils::StrEqualIgnoreCase(p_param, RepresentStr)) { \
        return SPTAG::Helper::Convert::ConvertToString(VarName);       \
    } else

#include "inc/Core/SPANN/ParameterDefinitionList.h"
#undef DefineSelectHeadParameter

            ;
        } else if (Helper::StrUtils::StrEqualIgnoreCase(p_section,
                                                        "BuildHead")) {
#define DefineBuildHeadParameter(VarName, VarType, DefaultValue, RepresentStr) \
    if (Helper::StrUtils::StrEqualIgnoreCase(p_param, RepresentStr)) {         \
        return SPTAG::Helper::Convert::ConvertToString(VarName);               \
    } else

#include "inc/Core/SPANN/ParameterDefinitionList.h"
#undef DefineBuildHeadParameter

            ;
        } else if (Helper::StrUtils::StrEqualIgnoreCase(p_section,
                                                        "BuildSSDIndex")) {
#define DefineSSDParameter(VarName, VarType, DefaultValue, RepresentStr) \
    if (Helper::StrUtils::StrEqualIgnoreCase(p_param, RepresentStr)) {   \
        return SPTAG::Helper::Convert::ConvertToString(VarName);         \
    } else

#include "inc/Core/SPANN/ParameterDefinitionList.h"
#undef DefineSSDParameter

            ;
        } else if (Helper::StrUtils::StrEqualIgnoreCase(p_section,
                                                        "RedundancyMerger")) {
#define DefineMergerParameter(VarName, VarType, DefaultValue, RepresentStr) \
    if (Helper::StrUtils::StrEqualIgnoreCase(p_param, RepresentStr)) {      \
        return SPTAG::Helper::Convert::ConvertToString(VarName);            \
    } else

#include "inc/Core/SPANN/ParameterDefinitionList.h"
#undef DefineMergerParameter

            ;
        } else if (Helper::StrUtils::StrEqualIgnoreCase(p_section,
                                                        "SeperatedIndex")) {
#define DefineSeperatedIndexParameter(VarName, VarType, DefaultValue,  \
                                      RepresentStr)                    \
    if (Helper::StrUtils::StrEqualIgnoreCase(p_param, RepresentStr)) { \
        return SPTAG::Helper::Convert::ConvertToString(VarName);       \
    } else

#include "inc/Core/SPANN/ParameterDefinitionList.h"
#undef DefineSeperatedIndexParameter

            ;
        }
        return std::string();
    }
};
}  // namespace SPANN
}  // namespace SPTAG

#endif  // _SPTAG_SPANN_OPTIONS_H_