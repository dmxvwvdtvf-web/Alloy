#include "FeedModel.h"

#include <omp.h>

#include <climits>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "inc/Core/Common.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Core/SPMerge.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/SSDServing/main.h"

using namespace SPTAG;
using namespace SPTAG::SPANN;
using namespace SPTAG::Helper;
using namespace SPTAG::SSDServing;
using namespace SPTAG::SPMerge;

int main(int argc, char* argv[]) {
    if (argc < 2) throw std::invalid_argument("need configFilePath");
    std::vector<SPMerge::MachineID> failed_machine_list;

    std::map<std::string, std::map<std::string, std::string>> config_map;
    std::string truth_file;
    VectorValueType value_type = VectorValueType::Undefined;
    SPMerge::MachineID mid = SPMerge::InvalidMID;

    {
        Helper::IniReader iniReader;
        iniReader.LoadIniFile(argv[1]);
        config_map[SEC_BASE] = iniReader.GetParameters(SEC_BASE);
        config_map[SEC_SELECT_HEAD] = iniReader.GetParameters(SEC_SELECT_HEAD);
        config_map[SEC_BUILD_HEAD] = iniReader.GetParameters(SEC_BUILD_HEAD);
        config_map[SEC_BUILD_SSD_INDEX] =
            iniReader.GetParameters(SEC_BUILD_SSD_INDEX);
        config_map[SEC_REDUNDANCY_MERGER] =
            iniReader.GetParameters(SEC_REDUNDANCY_MERGER);

        value_type = iniReader.GetParameter(SEC_BASE, "ValueType", value_type);
        truth_file = iniReader.GetParameter(SEC_BASE, "TruthPath", truth_file);
        mid = iniReader.GetParameter(SEC_REDUNDANCY_MERGER, "MachineID", mid);

        for (auto& KV : iniReader.GetParameters(SEC_SEARCH_SSD_INDEX)) {
            std::string param = KV.first, value = KV.second;
            if (Helper::StrUtils::StrEqualIgnoreCase(param.c_str(),
                                                     "PostingPageLimit"))
                param = "SearchPostingPageLimit";
            if (Helper::StrUtils::StrEqualIgnoreCase(param.c_str(),
                                                     "InternalResultNum"))
                param = "SearchInternalResultNum";
            config_map[SEC_BUILD_SSD_INDEX][param] = value;
        }
        config_map[SEC_REDUNDANCY_MERGER]["loadmergerresult"] = true;
    }

    if (mid == SPMerge::InvalidMID) {
        throw std::logic_error("should specify mid");
    }
    if (truth_file.empty()) {
        throw std::logic_error("should set TruthPath");
    }

    // load index
    std::shared_ptr<VectorIndex> index =
        VectorIndex::CreateInstance(IndexAlgoType::SPANN, value_type);
    if (index == nullptr) {
        throw std::runtime_error("cannot create index!");
    }

    for (auto& sectionKV : config_map) {
        for (auto& KV : sectionKV.second) {
            index->SetParameter(KV.first, KV.second, sectionKV.first);
        }
    }
    index->LoadHeadIndex();
    index->LoadPatchMeta();

    omp_set_num_threads(omp_get_num_procs());

#define DefineVectorValueType(Name, Type)                                  \
    if (value_type == VectorValueType::Name) {                             \
        auto spann_index = dynamic_cast<SPANN::Index<Type>*>(index.get()); \
        FeedModelBatch(spann_index);                                       \
    }

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

    return 0;
}