#include <arpa/inet.h>

#include <stdexcept>
#include <string>

#include "inc/Core/Common.h"
#include "inc/Core/SPANN/Index.h"
#include "inc/Core/SPMerge.h"
#include "inc/Core/VectorIndex.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/SSDServing/main.h"

using namespace SPTAG;
using namespace SPTAG::SPANN;
using namespace SPTAG::SSDServing;

int main(int argc, char* argv[]) {
    if (argc < 5) {
        printf("Usage: %s [config file] [fail mid] [ip] [port]", argv[0]);
        return -1;
    }

    std::map<std::string, std::map<std::string, std::string>> config_map;

    VectorValueType value_type = VectorValueType::Undefined;
    {
        Helper::IniReader iniReader;
        iniReader.LoadIniFile(argv[1]);
        config_map[SEC_BASE] = iniReader.GetParameters(SEC_BASE);
        config_map[SEC_REDUNDANCY_MERGER] =
            iniReader.GetParameters(SEC_REDUNDANCY_MERGER);
        config_map[SEC_SEPERATED_INDEX] =
            iniReader.GetParameters(SEC_SEPERATED_INDEX);

        config_map[SEC_REDUNDANCY_MERGER]["loadmergerresult"] = "true";

        value_type = iniReader.GetParameter(SEC_BASE, "ValueType", value_type);
    }

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

    index->LoadSSDIndex();

    // NOTE: should receive from rpc
    SPMerge::MachineID mid = std::stoi(argv[2]);
    std::string fail_ip(argv[3]);
    int fail_port = std::stoi(argv[4]);

    clear_page_cache();
    index->HelpRecovery(mid, fail_ip, fail_port);

    return 0;
}
