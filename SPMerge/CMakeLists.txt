set(SPMergeDir ${PROJECT_SOURCE_DIR}/SPMerge)

include_directories(${SPMergeDir})
include_directories(${PROJECT_SOURCE_DIR}/ThirdParty/tqdm)
include_directories(${PROJECT_SOURCE_DIR}/AnnService)

add_library(MergerLib STATIC
    ${SPMergeDir}/Merger.h
    ${SPMergeDir}/Merger.cpp
)
target_link_libraries(MergerLib SPTAGLibStatic)

add_executable(merger-test MergerTest.cpp)
target_link_libraries(merger-test MergerLib)

add_executable(recovery-test RecoveryTest.cpp)
target_link_libraries(recovery-test SPTAGLibStatic)

add_executable(output-helper OutputHelper.cpp)
target_link_libraries(output-helper SPTAGLibStatic)

add_executable(feed-model FeedModel.cpp FeedModel.h)
target_link_libraries(feed-model SPTAGLibStatic absl::flat_hash_set)

add_executable(rescue-test RescueTest.cpp)
add_executable(victim-test VictimTest.cpp)
add_executable(primary-rescue-test PrimaryRescueTest.cpp)
add_executable(second-rescue-test SecondRescueTest.cpp)
target_link_libraries(rescue-test SPTAGLibStatic)
target_link_libraries(victim-test SPTAGLibStatic)
target_link_libraries(primary-rescue-test SPTAGLibStatic)
target_link_libraries(second-rescue-test SPTAGLibStatic)


add_custom_target(SPMergeTest DEPENDS
    merger-test
    recovery-test
    output-helper
    rescue-test
    victim-test
    primary-rescue-test
    second-rescue-test
    COMMENT "Building all test targets for SPMerge"
)
