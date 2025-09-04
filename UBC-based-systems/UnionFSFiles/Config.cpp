#include "Config.hpp"

namespace Config{
    const uint64_t NUM_BLOCK_MAP_RESERVE=10;
    const uint64_t NUM_LOCAL_BLOCK_RESERVE=10;
    const uint64_t NUM_FS_FILES_RESERVE=10;
    const int32_t DEFAULT_INITIAL_BLOCK_SIZE = -1;
    const int32_t FILES_SN_RESERVE_SIZE = 10;
    namespace FS_HEADER
    {
        const uint32_t HEADER_EXPECTED_SIZE=13;
        const uint32_t INPUT_FILE_INDEX = 1;
        const uint32_t NUM_FILES_INDEX = 4;
        const uint32_t PHYSICAL_NUM_BLOCKS_INDEX = 6;
        const uint32_t LOGICAL_NUM_BLOCKS_INDEX = 7;
        const uint32_t USERNAME_INDEX = 8;
        const uint32_t HOSTNAME_INDEX = 9;
        const uint32_t WIN_VERSION_INDEX = 10;
        const uint32_t SNAPSHOT_TIME_INDEX = 11;
        const uint32_t LOGICAL_SIZE_INDEX = 12;
    }

    namespace BLOCK_LINE
    {
        const uint32_t LOCAL_SN_INDEX = 1;
        const uint32_t FP_INDEX = 2;
        const uint32_t MIN_NUM_ELEMENT_IN_BLOCK_LINE = 4;
    }

    namespace FILE_LINE
    {
        const uint32_t NUM_BLOCKS_INDEX = 4;
    }
}
