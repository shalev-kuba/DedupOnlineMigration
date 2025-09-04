#include "Config.hpp"

namespace Config
{
    uint64_t BLOCK_RESERVE=3000000;
    const char* const TRACE_EOF = "LOGCOMPLETE";
    namespace LOCAL_FILE_HEADER
    {
        uint32_t NUM_OF_CONST_FILES_HEADERS = 12;
        uint32_t FILE_LOGICAL_SIZE_INDEX = 4;
    }

    namespace TRACE_HEADER
    {
        uint64_t LINES_IN_TRACE_HEADER = 33;
        uint32_t USERNAME_INDEX = 1;
        uint32_t HOSTNAME_INDEX = 2;
        uint32_t TIMESTAMP_INDEX = 4;
        uint32_t WINDOWS_VERSION_INDEX = 5;
    }
}