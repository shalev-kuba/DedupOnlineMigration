#include "TraceFile.hpp"

#include <iostream>
#include <cinttypes>

enum Arguments: uint32_t{
    PROGRAM_NAME =0,
    INPUT_FILE,
    OUT_DIR,
    ARGUMENTS_COUNT
};

int main(int argc, char** argv) {
    if(argc != static_cast<uint32_t>(Arguments::ARGUMENTS_COUNT))
    {
        std::cout<<"./trace_parser <input_file_path> <out_directory>"<<std::endl;
        return EXIT_FAILURE;
    }

    TraceFile trace_file(argv[Arguments::INPUT_FILE]);
    trace_file.print_to_file(argv[Arguments::OUT_DIR]);

    return EXIT_SUCCESS;
}

