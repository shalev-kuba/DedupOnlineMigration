#include "FSFile.hpp"
#include "../Common/FileLinesIterator.hpp"
#include "FSUnioner.hpp"
#include "../Common/Utility.hpp"

#include <fstream>
#include <iostream>

enum InputIndex: uint32_t{
    PROGRAM_NAME =0,
    INPUT_FILE,
    OUT_PATH,
    IS_FILTER,
    ARGS_COUNT
};


int main(int argc, char** argv) {
    if(argc != InputIndex::ARGS_COUNT)
    {
        std::cout << "Usage: ./union_fs_files <input_file> <out_path> [is_filter (1 or 0)]"<<std::endl;
        std::cout << "input_file: The input file is a line separated file where each line is a path "
                     "of a FSFile (an output of ./trace_parser)"<<std::endl;
        return EXIT_FAILURE;
    }

    const bool is_filter = argv[InputIndex::IS_FILTER][0] == '1';
    if(is_filter){
        std::cout << "Running in filter mode"<<std::endl;
    }
    FSUnioner unioner(argv[InputIndex::OUT_PATH], is_filter);
    for(FileLinesIterator it(argv[InputIndex::INPUT_FILE]); !it.is_done(); it.next())
    {
        unioner.add_fs(it.get_current_line());
    }

    unioner.print_to_file();

    return EXIT_SUCCESS;
}

