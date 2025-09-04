#include "VolumeFile.hpp"
#include "../Common/FileLinesIterator.hpp"
#include "../Common/Utility.hpp"

#include <unordered_map>
#include <fstream>
#include <iostream>

enum InputIndex: uint32_t{
    PROGRAM_NAME =0,
    INPUT_FILE,
    OUTPUT_FILE,
    ARGS_COUNT
};


int main(int argc, char** argv) {
    if(argc != InputIndex::ARGS_COUNT)
    {
        std::cout << "Usage: ./summary_system <input_file> <output_file>"<<std::endl;
        std::cout << "input_file: The input file is a line separated file where each line is a Volume of the system in "
                     "a FSFile format (an output of ./UnionFSFiles)"<<std::endl;
        std::cout <<"Output: system summary, written to output_file"<<std::endl;
        return EXIT_FAILURE;
    }

    uint64_t blocks_sn = 0;
    uint64_t files_sn = 0;
    uint32_t vol_counter = 0;

    std::unordered_map<std::string, uint64_t> fp_so_sn = {};

    for(FileLinesIterator it(argv[InputIndex::INPUT_FILE]); !it.is_done(); it.next())
    {
        VolumeFile current_vol(it.get_current_line(), "/dev/null", fp_so_sn, files_sn, vol_counter);

        files_sn += current_vol.get_num_files();
        ++vol_counter;
    }

    std::ofstream out = Utility::get_output_file_stream(argv[InputIndex::OUTPUT_FILE]);
    out<<"Num unique blocks: " << fp_so_sn.size()<<std::endl;
    out<<"Num unique files: " << files_sn<<std::endl;
    out<<"Num volumes: " << vol_counter<<std::endl;
    unsigned long long int physical_size = 0;
    for(auto& fp_block: fp_so_sn){
        physical_size+=fp_block.second;
    }
    out<<"Ideal Physical size: " << physical_size<<std::endl;

    return EXIT_SUCCESS;
}

