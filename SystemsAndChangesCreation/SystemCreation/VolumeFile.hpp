#pragma once
#include <string>
#include <unordered_map>
#include <fstream>

class VolumeFile final{
public:
    explicit VolumeFile(const std::string& in_vol_path,
                        const std::string& out_vol_path,
                        std::unordered_map<std::string, uint64_t>& fp_to_sn,
                        const uint64_t file_sn_start=0,
                        const uint32_t vol_id =0);

    ~VolumeFile();

public:
    uint64_t get_num_files() {return _num_files;};

private:
    void copy_header();
    void translate_blocks();
    void translate_files();

private:
    std::ifstream _input_stream;
    std::ofstream _output_stream;
    const uint64_t _sn_files_start;
    std::unordered_map<std::string, uint64_t>& _fp_to_sn;
    std::unordered_map<uint64_t, std::string> _local_sn_to_fp;
    const uint32_t _vol_id;
    uint64_t _num_files;

public:
    VolumeFile(VolumeFile&) = delete;
    VolumeFile(VolumeFile&&) = delete;
    VolumeFile& operator=(VolumeFile&) = delete;
    VolumeFile& operator=(VolumeFile&&) = delete;
};
