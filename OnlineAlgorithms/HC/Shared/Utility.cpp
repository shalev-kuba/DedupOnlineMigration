#include "Utility.hpp"
#include "Calculator.hpp"

#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <limits.h>
#include <stdlib.h>
#include <exception>
#include <iomanip>
#include <unistd.h>
#include <iostream>
#include <fstream>

std::map<int,long long int> Utility::getMergedMap(const std::map<int,long long int>& map1, const std::map<int,long long int>& map2,
                                                  const std::map<int,long long int>& map_subtract){
    std::map<int,long long int> result(map1);
    for ( const auto &key_val : map2 ) {
        auto res_find = result.find(key_val.first);
        if(res_find == result.end()){
            result[key_val.first] = key_val.second;
        }
        else{
            result[key_val.first] += key_val.second;
        }
    }

    for ( const auto &key_val : map_subtract ) {
        auto res_find = result.find(key_val.first);
        if(res_find != result.end()){
            result[key_val.first] -= key_val.second;
        }
        else{
            throw std::runtime_error("should not arrive here");
        }
    }

    return std::move(result);
}

int Utility::binarySearch(const std::vector<int>& array, int left_index, int right_index, int value) {
    while (left_index <= right_index) {
        const int middle_index = left_index + (right_index - left_index) / 2;
        // check if x is here
        if (array[middle_index] == value)
            return middle_index;
        // if x is greater, ignore left
        if (array[middle_index] < value)
            left_index = middle_index + 1;
        // if x is smaller, ignore right
        else
            right_index = middle_index - 1;
    }
    // if we reach here, we did not find anything
    return -1;
}

std::vector<std::string> Utility::splitString(const std::string &line, const char delimiter) {
    std::stringstream ss(line);
    std::string item;
    std::vector<std::string> elements;
    while (getline(ss, item, delimiter)) {
        elements.push_back(std::move(item));
    }

    return std::move(elements);
}

std::vector<std::string> Utility::splitString(const std::string & str, const std::string &delimiter) {
    std::vector<std::string> result;
    size_t start = 0, end;

    while ((end = str.find(delimiter, start)) != std::string::npos) {
        result.push_back(str.substr(start, end - start));
        start = end + delimiter.length();
    }

    result.push_back(str.substr(start)); // Add the last segment
    return result;
}

bool Utility::isFileExists(const std::string& path) {
    struct stat buffer{};
    return (stat(path.c_str(), &buffer) == 0);
}

float Utility::getJaccardDistance(const std::vector<bool>& row1,const std::vector<bool>& row2) {
    int co_absence = 0;
    int xor_exists = 0;

    for (int k = 0; k < row1.size(); ++k) {
        co_absence += ((!row1[k]) && (!row2[k]))? 1 : 0;
        xor_exists += (((!row1[k]) && (row2[k])) || ((row1[k]) && (!row2[k]))) ? 1: 0;
    }

    const int diff = row1.size() - co_absence;
    // in case we divide by 0, set value to 0
    if(diff == 0)
        return 0;

    return static_cast<float>(xor_exists) / static_cast<float>(diff);
}

bool Utility::isWorkloadBlockLine(const std::vector<std::string>& splitted_line_content){
    static const std::string BLOCK_LINE_START_STRING = "B";
    return splitted_line_content.front() == BLOCK_LINE_START_STRING;
}

bool Utility::isWorkloadFileLine(const std::vector<std::string>& splitted_line_content){
    static const std::string FILE_LINE_START_STRING = "F";

    return splitted_line_content.front() == FILE_LINE_START_STRING;
}

int Utility::getNumOfBlockInFile(const std::vector<std::string>& splitted_line_content) {
    static constexpr int FILE_NUM_OF_BLOCKS_INDEX = 4;
    return stoi(splitted_line_content[FILE_NUM_OF_BLOCKS_INDEX]);
}

std::string Utility::getInputFileOfFile(const std::vector<std::string>& splitted_line_content) {
    static constexpr int INPUT_FILE_INDEX = 2;
    return std::move(splitString(splitted_line_content[INPUT_FILE_INDEX], '_')[1]); // assumes fileId is <vol>_<inputFile>
}

int Utility::getBlockSNInFileByIndex(const std::vector<std::string>& splitted_line_content, const int block_index) {
    static constexpr int FILE_RECIPE_BLOCK_SN_START_INDEX = 5;
    static constexpr int BLOCK_SN_JUMP = 2;
    return stoi(splitted_line_content[FILE_RECIPE_BLOCK_SN_START_INDEX + block_index * BLOCK_SN_JUMP]);
}

int Utility::getBlockSizeInFileByIndex(const std::vector<std::string>& splitted_line_content, const int block_index) {
    static constexpr int FILE_RECIPE_BLOCK_SIZE_START_INDEX = 6;
    static constexpr int BLOCK_SIZE_JUMP = 2;
    return stoi(splitted_line_content[FILE_RECIPE_BLOCK_SIZE_START_INDEX + block_index * BLOCK_SIZE_JUMP]);
}

int Utility::getWorkloadBlockSN(const std::vector<std::string>& splitted_line_content) {
    static constexpr int BLOCK_LINE_SN_INDEX = 1;
    return stoi(splitted_line_content[BLOCK_LINE_SN_INDEX]);
}

const std::string& Utility::getWorkloadBlockFingerprint(const std::vector<std::string>& splitted_line_content) {
    static constexpr int BLOCK_LINE_FP_INDEX = 2;
    return splitted_line_content[BLOCK_LINE_FP_INDEX];
}

int Utility::getWorkloadFileSN(const std::vector<std::string>& splitted_line_content) {
    static constexpr int FILE_LINE_SN_INDEX = 1;
    return stoi(splitted_line_content[FILE_LINE_SN_INDEX]);
}

std::string Utility::getBaseName(const std::string &path) {
    // Find the last slash in the path
    size_t pos = path.find_last_of("/");

    // If no slash is found, return the entire path as the file name
    if (pos == std::string::npos) {
        return path;
    }

    // Return the substring after the last slash
    return path.substr(pos + 1);
}

std::string Utility::getCommaToken(std::istream& input_stream, bool skip_space) {
    std::string res;
    if (!std::getline(input_stream, res, ',')) {
        return "";
    }

    if (skip_space && !res.empty() && res[0] == ' ') {
        return res.substr(1); // Skip space
    }

    return res;
}

std::string Utility::getLine(std::istream& input_stream) {
    std::string line = "";

    if(!std::getline(input_stream, line))
    {
        return line;
    }

    static constexpr char CR_CHAR = '\r';
    if(!line.empty() && line[line.size()-1] == CR_CHAR)
    {
        line.erase(line.size()-1); // need dos2unix. this is a fix for removing '\r'
    }

    return line;
}

std::string Utility::getFullPath(const std::string &path) {
    char resolved_path[PATH_MAX];
    if(!realpath(path.c_str(), resolved_path))
        throw std::runtime_error("getFullPath failed");
    return std::string(resolved_path);
}

std::string Utility::getString(const double val) {
    std::stringstream stream;
    stream << std::fixed << std::setprecision(2) << val;
    return stream.str();
}

std::string Utility::getHostName() {
    char hostname[HOST_NAME_MAX];
    const int result = gethostname(hostname, HOST_NAME_MAX);
    if (result)
        throw std::runtime_error("failed to get host name");

    return std::string(hostname);
}

void Utility::createDir(const std::string &path) {
    struct stat info{};

    if( stat( path.c_str(), &info ) == 0 && !S_ISDIR(info.st_mode))
        throw std::runtime_error("could not create dir " + path + ". a file is already exist there.");

    if(S_ISDIR(info.st_mode))
        return;

    if (mkdir(path.c_str(), 0777) != 0 && errno != EEXIST)
        throw std::runtime_error("could not create dir " + path);
}

void Utility::printMigrationStep(
        const std::string &server_name, const uint32_t num_total_iter,
        const uint32_t num_incremental_iter, const uint32_t num_change_iter,
        const uint64_t iter_max_traffic, double internal_margin,
        const std::map<std::string, std::set<int>> &result_mapping,
        const std::map<std::string, std::set<int>> &initial_mapping,
        const std::shared_ptr<Calculator::CostResult> &cost_result,
        const std::string &output_path, const bool append){

    ofstream result_csv_file;
    if(append)
        result_csv_file.open(output_path, std::ios_base::app);
    else
        result_csv_file.open(output_path);

    std::string iter_indicator = std::to_string(num_total_iter) + "_m" + std::to_string(num_incremental_iter)
                                 + "_c" + std::to_string(num_change_iter);
    try {
        result_csv_file
                << "Server name, Iteration num, Iteration Max Traffic Bytes, WT, Gap, Seed, Eps, "
                   "Initial Internal Margin, Ensure load balance, Num attempts"<< endl;
        result_csv_file << server_name << "," << iter_indicator << ","
                        << iter_max_traffic << ",-,-,-,-,"
                        << internal_margin << ",1,-"<< endl << endl;

        result_csv_file<<"Cluster path,Initial files,Final files,initial size B,initial size %,received B,deleted B" //UPDATED
                         ",traffic B, overlap traffic B, block reuse B, aborted traffic B" //NEW
                         ",final size B,final size %,total traffic B,total traffic%,total deletion B,total deletion%,"
                         "lb_score,is_valid_traffic, is_valid_lb,error_message" << endl;

        for (const auto &volume_info: cost_result->volumes_info) {
            int i = 1;
            const string &volume_name = volume_info->getVolumeName();
            result_csv_file << volume_name + ",";
            for (const auto &file_sn: initial_mapping.at(volume_name)) {
                result_csv_file << file_sn << (i++ == initial_mapping.at(volume_name).size() ? "" : "-");
            }

            result_csv_file << ",";
            i = 1;
            for (const auto &file_sn: result_mapping.at(volume_name)) {
                result_csv_file << file_sn << (i++ == result_mapping.at(volume_name).size() ? "" : "-");
            }
            result_csv_file << ",";
            result_csv_file << volume_info->getInitVolumeSize() << ","
                            << static_cast<double>(volume_info->getInitVolumeSize()) / cost_result->init_system_size *
                               100 << ","
                            << volume_info->getReceivedBytes() << ","
                            << volume_info->getNumBytesDeleted() << ","
                            <<volume_info->getTrafficBytes()<<"," //NEW
                            <<volume_info->getOverlapTrafficBytes()<<","//NEW
                            <<volume_info->getBlockReuseBytes()<<","//NEW
                            <<volume_info->getAbortedTrafficBytes()<<","//NEW
                            << volume_info->getFinalVolumeSize() << ","
                            << static_cast<double>(volume_info->getFinalVolumeSize()) / cost_result->final_system_size *
                               100 << endl;
        }
        result_csv_file << ",,,,,,,,,,,,,"
        << cost_result->traffic_bytes << "," << cost_result->traffic_percentage << ","
        << cost_result->deletion_bytes << "," << cost_result->deletion_percentage << ","
        << cost_result->lb_score << "," << cost_result->is_traffic_valid << "," << cost_result->is_lb_valid
        << "," << cost_result->error_message << endl << endl;
    }
    catch (...) {
        cerr << "error in outputConvertedResultsToCsv" << endl;
        result_csv_file.close();
    }
}

void Utility::printSummedResults(
        const string &file_path,
        const long long int init_system_size,
        const long long int summed_traffic,
        const long long int summed_deletion_bytes,
        const long long int elapsed_time_seconds,
        const long long int summed_wt_elapsed_time,
        const double lb_score,
        const bool is_valid_traffic,
        const bool is_valid_lb){

    ofstream result_csv_file(file_path, std::ios_base::app);
    result_csv_file<<endl <<"Summed results:" <<endl;
    result_csv_file<<"Summ traffic,Summ traffic%, Deletion B, Deletion %, Total Elapsed time seconds, Sum chosen WT elapsed time, lb_score, is_valid_traffic, is_valid_lb"<<endl;
    result_csv_file<<summed_traffic<<"," << static_cast<double>(summed_traffic)/ init_system_size * 100<<","
                   << summed_deletion_bytes<<","<< static_cast<double>(summed_deletion_bytes)/ init_system_size * 100 <<","
                   << elapsed_time_seconds <<","
                   << summed_wt_elapsed_time <<","
                   << lb_score<< ","<<is_valid_traffic<< ","<<is_valid_lb<<endl;

    result_csv_file.close();
}
