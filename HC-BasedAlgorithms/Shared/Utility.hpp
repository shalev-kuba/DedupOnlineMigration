#pragma once

#include "Calculator.hpp"

#include <string>
#include <map>
#include <vector>
#include <unordered_set>
#include <set>
#include <memory>

namespace Utility {
    /**
     * simple binary search for a sorted array
     * @param array - array (as vector) to search
     * @param left_index - current left bound
     * @param right_index - current right bound
     * @param value - value to find
     * @return - index of value or -1 if does not exist
     */
    int binarySearch(const std::vector<int> &array, const int left_index, const int right_index, const int value);

    /**
     * splits a string to tokens
     * @param line- string to split
     * @param delimiter - char to tokenize by
     * @return a vector with the tokens
     */
    std::vector<std::string> splitString(const std::string &line, const char delimiter);

    std::vector<std::string> splitString(const std::string &str, const std::string &delimiter);

    /**
     * @param path- absolute or relative path
     * @return the base name of a given path
     */
    std::string getBaseName(const std::string &path);

    /**
     * @param path- absolute or relative path
     * @return the full path of the given path
     */
    std::string getFullPath(const std::string &path);

    std::string getCommaToken(std::istream &input_stream, bool skip_space);

    std::string getLine(std::istream &input_stream);

    /**
     *
     * @param path - path to be checked
     * @return true if file path exists, false otherwise
     */
    bool isFileExists(const std::string &path);

    /**
     *
     * @param path - path to create dir in
     * creates the directory in the given *relative* path
     */
    void createDir(const std::string &path);

    /**
     *
     * @param val - value
     * @return string of the given double val only with precision of 2 after dot
     */
    std::string getString(const double val);

    /**
     *
     * @param map1 - a map of int to int
     * @param map2 - a map of int to int
     * @param map_subtract - a map of int to int to subtract (duplicated)
     * @return the merged map of map 1 and map 2 after subtract `map_subtract`
     */
    std::map<int, long long int>
    getMergedMap(const std::map<int, long long int> &map1, const std::map<int, long long int> &map2,
                 const std::map<int, long long int> &map_subtract);

    /**
     * simple function to calculate the dissimilarity between two rows (with same lengths)
     * @param row1 - the first row to compare
     * @param row2 - the second row to compare
     * @return the Jaccard distance
     */
    float getJaccardDistance(const std::vector<bool> &row1, const std::vector<bool> &row2);

    /**
     * @param splitted_line_content - line from a workload csv file splitted by ','
     * @return whether the line describes a block
     */
    bool isWorkloadBlockLine(const std::vector<std::string> &splitted_line_content);

    /**
     * @param splitted_line_content - line from a workload csv file splitted by ','
     * @return whether the line describes a file
     */
    bool isWorkloadFileLine(const std::vector<std::string> &splitted_line_content);

    /**
     * @param splitted_line_content - block line from a workload csv file splitted by ','
     * @return return the workload's block sn as described in workload_line
     */
    int getWorkloadBlockSN(const std::vector<std::string> &splitted_line_content);

    /**
     * @param splitted_line_content - file line from a workload csv file splitted by ','
     * @return return the workload's file sn as described in workload_line
     */
    int getWorkloadFileSN(const std::vector<std::string> &splitted_line_content);

    /**
     * @param splitted_line_content - a line from a workload csv file describing a file splitted by ','
     * @return return the num of blocks in file as described in workload_line
     */
    int getNumOfBlockInFile(const std::vector<std::string> &splitted_line_content);

    /**
     * @param splitted_line_content - a line from a workload csv file describing a file splitted by ','
     * @return return the input file of the file (removing the volume prefix - assumes fileId is <vol>_<inputFile>)
     */
    std::string getInputFileOfFile(const std::vector<std::string> &splitted_line_content);

    /**
     * @param splitted_line_content - a line from a workload csv file describing a file splitted by ','
     * @param block_index - index of the block in the file recipe
     * @return return the block's SN
     */
    int getBlockSNInFileByIndex(const std::vector<std::string> &splitted_line_content, const int block_index);

    /**
    * @param splitted_line_content - a line from a workload csv file describing a file splitted by ','
    * @param block_index - index of the block in the file recipe
    * @return return the block's size
    */
    int getBlockSizeInFileByIndex(const std::vector<std::string> &splitted_line_content, const int block_index);

    /**
     * @param splitted_line_content - a line from a workload csv file splitted by ','
     * @return return the workload's block fingerprint as described in workload_line
     */
    const std::string &getWorkloadBlockFingerprint(const std::vector<std::string> &splitted_line_content);

    /**
     * @return return current_host_name
     */
    std::string getHostName();

    void printMigrationStep(
            const std::string &server_name, uint32_t num_total_iter,
            uint32_t num_incremental_iter, uint32_t num_change_iter,
            uint64_t iter_max_traffic, double internal_margin,
            const std::map<std::string, std::set<int>> &result_mapping,
            const std::map<std::string, std::set<int>> &initial_mapping,
            const std::shared_ptr<Calculator::CostResult> &cost_result,
            const std::string &output_path, bool append);

    void printSummedResults(const string &file_path,
            long long int init_system_size,
            long long int summed_traffic,
            long long int summed_deletion_bytes,
            long long int elapsed_time_seconds,
            long long int summed_wt_elapsed_time,
            double lb_score,
            bool is_valid_traffic,
            bool is_valid_lb);
}
