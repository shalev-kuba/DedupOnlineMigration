/**
Created by Ariel Kolikant
Modified by Shalev Kuba
*/
#include <algorithm>
#include <numeric>
#include <cfloat>
#include <vector>
#include <iostream>
#include <sstream>
#include <fstream>
#include <chrono>
#include <set>
#include <boost/algorithm/string.hpp>
#include <unordered_map>
#include <cmath>
#include <string>
#include <iostream>
#include<cstdlib>
#include <cstdint>
#include <cmath>
#include "json.hpp"

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*
    Data structure explanation:
    _____________________________________
    std::vector<std::string> source_volume_list:
        A vector containing the paths of all source volumes.

    std::vector<std::string> target_volume_list:
        A vector containing the paths of all target volumes.

    std::vector<double> desired_balance:
        A vector containing the desired_balance of all volumes. given fractions can be between 0 to 1 and must sum up to a total of 1.

    std::vector<std::vector<short>> block_volume_sourceRefCount:
        The data structure saving for each source volume how many references it has for a block. IE how many files are using it.
        If there are 4 files using block 10 in volume 3. then block_volume_sourceRefCount[10][3] == 4

    std::vector<std::vector<short>> block_volume_targetRefCount:
        The data structure saving for each target volume how many references it has for a block. IE how many files are using it.
        If there are 4 files using block 10 in volume 3. then block_volume_targetRefCount[10][3] == 4

     std::vector<std::vector<uint64_t>> g__volume_file_fileList:
        The data structure saving for each volume list of files sn it contains

    std::pair<int, int> lastSourceSn_block_file:
        A pair of the last SN in the source volume of blocks and of files

    std::pair<int, int> lastTargetSn_block_file:
        A pair of the last SN in the target volume of blocks and of files

    std::vector<double> blockSizes:
        The data structure saving every size of the blocks.
        blockSizes[10] is the size in KB of the block with SN 10.

    std::vector<double> volumeSizes:
        The data structure saving the current size of all of the volumes.

    std::unordered_map<std::string, uint64_t> g__block_fp_to_sn
        The data structure saving the mapping from block fp to block sn

    std::unordered_map<std::string, uint64_t> g__input_file_to_sn
        The data structure saving the mapping from input file to file sn

    std::unordered_map<uint64_t, std::string> g__file_sn_to_line
        The data structure saving the mapping from file sn to line content

    std::unordered_map<uint64_t, int> g__file_sn_to_vol_num
        The data structure saving the mapping from file sn its current vol num
*/

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*
    Global variables and their default values
*/

bool fullMigration = true;
std::vector<int> loopEvade(2, -2);
double initialTotalSourceSize = 0;
double g__lb_score = 0;
std::vector<double> g__desired_balance;
std::unordered_map<std::string, uint64_t> g__block_fp_to_sn = {};
auto g__begin = std::chrono::high_resolution_clock::now(); //Start the stopwatch for the total time.
double g__changeElapsedTimeSec = 0;
double g__currentTraffic = 0;
double g__calcDeletion = 0;
double g__initialTotalSourceSize = 0;
double g__totalTraffic = 0;
std::unordered_map<uint64_t, uint64_t> g__transfers_sn_to_volume= {};
std::unordered_map<uint64_t, uint64_t> g__transfers_sn_to_source_volume= {};
std::map<int, std::vector<std::string>> g__host_to_input_file_ordered = {};
std::string g__change_insert_type = "";
std::vector<std::vector<uint64_t>> g__volume_file_fileList = {};
std::unordered_map<std::string, uint64_t> g__input_file_to_sn ={};
std::unordered_map<uint64_t, std::string> g__file_sn_to_line = {};
std::unordered_map<uint64_t, int> g__file_sn_to_vol_num = {};
static bool g__is_lb_valid=true;
static bool g__filter_blocks = true;

int g__num_files=0;
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*
    Utility general cpp functions
*/

void updateLoopEvade(int fileSn) {
    loopEvade.insert(loopEvade.begin(), fileSn);
    loopEvade.pop_back();
}

void resetLoopEvade() {
    loopEvade = std::vector<int>{-2, -2};
}

bool isLoop(int fileSn) {
    return std::find(loopEvade.begin(), loopEvade.end(), fileSn) != loopEvade.end();
}

bool isLastMove(int fileSn) {
    return std::find(loopEvade.begin(), loopEvade.end(), fileSn) == loopEvade.begin();
}

/**
 * @brief Returns the indexes of the sorted array from the given array
 *        [1,2,3,4] -> [0,1,2,3]
 *        [4,2,3,1] -> [3,1,2,0]
 * @param v Is the array from which we want the sorted indexes
 */
template<typename T>
std::vector<std::size_t> sort_indexes(const std::vector<T> &v) {

    // Initialize original index locations
    std::vector<std::size_t> idx(v.size());
    iota(idx.begin(), idx.end(), 0);

    // Sort indexes based on comparing values in v
    // using std::stable_sort instead of std::sort
    // to avoid unnecessary index re-orderings
    // when v contains elements of equal values
    stable_sort(idx.begin(), idx.end(),
                [&v](std::size_t i1, std::size_t i2) { return v[i1] < v[i2]; });

    return idx;
}

/**
 * @brief Converts a string to a bool value
 * @param v The string to convert
 */
bool string2bool(const std::string &v) {
    return !v.empty() &&
           (strcasecmp(v.c_str(), "true") == 0 ||
            atoi(v.c_str()) != 0);
}

/**
 * @brief Splits a string to an array by a delimiter
 * @param str The string
 * @param delimiter The delimiter
 */
std::vector<std::string> split_string(std::string str, const std::string &delimiter) {
    std::vector<std::string> result;
    boost::split(result, str, boost::is_any_of(delimiter));
    return result;
}

std::vector<std::string> split_string_by_string(std::string str, const std::string &delimiter) {
    std::vector<std::string> result;
    size_t start = 0, end;

    while ((end = str.find(delimiter, start)) != std::string::npos) {
        result.push_back(str.substr(start, end - start));
        start = end + delimiter.length();
    }

    result.push_back(str.substr(start)); // Add the last segment
    return result;
}


/**
 * @brief Replaces all occurrences of 'from' to 'to' in the string
 * @param str The string
 * @param from The string to be replaced
 * @param to The string to be replaced with
 */
std::string replaceAll(std::string &str, const std::string &from, const std::string &to) {
    std::string returnString = str;
    if (from.empty())
        return returnString;
    size_t start_pos = 0;
    while ((start_pos = returnString.find(from, start_pos)) != std::string::npos) {
        returnString.replace(start_pos, from.length(), to);
        start_pos += to.length(); // In case 'to' contains 'from', like replacing 'x' with 'yx'
    }
    return returnString;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*
    Utility problem specific functions
*/

/**
 * @brief Calculates the volume size
 * @param volumeSizes Reference where the result would be put
 * @param block_volume_targetRefCount The block reference count object
 * @param blockSizes An array with the sizes of all the blocks
 */
void getVolumeSizes(std::vector<double> &volumeSizes, std::vector<std::vector<short>> &block_volume_targetRefCount,
                    std::vector<double> &blockSizes) {
    for (int volume = 0; volume < volumeSizes.size(); volume++) {
        volumeSizes[volume] = 0.0;
        for (int block = 0; block < blockSizes.size(); block++) {
            if (block < block_volume_targetRefCount.size() && block_volume_targetRefCount[block][volume] > 0) {
                volumeSizes[volume] += blockSizes[block];
            }
        }
    }
}

/**
 * @brief Calculate each volume's percentage of the total size
 * @param volumeSizes Reference where the result would be put
 * @param block_volume_targetRefCount The block reference count object
 * @param blockSizes An array with the sizes of all the blocks
 */
void
getVolumePercentages(std::vector<double> &volumeSizes, std::vector<std::vector<short>> &block_volume_targetRefCount,
                     std::vector<double> &blockSizes) {
    double totalSize = 0;
    for (int volume = 0; volume < volumeSizes.size(); volume++) {
        volumeSizes[volume] = 0.0;
        for (int block = 0; block < blockSizes.size(); block++) {
            if (block < block_volume_targetRefCount.size() && block_volume_targetRefCount[block][volume] > 0) {
                volumeSizes[volume] += blockSizes[block];
            }
        }
        totalSize += volumeSizes[volume];
    }

    for (int volume = 0; volume < volumeSizes.size(); volume++) {
        volumeSizes[volume] = volumeSizes[volume] / totalSize;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*
    Utility ingestion phase functions
*/

/**
 * @brief Calculate each volume's percentage of the total size
 * @param blockSizes An array where the block sizes would be put
 * @param block_volume_targetRefCount The block reference count object
 */
void calcBlockSizes(std::vector<double> &blockSizes, std::vector<std::string> &volumeList) {
    std::string content;
    std::vector<std::string> splitted_content;
    for (auto &volume : volumeList) {
        std::ifstream volume_stream(volume.c_str(), std::ifstream::in);
        if (!volume_stream.is_open()) {
            std::cout << "error opening volume file - calcBlockSizes" << volume << std::endl;
            exit(1);
        }

        while (std::getline(volume_stream, content)) {
            splitted_content = split_string(content, ",");
            if (splitted_content[0] == "F") {
                int number_of_blocks_in_file_line = std::stoi(splitted_content[4]);
                for (register int i = 0; i < 2 *
                                             number_of_blocks_in_file_line; i += 2) //Read block_sn and block_size simultaneously and add constrains to the model.
                {
                    int blockSn = std::stoi(splitted_content[5 + i]);
                    int size_read = std::stoi(splitted_content[6 + i]);
                    if (blockSizes[blockSn] == 0.0) {
                        double size = ((double) size_read) / 1024.0;
                        blockSizes[blockSn] = size;
                    }
                }
            }
        }
    }
}

/**
 * @brief Find the last block index and last file index and return a pair of <lastBlock, lastFile>
 * @param volumeList A list of all the volume paths
 */
std::pair<int, int> getLastBlockAndFileSn(std::vector<std::string> &volumeList) {
    int lastBlockSn = 0;
    int lastFileSn = 0;
    for (auto &source_volume : volumeList) {
        std::ifstream source_volume_stream(source_volume.c_str(), std::ifstream::in);
        if (!source_volume_stream.is_open()) {
            std::cout << "error opening volume file - getLastBlockSn" << source_volume << std::endl;
            exit(1);
        }

        std::string content;
        std::vector<std::string> splitted_content;

        while (std::getline(source_volume_stream, content)) {
            splitted_content = split_string(content, ",");
            if (splitted_content[0] == "F") {
                lastFileSn = std::max(lastFileSn, std::stoi(splitted_content[1]));
            }
            if (splitted_content[0] == "B") {
                lastBlockSn = std::max(lastBlockSn, std::stoi(splitted_content[1]));
            }
        }
        source_volume_stream.close();
    }
    return std::make_pair(lastBlockSn, lastFileSn);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*
    Utility optimization phase functions
*/

/**
 * @brief Check if the provided solution object is an empty solution object
 * solution is either marked by an illegal sourceVolume of -1 or is de facto empty by moving nothing
 * @param solution A tuple representing a transaction: (sourceVolume, targetVolume, fileSn, best replicate, best delete, best move, csvLineOfMovedFile)
 */

bool isEmptySolution(std::tuple<int, int, int, double, double, double, std::string, double> &solution) {

    if (std::get<0>(solution) == -1) {
        return true;
    }
    // return ((std::get<3>(solution) == 0) && (std::get<4>(solution) == 0) && (std::get<5>(solution) == 0));
}

/**
 * @brief Check if the given state is a legal state
 * a legal state is when all the volumes' relative size to the total size is within the given margin
 * @param volumeSizes The current size of the volumes
 * @param desired_balance The desired balance we want in the system
 * @param margin The error margin we allow ourselves from the desired balance for each volume
 */
bool isLegalState(std::vector<double> &volumeSizes, std::vector<double> &desired_balance, double margin) {
    double totalSize = 0.0;
    for (int volume = 0; volume < volumeSizes.size(); volume++) {
        totalSize += volumeSizes[volume];
    }
    for (int volume = 0; volume < volumeSizes.size(); volume++) {
        double load = volumeSizes[volume] / totalSize;
        if ((load > desired_balance[volume] + margin) || (load < desired_balance[volume] - margin)) {
            return false;
        }
    }
    return true;
}

/*
    bestTransfer: (fileSn, replicated, deleted, movement, csv line of the best file)
*/
/**
 * @brief Check if a given transfer would result in a legal state
 * a legal state is when all the volumes' relative size to the total size is within the given margin
 * a given bestTransfer isBalanced if simulating the transfer would result in a legal state
 *
 * @param volumeSizes The current size of the volumes
 * @param bestTransfer: the transfer given to ve checked (fileSn, replicated, deleted, movement, csv line of the best file)
 * @param source: Index of source volume
 * @param target: Index of target volume
 * @param desired_balance The desired balance we want in the system
 * @param margin The error margin we allow ourselves from the desired balance for each volume
 */
bool
isBalanced(std::vector<double> &volumeSizes, std::tuple<int, double, double, double, std::string, double> bestTransfer,
           int source, int target, std::vector<double> &desired_balance, double margin) {
    std::vector<double> copy(volumeSizes);
    double replicated = std::get<1>(bestTransfer);
    double deleted = std::get<2>(bestTransfer);
    double movement = std::get<3>(bestTransfer);
    copy[source] -= deleted - movement;
    copy[target] += movement + replicated;
    return isLegalState(copy, desired_balance, margin);
}

double
getLbScore(std::vector<double> &volumeSizes, std::tuple<int, double, double, double, std::string, double> bestTransfer,
           int source, int target, std::vector<double> &desired_balance, double margin) {
    std::vector<double> copy(volumeSizes);
    double replicated = std::get<1>(bestTransfer);
    double deleted = std::get<2>(bestTransfer);
    double movement = std::get<3>(bestTransfer);
    copy[source] -= deleted - movement;
    copy[target] += movement + replicated;

    return *min_element(copy.cbegin(),copy.cend())/
           *max_element(copy.cbegin(),copy.cend());
}


bool is_block_filtered(const std::string& fp) {
    if(fp[0] == '0' && fp[1] == '0'&& fp[2] == '0'){
        unsigned int x;
        std::stringstream ss;
        ss << std::hex << fp[3];
        ss >> x;

        return x>7;
    }

    return true;
}

std::string get_comma_token(std::istream& input_stream, bool skip_space) {
    std::string res;
    if (!std::getline(input_stream, res, ',')) {
        return "";
    }

    if (skip_space && !res.empty() && res[0] == ' ') {
        return res.substr(1); // Skip space
    }

    return res;
}

std::string get_line(std::istream& input_stream) {
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

std::string getFileName(const std::string& fullPath) {
    // Find the last slash in the path
    size_t pos = fullPath.find_last_of("/");

    // If no slash is found, return the entire path as the file name
    if (pos == std::string::npos) {
        return fullPath;
    }

    // Return the substring after the last slash
    return fullPath.substr(pos + 1);
}

int getHostByPath(const std::string& filePath){
    std::string fileName = getFileName(filePath);
    // assumes file name is in the following format: U241_H247_IF5484_G1_TS31_10_2009_10_16
    return std::stoi(split_string_by_string(split_string_by_string(fileName, "_H")[1], "_IF")[0]);
}

std::string getInputFileByPath(const std::string& filePath){
    std::string fileName = getFileName(filePath);
    // assumes file name is in the following format: U241_H247_IF5484_G1_TS31_10_2009_10_16
    return split_string_by_string(split_string_by_string(fileName, "_IF")[1], "_TS")[0];
};
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/*
    Core logic function
*/

/**
 * @brief Parse the input and initialize all the needed databases with input
 * a seemingly superfluous distinction between source and target is made, this is due to the need to allow the code to regress back to previous models
 * in which not all volumes were both source and target volumes.
 * @param source_volume_list A list of all source volumes
 * @param target_volume_list A list of all target volumes
 * @param block_volume_sourceRefCount Reference to the block reference object for source volume
 * @param block_volume_targetRefCount Reference to the block reference object for target volume
 * @param lastSourceSn_block_file A tuple of the last index of the blocks and files in the source volumes
 * @param lastTargetSn_block_file A tuple of the last index of the blocks and files in the target volumes
 */
void ingest(std::vector<std::string> &source_volume_list, std::vector<std::string> &target_volume_list,
            std::vector<std::vector<short>> &block_volume_sourceRefCount,
            std::vector<std::vector<short>> &block_volume_targetRefCount, std::pair<int, int> &lastSourceSn_block_file,
            std::pair<int, int> &lastTargetSn_block_file) {

    int lastBlock = std::max(lastSourceSn_block_file.first, lastTargetSn_block_file.first);

    for (int i = 0; i < lastBlock + 1; i++) {
        std::vector<short> volumeList = std::vector<short>(source_volume_list.size(), 0);
        block_volume_sourceRefCount.push_back(volumeList);
    }
    for (int i = 0; i < lastBlock + 1; i++) {
        std::vector<short> volumeList = std::vector<short>(target_volume_list.size(), 0);
        block_volume_targetRefCount.push_back(volumeList);
    }

    for (int source = 0; source < source_volume_list.size(); source++) {
        std::ifstream source_volume_stream(source_volume_list[source].c_str(), std::ifstream::in);
        if (!source_volume_stream.is_open()) {
            std::cout << "error opening volume file - addConstraint_RemmapedFileHasAllItsBlocks"
                      << source_volume_list[source] << std::endl;
            exit(1);
        }

        std::string content;
        std::vector<std::string> splitted_content;
        //File line only relevant for source volumes since target volumes cant move files
        std::vector<uint64_t> fileSns;
        while (std::getline(source_volume_stream, content)) {
            splitted_content = split_string(content, ",");
            if (splitted_content[0] == "F") {
                uint64_t fileSn = std::stoull(splitted_content[1]);
                g__file_sn_to_line[fileSn] = content;
                g__num_files++;

                std::string& fileId = splitted_content[2];
                std::string inputFile = (split_string(fileId, "_")[1]); // assumes fileId is <vol>_<inputFile>
                g__input_file_to_sn[inputFile]=fileSn;
                fileSns.emplace_back(fileSn);
                g__file_sn_to_vol_num[fileSn] = source;
            }
            if (splitted_content[0] == "B") {
                int blockSn = std::stoi(splitted_content[1]);
                const std::string& blockFp = splitted_content[2];
                int refCount = std::stoi(splitted_content[3]);
                block_volume_sourceRefCount[blockSn][source] = refCount;
                // assuming the blocks sn are already in system mode (global across volumes), so no need to search fp
                // and create new sn and filtered if needed
                g__block_fp_to_sn[blockFp] = blockSn;
            }
        }
        source_volume_stream.close();
        g__volume_file_fileList.emplace_back(fileSns);
    }

    for (int target = 0; target < target_volume_list.size(); target++) {
        std::ifstream target_volume_stream(target_volume_list[target].c_str(), std::ifstream::in);
        if (!target_volume_stream.is_open()) {
            std::cout << "error opening volume file - addConstraint_RemmapedFileHasAllItsBlocks"
                      << target_volume_list[target] << std::endl;
            exit(1);
        }

        std::string content;
        std::vector<std::string> splitted_content;
        while (std::getline(target_volume_stream, content)) {
            splitted_content = split_string(content, ",");
            if (splitted_content[0] == "B") {
                int blockSn = std::stoi(splitted_content[1]);
                int refCount = std::stoi(splitted_content[3]);
                block_volume_targetRefCount[blockSn][target] = refCount;
            }
        }
        target_volume_stream.close();
    }
}

/**
 * @brief Find the best file to transfer between a source and target volume
 * retVal: (fileSn, replicated, deleted, movement, csv line of the best file)
 * @param source Source volume index
 * @param target Target volume index
 * @param block_volume_sourceRefCount Reference to the block reference object for source volume
 * @param block_volume_targetRefCount Reference to the block reference object for target volume
 * @param currentTraffic How much traffic had allready been used
 * @param trafficSize How much traffic in total can be used
 * @param file_fileList All files
 * @param checkLegalState A configuration, if true we make sure the transfer is legal within margin, else we allow illegal transfers. Illegal transfers are necessary
 * for the balancing phase since the system needs several transfers before ending in a legal state
 * @param volumeSizes A list of the current volumeSizes
 * @param desired_balancethe Desired relative sizes of the volumes
 * @param margin Error margin from the desired relative size
 */
std::tuple<int, double, double, double, std::string, double> getBestTransfer(int source, int target,
                                                                             std::vector<std::vector<short>> &block_volume_sourceRefCount,
                                                                             std::vector<std::vector<short>> &block_volume_sourceRefCount_iter,
                                                                             std::vector<std::vector<short>> &block_volume_targetRefCount,
                                                                             std::vector<std::vector<short>> &block_volume_targetRefCount_iter,
                                                                             double currentTraffic, double trafficSize,
                                                                             bool checkLegalState,
                                                                             std::vector<double> &volumeSizes,
                                                                             std::vector<double> &desired_balance,
                                                                             double margin) {

    //Create empty transfer
    std::tuple<int, double, double, double, std::string, double> retVal;
    std::get<0>(retVal) = -1;       //Best fileSn
    std::get<1>(retVal) = DBL_MAX;  //Best totalReplication
    std::get<2>(retVal) = -DBL_MAX;  //Best totalDeletion
    std::get<3>(retVal) = DBL_MAX;  //Best movement
    std::get<4>(retVal) = "";       //Csv line of the best file
    std::get<5>(retVal) = DBL_MAX;  //Best traffic

    if (fullMigration && source == target) {
        return retVal;
    }

    std::vector<std::string> splitted_content;

    for (auto &fileSn : g__volume_file_fileList[source]) {
        if (isLastMove(fileSn)) {
            continue;
        }
        std::string& fileLine = g__file_sn_to_line[fileSn];
        splitted_content = split_string(fileLine, ",");
        if (splitted_content[0] == "F") {
            int number_of_blocks_in_file_line = std::stoi(splitted_content[4]);
            double replication = 0;
            double deletion = 0;
            double movement = 0;
            double traffic = 0;
            for (register int i = 0; i < 2 *
                                         number_of_blocks_in_file_line; i += 2) //Read block_sn and block_size simultaneously and add constrains to the model.
            {
                int block_sn = std::stoi(splitted_content[5 + i]);
                int size_read = std::stoi(splitted_content[6 + i]);
                double blockSize = ((double) size_read) / 1024.0;
                int refInSource = block_volume_sourceRefCount[block_sn][source];
//                int refInSource_iter = block_volume_sourceRefCount_iter[block_sn][source];
                int refInTarget = block_volume_targetRefCount[block_sn][target];
//                int refInTarget_iter = block_volume_targetRefCount_iter[block_sn][target];
                if (refInSource == 1) {
                    if (refInTarget > 0) {
                        deletion += blockSize; // Can delete
                    } else {
                        movement += blockSize; // Needs to move
                        //if (refInTarget_iter == 0) {
                        traffic += blockSize;
                        //}
                    }
                } else { //refInSource > 1
                    if (refInTarget == 0) {
                        replication += blockSize; //Must replicate
                        movement += blockSize; // Needs to move

                        //if (refInTarget_iter == 0) {
                        traffic += blockSize;
                        //}
                    }
                }
            }

            if(isLoop(fileSn) && traffic == 0 && deletion<=replication) {
                continue;
            }

            // if((deletion == 0 && movement == 0) || (deletion == 0 && replication == 0)) {
            if ((checkLegalState && deletion == 0) ||
                (!checkLegalState && (deletion == 0 && movement == 0 && replication ==
                                                                        0))) { //first condition is for deletion step, the second is for lb step
                continue;
            }
            double bestReclaim = std::get<1>(retVal) / std::max(1.0, std::get<2>(retVal));
            double currentReclaim = replication / std::max(1.0, deletion);
            if ((currentTraffic + traffic <= trafficSize) && (currentReclaim < bestReclaim)) {
                std::tuple<int, double, double, double, std::string, double> currentTransfer;
                std::get<0>(currentTransfer) = fileSn;
                std::get<1>(currentTransfer) = replication;
                std::get<2>(currentTransfer) = deletion;
                std::get<3>(currentTransfer) = movement;
                std::get<4>(currentTransfer) = fileLine;
                std::get<5>(currentTransfer) = traffic;
                if (!checkLegalState ||
                    isBalanced(volumeSizes, currentTransfer, source, target, desired_balance, margin)) {
                    std::get<0>(retVal) = fileSn;
                    std::get<1>(retVal) = replication;
                    std::get<2>(retVal) = deletion;
                    std::get<3>(retVal) = movement;
                    std::get<4>(retVal) = fileLine;
                    std::get<5>(retVal) = traffic;
                }
            }
        }
    }
    return retVal;
}

/**
 * @brief Perform a single greedy iteration and return the greediest iteration
 * retVal: (sourceVolume, targetVolume, fileSn, replicate, delete, move, csv line of the best file to move)
 * @param source_volume_list List of source volumes
 * @param target_volume_list List of target volumes
 * @param block_volume_sourceRefCount Reference to the block reference object for source volume
 * @param block_volume_targetRefCount Reference to the block reference object for target volume
 * @param currentTraffic How much traffic had allready been used
 * @param trafficSize How much traffic in total can be used
 * @param volume_file_fileList List saving the files of each volume as string lines
 * @param makeLegal A configuration, if true then the iteration would attempt to move the system to a legal state, if false then the iteration
 * would assume it's in a legal state and try to optimize the system without leaving the legal state
 * @param volumeSizes A list of the current volumeSizes
 * @param desired_balancethe Desired relative sizes of the volumes
 * @param margin Error margin from the desired relative size
 */
std::tuple<int, int, int, double, double, double, std::string, double>
greedyIterate(std::vector<std::string> &source_volume_list,
              std::vector<std::string> &target_volume_list,
              std::vector<std::vector<short>> &block_volume_sourceRefCount,
              std::vector<std::vector<short>> &block_volume_sourceRefCount_iter,
              std::vector<std::vector<short>> &block_volume_targetRefCount,
              std::vector<std::vector<short>> &block_volume_targetRefCount_iter,
              double currentTraffic, double trafficSize,
              bool makeLegal,
              std::vector<double> &volumeSizes,
              std::vector<double> &desired_balance,
              double margin
) {
    // Create an empty solution
    std::tuple<int, int, int, double, double, double, std::string, double> retVal;
    std::get<0>(retVal) = -1;       //Best sourceVolume
    std::get<1>(retVal) = -1;       //Best targetVolume
    std::get<2>(retVal) = -1;       //Best fileSn
    std::get<3>(retVal) = DBL_MAX;  //Best replicate
    std::get<4>(retVal) = -DBL_MAX; //Best delete
    std::get<5>(retVal) = DBL_MAX;  //Best move
    std::get<6>(retVal) = "";       //Csv line of the best file to move
    std::get<7>(retVal) = DBL_MAX;       //Best traffic

    //Capacity-reduction step
    if (!makeLegal) {
        for (int source = 0; source < source_volume_list.size(); source++) {
            for (int target = 0; target < target_volume_list.size(); target++) {
                std::tuple<int, double, double, double, std::string, double> bestTransfer;
                bestTransfer = getBestTransfer(source, target, block_volume_sourceRefCount,
                                               block_volume_sourceRefCount_iter,
                                               block_volume_targetRefCount, block_volume_targetRefCount_iter,
                                               currentTraffic, trafficSize,
                                               true, volumeSizes, desired_balance, margin);
                double bestReclaim = std::get<3>(retVal) / std::max(1.0, std::get<4>(retVal));
                double currentReclaim = std::get<1>(bestTransfer) / std::max(1.0, std::get<2>(bestTransfer));
                if (currentTraffic + std::get<5>(bestTransfer) <= trafficSize && currentReclaim < bestReclaim) {
                    if (isBalanced(volumeSizes, bestTransfer, source, target, desired_balance, margin)) {
                        std::get<0>(retVal) = source;
                        std::get<1>(retVal) = target;
                        std::get<2>(retVal) = std::get<0>(bestTransfer);
                        std::get<3>(retVal) = std::get<1>(bestTransfer);
                        std::get<4>(retVal) = std::get<2>(bestTransfer);
                        std::get<5>(retVal) = std::get<3>(bestTransfer);
                        std::get<6>(retVal) = std::get<4>(bestTransfer);
                        std::get<7>(retVal) = std::get<5>(bestTransfer);
                    }
                }
            }
        }
    }

    //Load-balancing step
    if (makeLegal) {
        // Heuristically to achieve balance we try to move from the biggest volume to the smallest volume
        // this heuristic would need to be changed should the assumption of a desired equal balance be broken
        // it would still work, just be extremely inefficient if not changed
        std::vector<std::size_t> sorted_indexes = sort_indexes(volumeSizes);

        int bigVolume_index = sorted_indexes.size() - 1;
        int smallVolume_index = 0;

        std::tuple<int, double, double, double, std::string, double> bestTransfer = getBestTransfer(
                sorted_indexes[bigVolume_index], sorted_indexes[smallVolume_index],
                block_volume_sourceRefCount, block_volume_sourceRefCount_iter,
                block_volume_targetRefCount, block_volume_targetRefCount_iter,
                currentTraffic, trafficSize,
                false, volumeSizes, desired_balance, margin);

        while ((std::get<0>(bestTransfer) == -1) && bigVolume_index > 0) {
            smallVolume_index++;
            if (smallVolume_index >= sorted_indexes.size()) {
                bigVolume_index--;
                smallVolume_index = 0;
            }
            if (bigVolume_index < 0) {
                break;
            }
            if (bigVolume_index == smallVolume_index) {
                continue;
            }
            bestTransfer = getBestTransfer(sorted_indexes[bigVolume_index],
                                           sorted_indexes[smallVolume_index],
                                           block_volume_sourceRefCount, block_volume_sourceRefCount_iter,
                                           block_volume_targetRefCount, block_volume_targetRefCount_iter,
                                           currentTraffic, trafficSize,
                                           false, volumeSizes, desired_balance, margin);
        }
        if (std::get<0>(bestTransfer) == -1) {
            std::get<0>(retVal) = -1;
            std::get<1>(retVal) = -1;
        } else {
            std::get<0>(retVal) = sorted_indexes[bigVolume_index];
            std::get<1>(retVal) = sorted_indexes[smallVolume_index];
        }
        std::get<2>(retVal) = std::get<0>(bestTransfer);
        std::get<3>(retVal) = std::get<1>(bestTransfer);
        std::get<4>(retVal) = std::get<2>(bestTransfer);
        std::get<5>(retVal) = std::get<3>(bestTransfer);
        std::get<6>(retVal) = std::get<4>(bestTransfer);
        std::get<7>(retVal) = std::get<5>(bestTransfer);
    }

    return retVal;
}

/**
 * @brief Receives a transfer object and applies the transfer to all of the objects saving the system state
 * @param bestIterationTransfer The transfer to apply
 * (sourceVolume, targetVolume, fileSn, replicate, delete, move, csv line of the best file to move)
 * @param currentTraffic How much traffic had allready been used
 * @param maxTraffic How much traffic in total can be used
 * @param block_volume_sourceRefCount Reference to the block reference object for source volume
 * @param block_volume_targetRefCount Reference to the block reference object for target volume
 */
void updateDbs(std::tuple<int, int, int, double, double, double, std::string, double> &bestIterationTransfer,
               double currentTraffic, double maxTraffic,
               std::vector<std::vector<short>> &block_volume_sourceRefCount,
               std::vector<std::vector<short>> &block_volume_sourceRefCount_iter,
               std::vector<std::vector<short>> &block_volume_targetRefCount,
               std::vector<std::vector<short>> &block_volume_targetRefCount_iter) {
    std::vector<std::string> splitted_content = split_string(std::get<6>(bestIterationTransfer), ",");
    int fileSn = std::stoi(splitted_content[1]);
    updateLoopEvade(fileSn);
    int number_of_blocks_in_file_line = std::stoi(splitted_content[4]);

    int source = std::get<0>(bestIterationTransfer);
    int target = std::get<1>(bestIterationTransfer);

    for (register int i = 0; i < 2 *
                                 number_of_blocks_in_file_line; i += 2) //Read block_sn and block_size simultaneously and add constrains to the model.
    {
        int block_sn = std::stoi(splitted_content[5 + i]);
        block_volume_sourceRefCount[block_sn][source]--;
        block_volume_targetRefCount[block_sn][target]++;
        block_volume_targetRefCount_iter[block_sn][target]++;
        if (fullMigration) {
            block_volume_targetRefCount[block_sn][source]--;
            block_volume_sourceRefCount[block_sn][target]++;
            block_volume_sourceRefCount_iter[block_sn][target]++;
        }
    }

    auto itr = std::find(g__volume_file_fileList[std::get<0>(bestIterationTransfer)].begin(),
                         g__volume_file_fileList[std::get<0>(bestIterationTransfer)].end(),
                         std::get<2>(bestIterationTransfer));
    if (itr != g__volume_file_fileList[std::get<0>(bestIterationTransfer)].end()) {
        g__volume_file_fileList[std::get<0>(bestIterationTransfer)].erase(itr);
    }

    //If we are under fullMigration then target volumes are also source volumes and so we need to give them the moved files as they can move again
    if (fullMigration) {
        g__volume_file_fileList[std::get<1>(bestIterationTransfer)].push_back(std::get<2>(bestIterationTransfer));
        g__file_sn_to_vol_num[std::get<2>(bestIterationTransfer)] = std::get<1>(bestIterationTransfer);
    }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void removeFile(
        const std::string& filePath,
        std::vector<std::vector<short>> &block_volume_sourceRefCount,
        std::vector<std::vector<short>> &block_volume_targetRefCount,
        std::pair<int, int>& lastSourceSn_block_file,
        std::pair<int, int>& lastTargetSn_block_file,
        std::vector<double>& blockSizes,
        std::vector<double>& volumeSizes
){
    std::string inputFile = getInputFileByPath(filePath);
    uint64_t fileSn = g__input_file_to_sn[inputFile];

    int currentVol = g__file_sn_to_vol_num[fileSn];
    std::cout<< "remove "<< filePath << " (file sn="<< fileSn << " ) from volume:"<< currentVol << std::endl;

    auto itr = std::find(g__volume_file_fileList[currentVol].begin(),
                         g__volume_file_fileList[currentVol].end(),
                         fileSn);
    if (itr != g__volume_file_fileList[currentVol].end()) {
        g__volume_file_fileList[currentVol].erase(itr);
        g__file_sn_to_vol_num[fileSn] = -1;

        //fix block reference
        std::string content = g__file_sn_to_line[fileSn];
        std::vector<std::string> splitted_content = split_string(content, ",");
        int number_of_blocks_in_file_line = std::stoi(splitted_content[4]);
        for (register int i = 0; i < 2 * number_of_blocks_in_file_line; i += 2) //Read block_sn and block_size simultaneously and add constrains to the model.
        {
            int block_sn = std::stoi(splitted_content[5 + i]);
            double block_size = ((double) std::stoull(splitted_content[5 + i + 1])) / 1024.0;

            block_volume_sourceRefCount[block_sn][currentVol]--;
            block_volume_targetRefCount[block_sn][currentVol]--;

            if(block_volume_sourceRefCount[block_sn][currentVol] == 0) { // new reference anymore
                volumeSizes[currentVol] -= block_size;
            }

        }
        g__file_sn_to_line[fileSn] = ""; //release space
    }
}

void addFileToVol(
        const std::string& filePath,
        const int volumeNum,
        std::vector<std::vector<short>> &block_volume_sourceRefCount,
        std::vector<std::vector<short>> &block_volume_targetRefCount,
        std::pair<int, int>& lastSourceSn_block_file,
        std::pair<int, int>& lastTargetSn_block_file,
        std::vector<double>& blockSizes,
        std::vector<double>& volumeSizes
){

    // assuming source volumes equals target volumes

    std::cout<< "add "<< filePath << " to volume:"<< volumeNum << std::endl;

    int lastBlock = std::max(lastSourceSn_block_file.first, lastTargetSn_block_file.first);
    int origLastBlock = lastBlock;

    std::ifstream fileStream(filePath.c_str(), std::ifstream::in);
    if (!fileStream.is_open()) {
        std::cout << "error opening file to add from changes. Path: " << filePath << std::endl;
        exit(1);
    }

    std::unordered_map<uint64_t, uint64_t> local_sn_to_global_sn = {};
    std::string content;
    std::string fileId;
    std::string fileLine = "";
    std::vector<std::string> splitted_content;
    uint64_t num_filtered_block = 0;
    while (std::getline(fileStream, content)) {
        std::istringstream ss(content);
        std::string firstToken = get_comma_token(ss, true);
        if (firstToken == "F") {
            if(fileLine != ""){
                std::cout << "More than 1 file in given change file.Path: " << filePath << std::endl;
                exit(1);
            }

            fileLine= content;
            get_comma_token(ss, true); // skip local sn
            fileId = get_comma_token(ss, true);
        }
        if (firstToken == "B") {
            splitted_content = split_string(content, ",");
            int blockSn = std::stoi(splitted_content[1]);
            std::string blockFp = splitted_content[2].substr(1);
            int refCount = std::stoi(splitted_content[3]); // always be 1 since only one logic file is within single file
            if(g__filter_blocks && is_block_filtered(blockFp)){
                continue;
            }

            num_filtered_block++;
            if(g__block_fp_to_sn.find(blockFp) == g__block_fp_to_sn.cend()){
                lastBlock++;
                // new block
                g__block_fp_to_sn[blockFp] = lastBlock;
                blockSizes.emplace_back(0); // a place holder, will be fixed later

                std::vector<short> sourceVolumeList = std::vector<short>(block_volume_sourceRefCount[0].size(), 0);
                block_volume_sourceRefCount.push_back(sourceVolumeList);

                std::vector<short> targetVolumeList = std::vector<short>(block_volume_targetRefCount[0].size(), 0);
                block_volume_targetRefCount.push_back(targetVolumeList);

            }
            int blockNewSn = g__block_fp_to_sn[blockFp];
            local_sn_to_global_sn[blockSn] = blockNewSn;

            block_volume_sourceRefCount[blockNewSn][volumeNum] += refCount;
            block_volume_targetRefCount[blockNewSn][volumeNum] += refCount;
        }
    }
    fileStream.close();

    if (fileId != "" && fileLine == "") {
        std::cout << "given file to add from changes has bad format. failed to get file id . Path: " << filePath << std::endl;
        exit(1);
    }

    int lastFile = std::max(lastSourceSn_block_file.second, lastTargetSn_block_file.second);
    ++lastFile;
    std::string file_sn = std::to_string(lastFile);
    std::string new_file_line = "F, " + file_sn + ", " + std::to_string(volumeNum) + "_" + fileId + ", " + file_sn +
                                "," + std::to_string(num_filtered_block);


    std::istringstream ss(fileLine);

    // skip file info
    static const int FILE_INFO_LAST_INDEX = 4;
    for(int i=0; i<FILE_INFO_LAST_INDEX; ++i){
        std::string a = get_comma_token(ss, true);
    }

    //add blocks
    std::string file_num_blocks = get_comma_token(ss, true);

    const uint64_t num_blocks = std::stoull(file_num_blocks);
    for(uint64_t i=0; i < num_blocks; ++i)
    {
        const std::string& block_sn = get_comma_token(ss, true);
        const std::string& block_size = (i==num_blocks-1) ? get_line(ss) : get_comma_token(ss, true);
        if(local_sn_to_global_sn.find(std::stoull(block_sn)) == local_sn_to_global_sn.cend()) {
            // probably filtered
            continue;
        }

        double block_size_double = std::stod(block_size);
        unsigned long block_size_long = static_cast<unsigned long>(block_size_double > 0 ? block_size_double: 4096.0); //should not happen. if it does we treat it as 4KB block
        const uint64_t block_new_sn = local_sn_to_global_sn[std::stoull(block_sn)];
        new_file_line += ", " + std::to_string(block_new_sn) + ", " + std::to_string(block_size_long);
        if(block_new_sn > origLastBlock){
            double size = ((double) block_size_long) / 1024.0;
            blockSizes[block_new_sn] = size;
        }

        if(block_volume_sourceRefCount[block_new_sn][volumeNum] == 1) { // the new file is the one brought the block -> size is added
            double size = ((double) block_size_long) / 1024.0;
            volumeSizes[volumeNum] += size;
        }
    }

    //fix file line
    g__file_sn_to_line[std::stoull(file_sn)] = new_file_line;
    g__volume_file_fileList[volumeNum].emplace_back(std::stoull(file_sn));
    g__input_file_to_sn[getInputFileByPath(filePath)]=std::stoull(file_sn);
    g__file_sn_to_vol_num[std::stoull(file_sn)] = volumeNum;

    //fix sns: (assumes source and target are the same volumes!
    lastSourceSn_block_file.first=lastBlock;
    lastTargetSn_block_file.first=lastBlock;
    lastSourceSn_block_file.second=lastFile;
    lastTargetSn_block_file.second=lastFile;

    const int host = getHostByPath(filePath);
    if(g__host_to_input_file_ordered.find(host) == g__host_to_input_file_ordered.cend()){
        g__host_to_input_file_ordered[host] = {};
    }

    g__host_to_input_file_ordered[host].emplace_back(getInputFileByPath(filePath));
}

int getSelectedVol(const std::string& filePath, std::vector<double>& volumeSizes){
    if(g__change_insert_type == "random"){
        return rand() % volumeSizes.size();
    }
    if(g__change_insert_type != "backup"){
        throw std::runtime_error("Not supported insert type: " + g__change_insert_type + " only random/backup is supported");
    }

    volatile int a = rand() % volumeSizes.size(); // just to not interrupt with seed
    std::cout <<"Ignore this line "<< a<< std::endl; // Now it won't be optimized out
    int host = getHostByPath(filePath);
    for(auto iter = g__host_to_input_file_ordered[host].rbegin(); iter != g__host_to_input_file_ordered[host].rend(); ++iter){
        const std::string newest_input_file = *iter;
        std::cout << "Try Find "<< newest_input_file<< std::endl;

        if(g__input_file_to_sn.find(newest_input_file) == g__input_file_to_sn.cend()){
            continue;
        }

        int sn = g__input_file_to_sn[newest_input_file];
        int vol = g__file_sn_to_vol_num[sn];
        static const int NOT_IN_SYSTEM = -1;
        if(vol == NOT_IN_SYSTEM){
            continue;
        }

        std::cout << "Found "<< newest_input_file << " in vol: "<< vol << std::endl;
        return vol;
    }

    throw std::runtime_error("did not find vol to insert the file: " + filePath + " in backup mode");
}

void applyChanges(
        const std::vector<std::string> &changes,
        std::vector<std::vector<short>> &block_volume_sourceRefCount,
        std::vector<std::vector<short>> &block_volume_targetRefCount,
        std::pair<int, int>& lastSourceSn_block_file,
        std::pair<int, int>& lastTargetSn_block_file,
        std::vector<double>& blockSizes,
        std::vector<double>& volumeSizes) {
    // **currently only random insert is supported**

    std::cout<< "********** changes start ***************" << std::endl;

    std::cout<< "Start applying " <<changes.size() << " Changes as follows:" << std::endl;

    for(const auto& change : changes){
        std::vector<std::string> splitted_change = split_string(change, ",");
        std::string del_path = split_string(splitted_change[1], ":")[1];
        std::string add_path = split_string(splitted_change[0], ":")[1];
        if(add_path!=""){
            int selectedVol = getSelectedVol(add_path, volumeSizes);
            addFileToVol(add_path, selectedVol, block_volume_sourceRefCount, block_volume_targetRefCount,
                         lastSourceSn_block_file, lastTargetSn_block_file, blockSizes,
                         volumeSizes);
            g__num_files++;

        }

        if(del_path!=""){
            removeFile(del_path, block_volume_sourceRefCount, block_volume_targetRefCount,
                       lastSourceSn_block_file, lastTargetSn_block_file, blockSizes, volumeSizes);
            g__num_files--;
        }
    }

    std::cout<< "********** changes end ***************" << std::endl;
}

void selectAndDoChanges(
        std::ifstream& changes_file,
        int max_num_changes,
        std::vector<std::vector<short>> &block_volume_sourceRefCount,
        std::vector<std::vector<short>> &block_volume_targetRefCount,
        std::pair<int, int>& lastSourceSn_block_file,
        std::pair<int, int>& lastTargetSn_block_file,
        std::vector<double>& blockSizes,
        std::vector<double>& volumeSizes) {
    double start_elapsed_secs = std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - g__begin).count();

    int num_changes = 0;
    std::string change_line;
    std::vector<std::string> changes = {};
    while(num_changes < max_num_changes && std::getline(changes_file, change_line)){
        if(!change_line.empty() && change_line[change_line.size()-1] == '\r')
        {
            change_line.erase(change_line.size()-1); // need dos2unix. this is a fix for removing '\r'
        }
        changes.emplace_back(change_line);
        ++num_changes;
    }

    applyChanges(changes, block_volume_sourceRefCount, block_volume_targetRefCount,
                 lastSourceSn_block_file, lastTargetSn_block_file, blockSizes, volumeSizes);

    double end_elapsed_secs = std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - g__begin).count();

    g__changeElapsedTimeSec += end_elapsed_secs - start_elapsed_secs;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
    Logging and output
*/

/**
 * @brief Print the chosen iteration into a csv solution file
 * @param outputCsvFile Solution file
 * @param iteration Iteration number
 * @param bestIterationTransfer The transfer chosen for the iteration
 * bestIterationTransfer: (sourceVolume, targetVolume, fileSn, replicate, delete, move, csv line of the best file to move)
 * @param currentTraffic Total traffic used
 * @param currentMigration Total data deleted
 * @param time Time that passed
 * @param iterationType Either balancing or optimization
 */
void printIteration(std::ofstream &outputCsvFile, int iteration, int global_step,
                    std::tuple<int, int, int, double, double, double, std::string, double> &bestIterationTransfer,
                    double currentTraffic,
                    double currentMigration, double time, std::string iterationType) {
    double trafficPercent = ((double) currentTraffic / initialTotalSourceSize) * 100;
    double migrationPercent = ((double) currentMigration / initialTotalSourceSize) * 100;

    outputCsvFile << iteration << "," << global_step << "," << std::get<0>(bestIterationTransfer) << ","
                  << std::get<1>(bestIterationTransfer) << "," << std::get<2>(bestIterationTransfer) << ","
                  << std::get<3>(bestIterationTransfer) << "," << std::get<4>(bestIterationTransfer) << ","
                  << std::get<5>(bestIterationTransfer) << "," << std::get<7>(bestIterationTransfer) << ","
                  << currentTraffic << "," << currentMigration << "," << trafficPercent << "," << migrationPercent
                  << "," << time << "," << iterationType << "\n";
}

/**
 * @brief Saves input after the solution, for debugging purposes
 * @param write_solution Solution file
 * @param volume_list Input list of the volumes
 * @param T_percentage Input traffic percentage
 * @param Margin Input error margin
 * @param model_time_limit Input time limit
 */
void saveInput(std::string write_solution, std::string volume_list, double T_percentage, double Margin,
               int model_time_limit) {
    std::ofstream solution(write_solution, std::ios_base::app);
    if (!solution) {
        std::cout << "Cannot open output file" << write_solution << std::endl;
        exit(1);
    }
    solution << "volume_list: " << volume_list << std::endl;
    solution << "T_percentage: " << T_percentage << std::endl;
    solution << "Margin: " << Margin << std::endl;
    solution << "model_time_limit: " << model_time_limit << std::endl;
}

/**
 * @brief Saves total run conclusion line, for statistic purposes
 * @param conclusionFilePath Path to the conclusion file
 * @param volume_list Input list of the volumes
 * @param maxTraffic Input traffic percentage
 * @param margin Input error margin
 * @param ingestionTime How long the ingestion took
 * @param totalTime Total runtime
 * @param totalTraffic Total traffic used
 * @param totalMigration Total deletion
 * @param initialVolumePercentage Volume state of input
 * @param finalVolumePercentage Volume state after migration plan
 */
void makeConclusionLine(std::string conclusionFilePath, std::string volume_list, int maxTraffic, double margin,
                        double ingestionTime, double totalTime,
                        double totalTraffic, double totalMigration, std::vector<double> &initialVolumePercentage,
                        std::vector<double> &finalVolumePercentage) {
    std::ofstream out(conclusionFilePath, std::ios_base::app);
    if (!out) {
        std::cout << "Cannot open output file\n";
    }
    std::cout << "In conclusion line. Volume list:" << volume_list << std::endl;

    auto splitSlashes = split_string(volume_list, "/");
    std::vector<std::string> splitted_content = split_string(
            replaceAll(splitSlashes[splitSlashes.size() - 1], "_ck8_", "_"), "_");

    std::string volumeName = splitted_content[0];
    std::string k = replaceAll(splitted_content[1], "k", "");
    std::string volumeNumber = "5";
    if (splitted_content.size() > 2) {
        volumeNumber = splitted_content[2];
    }
    double OptimizationTime = totalTime - ingestionTime;

    out << "GREEDY load balance" << ", "
        << volumeName << ", "
        << volumeNumber << ", "
        << k << ", "
        << maxTraffic << ", "
        << margin << ", "
        << ingestionTime << ", "
        << OptimizationTime << ","
        << totalTraffic << ", "
        << totalMigration << ", ";

    for (int i = 0; i < initialVolumePercentage.size(); i++) {
        out << initialVolumePercentage[i] << ", "
            << finalVolumePercentage[i] << ", ";
    }
    out << std::endl;
    out.close();

    std::cout << "End conclusion line:" << std::endl;

}

/**
 * @brief Saves the data required for creating the migration plan file and prints it into the given file
 * @param migrationPlanFilePath Path to where the migration plan should be saved
 * @param source_volume_list the data structures saving the paths of all the source volumes
 * @param volume_file_fileList the data structures of the current file location
 * @param isInitilization we want to save the file locations at the start and at the end. if false it means we are at the end and should print
 */
std::vector<std::vector<std::string> > parsedCSV;

void saveMigrationPlanState(int total_iteration, int migration_iteration, int change_iteration,
                            double iteration_max_traffic, double internal_iter_margin,double desired_margin,
                            std::string migrationPlanFilePath, std::vector<std::string> source_volume_list,
                            std::vector<std::vector<short>>& initial_block_volume_targetRefCount,
                            std::vector<std::vector<short>>& final_block_volume_targetRefCount_before_changes,
                            std::vector<std::vector<short>>& final_block_volume_targetRefCount,
                            std::vector<double>& blockSizes,
                            bool isInitilization,
                            bool isOnlyChanges) {
    std::cout << "saveMigrationPlanState " << isInitilization << std::endl;
    static auto iter_start = std::chrono::high_resolution_clock::now();
    try {
        if (isInitilization) {
            iter_start = std::chrono::high_resolution_clock::now();;
            for (int i = 0; i < source_volume_list.size(); i++) {
                parsedCSV.push_back(std::vector<std::string>());
                parsedCSV[i].push_back(source_volume_list[i]);
                std::string filesSns = "";
                for(const auto sn : g__volume_file_fileList[i]){
                    if(filesSns == ""){
                        filesSns = std::to_string(sn);

                    }
                    else{
                        filesSns += "-" + std::to_string(sn);
                    }
                }
                parsedCSV[i].push_back(filesSns);
            }
        } else {

            std::ofstream out(migrationPlanFilePath, std::ios_base::app);
            if (!out) {
                std::cout << "Cannot open migration plan output file\n";
            }

            char hostname[1024];
            gethostname(hostname, 1024);

            out
                    << "Server name,Iteration num,Iteration Max Traffic Bytes,PlaceHolder,PlaceHolder,PlaceHolder,Eps,Initial Internal Margin,Ensure load balance,PlaceHolder,Elapsed Time (seconds)"
                    << std::endl;
            std::string iter_indicator = std::to_string(total_iteration) + "_m" + std::to_string(migration_iteration)
                                         + "_c" + std::to_string(change_iteration);
            out<<hostname<<","<<iter_indicator<<","<<iteration_max_traffic<<",-,-,-,"<<desired_margin<<","<<internal_iter_margin<<",1,-,"
               <<(uint64_t)(std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - iter_start).count() - g__changeElapsedTimeSec) <<std::endl<<std::endl;

            out<<"Cluster path,Initial files,Final files,initial size B,initial size %,received B,deleted B,traffic B,overlap traffic B,block reuse B,aborted traffic B,final size B,final size %,total traffic B,total traffic%,"
                 "total deletion B,total deletion%,lb_score,is_valid_traffic,is_valid_lb,error_message"<<std::endl;

            std::vector<double> volumeInitSizes(source_volume_list.size(), 0.0);
            getVolumeSizes(volumeInitSizes, initial_block_volume_targetRefCount, blockSizes);

            double total_init_size = 0;
            for(double size: volumeInitSizes){
                total_init_size += size;
            }

            std::vector<double> volumeFinalSizes(source_volume_list.size(), 0.0);
            getVolumeSizes(volumeFinalSizes, final_block_volume_targetRefCount, blockSizes);
            double total_final_size = 0;
            for(double size: volumeFinalSizes){
                total_final_size += size;
            }

            double iter_traffic = 0;
            for (int i = 0; i < source_volume_list.size(); i++) {
                std::string filesSns = "";
                for(const auto sn : g__volume_file_fileList[i]){
                    if(filesSns == ""){
                        filesSns = std::to_string(sn);

                    }
                    else{
                        filesSns += "-" + std::to_string(sn);
                    }
                }
                parsedCSV[i].push_back(filesSns);

                //initial sizeB
                parsedCSV[i].push_back(std::to_string(volumeInitSizes[i]));

                //initial size%
                parsedCSV[i].push_back(std::to_string(static_cast<double>(volumeInitSizes[i])*100/total_init_size));

                //received B
                double traffic = 0;
                // PAY ATTENTION: in the following loop we only go over block that were existed in the initial state
                // so we won't take traffic of outer changes into account.
                for(int block=0; block< initial_block_volume_targetRefCount.size(); ++block){
                    // No taking traffic into account if:
                    // 1 - not marked as "only changes" iteration
                    // 2 - no new block number, in that case it's definitely was caused from outer change
                    // 3 - ref count was 0 in init state, otherwise, no copy/transfer of that block was needed
                    // 4 -  block is old, ref count in before change is x>0, but after changes it's y>x.
                    //      in that case, a new file that has the block was added from outer change. That means that the
                    //      transfer of that block would have done anyway, so we do now take that traffic into account
                    //      when calculating the algorithm's traffic
                    //      Attention: if ref count was 0 before, no file that has the block was in that volume.
                    //                 so there's no scenario of change that requires that block that will not increase
                    //                 ref count/will make the ref count 0 after changes (changes only remove existing
                    //                 file or add new file - it does not move file between volumes)
                    // 5-   if there's an overlap traffic of migration and changes, we take half of the traffic

                    if(!isOnlyChanges && block < initial_block_volume_targetRefCount.size() &&
                       initial_block_volume_targetRefCount[block][i] == 0 &&
                       final_block_volume_targetRefCount[block][i] > 0 &&
                       final_block_volume_targetRefCount_before_changes[block][i] > 0){
                        if(final_block_volume_targetRefCount[block][i] == final_block_volume_targetRefCount_before_changes[block][i]){
                            traffic+=blockSizes[block];
                            iter_traffic += blockSizes[block];
                        }
                        if(final_block_volume_targetRefCount[block][i] > final_block_volume_targetRefCount_before_changes[block][i]){
                            traffic+=blockSizes[block]/2;
                            iter_traffic += blockSizes[block]/2;
                        }
                    }

                    if(!isOnlyChanges && block < initial_block_volume_targetRefCount.size() &&
                       initial_block_volume_targetRefCount[block][i] == 0 &&
                       final_block_volume_targetRefCount[block][i] == 0 &&
                       final_block_volume_targetRefCount_before_changes[block][i] > 0){
                        traffic+=blockSizes[block]/2;
                        iter_traffic += blockSizes[block]/2;
                    }
                }

                parsedCSV[i].push_back(std::to_string(traffic));

                //deleted B
                double deleted = 0;
                for(int block=0; block< final_block_volume_targetRefCount.size(); ++block){
                    if(block >= initial_block_volume_targetRefCount.size()){
                        if(final_block_volume_targetRefCount[block][i] > 0){
                            deleted-=blockSizes[block];
                        }
                    }
                    else if(final_block_volume_targetRefCount[block][i]==0 && initial_block_volume_targetRefCount[block][i] > 0){
                        deleted+=blockSizes[block];
                    }
                }
                parsedCSV[i].push_back(std::to_string(deleted));

                //traffic B
                parsedCSV[i].push_back(std::to_string(traffic));

                //overlap traffic B - not implemented
                parsedCSV[i].push_back(std::to_string(0));

                //block reuse B - not implemented
                parsedCSV[i].push_back(std::to_string(0));

                //aborted traffic B - not implemented
                parsedCSV[i].push_back(std::to_string(0));

                //final size B
                parsedCSV[i].push_back(std::to_string(volumeFinalSizes[i]));

                //final size %
                parsedCSV[i].push_back(std::to_string(static_cast<double>(volumeFinalSizes[i])*100/total_final_size));
            }

            std::vector<std::string> sum_res;
            sum_res.push_back(",,,,,,,,,,,,");
            sum_res.push_back(std::to_string(iter_traffic)); // traffic
            sum_res.push_back(std::to_string(iter_traffic*100/initialTotalSourceSize));
            sum_res.push_back(std::to_string(total_init_size-total_final_size)); // dell
            sum_res.push_back(std::to_string((total_init_size-total_final_size)*100/initialTotalSourceSize));

            //lb score !!not normalized!! - assumes even spread
            double lb_score = *min_element(volumeFinalSizes.cbegin(),volumeFinalSizes.cend())/
                              *max_element(volumeFinalSizes.cbegin(),volumeFinalSizes.cend());
            sum_res.push_back(std::to_string(lb_score));
            g__lb_score = lb_score;


            sum_res.push_back(iteration_max_traffic >= iter_traffic ? "1" : "0");
            sum_res.push_back(isLegalState(volumeFinalSizes, g__desired_balance,internal_iter_margin) ? "1" : "0");
            sum_res.push_back("");
            parsedCSV.push_back(sum_res);
            for (int i = 0; i < parsedCSV.size(); i++) {
                for (int j = 0; j < parsedCSV[i].size(); j++) {
                    out << parsedCSV[i][j] << ",";
                }
                out << std::endl;
            }

            out << std::endl;

            g__totalTraffic += iter_traffic;
            parsedCSV = {};
        }
    } catch (const std::exception &e) {
        std::cout << e.what();
    }
}

void saveMigrationPlanSummary(const std::string& migrationPlanFilePath,
                              std::vector<std::string> source_volume_list,
                              std::vector<std::vector<short>>& initial_block_volume_targetRefCount,
                              std::vector<std::vector<short>>& final_block_volume_targetRefCount,
                              std::vector<double>& blockSizes,
                              double desired_margin){
    std::ofstream out(migrationPlanFilePath, std::ios_base::app);
    if (!out) {
        std::cout << "Cannot open migration plan output file\n";
    }

    out << "Summed results:" << std::endl;
    out << "Summ traffic,Summ traffic%,Deletion B,Deletion %,Total Elapsed time seconds,Sum chosen WT elapsed time,lb_score,is_valid_traffic,is_valid_lb" << std::endl;

    std::vector<double> volumeInitSizes(source_volume_list.size(), 0.0);
    getVolumeSizes(volumeInitSizes, initial_block_volume_targetRefCount, blockSizes);

    double total_init_size = 0;
    for(double size: volumeInitSizes){
        total_init_size += size;
    }

    std::vector<double> volumeFinalSizes(source_volume_list.size(), 0.0);
    getVolumeSizes(volumeFinalSizes, final_block_volume_targetRefCount, blockSizes);
    double total_final_size = 0;
    for(double size: volumeFinalSizes){
        total_final_size += size;
    }

    double totalDeletion = total_init_size-total_final_size;

    double lb_score = *min_element(volumeFinalSizes.cbegin(),volumeFinalSizes.cend())/
                      *max_element(volumeFinalSizes.cbegin(),volumeFinalSizes.cend());

    g__is_lb_valid &= isLegalState(volumeFinalSizes, g__desired_balance, desired_margin);

    out << g__totalTraffic << "," << g__totalTraffic*100/total_init_size <<","<<totalDeletion
        <<","<<totalDeletion*100/total_init_size << "," << std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - g__begin).count() - g__changeElapsedTimeSec << ","<<
        std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - g__begin).count() - g__changeElapsedTimeSec << ","<<lb_score <<","<<"1"<<","<<(g__is_lb_valid? "1" : "0")<<std::endl;
}

void initFileIndexInfo(const std::string& path){
    std::ifstream f(path);
    if(!f.is_open()){
        throw std::runtime_error("Could not open index file");
    }
    try {
        nlohmann::json file_index_data = nlohmann::json::parse(f);
        for (auto it = file_index_data.begin(); it != file_index_data.end(); ++it) {
            const std::string& user = it.key();
            for (auto user_data_it = it.value().begin(); user_data_it != it.value().end(); ++user_data_it) {
                const std::string &host = user_data_it.key();
                const int host_int = std::stoi(host);
                g__host_to_input_file_ordered[host_int] = {};

                auto host_list = user_data_it.value();
                for (auto list_it = host_list.begin(); list_it != host_list.end(); ++list_it) {
                    auto snap_json = list_it.value();
                    std::string snap_name = snap_json["snap_name"];
                    g__host_to_input_file_ordered[host_int].emplace_back(getInputFileByPath(snap_name));
                }
            }
        }
        f.close();
    }
    catch (...){
        f.close();
        throw;
    }

}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
std::tuple<int, int, int, double, double, double, std::string, double> getBestTransferFromList(
        std::vector<std::tuple<int,int,int>> transfers,
        std::vector<std::vector<short>> &block_volume_sourceRefCount,
        std::vector<std::vector<short>> &block_volume_targetRefCount,
        double currentTraffic, double trafficSize,
        std::vector<double> &volumeSizes,
        std::vector<double> &desired_balance,
        double margin,
        const std::string& split_type) {
    //Create empty transfer
    std::tuple<int, int, int, double, double, double, std::string, double> retVal;
    std::get<0>(retVal) = -1;       //Best sourceVolume
    std::get<1>(retVal) = -1;       //Best targetVolume
    std::get<2>(retVal) = -1;       //Best fileSn
    std::get<3>(retVal) = DBL_MAX;  //Best replicate
    std::get<4>(retVal) = -DBL_MAX; //Best delete
    std::get<5>(retVal) = DBL_MAX;  //Best move
    std::get<6>(retVal) = "";       //Csv line of the best file to move
    std::get<7>(retVal) = DBL_MAX;       //Best traffic

    std::vector<std::string> splitted_content;

    double best_lb_score = 0;
    bool best_lb_score_balanced = false;
    bool result_exist = false;

    for(auto& trans : transfers){
        uint64_t source = std::get<0>(trans);
        uint64_t target = std::get<1>(trans);
        uint64_t fileSn = std::get<2>(trans);
        std::string& fileLine = g__file_sn_to_line[fileSn];

        splitted_content = split_string(fileLine, ",");

        if (splitted_content[0] != "F") {
            continue;
        }

        int number_of_blocks_in_file_line = std::stoi(splitted_content[4]);
        double replication = 0;
        double deletion = 0;
        double movement = 0;
        double traffic = 0;
        for (register int i = 0; i < 2 *
                                     number_of_blocks_in_file_line; i += 2) //Read block_sn and block_size simultaneously and add constrains to the model.
        {
            int block_sn = std::stoi(splitted_content[5 + i]);
            int size_read = std::stoi(splitted_content[6 + i]);
            double blockSize = ((double) size_read) / 1024.0;
            int refInSource = block_volume_sourceRefCount[block_sn][source];
            int refInTarget = block_volume_targetRefCount[block_sn][target];
            if (refInSource == 1) {
                if (refInTarget > 0) {
                    deletion += blockSize; // Can delete
                } else {
                    movement += blockSize; // Needs to move
                    traffic += blockSize;
                }
            } else { //refInSource > 1
                if (refInTarget == 0) {
                    replication += blockSize; //Must replicate
                    movement += blockSize; // Needs to move
                    traffic += blockSize;
                }
            }
        }

        if(split_type == "naive_split"){
            double bestReclaim = std::get<3>(retVal) / std::max(1.0, std::get<4>(retVal));
            double currentReclaim = replication / std::max(1.0, deletion);
            if (!result_exist || ((currentTraffic + traffic <= trafficSize) && (currentReclaim < bestReclaim))) {
                result_exist = true;
                std::tuple<int, double, double, double, std::string, double> currentTransfer;
                std::get<0>(currentTransfer) = fileSn;
                std::get<1>(currentTransfer) = replication;
                std::get<2>(currentTransfer) = deletion;
                std::get<3>(currentTransfer) = movement;
                std::get<4>(currentTransfer) = fileLine;
                std::get<5>(currentTransfer) = traffic;

                std::get<0>(retVal) = source;
                std::get<1>(retVal) = target;
                std::get<2>(retVal) = fileSn;
                std::get<3>(retVal) = replication;
                std::get<4>(retVal) = deletion;
                std::get<5>(retVal) = movement;
                std::get<6>(retVal) = fileLine;
                std::get<7>(retVal) = traffic;
            }
        }
        if(split_type == "lb_split" || split_type == "smart_split"){
            std::tuple<int, double, double, double, std::string, double> currentTransfer;
            std::get<0>(currentTransfer) = fileSn;
            std::get<1>(currentTransfer) = replication;
            std::get<2>(currentTransfer) = deletion;
            std::get<3>(currentTransfer) = movement;
            std::get<4>(currentTransfer) = fileLine;
            std::get<5>(currentTransfer) = traffic;

            double current_lb_score = getLbScore(volumeSizes, currentTransfer, source, target, desired_balance, margin);

            bool is_balanced = isBalanced(volumeSizes, currentTransfer, source, target, desired_balance, margin);
            if(!result_exist || ((!best_lb_score_balanced && is_balanced) || (current_lb_score > best_lb_score && (is_balanced || (!is_balanced && !best_lb_score_balanced))))){
                best_lb_score = current_lb_score;
                best_lb_score_balanced = is_balanced;
                result_exist = true;
                std::get<0>(retVal) = source;
                std::get<1>(retVal) = target;
                std::get<2>(retVal) = fileSn;
                std::get<3>(retVal) = replication;
                std::get<4>(retVal) = deletion;
                std::get<5>(retVal) = movement;
                std::get<6>(retVal) = fileLine;
                std::get<7>(retVal) = traffic;
            }
        }
    }
    return retVal;
}
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char *argv[]) {
    /*
        Read configurations
    */
    g__begin = std::chrono::high_resolution_clock::now(); //Start the stopwatch for the total time.
    if (argc != 16) {
        std::cout
                << "arguments format is: {volumelist} {output} {conclusionFile} {timelimit} {Traffic} {margin} "
                   "{num_migration_iters} {num_changes_iters} {seed} {changes_file} {file_index} {change_pos}[start/end/middle] {total_perc_changes} "
                   "{change_insert_type} {num_runs}"
                << std::endl;
        return 0;
    }


    std::string volume_list = std::string(argv[1]);
    std::string output = std::string(argv[2]);
    std::string conclusionFile = std::string(argv[3]);
    int timelimit_inSeconds = std::stoi(std::string(argv[4]));
    int trafficPercent = std::stoi(std::string(argv[5]));
    double margin = std::stod(std::string(argv[6]));
    int num_migration_iters = std::stoi(std::string(argv[7]));
    int num_changes_iters = std::stoi(std::string(argv[8]));
    int seed = std::stoi(std::string(argv[9]));
    std::string changes_path = std::string(argv[10]);
    std::string files_index_path = std::string(argv[11]);
    std::string change_pos = std::string(argv[12]);
    int total_perc_changes = std::stoi(std::string(argv[13]));
    g__change_insert_type = std::string(argv[14]);
    int num_runs = std::stoi(std::string(argv[15]));
    initFileIndexInfo(files_index_path);

    std::ifstream changes_file(changes_path);
    std::srand(seed);

    // The current model has no differentiation between source ant target volumes, but for regression we leave the configuration here
    // Making this false would make source volumes only capable of sending files to target volumes
    fullMigration = true;

    /*
        Initialize data structures
    */
    std::string content;
    std::vector<std::string> source_volume_list;
    std::vector<std::string> target_volume_list;
    std::vector<double> desired_balance;

    std::ifstream volume_list_f(volume_list.c_str(), std::ifstream::in);
    if (!volume_list_f.is_open()) {
        std::cout << "error opening volume list." << std::endl;
        exit(1);
    }

    /*
        Parse volume strings and desired balances
        The input files are made of lines made of
        [path to volume file in Michal & Paulina standard], [a|s|t where s is source, t is target and a is all], [fraction of desired balance]
    */
    while (std::getline(volume_list_f, content)) {
        std::size_t firstComma = content.find(", ");
        std::size_t secondComma = content.find(", ", firstComma + 1);

        std::string file_path = content.substr(0, firstComma);
        std::string volume_type = content.substr(firstComma + 1, secondComma - firstComma - 1);
        std::string::iterator end_pos = std::remove(volume_type.begin(), volume_type.end(), ' ');
        volume_type.erase(end_pos, volume_type.end());

        double desiredLoad = std::stod(content.substr(secondComma + 1));
        if (volume_type == "s" || volume_type == "a") {
            source_volume_list.push_back(file_path);
        }
        if (volume_type == "t" || volume_type == "a") {
            target_volume_list.push_back(file_path);
            desired_balance.push_back(desiredLoad);
        }
    }

    g__desired_balance = desired_balance;

    /*
        Validate correct load distribution
    */
    double total = 0;
    for (int volume = 0; volume < desired_balance.size(); volume++) {
        total += desired_balance[volume];
    }

    // if(total != 1) {
    //     std::cout << "illegal load distribution! " << total << " != 1" << std::endl;
    // }

    /*
        Ingest data
    */
    std::vector<std::vector<short>> block_volume_sourceRefCount;
    std::vector<std::vector<short>> block_volume_targetRefCount;

    std::pair<int, int> lastSourceSn_block_file = getLastBlockAndFileSn(source_volume_list);
    std::pair<int, int> lastTargetSn_block_file = getLastBlockAndFileSn(target_volume_list);
    std::vector<double> blockSizes(lastSourceSn_block_file.first + 1, 0.0);
    std::vector<double> volumeSizes(target_volume_list.size(), 0.0);

    calcBlockSizes(blockSizes, source_volume_list);

    ingest(source_volume_list, target_volume_list, block_volume_sourceRefCount, block_volume_targetRefCount,
           lastSourceSn_block_file, lastTargetSn_block_file);

    getVolumeSizes(volumeSizes, block_volume_targetRefCount, blockSizes);
    std::vector<std::vector<short>> block_volume_targetRefCount_orig_init = block_volume_targetRefCount;

    initialTotalSourceSize = 0;
    for (int volume = 0; volume < volumeSizes.size(); volume++) {
        initialTotalSourceSize += volumeSizes[volume];
    }
    g__initialTotalSourceSize = initialTotalSourceSize;

    std::vector<double> startingVolumePercentages(target_volume_list.size(), 0.0);
    getVolumePercentages(startingVolumePercentages, block_volume_targetRefCount, blockSizes);

    double trafficSize = ((double) trafficPercent * initialTotalSourceSize) / 100;
    // Greedy seldom uses it's traffic due to superfluous movements, using only around 83.33 of it's given traffic
    // So we allow it a relative extra 20% as to allow it to use all it's traffic
    double handicappedTraffic =
            trafficSize + 0.2 * trafficSize; // this was take from the orig version of this algorithm
    /*
        Prepare first iteration
    */
    double previousTraffic = -1;
    int step = 0;
    int total_iteration = 0;
    int total_migration_iteration = 0;
    int total_change_iteration = 0;
    std::ofstream outputCsvFile;
    outputCsvFile.open(output);
    outputCsvFile
            << "Iteration, Global Iteration,sourceVolume,targetVolume,fileSn,replicated KB,deleted KB,moved KB,traffic KB,totalTraffic KB,totalMigration KB,traffic% of Total source,deletion% of Total source,Time(s),iteration type\n";

    double elapsed_secs = std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - g__begin).count();
    double ingestionTime = elapsed_secs;

    /*
        Empirically we found that optimizing to the final margin is worse than slowly refining the margin
        empirically we found the best results for 5 phases starting from 1.5 * margin and ending in 1 * margin
        we also found that an extension smaller than 0.05 results in worse results
        for every margin we first make sure we are in a legal state under the margin and then we optimize under the margin
        margin + marginExtension; margin + 0.75 * marginExtension; margin + 0.5 * marginExtension; margin + 0.25 * marginExtension; margin
    */
    bool skipBalancing = false;

    double marginExtension = std::max(0.05, 0.5 * margin);

    /*
    Receiving a margin of above 1 indicates we wish no load balancing and can skip the strenuous load balancing logic
    */
    if (margin >= 1) {
        marginExtension = 0;
        skipBalancing = true;
    }

    const int num_max_changes_total = std::round((total_perc_changes * g__num_files) / 100);
    for(int num_run=1; num_run<=num_runs;++num_run){
        int migration_iteration = 0;
        int change_iteration = 0;
        double trafficSizeForIter = handicappedTraffic / num_migration_iters;
        double remainingHandicappedTraffic = handicappedTraffic;

        double iter_margin_extender = marginExtension / num_migration_iters;
        double iter_margin = margin + marginExtension;

        double marginSize = ((double) margin * initialTotalSourceSize);

        std::cout << "initialTotalSourceSize: " << initialTotalSourceSize << std::endl;
        std::cout << "max Traffic: " << handicappedTraffic << std::endl;
        std::cout << "margin size: " << marginSize << std::endl;

        int num_max_changes_iter = 0;
        int left_changes = 0;
        if (num_changes_iters != 0) {
            num_max_changes_iter = std::round(num_max_changes_total / num_changes_iters);
            left_changes = num_max_changes_total % num_changes_iters; // Run again
        }

        std::cout << "num_max_changes_total: " << num_max_changes_total << std::endl;
        std::cout << "num_max_changes_iter: " << num_max_changes_iter << std::endl;
        std::cout << "left_changes: " << left_changes << std::endl;

        while ((change_pos == "only_changes" && change_iteration < num_changes_iters)) {
            total_change_iteration++;

            int num_max_changes_for_iter = num_max_changes_iter;
            if (left_changes > 0) {
                num_max_changes_for_iter++;
                left_changes--;
            }

            std::vector<std::vector<short>> block_volume_targetRefCount_bk = block_volume_targetRefCount;

            // just print headers
            static const int NO_NEEDED = 0;
            static const bool ONLY_CHANGES_ITER = true;
            saveMigrationPlanState(total_iteration, total_migration_iteration, total_change_iteration, NO_NEEDED, margin, margin,
                                   output + "_migration_plan.csv", source_volume_list, block_volume_targetRefCount_bk,
                                   block_volume_targetRefCount_bk, block_volume_targetRefCount, blockSizes, true,
                                   ONLY_CHANGES_ITER);
            selectAndDoChanges(changes_file, num_max_changes_for_iter, block_volume_sourceRefCount,
                               block_volume_targetRefCount,
                               lastSourceSn_block_file, lastTargetSn_block_file, blockSizes,
                               volumeSizes);
            change_iteration++;
            total_iteration++;
            saveMigrationPlanState(total_iteration, total_migration_iteration, total_change_iteration, NO_NEEDED, margin, margin,
                                   output + "_migration_plan.csv", source_volume_list,
                                   block_volume_targetRefCount_bk, block_volume_targetRefCount_bk,
                                   block_volume_targetRefCount, blockSizes, false,
                                   ONLY_CHANGES_ITER);
        }

        if (change_pos == "only_changes") {
            if(num_run == num_runs){
                saveMigrationPlanSummary(output + "_migration_plan.csv", source_volume_list,
                                         block_volume_targetRefCount_orig_init,
                                         block_volume_targetRefCount, blockSizes, margin);
                return 0;
            }else{
                continue;
            }
        }

        double leftovers = 0;
        for (int iter = 1; iter <= num_migration_iters; ++iter) {
            iter_margin -= iter_margin_extender;

            //
            std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!starting iter " << iter << " out of " << num_migration_iters
                      << " !!!!!!!!!!!!!!!!!!" << std::endl;
            resetLoopEvade();
            std::vector<std::vector<short>> block_volume_sourceRefCount_iter = block_volume_sourceRefCount;
            std::vector<std::vector<short>> block_volume_targetRefCount_iter = block_volume_targetRefCount;
            std::vector<std::vector<short>> block_volume_targetRefCount_bk = block_volume_targetRefCount;
            getVolumeSizes(volumeSizes, block_volume_targetRefCount, blockSizes);
            std::vector<double> initVolumeSizes(target_volume_list.size(), 0.0);
            initVolumeSizes = volumeSizes;
            total_iteration++;
            migration_iteration++;
            total_migration_iteration++;
            double totalStartSize = 0;

            for (int volume = 0; volume < volumeSizes.size(); volume++) {
                totalStartSize += volumeSizes[volume];
            }

            double iterTrafficSize = 0;
            if(change_pos == "smart_split"){
                iterTrafficSize = num_run<num_runs ?  handicappedTraffic :remainingHandicappedTraffic;
            }else{
                iterTrafficSize = trafficSizeForIter + leftovers;
            }

            double iterHandicappedTrafficForPlanner = iterTrafficSize;
            if(change_pos == "lb_split" || change_pos == "naive_split"){
                iterHandicappedTrafficForPlanner = remainingHandicappedTraffic;
            }
            double iterHandicappedTraffic = iterTrafficSize;
            double iter_current_traffic = 0;

            static const bool NOT_ONLY_CHANGES_ITER = false;
            saveMigrationPlanState(total_iteration, total_migration_iteration, total_change_iteration, iterTrafficSize, iter_margin,
                                   margin, output + "_migration_plan.csv", source_volume_list,
                                   block_volume_targetRefCount_bk, block_volume_targetRefCount, block_volume_targetRefCount,
                                   blockSizes, true, NOT_ONLY_CHANGES_ITER);

            // backup for the planner
            std::vector<std::vector<short>> block_volume_sourceRefCount_before_planner = block_volume_sourceRefCount;
            std::vector<std::vector<short>> block_volume_targetRefCount_before_planner = block_volume_targetRefCount;
            std::vector<std::vector<uint64_t>> volume_file_fileList_bk = g__volume_file_fileList;
            std::unordered_map<uint64_t, int>  file_sn_to_vol_num_bk= g__file_sn_to_vol_num;
            double currentTraffic_bk = g__currentTraffic;
            double calcDeletion_bk= g__calcDeletion;
            if(change_pos == "smart_split"){
                std::unordered_map<uint64_t, uint64_t> g__transfers_sn_to_volume= {};
                std::unordered_map<uint64_t, uint64_t> g__transfers_sn_to_source_volume= {};
            }

            if(change_pos == "smart_split" || change_pos == "migration_with_continuous_changes"||
               ((change_pos == "lb_split" || change_pos == "naive_split" || change_pos == "migration_before_changes" ) &&iter == 1) ||
               ((change_pos == "migration_after_changes" ) &&iter == num_migration_iters)){
                // Greedy algorithm planner
                double greedy_ext_step = iter_margin * 0.5;
                for (double currentMargin = iter_margin + greedy_ext_step;
                     currentMargin >= iter_margin; currentMargin -= (greedy_ext_step / 4)) {
                    double iterMaxIterationTrafic = iter_current_traffic + (iterHandicappedTrafficForPlanner - iter_current_traffic) /
                                                                           5; // as previous step + remaining/5
                    if (currentMargin == iter_margin) {
                        iterMaxIterationTrafic = iterHandicappedTrafficForPlanner;
                    }
                    getVolumeSizes(volumeSizes, block_volume_targetRefCount, blockSizes);

                    std::tuple<int, int, int, double, double, double, std::string, double> bestIterationTransfer = greedyIterate(
                            source_volume_list, target_volume_list, block_volume_sourceRefCount,
                            block_volume_sourceRefCount_iter,
                            block_volume_targetRefCount, block_volume_targetRefCount_iter, iter_current_traffic,
                            iterMaxIterationTrafic,
                            true, volumeSizes,
                            desired_balance, currentMargin);
                    /*
                        Load-balancing step
                    */
                    while (!skipBalancing && !isEmptySolution(bestIterationTransfer) &&
                           !isLegalState(volumeSizes, desired_balance, currentMargin)) {
                        step++;
                        iter_current_traffic += std::get<7>(bestIterationTransfer);
                        g__currentTraffic += std::get<7>(bestIterationTransfer);
                        g__calcDeletion += std::get<4>(bestIterationTransfer) - std::get<3>(bestIterationTransfer);

                        g__transfers_sn_to_volume[std::get<2>(bestIterationTransfer)]=std::get<1>(bestIterationTransfer); // change
                        if(g__transfers_sn_to_source_volume.find(std::get<2>(bestIterationTransfer)) != g__transfers_sn_to_source_volume.end()){
                            g__transfers_sn_to_source_volume[std::get<2>(bestIterationTransfer)] = std::get<0>(bestIterationTransfer); // change
                        }

                        elapsed_secs =
                                std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - g__begin).count() -
                                g__changeElapsedTimeSec;
                        printIteration(outputCsvFile, iter, step, bestIterationTransfer, g__currentTraffic, g__calcDeletion,
                                       elapsed_secs, "balance");
                        updateDbs(bestIterationTransfer, iter_current_traffic, iterMaxIterationTrafic,
                                  block_volume_sourceRefCount,
                                  block_volume_sourceRefCount_iter,
                                  block_volume_targetRefCount,
                                  block_volume_targetRefCount_iter);
                        getVolumeSizes(volumeSizes, block_volume_targetRefCount, blockSizes);

                        bestIterationTransfer = greedyIterate(source_volume_list, target_volume_list,
                                                              block_volume_sourceRefCount, block_volume_sourceRefCount_iter,
                                                              block_volume_targetRefCount, block_volume_targetRefCount_iter,
                                                              iter_current_traffic, iterMaxIterationTrafic,
                                                              true, volumeSizes,
                                                              desired_balance, currentMargin);
                    }
                    if (!skipBalancing) {
                        if (!isLegalState(volumeSizes, desired_balance, currentMargin)) {
                            std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!cannot balance to " << currentMargin
                                      << " !!!!!!!!!!!!!!!!!!" << std::endl;
                        } else {
                            std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!balanced to " << currentMargin << " !!!!!!!!!!!!!!!!!!"
                                      << std::endl;
                        }
                    }

                    double totalSize = 0.0;
                    for (int volume = 0; volume < volumeSizes.size(); volume++) {
                        totalSize += volumeSizes[volume];
                    }

                    getVolumeSizes(volumeSizes, block_volume_targetRefCount, blockSizes);
                    bestIterationTransfer = greedyIterate(source_volume_list, target_volume_list,
                                                          block_volume_sourceRefCount, block_volume_sourceRefCount_iter,
                                                          block_volume_targetRefCount, block_volume_targetRefCount_iter,
                                                          iter_current_traffic, iterMaxIterationTrafic,
                                                          false, volumeSizes, desired_balance, currentMargin);
                    /*
                        Capacity-reduction step
                    */
                    while (!isEmptySolution(bestIterationTransfer)) {
                        step++;
                        iter_current_traffic += std::get<7>(bestIterationTransfer);
                        g__currentTraffic += std::get<7>(bestIterationTransfer);

                        g__calcDeletion += std::get<4>(bestIterationTransfer) - std::get<3>(bestIterationTransfer);

                        g__transfers_sn_to_volume[std::get<2>(bestIterationTransfer)]=std::get<1>(bestIterationTransfer); // change
                        if(g__transfers_sn_to_source_volume.find(std::get<2>(bestIterationTransfer)) != g__transfers_sn_to_source_volume.end()){
                            g__transfers_sn_to_source_volume[std::get<2>(bestIterationTransfer)] = std::get<0>(bestIterationTransfer); // change
                        }

                        elapsed_secs =
                                std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - g__begin).count() -
                                g__changeElapsedTimeSec;
                        printIteration(outputCsvFile, iter, step, bestIterationTransfer, g__currentTraffic, g__calcDeletion,
                                       elapsed_secs, "optimize");
                        if (elapsed_secs > timelimit_inSeconds) {
                            break;
                        }
                        updateDbs(bestIterationTransfer, iter_current_traffic, iterMaxIterationTrafic,
                                  block_volume_sourceRefCount, block_volume_sourceRefCount_iter,
                                  block_volume_targetRefCount, block_volume_targetRefCount_iter);
                        getVolumeSizes(volumeSizes, block_volume_targetRefCount, blockSizes);

                        bestIterationTransfer = greedyIterate(source_volume_list, target_volume_list,
                                                              block_volume_sourceRefCount, block_volume_sourceRefCount_iter,
                                                              block_volume_targetRefCount, block_volume_targetRefCount_iter,
                                                              iter_current_traffic,
                                                              iterMaxIterationTrafic, false,
                                                              volumeSizes, desired_balance, currentMargin);
                    }
                    if (isLegalState(volumeSizes, desired_balance, currentMargin)) {
                        std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!optimized " << currentMargin << " !!!!!!!!!!!!!!!!!!"
                                  << std::endl;
                    } else {
                        std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!unable to optimize " << currentMargin
                                  << " !!!!!!!!!!!!!!!!!!" << std::endl;
                    }
                    if (skipBalancing) {
                        break;
                    }
                }
            }

            ////////////////////////
            // split phase
            if(change_pos == "smart_split" || change_pos == "lb_split" || change_pos == "naive_split"){
                block_volume_sourceRefCount = block_volume_sourceRefCount_before_planner;
                block_volume_targetRefCount = block_volume_targetRefCount_before_planner;
                g__volume_file_fileList = volume_file_fileList_bk;
                g__file_sn_to_vol_num = file_sn_to_vol_num_bk;
                g__currentTraffic = currentTraffic_bk;
                g__calcDeletion = calcDeletion_bk;
                iter_current_traffic = 0;

                std::vector<std::tuple<int, int, int>> transfers;
                for (const auto& pair : g__transfers_sn_to_volume) {
                    //each transfer is source_vol, dest_vol, file_sn
                    transfers.emplace_back(g__transfers_sn_to_source_volume[pair.first], pair.second, pair.first);
                }

                //split of transfers
                std::tuple<int, int, int, double, double, double, std::string, double> bestTransfer;
                bestTransfer = getBestTransferFromList(transfers, block_volume_sourceRefCount,
                                                       block_volume_targetRefCount,
                                                       iter_current_traffic, trafficSizeForIter,
                                                       volumeSizes, desired_balance, margin, change_pos);
                while (!isEmptySolution(bestTransfer)) {
                    iter_current_traffic += std::get<7>(bestTransfer);
                    g__currentTraffic += std::get<7>(bestTransfer);
                    g__calcDeletion += std::get<4>(bestTransfer) - std::get<3>(bestTransfer);

                    //remove sn transfer from vector
                    transfers.erase(
                            std::remove_if(transfers.begin(), transfers.end(),
                                           [&bestTransfer](auto& trans) { return std::get<2>(trans) == std::get<2>(bestTransfer);}  // Predicate by file sn
                            ),
                            transfers.end()
                    );

                    elapsed_secs =
                            std::chrono::duration<double>(std::chrono::high_resolution_clock::now() - g__begin).count() -
                            g__changeElapsedTimeSec;
                    printIteration(outputCsvFile, iter, step, bestTransfer, g__currentTraffic, g__calcDeletion,
                                   elapsed_secs, "split");
                    updateDbs(bestTransfer, iter_current_traffic, trafficSizeForIter,
                              block_volume_sourceRefCount,
                              block_volume_sourceRefCount_iter,
                              block_volume_targetRefCount,
                              block_volume_targetRefCount_iter);
                    getVolumeSizes(volumeSizes, block_volume_targetRefCount, blockSizes);
                    bestTransfer = getBestTransferFromList(transfers, block_volume_sourceRefCount,
                                                           block_volume_targetRefCount,
                                                           iter_current_traffic, trafficSizeForIter,
                                                           volumeSizes, desired_balance, margin, change_pos);
                }
            }

            ///////////////////////
            std::vector<std::vector<short>> block_volume_targetRefCount_before_changes = block_volume_targetRefCount;

            // Do change before we calculate migration if change iter has left
            if (change_iteration < num_changes_iters) {
                total_change_iteration++;
                int num_max_changes_for_iter = num_max_changes_iter;
                if (left_changes > 0) {
                    num_max_changes_for_iter++;
                    left_changes--;
                }

                selectAndDoChanges(changes_file, num_max_changes_for_iter, block_volume_sourceRefCount,
                                   block_volume_targetRefCount,
                                   lastSourceSn_block_file, lastTargetSn_block_file, blockSizes,
                                   volumeSizes);
                change_iteration++;
            }

            std::vector<double> finalVolumePercentages(target_volume_list.size(), 0.0);
            getVolumePercentages(finalVolumePercentages, block_volume_targetRefCount, blockSizes);

            double actual_traffic_bk = g__totalTraffic;
            saveMigrationPlanState(total_iteration, total_migration_iteration, total_change_iteration, iterTrafficSize,
                                   iter_margin, margin, output + "_migration_plan.csv", source_volume_list,
                                   block_volume_targetRefCount_bk, block_volume_targetRefCount_before_changes,
                                   block_volume_targetRefCount, blockSizes, false, NOT_ONLY_CHANGES_ITER);
            double iter_actual_traffic = g__totalTraffic - actual_traffic_bk;
            std::cout << "Write conclusion line:" << std::endl;

            /*
                Save statistics
            */
            makeConclusionLine(conclusionFile, volume_list, trafficPercent, margin, ingestionTime, elapsed_secs,
                               g__currentTraffic / initialTotalSourceSize, g__calcDeletion / initialTotalSourceSize,
                               startingVolumePercentages, finalVolumePercentages);
            std::cout << "Write saveInput:" << std::endl;

            saveInput(output, volume_list, trafficPercent, margin, timelimit_inSeconds);
            std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Finish iter " << iter << " out of " << num_migration_iters
                      << " !!!!!!!!!!!!!!!!!!" << std::endl;
            std::cout << "Traffic:" << iter_current_traffic << std::endl;
            std::cout << "Actual Traffic:" << iter_actual_traffic << std::endl;

            leftovers = 0;
            remainingHandicappedTraffic -= iter_actual_traffic;
            g__is_lb_valid &= isLegalState(volumeSizes, g__desired_balance,
                                           iter_margin);
        }
    }
    saveMigrationPlanSummary(output + "_migration_plan.csv",source_volume_list, block_volume_targetRefCount_orig_init,
                             block_volume_targetRefCount, blockSizes, margin);
}
