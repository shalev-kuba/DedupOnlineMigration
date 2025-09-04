#include "AlgorithmDSManager.hpp"

#include <iostream>
#include <algorithm>
#include <utility>
#include <cfloat>
#include <math.h>
#include <sstream>
#include <string>
#include <istream>
#include "json.hpp"

AlgorithmDSManager::AlgorithmDSManager(
        const std::vector<std::string>& workloads_paths,
        const int requested_number_of_fingerprints,
        const int num_changes_iterations,
        const int change_seed,
        const int changes_perc,
        const std::string& changes_input_file,
        const std::string& index_path,
        const bool load_balance,
        const ChangeType change_type,
        const int num_runs,
        const bool is_only_appearances_mat) :
        m_requested_number_of_fingerprints(requested_number_of_fingerprints),
        m_workloads_paths(getWorkloadsFullPaths(workloads_paths)),
        m_number_of_files_for_clustering(0),
        m_number_of_fingerprints_for_clustering(0),
        m_initial_system_size_with_deduplication(0),
        m_optimal_system_size_with_deduplication(0),
        m_host_name(Utility::getHostName()),
        m_change_input_file_path(Utility::getFullPath(changes_input_file)),
        m_max_file_sn(0)
        {
            const std::vector<int> fingerprints_for_clustering_ordered_by_SN = initializeAndSelectFingerprints();
            initializeAppearancesMatrix(fingerprints_for_clustering_ordered_by_SN, load_balance, is_only_appearances_mat);
            initializeChangesList(changes_input_file, change_seed,
                                  num_changes_iterations, changes_perc, change_type, num_runs);
            initFileIndexInfo(index_path);
            if(!is_only_appearances_mat)
            {
                initializeDissimilaritiesMatrix();
            }
        }

void AlgorithmDSManager::initFileIndexInfo(const std::string& path){
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
                m_host_to_file_ordered[host_int] = {};

                auto host_list = user_data_it.value();
                for (auto list_it = host_list.begin(); list_it != host_list.end(); ++list_it) {
                    auto snap_json = list_it.value();
                    std::string snap_name = snap_json["snap_name"];
                    m_host_to_file_ordered[host_int].emplace_back(Utility::splitString(Utility::splitString(snap_name, "_IF")[1], "_TS")[0]);
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

std::vector<std::ifstream> AlgorithmDSManager::getWorkloadsStreams() const{

    std::vector<std::ifstream> workloads_ifstream;
    //read files
    for (int i = 0; i < m_workloads_paths.size(); ++i) {
        workloads_ifstream.emplace_back(m_workloads_paths[i]);
        if (!workloads_ifstream[i].is_open()) {
            workloads_ifstream[i].close();
            throw std::runtime_error("error opening workload with index " + std::to_string(i));
        }
    }

    return std::move(workloads_ifstream);
}

void AlgorithmDSManager::closeWorkloadsStreams(std::vector<std::ifstream>& workloads_streams){

    //close all workloads' streams we opened before if still opened
    for(auto& f : workloads_streams){
        if (!f.is_open())
            f.close();
    }
}

void AlgorithmDSManager::updateFPWithWorkloadBlockLine(const std::vector<std::string>& splitted_line_content,
                                                       std::unordered_set<int>& current_blocks,
                                                       const int requested_num_fps,
                                                       std::priority_queue<std::pair<std::string, int>>& minhash_fingerprints){
    const int block_sn = Utility::getWorkloadBlockSN(splitted_line_content);
    if (!current_blocks.insert(block_sn).second)//already used this fp
        return;

    const std::string& block_fingerprint = Utility::getWorkloadBlockFingerprint(splitted_line_content);
    static const int NO_LIMIT = -1;

    //if we yet to collect m_requested_number_of_fingerprints fps, or we need all the fps, add it
    if (requested_num_fps == NO_LIMIT || minhash_fingerprints.size() < requested_num_fps){
        minhash_fingerprints.push(std::make_pair(block_fingerprint, block_sn));
        return;
    }

    //if fp is lexi prior to the max value we currently have, replace them
    if (block_fingerprint < minhash_fingerprints.top().first) {
        minhash_fingerprints.pop();
        minhash_fingerprints.push(std::make_pair(block_fingerprint, block_sn));
    }
}

void AlgorithmDSManager::updateFPWithWorkload(std::ifstream& workload_file, const int workload_index,
                                              int& current_algo_file_index,std::unordered_set<int>& current_blocks,
                                              const int requested_num_fps,
                                              std::priority_queue<std::pair<std::string, int>>& minhash_fingerprints){
    const std::string& workload_path = getWorkloadFullPath(workload_index);
    m_initial_mapping[workload_path] = {};
    std::string line_content;
    while (std::getline(workload_file, line_content)) {
        if(line_content.length() == 0){
            continue;
        }
        const std::vector<std::string> splitted_line_content = Utility::splitString(line_content, ',');
        if(Utility::isWorkloadBlockLine(splitted_line_content)){
            updateFPWithWorkloadBlockLine(splitted_line_content, current_blocks, requested_num_fps, minhash_fingerprints);
        }
        else if (Utility::isWorkloadFileLine(splitted_line_content)) {
            //count files and match workload i and file m_number_of_files_for_clustering (algorithm's sn) to original file sn
            m_initial_mapping.at(workload_path).insert(current_algo_file_index);
            const int file_sn = Utility::getWorkloadFileSN(splitted_line_content);
            m_file_sn_to_algo_index[file_sn] = current_algo_file_index;
            m_max_file_sn = std::max(m_max_file_sn, file_sn);
            m_file_index_to_sn[current_algo_file_index++] = file_sn;
        }
    }
}

std::vector<int> AlgorithmDSManager::getFPForClusteringOrderedBySN(std::priority_queue<std::pair<std::string, int>>& minhash_fingerprints,
                                                                   std::map<std::string, int>& fp_to_index){
    std::vector<int> fingerprints_for_clustering_ordered_by_SN;
    fingerprints_for_clustering_ordered_by_SN.reserve(minhash_fingerprints.size());

    std::map<int, std::string> sn_to_fp = {};
    while (!minhash_fingerprints.empty()){
        sn_to_fp[minhash_fingerprints.top().second] = minhash_fingerprints.top().first;
        fingerprints_for_clustering_ordered_by_SN.emplace_back(minhash_fingerprints.top().second);
        minhash_fingerprints.pop();
    }

    //sort by SN
    sort(fingerprints_for_clustering_ordered_by_SN.begin(), fingerprints_for_clustering_ordered_by_SN.end());

    for(int i=0; i < fingerprints_for_clustering_ordered_by_SN.size(); ++i){
        fp_to_index[sn_to_fp[fingerprints_for_clustering_ordered_by_SN[i]]] = i;
    }

    return std::move(fingerprints_for_clustering_ordered_by_SN);
}

std::vector<AlgorithmDSManager::ChangeInfo> AlgorithmDSManager::translateChangeLinesToChangeInfos(
        const std::vector<std::string>& changes_lines, const ChangeType change_type){
    std::vector<AlgorithmDSManager::ChangeInfo> res;
    res.reserve(changes_lines.size());
    for(const auto& change : changes_lines){
        std::vector<std::string> splitted_change = Utility::splitString(change, ",");
        std::string del_path = Utility::splitString(splitted_change[1], ":")[1];
        std::string add_path = Utility::splitString(splitted_change[0], ":")[1];

        int rand_vol = ChangeInfo::NOT_RELEVANT;
        if(add_path != ChangeInfo::NOT_EXISTS_STR){
            rand_vol = rand()%m_workloads_paths.size();
        }

        res.emplace_back(change_type,del_path, add_path,
                         rand_vol);
    }

    return std::move(res);
}

void AlgorithmDSManager::initializeChangesList(
        const std::string& changes_input_file_path, const int seed, const int num_changes_iters,
        const int changes_perc, const ChangeType change_type, const int num_runs) {
    m_changes_list = {};
    m_changes_list.reserve(num_changes_iters * num_runs);

    if(num_changes_iters == 0)
        return;
    std::ifstream changes_input_file(changes_input_file_path);
    if (!changes_input_file.is_open()) {
        changes_input_file.close();
        throw std::runtime_error("error opening changes input file");
    }

    srand(seed);

    int num_files_in_system = m_number_of_files_for_clustering;
    const int num_max_changes_total_for_run = std::round((changes_perc * num_files_in_system) / 100);

    for(int num_run = 1; num_run <= num_runs; ++num_run) {
        const int num_max_changes_iter = std::round(num_max_changes_total_for_run / num_changes_iters);
        int left_changes = num_max_changes_total_for_run % num_changes_iters;
        for (int i = 0; i < num_changes_iters; ++i) {
            int num_changes_for_iter = num_max_changes_iter;
            if (left_changes > 0) {
                num_changes_for_iter++;
                left_changes--;
            }

            std::string change_line;
            std::vector<std::string> changes_raw_lines = {};
            changes_raw_lines.reserve(num_changes_for_iter);
            while (changes_raw_lines.size() < num_changes_for_iter && std::getline(changes_input_file, change_line)) {
                if (!change_line.empty() && change_line[change_line.size() - 1] == '\r') {
                    change_line.erase(change_line.size() - 1); // need dos2unix. this is a fix for removing '\r'
                }
                changes_raw_lines.emplace_back(change_line);
            }

            std::vector<AlgorithmDSManager::ChangeInfo> change_bulk = translateChangeLinesToChangeInfos(changes_raw_lines, change_type);

            m_changes_list.emplace_back(change_bulk);
        }
    }
}

void AlgorithmDSManager::removeFile(const std::string& input_file_to_rem) {
    const int index = m_input_file_to_file_index[input_file_to_rem];

    int i = 0;
    bool file_removed = false;
    for(const auto& workload_path: m_workloads_paths){
        auto iter = m_initial_mapping[workload_path].find(index);
        if(iter != m_initial_mapping[workload_path].cend()){
            if(m_initial_mapping[workload_path].erase(iter)!= m_initial_mapping[workload_path].cend()){
                file_removed = true;
                std::cout << "remove " << input_file_to_rem << " from volume:" << i << std::endl;
                break;
            }
        }
        ++i;
    }

//    if(!file_removed){
//        throw std::runtime_error("Could not find file in system to remove it: " + input_file_to_rem);
//    }

    m_removed_files.emplace(m_input_file_to_file_index[input_file_to_rem]);
}

void AlgorithmDSManager::addFile(const std::string& file_path_to_add, const std::string& input_file_to_add,
                                 const int vol_index) {
    std::ifstream fileStream(file_path_to_add.c_str(), std::ifstream::in);
    if (!fileStream.is_open()) {
        throw std::runtime_error("Failed to open change file=" + file_path_to_add);
    }

    std::unordered_map<uint64_t, uint64_t> block_local_sn_to_index = {};
    std::string content;
    std::vector<std::string> splitted_content;
    std::string fileLine = "";
    while (std::getline(fileStream, content)) {
        std::istringstream ss(content);
        std::string firstToken = Utility::getCommaToken(ss, true);
        if (firstToken == "F") {
            if(fileLine != ""){
                throw std::runtime_error("More than 1 file in given change file.Path: " + file_path_to_add);
            }

            fileLine = content;

            m_max_file_sn++;
            m_file_sn_to_algo_index[m_max_file_sn] = m_number_of_files_for_clustering;
            m_file_index_to_sn[m_number_of_files_for_clustering] = m_max_file_sn;
            m_file_index_to_input_file[m_number_of_files_for_clustering] = input_file_to_add;
            m_input_file_to_file_index[input_file_to_add] = m_number_of_files_for_clustering;
            m_file_to_size[m_number_of_files_for_clustering] = 0;

            m_initial_mapping[m_workloads_paths[vol_index]].emplace(m_number_of_files_for_clustering);
            std::cout << "add " << input_file_to_add << " to volume:" << vol_index << std::endl;

        }
        if (firstToken == "B") {
            splitted_content = Utility::splitString(content, ",");
            int blockLocalSn = std::stoi(splitted_content[1]);
            std::string blockFp = splitted_content[2].substr(1);

            if(m_fingerprint_to_index.find(blockFp) == m_fingerprint_to_index.cend()){
                // new block
                m_fingerprint_to_index[blockFp] = m_fingerprint_to_size.size();
                m_fingerprint_to_size[m_fingerprint_to_size.size()] = 0;

                block_local_sn_to_index[blockLocalSn] = m_fingerprint_to_size.size();

                m_max_block_sn++;
                m_number_of_fingerprints_for_clustering++;
            }

            int blockIndex = m_fingerprint_to_index[blockFp];
            block_local_sn_to_index[blockLocalSn] = blockIndex;
        }
    }
    fileStream.close();


    std::istringstream ss(fileLine);

    // skip file info
    static const int FILE_INFO_LAST_INDEX = 4;
    for(int i=0; i<FILE_INFO_LAST_INDEX; ++i){
        std::string a = Utility::getCommaToken(ss, true);
    }

    //add blocks
    std::string file_num_blocks = Utility::getCommaToken(ss, true);
    m_appearances_matrix.emplace_back(m_fingerprint_to_index.size(), false);

    const uint64_t num_blocks = std::stoull(file_num_blocks);
    for(uint64_t i=0; i < num_blocks; ++i)
    {
        const std::string& block_sn = Utility::getCommaToken(ss, true);
        const std::string& block_size = (i==num_blocks-1) ? Utility::getLine(ss) : Utility::getCommaToken(ss, true);

        double block_size_double = std::stod(block_size);
        unsigned long block_size_long = static_cast<unsigned long>(block_size_double > 0 ? block_size_double: 4096.0); //should not happen. if it does we treat it as 4KB block
        const int fp_index = block_local_sn_to_index[std::stoull(block_sn)];
        m_fingerprint_to_size[fp_index] = block_size_long;
        m_file_to_size[m_number_of_files_for_clustering] += block_size_long;

        m_appearances_matrix.back()[fp_index] = true;
    }

    m_number_of_files_for_clustering++;
}

std::vector<int> AlgorithmDSManager::getRemovedFilesInUpdateIter(const int change_iter) {
    std::vector<int> res;
    if(change_iter == 0){
        return std::move(res);
    }
    for (const auto& change: m_changes_list[change_iter-1]){
        if(change.file_path_to_remove != change.NOT_EXISTS_STR){
            res.emplace_back(m_input_file_to_file_index[change.input_file_to_remove]);
        }
    }

    return std::move(res);
}

std::string AlgorithmDSManager::getChangeFilePath() const {
    return std::move(m_change_input_file_path);
}

std::map<std::string, std::set<int>> AlgorithmDSManager::getAddedFilesInUpdateIter(const int change_iter) {
    std::map<std::string, std::set<int>> res;
    if(change_iter == 0){
        return std::move(res);
    }

    for (const auto& change: m_changes_list[change_iter-1]){
        if(change.file_path_to_add != change.NOT_EXISTS_STR){
            //Currently that's oly supporting random insert (that given in changeInfo)

            if(res.find(m_workloads_paths[change.volume_index_to_add_file_to]) == res.cend()){
                res[m_workloads_paths[change.volume_index_to_add_file_to]] = {};
            }

            res[m_workloads_paths[change.volume_index_to_add_file_to]].emplace(m_input_file_to_file_index[change.input_file_to_add]);
        }
    }

    return std::move(res);
}

int AlgorithmDSManager::getHostByPath(const std::string& path){
    // assumes file name is in the following format: U241_H247_IF5484_G1_TS31_10_2009_10_16
    std::string fileName = Utility::getBaseName(path);
    const int host_of_file = std::stoi(Utility::splitString(
            Utility::splitString(fileName, "_H")[1],"_IF")[0]);
    return host_of_file;
}

int AlgorithmDSManager::findFileAndGetVolIndex(const int file_index, const std::shared_ptr<std::map<std::string, std::set<int>>>& backup_hints) {
    if(backup_hints == nullptr)
    {
        int i = 0;
        for(const auto& workload_files: m_initial_mapping){
            if(workload_files.second.find(file_index) != workload_files.second.cend()){
                return i;
            }
            ++i;
        }

        throw std::runtime_error("Not found file index in system: "+std::to_string(file_index));
    }else{
        int i = 0;
        for(const auto& workload_files: *backup_hints){
            if(workload_files.second.find(file_index) != workload_files.second.cend()){
                return i;
            }
            ++i;
        }

        throw std::runtime_error("Not found file index in system: "+std::to_string(file_index));
    }
}

void AlgorithmDSManager::chooseVolToInsertAndUpdateInfo(ChangeInfo& change, const std::shared_ptr<std::map<std::string, std::set<int>>>& backup_hints) {
    if(change.type != BACKUP_INSERT || change.file_path_to_add == change.NOT_EXISTS_STR)
        return;

    const int host_of_file = getHostByPath(change.file_path_to_add);

    int desired_new_vol = -1;
    for(auto iter = m_host_to_file_ordered[host_of_file].rbegin(); iter != m_host_to_file_ordered[host_of_file].rend(); ++iter){
        const std::string newest_input_file = *iter;
        if(m_input_file_to_file_index.find(newest_input_file) == m_input_file_to_file_index.cend()){
            continue;
        }

        const int newest_file_index = m_input_file_to_file_index[newest_input_file];
        desired_new_vol = findFileAndGetVolIndex(newest_file_index, backup_hints);
        break;
    }

    if(desired_new_vol == -1){
        throw std::runtime_error("Did not find desired new vol for file in backup mode: " + change.file_path_to_add);
    }

    change.volume_index_to_add_file_to = desired_new_vol;
}

void AlgorithmDSManager::applyUpdatesIter(const int change_iter, const std::shared_ptr<std::map<std::string, std::set<int>>>& backup_hints) {
    if(change_iter > m_changes_list.size()){
        throw std::runtime_error("Got request to update system with change iter=" +
        std::to_string(change_iter) + " which is bigger than actual num "
                                      "of changes iter=" + std::to_string(m_changes_list.size()));
    }

    // we only consider addition since we only add info
    for (auto& change: m_changes_list[change_iter - 1]){
        if(change.file_path_to_add != change.NOT_EXISTS_STR){
            chooseVolToInsertAndUpdateInfo(change, backup_hints);
            addFile(change.file_path_to_add, change.input_file_to_add, change.volume_index_to_add_file_to);
            const int host_of_file = getHostByPath(change.file_path_to_add);
            m_host_to_file_ordered[host_of_file].emplace_back(change.input_file_to_add);
        }
        if(change.file_path_to_remove != change.NOT_EXISTS_STR){
            removeFile(change.input_file_to_remove);
        }
    }

    // expand appearances matrix to have cells for the new blocks
    for(int i=0; i < m_number_of_files_for_clustering; ++i){
        for(int b=m_appearances_matrix[i].size(); b < m_number_of_fingerprints_for_clustering;++b){
            m_appearances_matrix[i].emplace_back(false);
        }
    }

    // recalculate m_initial_system_size_with_deduplication and m_optimal_system_size_with_deduplication after new blocks
    m_initial_system_size_with_deduplication = 0;
    m_optimal_system_size_with_deduplication = 0;

    std::unordered_set<int> optimal_volumes;
    for(const auto& workload_files: m_initial_mapping){
        std::unordered_set<int> current_volume;
        for(const auto& file: workload_files.second){
            for(int block_index = 0; block_index< m_number_of_fingerprints_for_clustering; ++block_index){
                if(m_appearances_matrix[file][block_index]){
                    if (current_volume.insert(block_index).second)
                        m_initial_system_size_with_deduplication += m_fingerprint_to_size[block_index];

                    if (optimal_volumes.insert(block_index).second)
                        m_optimal_system_size_with_deduplication += m_fingerprint_to_size[block_index];
                }
            }
        }
    }

    initializeDissimilaritiesMatrix();
}

std::vector<int> AlgorithmDSManager::initializeAndSelectFingerprints() {
    // current_blocks for not using same fp twice
    std::unordered_set<int> current_blocks;
    std::vector<std::ifstream> workloads_streams = getWorkloadsStreams();
    std::priority_queue<std::pair<std::string, int>> minhash_fingerprints; // priority queue of fps' <fp, sn>

    for (int workload_index = 0; workload_index < workloads_streams.size(); ++workload_index) {
        updateFPWithWorkload(workloads_streams[workload_index], workload_index, m_number_of_files_for_clustering,
                             current_blocks, m_requested_number_of_fingerprints, minhash_fingerprints);
    }

    std::map<int, std::string> sn_to_fp = {};

    std::vector<int> fingerprints_for_clustering_ordered_by_SN = getFPForClusteringOrderedBySN(minhash_fingerprints,
                                                                                               m_fingerprint_to_index);
    m_number_of_fingerprints_for_clustering = fingerprints_for_clustering_ordered_by_SN.size();

    closeWorkloadsStreams(workloads_streams);

    return std::move(fingerprints_for_clustering_ordered_by_SN);
}

void AlgorithmDSManager::clearAppearancesMatrix(){
    //create the appearances_matrix with initial values false
    m_appearances_matrix = std::vector<std::vector<bool>>();
    m_appearances_matrix.reserve(m_number_of_files_for_clustering);
    for (int i = 0; i < m_number_of_files_for_clustering; ++i)
        m_appearances_matrix.emplace_back(m_number_of_fingerprints_for_clustering, false);
}

void AlgorithmDSManager::updateAppearancesMatWithWorkloadFileLine(
        const std::vector<int>& fingerprints_for_clustering_ordered_by_SN,
        const std::vector<std::string>& splitted_line_content,
        const int file_index,
        std::unordered_set<int>& current_volume,
        std::unordered_set<int>& optimal_volumes,
        const bool is_only_appearances_mat){
    const int number_of_blocks_in_file_line = Utility::getNumOfBlockInFile(splitted_line_content);

    const std::string input_file = Utility::getInputFileOfFile(splitted_line_content);
    m_input_file_to_file_index[input_file] = file_index;
    m_file_index_to_input_file[file_index] = input_file;

    //for each fp for this file if the fp was selected, add to relevant data structures
    for (int block_index = 0; block_index < number_of_blocks_in_file_line; ++block_index) {
        const int block_sn = Utility::getBlockSNInFileByIndex(splitted_line_content, block_index);
        const int fingerprint_index = Utility::binarySearch(
                fingerprints_for_clustering_ordered_by_SN,
                0,
                fingerprints_for_clustering_ordered_by_SN.size() - 1,
                block_sn);

        static constexpr int FINGERPRINT_NOT_FOUND = -1;
        if (fingerprint_index == FINGERPRINT_NOT_FOUND)
            continue;

        const int block_size= Utility::getBlockSizeInFileByIndex(splitted_line_content, block_index);
        m_appearances_matrix[file_index][fingerprint_index] = true;
        m_file_to_size[file_index] += block_size;
        m_fingerprint_to_size[fingerprint_index] = block_size;
        m_max_block_sn=std::max(m_max_block_sn, block_sn);

        if(!is_only_appearances_mat){
            //new fp for specific volume
            if (current_volume.insert(fingerprint_index).second)
                m_initial_system_size_with_deduplication += block_size;

            //new fp for entire system
            if (optimal_volumes.insert(fingerprint_index).second)
                m_optimal_system_size_with_deduplication += block_size;
        }
    }
}

void AlgorithmDSManager::initializeAppearancesMatrix(
        const std::vector<int>& fingerprints_for_clustering_ordered_by_SN,
        bool load_balance, const bool is_only_appearances_mat) {

    clearAppearancesMatrix();

    //initialize fields
    m_fingerprint_to_size = {};
    m_file_to_size = {};
    m_file_index_to_input_file = {};
    m_input_file_to_file_index = {};
    m_initial_system_size_with_deduplication = 0;
    m_optimal_system_size_with_deduplication = 0;

    std::unordered_set<int> optimal_volumes_size;//fps when all fps in same volume
    int file_index = 0;
    std::vector<std::ifstream> workloads_streams = getWorkloadsStreams();
    for (auto & workloads_stream : workloads_streams) {
        //fps for specific initial volume
        std::unordered_set<int> current_volume;

        std::string workload_file_line;
        while (std::getline(workloads_stream, workload_file_line)) {
            if(workload_file_line.length() == 0){
                continue;
            }
            const std::vector<std::string> splitted_line_content = Utility::splitString(workload_file_line, ',');
            if(!Utility::isWorkloadFileLine(splitted_line_content))
                continue;

            m_file_to_size[file_index] = 0;

            //if we got here it is file line
            updateAppearancesMatWithWorkloadFileLine(fingerprints_for_clustering_ordered_by_SN, splitted_line_content,
                                                     file_index, current_volume, optimal_volumes_size, is_only_appearances_mat);

            file_index++;
        }
    }

    closeWorkloadsStreams(workloads_streams);
}

void AlgorithmDSManager::initializeDissimilaritiesMatrix() {
    std::unordered_map<int, int> initial_clusters = getInitialFileToClusterMapping();

    //create the dissimilarity_matrix, do it row by row
    m_dissimilarities_matrix = std::vector<std::vector<DissimilarityCell>>();
    m_dissimilarities_matrix.reserve(m_number_of_files_for_clustering);

    for (int i = 0; i < m_number_of_files_for_clustering; ++i) {
        m_dissimilarities_matrix.emplace_back(m_number_of_files_for_clustering);

        // fill the diagonal - irrelevant
        m_dissimilarities_matrix[i][i] = {0, {std::make_pair(initial_clusters[i], m_file_to_size[i])}};

        for (int j = i + 1; j < m_number_of_files_for_clustering; ++j) {
            const float distance = Utility::getJaccardDistance(m_appearances_matrix[i],m_appearances_matrix[j]);

            std::map<int,long long int > size_in_orig_clusters = {std::make_pair(initial_clusters[i], m_file_to_size[i])};
            if(initial_clusters[i] == initial_clusters[j])
            {
                size_in_orig_clusters[initial_clusters[j]] += m_file_to_size[j];
            }
            else
            {
                size_in_orig_clusters[initial_clusters[j]] = m_file_to_size[j];

            }

            m_dissimilarities_matrix[i][j] = {distance, size_in_orig_clusters};
        }
    }

    resetDMBottomTriangle();

    for(const auto& removed_file: m_removed_files){
        deactivateClusterInDissimilarityMat(removed_file);
    }

    std::cout<< "Finished initializeDissimilaritiesMatrix" <<std::endl;
}

std::unordered_map<int,int> AlgorithmDSManager::getFileToClusterMapping(const std::map<std::string, std::set<int>>& clustering){
    std::unordered_map<int, int> initial_clusters;

    int i =0;
    for(const auto& cluster_name_set : clustering){
        for(const int file_index : cluster_name_set.second)
            initial_clusters[file_index] = i;
        ++i;
    }

    return std::move(initial_clusters);
}

std::unordered_map<int,int> AlgorithmDSManager::getInitialFileToClusterMapping(){
    return getFileToClusterMapping(m_initial_mapping);
}

std::vector<std::vector<AlgorithmDSManager::ChangeInfo>> AlgorithmDSManager::getChangesList(){
    return m_changes_list;
}

const std::string& AlgorithmDSManager::getCurrentHostName(){
    return m_host_name;
}

void AlgorithmDSManager::resetDMDiagonal(){
    std::unordered_map<int, int> initial_clusters = getInitialFileToClusterMapping();

    for (int i = 0; i < m_dissimilarities_matrix.size(); ++i) {
        // fill the diagonal - irrelevant
        m_dissimilarities_matrix[i][i] = {0, {std::make_pair(initial_clusters[i], m_file_to_size[i])}};
    }
}

void AlgorithmDSManager::resetDMBottomTriangle(){
    // fill the bottom half
    for (int i = 0; i < m_dissimilarities_matrix.size(); ++i) {
        for (int j = 0; j < i; ++j) {
            m_dissimilarities_matrix[i][j] = m_dissimilarities_matrix[j][i];
        }
    }
}

const AlgorithmDSManager::DissimilarityCell& AlgorithmDSManager::getDissimilarityCell(const int cluster1, const int cluster2) const{

    const int column=std::min(cluster1,cluster2);
    const int row=std::max(cluster1,cluster2);

    return m_dissimilarities_matrix[row][column];
}

void AlgorithmDSManager::setDissimilarityCell(const int cluster1, const int cluster2,
                                              const AlgorithmDSManager::DissimilarityCell& dissimilarity_cell){
    const int column=std::min(cluster1,cluster2);
    const int row=std::max(cluster1,cluster2);

    m_dissimilarities_matrix[row][column] = dissimilarity_cell;
}

void AlgorithmDSManager::deactivateClusterInDissimilarityMat(const int cluster_index){
    for (int i = 0; i < m_dissimilarities_matrix.size(); ++i)
        setDissimilarityCell(cluster_index, i, {DBL_MAX, {}});
}

int AlgorithmDSManager::getNumberOfFilesForClustering() const{
    return m_number_of_files_for_clustering;
}

int AlgorithmDSManager::getNumberOfRemovedFiles() const{
    return m_removed_files.size();
}

bool AlgorithmDSManager::isFileRemoved(const int file_index) const{
    return m_removed_files.find(file_index) != m_removed_files.cend();
}

int AlgorithmDSManager::getNumberOfFingerprintsForClustering() const{
    return m_number_of_fingerprints_for_clustering;
}

int AlgorithmDSManager::getFingerprintSize(const int fp_index) const{
    return m_fingerprint_to_size.at(fp_index);
}

int AlgorithmDSManager::getFileSize(const int file_index) const{
    return m_file_to_size.at(file_index);
}

bool AlgorithmDSManager::isFileHasFingerprint(const int file_index, const int fp_index) const{
    return m_appearances_matrix[file_index][fp_index];
}

int AlgorithmDSManager::getFileIndex(const int file_sn) const{
    return m_file_sn_to_algo_index.at(file_sn);
}

int AlgorithmDSManager::getFileSN(const int file_index) const{
    return m_file_index_to_sn.at(file_index);
}

std::vector<std::set<int>> AlgorithmDSManager::getInitialClusteringAsVector() const{
    std::vector<std::set<int>> result;
    result.reserve(m_initial_mapping.size());
    for(const auto& mapping: m_initial_mapping)
        result.emplace_back(mapping.second);

    return result;
}

static std::map<std::string, std::set<int>> deepCopyClusterMap(const std::map<std::string, std::set<int>>& original) {
    auto newMap = std::map<std::string, std::set<int>>(); // Create a new shared_ptr with an empty map

    for (const auto& pair : original) {
        newMap[pair.first] = pair.second;
    }

    return newMap;
}

std::map<std::string, std::set<int>> AlgorithmDSManager::getInitialClusteringCopy() const{
    return std::move(deepCopyClusterMap(m_initial_mapping));
}

const std::map<std::string, std::set<int>>& AlgorithmDSManager::getInitialClustering() const{
    return m_initial_mapping;
}

const std::string& AlgorithmDSManager::getWorkloadFullPath(const int workload_index) const{
    return m_workloads_paths[workload_index];
}

int AlgorithmDSManager::getNumOfWorkloads() const{
    return m_workloads_paths.size();
}

long long int AlgorithmDSManager::getInitialSystemSize() const{
    return m_initial_system_size_with_deduplication;
}

long long int AlgorithmDSManager::getOptimalSystemSize() const{
    return m_optimal_system_size_with_deduplication;
}

const std::map<int, int>& AlgorithmDSManager::getBlockToSizeMap() const {
    return m_fingerprint_to_size;
}

std::map<std::string, double> AlgorithmDSManager::getArrangedLbSizes(const std::vector<double> &lb_sizes) const {
    std::map<std::string, double> arranged_result;
    for(int i=0; i< lb_sizes.size(); ++i)
        arranged_result[getWorkloadFullPath(i)] = lb_sizes[i];

    return std::move(arranged_result);
}

std::vector<std::string> AlgorithmDSManager::getWorkloadsFullPaths(const std::vector<std::string> &paths) {
    std::vector<std::string> full_paths;
    full_paths.reserve(paths.size());
    for(const std::string& path : paths)
        full_paths.emplace_back(Utility::getFullPath(path));

    std::sort(full_paths.begin(), full_paths.end());
    return std::move(full_paths);
}

void AlgorithmDSManager::
applyPlan(const std::map<std::string, std::set<int>> &clustering, const long long int system_size) {
    m_initial_mapping = clustering;
    m_initial_system_size_with_deduplication = system_size;

    initializeDissimilaritiesMatrix();
}
