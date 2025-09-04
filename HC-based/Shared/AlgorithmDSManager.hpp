#pragma once

#include "Utility.hpp"

#include <fstream>
#include <queue>
#include <unordered_map>
#include <memory>
#include <set>
#include <map>
#include <string>
#include <vector>
#include <random>

class AlgorithmDSManager final {
public:
    /**
     * DissimilarityCell struct - a cell value for each cell in dissimilarity matrix
     */
    struct DissimilarityCell {
        DissimilarityCell() : jaccard_distance(0), size_from_origin_clusters()
        {
        }

        DissimilarityCell(const double jaccard_distance, std::map<int, long long int> size_from_origin_clusters) :
        jaccard_distance(jaccard_distance),
        size_from_origin_clusters(std::move(size_from_origin_clusters))
        {
        }

        double jaccard_distance;
        std::map<int, long long int> size_from_origin_clusters;
    };


    enum ChangeType{
        RANDOM_INSERT,
        BACKUP_INSERT,

    };

    /**
     * ChangeInfo struct - cotains an info about external change in dynamic system
     */
    struct ChangeInfo {
        static const int NOT_RELEVANT = -1;
        static constexpr char* const NOT_EXISTS_STR = "";
        ChangeInfo(const ChangeType type,const std::string& file_path_to_remove, const std::string& file_path_to_add,
                   const int volume_index_to_add_file_to) :
                type(type),
                file_path_to_add(file_path_to_add),
                file_path_to_remove(file_path_to_remove),
                volume_index_to_add_file_to(volume_index_to_add_file_to)
        {
            // assumes file name is in the following format: U241_H247_IF5484_G1_TS31_10_2009_10_16

            std::string fileName = Utility::getBaseName(file_path_to_remove);
            input_file_to_remove = fileName != "" ? Utility::splitString(Utility::splitString(fileName, "_IF")[1],
                                                        "_TS")[0]: NOT_EXISTS_STR;
            fileName = Utility::getBaseName(file_path_to_add);
            input_file_to_add = fileName != "" ? Utility::splitString(Utility::splitString(fileName, "_IF")[1],
                                                     "_TS")[0] : NOT_EXISTS_STR;
        }

        ChangeType type;
        std::string input_file_to_remove;
        std::string input_file_to_add;
        std::string file_path_to_add;
        std::string file_path_to_remove;
        int volume_index_to_add_file_to;
    };

public:
    /**
     * @param workloads_paths - paths' vector to the workloads
     * @param requested_number_of_fingerprints - how many fps to use
     * @param load_balance - whether to use load balance or not
     */
    explicit AlgorithmDSManager(const std::vector<std::string>& workloads_paths,
                                const int requested_number_of_fingerprints,
                                const int num_changes_iterations,
                                const int change_seed,
                                const int changes_perc,
                                const std::string& changes_input_file,
                                const std::string& index_path,
                                const bool load_balance,
                                const ChangeType change_type,
                                const int num_runs = 1,
                                const bool is_only_appearances_mat = false);
    AlgorithmDSManager& operator=(const AlgorithmDSManager&) = delete;
    ~AlgorithmDSManager() = default;

    /**
     * @return current host name
     */
    const std::string& getCurrentHostName();

    /**
     *
     * resets the dissimilarities matrix's bottom triangle by copying corresponding values from its upper triangle
     */
    void resetDMBottomTriangle();

    /**
     *
     * resets the dissimilarities matrix's diagonal
     */
    void resetDMDiagonal();

    /**
     *
     * @param cluster1 - a cluster index
     * @param cluster2 - a cluster index
     * @return the dissimilarity cell between cluster1 and cluster2 (as written in the dissimilarities matrix)
     */
    const DissimilarityCell& getDissimilarityCell(const int cluster1, const int cluster2) const;

    /**
     *
     * @param cluster1 - a cluster index
     * @param cluster2 - a cluster index
     * @param dissimilarity_cell - a DissimilarityCell object
     * set the relevant cluster1,cluster2 dissimilarities matrix's cell to be the given dissimilarity_cell
     */
    void setDissimilarityCell(const int cluster1, const int cluster2, const DissimilarityCell& dissimilarity_cell);

    /**
     * deactivate the given cluster in the dissimilarity matrix
     * @param cluster_index - a cluster index
     */
    void deactivateClusterInDissimilarityMat(const int cluster_index);

    /**
     *
     * @return number of fingerprints for clustering
     */
    int getNumberOfFingerprintsForClustering() const;

    /**
     *
     * @return number of files for clustering
     */
    int getNumberOfFilesForClustering() const;

    /**
     *
     * @return the number of removed files so far
     */
    int getNumberOfRemovedFiles() const;

    /**
     *
     * @return whether the given file_index had been removed
     */
    bool isFileRemoved(const int file_index) const;

    /**
     * @param fp_index - a fingerprint index
     * @return the size of the corresponding fingerprint as given by fp_index
     */
    int getFingerprintSize(const int fp_index) const;


    /**
     * @param file_index - a file index
     * @return the size of the corresponding file as given by file_index
     */
    int getFileSize(const int file_index) const;

    /**
     * @param file_index - a file index
     * @param fp_index - a fingerprint index
     * @return whether file(file_index) contains fingerprint (fp_index). use the appearances' matrix
     */
    bool isFileHasFingerprint(const int file_index, const int fp_index) const;

    /**
     * @param file_index - a file index
     * @return the SN of the give file index
     */
    int getFileSN(const int file_index) const;

    /**
     * @param file_sn - a file sn
     * @return the algo index of the give file sn
     */
    int getFileIndex(const int file_sn) const;

    std::vector<std::set<int>> getInitialClusteringAsVector() const;

    /**
     * @return the initial clustering mapping
     */
    const std::map<std::string, std::set<int>>& getInitialClustering() const;
    std::map<std::string, std::set<int>> getInitialClusteringCopy() const;

    /**
     * @param workload_index - a workload index
     * @return the full path of the corresponding workload
     */
    const std::string& getWorkloadFullPath(const int workload_index) const;

    /**
     * @return num of workloads
     */
    int getNumOfWorkloads() const;

    /**
     * @return the initial system size
     */
    long long int getInitialSystemSize() const;

    /**
     * @return the optimal system size
     */
    long long int getOptimalSystemSize() const;

    /**
     * get the appearances matrix
     */
    const std::vector<std::vector<bool>>& getAppearancesMatrix() const {return m_appearances_matrix;}

    /**
     * get block to size mapping
     */
    const std::map<int, int>& getBlockToSizeMap() const;

    /**
     * @param lb_sizes - a lb sizes vector
     * @return map where index is workload name and value is corresponding lb size
     */
    std::map<std::string, double> getArrangedLbSizes(const std::vector<double>& lb_sizes) const;

    void applyPlan(const std::map<std::string, std::set<int>>& clustering, const long long int system_size);

    /**
     *
     * @return - the initial state files' algorithm sn to cluster mapping
     */
    std::unordered_map<int,int> getInitialFileToClusterMapping();

    /**
     *
     * @return - list of changes list in the order of the change iter
     */
    std::vector<std::vector<ChangeInfo>> getChangesList();

    /**
     * @param change_iter - a change iter index
     *
     * apply the chages in the given change iter index to the system
     */
     void applyUpdatesIter(const int change_iter, const std::shared_ptr<std::map<std::string, std::set<int>>>& backup_hints= nullptr);

    /**
    * @param change - a change to conduct
    *
    * update the given change with the correct volume to insert to
    */
    void chooseVolToInsertAndUpdateInfo(ChangeInfo& change, const std::shared_ptr<std::map<std::string, std::set<int>>>& backup_hints);

    int findFileAndGetVolIndex(const int file_index, const std::shared_ptr<std::map<std::string, std::set<int>>>& backup_hints);

    int getHostByPath(const std::string& path);

    /**
     * @param change_iter - a change iter index
     * @return - map of volume to the set of file indices that was inserted to it at the given change iter
     */
     std::map<std::string, std::set<int>>  getAddedFilesInUpdateIter(const int change_iter);

    /**
     * @param change_iter - a change iter index
     * @return - list of the removed files indices at a given change iter
     */
    std::vector<int> getRemovedFilesInUpdateIter(const int change_iter);

    /**
     * @return - the path to the change path the algorithm working with
     */
    std::string getChangeFilePath() const;

private:
    /**
     * initialize the Fingerprint related Data structures and returns the selected fps ordered by their SN
     */
    std::vector<int> initializeAndSelectFingerprints();

    /**
     * @param changes_input_file_path - a change input path path
     * @param seed - an initial seed to be used in the randomized operations
     * @param num_changes_iters - num of changes iterations
     * @param changes_perc - num of changes as percentage from the total initial system num of files
     * @param change_type - the type of desired change
     *
     * initialize the changes list for all the changes iters
     */
    void initializeChangesList(const std::string& changes_input_file_path,
                               const int seed, const int num_changes_iters,
                               const int changes_perc, ChangeType change_type,
                               const int num_runs);

    void initFileIndexInfo(const std::string& path);

    /**
     * @param input_file_to_rem - input file name to be removed from system
     * the function removes it from the system or throws std::runtime_error if not finding it
     */
     void removeFile(const std::string& input_file_to_rem);

    /**
     * @param file_path_to_add - path to the file we will add
     * @param input_file_to_add - input file name to be added to system
     * @param vol_index - vol index to add file to
     * the function add the given file to system and place it in the given vol_index
     */
     void addFile(const std::string& file_path_to_add, const std::string& input_file_to_add, const int vol_index);

    /**
     * @param changes_lines - vector of changes line
     * @param change_type - desired change type
     * @return list of the translated changes for the given changes line
     */
    std::vector<ChangeInfo> translateChangeLinesToChangeInfos(const std::vector<std::string>& changes_lines,
                                                              const ChangeType change_type);
    /**
     * initialize the appearances matrix with the initial+optimal system size with deduplication fields
     * @param fingerprints_for_clustering_ordered_by_SN - fingerprints for clustering ordered by their SN
     * @param load_balance - whether to calculate load balance stats
     * @return - pair of initial system size and optimal system size
     */
    void initializeAppearancesMatrix(const std::vector<int>& fingerprints_for_clustering_ordered_by_SN,
                                     bool load_balance, const bool is_only_appearances_mat);

    /**
     * initialize the final dissimilarity matrix
     */
    void initializeDissimilaritiesMatrix();

    /**
     * clears the appearances matrix
     */
    void clearAppearancesMatrix();

    /**
     * returns vector of workloads' streams (files located at m_workloads_paths)
     */
    std::vector<std::ifstream> getWorkloadsStreams() const;

    /**
     * inner function of initializeAndSelectFingerprints
     * updates the given current_blocks,current_algo_file_index  and minhash_fingerprints
     * with the given workload file
     * @param workload_file - a workload's file
     * @param workload_index - index of the workload's file
     * @param current_algo_file_index - the current algo file index
     * @param current_blocks - set of blocks we already handled
     * @param requested_num_fps - threshold of number of fingerprints we want to collect
     * @param minhash_fingerprints - priority queue of fingerprints
     */
    void updateFPWithWorkload(std::ifstream& workload_file, const int workload_index,
                              int& current_algo_file_index,
                              std::unordered_set<int>& current_blocks,
                              const int requested_num_fps,
                              std::priority_queue<std::pair<std::string, int>>& minhash_fingerprints);

    /**
     * inner function of initializeAppearancesMatrix
     * updates the given initial_volumes_size and optimal_volumes_size
     * also updating the class fields: m_appearances_matrix, m_fingerprint_to_size
     * and m_initial_system_size_with_deduplication
     * @param fingerprints_for_clustering_ordered_by_SN - selected fingerprints ordered by the SN (ascending)
     * @param splitted_line_content -  a workload line representing a file splitted by ','
     * @param file_index - the current algo file index
     * @param current_volume - set of blocks we already handled in current volume
     * @param optimal_volumes - set of blocks we already handled in all volumes
     */
    void updateAppearancesMatWithWorkloadFileLine(const std::vector<int>& fingerprints_for_clustering_ordered_by_SN,
                                                  const std::vector<std::string>& splitted_line_content,
                                                  const int file_index,
                                                  std::unordered_set<int>& current_volume,
                                                  std::unordered_set<int>& optimal_volumes,
                                                  const bool is_only_appearances_mat);
private:
    /**
     *
     * @return - the initial state files' algorithm sn to cluster mapping
     */
    static std::unordered_map<int,int> getFileToClusterMapping(const std::map<std::string, std::set<int>>& clustering);

    /**
     * close the workloads' streams as given in workloads_streams
     * @param workloads_streams - workloads' streams vector
     */
    static void closeWorkloadsStreams(std::vector<std::ifstream>& workloads_streams);

    /**
     * inner function of initializeAndSelectFingerprints
     * updates the given minhash_fingerprints and current_blocks with the given workload_line
     * @param splitted_line_content - a workload line representing a block splitted by ','
     * @param current_blocks - set of blocks we already handled
     * @param requested_num_fps - threshold of number of fingerprints we want to collect
     * @param minhash_fingerprints - priority queue of fingerprints
     */
    static void updateFPWithWorkloadBlockLine(const std::vector<std::string>& splitted_line_content,
                                              std::unordered_set<int>& current_blocks, const int requested_num_fps,
                                              std::priority_queue<std::pair<std::string, int>>& minhash_fingerprints);

    /**
     * inner function of initializeAndSelectFingerprints
     * @param minhash_fingerprints - priority queue of selected fingerprints
     * @return selected fingerprints ordered by the SN (ascending)
     */
    static std::vector<int> getFPForClusteringOrderedBySN(
            std::priority_queue<std::pair<std::string, int>>& minhash_fingerprints,
            std::map<std::string, int>& fp_to_index);

    /**
     * @param paths - paths of the workloads as given by command line
     * @return vector of all full paths
     */
    static std::vector<std::string> getWorkloadsFullPaths(const std::vector<std::string>& paths);

    // m_dissimilarities_matrix - the dissimilarities' matrix
    // m_appearances_matrix - the appearances' matrix -> is file x contains fp y
    // m_fingerprint_to_size - fingerprint's algo index to size mapping
    // m_initial_mapping - initial system's volume to files set (algo indices) mapping
    // m_workloads_paths - workload paths vector
    // m_file_index_to_sn - file's algo index to file's sn mapping
    // m_requested_number_of_fingerprints - number of fingerprints to use, -1 for 'all'
    // m_number_of_files_for_clustering - number of fingerprints to use, -1 for 'all'
    // m_number_of_fingerprints_for_clustering - actual number of fingerprints
    // m_number_of_fingerprints_for_clustering - actual number of fingerprints
    // m_initial_system_size_with_deduplication - system initial size with deduplication
    // m_optimal_system_size_with_deduplication - system optimal size with deduplication
    // m_host_name - name of the current host
    // m_changes_list - holds the changes list in the form of ChangeInfo objects vector
    // m_removed_files - holds a set of all the removed file indices
    // m_max_block_sn - holds a maximum block sn
    // m_max_file_sn - holds a maximum block sn
    // m_fingerprint_to_index - map of fp to algo index
    // m_change_input_file_path - path to change input file
    // m_file_index_to_input_file - algo index to input file name
    // m_input_file_to_file_index - input file to file index
    // m_host_to_file_ordered - map from host to all its files ordered (old to new)
private:
    std::vector<std::vector<DissimilarityCell>> m_dissimilarities_matrix;
    std::vector<std::vector<bool>> m_appearances_matrix;
    std::map<int,int> m_fingerprint_to_size;
    std::map<int,long long int> m_file_to_size;
    std::map<std::string, std::set<int>> m_initial_mapping;
    std::vector<std::string> m_workloads_paths;
    std::map<int, int> m_file_index_to_sn;
    std::map<int, int> m_file_sn_to_algo_index;

    std::map<int, std::string> m_file_index_to_input_file;
    std::map<std::string, int> m_input_file_to_file_index;
    const int m_requested_number_of_fingerprints;
    int m_number_of_files_for_clustering;
    int m_number_of_fingerprints_for_clustering;
    long long int m_initial_system_size_with_deduplication;
    long long int m_optimal_system_size_with_deduplication;
    const std::string m_host_name;
    std::vector<std::vector<ChangeInfo>> m_changes_list;
    std::set<int> m_removed_files;
    int m_max_block_sn;
    int m_max_file_sn;
    std::map<std::string ,int> m_fingerprint_to_index;
    std::string m_change_input_file_path;
    std::map<int, std::vector<std::string>> m_host_to_file_ordered;
};
