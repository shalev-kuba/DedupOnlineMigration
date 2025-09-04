#include "HierarchicalClustering.hpp"
#include "GreedySplit.hpp"

#include <algorithm>
#include <unordered_set>
#include <cmath>
#include <cfloat>
#include <random>
#include <iostream>
#include <unordered_map>
#include <utility>
#include <chrono>
#include <memory>

using namespace std;

vector<HierarchicalClustering::resultCompareFunc> HierarchicalClustering::ClusteringResult::sort_order = vector<HierarchicalClustering::resultCompareFunc>();

struct HierarchicalClustering::ClustersMergeOffer final{
public:
    ClustersMergeOffer(double weighted_dissimilarity, int cluster1, int cluster2):
            weighted_dissimilarity(weighted_dissimilarity), cluster1(cluster1), cluster2(cluster2)
    {
    }

public:
    double weighted_dissimilarity;
    int cluster1;
    int cluster2;
};

int HierarchicalClustering::ClusteringResult::compareDeletion(const shared_ptr <ClusteringResult> &res1,
                                                              const shared_ptr <ClusteringResult> &res2) {
    if(res1->cost_result->deletion_percentage > res2->cost_result->deletion_percentage)
        return 1;

    if(res1->cost_result->deletion_percentage == res2->cost_result->deletion_percentage)
        return 0;

    return -1;
}

int HierarchicalClustering::ClusteringResult::compareLbVaild(const shared_ptr <ClusteringResult> &res1,
                                                             const shared_ptr <ClusteringResult> &res2) {
    if(res1->cost_result->is_lb_valid > res2->cost_result->is_lb_valid)
        return 1;

    if(res1->cost_result->is_lb_valid == res2->cost_result->is_lb_valid)
        return 0;

    return -1;
}

int HierarchicalClustering::ClusteringResult::compareTrafficVaild(const shared_ptr <ClusteringResult> &res1,
                                                                  const shared_ptr <ClusteringResult> &res2) {
    if(res1->cost_result->is_traffic_valid > res2->cost_result->is_traffic_valid)
        return 1;

    if(res1->cost_result->is_traffic_valid == res2->cost_result->is_traffic_valid)
        return 0;

    return -1;
}

int HierarchicalClustering::ClusteringResult::compareTraffic(const shared_ptr <ClusteringResult> &res1,
                                                             const shared_ptr <ClusteringResult> &res2) {
    if(res1->cost_result->traffic_percentage < res2->cost_result->traffic_percentage)
        return 1;

    if(res1->cost_result->traffic_percentage == res2->cost_result->traffic_percentage)
        return 0;

    return -1;
}

int HierarchicalClustering::ClusteringResult::compareLbScore(const shared_ptr <ClusteringResult> &res1,
                                                             const shared_ptr <ClusteringResult> &res2) {
    if(res1->cost_result->lb_score > res2->cost_result->lb_score)
        return 1;

    if(res1->cost_result->lb_score == res2->cost_result->lb_score)
        return 0;

    return -1;
}

bool HierarchicalClustering::ClusteringResult::sortResultDesc(const shared_ptr<HierarchicalClustering::ClusteringResult> &res1,
                                                              const shared_ptr<HierarchicalClustering::ClusteringResult> &res2) {
    int current_result = 0, last_result = 0;
    for(const auto& compare_func : ClusteringResult::sort_order){
        last_result = current_result;
        current_result = compare_func(res1,res2);
        if(last_result == 0 && current_result > 0)
            return true;
        else if(last_result < 0)
            return false;

    }

    return false;
}


HierarchicalClustering::HierarchicalClustering(
        std::unique_ptr<AlgorithmDSManager>&  ds,
        const vector<double> &lb_sizes):
        m_lb_sizes(lb_sizes),
        m_current_system_size(0),
        m_ds(ds.release())
{
    initClusters();
}

void HierarchicalClustering::initClusters(){
    // if only need to reset, no new file was added to the system. otherwise we'll calc again
    if(m_clusters.size() == m_ds->getNumberOfFilesForClustering())
    {
        m_current_system_size = 0;
        for(const auto& node : m_clusters){
            if(m_ds->isFileRemoved(*node->getCurrentFiles().begin())){
                node->disableNode();
                m_ds->deactivateClusterInDissimilarityMat(*node->getCurrentFiles().begin());
                continue;
            }

            node->reset();
            m_current_system_size+= node->getSize();
        }

        return;
    }

    m_clusters = std::vector<std::unique_ptr<Node>>(m_ds->getNumberOfFilesForClustering());

    m_current_system_size = 0;
    // create a node for each file with its size
    for (int i = 0; i < m_clusters.size(); ++i) {
        const double cluster_size = getClusterSize({i});
        m_clusters[i] = std::make_unique<Node>(i, cluster_size);

        if(m_ds->isFileRemoved(i)){
            m_clusters[i]->disableNode();
            m_ds->deactivateClusterInDissimilarityMat(i);
            continue;
        }

        m_current_system_size += cluster_size;
    }
}

HierarchicalClustering::ClustersMergeOffer HierarchicalClustering::findBestMerge(
        const ClusteringParams& clustering_params, const int num_current_clusters,
        const int num_files_for_clustering){

    vector<ClustersMergeOffer> sorted_merge_offers;
    sorted_merge_offers.reserve(clustering_params.max_results_size);

    // iterate the lower triangular of the matrix and find our maximum values
    for (int i = 0; i < m_ds->getNumberOfFilesForClustering(); i++) {
        for (int j = 0; j < i; j++) {
            updateSortedMergeOffers(clustering_params, sorted_merge_offers, i, j, num_current_clusters,
                                    num_files_for_clustering);
        }
    }

    // in case we did not find anything, sanity check, not suppose to happen unless we use load balancing
    if (sorted_merge_offers.size() == 0)
        return {-1, -1, -1};

    // sort them in *ascending* order
    sort(sorted_merge_offers.begin(), sorted_merge_offers.end(), HierarchicalClustering::sortAsc);

    return randClustersMergeOfferFromVector(sorted_merge_offers, clustering_params.gap);
}

void HierarchicalClustering::updateSortedMergeOffers(
        const ClusteringParams& clustering_params,
        vector<ClustersMergeOffer>&  sorted_merge_offers,
        const int cluster1,
        const int cluster2,
        const int num_current_clusters,
        const int num_files_for_clustering)
{
    // if one of the given cluster is invalid there is no need to update the sorted dissimilarities values
    if(m_ds->getDissimilarityCell(cluster1, cluster2).jaccard_distance == DBL_MAX)
        return;

    const double current_clusters_distance = getDistanceBetweenClusters(clustering_params, cluster1, cluster2);

    const double current_weighted_dissimilarity = current_clusters_distance;
    // in case we already collect max_results_size values and this value is greater than our max value, we will not
    // consider inserting it to our "best" dissimilarities solution cache
    if (sorted_merge_offers.size() >= clustering_params.max_results_size &&
        current_weighted_dissimilarity >= sorted_merge_offers.front().weighted_dissimilarity)
        return;

    // in case we are using load balance and the new sizes in case of a merge are invalid
    if (clustering_params.load_balance && !isValidClustersMerge(clustering_params, cluster1, cluster2))
        return;

    if (sorted_merge_offers.size() < clustering_params.max_results_size){
        // we are yet to collect max_results_size values
        sorted_merge_offers.emplace_back(current_weighted_dissimilarity, cluster1, cluster2);
    }
    else if (current_weighted_dissimilarity < sorted_merge_offers.front().weighted_dissimilarity){
        // this value is smaller than our max value, replace
        sorted_merge_offers.front() = {current_weighted_dissimilarity, cluster1, cluster2};
    }

    // sort them in descending order for keeping the max value at front
    sort(sorted_merge_offers.begin(), sorted_merge_offers.end(), HierarchicalClustering::sortDesc);
}

bool HierarchicalClustering::sortDesc(const ClustersMergeOffer &a, const ClustersMergeOffer &b) {
    if(a.weighted_dissimilarity > b.weighted_dissimilarity)
        return true;

    if(a.weighted_dissimilarity == b.weighted_dissimilarity && a.cluster1 > b.cluster1)
        return true;

    if(a.weighted_dissimilarity == b.weighted_dissimilarity && a.cluster1 == b.cluster1 && a.cluster2 > b.cluster2)
        return true;

    return false;
}

bool HierarchicalClustering::sortAsc(const ClustersMergeOffer &a, const ClustersMergeOffer &b) {
    return sortDesc(b,a);
}

double HierarchicalClustering::getPhysicalDistanceOfCluster(const int max_number_of_clusters,
                                                            const AlgorithmDSManager::DissimilarityCell& diss_cell,
                                                            const double W_T, const bool use_new_dist_metric){
    long long summed_files_size = 0;
    long long max_origin_files_size = 0;
    for(const auto origin_cluster_size_pair : diss_cell.size_from_origin_clusters)
    {
        summed_files_size += origin_cluster_size_pair.second;
        if(origin_cluster_size_pair.second > max_origin_files_size){
            max_origin_files_size = origin_cluster_size_pair.second;
        }
    }

    const double num_cluster_fraction =
            static_cast<double>(diss_cell.size_from_origin_clusters.size()) / max_number_of_clusters;

    if(!use_new_dist_metric){
        return num_cluster_fraction;
    }

    const double files_size_fraction =
            static_cast<double>(summed_files_size - (static_cast<double>(1-W_T) * max_origin_files_size)) /
            summed_files_size;

    return files_size_fraction * num_cluster_fraction;
}

double HierarchicalClustering::getDistanceBetweenClusters(const ClusteringParams& clustering_params,
                                                          const int cluster1, const int cluster2){
    const int max_number_of_clusters = m_ds->getNumOfWorkloads();
    const double W_T = (clustering_params.w_traffic / 100.0);
    const AlgorithmDSManager::DissimilarityCell& cluster1_2_dissimilarity = m_ds->getDissimilarityCell(cluster1, cluster2);

    const double physical_distance = getPhysicalDistanceOfCluster(max_number_of_clusters,
                                                                  cluster1_2_dissimilarity,
                                                                  W_T, clustering_params.use_new_dist_metric);

    return (1 - W_T) * physical_distance + W_T * cluster1_2_dissimilarity.jaccard_distance;
}

HierarchicalClustering::ClustersMergeOffer HierarchicalClustering::randClustersMergeOfferFromVector(
        const vector<ClustersMergeOffer>& sorted_merge_offers,
        const double gap){
    // get the last index which is in gap range
    int max_index_in_gap = sorted_merge_offers.size() - 1;
    while (max_index_in_gap >= 1 ){
        if(sorted_merge_offers[max_index_in_gap].weighted_dissimilarity <=
           sorted_merge_offers.front().weighted_dissimilarity * (1 + gap / 100.0))
            break;

        max_index_in_gap--;
    }

    // fetch random value from the vector
    const int random_index = rand() % (max_index_in_gap + 1);

    return sorted_merge_offers[random_index];
}

bool HierarchicalClustering::isValidClustersMerge(const ClusteringParams& clustering_params, const int cluster1,
                                                  const int cluster2) const{

    // compile a list of all the current clusters' sizes in the system beside those we merge
    vector<double> curr_sizes;
    curr_sizes.reserve(m_ds->getNumberOfFilesForClustering());

    for (int i = 0; i < m_ds->getNumberOfFilesForClustering(); ++i) {
        if (i != cluster1 && i != cluster2 && m_clusters[i]->isActivated())
            curr_sizes.push_back(m_clusters[i]->getSize());
    }

    // add the size of the clusters we merge into one
    curr_sizes.push_back(getMergedClusterSize(cluster1, cluster2));

    // sort in descending order
    std::sort(curr_sizes.begin(), curr_sizes.end(), std::greater<double>());

    // assert that our constraints are satisfied
    for (int i = 0; i < m_lb_sizes.size(); ++i) {
        if (curr_sizes[i] > ((m_lb_sizes[i] + clustering_params.internal_margin)/ 100.0) *
                            clustering_params.approx_system_size){
            return false;
        }
    }

    return true;
}

int HierarchicalClustering::getClusterSize(const set<int>& files_indices) const{
    // find the files of the merged cluster
    unordered_set<int> cluster_blocks;
    int cluster_size = 0;

    for (int fp_index = 0; fp_index < m_ds->getNumberOfFingerprintsForClustering(); ++fp_index) {
        for (const auto& file_index : files_indices) {
            // we will add its size to cluster_size
            if (m_ds->isFileHasFingerprint(file_index, fp_index) && cluster_blocks.find(fp_index) == cluster_blocks.cend()){
                cluster_size += m_ds->getFingerprintSize(fp_index);
                cluster_blocks.insert(fp_index);
            }
        }
    }

    return cluster_size;
}

double HierarchicalClustering::getMergedClusterSize(const int cluster1, const int cluster2) const{
    // find the files of the merged cluster
    unordered_set<int> cluster_blocks;
    int cluster_size = 0;

    for (int fp_index = 0; fp_index < m_ds->getNumberOfFingerprintsForClustering(); ++fp_index) {
        for (const auto& file_index : m_clusters[cluster1]->getCurrentFiles()) {

            if (m_ds->isFileHasFingerprint(file_index, fp_index) && cluster_blocks.find(fp_index) == cluster_blocks.cend()){
                cluster_size += m_ds->getFingerprintSize(fp_index);
                cluster_blocks.insert(fp_index);
            }
        }

        for (const auto& file_index : m_clusters[cluster2]->getCurrentFiles()) {
            if (m_ds->isFileHasFingerprint(file_index, fp_index) && cluster_blocks.find(fp_index) == cluster_blocks.cend()){
                cluster_size += m_ds->getFingerprintSize(fp_index);
                cluster_blocks.insert(fp_index);
            }
        }
    }

    return cluster_size;
}

void HierarchicalClustering::mergeClusters(const ClustersMergeOffer& merge_offer,
                                           const std::unordered_map<int,int>& file_to_cluster) {
    m_current_system_size -= m_clusters[merge_offer.cluster2]->getSize();
    m_current_system_size -= m_clusters[merge_offer.cluster1]->getSize();

    completeLinkage(merge_offer, file_to_cluster);

    // disable cluster 2 files to cluster 1 since we gonna it as the merged cluster
    m_clusters[merge_offer.cluster1]->addFiles( m_clusters[merge_offer.cluster2]->getCurrentFiles());

    //calculate cluster 1 size after merging cluster 2 to it
    m_clusters[merge_offer.cluster1]->setSize(getClusterSize(m_clusters[merge_offer.cluster1]->getCurrentFiles()));

    // disable nodes since we gonna user cluster1
    m_clusters[merge_offer.cluster2]->disableNode();

    m_current_system_size += m_clusters[merge_offer.cluster1]->getSize();

    //deactivate merge_offer.cluster2 since we are going to use merge_offer.cluster1 index only
    m_ds->deactivateClusterInDissimilarityMat(merge_offer.cluster2);
}

void HierarchicalClustering::completeLinkage(const ClustersMergeOffer& merge_offer,
                                             const std::unordered_map<int,int>& file_to_cluster) {
    // complete linkage before performing the merge_offer (update dissimilarities matrix)

    // iterate each file
    for (int i = 0; i < m_ds->getNumberOfFilesForClustering(); ++i) {
        const AlgorithmDSManager::DissimilarityCell cluster2_and_i_dissimilarity = m_ds->getDissimilarityCell(merge_offer.cluster2, i);

        if ((cluster2_and_i_dissimilarity.jaccard_distance != DBL_MAX)) {
            const AlgorithmDSManager::DissimilarityCell cluster1_and_i_dissimilarity = m_ds->getDissimilarityCell(merge_offer.cluster1, i);

            const AlgorithmDSManager::DissimilarityCell new_dissimilarity_cell = {
                    max(cluster2_and_i_dissimilarity.jaccard_distance, cluster1_and_i_dissimilarity.jaccard_distance),
                    Utility::getMergedMap(cluster2_and_i_dissimilarity.size_from_origin_clusters,
                                          cluster1_and_i_dissimilarity.size_from_origin_clusters,
                                          m_ds->getDissimilarityCell(i, i).size_from_origin_clusters)
            };

            // set the new value ( we only set cluster1 and i since cluster2 will be soon deactivated)
            m_ds->setDissimilarityCell(merge_offer.cluster1, i, new_dissimilarity_cell);
        }
    }
}

string HierarchicalClustering::getResultFileName(const bool contain_changes, const int num_total_iter, const int num_change_iter,
                                                 const ClusteringParams& clustering_params, const bool is_valid_result){
    std::string iter_indicator = std::to_string(num_total_iter) + "_m" + std::to_string(clustering_params.num_iter)
                                 + "_c" + std::to_string(num_change_iter);

    return  "_T" + Utility::getString(clustering_params.traffic_max) +
            "_I" + iter_indicator +
            "_WT" + Utility::getString(clustering_params.w_traffic) +
            "_S" + Utility::getString(clustering_params.seed) +
            "_G" + Utility::getString(clustering_params.gap) +
            "_E" + Utility::getString(clustering_params.eps) +
            "_M" + Utility::getString(clustering_params.initial_internal_margin) +
            (contain_changes? "_wc" : "")  +
            (clustering_params.load_balance? "_lb" : "") +
            (is_valid_result? "_V": "_NV") + ".csv";
}

shared_ptr<Calculator::CostResult> HierarchicalClustering::applyChangesToMappingAndGetCost(
        const bool is_iter_contains_changes, const int current_change_iter, const bool load_balance,const bool use_cache,
        const std::string& cache_path, const long long int allowed_traffic_bytes, const bool dont_validate_cost,
        const double internal_margin, const shared_ptr<map<string, set<int>>>& init_mapping,
        const shared_ptr<map<string, set<int>>>& final_mapping){

    std::map<std::string, std::set<int>> added_files = {};
    std::map<std::string, std::set<int>> removed_files_map = {};
    if(is_iter_contains_changes) {
        added_files = m_ds->getAddedFilesInUpdateIter(current_change_iter);
        const auto &removed_files = m_ds->getRemovedFilesInUpdateIter(current_change_iter);

        for (auto &specific_set_map: *final_mapping) {
            //remove all deleted files from final mapping
            for (const int &element: removed_files) {
                if(specific_set_map.second.erase(element)>0){
                    removed_files_map[specific_set_map.first].emplace(element);
                };
            }

            //add all new files to mapping
            specific_set_map.second.insert(added_files[specific_set_map.first].cbegin(),
                                           added_files[specific_set_map.first].cend());
        }
    }
    const map<string, double> arranged_lb_sized = m_ds->getArrangedLbSizes(m_lb_sizes);
    const map<int, int> &block_to_size_mapping = m_ds->getBlockToSizeMap();

    return Calculator::getClusteringCost(is_iter_contains_changes,
            use_cache, m_ds->getAppearancesMatrix(), block_to_size_mapping,
            *init_mapping,
            *final_mapping,
            added_files,
            removed_files_map,
            m_ds->getChangeFilePath(),
            allowed_traffic_bytes,
            dont_validate_cost, internal_margin,
            load_balance, arranged_lb_sized, cache_path);
}

void HierarchicalClustering::doChangeIter(
        const double traffic, const int current_total_iter, const int current_change_iter, const int current_iter, const int num_iters, const bool load_balance,
        const bool use_cache, const int eps, const int internal_margin, const std::string& cache_path,
        const bool dont_validate_cost,const std::string& output_path_prefix,
        std::vector<std::shared_ptr<ClusteringResult>>& incremental_migration_steps,
        bool is_contain_change){
    static const int NOT_RELEVANT = 0;
    static constexpr double NO_TRAFFIC_ON_CHANGES = 0;
    static constexpr double NOTHING_VAL = 0;
    static constexpr int MAX_RESULTS_SIZE = 10;

    const shared_ptr<map<string, set<int>>> init_mapping = make_shared<map<string, set<int>>>(m_ds->getInitialClustering());
    if(is_contain_change){
        m_ds->applyUpdatesIter(current_change_iter);
    }

    const shared_ptr<map<string, set<int>>> final_mapping = make_shared<map<string, set<int>>>(*init_mapping);
    const auto& cost = applyChangesToMappingAndGetCost(is_contain_change,
            current_change_iter, load_balance, use_cache, cache_path, NO_TRAFFIC_ON_CHANGES,
            dont_validate_cost, internal_margin, init_mapping, final_mapping);

    static const bool DIST_METRIC_NOT_RELEVANT = false;
    ClusteringParams change_clustering_params(NOTHING_VAL, traffic,
                                              NO_TRAFFIC_ON_CHANGES,NO_TRAFFIC_ON_CHANGES,
                                              NOTHING_VAL, NOTHING_VAL, m_ds->getInitialSystemSize(),
                                              m_ds->getOptimalSystemSize(), load_balance,
                                              m_ds->getNumOfWorkloads(), MAX_RESULTS_SIZE,
                                              eps,internal_margin, current_iter, m_ds->getCurrentHostName(),
                                              num_iters, current_change_iter, current_total_iter,
                                              DIST_METRIC_NOT_RELEVANT);

    incremental_migration_steps.emplace_back(std::make_unique<ClusteringResult>(
            change_clustering_params,init_mapping,
            final_mapping,cost));

    outputBestIncrementalStepResult(output_path_prefix, traffic, current_iter,
                                    current_total_iter, current_change_iter,
                                    incremental_migration_steps.back());

    m_ds->applyPlan(*(incremental_migration_steps.back()->clustering_final_mapping),
                    incremental_migration_steps.back()->cost_result->final_system_size);
}

static std::map<std::string, std::set<int>> deepCopyClusterMap(const std::map<std::string, std::set<int>>& original) {
    auto newMap = std::map<std::string, std::set<int>>(); // Create a new shared_ptr with an empty map

    for (const auto& pair : original) {
        newMap[pair.first] = pair.second;
    }

    return newMap;
}

void HierarchicalClustering::recalcAndOutputClusterResultWithChanges(const bool is_iter_contains_changes,
        const vector<shared_ptr<HierarchicalClustering::ClusteringResult>>& iter_specific_results,
        const int current_total_iter, const int current_change_iter, const bool load_balance,const bool use_cache,
        const std::string& cache_path, const long long int iter_allowed_traffic, const bool dont_validate_cost,
        const double internal_margin, const std::string& output_path_prefix){
    // recalc for each result the cost and output it
    for (auto &iter_specific_result: iter_specific_results) {
        iter_specific_result->cost_result = applyChangesToMappingAndGetCost(is_iter_contains_changes,
                current_change_iter, load_balance, use_cache, cache_path, iter_allowed_traffic,
                dont_validate_cost, internal_margin,
                iter_specific_result->clustering_initial_mapping,
                iter_specific_result->clustering_final_mapping);

        const string file_path = output_path_prefix +
                                 getResultFileName(
                                         is_iter_contains_changes,
                                         current_total_iter, current_change_iter,
                                         iter_specific_result->clustering_params,
                                         iter_specific_result->cost_result->is_traffic_valid &&
                                         iter_specific_result->cost_result->is_lb_valid);

        Utility::printMigrationStep(iter_specific_result->clustering_params.server_name,
                                    iter_specific_result->clustering_params.num_total_iter,
                                    iter_specific_result->clustering_params.num_iter,
                                    iter_specific_result->clustering_params.num_change_iter,
                                    iter_specific_result->clustering_params.iter_traffic_max_bytes,
                                    iter_specific_result->clustering_params.initial_internal_margin,
                                    getConvertedResults(*iter_specific_result->clustering_final_mapping),
                                    getConvertedResults(*iter_specific_result->clustering_initial_mapping),
                                    iter_specific_result->cost_result,
                                    file_path,
                                    false);
    }
}
static void createDirsInPrefix(const vector<string>& splitted_prefix){
    string accumulated_prefix;

    for(int i=0; i < splitted_prefix.size() -1; ++i){
        if(splitted_prefix[i].size() ==0)
            continue;

        accumulated_prefix += splitted_prefix[i] + "/";
        Utility::createDir(accumulated_prefix);
    }
}

void HierarchicalClustering::runWithNaiveSplit(const vector<string>& workload_paths, const std::string& change_pos,
                                 const int num_run_changes_iter, const bool load_balance, const bool use_cache,
                                 const std::string& cache_path, const int margin, const int eps, const double traffic,
                                 const std::vector<double> &wts, const std::vector<int> &seeds,
                                 const std::vector<double> &gaps,
                                 const int nums_incremental_iterations, const std::string& output_path_prefix,
                                 const int num_runs, bool is_converging_margin, bool use_new_dist_metric,
                                 GreedySplit::TransferSort split_sort_order, const bool carry_traffic){
    static bool VALIDATE = false;
    const map<string, double> arranged_lb_sized = m_ds->getArrangedLbSizes(m_lb_sizes);
    const auto traffic_start_time = chrono::high_resolution_clock::now();
    std::vector<std::shared_ptr<ClusteringResult>> incremental_migration_steps;
    incremental_migration_steps.reserve(nums_incremental_iterations * num_runs);

    map<string, int> workload_path_to_index;
    for (int i = 0; i < (int) workload_paths.size(); ++i) {
        workload_path_to_index[workload_paths[i]] = i;
    }

    int total_current_change_iter = 0;
    int total_num_incremental_iter = 0;
    int total_current_iter = 0;

    const long long int run_orig_initial_system_size = m_ds->getInitialSystemSize();
    for(int num_run = 1; num_run <= num_runs; ++num_run) {
        int run_num_incremental_iter = 0;
        int current_run_change_iter = 0;

        long long int allowed_traffic_in_bytes = static_cast<double>(run_orig_initial_system_size) * (traffic / 100.0);
        long long int allowed_traffic_in_bytes_remaining = allowed_traffic_in_bytes;
        long long int allowed_traffic_for_iter_in_bytes = 0;
        long long int leftovers = 0;
        if (nums_incremental_iterations > 0) {
            allowed_traffic_for_iter_in_bytes = allowed_traffic_in_bytes / nums_incremental_iterations;
            leftovers = allowed_traffic_in_bytes % nums_incremental_iterations;
        }


        leftovers = carry_traffic? leftovers : 0;
        static constexpr double NOT_RELEVANT = 0;
        // --------- immediate program ------------
        std::string immediate_prefix = "immediate_run"+ to_string(num_run) + "/";
        std::string immediate_cache = immediate_prefix +cache_path;
        std::string immediate_output = immediate_prefix + output_path_prefix;

        createDirsInPrefix(Utility::splitString(immediate_cache, '/'));
        createDirsInPrefix(Utility::splitString(immediate_output, '/'));
        vector<std::shared_ptr<transfer_t>> transfers = {};
        {
        vector<shared_ptr<HierarchicalClustering::ClusteringResult>> iter_specific_results =
                performIncrementalClustering(run_orig_initial_system_size, load_balance, use_cache, immediate_cache, seeds,
                                             gaps, wts, immediate_output, total_num_incremental_iter,
                                             NOT_RELEVANT, allowed_traffic_in_bytes,
                                             traffic, eps, margin,
                                             nums_incremental_iterations, total_current_change_iter, total_current_iter,
                                             use_new_dist_metric);
        static const bool CALC_NO_CHANGES = false;
        recalcAndOutputClusterResultWithChanges(
                CALC_NO_CHANGES, iter_specific_results, total_current_iter,
                total_current_change_iter, load_balance, use_cache, immediate_cache, allowed_traffic_in_bytes,
                VALIDATE, margin, immediate_output);
        sort(iter_specific_results.begin(), iter_specific_results.end(), ClusteringResult::sortResultDesc);
        shared_ptr<HierarchicalClustering::ClusteringResult> best_iter_res = iter_specific_results.front();

        transfers =
                GreedySplit::getTransfers(workload_path_to_index, workload_paths,
                                          *best_iter_res->clustering_initial_mapping,
                                          *best_iter_res->clustering_final_mapping);}

        // --------- immediate program ------------

        for (run_num_incremental_iter = 0;
             run_num_incremental_iter < nums_incremental_iterations && change_pos != "only_changes";) {
            total_current_iter++;
            run_num_incremental_iter++;
            total_num_incremental_iter++;

            leftovers = carry_traffic? leftovers : 0;
            const long long int iter_allowed_traffic = allowed_traffic_for_iter_in_bytes + leftovers;
            const double initial_internal_margin = get_iter_margin(
                    is_converging_margin, margin,run_num_incremental_iter,
                    nums_incremental_iterations);

            shared_ptr<Calculator::CostResult> iter_cost = nullptr;
            shared_ptr<map<string, set<int>>> final_mapping = make_shared<map<string, set<int>>>();
            if(change_pos == "naive_split" || change_pos == "lb_split"){
                if(change_pos == "naive_split"){
                    *final_mapping = GreedySplit::doNaiveIter(
                            *m_ds, iter_allowed_traffic,
                            initial_internal_margin,
                            arranged_lb_sized,transfers,
                            workload_paths, m_ds->getInitialClustering());
                }
                else{ // change_pos == "lb_split"
                    *final_mapping = GreedySplit::doIter(
                            *m_ds, iter_allowed_traffic,
                            initial_internal_margin,
                            arranged_lb_sized,transfers,
                            workload_paths, m_ds->getInitialClustering(), GreedySplit::HARD_LB);
                }

                std::cout << "left "<< transfers.size() <<" transfers out" << std::endl;
                iter_cost = Calculator::getClusteringCost(
                        false, use_cache, m_ds->getAppearancesMatrix(),
                        m_ds->getBlockToSizeMap(),
                        m_ds->getInitialClustering(),
                        *final_mapping,
                        {},
                        {},
                        m_ds->getChangeFilePath(),
                        iter_allowed_traffic,
                        VALIDATE, initial_internal_margin,
                        load_balance, arranged_lb_sized, cache_path);
            }

            bool is_iter_contains_changes = false;
            const auto orig_init = std::make_shared<map<string, set<int>>>(m_ds->getInitialClusteringCopy());
            // Apply changes here if needed
            if (current_run_change_iter < num_run_changes_iter) {
                current_run_change_iter++;
                total_current_change_iter++;
                is_iter_contains_changes = true;
                m_ds->applyUpdatesIter(total_current_change_iter, final_mapping);
            }


            // recalc for each result the cost and output it
            if(is_iter_contains_changes){
                iter_cost = applyChangesToMappingAndGetCost(is_iter_contains_changes,
                                                            total_current_change_iter, load_balance, use_cache, cache_path, iter_allowed_traffic,
                                                            VALIDATE, initial_internal_margin,
                                                            orig_init,
                                                            final_mapping);
            }

            ClusteringParams dummy_clustering_params(0, traffic, iter_allowed_traffic, NOT_RELEVANT,0, 0,
                                               m_ds->getInitialSystemSize(),
                                               m_ds->getOptimalSystemSize(), load_balance,
                                               m_ds->getNumOfWorkloads(), 0,
                                               eps, margin, total_num_incremental_iter, m_ds->getCurrentHostName(),
                                               nums_incremental_iterations, total_current_change_iter, total_current_iter,
                                               use_new_dist_metric);

            dummy_clustering_params.wt_elapsed_time_seconds = 0; // "do nothing" took 0 seconds to calc :)
            incremental_migration_steps.emplace_back(
                    make_unique<ClusteringResult>(dummy_clustering_params, orig_init,
                                                  final_mapping, iter_cost));
            leftovers = iter_allowed_traffic - iter_cost->traffic_bytes;
            allowed_traffic_in_bytes_remaining -= iter_cost->traffic_bytes;

            // apply the best result and continue to next loop
            m_ds->applyPlan(*(final_mapping), iter_cost->final_system_size);
        }
    }

    const long long int traffic_elapsed_time_seconds =
            chrono::duration_cast<chrono::seconds>(chrono::high_resolution_clock::now() - traffic_start_time).count();
    outputIncrementalMigration(output_path_prefix + "_T" + Utility::getString(traffic) + "_migration_plan.csv",
                               incremental_migration_steps, traffic_elapsed_time_seconds);
}

void HierarchicalClustering::run(const vector<string>& workload_paths, const std::string& change_pos,
                                 const int num_run_changes_iter, const bool load_balance, const bool use_cache,
                                 const std::string& cache_path, const int margin, const int eps, const double traffic,
                                 const std::vector<double> &wts, const std::vector<int> &seeds,
                                 const std::vector<double> &gaps,
                                 const int nums_incremental_iterations, const std::string& output_path_prefix,
                                 const int num_runs, bool is_converging_margin, bool use_new_dist_metric,
                                 GreedySplit::TransferSort split_sort_order, const bool carry_traffic) {
    if(change_pos == "naive_split" || change_pos == "lb_split"){
        return runWithNaiveSplit(workload_paths, change_pos, num_run_changes_iter, load_balance, use_cache, cache_path,
                                 margin, eps, traffic, wts, seeds, gaps, nums_incremental_iterations, output_path_prefix,
                                 num_runs, is_converging_margin, use_new_dist_metric, split_sort_order, carry_traffic);
    }
    static bool VALIDATE = false;
    const map<string, double> arranged_lb_sized = m_ds->getArrangedLbSizes(m_lb_sizes);
    const auto traffic_start_time = chrono::high_resolution_clock::now();
    std::vector<std::shared_ptr<ClusteringResult>> incremental_migration_steps;
    incremental_migration_steps.reserve(nums_incremental_iterations * num_runs);

    map<string, int> workload_path_to_index;
    for (int i = 0; i < (int) workload_paths.size(); ++i) {
        workload_path_to_index[workload_paths[i]] = i;
    }

    int total_current_change_iter = 0;
    int total_num_incremental_iter = 0;
    int total_current_iter = 0;

    const long long int run_orig_initial_system_size = m_ds->getInitialSystemSize();
    for(int num_run = 1; num_run <= num_runs; ++num_run) {
        int run_num_incremental_iter = 0;
        int current_run_change_iter = 0;

        long long int allowed_traffic_in_bytes = static_cast<double>(run_orig_initial_system_size) * (traffic / 100.0);
        long long int allowed_traffic_in_bytes_remaining = allowed_traffic_in_bytes;
        long long int allowed_traffic_for_iter_in_bytes = 0;
        long long int leftovers = 0;
        if (nums_incremental_iterations > 0) {
            allowed_traffic_for_iter_in_bytes = allowed_traffic_in_bytes / nums_incremental_iterations;
            leftovers = allowed_traffic_in_bytes % nums_incremental_iterations;
        }

        // ------------------------------------------------
        // Apply changes before migration start if needed
        while ((change_pos == "migration_after_changes" && current_run_change_iter < num_run_changes_iter - 1) ||
                (change_pos == "only_changes" && current_run_change_iter < num_run_changes_iter)) {
            total_current_iter++;

            current_run_change_iter++;
            total_current_change_iter++;

            doChangeIter(traffic, total_current_iter, total_current_change_iter,
                         total_num_incremental_iter,
                         nums_incremental_iterations,
                         load_balance, use_cache, eps, margin, cache_path, VALIDATE,
                         output_path_prefix, incremental_migration_steps);
        }

        // ------------------------------------------------

        for (run_num_incremental_iter = 0;
             run_num_incremental_iter < nums_incremental_iterations && change_pos != "only_changes";) {
            total_current_iter++;
            run_num_incremental_iter++;
            total_num_incremental_iter++;

            leftovers = carry_traffic? leftovers : 0;
            const long long int iter_allowed_traffic = allowed_traffic_for_iter_in_bytes + leftovers;
            static constexpr double NOT_RELEVANT = 0;
            const double initial_internal_margin = get_iter_margin(
                    is_converging_margin, margin,run_num_incremental_iter,
                    nums_incremental_iterations);

            const long long int max_allowed_remaining_traffic_to_all_runs =  num_run < num_runs ?
                    allowed_traffic_in_bytes : allowed_traffic_in_bytes_remaining;
            const long long int hc_allowed_traffic_for_iter = (change_pos != "smart_split") ? iter_allowed_traffic:
                                                              max_allowed_remaining_traffic_to_all_runs;

            vector<shared_ptr<HierarchicalClustering::ClusteringResult>> iter_specific_results =
                    performIncrementalClustering(run_orig_initial_system_size, load_balance, use_cache, cache_path, seeds,
                                                 gaps, wts, output_path_prefix, total_num_incremental_iter,
                                                 NOT_RELEVANT, hc_allowed_traffic_for_iter,
                                                 traffic, eps, initial_internal_margin,
                                                 nums_incremental_iterations, total_current_change_iter, total_current_iter,
                                                 use_new_dist_metric);

            static const bool CALC_NO_CHANGES = false;
            recalcAndOutputClusterResultWithChanges(
                    CALC_NO_CHANGES, iter_specific_results, total_current_iter,
                    total_current_change_iter, load_balance, use_cache, cache_path, hc_allowed_traffic_for_iter,
                    VALIDATE, initial_internal_margin, output_path_prefix);

            sort(iter_specific_results.begin(), iter_specific_results.end(), ClusteringResult::sortResultDesc);
            shared_ptr<HierarchicalClustering::ClusteringResult> best_iter_res = iter_specific_results.front();

            if(change_pos == "smart_split"){
                vector<std::shared_ptr<transfer_t>> transfers =
                            GreedySplit::getTransfers(workload_path_to_index, workload_paths,
                                                      *best_iter_res->clustering_initial_mapping,
                                                      *best_iter_res->clustering_final_mapping);

                *best_iter_res->clustering_final_mapping = GreedySplit::doIter(
                        *m_ds, iter_allowed_traffic,
                        initial_internal_margin,
                        arranged_lb_sized,transfers,
                        workload_paths, *best_iter_res->clustering_initial_mapping,
                        split_sort_order);

                std::cout << "left "<< transfers.size() <<" transfers out" << std::endl;
                best_iter_res->cost_result = Calculator::getClusteringCost(
                        false, use_cache, m_ds->getAppearancesMatrix(),
                        m_ds->getBlockToSizeMap(),
                        *best_iter_res->clustering_initial_mapping,
                        *best_iter_res->clustering_final_mapping,
                        {},
                        {},
                        m_ds->getChangeFilePath(),
                        iter_allowed_traffic,
                        VALIDATE, initial_internal_margin,
                        load_balance, arranged_lb_sized, cache_path);
            }

            bool is_iter_contains_changes = false;
            const auto orig_init = std::make_shared<map<string, set<int>>>(m_ds->getInitialClusteringCopy());
            // Apply changes here if needed
            if (current_run_change_iter < num_run_changes_iter) {
                current_run_change_iter++;
                total_current_change_iter++;
                is_iter_contains_changes = true;
                best_iter_res->clustering_params.num_change_iter++;
                m_ds->applyUpdatesIter(total_current_change_iter, best_iter_res->clustering_final_mapping);
            }

            for(auto& res : iter_specific_results){
                res->clustering_initial_mapping = orig_init;
            }

            // recalc for each result the cost and output it
            if(is_iter_contains_changes){
                recalcAndOutputClusterResultWithChanges(is_iter_contains_changes,
                                                        iter_specific_results, total_current_iter, total_current_change_iter,
                                                        load_balance, use_cache, cache_path,
                                                        iter_allowed_traffic, VALIDATE, initial_internal_margin,
                                                        output_path_prefix);
            }


            outputBestIncrementalStepResult(output_path_prefix, traffic, total_num_incremental_iter, total_current_iter,
                                            total_current_change_iter, best_iter_res);

            // apply the best result and continue to next loop
            m_ds->applyPlan(*(best_iter_res->clustering_final_mapping),
                            best_iter_res->cost_result->final_system_size);
            incremental_migration_steps.emplace_back(best_iter_res);
            leftovers = iter_allowed_traffic - best_iter_res->cost_result->traffic_bytes;
            allowed_traffic_in_bytes_remaining -= best_iter_res->cost_result->traffic_bytes;
        }

        // ------------------------------------------------
        // apply rest of changes after the migration if needed
        while (change_pos == "migration_before_changes" && current_run_change_iter < num_run_changes_iter) {
            current_run_change_iter++;
            total_current_change_iter++;
            total_current_iter++;
            doChangeIter(traffic, total_current_iter, total_current_change_iter, total_num_incremental_iter,
                         nums_incremental_iterations,
                         load_balance, use_cache, eps, margin, cache_path, VALIDATE,
                         output_path_prefix, incremental_migration_steps);
        }
        // ------------------------------------------------
    }

    const long long int traffic_elapsed_time_seconds =
            chrono::duration_cast<chrono::seconds>(chrono::high_resolution_clock::now() - traffic_start_time).count();
    outputIncrementalMigration(output_path_prefix + "_T" + Utility::getString(traffic) + "_migration_plan.csv",
                               incremental_migration_steps, traffic_elapsed_time_seconds);
}

void HierarchicalClustering::outputBestIncrementalStepResult(
        const string &output_path_prefix,
        const double traffic, int num_incremental_iter, int num_total_iter,int num_change_iter,
        const shared_ptr<ClusteringResult> &iter_best_result) const {

    // output results to csv
    std::string iter_indicator = std::to_string(num_total_iter) + "_m" + std::to_string(num_incremental_iter)
                                 + "_c" + std::to_string(num_change_iter);
    const string file_path = output_path_prefix + "_T" + Utility::getString(traffic) + "_best_iter_" +
            iter_indicator + ".csv";

    Utility::printMigrationStep(iter_best_result->clustering_params.server_name,
                                iter_best_result->clustering_params.num_total_iter,
                                iter_best_result->clustering_params.num_iter,
                                iter_best_result->clustering_params.num_change_iter,
                                iter_best_result->clustering_params.iter_traffic_max_bytes,
                                iter_best_result->clustering_params.initial_internal_margin,
                                getConvertedResults(*iter_best_result->clustering_final_mapping),
                                getConvertedResults(*iter_best_result->clustering_initial_mapping),
                                iter_best_result->cost_result,
                                file_path,
                                false);
}

vector<shared_ptr<HierarchicalClustering::ClusteringResult>> HierarchicalClustering::performIncrementalClustering(
        const long long int orig_initial_system_size, const bool load_balance, const bool use_cache,
        const std::string& cache_path, const vector<int> &seeds, const vector<double> &gaps,
        const std::vector<double> &wts, const string &output_path_prefix, const int num_iter, double iter_traffic,
        const long long int iter_traffic_bytes,
        const double total_traffic_max, const double eps, const double margin_iter, const int32_t num_iterations,
        const int32_t current_change_iter,const int32_t current_total_iter, const bool use_new_dist_metric) {

    vector<shared_ptr<ClusteringResult>> iter_specific_results;
    iter_specific_results.reserve(seeds.size() * gaps.size() * wts.size() + 1);

    const shared_ptr<map<string, set<int>>> init_cluster_map = make_shared<map<string, set<int>>>(m_ds->getInitialClusteringCopy());
    const vector<set<int>> init_cluster_vector = m_ds->getInitialClusteringAsVector();
    map<double, long long int > timer_per_wt = {};
    static constexpr int MAX_RESULTS_SIZE = 10;

    // for each combo of seed, gap, wt execute the algorithm
    for(const double wt: wts){
        const auto wt_run_start_time = chrono::high_resolution_clock::now();
        for (const int seed : seeds){
            srand(seed);
            for (const double gap :gaps){
                // create our results csv

                ClusteringParams clustering_params(wt, total_traffic_max, iter_traffic_bytes, iter_traffic,seed, gap,
                                                   m_ds->getInitialSystemSize(),
                                                   m_ds->getOptimalSystemSize(), load_balance,
                                                   m_ds->getNumOfWorkloads(), MAX_RESULTS_SIZE,
                                                   eps, margin_iter, num_iter, m_ds->getCurrentHostName(),
                                                   num_iterations, current_change_iter, current_total_iter,
                                                   use_new_dist_metric);

                // loop until we succeed to build a valid dendrogram
                while(!performClustering(clustering_params)){
                    std::cout<< "Failed in iter Num:"<< clustering_params.num_attempts << std::endl;

                    clustering_params.internal_margin=margin_iter *
                                                      pow(1 + clustering_params.eps / 100.0, clustering_params.num_attempts);

                    clustering_params.num_attempts++;
                }

                const shared_ptr<map<string, set<int>>> clustering_result = getClusteringResult(init_cluster_vector);

                std::cout << "Finish for param Wt=" << wt << ",seed=" << seed << ",gap=" <<gap<< " Took  from start = " <<
                    chrono::duration_cast<chrono::seconds>(chrono::high_resolution_clock::now() - wt_run_start_time).count()<<std::endl;
                iter_specific_results.emplace_back(make_unique<ClusteringResult>(clustering_params, init_cluster_map, clustering_result,
                                                                                 nullptr));
            }
        }

        timer_per_wt[wt]= chrono::duration_cast<chrono::seconds>(chrono::high_resolution_clock::now() - wt_run_start_time).count();
    }

    // Add for each result the w_t elapsed time that relevant for it
    for(auto& iter_specific_result: iter_specific_results){
        iter_specific_result->clustering_params.wt_elapsed_time_seconds = timer_per_wt[iter_specific_result->clustering_params.w_traffic];
    }

    // Add the "do nothing" program
    static constexpr double NOTHING_VAL = 0;
    static const bool DIST_METRIC_NOT_RELEVANT = false;
    ClusteringParams nothing_clustering_params(NOTHING_VAL, total_traffic_max, iter_traffic_bytes,iter_traffic, NOTHING_VAL,
                                               NOTHING_VAL, m_ds->getInitialSystemSize(),
                                               m_ds->getOptimalSystemSize(), load_balance,
                                               m_ds->getNumOfWorkloads(), MAX_RESULTS_SIZE,
                                               eps,margin_iter, num_iter, m_ds->getCurrentHostName(),
                                               num_iterations, current_change_iter, current_total_iter,
                                               DIST_METRIC_NOT_RELEVANT);

    nothing_clustering_params.wt_elapsed_time_seconds = 0; // "do nothing" took 0 seconds to calc :)
    iter_specific_results.emplace_back(make_unique<ClusteringResult>(nothing_clustering_params, init_cluster_map,
                                                                     init_cluster_map, nullptr));

    return std::move(iter_specific_results);
}


shared_ptr<map<string, set<int>>> HierarchicalClustering::getClusteringResult(const vector<set<int>>& initial_clusters) const {
    const vector<set<int>> final_clusters = getCurrentClustering();

    const vector<int> workload_to_cluster_map = getGreedyWorkloadToClusterMapping(initial_clusters, final_clusters);

    return getArrangedResult(final_clusters, workload_to_cluster_map);
}

shared_ptr<map<string, set<int>>> HierarchicalClustering::getArrangedResult(const vector<set<int>>& final_clusters,
                                                                            const vector<int>& workload_to_cluster_map) const{
    shared_ptr<map<string, set<int>>> arranged_results = make_shared<map<string, set<int>>>();
    for (int i = 0; i < workload_to_cluster_map.size(); ++i)
        arranged_results->insert({ m_ds->getWorkloadFullPath(i), final_clusters[workload_to_cluster_map[i]]});

    return arranged_results;
}

map<string, set<int>> HierarchicalClustering::getConvertedResults(const map<string, set<int>>& clustering) const{
    map<string, set<int>> converted_results;
    for(const auto& workload_to_files_mapping: clustering){
        const string workload_name = workload_to_files_mapping.first;
        set<int> cluster_file_sn;
        for (const int file_index : workload_to_files_mapping.second) {
            cluster_file_sn.insert(m_ds->getFileSN(file_index));
        }
        converted_results.insert({ workload_name, cluster_file_sn});
    }

    return std::move(converted_results);
}

vector<int> HierarchicalClustering::getGreedyWorkloadToClusterMapping(const vector<set<int>> &initial_clusters,
                                                                      const vector<set<int>> &final_clusters) const{

    const vector<set<int>> initial_clusters_blocks = getBlocksInClusters(initial_clusters);
    const vector<set<int>> final_clusters_blocks = getBlocksInClusters(final_clusters);

    vector<vector<int>> intersections = getBlocksIntersectionOfClusters(initial_clusters_blocks, final_clusters_blocks);

    // index i is the i'th original workload
    vector<int> workload_to_cluster_map(initial_clusters_blocks.size(), 0);

    // pair the volumes and clusters based on that matrix in a greedy manner
    for (int i = 0; i < initial_clusters_blocks.size(); ++i) {
        const pair<int, int> max_indices = findMax(intersections);

        for (int j = 0; j < final_clusters_blocks.size(); j++)
            intersections[max_indices.first][j] = -1;

        for(int k=0;k<initial_clusters_blocks.size(); ++k)
            intersections[k][max_indices.second] = -1;

        workload_to_cluster_map[max_indices.first] = max_indices.second;
    }

    return workload_to_cluster_map;
}

vector<vector<int>> HierarchicalClustering::getBlocksIntersectionOfClusters(const vector<set<int>>& clusters1_blocks,
                                                                            const vector<set<int>>& clusters2_blocks){
    // init intersection matrix
    vector<vector<int>> intersections;
    intersections.reserve(clusters1_blocks.size());
    for (int i = 0; i < clusters1_blocks.size(); ++i)
        intersections.emplace_back(vector<int>(clusters2_blocks.size(), 0));

    // set each cell to the size of block intersection between the clusters1 to a clusters2
    for (int i = 0; i < clusters1_blocks.size(); ++i) {
        for (int j = 0; j < clusters2_blocks.size(); ++j) {
            intersections[i][j] =
                    count_if(clusters1_blocks[i].cbegin(), clusters1_blocks[i].cend(),
                             [&](int fp_index)
                             {
                                 return clusters2_blocks[j].find(fp_index) != clusters2_blocks[j].cend();
                             });
        }
    }

    return intersections;
}

vector<set<int>> HierarchicalClustering::getBlocksInClusters(const vector<set<int>>& clusters) const{
    const int max_number_of_clusters = m_ds->getNumOfWorkloads();

    vector<set<int>> clusters_blocks;
    clusters_blocks.reserve(max_number_of_clusters);

    //find blocks for each cluster in the given clusters
    for(const auto& cluster : clusters){
        set<int> cluster_blocks;
        for(const int file_index : cluster){
            for (int fp_index = 0; fp_index < m_ds->getNumberOfFingerprintsForClustering(); ++fp_index) {
                if (m_ds->isFileHasFingerprint(file_index, fp_index)) // if block exists in file - add it
                    cluster_blocks.insert(fp_index);
            }
        }

        clusters_blocks.emplace_back(cluster_blocks);
    }

    return std::move(clusters_blocks);
}

pair<int, int> HierarchicalClustering::findMax(vector<vector<int>> mat) {
    static constexpr int UNSET_VALUE = -1;

    int maxX=UNSET_VALUE;
    int maxY=UNSET_VALUE;
    int maxElement=UNSET_VALUE;

    // look for the largest intersection
    for (int i = 0; i < mat.size(); ++i) {
        for (int j = 0; j < mat[i].size(); ++j) {
            if (mat[i][j] > maxElement) {
                maxElement = mat[i][j];
                maxX = i;
                maxY = j;
            }
        }
    }

    // return our result
    return {maxX, maxY};
}

vector<set<int>> HierarchicalClustering::getCurrentClustering() const{
    vector<set<int>> result;
    result.reserve(m_ds->getNumberOfFilesForClustering());

    for(const auto& cluster : m_clusters){
        if(cluster->isActivated())
            result.emplace_back(cluster->getCurrentFiles());
    }

    return std::move(result);
}

void HierarchicalClustering::outputIncrementalMigration(const string& file_path,
                                                        const vector<shared_ptr<ClusteringResult>>& traffic_specific_result,
                                                        const long long int elapsed_time_seconds){
    const long long int init_system_size = traffic_specific_result.front()->cost_result->init_system_size;

    const long long int summed_deletion_bytes = init_system_size - traffic_specific_result.back()->cost_result->final_system_size;

    long long int summed_traffic = 0;
    long long int summed_wt_elapsed_time = 0;
    bool is_valid_traffic=true, is_valid_lb= true;
    for(const auto& iter_result : traffic_specific_result){
        summed_wt_elapsed_time += iter_result->clustering_params.wt_elapsed_time_seconds;
        summed_traffic += iter_result->cost_result->traffic_bytes;
        Utility::printMigrationStep(iter_result->clustering_params.server_name,
                                    iter_result->clustering_params.num_total_iter,
                                    iter_result->clustering_params.num_iter,
                                    iter_result->clustering_params.num_change_iter,
                                    iter_result->clustering_params.iter_traffic_max_bytes,
                                    iter_result->clustering_params.initial_internal_margin,
                                    getConvertedResults(*iter_result->clustering_final_mapping),
                                    getConvertedResults(*iter_result->clustering_initial_mapping),
                                    iter_result->cost_result,
                                    file_path,
                                    true);
        is_valid_traffic &=iter_result->cost_result->is_traffic_valid;
        is_valid_lb &=iter_result->cost_result->is_lb_valid;
    }

    Utility::printSummedResults(file_path, init_system_size, summed_traffic,
                                summed_deletion_bytes, elapsed_time_seconds,
                                summed_wt_elapsed_time, traffic_specific_result.back()->cost_result->lb_score,
                                is_valid_traffic, is_valid_lb);
}

bool HierarchicalClustering::performClustering(const ClusteringParams& clustering_params) {
    resetDataStructures();

    const auto initial_file_to_cluster = m_ds->getInitialFileToClusterMapping();
    const int num_files_for_clustering = m_ds->getNumberOfFilesForClustering();
    const int num_removed_files = m_ds->getNumberOfRemovedFiles();
    for (int num_current_clusters = num_files_for_clustering - num_removed_files; num_current_clusters > clustering_params.number_of_clusters; num_current_clusters--) {
        //resetDataStructures also disable all removed files
        const ClustersMergeOffer chosen_merge_offer = findBestMerge(clustering_params, num_current_clusters,
                                                                    num_files_for_clustering);

        // check if we did not find any suitable clusters to merge, and we are yet to receive number_of_clusters clusters
        if (chosen_merge_offer.weighted_dissimilarity == -1)
            return false;

        // merge the clusters we found and update our data structures
        mergeClusters(chosen_merge_offer, initial_file_to_cluster);
    }

    return true;
}

void HierarchicalClustering::resetDataStructures() {
    m_ds->resetDMDiagonal();
    m_ds->resetDMBottomTriangle();

    initClusters();
}

double HierarchicalClustering::get_iter_margin(const bool is_converging_margin, const double final_target_margin,
                                               const unsigned int num_iter, const unsigned int num_of_iterations) {
    if(!is_converging_margin){
        return final_target_margin;
    }

    return static_cast<double>(final_target_margin)*(1.5 - num_iter * (0.5 / num_of_iterations));
}
