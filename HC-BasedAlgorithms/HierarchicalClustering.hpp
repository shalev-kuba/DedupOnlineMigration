#pragma once

#include "Node.hpp"
#include "Utility.hpp"
#include "Shared/AlgorithmDSManager.hpp"
#include "Shared/GreedySplit.hpp"
#include "Calculator/Calculator.hpp"

#include <map>
#include <string>
#include <utility>
#include <vector>
#include <set>
#include <memory>
#include <functional>
#include <iostream>


class HierarchicalClustering final {
private:
    struct ClustersMergeOffer;
    struct ClusteringParams;

public:
    struct ClusteringResult;

public:
    using resultCompareFunc = function<int(const shared_ptr<ClusteringResult>&, const shared_ptr<ClusteringResult>& )>;

public:
    /**
     * creates a new instance of Hierarchical Clustering
     * @param ds - a AlgorithmDSManager's object which contains all matrices and data structures for the algorithm
     * @param lb_sizes - sizes to load balance to, in case you're not using load balance, this argument can be anything
     */
    explicit HierarchicalClustering(std::unique_ptr<AlgorithmDSManager>& ds, const std::vector<double>& lb_sizes);

    HierarchicalClustering(const HierarchicalClustering&) = delete;
    HierarchicalClustering& operator=(const HierarchicalClustering&) = delete;
    ~HierarchicalClustering() = default;

    /**
     * runs the current instance of Hierarchical Clustering
     * @param load_balance - whether to use load balance
     * @param use_cache - whether to cache in calc
     * @param cache_path - path to cost's cache (if use_cache=true)
     * @param margin - desired final margin, relevant only with load balance constrain
     * @param eps - increment the system size by this every iteration, relevant only with load balance constrain
     * @param traffics - list of traffics to examine
     * @param wts - list of W_T to examine
     * @param seeds - list of seeds to examine
     * @param gaps - list of gaps to examine, in % (0.5, 1, 3 for instance)
     * @param num_iterations - num of clustering incremental iterations
     * @param output_path_prefix - output file path prefix
     * Note that in case you want to add volumes to the system, for example from 5 to 7, use your 5 volumes as input
     * and add to this input 2 empty volumes. A total of 7 volumes where 2 are empty
     */
    void run(const vector<string>& workload_paths, const std::string& change_pos, const int num_run_changes_iter,
             const bool load_balance, const bool use_cache, const std::string& cache_path, const int margin,
             const int eps, const double traffic, const std::vector<double> &wts,
             const std::vector<int> &seeds, const std::vector<double> &gaps,
             const int nums_incremental_iterations, const std::string& output_path_prefix,
             const int num_runs = 1, bool is_converging_margin=false, bool use_new_dist_metric=false,
             GreedySplit::TransferSort split_sort_order=GreedySplit::HARD_DELETION, const bool carry_traffic= false);


private:

    void runWithNaiveSplit(const vector<string>& workload_paths, const std::string& change_pos,
                           const int num_run_changes_iter, const bool load_balance, const bool use_cache,
                           const std::string& cache_path, const int margin, const int eps, const double traffic,
                           const std::vector<double> &wts, const std::vector<int> &seeds,
                           const std::vector<double> &gaps,
                           const int nums_incremental_iterations, const std::string& output_path_prefix,
                           const int num_runs = 1, bool is_converging_margin=false, bool use_new_dist_metric=false,
                           GreedySplit::TransferSort split_sort_order=GreedySplit::HARD_DELETION, const bool carry_traffic= false);

    shared_ptr<Calculator::CostResult> applyChangesToMappingAndGetCost( const bool is_iter_contains_changes,
            const int current_change_iter, const bool load_balance,const bool use_cache,
            const std::string& cache_path, const long long int allowed_traffic_bytes, const bool dont_validate_cost,
            const double internal_margin, const shared_ptr<map<string, set<int>>>& init_mapping,
            const shared_ptr<map<string, set<int>>>& final_mapping);


    void doChangeIter(
            const double traffic, const int current_total_iter, const int current_change_iter, const int current_iter, const int num_iters, const bool load_balance,
            const bool use_cache, const int eps, const int margin, const std::string& cache_path,
            const bool dont_validate_cost,const std::string& output_path_prefix,
            std::vector<std::shared_ptr<ClusteringResult>>& incremental_migration_steps,
            bool is_contain_change=true);


    void recalcAndOutputClusterResultWithChanges(const bool is_iter_contains_changes,
            const vector<shared_ptr<HierarchicalClustering::ClusteringResult>>& iter_specific_results,
            const int current_total_iter, const int current_change_iter, const bool load_balance,const bool use_cache,
            const std::string& cache_path, const long long int iter_allowed_traffic, const bool dont_validate_cost,
            const double internal_margin, const std::string& output_path_prefix);
    /**
     * perform the clustering process
     * @param clustering_params - clustering parameters
     * @return - whether the process was successful or not
     */
    bool performClustering(const ClusteringParams& clustering_params);

    /**

     * @return - the best clustering result of all clustering results for the current incremental iteration
     */
    vector<shared_ptr<ClusteringResult>> performIncrementalClustering(
            const long long int orig_initial_system_size, const bool load_balance, const bool use_cache, const std::string& cache_path,
            const std::vector<int> &seeds, const std::vector<double> &gaps,  const std::vector<double> &wts,
            const std::string &output_path_prefix, int num_iter, double iter_traffic,  const long long int iter_traffic_bytes, const double total_traffic_max,
            const double eps, const double margin_iter, const int32_t num_iterations,
            const int32_t current_change_iter,const int32_t current_total_iter, const bool use_new_dist_metric);

    /**
     * reset our data structures for the next execution of the algorithm
     */
    void resetDataStructures();

    /**
     * initialize the clusters vector
     */
    void initClusters();

    /**
     * @param initial_clusters - initial clusters as vector of sets
     * @return the calculated clustering result of our execution in a format of
     * mapping between workload name to its final files
     *
     * Should be called only after a successful performClustering
    */
    std::shared_ptr<std::map<std::string, std::set<int>>> getClusteringResult(const std::vector<std::set<int>>& initial_clusters) const;

    /**
     * @param final_clusters - the final clustering
     * @param workload_to_cluster_map - the mapping between the initial state clustering to the final clustering
     * @return arranged clustering result by convert the given parameters to a map where every key is a workload name
     * and the value is the set of its files in the clustering result
     */
    std::shared_ptr<std::map<std::string, std::set<int>>> getArrangedResult(const std::vector<std::set<int>>& final_clusters,
                                                                            const std::vector<int>& workload_to_cluster_map) const;

    /**
     * activates only in case load balancing is in use
     * @param clustering_params - clustering parameters
     * @param cluster1 - a cluster 1 index
     * @param cluster2 - a cluster 2 index
     * @return whether a merge between the clusters 1+2 is valid, size wise
     */
    bool isValidClustersMerge(const ClusteringParams& clustering_params, const int cluster1, const int cluster2) const;

    /**
     * @param cluster1 - a cluster 1 index
     * @param cluster2 - a cluster 2 index
     * @return the size of the cluster in case clusters 1 +2 are merged
     */
    double getMergedClusterSize(const int cluster1, const int cluster2) const;

    /**
     * @param files_indices - set of cluster's files indexes
     * @return the size of the given cluster
     */
    int getClusterSize(const std::set<int>& files_indices) const;

    /**
     * finds a random merge offer within the merge offers with the smallest dissimilarity value
     * (in the lower triangular of dissimilarity_matrix)
     *
     * @param clustering_params - clustering parameters
     * @return a random merge offer from the smallest merge offers
     */
    ClustersMergeOffer findBestMerge(const ClusteringParams& clustering_params, const int num_current_clusters,
                                     const int num_files_for_clustering);

    /**
     * performs complete linkage hierarchical clustering
     * @param merge_offer - the chosen merge offer
     * @param file_to_cluster - initial file to cluster mapping
     */
    void completeLinkage(const ClustersMergeOffer& merge_offer, const std::unordered_map<int,int>& file_to_cluster);

    /**
     * merges the clusters in the given merge_offer (random merge offer within the merge offers with the smallest
     * dissimilarity value)
     *
     * @param merge_offer - the chosen merge offer
     * @param file_to_cluster - initial file_to_cluster mapping
     */
    void mergeClusters(const ClustersMergeOffer& merge_offer, const std::unordered_map<int,int>& file_to_cluster);

    /**
     * inner function for considering a new merge offer between cluster 1 and cluster 2 when we already have merge
     * offers in the given sorted_merge_offers
     * @param cluster1 - a cluster index
     * @param cluster2 - a cluster index
     * @param sorted_merge_offers - vector of the 'best' merge offers
     * @param clustering_params - clustering parameters
     */
    void updateSortedMergeOffers(const ClusteringParams& clustering_params,
                                 std::vector<ClustersMergeOffer>&  sorted_merge_offers,
                                 const int cluster1,
                                 const int cluster2,
                                 const int num_current_clusters,
                                 const int num_files_for_clustering);

    /**
     * @return the current clustering result where every set in the given vector is a cluster
     */
    std::vector<std::set<int>> getCurrentClustering() const;

    /**
     * ClustersMergeOffer's ascending sort function
     * @param clusters - a clustering where every cluster's set contains its files indices
     * @return returns a clustering where every cluster's set contains all the blocks of its files
     */
    std::vector<std::set<int>> getBlocksInClusters(const std::vector<std::set<int>>& clusters) const;

    /**
     * returns the weighted dissimilarity value between two clusters
     * @param cluster1 - a cluster index
     * @param cluster2 - a cluster index
     * @param clustering_params - clustering parameters
     */
    double getDistanceBetweenClusters(const ClusteringParams& clustering_params, const int cluster1,
                                      const int cluster2);

    /**
     * returns the gravity forcebetween two clusters
     * @param cluster1 - a cluster index
     * @param cluster2 - a cluster index
     * @param clusters_distance - distance between two clusters
     * @param clustering_params - clustering parameters
     */
    double getGravityForce(const ClusteringParams& clustering_params, const double clusters_distance,
                           const int num_current_clusters, const int num_initial_clusters, const int cluster1, const int cluster2);

    /**
    * @return the current system's margin diffs
    */
    double getCurrentSystemMargin(const shared_ptr<Calculator::CostResult>& current_state_cost,
                                  const map<string, double>& lb_sizes) const;

    double getEstimatedSumTrafficToBalanceSystem(const shared_ptr<Calculator::CostResult>& current_state_cost,
                                                 const map<string, double>& lb_sizes, double iteration_margin) const;

    /**
     * @param max_number_of_clusters - total num of clusters
     * @param diss_cell - dissimilarity cell of the cluster we want to calculate its physical distance
     * @param W_T - weighted traffic
     * @return the physical distance of the relevant cluster
     */
    double getPhysicalDistanceOfCluster(const int max_number_of_clusters,
                                        const AlgorithmDSManager::DissimilarityCell& diss_cell,
                                        const double W_T, const bool use_new_dist_metric);

    /**
     * @param initial_clusters - the initial clustering
     * @param final_clusters - the final clustering
     * @return a greedy mapping between the initial clustering to the final clustering based on blocks intersection
     * between the clustering
     */
    std::vector<int> getGreedyWorkloadToClusterMapping(const std::vector<std::set<int>>& initial_clusters,
                                                       const std::vector<std::set<int>>& final_clusters) const;

    /**
     * @param clustering - an arranged clustering mapping as returned from getArrangedResults
     * @return convert the algo based index of the file to the actual file index (SN) of the workload
     */
    std::map<std::string, std::set<int>> getConvertedResults(const std::map<std::string, std::set<int>>& clustering) const;

private:
    /**
     * ClustersMergeOffer's descending sort function
     * @param a - first merge offer
     * @param b - second merge offer
     * @return whether a > b by (weighted_dissimilarity, cluster1, cluster2)
     */
    static bool sortDesc(const ClustersMergeOffer &a, const ClustersMergeOffer &b);

    /**
     * ClustersMergeOffer's ascending sort function
     * @param a - first merge offer
     * @param b - second merge offer
     * @return whether a < b by (weighted_dissimilarity, cluster1, cluster2)
     */
    static bool sortAsc(const ClustersMergeOffer &a, const ClustersMergeOffer &b);

    /**
     * ClustersMergeOffer's ascending sort function
     * @param clusters1_blocks - a clustering where every cluster's ser contains all the blocks of its files
     * @param clusters2_blocks - a clustering where every cluster's set contains all the blocks of its files
     * @return a intersection matrix where every cell [i,j] holds the number of shared blocks between
     * cluster i and cluster j
     */
    static std::vector<std::vector<int>> getBlocksIntersectionOfClusters(
            const std::vector<std::set<int>>& clusters1_blocks,
            const std::vector<std::set<int>>& clusters2_blocks);

    /**
     * finds the best suited cluster and original volume to match in our assignment of cluster to volumes
     * @param mat - the matrix of blocks' number intersection between clusters and original volumes
     * @return the best suited cluster and original volume to match
     */
    static std::pair<int, int> findMax(std::vector<std::vector<int>> mat);

    /**
     * @param sorted_merge_offers - list of sorted merge offers
     * @param gap - gap param of the clustering
     * @return a randomized offer from all offers in sorted_merge_offers that within the gap range
     */
    static ClustersMergeOffer randClustersMergeOfferFromVector(
            const std::vector<ClustersMergeOffer>& sorted_merge_offers,
            const double gap);

    void outputBestIncrementalStepResult( const string &output_path_prefix, const double traffic,
                                          int num_incremental_iter,int num_total_iter,int num_change_iter,
                                          const shared_ptr<HierarchicalClustering::ClusteringResult> &iter_best_result) const;

    void outputIncrementalMigration(const std::string& file_path,
                                    const std::vector<std::shared_ptr<ClusteringResult>>& traffic_specific_result,
                                    const long long int elapsed_time_seconds);

    static std::string getResultFileName(const bool contain_changes, const int num_total_iter, const int num_change_iter,
                                         const ClusteringParams& clustering_params, const bool is_valid_result);

    /**
     * @param final_target_margin - the final target margin
     * @param num_iter - the num of iteration
     * @param num_of_iterations - total number of incremental iterations
     * @return the margin that should be used for the given num_iter
     */
    double get_iter_margin(const bool is_converging_margin, const double final_target_margin,
                           const unsigned int num_iter, const unsigned int num_of_iterations);

    // m_clusters - list of nodes which represent the cluster. each cluster contains set of all the files in it
    // m_lb_sizes - sizes to load balance to, relevant for load balance only
    // m_ds - a AlgorithmDSManager's object which contains all the data structures for the algorithm
private:
    double m_current_system_size;
    std::vector<std::unique_ptr<Node>> m_clusters;
    const std::vector<double> m_lb_sizes;
    const std::unique_ptr<AlgorithmDSManager> m_ds;
};


struct HierarchicalClustering::ClusteringParams final{
public:
    ClusteringParams(
            const double w_traffic, const double traffic_max, const long long int iter_traffic_max_bytes,
            const double iter_traffic, const int seed, const double gap,
            const long long int initial_system_size, const long long int optimal_system_size,
            const bool load_balance, const int number_of_clusters, const int max_results_size, const double eps,
            const double margin, const int num_iter, string server_name, const int32_t num_iters,int num_change_iter,
            int num_total_iter, const bool use_new_dist_metric) :
            server_name(std::move(server_name)),
            wt_elapsed_time_seconds(-1),
            num_attempts(1),
            seed(seed),
            gap(gap),
            w_traffic(w_traffic),
            traffic_max(traffic_max),
            iter_traffic_max_bytes(iter_traffic_max_bytes),
            load_balance(load_balance),
            number_of_clusters(number_of_clusters),
            max_results_size(max_results_size),
            eps(eps),
            internal_margin(margin),
            initial_internal_margin(margin),
            num_iter(num_iter),
            current_system_size(0),
            num_iters(num_iters),
            num_change_iter(num_change_iter),
            num_total_iter(num_total_iter),
            use_new_dist_metric(use_new_dist_metric)
            {

        const double W_T = (w_traffic / 100.0);
        approx_system_size = initial_system_size - W_T * (initial_system_size - optimal_system_size);
            }
public:
    long long int iter_traffic_max_bytes;
    double traffic_max;
    double w_traffic;
    double gap;
    double approx_system_size;
    long long int wt_elapsed_time_seconds;
    int seed;
    double eps;
    double initial_internal_margin;
    double internal_margin;
    int num_attempts;
    int num_iter;
    int number_of_clusters;
    double current_system_size;
    int max_results_size;
    bool load_balance;
    bool use_new_dist_metric;
    int num_iters;
    int num_change_iter;
    int num_total_iter;
    const string server_name;
};

struct HierarchicalClustering::ClusteringResult final{
public:
    ClusteringResult(ClusteringParams clustering_params,
                     const shared_ptr<map<string, set<int>>>& clustering_initial_mapping,
                     const shared_ptr<map<string, set<int>>>& clustering_final_mapping,
                     const shared_ptr<Calculator::CostResult>& cost_result):
                     clustering_params(std::move(clustering_params)),
                     clustering_initial_mapping(clustering_initial_mapping),
                     clustering_final_mapping(clustering_final_mapping),
                     cost_result(cost_result) {}

                     ClusteringResult(const ClusteringResult&) = delete;
    ClusteringResult& operator=(const ClusteringResult&) = delete;
    ~ClusteringResult() = default;

public:
    static vector<resultCompareFunc> sort_order;

public:
    static int compareDeletion(const shared_ptr<HierarchicalClustering::ClusteringResult>& res1,
                               const shared_ptr<HierarchicalClustering::ClusteringResult>& res2);

    static int compareLbVaild(const shared_ptr<HierarchicalClustering::ClusteringResult>& res1,
                              const shared_ptr<HierarchicalClustering::ClusteringResult>& res2);

    static int compareTrafficVaild(const shared_ptr<HierarchicalClustering::ClusteringResult>& res1,
                                   const shared_ptr<HierarchicalClustering::ClusteringResult>& res2);

    static int compareTraffic(const shared_ptr<HierarchicalClustering::ClusteringResult>& res1,
                              const shared_ptr<HierarchicalClustering::ClusteringResult>& res2);

    static int compareLbScore(const shared_ptr<HierarchicalClustering::ClusteringResult>& res1,
                              const shared_ptr<HierarchicalClustering::ClusteringResult>& res2);

    static bool sortResultDesc(const shared_ptr<HierarchicalClustering::ClusteringResult>& res1,
                               const shared_ptr<HierarchicalClustering::ClusteringResult>& res2);

public:
    ClusteringParams clustering_params;
    const shared_ptr<map<string, set<int>>> clustering_final_mapping;
    shared_ptr<map<string, set<int>>> clustering_initial_mapping;
    shared_ptr<Calculator::CostResult> cost_result;
};
