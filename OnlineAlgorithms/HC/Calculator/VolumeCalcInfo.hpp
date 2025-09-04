#pragma once

#include <string>
#include <set>
#include <vector>
#include <map>

using namespace std;

class VolumeCalcInfo final {
public:
    static constexpr char* const MEMORY_CACHE_PATH = "!memory_cache!";
public:
    explicit VolumeCalcInfo(string  volume_name, const set<int>& initial_files, const set<int>& final_files,
                            const set<int>& added_files, const set<int>& removed_files, const vector<vector<bool>> &appearances_matrix,
                            const map<int, int>& block_to_size, const bool use_cache, const string& cache_path);

    explicit VolumeCalcInfo(string  volume_name, const  long long int init_volume_size,
                            const long long int received_bytes, const  long long int delete_bytes,
                            const  long long int traffic,
                            const  long long int overlap_mig_changes_traffic,
                            const long long int mig_reuse_blocks_spare_traffic_bytes,
                            const long long int migration_aborted_traffic);

    explicit VolumeCalcInfo(const VolumeCalcInfo& a) = default;

public:
    const string& getVolumeName() {return m_volume_name;}
    long long int getInitVolumeSize() const {return m_init_volume_size;}
    long long int getFinalVolumeSize() const {return m_init_volume_size + m_received_bytes - m_num_bytes_deleted;}
    long long int getReceivedBytes() const {return m_received_bytes;}
    long long int getTrafficBytes() const {return m_only_mig_traffic + m_migration_aborted_traffic/2 + m_overlap_mig_changes_traffic/2;}
    long long int getNumBytesDeleted() const {return m_num_bytes_deleted;}
    long long int getOverlapTrafficBytes() const {return m_overlap_mig_changes_traffic;}
    long long int getBlockReuseBytes() const {return m_mig_reuse_blocks_spare_traffic_bytes;}
    long long int getAbortedTrafficBytes() const {return m_migration_aborted_traffic;}
    static set<int> getBlocksInFiles(const vector<vector<bool>> &appearances_matrix, const set<int>& file_indices);
    static set<int> getBlocksInFile(const vector<vector<bool>> &appearances_matrix, const int file_index);

private:
    void initializeVolumeCalculations(const vector<vector<bool>> &appearances_matrix,
                                      const set<int> &initial_files, const set<int> &final_files,
                                      const set<int> &added_files, const set<int> &removed_files,
                                      const map<int, int>& block_to_size, const bool use_cache, const string& cache_path);

private:
    static std::string getMemoryCacheKey(const std::string& vol_name, const std::set<int>& init_files,
                                                  const std::set<int>& final_files, const set<int> &added_files, const set<int> &removed_files);
private:
    static map<string, VolumeCalcInfo> _cache;

private:
    const string m_volume_name;
    long long int m_received_bytes;
    long long int m_num_bytes_deleted;
    long long int m_init_volume_size;
    long long int m_overlap_mig_changes_traffic;
    long long int m_only_mig_traffic;
    long long int m_mig_reuse_blocks_spare_traffic_bytes;
    long long int m_migration_aborted_traffic;
};
