#pragma once

#include "Lock.hpp"

#include <sqlite3.h>
#include <string>
#include <vector>
#include <memory>
#include <tuple>
#include <functional>

using namespace std;

using CallbackFunc = int (*)(void*,int,char**,char**);

class Cache final {
public:
    struct SystemCacheLine final{
        SystemCacheLine() :
        initial_system_size(-1),
        num_bytes_deleted(0),
        traffic_bytes(0)
        {
        }

        long long int initial_system_size;
        long long int num_bytes_deleted;
        long long int traffic_bytes;
        long long int receive_bytes;
        long long int overlap_traffic;
        long long int block_reuse;
        long long int aborted_traffic;

        vector<long long int> initial_volumes_sizes;
        vector<long long int> volumes_deletion;
        vector<long long int> volumes_traffic;
        vector<long long int> volumes_receive_bytes;
        vector<long long int> volumes_overlap_traffic;
        vector<long long int> volumes_block_reuse;
        vector<long long int> volumes_aborted_traffic;
    };

    struct VolCacheLine final{
        VolCacheLine() :
                initial_vol_size(-1),
                vol_num_bytes_deleted(0),
                vol_num_bytes_received(0)
        {
        }

        long long int initial_vol_size;
        long long int vol_num_bytes_deleted;
        long long int vol_num_bytes_received;
    };

public:
    explicit Cache(const string& cache_path);

public:
    /**
     * inserts a record to our calculator cache table
     * @param volumes - the hash of the volumes
     * @param init - the hash of the initial files
     * @param final - the hash of the final files
     * @param init_sys_size - the initial size of the system in bytes
     * @param init_vol_sizes - the initial sizes of each volume in bytes
     * @param vol_traffics - the resulted traffic sizes of each volume in bytes
     * @param vol_deletions - the resulted deletion sizes of each volume in bytes
     */
    void insertSystemResult(const string& volumes, const string& init, const string& final, long long int init_sys_size,
                            const vector<long long int> &init_vol_sizes,
                            const vector<long long int> &vol_traffics,
                            const vector<long long int> &vol_deletions,
                            const vector<long long int> &vol_receive_bytes,
                            const vector<long long int> &vol_overlap_traffic,
                            const vector<long long int> &vol_block_reuse,
                            const vector<long long int> &vol_aborted_traffic);

    void insertVolumeResult(const string& volume, const string& init, const string& final,
                                   long long int init_vol_size, long long int vol_traffic, long long int vol_deletion);

    /**
     * retrieves the result from the database, based on the input
     * @param volumes - the hash of the volumes
     * @param init - the hash of the initial files
     * @param final - the hash of the final files
     */
    SystemCacheLine getSystemResult(const string& volumes, const string& init, const string& final);

    VolCacheLine getVolumeResult(const string& volume, const string& init, const string& final);

    /**
     * run a sql query on the current db
     * @param sql_query - the sql query
     * @param callback - the result callback function
     * @param callback_params - params for the callback function
     */
    void runQuery(const string& sql_query, CallbackFunc callback, void* callback_params);

    /**
     * creates our cache tables in the given sql object
     * @param db - unique ptr to sqlite3 object
     */
    void createTables(const unique_ptr<sqlite3, function<void (sqlite3*)>>& db);

public:
    /**
     * inserts a record to our calculator cache table
     * @param cache_path - path to the cache db file
     * @param volumes - the hash of the volumes
     * @param init - the hash of the initial files
     * @param final - the hash of the final files
     * @param init_sys_size - the initial size of the system in bytes
     * @param init_vol_sizes - the initial sizes of each volume in bytes
     * @param vol_traffics - the resulted traffic sizes of each volume in bytes
     * @param vol_deletions - the resulted deletion sizes of each volume in bytes
     */
    static void insertSystemResult(const string& cache_path, const string& volumes, const string& init, const string& final,
                                   long long int init_sys_size, const vector<long long int> &init_vol_sizes,
                                   const vector<long long int> &vol_traffics,
                                   const vector<long long int> &vol_deletions,const vector<long long int> &vol_receive_bytes,
                                   const vector<long long int> &vol_overlap_traffic,
                                   const vector<long long int> &vol_block_reuse,
                                   const vector<long long int> &vol_aborted_traffic);

    static void insertVolumeResult(const string& cache_path, const string& volume, const string& init, const string& final,
                                          long long int init_vol_size, long long int vol_traffic, long long int vol_deletion);
    /**
     * retrieves the result from the database, based on the input
     * @param cache_path - path to the cache db file
     * @param volumes - the hash of the volumes
     * @param init - the hash of the initial files
     * @param final - the hash of the final files
     * @return a SystemCacheLine object which represent the stored values for the given keys
     */
    static SystemCacheLine getSystemResult(const string& cache_path, const string& volumes, const string& init, const string& final);
    static VolCacheLine getVolumeResult(const string& cache_path, const string& volume, const string& init, const string& final);
private:
    /**
     * creates a db with our calculator cache table in the given path
     * @param cache_path - a path to the cache db file
     * @param lock - a lock for the cache db file
     * @return unique ptr to sqlite3 object
     */
    static unique_ptr<sqlite3, function<void (sqlite3*)>> createDB(const string& cache_path, Lock& lock);

    /**
     * calllback for the select query, this is applied on each row returned from the select query in order to fill
     * the given cacheLine (data) with the specific cache values
     * @param data - a pointer to cache line object
     * @param argc - num of table fields
     * @param argv - array of fields value
     * @param azColName - array of fields name
     * @return EXIT_SUCCESS on success, otherwise returns EXIT_FAILURE/throws an exception
     */
    static int system_callback(void *data, int argc, char **argv, char **azColName);

    static int volume_callback(void *data, int argc, char **argv, char **azColName);

    /**
     * run a sql query on the given db
     * @param cache - a Cache object
     * @param sql_query - the sql query
     * @param callback - the result callback function
     * @param callback_params - params for the callback function
     */
    static void runQuery(Cache& cache, const string& sql_query, CallbackFunc callback, void* callback_params);

    /**
     * @return string of all the volume specific headers
     */
    static string createVolumesSpecificHeaders(const bool is_with_types= false);

    /**
    * @return string of volume specific headers
    */
    static string createVolumeSpecificHeaders(const bool is_with_types= false, const unsigned int index=1);

    /**
     * @return string of all the volume specific headers' values
     */
    static string getSpecificVolumesValues(const vector<long long int> &init_vol_sizes,
                                           const vector<long long int> &vol_traffics,
                                           const vector<long long int> &vol_deletions,
                                           const vector<long long int> &vol_receive_bytes,
                                           const vector<long long int> &vol_overlap_traffic,
                                           const vector<long long int> &vol_block_reuse,
                                           const vector<long long int> &vol_aborted_traffic);

    static string getSpecificVolumeValues(long long int init_vol_size, long long int vol_traffic,
                                          long long int vol_deletion);
private:
    Lock m_lock;
    unique_ptr<sqlite3, function<void (sqlite3*)>> m_db;
};
