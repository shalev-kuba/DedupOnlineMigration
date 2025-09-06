# Greedy

This subproject implements the Greedy-based online deduplicated migration algorithms presented in our paper:  
*“Let It Slide: Online Deduplicated Data Migration.”* SYSTOR ’25: Proceedings of the 18th ACM International Systems and Storage Conference  
[https://dl.acm.org/doi/10.1145/3757347.3759144](https://dl.acm.org/doi/10.1145/3757347.3759144)

This implementation is based on a fork of the greedy deduplicated migration code introduced in:  
*“The What, The From, and The To: The Migration Games in Deduplicated Systems.”*  
20th USENIX Conference on File and Storage Technologies (FAST ’22), 2022  
<https://www.usenix.org/conference/fast22/presentation/kisous>

Original implementation on GitHub:  
<https://github.com/roei217/DedupMigration/tree/master/Greedy>
----


## Prerequisites
- Linux machine (tested on Ubuntu 22.04)
- CMake ≥ 3.10
- GCC ≥ 5 (for C++14)
- libboost
- Workloads and changes list created using the SystemsAndChangesCreation subproject
----

## Build steps
```shell
$ git clone https://github.com/shalev-kuba/DedupOnlineMigration.git
$ cd DedupOnlineMigration/OnlineAlgorithms/Greedy/
$ make
```
After the make command ends, `GreedyLoadBalancerUnited` binary should appear in your directory.
----

## Running Greedy-based online migration algorithms
In this subproject, the presented online migration approaches use different names.
The following is a mapping from the project’s terminology to the paper’s terminology:
* migration_after_changes -> post
* migration_before_changes ->pre
* migration_with_continuous_changes -> multiple
* naive_split -> space-split
* lb_split -> balance-split
* smart_split -> slide
* only_changes -> none
----

### Using hc command line directly

1. > ./GreedyLoadBalancerUnited {volumelist} {output} {summaryFile} {timelimit} {Traffic} {margin} {num_migration_iters} {num_changes_iters} {seed} {changes_file} {file_index} {change_pos} {total_perc_changes} {change_insert_type} {num_runs}
    1) Volumelist: volume List section
    2) Output: the path to the where the csv migration plan is to be written.
    3) summaryFile: path to the summary file.
    4) TimeLimit: after how many seconds should the algorithm stop
    5) Traffic: what is the maximum amount of traffic allowed for the algorithm (0-100, representing percentage)
    6) Margin: the error margin allowed from the desired balance. If this margin is 1 or more, the code would run without load balancing. (0-1, representing fraction)
    7) num_migration_iters: num of epochs in a migration window that contains migration
    8) num_changes_iters: num of epochs in a migration window that contains system changes
    9) seed: seed for volume selection for random file insertion as part of system changes. 
    10) changes_file: path to file that contains changes list (as outputted by SystemsAndChangesCreation/TraceParser/scripts/volumes_split_creator.py)
    11) file_index: path to file that index of files (as outputted by SystemsAndChangesCreation/TraceParser/scripts/volumes_split_creator.py with the flag of --output_filter_snaps_path)
    12) change_pos: Online algorithm to simulate. (previously called Change pos). that should be one of: migration_after_changes (post),migration_before_changes (pre), migration_with_continuous_changes (multiple), naive_split (space-split), lb_split (balance-split), smart_split (slide), or only_changes
    13) total_perc_changes: num of changes as perc from num files in system
    14) change_insert_type: changes insert type. options are random/backup
    15) num_runs: num of running the experiment one after another
----

### Using helper script
1. Set config to system. For example, such file should be in the following format:
    ```json
    {
      "mask_to_volumes_names": {
        "k13":[
          "./filtered/vol_0.out.system",
          "./filtered/vol_1.out.system",
          "./filtered/vol_2.out.system",
          "./filtered/vol_3.out.system",
          "./filtered/vol_4.out.system"
        ],
        "no_mask": [
          "./vol_0.out.system",
          "./vol_1.out.system",
          "./vol_2.out.system",
          "./vol_3.out.system",
          "./vol_4.out.system"
        ]
      },
      "fps_size": "all"
    }
    ```
2. Each line inside "k13" means a filtered volume (only blocks with 13 leading zero in their hash). This is the filter we ran our experiments on. The "no_mask" means unfiltered volumes.
3. Inside `run_exp.py` add your new system's configuration path to `workload_to_conf_map` map:
    ```python
    workload_to_conf_map = {
        "ubc150_5vols_by_user": "configs/conf_exp_ubc150_5vols_by_user.json",
    }
    ```
4. Inside `run_exp.py` edit `INDEX_FILE` to point to index file created by `volumes_split_creator.py` with the flag `--output_filter_snaps_path`
5. Then, run the experiment script (`run_greedy_exp.py`):
   ```shell
   $ python3 run_greedy_exp.py --help
   usage: run_greedy_exp.py [-h] [--mask MASK] [--greedy_path GREEDY_PATH]
   [--traffics TRAFFICS [TRAFFICS ...]] [--time_limit TIME_LIMIT]
   [--margin MARGIN] [--num_iterations NUM_ITERATIONS]
   [--num_changes_iterations NUM_CHANGES_ITERATIONS]
   [--workload WORKLOAD] [--seed_list SEED_LIST [SEED_LIST ...]]
   [--changes_list_seeds CHANGES_LIST_SEEDS [CHANGES_LIST_SEEDS ...]]
   [--change_pos_list CHANGE_POS_LIST [CHANGE_POS_LIST ...]]
   [--change_perc CHANGE_PERC] [--num_of_runs NUM_OF_RUNS]
   [--is_filtered_changes IS_FILTERED_CHANGES]
   [--change_prefix CHANGE_PREFIX]
   [--change_insert_type CHANGE_INSERT_TYPE]
   
   Runs greedy's experiments in parallel
   
   optional arguments:
   -h, --help            show this help message and exit
   --mask MASK           which mask to use, for example "k13". default=no_mask
   --greedy_path GREEDY_PATH
   greedy binary location. default is binary named
   GreedyLoadBalancerUnited in the current working
   directory
   --traffics TRAFFICS [TRAFFICS ...]
   traffics for hc. default is20.0 40.0 60.0 80.0 100.0
   --time_limit TIME_LIMIT
   time limit forgreedy. default is 100000
   --margin MARGIN       desired margin for greedy, default is 0.05
   --num_iterations NUM_ITERATIONS
   num of incremental iterations, default is 1
   --num_changes_iterations NUM_CHANGES_ITERATIONS
   num of incremental iterations of changes, default is 1
   --workload WORKLOAD   a workload name to run greedy on. one from the
   following - ubc150_5vols_by_user
   --seed_list SEED_LIST [SEED_LIST ...]
   list of seeds
   --changes_list_seeds CHANGES_LIST_SEEDS [CHANGES_LIST_SEEDS ...]
   list of change list seeds
   --change_pos_list CHANGE_POS_LIST [CHANGE_POS_LIST ...]
   where to do changes? only_changes/migration_after_chan
   ges/migration_with_continuous_changes/migration_before
   _changes/(other means no changes at all) List means
   different jobs
   --change_perc CHANGE_PERC
   num of changes to do as percentages from number of
   files in system
   --num_of_runs NUM_OF_RUNS
   num of times to run experiment one after the other
   --is_filtered_changes IS_FILTERED_CHANGES
   is changes are taken from filtered parsed traces
   --change_prefix CHANGE_PREFIX
   prefix of change that represent the change type file
   to take
   filter_backup/filter_add/filter_add_rem/add/add_rem
   --change_insert_type CHANGE_INSERT_TYPE
   change insert type random/backup
   ```

----

## Output
1. **Migration plan results.**  
   Example: `ubc150_5vols_by_user_T40.0_CPmigration_after_changes_CPR10_S8080_CT_filter_backup_CLS123_NR2_migration_plan.csv`
   1. `ubc150_5vols_by_user` is the workload
   2. `T40.0` is the allocated traffic
   3. `CPmigration_after_changes` is the online algorithm that was used
   4. `CPR10` is the change percentages
   5. `S8080` is the seed for volume insertion
   6. `CT_filter_backup` is the change type
   7. `CLS123` is the change list seed
   8. `NR2` is the num of consecutive migration windows in the experiment (here is 2 windows) 

----
### Aggregated results
Results from multiple experiments can be combined into a single CSV using `greedy_union_result.py`.

- By editing the constants at the start of `greedy_union_result.py`, you can choose which results to include.
- Running
  ```shell
  python greedy_union_result.py
  ```
  produces an aggregated file named summarized_results_with_iterations.csv.
- This aggregated file can then be passed to
  Experiment/Graph-generator/LetItSlide-graph_generator.ipynb
  to generate graphs similar to those shown in the papers.
