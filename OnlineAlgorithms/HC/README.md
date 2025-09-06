# Clustering-Based Online Deduplicated Migration

This subproject implements the HC-based online deduplicated migration algorithms presented in our paper:  
*“Let It Slide: Online Deduplicated Data Migration.”* SYSTOR ’25: Proceedings of the 18th ACM International Systems and Storage Conference  
[https://dl.acm.org/doi/10.1145/3757347.3759144](https://dl.acm.org/doi/10.1145/3757347.3759144)

This implementation is based on a fork of the original clustering-based deduplicated migration code introduced in:  
*“The What, The From, and The To: The Migration Games in Deduplicated Systems.”*  
20th USENIX Conference on File and Storage Technologies (FAST ’22), 2022  
<https://www.usenix.org/conference/fast22/presentation/kisous>

Original implementation on GitHub:  
<https://github.com/roei217/DedupMigration/tree/master/Clustering>

## Prerequisites
- Linux machine (tested on Ubuntu 22.04)
- Python 3.6+
- CMake ≥ 3.10
- GCC ≥ 5 (for C++14)
- `sqlite3` and `libsqlite3-dev`
- Workloads and changes list created using the SystemsAndChangesCreation subproject 

## Build steps
```shell
$ git clone https://github.com/shalev-kuba/DedupOnlineMigration.git
$ cd DedupOnlineMigration/OnlineAlgorithms/HC/
$ mkdir Build/ && cd Build
$ cmake .. && make
```

After the make command ends, `hc` binary should appear in your Build directory (DedupOnlineMigration/OnlineAlgorithms/HC/Build/hc).

----

## Running HC-based online migration algorithms
In this subproject, the presented online migration approaches use different names.
The following is a mapping from the project’s terminology to the paper’s terminology:
* migration_after_changes -> post
* migration_before_changes ->pre
* migration_with_continuous_changes -> multiple
* naive_split -> space-split
* lb_split -> balance-split
* smart_split -> slide
* only_changes -> none

### Using hc command line directly
```shell
$ ./hc --help
[USAGE]:
./hc -workloads(TYPE=STRING - VARIABLE LENGTH LIST) -fps(TYPE=STRING*1) -traffic(TYPE=INT - VARIABLE LENGTH LIST) [-wt_list(TYPE=INT - VARIABLE LENGTH LIST)] [-lb] [-converge_margin] [-use_new_dist_metric] [-carry_traffic] -seed(TYPE=INT - VARIABLE LENGTH LIST) -gap(TYPE=DOUBLE - VARIABLE LENGTH LIST) [-lb_sizes(TYPE=DOUBLE - VARIABLE LENGTH LIST)] [-eps(TYPE=INT*1)] [-output_path_prefix(TYPE=STRING - VARIABLE LENGTH LIST)] [-result_sort_order(TYPE=STRING - VARIABLE LENGTH LIST)] [-no_cache] [-cache_path(TYPE=STRING*1)] [-num_iterations(TYPE=INT*1)] [-num_changes_iterations(TYPE=INT*1)] [-changes_input_file(TYPE=STRING*1)] -change_pos(TYPE=STRING*1) [-changes_seed(TYPE=INT*1)] [-changes_perc(TYPE=INT*1)] [-num_runs(TYPE=INT*1)] [-files_index_path(TYPE=STRING*1)] [-changes_insert_type(TYPE=STRING*1)] [-split_sort_order(TYPE=STRING*1)]

PARAMS DESCRIPTION:
	-workloads:  workloads to load, list of strings
	-fps: number of min hash fingerprints (input 'all' for all fps) - int or 'all'
	-traffic: traffic - list of int
	-wt_list: list of W_T to try - list of int, default is 0, 20, 40, 60, 100
	-lb: use load balancing (optional, default false)
	-converge_margin: use converging margin (optional, default false)
	-use_new_dist_metric: use the new dist metric (optional, default false)
	-carry_traffic: carry traffic across epochs (optional, default false)
	-seed: seeds for the algorithm, list of int
	-gap: pick random results value from result values in this gap - list of double
	-lb_sizes: a list of clusters' requested sizes - list of int, must sum to 100 (optional, default is even distribution)
	-eps: % to add to system's initial size at every iteration - int (mandatory if -lb is used, default is 5)
	-output_path_prefix: prefix of output path (optional, default is results/result in the working directory)
	-result_sort_order: sort order for the best result selection. use the following literals: (traffic_valid, lb_valid, deletion, lb_score, traffic). The default sort order is 'traffic_valid lb_valid deletion lb_score traffic'
	-no_cache: flag for not using cache to calculate cost
	-cache_path: path to cost's cache (default is cache.db)
	-num_iterations: num of epochs in a migration window (default=1 epoch)
	-num_changes_iterations: num of epochs with system changes in a migration window, default is 0
	-changes_input_file: changes input file, default is ""
	-change_pos: Online algorithm to simulate. (previously called Change pos). that should be one of: migration_after_changes (post),migration_before_changes (pre), migration_with_continuous_changes (multiple), naive_split (space-split), lb_split (balance-split), smart_split (slide), or only_changes
	-changes_seed: changes seed, default is 22
	-changes_perc: num of changes as perc from num files in system, default is 0%
	-num_runs: num of running the experiment one after another
	-files_index_path: path to files index (output file of volumes_split_creator.py with the flag of --output_filter_snaps_path)
	-changes_insert_type: changes insert type, default is random. options are random/backup
	-split_sort_order: split transfer sort order, default is hard_deletion. options are hard_deletion, soft_deletion, hard_lb and soft_lb. hard_lb is the option used for Slide and Balance split

Got exception: ERROR: The param -workloads is missing
```
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
        "ubc150_5vols_by_random": "configs/conf_exp_ubc150_5vols_by_random.json",
        "ubc150_5vols_by_date": "configs/conf_exp_ubc150_5vols_by_date.json",
    }
    ```

4. Then, run the experiment script:
    ```shell
    $ python3 run_exp.py --help
    usage: run_exp.py [-h] [--load_balance] [--dont_cache] [--mask MASK]
                   [--hc_path HC_PATH] [--lb_sizes LB_SIZES [LB_SIZES ...]]
                   [--traffics TRAFFICS [TRAFFICS ...]] [--gaps GAPS [GAPS ...]]
                   [--seeds SEEDS [SEEDS ...]]
                   [--result_sort_order RESULT_SORT_ORDER] [--eps EPS]
                   [--margin MARGIN] [--num_iterations NUM_ITERATIONS]
                   [--workload WORKLOAD]
                   [--changes_seeds CHANGES_SEEDS [CHANGES_SEEDS ...]]
                   [--change_type CHANGE_TYPE] [--change_pos CHANGE_POS]
                   [--num_changes_iterations NUM_CHANGES_ITERATIONS]
                   [--changes_perc CHANGES_PERC]
                   [--is_filtered_changes IS_FILTERED_CHANGES]
                   [--changes_insert_type CHANGES_INSERT_TYPE]
                   [--num_of_runs NUM_OF_RUNS]
                   [--changes_list_seeds CHANGES_LIST_SEEDS [CHANGES_LIST_SEEDS ...]]
                   [--files_index_path FILES_INDEX_PATH]
                   [--split_sort_order SPLIT_SORT_ORDER] [--converge_margin]
                   [--new_dist_metric] [--carry_traffic]
    
    Runs hc's experiments in parallel
    
    optional arguments:
      -h, --help            show this help message and exit
      --load_balance        Used to enable load balancing feature
      --dont_cache          whether or not to use cache
      --mask MASK           which mask to use, for example "k13". default=no_mask
      --hc_path HC_PATH     hc location. default is binary named hc in the current
                            working directory
      --lb_sizes LB_SIZES [LB_SIZES ...]
                            lb sizes
      --traffics TRAFFICS [TRAFFICS ...]
                            traffics for hc. default is20 40 60 80 100
      --gaps GAPS [GAPS ...]
                            gaps for hc. default is0.5 1.0 3.0
      --seeds SEEDS [SEEDS ...]
                            seeds for hc. default is0 37 41 58 99 111 185 199 523
                            666
      --result_sort_order RESULT_SORT_ORDER
                            sort order for the best result. use the following
                            literals (space seperated): (traffic_valid, lb_valid,
                            deletion, lb_score, traffic).
      --eps EPS             eps for hc, default is 30
      --margin MARGIN       desired margin for hc, default is 5
      --num_iterations NUM_ITERATIONS
                            num of epochs in a window, default is 1
      --workload WORKLOAD   a workload name to run hc on. one from the following -
                            ubc150_5vols_by_user, ubc150_5vols_by_random,
                            ubc150_5vols_by_date
      --changes_seeds CHANGES_SEEDS [CHANGES_SEEDS ...]
                            seeds for changes steps. default is22 80 443 8080
      --change_type CHANGE_TYPE
                            change type for migration. Options:
                            filter_add/filter_add_rem filter_backup default is
                            filter_add
      --change_pos CHANGE_POS
                            Online migration algorithm to use (previosuly called
                            change position for migration). Options: only_changes/
                            migration_after_changes/migration_before_changes/migra
                            tion_with_continuous_changes/smart_split/naive_split/l
                            b_split default is only_changes
      --num_changes_iterations NUM_CHANGES_ITERATIONS
                            num of epochs contains changes, default is 4
      --changes_perc CHANGES_PERC
                            num of changes as perc from system size, default is 10
                            perc
      --is_filtered_changes IS_FILTERED_CHANGES
                            is changes are taken from filtered parsed traces
      --changes_insert_type CHANGES_INSERT_TYPE
                            insert type random/backup
      --num_of_runs NUM_OF_RUNS
                            num of times to continue run the experiment one after
                            another. The final state of experiment becomes the
                            initial state for the next run
      --changes_list_seeds CHANGES_LIST_SEEDS [CHANGES_LIST_SEEDS ...]
                            seeds for changes list. default is1010 999 7192
      --files_index_path FILES_INDEX_PATH
                            path to index json file. default is
                            filtered_indexed_input_files.json. (output file of
                            volumes_split_creator.py with the flag of
                            --output_filter_snaps_path)
      --split_sort_order SPLIT_SORT_ORDER
                            split sort order: hard_deletion, soft_deletion,
                            soft_lb, hard_lb. default is soft_lb (hard_lb used for Slide
                            and Balance-split)
      --converge_margin     Used to enable load balancing converging
      --new_dist_metric     Used to enable new dist metric feature
      --carry_traffic       Used to enabl carry traffic feature
    ```
5. Example command (balance-split lb_split, 2 consecutive windows, 4 epochs each, ubc750 dataset, 10 volumes, 40% budget):
    ```shell
    python3 ./run_exp.py --num_of_runs 2 --workload ubc75_10vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 4 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 7979 2222 7192 1010 999 --change_pos lb_split --changes_perc 10 --change_type filter_backup --changes_insert_type backup  --split_sort_order hard_lb
    ```
----

## Output
1. **Per-epoch results.**  
   Example: `ubc150_5vols_by_user_T80.00_I8_m8_c8_WT60.00_S58.00_G3.00_E30.00_M5.00_wc_lb_V.csv`
   - `ubc150_5vols_by_user` → workload name
   - `80.00` → allocated traffic as a percentage of the initial system size
   - `I8_m8_c8` → epoch number 8, with 8 epochs containing migrations (`m8`) and 8 epochs containing changes (`c8`)
   - `WT60.00_S58.00_G3.00_E30.00_M5.00` → clustering parameters for this run
   - `wc` → results measured after changes occur, without is results as calculated before changes occur
   - `lb_V` → load balancing constraint enabled, with `V` indicating a valid result (`NV` means not valid)

2. **Best iteration results.**  
   Example: `ubc150_5vols_by_user_T40.00_best_iter_2_m2_c2.csv`  
   This file contains the best result of epoch number 2.

3. **Migration plan results.**  
   Example: `ubc150_5vols_by_user_T80.00_migration_plan.csv`
----
### Aggregated results

Results from multiple experiments can be combined into a single CSV using `hc_union_result.py`.

- By editing the constants at the start of `hc_union_result.py`, you can choose which results to include.
- Running
  ```shell
  python hc_union_result.py
  ```
  produces an aggregated file named summarized_results_with_iterations.csv.
- This aggregated file can then be passed to
  Experiment/Graph-generator/LetItSlide-graph_generator.ipynb
  to generate graphs similar to those shown in the papers.
