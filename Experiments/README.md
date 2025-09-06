# Experiments

## Experiment parameters

### System creation
We created a systems with the following configurations (All experiments ran over filtered systems):
- **Users-750** 
  - divide_method = **by_user** 
  - 150 files per volume
  - 5 volumes
  - Don't balance system
  - 0% noise percentages

- **Users-750-10vols**
    - divide_method = **by_user**
    - 75 files per volume
    - 10 volumes
    - Don't balance system
    - 0% noise percentages

- **Random**
    - divide_method = **by_user**
    - 150 files per volume
    - 5 volumes
    - Don't balance system
    - 100% noise percentages


### Change list seed
We used the following change list seeds in our experiment: `1010, 7192, 999, 123, 0, 2222`

- In Backup (within the `volumes_split_creator.py` script)
  - `BAK_NUM_HOSTS_PERC` is equal no change rate percentages of that experiment.
  - `BAK_NUM_CHANGES_ITERS` is equal to number of epochs in a window
  - `BAK_NUM_RUNS` is equal to number consecutive windows in the experiment

### Volume insertion seed
For selecting volume to insert changes to in the random type change stream, we used the following seeds: `22, 80, 443, 8080`

### HC params
We used the following HC params in our HC-based online deduplicated migration: 
```shell
WT_TRAFFICS = [20, 40, 60, 80, 100]
GAPS = [0.5, 1.0, 3.0]
SEEDS = [0, 37, 41, 58, 99, 111, 185, 199, 523, 666]
```

---

## Graph Generation

To generate the graphs shown in the paper, we used the Jupyter notebook  
`Experiments/Graph-generator/LetItSlide-graph_generator.ipynb`.

We added a `Consts` section to this notebook. In particular, please edit the following constants:

1. `BASE_DIR_FOR_ALL` — path where the generated graphs will be saved
2. `CSV_NAME` — path to the aggregated results CSV file (output of the `*_union_result.py` scripts)

---

### Notes
* In section 2 (*"2-base, 8 epochs (2 runs, 40%), metrics per epoch"*), we manually added the initial system values so that the per-epoch view starts at epoch 0.
* Each section begins with constants that can be adjusted to your needs.
* In our experiments, we created a unified CSV file that contains both `hc` and `greedy` results.  

## Experiments run commands
Here are the run command to reproduce the first 4 experiments of the paper.
The rest can be reproduced similarly:
```shell
#-------------
# Base experiment run 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 1 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos only_changes --changes_perc 10 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 1 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_after_changes --changes_perc 10 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 1 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_before_changes --changes_perc 10 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 4 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_with_continuous_changes --changes_perc 10 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 4 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos naive_split --changes_perc 10 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 4 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos smart_split --changes_perc 10 --change_type filter_backup --changes_insert_type backup  --split_sort_order hard_lb 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 4 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos lb_split --changes_perc 10 --change_type filter_backup --changes_insert_type backup  --split_sort_order hard_lb 
#-------------

#-------------
# Experiment of different traffic budgets 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 20 60 80 100 --mask k13 --num_iterations 1 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos only_changes --changes_perc 10 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 20 60 80 100 --mask k13 --num_iterations 1 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_after_changes --changes_perc 10 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 20 60 80 100 --mask k13 --num_iterations 1 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_before_changes --changes_perc 10 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 20 60 80 100 --mask k13 --num_iterations 4 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_with_continuous_changes --changes_perc 10 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 20 60 80 100 --mask k13 --num_iterations 4 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos naive_split --changes_perc 10 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 20 60 80 100 --mask k13 --num_iterations 4 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos smart_split --changes_perc 10 --change_type filter_backup --changes_insert_type backup  --split_sort_order hard_lb 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 20 60 80 100 --mask k13 --num_iterations 4 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos lb_split --changes_perc 10 --change_type filter_backup --changes_insert_type backup  --split_sort_order hard_lb 
#-------------

#-------------
# Experiment of different change rates

## 15% change rate
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 1 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos only_changes --changes_perc 15 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 1 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_after_changes --changes_perc 15 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 1 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_before_changes --changes_perc 15 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 4 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_with_continuous_changes --changes_perc 15 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 4 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos naive_split --changes_perc 15 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 4 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos smart_split --changes_perc 15 --change_type filter_backup --changes_insert_type backup  --split_sort_order hard_lb 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 4 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos lb_split --changes_perc 15 --change_type filter_backup --changes_insert_type backup  --split_sort_order hard_lb 

## 20% change rate
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 1 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos only_changes --changes_perc 20 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 1 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_after_changes --changes_perc 20 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 1 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_before_changes --changes_perc 20 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 4 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_with_continuous_changes --changes_perc 20 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 4 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos naive_split --changes_perc 20 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 4 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos smart_split --changes_perc 20 --change_type filter_backup --changes_insert_type backup  --split_sort_order hard_lb 
nohup python3 ./run_exp.py --num_of_runs 2 --workload ubc150_5vols_by_user --load_balance --traffics 40 --mask k13 --num_iterations 4 --num_changes_iterations 4 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos lb_split --changes_perc 20 --change_type filter_backup --changes_insert_type backup  --split_sort_order hard_lb 
#-------------

#-------------
# Different window size experiment

## Basic experiment already covers window of 4 epochs over 2 runs
 
## window of 8 epochs (1 run)
nohup python3 ./run_exp.py --num_of_runs 1 --workload ubc150_5vols_by_user --load_balance --traffics 80 --mask k13 --num_iterations 1 --num_changes_iterations 8 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos only_changes --changes_perc 20 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 1 --workload ubc150_5vols_by_user --load_balance --traffics 80 --mask k13 --num_iterations 1 --num_changes_iterations 8 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_after_changes --changes_perc 20 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 1 --workload ubc150_5vols_by_user --load_balance --traffics 80 --mask k13 --num_iterations 1 --num_changes_iterations 8 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_before_changes --changes_perc 20 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 1 --workload ubc150_5vols_by_user --load_balance --traffics 80 --mask k13 --num_iterations 8 --num_changes_iterations 8 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_with_continuous_changes --changes_perc 20 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 1 --workload ubc150_5vols_by_user --load_balance --traffics 80 --mask k13 --num_iterations 8 --num_changes_iterations 8 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos naive_split --changes_perc 20 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 1 --workload ubc150_5vols_by_user --load_balance --traffics 80 --mask k13 --num_iterations 8 --num_changes_iterations 8 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos smart_split --changes_perc 20 --change_type filter_backup --changes_insert_type backup  --split_sort_order hard_lb 
nohup python3 ./run_exp.py --num_of_runs 1 --workload ubc150_5vols_by_user --load_balance --traffics 80 --mask k13 --num_iterations 8 --num_changes_iterations 8 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos lb_split --changes_perc 20 --change_type filter_backup --changes_insert_type backup  --split_sort_order hard_lb 


## window of 2 epochs (4 run)
nohup python3 ./run_exp.py --num_of_runs 4 --workload ubc150_5vols_by_user --load_balance --traffics 20 --mask k13 --num_iterations 1 --num_changes_iterations 2 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos only_changes --changes_perc 5 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 4 --workload ubc150_5vols_by_user --load_balance --traffics 20 --mask k13 --num_iterations 1 --num_changes_iterations 2 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_after_changes --changes_perc 5 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 4 --workload ubc150_5vols_by_user --load_balance --traffics 20 --mask k13 --num_iterations 1 --num_changes_iterations 2 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_before_changes --changes_perc 5 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 4 --workload ubc150_5vols_by_user --load_balance --traffics 20 --mask k13 --num_iterations 2 --num_changes_iterations 2 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos migration_with_continuous_changes --changes_perc 5 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 4 --workload ubc150_5vols_by_user --load_balance --traffics 20 --mask k13 --num_iterations 2 --num_changes_iterations 2 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos naive_split --changes_perc 5 --change_type filter_backup --changes_insert_type backup 
nohup python3 ./run_exp.py --num_of_runs 4 --workload ubc150_5vols_by_user --load_balance --traffics 20 --mask k13 --num_iterations 2 --num_changes_iterations 2 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos smart_split --changes_perc 5 --change_type filter_backup --changes_insert_type backup  --split_sort_order hard_lb 
nohup python3 ./run_exp.py --num_of_runs 4 --workload ubc150_5vols_by_user --load_balance --traffics 20 --mask k13 --num_iterations 2 --num_changes_iterations 2 --changes_seeds 8080 --changes_list_seed 0 123 2222 7192 1010 999 --change_pos lb_split --changes_perc 5 --change_type filter_backup --changes_insert_type backup  --split_sort_order hard_lb 
#-------------

```