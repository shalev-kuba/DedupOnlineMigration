# Experiments

## Experiment parameters

### Change list seed
We used the following change list seeds in our experiment: `1010, 7192, 999, 123, 0, 2222`

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
