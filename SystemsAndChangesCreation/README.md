# Creation of systems and change streams

This subproject implements the creation of the UBC-based workloads and the change streams used in the paper:  
*“Let It Slide: Online Deduplicated Data Migration.”* SYSTOR ’25: Proceedings of the 18th ACM International Systems and Storage Conference  
[https://dl.acm.org/doi/10.1145/3757347.3759144](https://dl.acm.org/doi/10.1145/3757347.3759144)

- Authors: Shalev Kuba and Gala Yadgar, Technion – Israel Institute of Technology
- Email: s.kuba@campus.technion.ac.il

---

## Prerequisites
- Linux machine (tested on Ubuntu 22.04)
- Python 3.6+
- CMake ≥ 3.10
- GCC ≥ 5 (for C++14)
- UBC dedup dataset
  - Can be retrieved by visiting [UBC-dedup](https://iotta.snia.org/traces/static/3382?n=30&page=1)
  - Download the `64r` version
---
## Subproject targets
This subproject contains multiple targets that will be explained later:
- **trace_parser**: translate UBC-dedup raw files to our format - FSFiles
- **union_fs_files**: creates a voluem file by union list of FSFiles to one file in our format - VolumeFile
- **create_system**: takes multiple volumes in the VolumeFile format and outputs system volumes
  - Same blocks and files are referenced to the same id across all volumes
- **summary_system**: does the same as `create_system` but does not output volumes, but just outputs summary of system
details to the given file

---
## Build steps
```shell
$ git clone https://github.com/shalev-kuba/DedupOnlineMigration.git
$ cd DedupOnlineMigration/SystemsAndChangesCreation
$ mkdir Build/ && cd Build
$ cmake .. && make
```
All the binaries of subproject targets described above should now be available under the Build directory.

---


## Translate UBC-dedup files to out format
First, we want UBC-dedup files to be translated to our file formats.
To do so, we will use the `run_trace_parser.py` script (located under TraceParser/scripts):
```shell
$ python3 run_trace_parser.py --help
usage: run_trace_parser.py [-h] [--trace_parser_path TRACE_PARSER_PATH]
                           [--traces_dir_path INPUT_TRACES_PATH]
                           [--traces_out_dir_path OUT_TRACES_PATH]
                           [--num_process NUM_PROCESSES]

Runs trace parser

optional arguments:
  -h, --help            show this help message and exit
  --trace_parser_path TRACE_PARSER_PATH
                        trace_parser location. default is binary named
                        trace_parser in the current working directory
  --traces_dir_path INPUT_TRACES_PATH
                        path to the dir where the input traces are located
  --traces_out_dir_path OUT_TRACES_PATH
                        path to the dir where the outputed traces will be
  --num_process NUM_PROCESSES
                        num of parallel processes
```

After the script ends, under `OUT_TRACES_PATH` should be a directories' hierarchy with all trace files in our format - TraceFile
- Each first level directory is a user directory
- Each trace file is in the following format: `U1_H1_IF2688_TS03_10_2009_05_01`
  - `U1` - user with id=1
  - `H1` - host with id=1
  - `IF2688` - input file (raw UBD-dedup file) for creating that file is 2688
  - `TS03_10_2009_05_01` - timestamp of the time this snapshot was taken

---

## Generate additional synthetic files
In order to have sufficient number of files to run experiment over 2 migration windows of 4 epochs each, we had to synthesize more files.

We did that first by analyzing the UBC-dedup files using the `trace_analysis.py` and `traces_summary.py` scripts.

Then, after we noticed an averaged ~2.5% blocks deletion and ~3% blocks addition we generated new files based on the last files of each user.
For more details see the paper.

That generation was done using the `trace_generator.py`:
```shell
python3 ./trace_generator.py --help
usage: trace_generator.py [-h] [-i OLD_TRACE_PATH] [-o OUTPUT_PATH] [-a ADD_PERC] [-d DEL_PERC] [-u GENERATE_ALL_USERS] [-uf NUM_FILES_PER_USER] [-np NUM_PROCESSES] [-si SPECIFIC_INDEX]

This script generates a new parsed trace given an old parsed trace and generation params

options:
  -h, --help            show this help message and exit
  -i OLD_TRACE_PATH, --input_parsed_trace_path OLD_TRACE_PATH
                        A path to the old parsed trace
  -o OUTPUT_PATH, --output_parsed_trace_path OUTPUT_PATH
                        Output path for the new trace that will be generated. Default will be the input file with a suffix of _G<gen number>
  -a ADD_PERC, --block_addition_percentage ADD_PERC
                        Num of blocks to add given as percentages from the given old trace
  -d DEL_PERC, --block_deletion_percentage DEL_PERC
                        Num of blocks to remove given as percentages from the given old trace
  -u GENERATE_ALL_USERS, --generate-for-filtered_users GENERATE_ALL_USERS
                        Generate files for all filtered users Stated in "baseFormattedUbcFSFiles/filtered_indexed_input_files.json"
  -uf NUM_FILES_PER_USER, --num-files-per-user NUM_FILES_PER_USER
                        Generate files for all filtered users Stated in "baseFormattedUbcFSFiles/"
  -np NUM_PROCESSES, --num-processes NUM_PROCESSES
                        Num processes to allocate for the run
  -si SPECIFIC_INDEX, --specific-index SPECIFIC_INDEX
                        change filter input file if needed
```

For generating files to all users, an index of files is first necessary. We'll later see how to generate it.

---


## Generate system volumes specification
To generate different system specifications. we use the `volumes_split_creator.py` script (located under TraceParser/scripts).
```shell
python3 volumes_split_creator.py --help
usage: volumes_split_creator.py [-h] [--input_dir INPUT_DIR] [--filtered_parsed_traces FILTERED_PARSED_TRACES] [--out_dir OUT_DIR] [--divide_method DIVIDE_METHOD] [--num_volumes NUM_VOLUMES] [--balance_system BALANCE_SYSTEM]
                                [--num_files_per_volume NUM_FILES_PER_VOLUME] [--noise_percentages NOISE_PERCENTAGES] [--output_filter_snaps_path OUT_FILTERED_SNAPS] [--changes_output_dir CHANGES_OUTPUT_DIR]
                                [--changes_type CHANGES_TYPE] [--seed SEED] [--gen_depth_limit GEN_DEPTH_LIMIT] [--bak_num_hosts_perc BAK_NUM_HOSTS_PERC] [--bak_num_changes_iters BAK_NUM_CHANGES_ITERS] [--bak_num_runs BAK_NUM_RUNS]

Create an input files for UnionFsFiles, each file represent volume

options:
  -h, --help            show this help message and exit
  --input_dir INPUT_DIR
                        Path to the root directory of the parsed traces
  --filtered_parsed_traces FILTERED_PARSED_TRACES
                        Path to the root directory of the filtered parsed traces
  --out_dir OUT_DIR     Path to the root directory where the output will be written to
  --divide_method DIVIDE_METHOD
                        The volumes dividing method
  --num_volumes NUM_VOLUMES
                        num volumes to divide to
  --balance_system BALANCE_SYSTEM
                        balance volumes across system, default is False
  --num_files_per_volume NUM_FILES_PER_VOLUME
                        num files per volume
  --noise_percentages NOISE_PERCENTAGES
                        num of percentages of random files in each volumes (adds "noise")
  --output_filter_snaps_path OUT_FILTERED_SNAPS
                        If given, the script will just output the filtered snaps dict without creating a system (as called filtered_indexed_input_files.json)
  --changes_output_dir CHANGES_OUTPUT_DIR
                        path to the dir where output the changes
  --changes_type CHANGES_TYPE
                        changes type, currently support random_filter_add_rem/random_filter_add/backup
  --seed SEED           seed for the random
  --gen_depth_limit GEN_DEPTH_LIMIT
                        depth limit for gen files
  --bak_num_hosts_perc BAK_NUM_HOSTS_PERC
                        perc of num hosts to by participated in backup routine
  --bak_num_changes_iters BAK_NUM_CHANGES_ITERS
                        num of backup iters
  --bak_num_runs BAK_NUM_RUNS
                        num of runs
```

### Create files index:
To do so, run `volumes_split_creator.py` with the `--output_filter_snaps_path` flag 

---
## Creating system volumes
To create system volumes we'll use the `run_system_creation.py` script that automates this process (located under TraceParser/scripts).
```shell
$ python3 run_system_creation.py --help
usage: run_system_creation.py [-h] [--union_fs_files UNION_FS_FILES]
                              [--create_system_elf CREATE_SYSTEM_ELF]
                              [--input_dir INPUT_DIR]
                              [--num_process NUM_PROCESSES]
                              [--filter_on IS_FILTER_ON]
                              [--only_system_creation ONLY_SYSTEM_CREATION]

Runs trace parser

optional arguments:
  -h, --help            show this help message and exit
  --union_fs_files UNION_FS_FILES
                        Path to the union_fs_files binary
  --create_system_elf CREATE_SYSTEM_ELF
                        Path to the create_system binary
  --input_dir INPUT_DIR
                        Path to the dir where all the volumes input file are
                        at
  --num_process NUM_PROCESSES
                        num of parallel processes
  --filter_on IS_FILTER_ON
                        is filter enabled
  --only_system_creation ONLY_SYSTEM_CREATION
                        is only system creation (Volume already exist)
```

- Here, `INPUT_DIR` is a path to a directory where volumes configurations should be in files that ends with `.in` suffix.
Each of those files is a line separated of TraceFiles in that volumes.
- In our experiment we use `--filter_on`
- This process may take time, be sure to run it with `nohup &`
- Outputted system volumes will be in the same `INPUT_DIR` with `.system` suffix (under `filtered/` folder if `IS_FILTER_OF` is set)
- `system_config` file is generated to log the system configurations
- These are the system volumes you can give as an input to HC-based and Greedy-based online migration algorithms later on
