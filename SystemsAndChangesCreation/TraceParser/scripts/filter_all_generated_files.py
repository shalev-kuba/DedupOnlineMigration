import argparse
import datetime
import json
import os
import subprocess
import random
from multiprocessing import Pool
from typing import Dict, List, Set, Optional
from pydantic import BaseModel
from pydantic import PydanticDeprecatedSince20
import warnings
warnings.filterwarnings("ignore", category=PydanticDeprecatedSince20)
FILTERED_PARSED_TRACES_PATH = 'baseFormattedUbcFSFiles/filtered_parsed_traces/'
PARSED_TRACES_PATH = 'baseFormattedUbcFSFiles/parsed_traces/'

def run_on_trace_file(filter_script_path:str, old_trace_path: str, output_path: str = ''):
    if os.path.isfile(output_path):
        print(f"{output_path} already exists. skipping")
        return output_path

    os.system(f'python3 {filter_script_path} -i {old_trace_path} -o {output_path}')


def filter_generated_file(filter_script_path, trace_path):
    dir_path = os.path.join(FILTERED_PARSED_TRACES_PATH, os.path.basename(os.path.dirname(trace_path)))
    os.system(f'mkdir -p {dir_path}')
    run_on_trace_file(filter_script_path, trace_path, os.path.join(dir_path, os.path.basename(trace_path)))


def get_generated_files():
    files = []
    for user in os.listdir(PARSED_TRACES_PATH):
        user_dir = os.path.join(PARSED_TRACES_PATH, user)
        user_files = [os.path.join(PARSED_TRACES_PATH, user_dir, gen_file) for gen_file in list(os.listdir(user_dir)) if '_G' in gen_file ]
        files.extend(user_files)

    return files

def run_filter(args):
    pool = Pool(args.num_processes)
    processes_results = [pool.apply_async(filter_generated_file, args=(args.filter_script_path, path)) for path in get_generated_files()]

    processes_results = [process_result.get() for process_result in processes_results]

    pool.close()
    pool.join()


def get_setup_args():
    parser = argparse.ArgumentParser(description='This script generates a new parsed trace given an old parsed '
                                                 'trace and generation params')
    parser.add_argument('-p', '--num_processes', dest='num_processes', type=int, default=30,
                        help='num of processes to filter files')
    parser.add_argument('-sp', '--filter_script_path', dest='filter_script_path', type=str, default='./trace_filter.py',
                        help='path to filter script')
    return parser.parse_args()


if __name__ == "__main__":
    args = get_setup_args()
    run_filter(args)
    # print(get_generated_files())
