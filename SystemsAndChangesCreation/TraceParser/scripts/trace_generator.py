import argparse
import datetime
import json
import os
import subprocess
import random
from multiprocessing import Pool
from typing import Dict, List, Set, Optional
from pydantic import BaseModel

METADATA_PRINTED_TO_FIELD_MAPPERS = {
    'Output type': 'output_type',
    'Input File': 'file_name',
    'Target depth': 'target_depth',
    'Num files': 'num_files',
    'Num logical files': 'num_logical_files',
    'Num directories': 'num_directories',
    'Num Physical Blocks': 'num_physical_blocks',
    'Num Logical Blocks': 'num_logical_blocks',
    'User': 'user',
    'Host name': 'host_name',
    'Windows Version': 'windows_version',
    'Snapshot Date': 'snap_date',
    'Logical File System Size': 'logical_file_system_size',
    'Generate Generation': 'generation',
}

METADATA_FIELD_TO_PRINTED_MAPPERS = {
    'output_type': 'Output type',
    'file_name': 'Input File',
    'target_depth': 'Target depth',
    'num_files': 'Num files',
    'num_logical_files': 'Num logical files',
    'num_directories': 'Num directories',
    'num_physical_blocks': 'Num Physical Blocks',
    'num_logical_blocks': 'Num Logical Blocks',
    'user': 'User',
    'host_name': 'Host name',
    'windows_version': 'Windows Version',
    'snap_date': 'Snapshot Date',
    'logical_file_system_size': 'Logical File System Size',
    'generation': 'Generate Generation'
}


class MetaData(BaseModel):
    output_type: str
    file_name: str
    target_depth: int
    num_files: int
    num_logical_files: int
    num_directories: int
    num_physical_blocks: int
    num_logical_blocks: int
    user: int
    host_name: int
    windows_version: str
    snap_date: str
    logical_file_system_size: int
    generation: int = 0

    @staticmethod
    def printed_to_field(s: str) -> str:
        return METADATA_PRINTED_TO_FIELD_MAPPERS[s]

    @staticmethod
    def field_to_printed(s: str) -> str:
        return METADATA_FIELD_TO_PRINTED_MAPPERS[s]

    def __repr__(self) -> str:
        return '\n'.join([f'#{METADATA_FIELD_TO_PRINTED_MAPPERS[key]}: {value}'
                          for key, value in self.dict().items()]) + '\n'


class Trace:
    block_index_to_size: Dict[int, int] = {}
    block_index_to_hash: Dict[int, str] = {}
    max_index: int = 1
    hashes: Set[str] = set({})
    metadata: MetaData


def load_and_get_metadata(raw_metadata: List[str]) -> MetaData:
    metadata_dict = {METADATA_PRINTED_TO_FIELD_MAPPERS[key]: value.strip() for (key, value) in
                     [metadata_line.split(':', maxsplit=1)
                      for metadata_line in raw_metadata]}

    return MetaData(**metadata_dict)


def get_old_trace_metadata(old_trace_path: str) -> MetaData:
    raw_metadata_list = []
    with open(old_trace_path) as old_trace:
        for line in old_trace:
            stripped_line = line.rstrip()
            if len(stripped_line) == 0:
                continue
            if stripped_line.startswith("#"):
                raw_metadata_list.append(line.replace('\n', '').split('#', maxsplit=1)[1])
                continue

            splitted_line = stripped_line.replace(" ", "").split(",")
            if splitted_line[0] == "F":
                break
            elif splitted_line[0] == "B":
                break
            else:
                continue

    return load_and_get_metadata(raw_metadata_list)


def load_and_get_old_trace(old_trace_path: str) -> Trace:
    old_trace_info = Trace()
    raw_metadata_list = []
    with open(old_trace_path) as old_trace:
        for line in old_trace:
            stripped_line = line.rstrip()
            if len(stripped_line) == 0:
                continue
            if stripped_line.startswith("#"):
                raw_metadata_list.append(line.replace('\n', '').split('#', maxsplit=1)[1])
                continue

            splitted_line = stripped_line.replace(" ", "").split(",")
            if splitted_line[0] == "F":
                NUM_BLOCKS_INDEX = 4
                FIRST_BLOCK_INDEX = 5
                BLOCK_INFO_NUM_INDICES = 2

                num_blocks = int(splitted_line[NUM_BLOCKS_INDEX])
                for i in range(num_blocks):
                    RELATIVE_BLOCK_INDEX_INDEX = 0
                    RELATIVE_BLOCK_SIZE_INDEX = 1
                    start_index = FIRST_BLOCK_INDEX + i * BLOCK_INFO_NUM_INDICES
                    old_trace_info.block_index_to_size[int(splitted_line[start_index + RELATIVE_BLOCK_INDEX_INDEX])] = \
                        int(splitted_line[start_index + RELATIVE_BLOCK_SIZE_INDEX])

            elif splitted_line[0] == "B":
                INDEX_INDEX = 1
                HASH_INDEX = 2
                old_trace_info.block_index_to_hash[int(splitted_line[INDEX_INDEX])] = splitted_line[HASH_INDEX]
                old_trace_info.hashes.add(splitted_line[HASH_INDEX])
                old_trace_info.max_index = max(old_trace_info.max_index, int(splitted_line[INDEX_INDEX]))
            else:
                continue

    old_trace_info.metadata = load_and_get_metadata(raw_metadata_list)
    assert len(old_trace_info.block_index_to_hash) == len(old_trace_info.block_index_to_size)
    return old_trace_info


def generate_hash():
    HASH_LENGTH_IN_BYTES = 5
    return bytes([random.randint(0, 255) for _ in range(0, HASH_LENGTH_IN_BYTES)]).hex()


def generate_size() -> int:
    SIZE_MEAN = 64 * 1024
    SIZE_SIGMA = 20 * 1024

    res = round(random.gauss(SIZE_MEAN, SIZE_SIGMA))
    if res > 0:
        return res
    return 4096  # we make sure it does not return negative value


def set_static_seed(old_trace_base_name):
    random.seed(old_trace_base_name)


def add_blocks(old_trace: Trace, num_blocks_to_add: int, previous_deleted_blocks: List[str]) -> List[str]:
    added_hashes = []
    num_added = 0
    while num_added < num_blocks_to_add:
        # Theoretical we can collide with existing hash. In case of collision we continue to generate hash, until we
        # generate a new hash
        new_hash = generate_hash()
        if new_hash in old_trace.hashes or new_hash in previous_deleted_blocks:
            continue

        added_hashes.append(new_hash)
        old_trace.max_index += 1
        new_index = old_trace.max_index
        new_size = generate_size()
        old_trace.block_index_to_size[new_index] = new_size
        old_trace.block_index_to_size[new_index] = new_size
        old_trace.block_index_to_hash[new_index] = new_hash
        old_trace.hashes.add(new_hash)
        num_added += 1

    assert len(added_hashes) == num_blocks_to_add

    return added_hashes


def delete_blocks(old_trace: Trace, num_blocks_to_delete: int) -> List[str]:
    deleted_hashes = []
    for _ in range(num_blocks_to_delete):
        block_index = random.choice(list(old_trace.block_index_to_hash.keys()))
        deleted_hash = old_trace.block_index_to_hash.pop(block_index)
        old_trace.block_index_to_size.pop(block_index)
        try:
            old_trace.hashes.remove(deleted_hash)
        except:
            pass
        deleted_hashes.append(deleted_hash)

    assert len(deleted_hashes) == num_blocks_to_delete
    return deleted_hashes


def refresh_trace_metadata(trace: Trace) -> None:
    trace.metadata.generation += 1
    trace.metadata.num_physical_blocks = len(trace.block_index_to_size)
    trace.metadata.file_name = f'{trace.metadata.file_name.split("_G", maxsplit=1)[0]}_G{trace.metadata.generation}'

    DATE_FORMAT = '%d_%m_%Y_%H_%M'
    old_snap_date = datetime.datetime.strptime(trace.metadata.snap_date, DATE_FORMAT)
    old_snap_date += datetime.timedelta(days=7)
    trace.metadata.snap_date = old_snap_date.strftime(DATE_FORMAT)


def output_new_trace(trace: Trace, output_path: str) -> None:
    with open(output_path, 'x') as output_trace:
        output_trace.write(f'{repr(trace.metadata)}\n')

        LOCAL_SN_IS_ZERO = 0
        ONLY_EXIST_IN_ONE_FILE = 1
        # outputs blocks
        for block_index in trace.block_index_to_size.keys():
            # Format is: B, <block_index>, <block_hash>, <num_files>, <file_sn_list> - in our scenario <num_files> is
            # always 1 with file_sn_list that contains only sn=0
            output_trace.write(f'B, {block_index}, {trace.block_index_to_hash[block_index]}, {ONLY_EXIST_IN_ONE_FILE}, '
                               f'{LOCAL_SN_IS_ZERO}\n')

        # outputs file line
        # Format is: F, <file_SN>, <file_name>, <parent_sn>, <num_blocks>, [<block_index> <block_size>]
        PARENT_LOCAL_SN_IS_ZERO = 0
        output_trace.write(f'F, {LOCAL_SN_IS_ZERO}, {trace.metadata.file_name}, {PARENT_LOCAL_SN_IS_ZERO}, '
                           f'{len(trace.block_index_to_size)}')

        for block_index in trace.block_index_to_size.keys():
            # Format for each block inside file line is: <block_index> <block_size>
            output_trace.write(f', {block_index}, {trace.block_index_to_size[block_index]}')

        output_trace.write('\n')


def get_output_path(metadata: MetaData, old_trace_path: str, given_output_path: str) -> str:
    if len(given_output_path) > 0:
        return given_output_path

    new_file_name = f'{metadata.file_name.split("_G", maxsplit=1)[0]}_G{metadata.generation + 1}'

    DATE_FORMAT = '%d_%m_%Y_%H_%M'
    old_snap_date = datetime.datetime.strptime(metadata.snap_date, DATE_FORMAT)
    old_snap_date += datetime.timedelta(days=7)
    new_snap_date = old_snap_date.strftime(DATE_FORMAT)

    return os.path.join(os.path.dirname(old_trace_path), f'U{metadata.user}_H{metadata.host_name}_'
                                                         f'IF{new_file_name}_TS{new_snap_date}')


def run_on_trace_file(old_trace_path: str, del_perc: float, add_perc: float, output_path: str = '') -> str:
    set_static_seed(os.path.basename(old_trace_path))
    metadata = get_old_trace_metadata(old_trace_path=old_trace_path)
    output_path = get_output_path(metadata, old_trace_path, output_path)

    if os.path.isfile(output_path):
        print(f"{output_path} already exists. skipping")
        return output_path

    trace = load_and_get_old_trace(old_trace_path=old_trace_path)
    num_orig_blocks = len(trace.block_index_to_size)

    print(f"{os.getpid()}: working on {output_path}")

    print(f"num_orig_blocks={num_orig_blocks}")

    # delete blocks
    num_blocks_to_delete = int(del_perc / 100.0 * num_orig_blocks)
    deleted_hashes = delete_blocks(trace, num_blocks_to_delete)

    # add blocks
    num_blocks_to_add = int(add_perc / 100.0 * num_orig_blocks)
    added_hashes = add_blocks(trace, num_blocks_to_add, deleted_hashes)

    refresh_trace_metadata(trace=trace)
    print(f"Num Deleted={len(deleted_hashes)} num_blocks_to_delete={num_blocks_to_delete}")
    print(f"Num Added={len(added_hashes)} num_blocks_to_add={num_blocks_to_add}")
    print(f"num new blocks={len(trace.block_index_to_size)}")

    output_new_trace(trace, output_path)
    print(f"result located at to={output_path}")
    return output_path


def run_on_user_host(user: str, host: str, del_perc: float, add_perc: float, num_files_to_create: int,
                     index_dict: dict):
    last_file_full_path = index_dict[user][host][-1]["full_path"]

    out_path = last_file_full_path
    for _ in range(num_files_to_create):
        out_path = run_on_trace_file(old_trace_path=out_path, del_perc=del_perc, add_perc=add_perc)


def run_on_user(user: str, del_perc: float, add_perc: float, num_files_to_create: int, index_dict: dict):
    print(f"{os.getpid()}: working on user:{user}")
    print(f"{os.getpid()}: user {user} has the following hosts {index_dict[user].keys()}")

    for host in index_dict[user].keys():
        print(f"{os.getpid()}: working on user:{user} host:{host}")
        run_on_user_host(user=user, host=host, del_perc=del_perc, add_perc=add_perc,
                         num_files_to_create=num_files_to_create, index_dict=index_dict)
        print(f"{os.getpid()}: Finish user:{user} host:{host}")

    print(f"{os.getpid()}: Finish  user:{user}")


def run_on_all_users(del_perc: float, add_perc: float, num_files_to_create: int, num_processes: int,
                     specific_index_file: str):
    with open(specific_index_file, 'r') as index_fp:
        index_dict = json.load(index_fp)

    pool = Pool(num_processes)

    users = [user for user in index_dict.keys()]

    processes_results = [pool.apply_async(run_on_user, args=(user, del_perc, add_perc, num_files_to_create, index_dict))
                         for user in users]

    processes_results = [process_result.get() for process_result in processes_results]

    pool.close()
    pool.join()


def run_generator(args):
    if not args.generate_all_users:
        return run_on_trace_file(old_trace_path=args.old_trace_path, del_perc=args.del_perc, add_perc=args.add_perc,
                                 output_path=args.output_path)

    run_on_all_users(del_perc=args.del_perc, add_perc=args.add_perc, num_files_to_create=args.num_files_per_user,
                     num_processes=args.num_processes, specific_index_file=args.specific_index)


def get_setup_args():
    parser = argparse.ArgumentParser(description='This script generates a new parsed trace given an old parsed '
                                                 'trace and generation params')
    parser.add_argument('-i', '--input_parsed_trace_path', dest='old_trace_path', type=str, required=False,
                        help='A path to the old parsed trace')

    parser.add_argument('-o', '--output_parsed_trace_path', dest='output_path', type=str, required=False, default='',
                        help='Output path for the new trace that will be generated. Default will be the input file with'
                             ' a suffix of _G<gen number>')

    parser.add_argument('-a', '--block_addition_percentage', dest='add_perc', type=float, default=0,
                        help='Num of blocks to add given as percentages from the given old trace')

    parser.add_argument('-d', '--block_deletion_percentage', dest='del_perc', type=float, default=0,
                        help='Num of blocks to remove given as percentages from the given old trace')

    parser.add_argument('-u', '--generate-for-filtered_users', dest='generate_all_users', type=bool, default=False,
                        help='Generate files for all filtered users Stated in '
                             '"baseFormattedUbcFSFiles/filtered_indexed_input_files.json"')

    parser.add_argument('-uf', '--num-files-per-user', dest='num_files_per_user', type=int, default=6,
                        help='Generate files for all filtered users Stated in '
                             '"baseFormattedUbcFSFiles/"')

    parser.add_argument('-np', '--num-processes', dest='num_processes', type=int, default=20,
                        help='Num processes to allocate for the run')
    parser.add_argument('-si', '--specific-index', dest='specific_index', type=str, default=
    "baseFormattedUbcFSFiles/filtered_indexed_input_files.json",
                        help='change filter input file if needed')

    return parser.parse_args()


if __name__ == "__main__":
    args = get_setup_args()
    run_generator(args)
