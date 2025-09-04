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

def is_block_filtered(fp: str) -> bool:
    # Remove leading and trailing spaces
    fp = fp.strip()

    # Check if the string has at least 4 characters for safe indexing
    if len(fp) < 4:
        raise ValueError("Input string is too short to process.")

    # Check the first three characters
    if fp[0] == '0' and fp[1] == '0' and fp[2] == '0':
        # Convert the fourth character to an integer
        x = int(fp[3], 16)
        return x > 7

    return True

def load_and_get_metadata(raw_metadata: List[str]) -> MetaData:
    metadata_dict = {METADATA_PRINTED_TO_FIELD_MAPPERS[key]: value.strip() for (key, value) in
                     [metadata_line.split(':', maxsplit=1)
                      for metadata_line in raw_metadata]}

    return MetaData(**metadata_dict)

def refresh_trace_metadata(trace: Trace) -> None:
    trace.metadata.num_physical_blocks = len(trace.block_index_to_size)


def output_filtered_trace(trace: Trace, output_path: str) -> None:
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


def load_and_get_filtered_trace(old_trace_path: str) -> Trace:
    filtered_trace_info = Trace()
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
                    if int(splitted_line[start_index + RELATIVE_BLOCK_INDEX_INDEX]) in filtered_trace_info.block_index_to_hash:
                        block_size = int(splitted_line[start_index + RELATIVE_BLOCK_SIZE_INDEX])
                        if block_size < 0:
                            print(f"Found in {old_trace_path} a block "
                                  f"(id={int(splitted_line[start_index + RELATIVE_BLOCK_INDEX_INDEX])}) "
                                  f"with size={block_size}, changing it to 4KB")
                            block_size = 4096
                        filtered_trace_info.block_index_to_size[int(splitted_line[start_index + RELATIVE_BLOCK_INDEX_INDEX])] = block_size

            elif splitted_line[0] == "B":
                INDEX_INDEX = 1
                HASH_INDEX = 2
                if is_block_filtered(splitted_line[HASH_INDEX]):
                    continue
                filtered_trace_info.block_index_to_hash[int(splitted_line[INDEX_INDEX])] = splitted_line[HASH_INDEX]
                filtered_trace_info.hashes.add(splitted_line[HASH_INDEX])
                filtered_trace_info.max_index = max(filtered_trace_info.max_index, int(splitted_line[INDEX_INDEX]))
            else:
                continue

    filtered_trace_info.metadata = load_and_get_metadata(raw_metadata_list)
    assert len(filtered_trace_info.block_index_to_hash) == len(filtered_trace_info.block_index_to_size)
    return filtered_trace_info


def run_on_trace_file(old_trace_path: str, output_path: str = '') -> str:
    if os.path.isfile(output_path):
        print(f"{output_path} already exists. skipping")
        return output_path

    filtered_trace = load_and_get_filtered_trace(old_trace_path=old_trace_path)
    refresh_trace_metadata(trace=filtered_trace)
    output_filtered_trace(filtered_trace, output_path)
    return output_path


def run_generator(args):
    run_on_trace_file(args.trace_path, args.filtered_path)


def get_setup_args():
    parser = argparse.ArgumentParser(description='This script filters a new parsed')
    parser.add_argument('-i', '--input_parsed_trace_path', dest='trace_path', type=str, required=False,
                        help='A path to the parsed trace to filter')

    parser.add_argument('-o', '--output_filtered_path', dest='filtered_path', type=str, required=False, default='',
                        help='Output path for the filtered trace')

    return parser.parse_args()


if __name__ == "__main__":
    args = get_setup_args()
    run_generator(args)
