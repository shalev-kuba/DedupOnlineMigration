import datetime
import os
import re
from copy import deepcopy
from typing import Dict, List, Set, Optional
from pydantic import BaseModel

DATE_FORMAT = "%m/%d/%Y %H:%M"
MAX_GEN_DEPTH = 8


class SnapAdditionalInfo(BaseModel):
    num_logical_files: int
    num_logical_blocks: int
    num_physical_blocks: int
    logical_size: int
    operating_system: str

    def asDict(self):
        return dict(num_logical_files=self.num_logical_files,
                    num_logical_blocks=self.num_logical_blocks,
                    num_physical_blocks=self.num_physical_blocks,
                    logical_size=self.logical_size,
                    operating_system=self.operating_system,
                    )

    def __repr__(self):
        return self.asDict().__repr__()


class SnapInfo(BaseModel):
    full_path: str
    host: int
    input_file: str
    date: datetime.datetime
    user: int
    snap_name: str
    generation: int
    additional_info: Optional[SnapAdditionalInfo] = None

    def asDict(self):
        return dict(full_path=self.full_path,
                    host=self.host,
                    input_file=self.input_file,
                    date=self.date.isoformat(),
                    user=self.user,
                    generation=self.generation,
                    snap_name=self.snap_name,
                    additional_info=None if self.additional_info is None else self.additional_info.asDict(),
                    )

    def __repr__(self):
        return self.asDict().__repr__()


def get_info_by_file_name(file_name):
    FILE_NAME_PATTERN = r'U(.*)_H(.*)_IF(.*)_TS(.*)_(.*)_(.*)_(.*)_(.*)'
    USER_INDEX = 1
    HOST_INDEX = 2
    IF_INDEX = 3
    DAY_INDEX = 4
    MONTH_INDEX = 5
    YEAR_INDEX = 6
    HOUR_INDEX = 7
    MINUTES_INDEX = 8

    regex_groups = re.search(FILE_NAME_PATTERN, file_name)

    return {
        'user': int(regex_groups[USER_INDEX]),
        'host': int(regex_groups[HOST_INDEX]),
        'input_file': regex_groups[IF_INDEX],
        'date': f'{regex_groups[MONTH_INDEX]}/{regex_groups[DAY_INDEX]}/{regex_groups[YEAR_INDEX]} '
                f'{regex_groups[HOUR_INDEX]}:{regex_groups[MINUTES_INDEX]}',
    }


def read_file_metadata(file_path):
    LINES_IN_HEADER = 13
    lines = []
    with open(file_path) as f:
        for _ in range(LINES_IN_HEADER):
            lines.append(f.readline())

    LOGICAL_FILES_INDEX = 4
    PHYSICAL_BLOCK_INDEX = 6
    LOGICAL_BLOCK_INDEX = 7
    OPERATING_SYSTEM_INDEX = 10
    LOGICAL_SIZE_INDEX = 12

    return SnapAdditionalInfo(
        num_logical_files=int(lines[LOGICAL_FILES_INDEX].split(': ')[1].strip('\n')),
        num_logical_blocks=int(lines[LOGICAL_BLOCK_INDEX].split(': ')[1].strip('\n')),
        num_physical_blocks=int(lines[PHYSICAL_BLOCK_INDEX].split(': ')[1].strip('\n')),
        operating_system=lines[OPERATING_SYSTEM_INDEX].split(': ')[1].strip('\n'),
        logical_size=int(lines[LOGICAL_SIZE_INDEX].split(': ')[1].strip('\n')),
    )


def add_user_snaps_to_dict(snaps_dict, generated_snaps_dict, user_dir):
    user = None
    for user_snap_name in os.listdir(user_dir):
        info_by_file_name = get_info_by_file_name(user_snap_name)
        user = info_by_file_name['user']
        host = info_by_file_name['host']
        if user not in snaps_dict:
            snaps_dict[user] = {}
            generated_snaps_dict[user] = {}

        if host not in snaps_dict[user]:
            snaps_dict[user][host] = []
            generated_snaps_dict[user][host] = []

        if '_G' not in info_by_file_name['input_file']:  # '_G' means the file is generated
            snaps_dict[user][host].append(SnapInfo(
                full_path=os.path.join(user_dir, user_snap_name),
                snap_name=user_snap_name,
                host=deepcopy(info_by_file_name['host']),
                input_file=deepcopy(info_by_file_name['input_file']),
                date=datetime.datetime.strptime(info_by_file_name['date'], DATE_FORMAT),
                user=int(deepcopy(user)),
                generation=0,
                additional_info=read_file_metadata(os.path.join(user_dir, user_snap_name))
            ))
        else:
            gen = int(info_by_file_name['input_file'].split('_G')[-1])
            if gen > MAX_GEN_DEPTH:
                continue

            generated_snaps_dict[user][host].append(SnapInfo(
                full_path=os.path.join(user_dir, user_snap_name),
                snap_name=user_snap_name,
                host=deepcopy(info_by_file_name['host']),
                input_file=deepcopy(info_by_file_name['input_file']),
                date=datetime.datetime.strptime(info_by_file_name['date'], DATE_FORMAT),
                user=int(deepcopy(user)),
                generation=int(info_by_file_name['input_file'].split('_G')[-1]),
                additional_info=read_file_metadata(os.path.join(user_dir, user_snap_name))
            ))

    for host in snaps_dict[user].keys():
        snaps_dict[user][host].sort(key=lambda snap: snap.date)

    for host in generated_snaps_dict[user].keys():
        generated_snaps_dict[user][host].sort(key=lambda snap: snap.date)


def get_snaps_dict(input_dir):
    snaps_dict = {}
    generated_snaps_dict = {}
    for user in os.listdir(input_dir):
        add_user_snaps_to_dict(snaps_dict=snaps_dict,
                               generated_snaps_dict=generated_snaps_dict,
                               user_dir=os.path.join(input_dir, user))

    return snaps_dict, generated_snaps_dict
