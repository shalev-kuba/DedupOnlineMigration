import argparse
import os
import subprocess
import random
import datetime
import re
import json
import statistics
import csv

from multiprocessing import Pool
from typing import Dict, List, Set, Optional
from pydantic import BaseModel


class TraceInfo(BaseModel):
    path: str = ""
    user: int = 0
    host: int = 0
    date: datetime.datetime = None
    input_file: str = ""
    hashes: Optional[Set[str]] = None
    size: Optional[int] = None


class TraceDiff(BaseModel):
    old_total_block: int
    new_total_blocks: int
    num_blocks_removed: int
    blocks_removed_perc: float
    num_new_blocks: int
    new_blocks_perc: float
    size_diff: int

    def as_dict(self):
        return dict(
            old_total_block=self.old_total_block,
            new_total_blocks=self.new_total_blocks,
            num_blocks_removed=self.num_blocks_removed,
            blocks_removed_perc=self.blocks_removed_perc,
            num_new_blocks=self.num_new_blocks,
            new_blocks_perc=self.new_blocks_perc,
            size_diff=self.size_diff,
        )


class UserAnalysis(BaseModel):
    user: int
    host: int
    traces: List[str]
    diffs: List[TraceDiff]
    avg_block_del_perc: float
    avg_block_add_perc: float
    avg_size_diff: float

    def as_dict(self):
        return dict(
            user=self.user,
            host=self.host,
            traces=self.traces,
            diffs=[t.as_dict() for t in self.diffs],
            avg_block_del_perc=self.avg_block_del_perc,
            avg_block_add_perc=self.avg_block_add_perc,
            avg_size_diff=self.avg_size_diff,
        )


def get_info_by_file_name(file_path: str) -> TraceInfo:
    FILE_NAME_PATTERN = r'U(.*)_H(.*)_IF(.*)_TS(.*)_(.*)_(.*)_(.*)_(.*)'
    USER_INDEX = 1
    HOST_INDEX = 2
    IF_INDEX = 3
    DAY_INDEX = 4
    MONTH_INDEX = 5
    YEAR_INDEX = 6
    HOUR_INDEX = 7
    MINUTES_INDEX = 8

    regex_groups = re.search(FILE_NAME_PATTERN, os.path.basename(file_path))

    return TraceInfo(
        path=file_path,
        user=int(regex_groups[USER_INDEX]),
        host=int(regex_groups[HOST_INDEX]),
        input_file=regex_groups[IF_INDEX],
        date=datetime.datetime(year=int(regex_groups[YEAR_INDEX]),
                               month=int(regex_groups[MONTH_INDEX]),
                               day=int(regex_groups[DAY_INDEX]),
                               hour=int(regex_groups[HOUR_INDEX]),
                               minute=int(regex_groups[MINUTES_INDEX])))


def load_trace(trace: TraceInfo) -> None:
    trace.hashes = set()
    trace.size = 0
    with open(trace.path, "r") as trace_f:
        for line in trace_f:
            stripped_line = line.rstrip()
            if len(stripped_line) == 0:
                continue

            splitted_line = stripped_line.replace(" ", "").split(",")
            if splitted_line[0] == "F":
                NUM_BLOCKS_INDEX = 4
                FIRST_BLOCK_INDEX = 5
                BLOCK_INFO_NUM_INDICES = 2

                num_blocks = int(splitted_line[NUM_BLOCKS_INDEX])
                for i in range(num_blocks):
                    RELATIVE_BLOCK_SIZE_INDEX = 1
                    start_index = FIRST_BLOCK_INDEX + i * BLOCK_INFO_NUM_INDICES
                    trace.size += int(splitted_line[start_index + RELATIVE_BLOCK_SIZE_INDEX])

            elif splitted_line[0] == "B":
                HASH_INDEX = 2
                trace.hashes.add(splitted_line[HASH_INDEX])
            else:
                continue


def load_traces_data(traces: List[TraceInfo]) -> None:
    for trace in traces:
        load_trace(trace)


def get_traces_diff(old_trace: TraceInfo, new_trace: TraceInfo) -> TraceDiff:
    deleted_blocks = old_trace.hashes.difference(new_trace.hashes)
    new_blocks = new_trace.hashes.difference(old_trace.hashes)

    return TraceDiff(
        old_total_block=len(old_trace.hashes),
        new_total_blocks=len(new_trace.hashes),
        num_blocks_removed=len(deleted_blocks),
        blocks_removed_perc=(len(deleted_blocks) / len(old_trace.hashes)) * 100.0,
        new_blocks_perc=(len(new_blocks) / len(old_trace.hashes)) * 100.0,
        num_new_blocks=len(new_blocks),
        size_diff=new_trace.size - old_trace.size
    )


def get_traces_diffs(traces: List[TraceInfo]) -> List[TraceDiff]:
    traces_diff = []

    old_trace = None
    for trace in traces:
        if old_trace is None:
            old_trace = trace
            continue

        traces_diff.append(get_traces_diff(old_trace, trace))
        old_trace = trace

    return traces_diff


def get_analysis(traces: List[TraceInfo], user: int, host: int) -> UserAnalysis:
    load_traces_data(traces)

    print(f'Info: handling user: {user} with host: {host}')

    assert all(int(t.host) == int(host) for t in traces)
    assert all(int(t.user) == int(user) for t in traces)

    diffs: List[TraceDiff] = get_traces_diffs(traces)
    if len(diffs) == 0:
        print(f'Warning: user: {user} with host: {host} has 0 diffs')
        avg_block_del_perc = 0
        avg_block_add_perc = 0
        avg_size_diff = 0
    else:
        avg_block_del_perc = sum(diff.blocks_removed_perc for diff in diffs) / len(diffs)
        avg_block_add_perc = sum(diff.new_blocks_perc for diff in diffs) / len(diffs)
        avg_size_diff = sum(diff.size_diff for diff in diffs) / len(diffs)

    return UserAnalysis(
        user=user,
        host=host,
        traces=[trace.path for trace in traces],
        diffs=diffs,
        avg_block_del_perc=avg_block_del_perc,
        avg_block_add_perc=avg_block_add_perc,
        avg_size_diff=avg_size_diff
    )


def load_filtered_index(index_path: str) -> dict:
    with open(index_path) as index_file:
        return json.loads(index_file.read())


def analyze_user_host(index_dict: dict, user: int, host: int) -> UserAnalysis:
    traces_info = [get_info_by_file_name(t['full_path']) for t in index_dict[str(user)][str(host)]]
    traces_info.sort(key=lambda t: t.date)
    return get_analysis(traces_info, user, host)


def analyze_user(index_dict: dict, user, out_dir: str):
    res: Dict[str, dict] = {}
    for host in index_dict[str(user)].keys():
        res[str(host)] = analyze_user_host(index_dict, user, host).as_dict()

    res_with_user: Dict = {str(user): res}
    with open(os.path.join(out_dir, f'analysis_U{user}.json'), "w") as out_json:
        json.dump(res_with_user, out_json, indent=4)


def collect_analyses(analyses_path: str, outliers_threshold: int):
    res = {
        "users": {},
        "outliers": {},
        "total": {},
    }

    for file_name in os.listdir(analyses_path):
        with open(os.path.join(analyses_path, file_name)) as fp:
            res["users"].update(json.load(fp))

    info_list = []
    for user in res["users"].keys():
        for host in res["users"][user].keys():
            info = res["users"][user][host]
            info_list += [
                dict(new_blocks_perc=diff["new_blocks_perc"],
                     blocks_removed_perc=diff["blocks_removed_perc"])
                for diff in info["diffs"]
                if abs(diff["new_blocks_perc"]) < outliers_threshold and
                   abs(diff["blocks_removed_perc"]) < outliers_threshold]

            outliers = [diff for diff in info["diffs"]
                        if abs(diff["new_blocks_perc"]) >= outliers_threshold or
                        abs(diff["blocks_removed_perc"]) >= outliers_threshold]

            if len(outliers) > 0:
                res["outliers"][f'U{user}_H{host}'] = outliers

    res["total"]["avg_block_del_perc"] = sum(i["blocks_removed_perc"] for i in info_list) / len(info_list)
    res["total"]["avg_block_add_perc"] = sum(i["new_blocks_perc"] for i in info_list) / len(info_list)
    res["total"]["median_block_del_perc"] = statistics.median([i["blocks_removed_perc"] for i in info_list])
    res["total"]["median_block_add_perc"] = statistics.median([i["new_blocks_perc"] for i in info_list])
    res["total"]["num_outliers_moves"] = sum(len(host_outliers) for host_outliers in res["outliers"].values())
    res["total"]["num_moves"] = len(info_list)
    res["total"]["outliers_per"] = (res["total"]["num_outliers_moves"] * 100.0) / \
                                   (len(info_list) + res["total"]["num_outliers_moves"])

    return res


def run_analyses(index_path: str, out_dir: str, num_processes: int):
    index_dict = load_filtered_index(index_path)

    pool = Pool(num_processes)
    users = [int(user) for user in index_dict.keys()]
    processes_results = [pool.apply_async(analyze_user, args=(index_dict, user, out_dir))
                         for user in users]

    processes_results = [process_result.get() for process_result in processes_results]

    pool.close()
    pool.join()


def output_to_xml(analyses_path, analysis_res):
    with open(os.path.join(analyses_path, "summarized_analysis.csv"), "w", newline='') as fp:
        writer = csv.writer(fp)
        headers = ['User', 'Host', 'Old Trace', 'New Trace', 'Old Total Blocks', 'New Total Blocks',
                   'Num Blocks Removed', 'Num Blocks Added', 'Block Removed %', 'Block Added %', 'Size Diff']
        writer.writerow(headers)
        for user in analysis_res["users"].keys():
            for host in analysis_res["users"][user].keys():
                info = analysis_res["users"][user][host]
                for i in range(len(info["diffs"])):
                    diffs_info = info["diffs"][i]
                    line = [user, host, info["traces"][i], info["traces"][i + 1], diffs_info["old_total_block"],
                            diffs_info["new_total_blocks"], diffs_info["num_blocks_removed"],
                            diffs_info["num_new_blocks"],
                            "%.2f" % diffs_info["blocks_removed_perc"],
                            "%.2f" % diffs_info["new_blocks_perc"],
                            diffs_info["size_diff"]]
                    writer.writerow(line)


def run(args):
    if args.is_collect_analyses:
        res = collect_analyses(args.out_dir, args.outliers_threshold)
        if not args.is_xml_export:
            with open(os.path.join(args.out_dir, "summarized_analysis.json"), "w") as fp:
                json.dump(res, fp, indent=4)
        else:
            output_to_xml(args.out_dir, res)

        return

    return run_analyses(args.filtered_index_path, args.out_dir, args.num_processes)


def get_setup_args():
    parser = argparse.ArgumentParser(description='This script gets a list of traces that belongs to a user and '
                                                 'generated an output json that contains the user traces analysis')
    parser.add_argument('-i', '--input_filtered_index_path', dest='filtered_index_path', type=str,
                        default='baseFormattedUbcFSFiles/filtered_indexed_input_files.json',
                        help='path to filtered index json')

    parser.add_argument('-o', '--output_dir', dest='out_dir', type=str,
                        default='baseFormattedUbcFSFiles/analysis',
                        help='Path to output analysis dir')

    parser.add_argument('--num_processes', dest='num_processes', type=int,
                        default=20,
                        help='Num parallel processes')

    parser.add_argument('--collect_analyses', dest='is_collect_analyses', type=bool,
                        default=False,
                        help='If stated true, collect all users analyses to one analysis')

    parser.add_argument('--outliers_threshold', dest='outliers_threshold', type=float,
                        default=100.0,
                        help='Outliers threshold. Affects only on collect_analyses')

    parser.add_argument('--export_as_xml', dest='is_xml_export', type=bool,
                        default=False,
                        help='Export analysis as xml (default is json)')

    return parser.parse_args()


if __name__ == "__main__":
    run(get_setup_args())
