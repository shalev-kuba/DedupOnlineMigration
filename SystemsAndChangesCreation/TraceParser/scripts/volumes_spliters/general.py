import random


class VolumeInfo:
    def __init__(self, index):
        self.index = index
        self.files_paths = []
        self.users = set({})
        self.hosts = set({})
        self.snaps = []
        self.sum_num_physical_blocks = 0


def add_snap_to_vol(vol, snap):
    vol.files_paths.append(snap.full_path)
    # THINK ABOUT ADD PHYSICAL SIZE TO TRACE FILE, THEN USE IT
    vol.sum_num_physical_blocks += snap.additional_info.num_physical_blocks
    vol.users.add(snap.user)
    vol.hosts.add(snap.host)
    vol.snaps.append(snap)


def create_system_empty_volumes(num_volumes):
    volumes = []
    for i in range(num_volumes):
        volumes.append(VolumeInfo(index=len(volumes)))

    return volumes


def has_enough_files(vol, num_files):
    return len(vol.files_paths) >= num_files


def is_below_tolerance_threshold(max_sum_physical_blocks, vol):
    diff_percentage = get_size_diff_percentage(max_sum_physical_blocks, vol)
    TOLERANCE_PERCENTAGE = 2
    return diff_percentage > TOLERANCE_PERCENTAGE


def get_size_diff_percentage(max_sum_physical_blocks, vol):
    diff = max_sum_physical_blocks - vol.sum_num_physical_blocks
    return (float(diff) / vol.sum_num_physical_blocks) * 100


def get_volumes_index_to_fix(volumes, max_sum_physical_blocks):
    res = []
    for vol in volumes:
        if is_below_tolerance_threshold(max_sum_physical_blocks, vol):
            res.append(vol.index)

    return res


def get_unused_files_in_system(volumes, snaps_dict):
    visited_files = get_used_files_in_system(volumes)

    new_files = []
    for user in sorted(snaps_dict.keys()):
        for host in snaps_dict[user]:
            for file in snaps_dict[user][host]:
                if file.full_path in visited_files or file.full_path in new_files:
                    continue

                new_files.append(file)

    return new_files


def get_used_files_in_system(volumes):
    used_files = set({})
    for volume in volumes:
        used_files = used_files.union(set(volume.files_paths))

    return used_files


def get_biggest_vol_in_system(volumes):
    if len(volumes) == 0:
        return None

    max_volume = volumes[0]
    for volume in volumes:
        if volume.sum_num_physical_blocks > max_volume.sum_num_physical_blocks:
            max_volume = volume

    return max_volume


def split_by_num_files_per_volume(snaps_dict, num_volumes, files_per_volume, snap_to_vol_index, noise_percentages,
                                  balance_system=False):
    volumes = create_system_empty_volumes(num_volumes=num_volumes)

    for user in sorted(snaps_dict.keys()):
        for host in snaps_dict[user]:
            for file in snaps_dict[user][host]:
                vol_index = snap_to_vol_index(file, num_volumes)
                if vol_index < 0:
                    continue  # file not mapped

                # no need to ignore generated files, they're in different dict

                current_vol = volumes[vol_index]
                if has_enough_files(current_vol, files_per_volume * (1 - noise_percentages)):
                    continue

                add_snap_to_vol(vol=current_vol, snap=file)

    fill_volumes_with_rand_files(snaps_dict, volumes, files_per_volume)

    if balance_system:
        balance_dict_with_more_data(snaps_dict, volumes, snap_to_vol_index)

    return volumes


def fill_volumes_with_rand_files(snaps_dict, volumes, files_per_volume):
    new_files = get_unused_files_in_system(volumes, snaps_dict)
    print(f'Started to fill volumes with random files')

    initial_seed = 12345
    random.seed(initial_seed)
    for current_vol in volumes:
        if len(new_files) == 0:
            raise Exception("Not enough file to create the requested system")

        while not has_enough_files(current_vol, files_per_volume):
            file_to_add = random.choice(new_files)
            new_files.remove(file_to_add)
            add_snap_to_vol(vol=current_vol, snap=file_to_add)

        print(f'Finished to fill volume number: {current_vol.index} with random files')

    print(f'Finished to fill volumes with random files')


def balance_dict_with_more_data(snaps_dict, volumes, snap_to_vol_index):
    visited_files = get_used_files_in_system(volumes)
    max_volume = get_biggest_vol_in_system(volumes)

    volumes_index_to_fix = get_volumes_index_to_fix(volumes, max_volume.sum_num_physical_blocks)

    print(f'volumes to balance {", ".join([str(vol_index) for vol_index in volumes_index_to_fix])}')

    for user in sorted(snaps_dict.keys()):
        if len(volumes_index_to_fix) == 0:
            break

        for host in snaps_dict[user]:
            for file in snaps_dict[user][host]:
                if file.full_path in visited_files:
                    continue

                visited_files.add(file.full_path)

                vol_index = snap_to_vol_index(file, len(volumes))

                if vol_index not in volumes_index_to_fix:
                    continue

                current_vol = volumes[vol_index]

                add_snap_to_vol(vol=current_vol, snap=file)

                if not is_below_tolerance_threshold(max_volume.sum_num_physical_blocks, current_vol):
                    volumes_index_to_fix.remove(current_vol.index)

    if len(volumes_index_to_fix) > 0:
        print(f'Warning: Could not balance {", ".join([str(vol_index) for vol_index in volumes_index_to_fix])}')

