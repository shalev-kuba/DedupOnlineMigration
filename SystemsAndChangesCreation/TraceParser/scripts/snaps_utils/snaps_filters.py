def filter_one_snap_per_user_per_day(snaps_dict, num_days_threshold):
    for user in snaps_dict.keys():
        for host in snaps_dict[user]:
            filtered_snaps = []
            for snap_info in snaps_dict[user][host]:
                if len(filtered_snaps) == 0:
                    filtered_snaps.append(snap_info)
                else:
                    last_snap = filtered_snaps[len(filtered_snaps) - 1]
                    last_date = last_snap.date.date()
                    date_diff = snap_info.date.date() - last_date
                    if date_diff.days < num_days_threshold:
                        if snap_info.additional_info.logical_size > last_snap.additional_info.logical_size:
                            filtered_snaps.pop(len(filtered_snaps) - 1)
                            filtered_snaps.append(snap_info)
                    else:
                        filtered_snaps.append(snap_info)

            snaps_dict[user][host] = filtered_snaps


def filter_less_than_num_snaps(snaps_dict, min_num_snaps):
    users_to_filter = []
    for user in snaps_dict.keys():
        num_total_snaps = 0
        for host in snaps_dict[user]:
            num_total_snaps += len(snaps_dict[user][host])
        if num_total_snaps < min_num_snaps:
            users_to_filter.append(user)

    for user in users_to_filter:
        del snaps_dict[user]
