dict_user_to_last_vol_offset_index = {}


def volume_index_by_user_adv(snap, num_volumes):
    num_vol_to_spread_to = (int)(num_volumes / 2) + num_volumes % 2

    res = snap.user % num_volumes
    if snap.user in dict_user_to_last_vol_offset_index:
        last_vol_offset = dict_user_to_last_vol_offset_index[snap.user]
        new_offset = (last_vol_offset + 1) % num_vol_to_spread_to
        dict_user_to_last_vol_offset_index[snap.user] = new_offset
        res = (snap.user % num_volumes + new_offset) % num_volumes
    else:
        dict_user_to_last_vol_offset_index[snap.user] = 0

    return res
