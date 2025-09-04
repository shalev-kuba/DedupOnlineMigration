def volume_index_by_user(snap, num_volumes):
    return snap.user % num_volumes
