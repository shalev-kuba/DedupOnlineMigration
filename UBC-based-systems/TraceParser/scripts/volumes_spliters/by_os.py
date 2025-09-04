OS_TO_INDEX = {
    'Microsoft Windows 7  (build 7100)': 0,
    'Microsoft Windows 7  (build 7600)': 1,
    'Microsoft Windows Server 2008 Enterprise Edition, 64-bit Service Pack 1 (build 6001)': 2,
    'Microsoft Windows Vista Enterprise Edition, 32-bit Service Pack 1 (build 6001)': 3,
    'Microsoft Windows Vista Enterprise Edition, 32-bit Service Pack 2 (build 6002)': 3,  # also 3 in purpose
    'Microsoft Windows Vista Enterprise Edition, 64-bit Service Pack 1 (build 6001)': 4,
    'Microsoft Windows Vista Enterprise Edition, 64-bit Service Pack 2 (build 6002)': 4  # also 4 in purpose
}


def volume_index_by_os(snap, num_volumes):
    if snap.additional_info.operating_system not in OS_TO_INDEX:
        return -1

    index = OS_TO_INDEX[snap.additional_info.operating_system]

    if index > (num_volumes - 1):
        return -1

    return index
