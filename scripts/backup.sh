#!/bin/sh

if [ $# -lt 3 ]
then
    echo "Usage: $0 src_zpool dest_zpool fs_name [, fs_name, ...]"
    exit 1
fi

ts=`date -u +%Y%m%d-%H%m`

src_pool=${1}
shift

dest_pool=${1}
shift 

for fs_name in $* 
do
    snap="${src_pool}/${fs_name}@${ts}"
    echo "Creating snapshot ${snap}"
    zfs snapshot ${snap} || { echo "Failed to create snapshot ${snap}"; exit 2; }
done

for fs_name in $* 
do
    snap="${src_pool}/${fs_name}@${ts}"
    dest_snap="${dest_pool}/${fs_name}"
    echo "Transferring snapshot ${snap} to ${dest_snap}..."

    zfs send -v -R "${snap}" | zfs receive ${dest_snap} || { echo "Failed to send."; exit 3; }
done

zfs list ${dest_pool}
