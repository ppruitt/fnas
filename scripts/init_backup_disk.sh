#!/bin/sh

if [ $# -lt 3 ] 
then
   echo "Usage: ${0} bkup_disk_dev bkup_disk_label bkup_disk_pool"
   exit 2
fi


bkup_dev=${1}
shift

bkup_label=${1}
shift

bkup_pool=${1}
shift

zpool_dev=/dev/label/${bkup_label}.eli

# label device
glabel label ${bkup_label} ${bkup_dev} || { echo "Failed to label ${bkup_label}."; exit 1; }

# Init encrypted volume
geli init -b -s 4096 -l 256 /dev/label/${bkup_label} || \
    { echo "Failed to init geli device."; exit 1; }

# Attach
geli attach /dev/label/${bkup_label} || { echo "Failed to attach geli device ${bkup_label}."; exit 1; }

# Init zfs pool
zpool create ${bkup_pool} ${zpool_dev} || { echo "Failed to create zpool ${bkup_pool}"; exit 3; }

zfs set compression=lz4 ${bkup_pool}

zpool status ${bkup_pool}



