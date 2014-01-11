#!/bin/sh

keyfile_mount_pt="/keydata"
pool="data"
disks="disk0 disk1 disk2 disk3 disk4 disk5"
disklist=
mounted=0

err()
{
    echo "$*"

    if [ $mounted == 1 ]
    then 
	umount ${keyfile_mount_pt} || { echo "Failed to unmount ${keyfile_mount_pt}"; }
    fi

    exit 9
}


for dk in ${disks} 
do
    keyfile="/keydata/${dk}.key"

    if [ ! -f ${keyfile} ]
    then
        echo "Mounting ${keyfile_mount_pt}."
        mount ${keyfile_mount_pt} || err "Failed to mount ${keyfile_mount_pt}."
        mounted=1

        [ -f ${keyfile} ] || err "${keyfile} still doesn't exist after mounting ${keyfile_mount_pt}. Aborting."
    fi

    echo "geli attach ${dk}"
    geli attach -p -k ${keyfile} /dev/label/${dk} || err "Failed to geli attach ${dk}"  
     
    disklist="${disklist} /dev/label/${dk}.eli"       
done

echo "Bringing ZFS pool ${pool} online."
zpool online ${pool} ${disklist} || err "Failed to bring ${pool} online."

if [ $mounted == 1 ]
then 
   umount ${keydata_mount_pt}
fi
