#!/bin/sh
# Creates a raidz2 zpool of the given name with the given devices
#
# Paul Pruitt

if [ $# -le 1 ] 
then
   echo "Usage: ${0} pool_name device ..."
   exit 1
fi

pool=${1}
shift

echo "Creating Pool ${pool}.."
zpool create ${pool} raidz2 $* || { echo "Failed to create ZFS Pool ${pool}."; exit 2; }

zpool status ${pool}


 
