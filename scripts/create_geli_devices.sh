#!/bin/sh
# Creates an encrypted device with UFS a filesystem (ideally a USB key). 
# Generates key material for each listed device and initializes a geli
# device in preparation for creating an encrypted ZFS pool. Keys are stored in
# the USB filesystem, which is unmounted and detached upon successful completion
#
# Paul Pruitt

if [ $# -le 1 ] 
then
   echo "Usage: ${0} key_device device ..."
   exit 2
fi

USB_DEV=${1}
shift

if [ ! -f ${0} ]
then 
   echo "Key device ${USB_DEV} doesn't exist or is not a device."
   exit 3
fi

LABEL=key
ENC_LABEL=${LABEL}.eli
KEYDATA_DIR=/keydata
KEYFS_LABEL=keydata

# label device
glabel label ${LABEL} ${USB_DEV} || { echo "Failed to label ${USB_DEV}."; exit 1; }

# Init encrypted volume
geli init -b -s 4096 -l 256 /dev/label/${LABEL} || \
    { echo "Failed to init geli device."; exit 1; }

# Attach encrypted volume
geli attach /dev/label/${LABEL} || { echo "Failed to attach geli device ${LABEL}."; exit 1; }

# Create filesystem
newfs -L ${KEYFS_LABEL} /dev/label/${ENC_LABEL} 

# Create mountpoint
mkdir -p ${KEYDATA_DIR} || { "Failed to create keydata dir."; exit 1; }

# Mount filesystem
mount /dev/label/${ENC_LABEL} ${KEYDATA_DIR} || { echo "Failed to mount ${ENC_LABEL}"; exit 1; }

cd ${KEYDATA_DIR}

while [ $# -gt 0 ]
do
   dk=${1}
   dk_eli=${dk}.eli
   keyfile=`basename ${dk}`.key 

   echo "Generating ${keyfile}..."
   dd if=/dev/random bs=256k of=${keyfile} count=1 || { echo "Failed to create keyfile ${keyfile}" ; exit 3; }
   
   echo "Initializing geli device ${dk_eli}"
   geli init -P -s 4096 -l 256 -K ${keyfile} ${dk} || { echo "Failed to initialize geli device ${dk}"; exit 2; }

   echo "Attaching ${dk_eli}."
   geli attach -p -k ${keyfile} ${dk} || { echo "Failed to attach ${dk}."; exit 2; }
   
   shift
done

cd

umount ${KEYDATA_DIR} || { echo "Failed to unmount ${KEYDATA_DIR}."; exit 3; }

geli detach /dev/label/${ENC_LABEL} || { echo "Failed to detach ${ENC_LABEL}"; exit 2; }




 
