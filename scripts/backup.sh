#!/bin/sh

bkup_script=./backup.py
default_args="-mu"
datasets="data/ca data/devel data/docs data/downloads data/music data/photos data/records data/video data/archaeology"

if [ $# -gt 0 ]
then
    args=$*
else
    args=${default_args}
fi

if [ ! -f ${bkup_script} ] 
then
    echo "Can't find backup script."
    exit 2
fi

echo "Executing:" ${bkup_script} ${args} ${datasets}
${bkup_script} ${default_args} ${args} ${datasets}

