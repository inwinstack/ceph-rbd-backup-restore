#!/bin/bash

python_path='python'

etc_path='/etc/rbd_backup_restore'
opt_path='/opt/rbd_backup_restore'
bin_path='/bin'


mkdir -p ${etc_path}
cp -afpR ./conf/config.ini ${etc_path}/config.ini
cp -afpR ./conf/backup_list.yaml ${etc_path}/backup_list.yaml

find . -name "*.pyc" -exec rm -f {} \;
#find . -name \*.pyc -delete

mkdir -p ${opt_path}
cp -afpR ./rbd_backup.py ${opt_path}
cp -afpR ./rbd_restore.py ${opt_path}
cp -afpR ./backup_show.py ${opt_path}
cp -afpR ./backup ${opt_path}
cp -afpR ./restore ${opt_path}
cp -afpR ./lib ${opt_path}
cp -afpR ./task ${opt_path}


cp -afpR ./rbd_brctl.py ./rbd-brctl
sed -i "s~_PYTHON_PATH_~${python_path}~g" ./rbd-brctl
sed -i "s~_INSTALL_PATH_~${opt_path}~g"   ./rbd-brctl

chmod +x ./rbd-brctl
cp -afpR ./rbd-brctl ${bin_path}

rm -rf ./rbd-brctl
