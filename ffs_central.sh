#!/bin/bash
cd "${0%/*}"
cmd=$1
#exec sudo -u ffs /bin/sh - << 'EOF'
#python3 ffs_central.py "$@"
#EOF
if [ "$(whoami)" != "ffs" ]; then
    sudo -u ffs /usr/bin/python3 ffs_central.py $@ 
else
    /usr/bin/python3 ffs_central.py $@ 
fi
