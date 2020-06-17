#!/bin/bash
cd "${0%/*}"
d="${0%/*}"
#exec sudo -u ffs /bin/sh - << 'EOF'
#python3 ffs_central.py "$@"
#EOF
if [ "$(whoami)" != "ffs" ]; then
    sudo -u ffs $d/python3 ffs_central.py $@ 
else
    $d/python3 ffs_central.py $@ 
fi
