#!/bin/bash
cd "${0%/*}"
cmd=$1
#exec sudo -u ffs /bin/sh - << 'EOF'
#python3 ffs_central.py "$@"
#EOF
if [ "$(whoami)" != "ffs" ]; then
    sudo -u ffs python3 ffs_central.py $@ 
else
    python3 ffs_central.py $@ 
fi
