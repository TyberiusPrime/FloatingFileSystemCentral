#!/bin/bash
# helper script to manually start ffs_central without systemd
cd -P "${0%/*}"
d="${0%/*}"
if [[ -f "$d/python3" ]]; then 
	cmd=$d/python3
else
	cmd=/opt/pipx/FloatingFileSystemCentral==editable/bin/python3
fi
	
#exec sudo -u ffs /bin/sh - << 'EOF'
#python3 ffs_central.py "$@"
#EOF
if [ "$(whoami)" != "ffs" ]; then
    sudo -u ffs $cmd ffs_central.py $@ 
else
    $cmd ffs_central.py $@ 
fi
