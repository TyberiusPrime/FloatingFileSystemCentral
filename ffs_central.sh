#!/bin/bash
cd "${0%/*}"
exec sudo -u ffs /bin/sh - << 'EOF'
python3 ffs_central.py 
EOF
