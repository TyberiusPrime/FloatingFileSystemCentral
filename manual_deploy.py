import pwd  # pylint: disable=C0413
import os
os.chdir(os.path.abspath(os.path.dirname(__file__)))
import subprocess
user = pwd.getpwuid(os.getuid())[0]
if user != 'ffs':
    print(os.getuid())
    raise ValueError(
        "Must be started as user ffs - use ffs.sh - was %s" % user)
import central.config as config
cfg = config.Config()
for node_name in sorted(cfg.get_nodes()):
    print(node_name)
    for fn in 'ssh.py', 'node.py':
        subprocess.check_call(['scp', '-P', '223', '-o', 'StrictHostKeyChecking=no', '-i',
         '/home/ffs/.ssh/id_rsa', './node/home/' + fn, 
        ('%s:/home/ffs/' % node_name) + fn])
    print('copied to', node_name)
