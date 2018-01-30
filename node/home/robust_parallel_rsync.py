#!/usr/bin/python3

import sys
import subprocess
import os
import multiprocessing
import json


def print_usage(error):
    print("Pass a json formated command on stdin")
    print("""example:
        {
            'source_path': source_path,
            'target_path': target_path,
            'target_host': 'localhost',
            'target_user': 'ffs',
            'target_ssh_cmd': ['ssh', '-p', '223'],
            'chown_user': 'finkernagel',
            'chown_group': 'zti',
            'chmod_rights': 'o+rwX,g+rwX,a-rwx',
            'cores': 2
        }
    """)
    print('error: %s' % error)
    sys.exit(1)


def chown_chmod_and_rsync(args):
    """the actual work horse"""
    sub_dir, recursive, cmd = args
    rsync_cmd = ['rsync',
        '--rsync-path=rprsync',
        '--delete',
        '--delay-updates',
        '--omit-dir-times',
        '-ltx',  # copy symlinks, times, don't cross file-systems
        ]
    if recursive:
        rsync_cmd.append('--recursive')
    else:
        rsync_cmd.append('--dirs')
    if 'target_ssh_cmd' in cmd:
        rsync_cmd.extend([
            '-e',
            " ".join(cmd['target_ssh_cmd']) 
        ])
    rsync_cmd.append(os.path.join(cmd['source_path'], sub_dir))
    rsync_cmd.append("%s@%s:%s" % (
         cmd['target_user'],
         cmd['target_host'],
         os.path.join(cmd['target_path'], sub_dir)
        ))
    p=subprocess.Popen(rsync_cmd, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
    stdout, stderr=p.communicate()
    stdout += ("\n" + " ".join(rsync_cmd)).encode('utf8')
    return p.returncode, stdout, stderr


def parallel_chown_chmod_and_rsync(cmd):
    def iter_subdirs():
        for d in os.listdir(cmd['source_path']):
            fd=os.path.join(cmd['source_path'], d)
            if os.path.isdir(fd):
                yield d, True, cmd
        yield '.', False, cmd
    #p=multiprocessing.Pool(cmd.get('cores', 2))
    #result=p.map(chown_chmod_and_rsync, iter_subdirs())
    result = map(chown_chmod_and_rsync, list(iter_subdirs()))
    #p.join()
    rc=0
    for rsync_return_code, stdout, stderr in result:
        if rsync_return_code != 0:
            rc=2
            sys.stderr.write(stdout.decode('utf8'))
            sys.stderr.write(stderr.decode('utf8'))
    if rc == 0:
        print("OK")
    sys.exit(rc)


def main():
    try:
        cmd=sys.stdin.read()
        if not cmd:
            print_usage('No cmd passed')
        cmd=json.loads(cmd)
        if 'target_ssh_cmd' in cmd and not isinstance(cmd['target_ssh_cmd'], list):
            print_usage("target_ssh_cmd must be a list")
    except ValueError:
        print_usage('not valid json')

    parallel_chown_chmod_and_rsync(cmd)

if __name__ == '__main__':
    main()
