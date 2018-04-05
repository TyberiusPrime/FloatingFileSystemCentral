#!/usr/bin/python3

import sys
import subprocess
import os
import multiprocessing
import itertools
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
            'chmod_rights': 'u+rwX,g+rwX,o-rwx',
            'cores': 2
        }
    """)
    print('error: %s' % error)
    sys.exit(1)

def grouper(n, iterable, padvalue=None):
    "grouper(3, 'abcdefg', 'x') --> ('a','b','c'), ('d','e','f'), ('g','x','x')"
    return itertools.zip_longest(*[iter(iterable)]*n, fillvalue=padvalue)

def do_rsync(args):
    """the actual work horse"""
    sub_dir, recursive, cmd = args
    source_dir_path = os.path.abspath(os.path.join(cmd['source_path'], sub_dir))
    if False:
        for c_cmd, c_opt in [
            ('chown',  ['chown_user', 'chown_group']),
            ('chmod',  ['chmod_rights']),
        ]:
            try:
                c_opt = ":".join([cmd[x] for x in c_opt])
            except KeyError:
                continue
            c_command = ['sudo', c_cmd, c_opt]
            if recursive:
                blocks = [[source_dir_path, '-R']]
            else:
                blocks = grouper(100, [os.path.join(source_dir_path, f) for f in os.listdir(source_dir_path)])
            for b in blocks:
                p = subprocess.Popen(c_command + [x for x in b if x is not None], stdout=subprocess.PIPE, stderr=subprocess.PIPE )
                stdout, stderr = p.communicate()
                if p.returncode != 0:
                    return c_cmd, p.returncode, stdout, stderr
        
    rsync_cmd = ['sudo', 'rsync',
        '--rsync-path=rprsync',
        '--delete',
        '--delay-updates',
        '--omit-dir-times',
        '-ltxx',  # copy symlinks, times, don't cross file-systems
        '-perms',
        '--super',
        '--owner',
        '--group',
        ]
    if 'chmod_rights' in cmd:
        rsync_cmd += ['--chmod=' + cmd['chmod_rights']]
    if recursive:
        rsync_cmd.append('--recursive')
    else:
        rsync_cmd.append('--dirs')
    if 'target_ssh_cmd' in cmd:
        rsync_cmd.extend([
            '-e',
            " ".join(cmd['target_ssh_cmd']) 
        ])
    rsync_cmd.append(source_dir_path + '/')
    rsync_cmd.append("%s@%s:%s" % (
         cmd['target_user'],
         cmd['target_host'],
         os.path.join(cmd['target_path'], sub_dir)
        ))
    p=subprocess.Popen(rsync_cmd, stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE)
    stdout, stderr=p.communicate()
    stdout += ("\n" + " ".join(rsync_cmd)).encode('utf8')
    stdout += (b"\n rsync returncode: %i" % p.returncode)
    return 'rsync', p.returncode, stdout, stderr


def parallel_chown_chmod_and_rsync(cmd):
    def iter_subdirs():
        try:
            dirs = os.listdir(cmd['source_path'])
            yield '.', False, cmd
            for d in dirs:
                fd=os.path.join(cmd['source_path'], d)
                if os.path.isdir(fd) and not os.path.ismount(fd):
                    yield d + '/', True, cmd
        except PermissionError:  # source dir unreadable - fall back to non-parallel processing and let rsync handle the permission implications
            yield '.', True, cmd

    cores = cmd.get('cores', 2)
    if cores == -1:
        cores = None
    p=multiprocessing.Pool(cores)
    result=p.map(do_rsync, iter_subdirs())
    p.close()
    p.join()
    #result = map(chown_chmod_and_rsync, list(iter_subdirs()))
    rc=0
    for return_mode, rsync_return_code, stdout, stderr in result:
        if rsync_return_code != 0:
            rc=2
            sys.stderr.write(return_mode + "\n")
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
    # if not 'chmod_rights' in cmd:
        # cmd['chmod_rights'] = 'u+rwX,g+rwX,o+rwX'
    if '@' in cmd['target_path']:
        print_usage("Must not have an at in target_path")

    parallel_chown_chmod_and_rsync(cmd)

if __name__ == '__main__':
    main()
