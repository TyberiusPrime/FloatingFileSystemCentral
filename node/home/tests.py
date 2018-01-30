#!/usr/bin/python3
import unittest
import os
import subprocess
import shutil
import tempfile
import json


def run_rsync(cmd, encode=True, show_errors=True):
    if encode:
        cmd = json.dumps(cmd)
    p = subprocess.Popen('./robust_parallel_rsync.py', stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate(cmd.encode('utf-8'))
    if p.returncode != 0 and show_errors:
        print(stdout.decode('utf-8'))
        print(stderr.decode('utf-8'))
    return p.returncode, stdout, stderr

def read_file(filename):
    with open(filename) as op:
        return op.read()

def write_file(filename, content):
    with open(filename, 'w') as op:
        op.write(content)

class RPsTests(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        self.paths = set('/tmp/RPsTests')
        unittest.TestCase.__init__(self, *args, **kwargs)

    def ensure_path(self, path):
        if os.path.exists(path):
            shutil.rmtree(path) 
        os.makedirs(path)
        self.paths.add(path)
    
    def tearDown(self):
        for p in self.paths:
            try:
                #shutil.rmtree(p)
                pass
            except:
                pass

    def test_non_json_input_error_return_1(self):
        return_code, stdout, stderr = run_rsync("SHU", False, False)
        self.assertEqual(return_code, 1)

    def test_simple(self):
        source_path = '/tmp/RPsTests/simple_from'
        target_path = '/tmp/RPsTests/simple_to'
        self.ensure_path(source_path)
        self.ensure_path(target_path)
        write_file(os.path.join(source_path, 'file1'), 'hello')
        rc, stdout, stderr = run_rsync({
            'source_path': source_path,
            'target_path': target_path,
            'target_host': 'localhost',
            'target_ssh_cmd': ['ssh', '-p', '223'],
            'target_user': 'ffs',
            'chown_user': 'finkernagel',
            'chown_group': 'zti',
            'chmod_rights': 'o+rwX,g+rwX,a-rwx'
        })
        self.assertEqual(rc, 0)
        self.assertEqual(read_file(os.path.join(target_path, 'file1')), 'hello')


if __name__ == '__main__':
    unittest.main()