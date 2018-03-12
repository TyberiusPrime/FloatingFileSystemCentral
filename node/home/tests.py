#!/usr/bin/python3
import unittest
import os
import subprocess
import shutil
import tempfile
import json
import stat


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
    
def chmod(filename, bits):
    subprocess.check_call(['sudo', 'chmod', bits, filename])

def chown(filename, owner):
    subprocess.check_call(['sudo', 'chown', owner, filename])


class RPsTests(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        self.paths = set('/tmp/RPsTests')
        unittest.TestCase.__init__(self, *args, **kwargs)

    def ensure_path(self, path):
        if path in self.paths:
            raise ValueError("path reuse!")
        if os.path.exists(path):
            try:
                shutil.rmtree(path) 
            except PermissionError:
                subprocess.check_call(['sudo', 'chown', 'ffs', path, '-R'])
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
        })
        self.assertEqual(rc, 0)
        self.assertEqual(read_file(os.path.join(target_path, 'file1')), 'hello')

    def test_sub_dirs(self):
        source_path = '/tmp/RPsTests/sub_dirs_from'
        target_path = '/tmp/RPsTests/sub_dirs_to'
        self.ensure_path(source_path)
        self.ensure_path(target_path)
        os.makedirs(os.path.join(source_path, '1'))
        os.makedirs(os.path.join(source_path, '2'))
        write_file(os.path.join(source_path, 'file1'), 'hello')
        write_file(os.path.join(source_path, '1', 'file2'), 'hello1')
        write_file(os.path.join(source_path, '2', 'file3'), 'hello2')
        rc, stdout, stderr = run_rsync({
            'source_path': source_path,
            'target_path': target_path,
            'target_host': 'localhost',
            'target_ssh_cmd': ['ssh', '-p', '223'],
            'target_user': 'ffs',
            'chown_user': 'finkernagel',
            'chown_group': 'zti',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(target_path, '1', 'file2')), 'hello1')
        self.assertEqual(read_file(os.path.join(target_path, '2', 'file3')), 'hello2')


    def test_other_users_file(self):
        source_path = '/tmp/RPsTests/test_other_user_file_from'
        target_path = '/tmp/RPsTests/test_other_user_file_to'
        self.ensure_path(source_path)
        self.ensure_path(target_path)
        os.makedirs(os.path.join(source_path, '1'))
        os.makedirs(os.path.join(source_path, '2'))
        write_file(os.path.join(source_path, 'file1'), 'hello')
        write_file(os.path.join(source_path, '1', 'file2'), 'hello1')
        write_file(os.path.join(source_path, '2', 'file3'), 'hello2')
        chmod(os.path.join(source_path, '2', 'file3'), '0700')
        chown(os.path.join(source_path, '2', 'file3'), 'nobody')
        rc, stdout, stderr = run_rsync({
            'source_path': source_path,
            'target_path': target_path,
            'target_host': 'localhost',
            'target_ssh_cmd': ['ssh', '-p', '223'],
            'target_user': 'ffs',
            'chown_user': 'finkernagel',
            'chown_group': 'zti',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(target_path, '1', 'file2')), 'hello1')
        self.assertEqual(read_file(os.path.join(target_path, '2', 'file3')), 'hello2')

    def test_other_user_dir(self):
        source_path = '/tmp/RPsTests/test_other_user_dir_from'
        target_path = '/tmp/RPsTests/test_other_user_dir_to'
        self.ensure_path(source_path)
        self.ensure_path(target_path)
        os.makedirs(os.path.join(source_path, '1'))
        os.makedirs(os.path.join(source_path, '2'))
        write_file(os.path.join(source_path, 'file1'), 'hello')
        write_file(os.path.join(source_path, '1', 'file2'), 'hello1')
        write_file(os.path.join(source_path, '2', 'file3'), 'hello2')
        chmod(os.path.join(source_path, '2'), '0600')
        chown(os.path.join(source_path, '2'), 'nobody')
        rc, stdout, stderr = run_rsync({
            'source_path': source_path,
            'target_path': target_path,
            'target_host': 'localhost',
            'target_ssh_cmd': ['ssh', '-p', '223'],
            'target_user': 'ffs',
            'chown_user': 'finkernagel',
            'chown_group': 'zti',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(target_path, '1', 'file2')), 'hello1')
        self.assertEqual(read_file(os.path.join(target_path, '2', 'file3')), 'hello2')

    def test_target_unwritable_dir(self):
        source_path = '/tmp/RPsTests/test_target_unwritable_dir_from'
        target_path = '/tmp/RPsTests/test_target_unwritable_dir_to'
        self.ensure_path(source_path)
        self.ensure_path(target_path)
        os.makedirs(os.path.join(source_path, '1'))
        os.makedirs(os.path.join(source_path, '2'))
        write_file(os.path.join(source_path, 'file1'), 'hello')
        write_file(os.path.join(source_path, '1', 'file2'), 'hello1')
        write_file(os.path.join(source_path, '2', 'file3'), 'hello2')

        os.makedirs(os.path.join(target_path, '2'))
        chmod(os.path.join(target_path, '2'), '0700')
        chown(os.path.join(target_path, '2'), 'nobody')
        rc, stdout, stderr = run_rsync({
            'source_path': source_path,
            'target_path': target_path,
            'target_host': 'localhost',
            'target_ssh_cmd': ['ssh', '-p', '223'],
            'target_user': 'ffs',
            'chown_user': 'finkernagel',
            'chown_group': 'zti',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(target_path, '1', 'file2')), 'hello1')
        self.assertEqual(read_file(os.path.join(target_path, '2', 'file3')), 'hello2')

    def test_target_unwritable_dir_nested(self):
        source_path = '/tmp/RPsTests/test_target_unwritable_dir_nested_from'
        target_path = '/tmp/RPsTests/test_target_unwritable_dir_nested_to'
        self.ensure_path(source_path)
        self.ensure_path(target_path)
        os.makedirs(os.path.join(source_path, '1'))
        os.makedirs(os.path.join(source_path, '2'))
        os.makedirs(os.path.join(source_path, '2', '2'))
        write_file(os.path.join(source_path, 'file1'), 'hello')
        write_file(os.path.join(source_path, '1', 'file2'), 'hello1')
        write_file(os.path.join(source_path, '2', '2', 'file3'), 'hello2')

        os.makedirs(os.path.join(target_path, '2', '2'))
        chmod(os.path.join(target_path, '2', '2'), '0700')
        chown(os.path.join(target_path, '2', '2'), 'nobody')
        rc, stdout, stderr = run_rsync({
            'source_path': source_path,
            'target_path': target_path,
            'target_host': 'localhost',
            'target_ssh_cmd': ['ssh', '-p', '223'],
            'target_user': 'ffs',
            'chown_user': 'finkernagel',
            'chown_group': 'zti',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(target_path, '1', 'file2')), 'hello1')
        self.assertEqual(read_file(os.path.join(target_path, '2', 'file3')), 'hello2')

    def test_target_unwritable_dir_deletion(self):
        source_path = '/tmp/RPsTests/test_target_unwritable_dir_from'
        target_path = '/tmp/RPsTests/test_target_unwritable_dir_to'
        self.ensure_path(source_path)
        self.ensure_path(target_path)
        os.makedirs(os.path.join(source_path, '1'))
        os.makedirs(os.path.join(source_path, '2'))
        write_file(os.path.join(source_path, 'file1'), 'hello')
        write_file(os.path.join(source_path, '1', 'file2'), 'hello1')
        write_file(os.path.join(source_path, '2', 'file3'), 'hello2')

        os.makedirs(os.path.join(target_path, '3'))
        chmod(os.path.join(target_path, '3'), '0700')
        chown(os.path.join(target_path, '3'), 'nobody')
        rc, stdout, stderr = run_rsync({
            'source_path': source_path,
            'target_path': target_path,
            'target_host': 'localhost',
            'target_ssh_cmd': ['ssh', '-p', '223'],
            'target_user': 'ffs',
            'chown_user': 'finkernagel',
            'chown_group': 'zti',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(target_path, '1', 'file2')), 'hello1')
        self.assertEqual(read_file(os.path.join(target_path, '2', 'file3')), 'hello2')
        self.assertFalse(os.path.exists(os.path.join(target_path, '3')))



    def test_target_unwritable_file(self):
        source_path = '/tmp/RPsTests/target_unwritable_file_from'
        target_path = '/tmp/RPsTests/target_unwritable_file_to'
        self.ensure_path(source_path)
        self.ensure_path(target_path)
        os.makedirs(os.path.join(source_path, '1'))
        os.makedirs(os.path.join(source_path, '2'))
        write_file(os.path.join(source_path, 'file1'), 'hello')
        write_file(os.path.join(source_path, '1', 'file2'), 'hello1')
        write_file(os.path.join(source_path, '2', 'file3'), 'hello2')

        os.makedirs(os.path.join(target_path, '2'))
        write_file(os.path.join(target_path, '2', 'file3'), 'hello2b')
        chmod(os.path.join(target_path, '2', 'file3'), '0000')
        chown(os.path.join(target_path, '2', 'file3'), 'nobody')
        rc, stdout, stderr = run_rsync({
            'source_path': source_path,
            'target_path': target_path,
            'target_host': 'localhost',
            'target_ssh_cmd': ['ssh', '-p', '223'],
            'target_user': 'ffs',
            'chown_user': 'finkernagel',
            'chown_group': 'zti',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(target_path, '1', 'file2')), 'hello1')
        self.assertEqual(read_file(os.path.join(target_path, '2', 'file3')), 'hello2')

        mode = os.stat(os.path.join(target_path, '2', 'file3'))[stat.ST_MODE]
        #make sure the files are readable afterwards...
        self.assertTrue(mode & stat.S_IRUSR)
        self.assertTrue(mode & stat.S_IRGRP)
        self.assertTrue(mode & stat.S_IROTH)
        self.assertTrue(mode & stat.S_IWUSR)
        self.assertTrue(mode & stat.S_IWGRP)
        self.assertTrue(mode & stat.S_IWOTH)
     

        def test_executable_stays(self):
            source_path = '/tmp/RPsTests/test_executable_stays_from'
            target_path = '/tmp/RPsTests/test_executable_stays_to'
            self.ensure_path(source_path)
            self.ensure_path(target_path)
            os.makedirs(os.path.join(source_path, '1'))
            os.makedirs(os.path.join(source_path, '2'))
            write_file(os.path.join(source_path, 'file1'), 'hello')
            write_file(os.path.join(source_path, '1', 'file2'), 'hello1')
            write_file(os.path.join(source_path, '2', 'file3'), 'hello2')
            chmod(os.path.join(source_path, '2'), 'o=x,g=x,a=x')
            chown(os.path.join(source_path, '2'), 'nobody')
            rc, stdout, stderr = run_rsync({
                'source_path': source_path,
                'target_path': target_path,
                'target_host': 'localhost',
                'target_ssh_cmd': ['ssh', '-p', '223'],
                'target_user': 'ffs',
                'chown_user': 'finkernagel',
                'chown_group': 'zti',
            })
            self.assertEqual(rc, 0)
            self.assertEqual(read_file(os.path.join(target_path, 'file1')), 'hello')
            self.assertEqual(read_file(os.path.join(target_path, '1', 'file2')), 'hello1')
            self.assertEqual(read_file(os.path.join(target_path, '2', 'file3')), 'hello2')
            mode = os.stat(os.path.join(target_path, '2', 'file3'))[stat.ST_MODEO]
            self.assertTrue(mode & stat.S_IXUSR)
            self.assertTrue(mode & stat.S_IXGRP)
            self.assertTrue(mode & stat.S_IXOTH)
        
        def test_cant_read_source_dir():
            raise NotImplementedError()
        
        def test_x_survives_wrong_user(self):
            raise NotImplementedError()
            pass

        def test_over_9000_files(self):
            #mostly a test of the chown expansion...
            source_path = '/tmp/RPsTests/test_over_9000_from'
            target_path = '/tmp/RPsTests/test_over_9000_to'
            self.ensure_path(source_path)
            self.ensure_path(target_path)
            for i in xrange(0, 9000):
                write_file(os.path.join(source_path, str(i)), str(i))
            rc, stdout, stderr = run_rsync({
                'source_path': source_path,
                'target_path': target_path,
                'target_host': 'localhost',
                'target_ssh_cmd': ['ssh', '-p', '223'],
                'target_user': 'ffs',
                'chown_user': 'finkernagel',
                'chown_group': 'zti',
            })
            self.assertEqual(rc, 0)
            self.assertEqual(len(os.listdir(target_path)), 9000)

    def test_at_in_target_raises(self):
        raise NotImplementedError()

    def test_other_side_user_correct_afterwards(self):
        raise NotImplementedError()
        pass

if __name__ == '__main__':
    unittest.main()