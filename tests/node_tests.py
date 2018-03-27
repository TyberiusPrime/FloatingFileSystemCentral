#!/usr/bin/python3
import pprint
import unittest
import os
import subprocess
import shutil
import tempfile
import json
import stat
import pwd
import grp
import pwd
user = pwd.getpwuid(os.getuid())[0]
if user != 'ffs':
    print(os.getuid())
    raise ValueError(
        "Node tests must run as user ffs - was %s" % user)
import sys
sys.path.insert(0, '../')
sys.path.insert(0, '../node/home')
from central import config
import node


target_ssh_cmd = config.config.get_ssh_cmd() + ['-i', '/home/ffs/.ssh/id_rsa']


def run_rsync(cmd, encode=True, show_errors=True):
    if encode:
        cmd = json.dumps(cmd)
    p = subprocess.Popen('../node/home/robust_parallel_rsync.py', stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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


def get_file_rights(filename):
    return os.stat(filename)[stat.ST_MODE]


def get_file_user(filename):
    return pwd.getpwuid(os.stat(filename).st_uid).pw_name


def get_file_group(filename):
    return grp.getgrgid(os.stat(filename).st_gid).gr_name


def chmod(filename, bits):
    subprocess.check_call(['sudo', 'chmod', bits, filename])


def chown(filename, owner, group=None):
    if group:
        subprocess.check_call(['sudo', 'chown', "%s:%s" %
                               (owner, group), filename])
    else:
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
                subprocess.check_call(['sudo', 'chmod', '+rwX', path, '-R'])
                shutil.rmtree(path)
        os.makedirs(path)
        self.paths.add(path)

    def tearDown(self):
        for p in self.paths:
            try:
                # shutil.rmtree(p)
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
            'target_host': '127.0.0.1',
            'target_ssh_cmd': target_ssh_cmd,
            'target_user': 'ffs',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(
            read_file(os.path.join(target_path, 'file1')), 'hello')

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
            'target_host': '127.0.0.1',
            'target_ssh_cmd': target_ssh_cmd,
            'target_user': 'ffs',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(
            read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(
            target_path, '1', 'file2')), 'hello1')
        self.assertEqual(read_file(os.path.join(
            target_path, '2', 'file3')), 'hello2')

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
        chmod(os.path.join(source_path, '2', 'file3'), '0744')
        chown(os.path.join(source_path, '2', 'file3'), 'nobody')
        rc, stdout, stderr = run_rsync({
            'source_path': source_path,
            'target_path': target_path,
            'target_host': '127.0.0.1',
            'target_ssh_cmd': target_ssh_cmd,
            'target_user': 'ffs',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(
            read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(
            target_path, '1', 'file2')), 'hello1')
        # file should have been chowned, since we set chown_user
        self.assertEqual(get_file_user(os.path.join(
            target_path, '2', 'file3')), 'nobody')
        self.assertEqual(get_file_group(os.path.join(target_path, '2', 'file3')),
                         get_file_group(os.path.join(source_path, '2', 'file3')))
        self.assertEqual(get_file_rights(os.path.join(
            target_path, '2', 'file3')) & 0o777, 0o744)
        self.assertEqual(read_file(os.path.join(
            target_path, '2', 'file3')), 'hello2')

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
        chmod(os.path.join(source_path, '2'), '0650')
        chown(os.path.join(source_path, '2'), 'nobody')
        rc, stdout, stderr = run_rsync({
            'source_path': source_path,
            'target_path': target_path,
            'target_host': '127.0.0.1',
            'target_ssh_cmd': target_ssh_cmd,
            'target_user': 'ffs',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(
            read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(
            target_path, '1', 'file2')), 'hello1')
        self.assertEqual(get_file_user(
            os.path.join(target_path, '2',)), 'nobody')
        self.assertEqual(get_file_group(os.path.join(target_path, '2',)),
                         get_file_group(os.path.join(source_path, '2',)))
        self.assertEqual(get_file_rights(
            os.path.join(target_path, '2')) & 0o777, 0o650)

        self.assertEqual(read_file(os.path.join(
            target_path, '2', 'file3')), 'hello2')

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
            'target_host': '127.0.0.1',
            'target_ssh_cmd': target_ssh_cmd,
            'target_user': 'ffs',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(
            read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(
            target_path, '1', 'file2')), 'hello1')
        self.assertEqual(read_file(os.path.join(
            target_path, '2', 'file3')), 'hello2')

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
            'target_host': '127.0.0.1',
            'target_ssh_cmd': target_ssh_cmd,
            'target_user': 'ffs',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(
            read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(
            target_path, '1', 'file2')), 'hello1')
        self.assertEqual(read_file(os.path.join(
            target_path, '2', '2', 'file3')), 'hello2')

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
            'target_host': '127.0.0.1',
            'target_ssh_cmd': target_ssh_cmd,
            'target_user': 'ffs',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(
            read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(
            target_path, '1', 'file2')), 'hello1')
        self.assertEqual(read_file(os.path.join(
            target_path, '2', 'file3')), 'hello2')
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
            'target_host': '127.0.0.1',
            'target_ssh_cmd': target_ssh_cmd,
            'target_user': 'ffs',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(
            read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(
            target_path, '1', 'file2')), 'hello1')
        self.assertEqual(read_file(os.path.join(
            target_path, '2', 'file3')), 'hello2')

        should_rights = get_file_rights(
            os.path.join(source_path, '2', 'file3'))
        mode = get_file_rights(os.path.join(target_path, '2', 'file3'))
        # just as unreadable as it was before...
        self.assertEqual(mode & 0o777, should_rights & 0o777)

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
        chmod(os.path.join(source_path, '2', 'file3'), 'o=rx,g=rx,a=rx')
        chown(os.path.join(source_path, '2', 'file3'), 'nobody')
        rc, stdout, stderr = run_rsync({
            'source_path': source_path,
            'target_path': target_path,
            'target_host': '127.0.0.1',
            'target_ssh_cmd': target_ssh_cmd,
            'target_user': 'ffs',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(
            read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(
            target_path, '1', 'file2')), 'hello1')
        self.assertEqual(read_file(os.path.join(
            target_path, '2', 'file3')), 'hello2')
        mode = get_file_rights(os.path.join(target_path, '2', 'file3'))
        self.assertTrue(mode & stat.S_IXUSR)
        self.assertTrue(mode & stat.S_IXGRP)
        self.assertTrue(mode & stat.S_IXOTH)

    def test_cant_read_source_dir(self):
        source_path = '/tmp/RPsTests/test_cant_read_source_dir_from'
        target_path = '/tmp/RPsTests/test_cant_read_source_dir_to'
        self.ensure_path(source_path)
        self.ensure_path(target_path)
        import time
        h = 'hello%s' % time.time()
        write_file(os.path.join(source_path, 'file1'), h)
        chmod(source_path, "0")
        rc, stdout, stderr = run_rsync({
            'source_path': source_path,
            'target_path': target_path,
            'target_host': '127.0.0.1',
            'target_ssh_cmd': target_ssh_cmd,
            'target_user': 'ffs',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(get_file_rights(target_path) & 0o777, 0)
        chmod(target_path, '+rX')
        self.assertEqual(read_file(os.path.join(target_path, 'file1')), h)

        # try it again straight away
        chmod(target_path, '-rX')
        rc, stdout, stderr = run_rsync({
            'source_path': source_path,
            'target_path': target_path,
            'target_host': '127.0.0.1',
            'target_ssh_cmd': target_ssh_cmd,
            'target_user': 'ffs',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(get_file_rights(target_path) & 0o777, 0)
        chmod(target_path, '+rX')
        self.assertEqual(read_file(os.path.join(target_path, 'file1')), h)

    def test_over_9000_files(self):
        # mostly a test of the chown expansion...
        source_path = '/tmp/RPsTests/test_over_9000_from'
        target_path = '/tmp/RPsTests/test_over_9000_to'
        self.ensure_path(source_path)
        self.ensure_path(target_path)
        for i in range(0, 9000):
            write_file(os.path.join(source_path, str(i)), str(i))
        rc, stdout, stderr = run_rsync({
            'source_path': source_path,
            'target_path': target_path,
            'target_host': '127.0.0.1',
            'target_ssh_cmd': target_ssh_cmd,
            'target_user': 'ffs',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(len(os.listdir(target_path)), 9000)

    def test_at_in_target_raises(self):
        source_path = '/tmp/RPsTests/test_source@in_target'
        target_path = '/tmp/RPsTests/test_target@in_target'
        self.ensure_path(source_path)
        self.ensure_path(target_path)
        rc, stdout, stderr = run_rsync({
            'source_path': source_path,
            'target_path': target_path,
            'target_host': '127.0.0.1',
            'target_ssh_cmd': target_ssh_cmd,
            'target_user': 'ffs',
        }, show_errors=False)
        self.assertEqual(rc, 1)

    def test_other_side_user_correct_afterwards(self):
        source_path = '/tmp/RPsTests/test_user_correct_source'
        target_path = '/tmp/RPsTests/test_user_correct_target'
        self.ensure_path(source_path)
        self.ensure_path(target_path)
        fn = os.path.join(source_path, 'file1')
        write_file(fn, 'hello')
        os.chmod(fn, 0o765)
        self.assertEqual(get_file_rights(fn) & 0o777, 0o765)
        nobody = pwd.getpwnam('nobody')
        chown(fn, 'nobody', 'nogroup')
        rc, stdout, stderr = run_rsync({
            'source_path': source_path,
            'target_path': target_path,
            'target_host': '127.0.0.1',
            'target_ssh_cmd': target_ssh_cmd,
            'target_user': 'ffs',
        }, show_errors=True)
        self.assertEqual(rc, 0)
        self.assertEqual(get_file_rights(fn) & 0o777, 0o765)
        fn_target = os.path.join(target_path, 'file1')
        self.assertEqual(get_file_rights(fn_target) & 0o777, 0o765)
        self.assertEqual(get_file_user(fn_target), 'nobody')
        self.assertEqual(get_file_group(fn_target), 'nogroup')


def touch(filename):
    with open(filename, 'w'):
        pass


class NodeTests(unittest.TestCase):
    @classmethod
    def get_pool(cls):
        if not hasattr(cls, '_pool'):
            lines = subprocess.check_output(['sudo','zpool','status']).split(b"\n")  # use status, it's in the sudoers
            for l in lines:
                l = l.strip()
                if l.startswith(b'pool:'):
                    pool = l[l.find(b':') + 2:].strip().decode('utf-8')
                    cls._pool =  pool + '/'
                    break
            else:
                raise ValueError("Could not find a zpool to create .ffs_testing zfs on")
        return cls._pool

    @classmethod
    def get_test_prefix(cls):
        return cls.get_pool() + '.ffs_testing_from/'
        
    @classmethod
    def get_test_prefix2(cls):
        return cls.get_pool() + '.ffs_testing_to/'


    @classmethod
    def setUpClass(cls):
        p = subprocess.Popen(['sudo', 'zfs', 'destroy', cls.get_test_prefix()[:-1], '-R'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        p = subprocess.Popen(['sudo', 'zfs', 'destroy', cls.get_test_prefix2()[:-1], '-R'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        
        subprocess.check_call(
            ['sudo', 'zfs', 'create', cls.get_test_prefix()[:-1]])
        subprocess.check_call(
            ['sudo', 'zfs', 'ffs:root=on', cls.get_test_prefix()[:-1]])
        subprocess.check_call(
            ['sudo', 'zfs', 'create', cls.get_test_prefix() + 'inherit_test'] )
        subprocess.check_call(
            ['sudo', 'zfs', 'create', cls.get_test_prefix2()[:-1]])
        subprocess.check_call(
            ['sudo', 'zfs', 'ffs:root=on', cls.get_test_prefix2()[:-1]])
        shutil.copy('../node/home/node.py','/home/ffs/node.py')
        shutil.copy('../node/home/ssh.py','/home/ffs/ssh.py')



    @classmethod
    def tearDownClass(cls):
        p = subprocess.Popen(['sudo', 'zfs', 'destroy', cls.get_test_prefix()[:-1], '-R'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        p = subprocess.Popen(['sudo', 'zfs', 'destroy', cls.get_test_prefix2()[:-1], '-R'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()


    def dispatch(self, in_msg):
        if not 'storage_prefix' in in_msg:
            in_msg['storage_prefix'] = '/' + self.get_test_prefix()[:-1]
        return node.dispatch(in_msg)
    
    def list_ffs(self, strip_prefix=False, include_testing=False):
        return node.list_ffs('/' + self.get_test_prefix()[:-1], True, True)

    def test_invalid_message_error_reply(self):
        in_msg = {"msg": 'no such message'}
        out_msg = self.dispatch(in_msg)
        self.assertTrue('error' in out_msg)

    def test_no_msg(self):
        in_msg = {}
        out_msg = self.dispatch(in_msg)
        self.assertTrue('error' in out_msg)
        self.assertTrue('content' in out_msg)

    def test_list_ffs(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'snapshot', self.get_test_prefix()[:-1] + '@a']) # so we always have a snapshot in the list ;)
        subprocess.check_call(
            ['sudo', 'zfs', 'create', self.get_test_prefix() + 'list_test'])
        subprocess.check_call(
            ['sudo', 'zfs', 'snapshot', self.get_test_prefix() + 'list_test@a']) # so we always have a snapshot in the list ;)
        in_msg = {'msg': 'list_ffs'}
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertEqual(out_msg['msg'], 'ffs_list')
        zfs_list = subprocess.check_output(
            ['sudo', 'zfs', 'list', '-H']).decode('utf-8').strip().split("\n")
        zfs_list = [x.split("\t")[0] for x in zfs_list]
        zfs_list = [x for x in zfs_list if x.startswith(self.get_test_prefix()) and not x.startswith(self.get_test_prefix() + '.')]
        l = len(self.get_test_prefix())
        zfs_list = [x[l:] for x in zfs_list]
        self.assertTrue(zfs_list)
        any_snapshots = False
        for x in zfs_list:
            if x not in out_msg['ffs']:
                pprint.pprint(out_msg)
                print('missing "%s"' % x)
            self.assertTrue(x in out_msg['ffs'])
            if out_msg['ffs'][x]['snapshots']:
                any_snapshots = True
            self.assertTrue('creation' in out_msg['ffs'][x]['properties'])
            self.assertEqual(out_msg['ffs'][x]['properties'][
                             'type'], 'filesystem')
        self.assertTrue(any_snapshots)
        self.assertTrue('list_test' in out_msg['ffs']) # the root
        self.assertTrue('a' in out_msg['ffs']['list_test']['snapshots']) # the root
        self.assertFalse('' in out_msg['ffs']) # can't have the root in this!
        self.assertEqual(out_msg['ffs']['inherit_test']['properties'].get('ffs:root','-'), '-')

    def test_list_ffs_invalid_storage_prefix(self):
        self.assertFalse(os.path.exists('/doesnotexist'))
        in_msg = {'msg': 'list_ffs', 'storage_prefix': '/doesnotexist'}
        out_msg = self.dispatch(in_msg)
        self.assertError(out_msg, 'storage prefix not a ZFS')

    def test_list_ffs_invalid_storage_prefix_but_a_zfs(self):
        in_msg = {'msg': 'list_ffs', 'storage_prefix': '/' + self.get_pool()[:-1]}
        out_msg = self.dispatch(in_msg)
        self.assertError(out_msg, 'ffs:root not set on storage_prefix')
      
    def test_set_properties(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', self.get_test_prefix() + 'one'])
        subprocess.check_call(
            ['sudo', 'zfs', 'set', 'ffs:test=one', self.get_test_prefix() + 'one'])
        subprocess.check_call(
            ['sudo', 'zfs', 'snapshot', self.get_test_prefix() + 'one@a'])

        in_msg = {'msg': 'set_properties', 'ffs': 'one',
                  'properties': {'ffs:test': 'two'}}
        self.assertEqual(node.get_zfs_property(
            NodeTests.get_test_prefix() + 'one', 'ffs:test'), 'one')
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertEqual(node.get_zfs_property(
            NodeTests.get_test_prefix() + 'one', 'ffs:test'), 'two')
        self.assertEqual(out_msg['msg'], 'set_properties_done')
        self.assertEqual(out_msg['ffs'], 'one')
        self.assertEqual(out_msg['properties']['ffs:test'], 'two')
        # as proxy for the remaining properties...
        self.assertTrue('creation' in out_msg['properties'])

    def test_set_properties_invalid_ffs(self):
        in_msg = {'msg': 'set_properties', 'ffs': 'does_not_exist',
                  'properties': {'ffs:test': 'two'}}
        out_msg = self.dispatch(in_msg)
        self.assertError(out_msg)
        self.assertTrue('invalid ffs' in out_msg['content'])

    def test_set_properties_invalid_property_name(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', self.get_test_prefix() + 'oneB'])
        in_msg = {'msg': 'set_properties', 'ffs': 'oneB',
                  'properties': {' ffs:test': 'two'}}
        out_msg = self.dispatch(in_msg)
        self.assertError(out_msg,'invalid property name')

    def test_set_properties_invalid_value(self):
        # max length is 1024
        subprocess.check_call(
            ['sudo', 'zfs', 'create', self.get_test_prefix() + 'oneC'])
        in_msg = {'msg': 'set_properties', 'ffs': 'oneC',
                  'properties': {'ffs:test': 't' * 1025}}
        out_msg = self.dispatch(in_msg)
        self.assertError(out_msg, 'invalid property value')

    def assertNotError(self, msg):
        if 'error' in msg:
            pprint.pprint(msg)
        self.assertFalse('error' in msg)

    def assertError(self, msg, content_check=None):
        if 'error' not in msg:
            pprint.pprint(msg)
        self.assertTrue('error' in msg)
        if content_check:
            if not content_check in msg['content']:
                pprint.pprint(msg)
            self.assertTrue(content_check in msg['content'])

    def test_new(self):
        in_msg = {'msg': 'new', 'ffs': 'four',
                  'properties': {'ffs:test1': 'one', 'ffs:test2': 'two'}}
        self.assertFalse('four' in self.list_ffs(True, True))
        out_msg = self.dispatch(in_msg)
        self.assertTrue('four' in self.list_ffs(True, True))
        self.assertNotError(out_msg)
        self.assertEqual(out_msg['msg'], 'new_done')
        self.assertEqual(out_msg['ffs'], 'four')
        self.assertEqual(out_msg['properties']['ffs:test1'], 'one')
        self.assertEqual(out_msg['properties']['ffs:test2'], 'two')
        # as proxy for the remaining properties...
        self.assertTrue('creation' in out_msg['properties'])

    def get_snapshots(self):
        snapshot_list = subprocess.check_output(
            ['sudo', 'zfs', 'list', '-t', 'snapshot', '-H']).decode("utf-8").strip().split("\n")
        return [x.split("\t")[0] for x in snapshot_list]

    def assertSnapshot(self, ffs, snapshot, use_second=False):
        if use_second:
            full_path = NodeTests.get_test_prefix2() + ffs + '@' + snapshot
        else:
            full_path = NodeTests.get_test_prefix() + ffs + '@' + snapshot
        sn = self.get_snapshots()
        self.assertTrue(full_path in sn)

    def assertNotSnapshot(self, ffs, snapshot, use_second=False):
        if use_second:
            full_path = NodeTests.get_test_prefix2() + ffs + '@' + snapshot
        else:
            full_path = NodeTests.get_test_prefix() + ffs + '@' + snapshot
        sn = self.get_snapshots()
        self.assertFalse(full_path in sn)

    def test_capture(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'five'])
        in_msg = {'msg': 'capture',
                  'ffs': 'five', 'snapshot': 'b'}
        self.assertNotSnapshot('five', 'b')
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertSnapshot('five', 'b')
        self.assertEqual(out_msg['msg'], 'capture_done')
        self.assertEqual(out_msg['ffs'], 'five')
        self.assertEqual(out_msg['snapshot'], 'b')

    def test_snapshot_exists(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'six'])
        subprocess.check_call(
            ['sudo', 'zfs', 'snapshot', NodeTests.get_test_prefix() + 'six@a'])
        in_msg = {'msg': 'capture', 'ffs': 'six', 'snapshot': 'a'}
        self.assertSnapshot('six', 'a')
        out_msg = self.dispatch(in_msg)
        self.assertError(out_msg)
        self.assertTrue('already exists' in out_msg['content'])

    def test_snapshot_invalid_ffs(self):
        in_msg = {'msg': 'capture',
                  'ffs': 'doesnotexist', 'snapshot': 'a'}
        out_msg = self.dispatch(in_msg)
        self.assertError(out_msg)

    def test_remove(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'two'])
        in_msg = {'msg': 'remove', 'ffs': 'two'}
        self.assertTrue('two' in self.list_ffs(True, True))
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertFalse('two' in self.list_ffs(True, True))
        self.assertEqual(out_msg['msg'], 'remove_done')
        self.assertEqual(out_msg['ffs'], 'two')

    def test_remove_invalid_ffs(self):
        in_msg = {'msg': 'remove', 'ffs': 'does_not_exist'}
        self.assertFalse(
            'does_not_exist' in self.list_ffs(True, True))
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertEqual(out_msg['msg'], 'remove_failed')
        self.assertEqual(out_msg['reason'], 'target_does_not_exists')

    def test_remove_snapshot(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'three'])
        subprocess.check_call(
            ['sudo', 'chmod', '777', '/' + NodeTests.get_test_prefix() + 'three'])
        touch('/' + NodeTests.get_test_prefix() + 'three/file_a')
        subprocess.check_call(
            ['sudo', 'zfs', 'snapshot', NodeTests.get_test_prefix() + 'three@a'])
        touch('/' + NodeTests.get_test_prefix() + 'three/file_b')
        subprocess.check_call(
            ['sudo', 'zfs', 'snapshot', NodeTests.get_test_prefix() + 'three@b'])
        touch('/' + NodeTests.get_test_prefix() + 'three/file_c')
        subprocess.check_call(
            ['sudo', 'zfs', 'snapshot', NodeTests.get_test_prefix() + 'three@c'])

        in_msg = {'msg': 'remove_snapshot',
                  'ffs': 'three', 'snapshot': 'c',
                  }
        self.assertSnapshot('three', 'a')
        self.assertSnapshot('three', 'b')
        self.assertSnapshot('three', 'c')
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertEqual(out_msg['msg'], 'remove_snapshot_done')
        self.assertEqual(out_msg['ffs'], 'three')
        self.assertEqual(out_msg['snapshots'], ['a', 'b'],)
        self.assertEqual(out_msg['snapshot'], 'c',)
        self.assertSnapshot('three', 'a')
        self.assertSnapshot('three', 'b')
        self.assertNotSnapshot('three', 'c')

        in_msg = {'msg': 'remove_snapshot',
                  'ffs': 'three', 'snapshot': 'a'}
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertEqual(out_msg['snapshots'], ['b'],)
        self.assertNotSnapshot('three', 'a')
        self.assertSnapshot('three', 'b')
        self.assertNotSnapshot('three', 'c')

    def test_remove_snapshot_invalid_snapshot(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'threeb'])
        in_msg = {'msg': 'remove_snapshot',
                  'ffs': 'threeb', 'snapshot': 'no_such_snapshot'}
        out_msg = self.dispatch(in_msg)
        self.assertError(out_msg)
        self.assertTrue("invalid snapshot" in out_msg['content'])

    def test_remove_snapshot_invalid_ffs(self):
        in_msg = {'msg': 'remove_snapshot',
                  'ffs': 'three_no_exists', 'snapshot': 'c'}
        out_msg = self.dispatch(in_msg)
        self.assertError(out_msg)
        self.assertTrue("invalid ffs" in out_msg['content'])

    def test_remove_while_open(self):
       # happens if we're rsyncing into the directory.!
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'rwo'])
        subprocess.check_call(
            ['sudo', 'chmod', '777', '/' + NodeTests.get_test_prefix() + 'rwo'])
        try:
            op = open('/' + NodeTests.get_test_prefix() + 'rwo/fileA', 'w')
            op.write("hello")
            in_msg = {'msg': 'remove', 'ffs': 'rwo'}
            self.assertTrue('rwo' in self.list_ffs(True, True))
            out_msg = self.dispatch(in_msg)
            self.assertNotError(out_msg)
            self.assertEqual(out_msg['msg'], 'remove_failed')
            self.assertEqual(out_msg['reason'], 'target_is_busy')
            prop_status = subprocess.check_output(
                ['sudo', 'zfs', 'get', 'ffs:remove_asap', self.get_test_prefix() + 'rwo', '-H']).split(b"\t")[2].decode('utf-8')
            self.assertEqual(prop_status, 'on')
        finally:
            op.close()

    def test_double_remove(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'rwo2'])
        in_msg = {'msg': 'remove', 'ffs': 'rwo2'}
        self.assertTrue('rwo2' in self.list_ffs(True, True))
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertEqual(out_msg['msg'], 'remove_failed')
        self.assertEqual(out_msg['reason'], 'target_does_not_exists')

    def test_zpool_status(self):
        in_msg = {'msg': 'zpool_status', }
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertTrue('status' in out_msg)
        self.assertTrue('pool:' in out_msg['status'])
        self.assertTrue('state:' in out_msg['status'])
        self.assertTrue('status:' in out_msg['status'])

    def test_chown_and_chmod(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'cac'])
        subprocess.check_call(
            ['sudo', 'chmod', '777', '/' + NodeTests.get_test_prefix() + 'cac'])

        in_msg = {'msg': 'chown_and_chmod', 'ffs': 'cac',
                  'user': 'nobody', 'rights': '0567', 'sub_path': '/'}
        fn = '/' + NodeTests.get_test_prefix() + 'cac/file_one'
        touch(fn)
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertEqual(get_file_user(fn), 'nobody')
        self.assertEqual(get_file_rights(fn) & 0o567, 0o567)
        self.assertEqual(out_msg['msg'], 'chown_and_chmod_done')

    def test_chown_and_chmod_rgwx(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'cac3'])
        subprocess.check_call(
            ['sudo', 'chmod', '777', '/' + NodeTests.get_test_prefix() + 'cac3'])

        in_msg = {'msg': 'chown_and_chmod', 'ffs': 'cac3',
                  'user': 'nobody', 'rights': 'o+rwX', 'sub_path': '/'}
        fn = '/' + NodeTests.get_test_prefix() + 'cac3/two/file_two'
        os.mkdir(os.path.dirname(fn))
        touch(fn)
        os.chmod(fn, 0o000)
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertEqual(get_file_user(fn), 'nobody')
        self.assertEqual(get_file_user(os.path.dirname(fn)), 'nobody')
        self.assertEqual(get_file_rights(fn) & 0o006, 0o006)
        self.assertEqual(get_file_rights(os.path.dirname(fn)) & 0o007, 0o007)
        self.assertEqual(out_msg['msg'], 'chown_and_chmod_done')

    def test_chown_and_chmod_invalid_ffs(self):
        in_msg = {'msg': 'chown_and_chmod', 'ffs': 'cac_not_existant',
                  'user': 'nobody', 'rights': '0567', 'sub_path': '/'}
        out_msg = self.dispatch(in_msg)
        self.assertError(out_msg)
        self.assertTrue('invalid ffs' in out_msg['content'])

    def test_chown_and_chmod_invalid_user(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'cac4'])
        in_msg = {'msg': 'chown_and_chmod', 'ffs': 'cac4',
                  'user': 'not_present_here', 'rights': '0567', 'sub_path': '/'}
        out_msg = self.dispatch(in_msg)
        self.assertError(out_msg)
        self.assertTrue('invalid user' in out_msg['content'])

    def test_chown_and_chmod_invalid_rights(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'cac5'])
        in_msg = {'msg': 'chown_and_chmod', 'ffs': 'cac5',
                  'user': 'nobody', 'rights': '+nope', 'sub_path': '/'}
        out_msg = self.dispatch(in_msg)
        self.assertError(out_msg)
        self.assertTrue('invalid rights' in out_msg['content'])

    def test_chown_and_chmod_within_capture(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'cac2'])
        subprocess.check_call(
            ['sudo', 'chmod', '777', '/' + NodeTests.get_test_prefix() + 'cac2'])
        in_msg = {'msg': 'capture', 'ffs': 'cac2', 'snapshot': 'a',
                  'chown_and_chmod': True, 'user': 'nobody', 'rights': '0567', 'sub_path': '/'}
        self.assertNotSnapshot('cac2', 'b')
        fn = '/' + NodeTests.get_test_prefix() + 'cac2/file_one'
        touch(fn)
        os.chmod(fn, 0o000)
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertSnapshot('cac2', 'a')
        self.assertEqual(out_msg['msg'], 'capture_done')
        self.assertEqual(out_msg['ffs'], 'cac2')
        self.assertEqual(out_msg['snapshot'], 'a')
        self.assertEqual(get_file_user(fn), 'nobody')
        self.assertEqual(get_file_rights(fn) & 0o567, 0o567)

    def test_send_snapshot(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'from_1'])
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix2() + 'from_1'])
        subprocess.check_call(
            ['sudo', 'chmod', '777', '/' + NodeTests.get_test_prefix() + 'from_1'])
        write_file('/' + NodeTests.get_test_prefix() + 'from_1/one', 'hello')
        subprocess.check_call(
            ['sudo', 'zfs', 'snapshot', NodeTests.get_test_prefix() + 'from_1@a'])

        self.assertSnapshot('from_1', 'a')
        self.assertFalse(os.path.exists(
            '/' + NodeTests.get_test_prefix2() + 'from_1/one'))
        self.assertTrue(os.path.exists(
            '/' + NodeTests.get_test_prefix() + 'from_1/one'))
        # so that the actual dir differes from the snapshot and we can test
        # that that we're reading from the snapshot!
        write_file('/' + NodeTests.get_test_prefix() + 'from_1/one', 'hello2')
        in_msg = {'msg': 'send_snapshot',
                  'ffs': 'from_1',
                  'snapshot': 'a',
                  'target_host': '127.0.0.1',
                  'target_user': 'ffs',
                  'target_ssh_cmd': target_ssh_cmd,
                  'target_ffs': 'from_1',
                  'target_storage_prefix': '/' + NodeTests.get_test_prefix2()[:-1]
        }
        self.assertNotSnapshot('from_1', 'a', True)
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertTrue(os.path.exists(
            '/' + NodeTests.get_test_prefix2() + 'from_1/one'))
        self.assertSnapshot('from_1', 'a', True)
        self.assertEqual(
            read_file('/' + NodeTests.get_test_prefix() + 'from_1/one'), 'hello2')
        self.assertEqual(
            read_file('/' + NodeTests.get_test_prefix2() + 'from_1/one'), 'hello')

        self.assertFalse(os.path.exists(
            '/' + NodeTests.get_test_prefix() + '.ffs_sync_clones/' + out_msg['clone_name']))

    def test_send_snapshot_invalid_ffs(self):
        # so that the actual dir differes from the snapshot and we can test
        # that that we're reading from the snapshot!
        in_msg = {'msg': 'send_snapshot',
                  'ffs': 'from_2',
                  'snapshot': 'a',
                  'target_host': '127.0.0.1',
                  'target_user': 'ffs',
                  'target_ssh_cmd': target_ssh_cmd,
                  'target_path': '/%%ffs%%/to_2',
                  }
        out_msg = self.dispatch(in_msg)
        self.assertError(out_msg)
        self.assertTrue('invalid ffs' in out_msg['content'])

    def test_send_snapshot_invalid_snapshot(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'from_2'])
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix2() + 'from_2'])
        self.assertNotSnapshot('from_2', 'a')
        in_msg = {'msg': 'send_snapshot',
                  'ffs': 'from_2',
                  'snapshot': 'a',
                  'target_host': '127.0.0.1',
                  'target_user': 'ffs',
                  'target_ssh_cmd': target_ssh_cmd,
                  'target_ffs': 'from_2',
                  'target_storage_prefix': '/' + NodeTests.get_test_prefix2()[:-1]
                  }
        self.assertNotSnapshot('from_2', 'a', True)
        out_msg = self.dispatch(in_msg)
        self.assertError(out_msg)
        self.assertTrue("invalid snapshot" in out_msg['content'])

    def test_send_snapshot_invalid_target(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'from_4'])
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix2() + 'from_4'])
        subprocess.check_call(
            ['sudo', 'chmod', '777', '/' + NodeTests.get_test_prefix() + 'from_4'])
        write_file('/' + NodeTests.get_test_prefix() + 'from_4/one', 'hello')
        subprocess.check_call(
            ['sudo', 'zfs', 'snapshot', NodeTests.get_test_prefix() + 'from_4@a'])

        self.assertSnapshot('from_4', 'a')
        self.assertFalse(os.path.exists(
            '/' + NodeTests.get_test_prefix2() + 'From_4/one'))
        self.assertTrue(os.path.exists(
            '/' + NodeTests.get_test_prefix() + 'from_4/one'))
        # so that the actual dir differes from the snapshot and we can test
        # that that we're reading from the snapshot!
        write_file('/' + NodeTests.get_test_prefix() + 'from_4/one', 'hello2')
        in_msg = {'msg': 'send_snapshot',
                  'ffs': 'from_4',
                  'snapshot': 'a',
                  'target_host': '203.0.113.0',  # that ip is reserved for documentation purposes
                  'target_user': 'ffs',
                  'target_ssh_cmd': target_ssh_cmd,
                  'target_ffs': 'from_4',
                  'target_storage_prefix': '/' + NodeTests.get_test_prefix2()[:-1]
                  }
        out_msg = self.dispatch(in_msg)
        self.assertError(out_msg)
        self.assertTrue('connect to host' in out_msg['content'])

    def test_rsync_respects_filesystem_boundaries(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'from_5'])
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix2() + 'from_5'])
        subprocess.check_call(
            ['sudo', 'chmod', '777', '/' + NodeTests.get_test_prefix() + 'from_5'])
        write_file('/' + NodeTests.get_test_prefix() + 'from_5/one', 'hello')

        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'from_5/suba'])
        subprocess.check_call(
            ['sudo', 'chmod', '777', '/' + NodeTests.get_test_prefix() + 'from_5/suba'])

        write_file('/' + NodeTests.get_test_prefix() +
                   'from_5/suba/two', 'hello2')
        subprocess.check_call(
            ['sudo', 'zfs', 'snapshot', NodeTests.get_test_prefix() + 'from_5@a'])
        subprocess.check_call(
            ['sudo', 'zfs', 'snapshot', NodeTests.get_test_prefix() + 'from_5/suba@a'])

        in_msg = {'msg': 'send_snapshot',
                  'ffs': 'from_5',
                  'snapshot': 'a',
                  'target_host': '127.0.0.1',
                  'target_user': 'ffs',
                  'target_ssh_cmd': target_ssh_cmd,
                  'target_ffs': 'from_5',
                  'target_storage_prefix': '/' + NodeTests.get_test_prefix2()[:-1]
                  }
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertSnapshot('from_5', 'a')
        self.assertSnapshot('from_5', 'a', True)
        self.assertEqual(
            read_file('/' + NodeTests().get_test_prefix2() + 'from_5/one'), 'hello')
        # the emty directory does get placed there
        self.assertTrue(os.path.exists(
            '/' + NodeTests().get_test_prefix2() + 'from_5/suba'))
        self.assertFalse(os.path.exists(
            '/' + NodeTests().get_test_prefix2() + 'from_5/suba/two'))

    def test_deploy(self):
        import zipfile
        import time
        import base64
        import io
        test_string = str(time.time())
        buffer = io.BytesIO()
        f = zipfile.ZipFile(buffer, "a", zipfile.ZIP_DEFLATED, False)
        f.writestr("test_deploy.txt", test_string)
        f.close()
        in_msg = {'msg': 'deploy', 'node.zip': base64.b64encode(
            buffer.getvalue()).decode('utf-8')}
        fn = '/home/ffs/test_deploy.txt'
        if os.path.exists(fn):
            self.assertNotEqual(read_file(fn), test_string)
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertEqual(read_file(fn), test_string)

    def test_new_in_readonly_parent(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'from_6'])
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'from_6/a'])
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'to_6'])
        subprocess.check_call(
            ['sudo', 'zfs', 'set', 'ffs:main=on', NodeTests.get_test_prefix() + 'from_6'])
        subprocess.check_call(
            ['sudo', 'zfs', 'set', 'ffs:main=on', NodeTests.get_test_prefix() + 'from_6/a'])
        subprocess.check_call(
            ['sudo', 'zfs', 'set', 'ffs:main=off', NodeTests.get_test_prefix() + 'to_6'])
        subprocess.check_call(
            ['sudo', 'zfs', 'set', 'readonly=on', NodeTests.get_test_prefix() + 'to_6'])
        in_msg = {'msg': 'new',
                  'ffs': 'to_6/a',
                  'properties': {},
                  }
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertTrue(os.path.exists(
            '/' + NodeTests.get_test_prefix() + 'to_6/a'))

    def test_rename_sets_ffs_renamed_from(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'from_7'])
        subprocess.check_call(
            ['sudo', 'chmod', '777', '/' + NodeTests.get_test_prefix() + 'from_7'])
        write_file('/' + NodeTests.get_test_prefix() + 'from_7/one', 'hello')

        subprocess.check_call(
            ['sudo', 'zfs', 'snapshot', NodeTests.get_test_prefix() + 'from_7@a'])

        in_msg = {'msg': 'rename',
                  'ffs': 'from_7',
                  'new_name': 'from_7_r'
        }
        self.assertSnapshot('from_7', 'a')
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertSnapshot('from_7_r', 'a')
        self.assertEqual(read_file('/' + NodeTests.get_test_prefix() + 'from_7_r/one'), 'hello')
        self.assertFalse(os.path.exists('/' + NodeTests.get_test_prefix() + 'from_7', ))
        self.assertEqual(node.get_zfs_property(NodeTests.get_test_prefix() + 'from_7_r', 'ffs:renamed_from'),
        'from_7')

    def test_chown_and_chmod(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'from_8'])
        subprocess.check_call(
            ['sudo', 'chmod', '777', '/' + NodeTests.get_test_prefix() + 'from_8']) 
        write_file('/' + NodeTests.get_test_prefix() + 'from_8/one', 'hello')
        subprocess.check_call(
            ['sudo', 'chmod', '000', '/' + NodeTests.get_test_prefix() + 'from_8/one']) 
        self.assertEqual(get_file_rights('/' + NodeTests.get_test_prefix() + 'from_8/one') & 0o777, 0)
        in_msg = {'msg': 'chown_and_chmod',
                  'ffs': 'from_8',
                  'sub_path': '/',
                  'user': 'nobody',
                  'rights': 'uog+rwX',
        }
        out_msg = self.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertEqual(get_file_rights('/' + NodeTests.get_test_prefix() + 'from_8/one') & 0o777, 0o666)
       
    def test_chmod_and_chown_sub_dir(self):
        subprocess.check_call(
            ['sudo', 'zfs', 'create', NodeTests.get_test_prefix() + 'from_9'])
        subprocess.check_call(
            ['sudo', 'chmod', '777', '/' + NodeTests.get_test_prefix() + 'from_9']) 
        os.makedirs('/' + NodeTests.get_test_prefix() + 'from_9/one')
        write_file('/' + NodeTests.get_test_prefix() + 'from_9/one/two', 'hello')
        write_file('/' + NodeTests.get_test_prefix() + 'from_9/a', 'hello')
        subprocess.check_call(
            ['sudo', 'chmod', '000', '/' + NodeTests.get_test_prefix() + 'from_9/one/two']) 
        subprocess.check_call(
            ['sudo', 'chmod', '000', '/' + NodeTests.get_test_prefix() + 'from_9/a']) 

        self.assertEqual(get_file_rights('/' + NodeTests.get_test_prefix() + 'from_9/one/two') & 0o777, 0)
        self.assertEqual(get_file_rights('/' + NodeTests.get_test_prefix() + 'from_9/a') & 0o777, 0)
        in_msg = {'msg': 'chown_and_chmod',
                  'ffs': 'from_9',
                  'sub_path': '/one',
                   'user': 'nobody',
                  'rights': 'uog+rwX',

        }
        out_msg = self.dispatch(in_msg)
        self.assertEqual(get_file_rights('/' + NodeTests.get_test_prefix() + 'from_9/one/two') & 0o777, 0o666)
        self.assertEqual(get_file_rights('/' + NodeTests.get_test_prefix() + 'from_9/a') & 0o777, 0)
 
if __name__ == '__main__':
    unittest.main()
