import unittest
import signal
import stat
import time
import pwd
import subprocess
import os
import json
user = pwd.getpwuid(os.getuid())[0]
if user != 'ffs':
    print(os.getuid())
    raise ValueError(
        "Node tests must run as user ffs - was %s" % user)

import sys
# sys.path.insert(0, '../')
sys.path.insert(0, '../node/home')
import node

hostname = subprocess.check_output('hostname')


def get_file_rights(filename):
    return os.stat(filename)[stat.ST_MODE]


def zfs_output(cmd_line):
    return subprocess.check_output(cmd_line).decode('utf-8')


def _get_zfs_properties(zfs_name):
    lines = zfs_output(['sudo', 'zfs', 'get', 'all', zfs_name, '-H']
                       ).strip().split("\n")
    lines = [x.split("\t") for x in lines]
    result = {x[1]: x[2] for x in lines}
    return result


def get_zfs_property(zfs_name, property_name):
    return _get_zfs_properties(zfs_name)[property_name]

global_engine_process = None


class ClientTests(unittest.TestCase):

    @classmethod
    def get_pool(cls):
        if not hasattr(cls, '_pool'):
            lines = subprocess.check_output(['sudo', 'zpool', 'status']).split(
                b"\n")  # use status, it's in the sudoers
            for l in lines:
                l = l.strip()
                if l.startswith(b'pool:'):
                    pool = l[l.find(b':') + 2:].strip().decode('utf-8')
                    cls._pool = pool + '/'
                    break
            else:
                raise ValueError(
                    "Could not find a zpool to create .ffs_testing zfs on")
        return cls._pool

    @classmethod
    def get_test_prefix(cls):
        return cls.get_pool() + '.ffs_testing_client_from/'

    @classmethod
    def get_test_prefix2(cls):
        return cls.get_pool() + '.ffs_testing_client_to/'

    @classmethod
    def start_engine(cls):
        global global_engine_process
        if global_engine_process is None:
                global_engine_process = subprocess.Popen(['../ffs_central.sh',
                                                    os.path.abspath('_config_engine_a.py')])
        if not hasattr(cls, 'engine_process'):
            cls.engine_process = global_engine_process 

    @classmethod
    def run_client(cls, cmd_args, cwd=None):
        cls.engine_process.poll()
        if cls.engine_process.returncode is not None:
            raise ValueError("engine has gone away")
        p = subprocess.Popen([os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                           '../../FloatingFileSystemClient/ffs.py')),
                              '--host=localhost',
                              '--port=47776',
                              ] + cmd_args,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
        stdout, stderr = p.communicate()
        return p.returncode, stdout, stderr

    @classmethod
    def run_expect_ok(cls, cmd_args, cwd=None):
        rc, stdout, stderr = cls.run_client(cmd_args, cwd)
        if rc != 0:
            raise ValueError("Error return: %i, %s, %s" % (rc, stdout, stderr))
        return stdout

    @classmethod
    def run_expect_error(cls, cmd_args, check_for_msg=None):
        rc, stdout, stderr = cls.run_client(cmd_args)
        if rc == 0:
            raise ValueError(
                "Unexpected non error return: %i, %s, %s" % (rc, stdout, stderr))
        if check_for_msg:
            if check_for_msg not in stdout:
                raise ValueError("stdout did not contain '%s': %i, %s, %s" % (
                    check_for_msg, rc, stdout, stderr))
        return stdout

    @classmethod
    def client_wait_for_startup(cls):
        start = time.time()
        while True:
            stdout = cls.run_expect_ok(['service', 'is_started'])
            q = json.loads(stdout.decode('utf-8'))
            if q['started']:
                break
            if time.time() > start + 30:
                raise ValueError("timeout")
            time.sleep(0.5)

    @classmethod
    def client_wait_for_empty_que(cls):
        start = time.time()
        while True:
            stdout = cls.run_expect_ok(['service', 'que'])
            q = json.loads(stdout.decode('utf-8'))
            if not any(q.values()):
                break
            if time.time() > start + 30:
                raise ValueError("timeout")
            time.sleep(0.1)

    @classmethod
    def setUpClass(cls):
        with open("client_test_debug.log",'w'):
            pass
        with open("client_test_error.log",'w'):
            pass

        for root in [cls.get_test_prefix()[:-1], cls.get_test_prefix2()[:-1]]:
            p = subprocess.Popen(['sudo', 'zfs', 'destroy', root, '-R'],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            subprocess.check_call(
                ['sudo', 'zfs', 'create', root])
            subprocess.check_call(
                ['sudo', 'zfs', 'set', 'ffs:root=on', root])

        for n in ['rename_test', 'capture_test', 'capture_test2', 'orphan', 'remove_test', 'chown_test',
                  'add_target_test', 'move_test_no_replicate', 'move_test_move_to_main',
                  'time_based_snapshot_tests']:
            subprocess.check_call(
                ['sudo', 'zfs', 'create', cls.get_test_prefix()[:-1] + '/' + n])
            subprocess.check_call(['sudo', 'chmod', '0777',
                                   '/' + cls.get_test_prefix()[:-1] + '/' + n])
            subprocess.check_call(
                ['sudo', 'zfs', 'set', 'ffs:main=on', cls.get_test_prefix()[:-1] + '/' + n])
            subprocess.check_call(
                ['sudo', 'zfs', 'set', 'atime=off', cls.get_test_prefix()[:-1] + '/' + n])
            subprocess.check_call(
                ['sudo', 'zfs', 'set', 'com.sun:auto-snapshot=false', cls.get_test_prefix()[:-1] + '/' + n])
             
        for n in ['remove_target_test', 'move_test']:
            subprocess.check_call(
                ['sudo', 'zfs', 'create', cls.get_test_prefix()[:-1] + '/' + n])
            subprocess.check_call(
                ['sudo', 'zfs', 'set', 'ffs:main=on', cls.get_test_prefix()[:-1] + '/' + n])
            subprocess.check_call(['sudo', 'chmod', '0777',
                                   '/' + cls.get_test_prefix()[:-1] + '/' + n])
            subprocess.check_call(
                ['sudo', 'zfs', 'create', cls.get_test_prefix2()[:-1] + '/' + n])
            subprocess.check_call(
                ['sudo', 'zfs', 'set', 'ffs:main=off', cls.get_test_prefix2()[:-1] + '/' + n])
            subprocess.check_call(
                ['sudo', 'zfs', 'set', 'readonly=on', cls.get_test_prefix2()[:-1] + '/' + n])

            subprocess.check_call(
                ['sudo', 'zfs', 'set', 'atime=off', cls.get_test_prefix()[:-1] + '/' + n])
            subprocess.check_call(
                ['sudo', 'zfs', 'set', 'com.sun:auto-snapshot=false', cls.get_test_prefix()[:-1] + '/' + n])
            subprocess.check_call(
                ['sudo', 'zfs', 'set', 'atime=off', cls.get_test_prefix2()[:-1] + '/' + n])
            subprocess.check_call(
                ['sudo', 'zfs', 'set', 'com.sun:auto-snapshot=false', cls.get_test_prefix2()[:-1] + '/' + n])





        time.sleep(1)  # give the ffs time to catch up ;)
        cls.start_engine()
        # self.run_expect_ok(['service', 'restart'])
        time.sleep(1)
        cls.client_wait_for_startup()

    @classmethod
    def teardownClass(cls):
        cls.engine_process.terminate()
        p = subprocess.Popen(['sudo', 'zfs', 'destroy', cls.get_test_prefix()[:-1], '-R'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        p = subprocess.Popen(['sudo', 'zfs', 'destroy', cls.get_test_prefix2()[:-1], '-R'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()

    def list_ffs(self):
        l = self.run_expect_ok(['list_ffs'])
        return json.loads(l.decode('utf-8'))

    def test_new(self):
        self.assertFalse(os.path.exists('/' + self.get_test_prefix() + 'one'))
        self.assertFalse('one' in self.list_ffs())
        self.run_expect_ok(['new', 'one', 'A'])
        self.client_wait_for_empty_que()
        self.assertTrue(os.path.exists('/' + self.get_test_prefix() + 'one'))
        self.assertTrue('one' in self.list_ffs())

    def test_new_replicated(self):
        self.assertFalse(os.path.exists(
            '/' + self.get_test_prefix() + 'one_two'))
        self.assertFalse('one_two' in self.list_ffs())
        self.run_expect_ok(['new', 'one_two', 'A', 'B'])
        self.client_wait_for_empty_que()
        self.assertTrue(os.path.exists(
            '/' + self.get_test_prefix() + 'one_two'))
        self.assertTrue(os.path.exists(
            '/' + self.get_test_prefix2() + 'one_two'))
        self.assertTrue('one_two' in self.list_ffs())

    def test_list_ffs(self):
        r = self.run_expect_ok(['list_ffs'])
        j = json.loads(r.decode('utf-8'))
        self.assertTrue(isinstance(j, dict))
        self.assertTrue('orphan' in j)

    def test_capture(self):
        with open('/' + self.get_test_prefix()[:-1] + '/capture_test2/one', 'w') as op:
            op.write("test")
        subprocess.check_call(
            ['sudo', 'chmod', '0000', '/' + self.get_test_prefix()[:-1] + '/capture_test2/one'])
        self.assertFalse(os.listdir('/' + self.get_test_prefix()
                                    [:-1] + '/capture_test2/.zfs/snapshot'))
        self.run_expect_ok(['capture', 'capture_test2'])
        self.client_wait_for_empty_que()
        self.assertTrue(os.listdir('/' + self.get_test_prefix()
                                   [:-1] + '/capture_test2/.zfs/snapshot'))
        self.assertTrue(os.listdir('/' + self.get_test_prefix()
                                   [:-1] + '/capture_test2/.zfs/snapshot'))
        # config default permissions are uog+rw
        self.assertEqual(get_file_rights('/' + self.get_test_prefix()[:-1] + '/capture_test2/one') & 0o0777,
                         0o000)

    def test_capture_and_chmod(self):
        with open('/' + self.get_test_prefix()[:-1] + '/capture_test/one', 'w') as op:
            op.write("test")
        subprocess.check_call(
            ['sudo', 'chmod', '0000', '/' + self.get_test_prefix()[:-1] + '/capture_test/one'])
        self.assertFalse(os.listdir('/' + self.get_test_prefix()
                                    [:-1] + '/capture_test/.zfs/snapshot'))
        self.run_expect_ok(['capture', 'capture_test',
                            '--chown_and_chmod', '--postfix=shu'])
        self.client_wait_for_empty_que()
        self.assertTrue(os.listdir('/' + self.get_test_prefix()
                                   [:-1] + '/capture_test/.zfs/snapshot'))
        self.assertTrue([
            x for x in
            os.listdir('/' + self.get_test_prefix()
                       [:-1] + '/capture_test/.zfs/snapshot')
            if x.endswith('-shu')
        ])
        # config default permissions are uog+rw
        self.assertEqual(get_file_rights('/' + self.get_test_prefix()[:-1] + '/capture_test/one') & 0o0777,
                         0o666)

    def test_list_orphans(self):
        r = self.run_expect_ok(['list_orphans'])
        j = json.loads(r.decode('utf-8'))
        self.assertTrue(isinstance(j, list))
        self.assertTrue('orphan' in j)

    def test_list_targets(self):
        r = self.run_expect_ok(['list_targets'])
        j = json.loads(r.decode('utf-8'))
        self.assertTrue(isinstance(j, list))
        self.assertTrue(j)

    def test_add_target(self):
        self.assertFalse(os.path.exists(
            '/' + self.get_test_prefix2() + 'add_target_test'))
        self.run_expect_ok(['add_targets', 'add_target_test', 'B'])
        self.client_wait_for_empty_que()
        self.assertTrue(os.path.exists(
            '/' + self.get_test_prefix() + 'add_target_test'))
        self.assertTrue(os.path.exists(
            '/' + self.get_test_prefix2() + 'add_target_test'))
        self.assertTrue('B' in self.list_ffs()['add_target_test'])

    def test_remove_target(self):
        self.assertTrue(os.path.exists(
            '/' + self.get_test_prefix2() + 'remove_target_test'))
        self.assertTrue('B' in self.list_ffs()['remove_target_test'])
        self.run_expect_ok(['remove_target', 'remove_target_test', 'B'])
        self.client_wait_for_empty_que()
        self.assertFalse(os.path.exists(
            '/' + self.get_test_prefix2() + 'remove_target_test'))
        self.assertFalse('B' in self.list_ffs()['remove_target_test'])

    def test_add_target_already(self):
        self.run_expect_error(
            ['add_targets', 'remove_test', 'A'], b'Add failed, target already in list')

    def test_remove_target_target_is_main(self):
        self.run_expect_error(
            ['remove_target', 'remove_test', 'A'], b'Remove failed, target is main')

    def test_rename(self):
        self.assertTrue(os.path.exists(
            '/' + self.get_test_prefix() + 'rename_test'))
        self.run_expect_ok(['rename_ffs', 'rename_test',
                            'renamed_test'])
        self.client_wait_for_empty_que()
        self.assertFalse(os.path.exists(
            '/' + self.get_test_prefix() + 'rename_test'))
        self.assertTrue(os.path.exists(
            '/' + self.get_test_prefix() + 'renamed_test'))
        props = _get_zfs_properties(self.get_test_prefix() + 'renamed_test')
        self.assertEqual(props.get('ffs:renamed_from', '-'), '-')

    def test_chown_and_chmod(self):
        with open('/' + self.get_test_prefix()[:-1] + '/chown_test/one', 'w') as op:
            op.write("test")
        subprocess.check_call(
            ['sudo', 'chmod', '0000', '/' + self.get_test_prefix()[:-1] + '/chown_test/one'])

        os.mkdir('/' + self.get_test_prefix()[:-1] + '/chown_test/two')
        with open('/' + self.get_test_prefix()[:-1] + '/chown_test/two/three', 'w') as op:
            op.write("test")
        subprocess.check_call(
            ['sudo', 'chmod', '0000', '/' + self.get_test_prefix()[:-1] + '/chown_test/two/three'])
        subprocess.check_call(
            ['sudo', 'chmod', '0000', '/' + self.get_test_prefix()[:-1] + '/chown_test/two/three'])
        self.assertEqual(get_file_rights(
            '/' + self.get_test_prefix()[:-1] + '/chown_test/two/three') & 0o777, 0)

        # first test: absolute path.
        self.run_expect_ok(
            ['chown', '/' + self.get_test_prefix()[:-1] + '/chown_test/two/three'])
        self.client_wait_for_empty_que()
        self.assertEqual(get_file_rights(
            '/' + self.get_test_prefix()[:-1] + '/chown_test/two/three') & 0o777, 0o666)

        # second: relative path to ffs
        subprocess.check_call(
            ['sudo', 'chmod', '0000', '/' + self.get_test_prefix()[:-1] + '/chown_test/two/three'])
        self.assertEqual(get_file_rights(
            '/' + self.get_test_prefix()[:-1] + '/chown_test/two/three') & 0o777, 0)

        self.run_expect_ok(['chown',  'chown_test/two/three'])
        self.client_wait_for_empty_que()
        self.assertEqual(get_file_rights(
            '/' + self.get_test_prefix()[:-1] + '/chown_test/two/three') & 0o777, 0o666)

        # third: using current directory.
        subprocess.check_call(
            ['sudo', 'chmod', '0000', '/' + self.get_test_prefix()[:-1] + '/chown_test/two/three'])
        self.assertEqual(get_file_rights(
            '/' + self.get_test_prefix()[:-1] + '/chown_test/two/three') & 0o777, 0)
        self.run_expect_ok(['chown'],  cwd='/' +
                           self.get_test_prefix()[:-1] + '/chown_test/two/')
        self.client_wait_for_empty_que()
        self.assertEqual(get_file_rights(
            '/' + self.get_test_prefix()[:-1] + '/chown_test/two/three') & 0o777, 0o666)

        # fourth: current directory is ffs
        subprocess.check_call(
            ['sudo', 'chmod', '0000', '/' + self.get_test_prefix()[:-1] + '/chown_test/two/three'])
        self.assertEqual(get_file_rights(
            '/' + self.get_test_prefix()[:-1] + '/chown_test/two/three') & 0o777, 0)
        self.run_expect_ok(['chown'],  cwd='/' +
                           self.get_test_prefix()[:-1] + '/chown_test')
        self.client_wait_for_empty_que()
        self.assertEqual(get_file_rights(
            '/' + self.get_test_prefix()[:-1] + '/chown_test/two/three') & 0o777, 0o666)

    def test_move(self):
        with open('/' + self.get_test_prefix()[:-1] + '/move_test/one', 'w') as op:
            op.write("test")
        self.assertFalse(os.path.exists(
            '/' + self.get_test_prefix2()[:-1] + '/move_test/one'))
        self.run_expect_ok(['move', 'move_test', 'B'])
        self.client_wait_for_empty_que()
        #time.sleep(
        # which does make a captured snapshot necessary, right?
        self.assertTrue(os.path.exists(
            '/' + self.get_test_prefix2()[:-1] + '/move_test/one'))

    def test_move_invalid_target(self):
        self.run_expect_error(['move', 'remove_test', 'shu'],
                              b'Move failed, invalid target')

    def test_move_no_replicate(self):
        self.run_expect_error(['move', 'move_test_no_replicate', 'B'],
            b'target does not have this ffs.'
        )

    def test_move_to_main(self):
        self.run_expect_error(['move', 'move_test_move_to_main', 'A'],
                              b'target is already main')
                            
    def test_set_snapshot_interval(self):
        # tests only the setting of the snapshot property
        props = _get_zfs_properties(self.get_test_prefix() + 'time_based_snapshot_tests')
        self.assertEqual(props.get('ffs:snapshot_interval', '-'), '-')
        self.run_expect_ok(['set_snapshot_interval', 'time_based_snapshot_tests', '15'])
        self.client_wait_for_empty_que()
        self.assertEqual(get_zfs_property(self.get_test_prefix() + 'time_based_snapshot_tests', 'ffs:snapshot_interval'), '15')
        self.run_expect_ok(['set_snapshot_interval', 'time_based_snapshot_tests', 'off'])
        self.client_wait_for_empty_que()
        self.assertEqual(get_zfs_property(self.get_test_prefix() + 'time_based_snapshot_tests', 'ffs:snapshot_interval'), '-')






class CleanChildProcesses:

    def __enter__(self):
        os.setpgrp()  # create new process group, become its leader

    def __exit__(self, type, value, traceback):
        try:
            os.killpg(0, signal.SIGINT)  # kill all processes in my group
        except KeyboardInterrupt:
            # SIGINT is delievered to this process as well as the child processes.
            # Ignore it so that the existing exception, if any, is returned. This
            # leaves us with a clean exit code if there was no exception.
            pass


if __name__ == '__main__':
    with CleanChildProcesses():
        unittest.main()
