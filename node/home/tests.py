#!/usr/bin/python3
import pprint
import unittest
import os
import subprocess
import shutil
import tempfile
import json
import stat
import node


def run_rsync(cmd, encode=True, show_errors=True):
    if encode:
        cmd = json.dumps(cmd)
    p = subprocess.Popen('./robust_parallel_rsync.py', stdin=subprocess.PIPE,
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
            'target_host': 'localhost',
            'target_ssh_cmd': ['ssh', '-p', '223'],
            'target_user': 'ffs',
            'chown_user': 'finkernagel',
            'chown_group': 'zti',
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
            'target_host': 'localhost',
            'target_ssh_cmd': ['ssh', '-p', '223'],
            'target_user': 'ffs',
            'chown_user': 'finkernagel',
            'chown_group': 'zti',
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
        self.assertEqual(
            read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(
            target_path, '1', 'file2')), 'hello1')
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
        self.assertEqual(
            read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(
            target_path, '1', 'file2')), 'hello1')
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
            'target_host': 'localhost',
            'target_ssh_cmd': ['ssh', '-p', '223'],
            'target_user': 'ffs',
            'chown_user': 'finkernagel',
            'chown_group': 'zti',
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
            'target_host': 'localhost',
            'target_ssh_cmd': ['ssh', '-p', '223'],
            'target_user': 'ffs',
            'chown_user': 'finkernagel',
            'chown_group': 'zti',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(
            read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(
            target_path, '1', 'file2')), 'hello1')
        self.assertEqual(read_file(os.path.join(
            target_path, '2', 'file3')), 'hello2')

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
            'target_host': 'localhost',
            'target_ssh_cmd': ['ssh', '-p', '223'],
            'target_user': 'ffs',
            'chown_user': 'finkernagel',
            'chown_group': 'zti',
        })
        self.assertEqual(rc, 0)
        self.assertEqual(
            read_file(os.path.join(target_path, 'file1')), 'hello')
        self.assertEqual(read_file(os.path.join(
            target_path, '1', 'file2')), 'hello1')
        self.assertEqual(read_file(os.path.join(
            target_path, '2', 'file3')), 'hello2')

        mode = os.stat(os.path.join(target_path, '2', 'file3'))[stat.ST_MODE]
        # make sure the files are readable afterwards...
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
            self.assertEqual(
                read_file(os.path.join(target_path, 'file1')), 'hello')
            self.assertEqual(read_file(os.path.join(
                target_path, '1', 'file2')), 'hello1')
            self.assertEqual(read_file(os.path.join(
                target_path, '2', 'file3')), 'hello2')
            mode = os.stat(os.path.join(target_path, '2', 'file3'))[
                stat.ST_MODEO]
            self.assertTrue(mode & stat.S_IXUSR)
            self.assertTrue(mode & stat.S_IXGRP)
            self.assertTrue(mode & stat.S_IXOTH)

        def test_cant_read_source_dir():
            raise NotImplementedError()

        def test_x_survives_wrong_user(self):
            raise NotImplementedError()
            pass

        def test_over_9000_files(self):
            # mostly a test of the chown expansion...
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

def touch(filename):
    with open(filename, 'w'):
        pass


class NodeTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        test_prefix = cls.get_prefix() + '.testing'
        p = subprocess.Popen(['sudo', 'zfs', 'destroy', test_prefix, '-r'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        subprocess.check_call(['sudo', 'zfs', 'create', test_prefix])
        subprocess.check_call(['sudo', 'zfs', 'create', test_prefix + '/one'])
        subprocess.check_call(
            ['sudo', 'zfs', 'set', 'ffs:test=one', test_prefix + '/one'])
        subprocess.check_call(['sudo', 'zfs', 'snapshot', test_prefix + '/one@a'])
        subprocess.check_call(['sudo', 'zfs', 'create', test_prefix + '/two'])
        subprocess.check_call(
            ['sudo', 'zfs', 'create', test_prefix + '/three'])
        subprocess.check_call(['sudo','chmod','777', '/' + test_prefix + '/three'])
        touch('/' + test_prefix + '/three/file_a')
        subprocess.check_call(['sudo', 'zfs', 'snapshot', test_prefix + '/three@a'])
        touch('/' + test_prefix + '/three/file_b')
        subprocess.check_call(['sudo', 'zfs', 'snapshot', test_prefix + '/three@b'])
        touch('/' + test_prefix + '/three/file_c')
        subprocess.check_call(['sudo', 'zfs', 'snapshot', test_prefix + '/three@c'])

    @classmethod
    def get_prefix(cls):
        if not hasattr(cls, '_prefix'):
            cls._prefix = node.find_ffs_prefix()
        return cls._prefix

    def test_invalid_message_error_reply(self):
        in_msg = {"msg": 'no such message'}
        out_msg = node.dispatch(in_msg)
        self.assertTrue('error' in out_msg)

    def test_no_msg(self):
        in_msg = {}
        out_msg = node.dispatch(in_msg)
        self.assertTrue('error' in out_msg)
        self.assertTrue('content' in out_msg)

    def test_list_ffs(self):
        in_msg = {'msg': 'list_ffs'}
        out_msg = node.dispatch(in_msg)
        self.assertEqual(out_msg['msg'], 'ffs_list')
        zfs_list = subprocess.check_output(
            ['sudo', '-u', 'ffs', 'zfs', 'list', '-H']).decode('utf-8').strip().split("\n")
        zfs_list = [x.split("\t")[0] for x in zfs_list]
        zfs_list = [x for x in zfs_list if '/ffs/' in x and not '/ffs/.' in x]
        zfs_list = [x[x.find('/ffs/') + len('/ffs/'):] for x in zfs_list]
        self.assertTrue(zfs_list)
        any_snapshots = False
        for x in zfs_list:
            self.assertTrue(x in out_msg['ffs'])
            if out_msg['ffs'][x]['snapshots']:
                any_snapshots = True
            self.assertTrue('creation' in out_msg['ffs'][x]['properties'])
            self.assertEqual(out_msg['ffs'][x]['properties'][
                             'type'], 'filesystem')
        self.assertTrue(any_snapshots)

    def test_set_properties(self):
        in_msg = {'msg': 'set_property', 'ffs': '.testing/one',
                  'properties': {'ffs:test': 'two'}}
        self.assertEqual(node.get_zfs_property(
            NodeTests.get_prefix() + '.testing/one', 'ffs:test'), 'one')
        out_msg = node.dispatch(in_msg)
        if 'error' in out_msg:
            pprint.pprint(out_msg)
        self.assertNotError(out_msg)
        self.assertEqual(node.get_zfs_property(
            NodeTests.get_prefix() + '.testing/one', 'ffs:test'), 'two')
        self.assertEqual(out_msg['msg'], 'set_properties_done')
        self.assertEqual(out_msg['ffs'], '.testing/one')
        self.assertEqual(out_msg['properties']['ffs:test'], 'two')
        # as proxy for the remaining properties...
        self.assertTrue('creation' in out_msg['properties'])

    def test_set_properties_invalid_ffs(self):
        in_msg = {'msg': 'set_property', 'ffs': '.testing/does_not_exist',
                  'properties': {'ffs:test': 'two'}}
        out_msg = node.dispatch(in_msg)
        self.assertError(out_msg)
        self.assertTrue('invalid ffs' in out_msg['content'])

    def test_set_properties_invalid_property_name(self):
        in_msg = {'msg': 'set_property', 'ffs': '.testing/one',
                  'properties': {' ffs:test': 'two'}}
        out_msg = node.dispatch(in_msg)
        self.assertError(out_msg)
        self.assertTrue('invalid property name' in out_msg['content'])

    def test_set_properties_invalid_value(self):
        # max length is 1024
        in_msg = {'msg': 'set_property', 'ffs': '.testing/one',
                  'properties': {'ffs:test': 't' * 1025}}
        out_msg = node.dispatch(in_msg)
        self.assertError(out_msg)
        self.assertTrue('invalid property value' in out_msg['content'])

    def assertNotError(self, msg):
        if 'error' in msg:
            pprint.pprint(msg)
        self.assertFalse('error' in msg)

    def assertError(self, msg):
        if not 'error' in msg:
            pprint.pprint(msg)
        self.assertTrue('error' in msg)

    def test_new(self):
        in_msg = {'msg': 'new', 'ffs': '.testing/four',
                  'properties': {'ffs:test1': 'one', 'ffs:test2': 'two'}}
        self.assertFalse('.testing/four' in node.list_ffs(True, True))
        out_msg = node.dispatch(in_msg)
        self.assertTrue('.testing/four' in node.list_ffs(True, True))
        self.assertNotError(out_msg)
        self.assertEqual(out_msg['msg'], 'new_done')
        self.assertEqual(out_msg['ffs'], '.testing/four')
        self.assertEqual(out_msg['properties']['ffs:test1'], 'one')
        self.assertEqual(out_msg['properties']['ffs:test2'], 'two')
        # as proxy for the remaining properties...
        self.assertTrue('creation' in out_msg['properties'])

    def get_snapshots(self):
        snapshot_list = subprocess.check_output(['sudo','zfs','list','-t', 'snapshot', '-H']).decode("utf-8").strip().split("\n")
        return [x.split("\t")[0] for x in snapshot_list]

    def assertSnapshot(self, ffs, snapshot):
        full_path = NodeTests.get_prefix() + ffs + '@' + snapshot
        sn = self.get_snapshots()
        self.assertTrue(full_path in sn)

    def assertNotSnapshot(self, ffs, snapshot):
        full_path = NodeTests.get_prefix() + ffs + '@' + snapshot
        sn = self.get_snapshots()
        self.assertFalse(full_path in sn)


    def test_capture(self):
        in_msg = {'msg': 'capture', 'ffs': '.testing/one', 'snapshot': 'b'}
        self.assertNotSnapshot('.testing/one','b')
        out_msg = node.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertSnapshot('.testing/one','b')
        self.assertEqual(out_msg['msg'], 'capture_done')
        self.assertEqual(out_msg['ffs'], '.testing/one')
        self.assertEqual(out_msg['snapshot'], 'b')


    def test_snapshot_exists(self):
        in_msg = {'msg': 'capture', 'ffs': '.testing/one', 'snapshot': 'a'}
        self.assertSnapshot('.testing/one','a')
        out_msg = node.dispatch(in_msg)
        self.assertError(out_msg)
        self.assertTrue('already exists' in out_msg['content'])

    def test_snapshot_invalid_ffs(self):
        in_msg = {'msg': 'capture', 'ffs': '.testing/doesnotexist', 'snapshot': 'a'}
        out_msg = node.dispatch(in_msg)
        self.assertError(out_msg)

    def test_remove(self):
        in_msg = {'msg': 'remove', 'ffs': '.testing/two' }
        self.assertTrue('.testing/two' in node.list_ffs(True, True))
        out_msg = node.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertFalse('.testing/two' in node.list_ffs(True, True))
        self.assertEqual(out_msg['msg'], 'remove_done')
        self.assertEqual(out_msg['ffs'], '.testing/two')


    def test_remove_invalid_ffs(self):
        in_msg = {'msg': 'remove', 'ffs': '.testing/does_not_exist' }
        self.assertFalse('.testing/does_not_exist' in node.list_ffs(True, True))
        out_msg = node.dispatch(in_msg)
        self.assertError(out_msg)

    def test_remove_snapshot(self):
        in_msg = {'msg': 'remove_snapshot', 'ffs': '.testing/three', 'snapshot': 'c'}
        self.assertSnapshot('.testing/three','a')
        self.assertSnapshot('.testing/three','b')
        self.assertSnapshot('.testing/three','c')
        out_msg = node.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertEqual(out_msg['msg'], 'remove_snapshot_done')
        self.assertEqual(out_msg['ffs'], '.testing/three')
        self.assertEqual(out_msg['snapshots'], ['a', 'b'],)
        self.assertSnapshot('.testing/three','a')
        self.assertSnapshot('.testing/three','b')
        self.assertNotSnapshot('.testing/three','c')

        in_msg = {'msg': 'remove_snapshot', 'ffs': '.testing/three', 'snapshot': 'a'}
        out_msg = node.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertEqual(out_msg['snapshots'], ['b'],)
        self.assertNotSnapshot('.testing/three','a')
        self.assertSnapshot('.testing/three','b')
        self.assertNotSnapshot('.testing/three','c')

    def test_remove_snapshot_invalid_snapshot(self):
        in_msg = {'msg': 'remove_snapshot', 'ffs': '.testing/three', 'snapshot': 'no_such_snapshot'}
        out_msg = node.dispatch(in_msg)
        self.assertError(out_msg)
        self.assertTrue("invalid snapshot" in out_msg['content'])

    def test_remove_snapshot_invalid_ffs(self):
        in_msg = {'msg': 'remove_snapshot', 'ffs': '.testing/three_no_exists', 'snapshot': 'c'}
        out_msg = node.dispatch(in_msg)
        self.assertError(out_msg)
        self.assertTrue("invalid ffs" in out_msg['content'])

    def test_zpool_status(self):
        in_msg = {'msg': 'zpool_status',}
        out_msg = node.dispatch(in_msg)
        self.assertNotError(out_msg)
        self.assertTrue('status' in out_msg)
        self.assertTrue('pool:' in out_msg['status'])
        self.assertTrue('state:' in out_msg['status'])
        self.assertTrue('status:' in out_msg['status'])



        

    def test_send_snapshot(self):
        raise NotImplemented()

    def test_send_snapshot_invalid_ffs(self):
        raise NotImplemented()
    def test_send_snapshot_invalid_snapshot(self):
        raise NotImplemented()
    def test_send_snapshot_invalid_target(self):
        raise NotImplemented()



if __name__ == '__main__':
    unittest.main()
