import unittest
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


def run_client(cmd_args):
    p = subprocess.Popen([os.path.join(os.path.dirname(__file__), '../../FloatingFileSystemClient/ffs.py')] + cmd_args,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return p.returncode, stdout, stderr

def run_expect_ok(cmd_args):
    rc, stdout, stderr = run_client(cmd_args)
    if rc != 0:
        raise ValueError("Error return: %i, %s, %s" %(rc, stdout, stderr))
    return stdout
    
def client_wait_for_startup():
    start = time.time()
    while True:
        stdout = run_expect_ok(['service', 'is_started'])
        q = json.loads(stdout.decode('utf-8'))
        if q['started']:
            print("startup done")
            break
        if time.time() > start + 30:
            raise ValueError("timeout")
        time.sleep(0.5)

def client_wait_for_empty_que():
    start = time.time()
    while True:
        stdout = run_expect_ok(['service', 'que'])
        q = json.loads(stdout.decode('utf-8'))
        if not any(q.values()):
            break
        if time.time() > start + 30:
            raise ValueError("timeout")
        time.sleep(0.1)



class ClientTests(unittest.TestCase):

    @classmethod
    def get_prefix(cls):
        if not hasattr(cls, '_prefix'):
            cls._prefix = node.find_ffs_prefix()
        return cls._prefix

    @classmethod
    def get_test_prefix(cls):
        return cls.get_prefix() + 'ffs_testing/'

    @classmethod
    def setUpClass(cls):
        p = subprocess.Popen(['sudo', 'zfs', 'destroy', cls.get_test_prefix()[:-1], '-R'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        subprocess.check_call(
            ['sudo', 'zfs', 'create', cls.get_test_prefix()[:-1]])

        run_expect_ok(['service', 'restart'])
        time.sleep(3)
        client_wait_for_startup()

    @classmethod
    def teardownClass(cls):
        p = subprocess.Popen(['sudo', 'zfs', 'destroy', cls.get_test_prefix()[:-1], '-R'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        
    def list_ffs(self):
        l = run_expect_ok(['list_ffs'])
        return json.loads(l.decode('utf-8'))

    def test_new(self):
        self.assertFalse(os.path.exists('/' + self.get_test_prefix() + 'one'))
        self.assertFalse('ffs_testing/one' in self.list_ffs())
        run_expect_ok(['new', 'ffs_testing/one', hostname])
        client_wait_for_empty_que()
        self.assertTrue(os.path.exists('/' + self.get_test_prefix() + 'one'))
        self.assertTrue('ffs_testing/one' in self.list_ffs())

    def test_list_ffs(self):
        raise NotImplementedError()

    def test_list_ffs_json(self):
        raise NotImplementedError()




if __name__ == '__main__':
    unittest.main()