import json
import os
import stat
import re
import subprocess
import time
import hashlib


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


def list_zfs():
    zfs_list = zfs_output(['sudo', 'zfs', 'list', '-H']).strip().split("\n")
    zfs_list = [x.split("\t")[0] for x in zfs_list]
    return zfs_list

_cached_ffs_prefix = None


def find_ffs_prefix():
    global _cached_ffs_prefix
    if _cached_ffs_prefix is None:
        for x in list_zfs():
            if x.endswith('/ffs'):
                _cached_ffs_prefix = x + "/"
    if _cached_ffs_prefix is None:
        raise KeyError()
    return _cached_ffs_prefix

clone_dir = find_ffs_prefix() + '.ffs_sync_clones'


def list_ffs(strip_prefix=False, include_testing=False):
    res = [x for x in list_zfs() if x.startswith(find_ffs_prefix()) and (not x.startswith(
        find_ffs_prefix() + '.') or x.startswith(find_ffs_prefix() + '.ffs_testing'))]
    if strip_prefix:
        ffs_prefix = find_ffs_prefix()
        res = [x[len(ffs_prefix):] for x in res]
    return res


def list_snapshots():
    return [x.split("\t")[0] for x in zfs_output(['sudo', 'zfs', 'list', '-t', 'snapshot', '-H', '-s', 'creation']).strip().split("\n")]


def get_snapshots(ffs):
    all_snapshots = list_snapshots()
    prefix = find_ffs_prefix() + ffs + '@'
    matching = [x[x.find("@") + 1:]
                for x in all_snapshots if x.startswith(prefix)]
    return matching


def msg_list_ffs():
    result = {'msg': 'ffs_list', 'ffs': {}}
    ffs_prefix = find_ffs_prefix()
    ffs_list = list_ffs()
    ffs_info = {x[len(ffs_prefix):]: {'snapshots': [],
                                      'properties': _get_zfs_properties(x)} for x in ffs_list}
    snapshots = list_snapshots()
    for x in snapshots:
        if x.startswith(ffs_prefix) and not x.startswith(ffs_prefix + '.'):
            ffs_name = x[len(ffs_prefix):x.find("@")]
            snapshot_name = x[x.find("@") + 1:]
            ffs_info[ffs_name]['snapshots'].append(snapshot_name)
    result['ffs'] = ffs_info
    return result


def check_property_name_and_value(name, value):
    """They may contain lowercase letters, numbers, and the following
    punctuation characters: colon (:), dash (-), period (.) and underscore
    (_). The expected convention is that the property name is divided into
    two portions such as module : property, but this namespace is not
    enforced by ZFS. User property names can be at most 256 characters, and
    cannot begin with a dash (-)."""
    if not re.match("^[a-z0-9:._-]+$", name):
        raise ValueError("invalid property name")
    if len(value) > 1024:  # straight from the docs as well
        raise ValueError("invalid property value")


def msg_set_properties(msg):
    ffs = msg['ffs']
    full_ffs_path = find_ffs_prefix() + ffs
    if full_ffs_path not in list_ffs(False, True):
        raise ValueError("invalid ffs")
    for prop, value in msg['properties'].items():
        check_property_name_and_value(prop, value)

    for prop, value in msg['properties'].items():
        subprocess.check_call(
            ['sudo', 'zfs', 'set', "%s=%s" % (prop, value), full_ffs_path])
    return {
        'msg': 'set_properties_done',
        'ffs': ffs,
        'properties': _get_zfs_properties(full_ffs_path),
    }


def msg_new(msg):
    ffs = msg['ffs']
    full_ffs_path = find_ffs_prefix() + ffs
    for prop, value in msg['properties'].items():
        check_property_name_and_value(prop, value)

    subprocess.check_call(['sudo', 'zfs', 'create', full_ffs_path])
    for prop, value in msg['properties'].items():
        subprocess.check_call(
            ['sudo', 'zfs', 'set', "%s=%s" % (prop, value), full_ffs_path])
    return {
        'msg': 'new_done',
        'ffs': ffs,
        'properties': _get_zfs_properties(full_ffs_path),
    }


def msg_capture(msg):
    ffs = msg['ffs']
    snapshot_name = msg['snapshot']
    full_ffs_path = find_ffs_prefix() + ffs
    if full_ffs_path not in list_ffs(False, True):
        raise ValueError("invalid ffs")
    combined = '%s@%s' % (full_ffs_path, snapshot_name)
    if combined in list_snapshots():
        raise ValueError("Snapshot already exists")
    if 'chown_and_chmod' in msg and msg['chown_and_chmod']:
        msg_chown_and_chmod(msg)  # fields do match

    subprocess.check_call(
        ['sudo', 'zfs', 'snapshot', combined])
    return {'msg': 'capture_done', 'ffs': ffs, 'snapshot': snapshot_name}


def msg_remove(msg):
    ffs = msg['ffs']
    full_ffs_path = find_ffs_prefix() + ffs
    if full_ffs_path not in list_ffs(False, True):
        return {'msg':'remove_failed', 'reason': 'target_does_not_exists', 'ffs': ffs}
    if not '/ffs' in full_ffs_path:
        raise ValueError("Unexpected")
    p = subprocess.Popen(['sudo', 'zfs', 'destroy', full_ffs_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode == 0:
        return {"msg": 'remove_done', 'ffs': ffs}
    else:
        if b'target is busy' in stderr:
            subprocess.check_call(['sudo', 'zfs', 'set', 'ffs:remove_asap=on', full_ffs_path])
            return {'msg':'remove_failed', 'reason': 'target_is_busy', 'ffs': ffs}
        else:
            return {'error': 'zfs_error_return', 'content': 'zfs destroy %s failed. stdout:%s \nstderr: %s' % (ffs, stdout, stderr)}



def msg_remove_snapshot(msg):
    ffs = msg['ffs']
    full_ffs_path = find_ffs_prefix() + ffs
    if full_ffs_path not in list_ffs(False, True):
        raise ValueError("invalid ffs")
    snapshot_name = msg['snapshot']
    combined = '%s@%s' % (full_ffs_path, snapshot_name)
    if combined not in list_snapshots():
        raise ValueError("invalid snapshot")
    if not '/ffs' in full_ffs_path:
        raise ValueError("Unexpected")
    subprocess.check_call(['sudo', 'zfs', 'destroy', combined])
    return {"msg": 'remove_snapshot_done', 'ffs': ffs, 'snapshots': get_snapshots(ffs), 'snapshot': snapshot_name}


def msg_zpool_status(msg):
    status = subprocess.check_output(
        ['sudo', 'zpool', 'status']).decode('utf-8')
    return {'msg': 'zpool_status', 'status': status}


def list_all_users():
    import pwd
    return [x.pw_name for x in pwd.getpwall()]


def msg_chown_and_chmod(msg):
    ffs = msg['ffs']
    full_ffs_path = find_ffs_prefix() + ffs
    if full_ffs_path not in list_ffs(False, True):
        raise ValueError("invalid ffs")
    if not 'user' in msg:
        raise ValueError("no user set")
    user = msg['user']
    if user not in list_all_users():
        raise ValueError("invalid user")
    if 'rights' not in msg:
        raise ValueError("no rights set")
    if not re.match("^0[0-7]{3}$", msg['rights']) and not re.match("^([ugoa][+=-][rwxXst]*)+$", msg['rights']):
        raise ValueError("invalid rights - needs to look like 0777")
    subprocess.check_call(['sudo', 'chown', user, '/' + full_ffs_path, '-R'])
    subprocess.check_call(
        ['sudo', 'chmod', msg['rights'], '/' + full_ffs_path, '-R'])
    return {'msg': 'chmod_and_chown_done', 'ffs': ffs}


def clean_up_clones():
    for fn in os.listdir('/' + clone_dir):
        cmd = ['sudo', 'zfs', 'destroy', clone_dir + '/' + fn]
        p = subprocess.Popen(cmd).communicate()


def msg_send_snapshot(msg):
    ffs_from = msg['ffs']
    full_ffs_path = find_ffs_prefix() + ffs_from
    if full_ffs_path not in list_ffs(False, True):
        raise ValueError("invalid ffs")
    target_host = msg['target_host']
    target_user = msg['target_user']
    target_ssh_cmd = msg['target_ssh_cmd']
    if not target_ssh_cmd[0] == 'ssh':
        raise ValueError("Invalid ssh command - first value must be 'ssh'")
    target_path = msg['target_path']
    target_ffs = target_path[target_path.find("/%%ffs%%/") + len('/%%ffs%%/'):]
    snapshot = msg['snapshot']
    my_hash = hashlib.md5()
    my_hash.update(ffs_from.encode('utf-8'))
    my_hash.update(target_path.encode('utf-8'))
    my_hash.update(target_host.encode('utf-8'))
    my_hash = my_hash.hexdigest()
    clone_name = "%f_%s" % (time.time(), my_hash)
    try:
        # step -1 - make sure we have an .ffs_sync_clones directory.
        subprocess.Popen(['sudo', 'zfs', 'create', clone_dir],
                         stderr=subprocess.PIPE).communicate() # ignore the error on this one.
        
        # step 0 - prepare a clone to rsync from
        p = subprocess.Popen(['sudo', 'zfs', 'clone', full_ffs_path + '@' + snapshot, clone_dir + '/' + clone_name],
            stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            if b'dataset does not exist' in stderr:
                raise ValueError("invalid snapshot")
            else:
                raise ValueError("Could not clone. Error:%s" % stderr)

        # step1 - set readonly=false on receiver
        cmd = target_ssh_cmd + ["%s@%s" % (target_user, target_host), '-T']

        p = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
        #
        stdout, stderr = p.communicate(json.dumps({
            'msg': 'set_properties',
            'ffs': target_ffs,
            'properties': {'readonly': 'off'}
        }).encode('utf-8'))
        if p.returncode != 0:
            return {
                'error': 'set_properties_read_only_off',
                'content': "stdout:\n%s\n\nstderr:\n%s" % (stdout, stderr)
            }
        # step2: rsync
        rsync_cmd = {
            'source_path': '/' + clone_dir + '/' + clone_name,
            'target_path': target_path,
            'target_host': target_host,
            'target_user': target_user,
            'target_ssh_cmd': target_ssh_cmd,
            'cores': -1,
        }
        p = subprocess.Popen(['python3', '/home/ffs/robust_parallel_rsync.py'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
        rsync_stdout, rsync_stderr = p.communicate(
            json.dumps(rsync_cmd).encode('utf-8'))
        rc = p.returncode
        if rc != 0:
            return {
                'error': 'rsync_failure',
                'content': "stdout:\n%s\n\nstderr:\n%s" % (rsync_stdout, rsync_stderr)
            }
        p = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
        # step4: restore readonly
        stdout, stderr = p.communicate(json.dumps({
            'msg': 'set_properties',
            'ffs': target_ffs,
            'properties': {'readonly': 'on'}
        }).encode('utf-8'))
        if p.returncode != 0:
            return {
                'error': 'set_properties_read_only_on',
                'content': "stdout:\n%s\n\nstderr:\n%s" % (stdout, stderr)
            }
        # step5: make a snapshot
        p = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
        #
        stdout, stderr = p.communicate(json.dumps({
            'msg': 'capture',
            'ffs': target_ffs,
            'snapshot': snapshot
        }).encode('utf-8'))
        if p.returncode != 0:
            return {
                'error': 'snapshot_after_rsync',
                'content': "stdout:\n%s\n\nstderr:\n%s" % (stdout, stderr)
            }
        return {
            'msg': 'send_snapshot_done',
            'target_host': target_host,
            'ffs': ffs_from,
            'clone_name': clone_name,
            'snapshot': snapshot,
        }

    finally:
        clean_up_clones()


def msg_deploy(msg):
    import base64
    import zipfile
    subprocess.check_call(['sudo', 'chmod', 'u+rwX', '/home/ffs/', '-R'])
    subprocess.check_call(['sudo', 'chmod', 'u+rwX', '/home/ffs/.ssh', '-R'])
    org_dir = os.getcwd()
    try:
        os.chdir('/home/ffs')
        with open("/home/ffs/node.zip", 'wb') as op:
            op.write(
                base64.decodebytes(msg['node.zip'].encode('utf-8'))
            )
        with zipfile.ZipFile("/home/ffs/node.zip") as zf:
            zf.extractall()
        return {"msg": 'deploy_done'}
    finally:
        os.chdir(org_dir)


def shell_cmd_rprsync(cmd_line):
    """The receiving end of an rsync sync"""
    def path_ok(target_path):
        if '@' in target_path:
            raise ValueError("invalid path")
        if target_path.startswith('/tmp/RPsTests'):
            return True
        if target_path.startswith('/mf/scb'):
            return True
        if target_path.startswith('/' + find_ffs_prefix()):
            return True
        return False

    target_path = cmd_line[cmd_line.find('/'):]
    target_path = target_path.replace(
        "/%%ffs%%/", '/' + find_ffs_prefix() + '/')
    cmd_line = cmd_line.replace("/%%ffs%%/", '/' + find_ffs_prefix() + '/')
    path_ok(target_path)
    reset_rights = False
    if target_path.endswith('/.'):
        try:
            org_rights = os.stat(target_path)[stat.ST_MODE]
        except PermissionError:  # target directory is without +X
            subprocess.check_call(['sudo', 'chmod', 'oug+rwX', target_path])
            reset_rights = True
    real_cmd = cmd_line.replace('rprsync', 'sudo rsync')
    p = subprocess.Popen(real_cmd, shell=True)
    p.communicate()
    # if reset_rights:
    #subprocess.check_call(['sudo','chmod', '%.3o'  % (org_rights & 0o777), target_path])


def dispatch(msg):
    try:
        if msg['msg'] == 'list_ffs':
            result = msg_list_ffs()
        elif msg['msg'] == 'set_properties':
            result = msg_set_properties(msg)
        elif msg['msg'] == 'new':
            result = msg_new(msg)
        elif msg['msg'] == 'capture':
            result = msg_capture(msg)
        elif msg['msg'] == 'remove':
            result = msg_remove(msg)
        elif msg['msg'] == 'remove_snapshot':
            result = msg_remove_snapshot(msg)
        elif msg['msg'] == 'zpool_status':
            result = msg_zpool_status(msg)
        elif msg['msg'] == 'chown_and_chmod':
            result = msg_chown_and_chmod(msg)
        elif msg['msg'] == 'send_snapshot':
            result = msg_send_snapshot(msg)
        elif msg['msg'] == 'deploy':
            result = msg_deploy(msg)
        else:
            result = {'error': 'message_not_understood'}
    except Exception as e:
        import traceback
        #import sys
        #exc_info = sys.exc_info()
        tb = traceback.format_exc()
        result = {"error": 'exception', 'content': str(e), 'traceback': tb}
    if not isinstance(result, dict):
        result = {'error': 'non_dict_result'}
    return result
