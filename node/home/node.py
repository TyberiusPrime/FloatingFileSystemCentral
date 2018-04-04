import json
import os
import stat
import re
import subprocess
import time
import hashlib


def check_call(cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        raise subprocess.CalledProcessError(p.returncode, cmd, "stdout:\n%s\n\nstderr:\n%s\n" % (stdout, stderr))
    return True

def check_output(cmd):
    return subprocess.check_output(cmd)


def zfs_output(cmd_line):
    p = subprocess.Popen(cmd_line, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        raise subprocess.CalledProcessError(
            p.returncode, cmd_line, stdout + stderr)
    return stdout.decode('utf-8')


def _get_zfs_properties(zfs_name):
    lines = zfs_output(['sudo', 'zfs', 'get', 'all', zfs_name, '-H']
                       ).strip().split("\n")
    lines = [x.split("\t") for x in lines]
    result = {x[1]: x[2] for x in lines}
    result = {x[1]: x[2] for x in lines if not(
        x[1].startswith('ffs:') and x[3].startswith('inherited')
    )}
    return result


def get_zfs_property(zfs_name, property_name):
    return _get_zfs_properties(zfs_name)[property_name]


def list_zfs():
    zfs_list = zfs_output(['sudo', 'zfs', 'list', '-H']).strip().split("\n")
    zfs_list = [x.split("\t")[0] for x in zfs_list]
    return zfs_list

_cached_ffs_prefix = None


def find_ffs_prefix(storage_prefix_or_msg):  # also takes msg
    if isinstance(storage_prefix_or_msg, dict):
        storage_prefix = storage_prefix_or_msg['storage_prefix']
    else:
        storage_prefix = storage_prefix_or_msg
    result = storage_prefix[1:]
    if not result.endswith('/'):
        result += '/'
    return result


def get_clone_dir(msg):
    return find_ffs_prefix(msg) + '.ffs_sync_clones'


def list_ffs(storage_prefix, strip_prefix=False, include_testing=False):
    res = [x for x in list_zfs() if x.startswith(find_ffs_prefix(storage_prefix)) and (not x.startswith(
        find_ffs_prefix(storage_prefix) + '.'))]
    if strip_prefix:
        ffs_prefix = find_ffs_prefix(storage_prefix)
        res = [x[len(ffs_prefix):] for x in res]
    return res


def list_snapshots():
    return [x.split("\t")[0] for x in zfs_output(['sudo', 'zfs', 'list', '-t', 'snapshot', '-H', '-s', 'creation']).strip().split("\n")]


def get_snapshots(ffs, storage_prefix):
    all_snapshots = list_snapshots()
    prefix = find_ffs_prefix(storage_prefix) + ffs + '@'
    matching = [x[x.find("@") + 1:]
                for x in all_snapshots if x.startswith(prefix)]
    return matching


def msg_list_ffs(msg):
    result = {'msg': 'ffs_list', 'ffs': {}}
    ffs_prefix = find_ffs_prefix(msg)
    ffs_list = list_ffs(msg['storage_prefix'])
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
    full_ffs_path = find_ffs_prefix(msg) + ffs
    if full_ffs_path not in list_ffs(msg['storage_prefix'], False, True):
        raise ValueError("invalid ffs: '%s'" % full_ffs_path)
    for prop, value in msg['properties'].items():
        check_property_name_and_value(prop, value)

    for prop, value in msg['properties'].items():
        check_call(
            ['sudo', 'zfs', 'set', "%s=%s" % (prop, value), full_ffs_path])
    return {
        'msg': 'set_properties_done',
        'ffs': ffs,
        'properties': _get_zfs_properties(full_ffs_path),
    }


def msg_new(msg):
    ffs = msg['ffs']
    full_ffs_path = find_ffs_prefix(msg) + ffs
    for prop, value in msg['properties'].items():
        check_property_name_and_value(prop, value)

    parent_zfs = full_ffs_path[:full_ffs_path.rfind('/')]
    parent_readonly = get_zfs_property(parent_zfs, 'readonly') == 'on'
    if parent_readonly:
        check_call(
            ['sudo', 'zfs', 'set', 'readonly=off', parent_zfs])
    check_call(['sudo', 'zfs', 'create', full_ffs_path])
    if parent_readonly:
        check_call(
            ['sudo', 'zfs', 'set', 'readonly=on', parent_zfs])

    for prop, value in msg['properties'].items():
        check_call(
            ['sudo', 'zfs', 'set', "%s=%s" % (prop, value), full_ffs_path])
    return {
        'msg': 'new_done',
        'ffs': ffs,
        'properties': _get_zfs_properties(full_ffs_path),
    }


def msg_capture(msg):
    ffs = msg['ffs']
    snapshot_name = msg['snapshot']
    full_ffs_path = find_ffs_prefix(msg) + ffs
    if full_ffs_path not in list_ffs(msg['storage_prefix'], False, True):
        raise ValueError("invalid ffs")
    combined = '%s@%s' % (full_ffs_path, snapshot_name)
    if combined in list_snapshots():
        raise ValueError("Snapshot already exists")
    if 'chown_and_chmod' in msg and msg['chown_and_chmod']:
        msg['sub_path'] = '/'
        msg_chown_and_chmod(msg)  # fields do match

    check_call(
        ['sudo', 'zfs', 'snapshot', combined])
    return {'msg': 'capture_done', 'ffs': ffs, 'snapshot': snapshot_name}


def msg_remove(msg):
    ffs = msg['ffs']
    full_ffs_path = find_ffs_prefix(msg) + ffs
    if full_ffs_path not in list_ffs(msg['storage_prefix'], False, True):
        return {'msg': 'remove_failed', 'reason': 'target_does_not_exists', 'ffs': ffs}
    p = subprocess.Popen(['sudo', 'zfs', 'destroy', full_ffs_path, '-r'],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode == 0:
        return {"msg": 'remove_done', 'ffs': ffs}
    else:
        if b'target is busy' in stderr:
            check_call(
                ['sudo', 'zfs', 'set', 'ffs:remove_asap=on', full_ffs_path])
            return {'msg': 'remove_failed', 'reason': 'target_is_busy', 'ffs': ffs}
        else:
            return {'error': 'zfs_error_return', 'content': 'zfs destroy %s failed. stdout:%s \nstderr: %s' % (ffs, stdout, stderr)}


def msg_remove_snapshot(msg):
    ffs = msg['ffs']
    full_ffs_path = find_ffs_prefix(msg) + ffs
    if full_ffs_path not in list_ffs(msg['storage_prefix'], False, True):
        raise ValueError("invalid ffs")
    snapshot_name = msg['snapshot']
    combined = '%s@%s' % (full_ffs_path, snapshot_name)
    if combined not in list_snapshots():
        raise ValueError("invalid snapshot")
    try:
        check_call(['sudo', 'zfs', 'destroy', combined])
    except subprocess.CalledProcessError as e:
        if 'snapshot has dependent clones' in e.output:
            return {'msg': 'remove_snapshot_failed', 'ffs': ffs, 'snapshot': snapshot_name, 'error_msg': 'Snapshot had dependent clones.'}

    return {"msg": 'remove_snapshot_done', 'ffs': ffs, 'snapshots': get_snapshots(ffs, msg['storage_prefix']), 'snapshot': snapshot_name}


def msg_zpool_status(msg):
    status = check_output(
        ['sudo', 'zpool', 'status']).decode('utf-8')
    return {'msg': 'zpool_status', 'status': status}


def list_all_users():
    import pwd
    return [x.pw_name for x in pwd.getpwall()]


def msg_chown_and_chmod(msg):
    ffs = msg['ffs']
    sub_path = msg['sub_path']
    if '/..' in sub_path or '../' in sub_path:
        raise ValueError("sub path must not contain ../")
    if not sub_path.startswith('/'):
        raise ValueError("sub path must start with /")
    full_ffs_path = find_ffs_prefix(msg) + ffs
    if full_ffs_path not in list_ffs(msg['storage_prefix'], False, True):
        raise ValueError("invalid ffs")
    if 'user' not in msg:
        raise ValueError("no user set")
    user = msg['user']
    if user not in list_all_users():
        raise ValueError("invalid user")
    if 'rights' not in msg:
        raise ValueError("no rights set")
    if not re.match("^0[0-7]{3}$", msg['rights']) and not re.match("^([ugoa]+[+=-][rwxXst]*,?)+$", msg['rights']):
        raise ValueError("invalid rights - needs to look like 0777")
    check_call(
        ['sudo', 'chown', user, '/' + full_ffs_path + sub_path, '-R'])
    check_call(
        ['sudo', 'chmod', msg['rights'], '/' + full_ffs_path + sub_path, '-R'])
    return {'msg': 'chown_and_chmod_done', 'ffs': ffs}


def clean_up_clones(storage_prefix):
    clone_dir = get_clone_dir(storage_prefix)
    try:
        for fn in os.listdir('/' + clone_dir):
            cmd = ['sudo', 'zfs', 'destroy', clone_dir + '/' + fn]
            p = subprocess.Popen(cmd).communicate()
    except OSError:
        pass


def msg_send_snapshot(msg):
    ffs_from = msg['ffs']
    full_ffs_path = find_ffs_prefix(msg) + ffs_from
    if full_ffs_path not in list_ffs(msg['storage_prefix'], False, True):
        raise ValueError("invalid ffs")
    target_node = msg['target_node']
    target_host = msg['target_host']
    target_user = msg['target_user']
    target_ssh_cmd = msg['target_ssh_cmd']
    if not target_ssh_cmd[0] == 'ssh':
        raise ValueError("Invalid ssh command - first value must be 'ssh'")
    target_ffs = msg['target_ffs']
    target_storage_prefix = msg['target_storage_prefix']
    if not target_storage_prefix.startswith('/') or target_storage_prefix.endswith('/'):
        raise ValueError("Malformated target_storage_prefix")
    target_path = target_storage_prefix + '/' + target_ffs
    snapshot = msg['snapshot']
    my_hash = hashlib.md5()
    my_hash.update(ffs_from.encode('utf-8'))
    my_hash.update(target_path.encode('utf-8'))
    my_hash.update(target_node.encode('utf-8'))
    my_hash = my_hash.hexdigest()
    clone_name = "%f_%s" % (time.time(), my_hash)
    clone_dir = get_clone_dir(msg['storage_prefix'])
    try:
        # step -1 - make sure we have an .ffs_sync_clones directory.
        subprocess.Popen(['sudo', 'zfs', 'create', clone_dir],
                         stderr=subprocess.PIPE).communicate()  # ignore the error on this one.

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
            'properties': {'readonly': 'off'},
            'storage_prefix': target_storage_prefix,

            'from_sender': True,
        }).encode('utf-8'))
        if p.returncode != 0:
            return {
                'error': 'set_properties_read_only_off',
                'content': "stdout:\n%s\n\nstderr:\n%s" % (stdout, stderr)
            }
        try:
            r = json.loads(stdout.decode('utf-8'))
            if not 'msg' in r or r['msg'] != 'set_properties_done':
                return {
                'error': 'set_properties_read_only_off_no_json',
                'content': "stdout:\n%s\n\nstderr:\n%s" % (stdout, stderr)
            }


        except ValueError:
            return {
                'error': 'set_properties_read_only_off_no_json',
                'content': "stdout:\n%s\n\nstderr:\n%s" % (stdout, stderr)
            }

        # step2: rsync
        rsync_cmd = {
            'source_path': '/' + clone_dir + '/' + clone_name,
            'target_path': target_path,
            'target_host': target_host,
            'target_user': target_user,
            'target_ssh_cmd': target_ssh_cmd,
            'cores': 4, # limit to a 'sane' value - you will run into ssh-concurrent connection limits otherwise
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
            'properties': {'readonly': 'on'},
            'storage_prefix': target_storage_prefix,
            'from_sender': True,
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
            'snapshot': snapshot,

            'storage_prefix': target_storage_prefix,
            'from_sender': True,
        }).encode('utf-8'))
        if p.returncode != 0:
            return {
                'error': 'snapshot_after_rsync',
                'content': "stdout:\n%s\n\nstderr:\n%s" % (stdout, stderr)
            }
        return {
            'msg': 'send_snapshot_done',
            'target_node': target_node,
            'ffs': ffs_from,
            'clone_name': clone_name,
            'snapshot': snapshot,
        }

    finally:
        clean_up_clones(msg['storage_prefix'])
        #pass


def msg_deploy(msg):
    import base64
    import zipfile
    check_call(['sudo', 'chmod', 'u+rwX', '/home/ffs/', '-R'])
    check_call(['sudo', 'chmod', 'u+rwX', '/home/ffs/.ssh', '-R'])
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


def msg_rename(msg):
    ffs = msg['ffs']
    full_ffs_path = find_ffs_prefix(msg) + ffs
    lf = list_ffs(msg['storage_prefix'], False, True)
    if full_ffs_path not in lf:
        raise ValueError("invalid ffs")
    if not 'new_name' in msg:
        raise ValueError("no new_name set")
    new_name = msg['new_name']
    full_new_path = find_ffs_prefix(msg) + new_name
    if full_new_path in lf:
        raise ValueError("new_name already exists")
    check_call(
        ['sudo', 'zfs', 'rename', full_ffs_path, full_new_path])
    check_call(
        ['sudo', 'zfs', 'set', "ffs:renamed_from=%s" % (ffs, ), full_new_path])
    return {'msg': 'rename_done',
            'ffs': ffs,
            'new_name': new_name}


def iterate_parent_paths(path):
    parts = path.split("/")
    for i in reversed(range(2, len(parts) + 1)):
        yield "/".join(parts[:i])


def is_inside_ffs_root(path):
    zfs = list_zfs()
    if not path.startswith('/'):
        return False
    for pp in iterate_parent_paths(path):
        if pp[1:] in zfs:
            try:
                if get_zfs_property(pp[1:], 'ffs:root') == 'on':
                    return True
                else:
                    pass
            except KeyError:
                continue
    return False


def shell_cmd_rprsync(cmd_line):
    """The receiving end of an rsync sync"""
    def path_ok(target_path):
        if '@' in target_path:
            raise ValueError("invalid path")
        if target_path.startswith('/tmp/RPsTests'):
            return True
        if target_path.startswith('/mf/scb'):
            return True
        if is_inside_ffs_root(target_path):
            return True
        raise ValueError("Path rejected: '%s" % target_path)

    target_path = cmd_line[cmd_line.find('/'):]
    chmod_after = False
    todo = []
    if '@' in cmd_line:
        parts = target_path.split("@")
        target_path = parts[0]
        for p in parts[1:]:
            if p.startswith('chmod='):
                rights = p[p.find('=') + 1:]
                #todo sanity check rights
                todo.append(['sudo', 'chmod', rights, target_path, '-R'])
            elif p.startswith('chown='):
                user_group = p[p.find('=') + 1:]
                #todo: very user and group exists ar at least is valid...
                todo.append(['sudo', 'chown', user_group, target_path, '-R'])
            #elif p.startswith('chmod_before'):
                #chmod_before = True
            elif p.startswith('chmod_after'):
                chmod_after = True
            else:
                raise ValueError("Invalid @ command")

    path_ok(target_path)
    reset_rights = False
    if target_path.endswith('/.'):
        try:
            try:
                org_rights = os.stat(target_path)[stat.ST_MODE]
            except FileNotFoundError:
                # asume it's not mounted...
                check_call(['sudo', 'zfs', 'mount', target_path[1:-2]])
                org_rights = os.stat(target_path)[stat.ST_MODE]
        except PermissionError:  # target directory is without +X
            check_call(['sudo', 'chmod', 'oug+rwX', target_path])
            reset_rights = True
    real_cmd = cmd_line.replace('rprsync', 'sudo rsync')
    p = subprocess.Popen(real_cmd, shell=True)
    p.communicate()
    if todo and chmod_after:
        for cmd in todo:
            subprocess.check_call(cmd)


    # if reset_rights:
    #subprocess.check_call(['sudo','chmod', '%.3o'  % (org_rights & 0o777), target_path])


def check_storage_prefix(msg):
    if 'storage_prefix' not in msg:
        raise ValueError("No storage_prefix in msg")
    zfs_name = msg['storage_prefix'][1:]
    try:
        is_root = get_zfs_property(zfs_name, 'ffs:root') == 'on'
        if not is_root:
            raise ValueError("ffs:root not set to 'on' on storage_prefix %s" % msg[
                             'storage_prefix'][1:])
    except KeyError:
        raise ValueError("ffs:root not set on storage_prefix %s" %
                         msg['storage_prefix'][1:])
    except subprocess.CalledProcessError:
        raise ValueError("storage prefix not a ZFS")


def dispatch(msg):
    try:
        check_storage_prefix(msg)
        if msg['msg'] == 'list_ffs':
            result = msg_list_ffs(msg)
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
        elif msg['msg'] == 'rename':
            result = msg_rename(msg)

        else:
            result = {'error': 'message_not_understood'}
    except subprocess.CalledProcessError as e:
        import traceback
        tb = traceback.format_exc()
        result = {"error": 'exception', 'content': str(e), 'traceback': tb,
        'output': e.output}

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        result = {"error": 'exception', 'content': str(e), 'traceback': tb}
    if not isinstance(result, dict):
        result = {'error': 'non_dict_result'}
    return result
