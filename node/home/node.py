import json
import re
import subprocess


def zfs_output(cmd_line):
    return subprocess.check_output(cmd_line).decode('utf-8')


def _get_zfs_properties(zfs_name):
    lines = zfs_output(['zfs', 'get', 'all', zfs_name, '-H']
                       ).strip().split("\n")
    lines = [x.split("\t") for x in lines]
    result = {x[1]: x[2] for x in lines}
    return result


def get_zfs_property(zfs_name, property_name):
    return _get_zfs_properties(zfs_name)[property_name]


def list_zfs():
    zfs_list = zfs_output(['zfs', 'list', '-H']).strip().split("\n")
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


def list_ffs(strip_prefix=False, include_testing=False):
    res = [x for x in list_zfs() if x.startswith(find_ffs_prefix()) and (not x.startswith(
        find_ffs_prefix() + '.') or x.startswith(find_ffs_prefix() + '.ffs_testing'))]
    if strip_prefix:
        ffs_prefix = find_ffs_prefix()
        res = [x[len(ffs_prefix):] for x in res]
    return res

def list_snapshots():
    return [x.split("\t")[0] for x in zfs_output(['zfs', 'list', '-t', 'snapshot', '-H']).strip().split("\n")]

def get_snapshots(ffs):
    all_snapshots = list_snapshots()
    prefix = find_ffs_prefix() + ffs + '@'
    matching = [x[x.find("@") + 1:] for x in all_snapshots if x.startswith(prefix)]
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


def msg_set_property(msg):
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
        msg_chown_and_chmod(msg) # fields do match

    subprocess.check_call(
        ['sudo', 'zfs', 'snapshot', combined])
    return {'msg': 'capture_done', 'ffs': ffs, 'snapshot': snapshot_name}

def msg_remove(msg):
    ffs = msg['ffs']
    full_ffs_path = find_ffs_prefix() + ffs
    if full_ffs_path not in list_ffs(False, True):
        raise ValueError("invalid ffs")
    if not '/ffs' in full_ffs_path:
        raise ValueError("Unexpected")
    subprocess.check_call(['sudo','zfs','destroy', full_ffs_path])
    return {"msg": 'remove_done', 'ffs': ffs}

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
    subprocess.check_call(['sudo','zfs','destroy', combined])
    return {"msg": 'remove_snapshot_done', 'ffs': ffs, 'snapshots': get_snapshots(ffs)}

def msg_zpool_status(msg):
    status = subprocess.check_output(['sudo','zpool','status']).decode('utf-8')
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
    subprocess.check_call(['sudo', 'chmod', msg['rights'], '/' + full_ffs_path, '-R'])
    return {'msg': 'chmod_and_chown_done', 'ffs': ffs}

 


def dispatch(msg):
    try:
        if msg['msg'] == 'list_ffs':
            result = msg_list_ffs()
        elif msg['msg'] == 'set_property':
            result = msg_set_property(msg)
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
