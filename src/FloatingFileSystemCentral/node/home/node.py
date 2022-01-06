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
        raise subprocess.CalledProcessError(
            p.returncode, cmd, "stdout:\n%s\n\nstderr:\n%s\n" % (stdout, stderr)
        )
    return True


def check_output(cmd, timeout=None):
    if timeout:
        cmd = ["timeout", str(timeout)] + cmd
        try:
            return subprocess.check_output(cmd, timeout=timeout)
        except subprocess.CalledProcessError as e:
            if "124" in str(e):
                raise subprocess.TimeoutExpired()
            else:
                raise
    else:
        return subprocess.check_output(cmd, timeout=timeout)


def zfs_output(cmd_line):
    p = subprocess.Popen(cmd_line, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        raise subprocess.CalledProcessError(p.returncode, cmd_line, stdout + stderr)
    return stdout.decode("utf-8")


def _get_zfs_properties(zfs_name):
    lines = (
        zfs_output(["sudo", "zfs", "get", "all", zfs_name, "-H"]).strip().split("\n")
    )
    lines = [x.split("\t") for x in lines]
    result = {x[1]: x[2] for x in lines}
    result = {
        x[1]: x[2]
        for x in lines
        if not (x[1].startswith("ffs:") and x[3].startswith("inherited"))
    }
    return result


def get_zfs_property(zfs_name, property_name):
    return _get_zfs_properties(zfs_name)[property_name]


def zpool_disk_info():
    """Retrieve serial -> disk size info for a given zpool"""
    raw = subprocess.check_output(['sudo', '-n','zpool','status','-P','-L']).decode('utf-8')
    devs = re.findall("/dev/[^ ]+", raw)
    output = {}
    for dev in devs:
        if '/nvme' in dev: # nvme devices have their 'partition' encoded pN at the end
            dev = re.sub("p\\d+$",'', dev)
        else: # asume it's an old style /dev/sdxN
            dev = re.sub("\\d+$",'', dev)
        udev = udev_adm_info(dev)
        serial_pretty = udev.get('SCSI_IDENT_SERIAL', '???')
        size = get_disk_size_bytes(dev)
        output[serial_pretty] = size
    return output

def udev_adm_info(dev):
    """query udevadm output for a disk"""
    raw = subprocess.check_output(['udevadm','info','--query=all', '--name', dev]).decode('utf-8')
    out = {}
    for line in raw.split("\n"):
        if line.startswith("E:"):
            line = line[3:]
            key, value = line.split("=", 2)
            out[key] = value
    return out

def get_disk_size_bytes(dev):
    """query lsblk for disk size"""
    raw = subprocess.check_output(['lsblk','-io','TYPE,SIZE', '--bytes', dev]).decode('utf-8')
    for line in raw.split("\n"):
        if line.startswith('disk'):
            _d, size = line.split()
            try:
                return int(size)
            except ValueError:
                return -1
    return -2

def list_zfs():
    zfs_list = zfs_output(["sudo", "zfs", "list", "-H"]).strip().split("\n")
    zfs_list = [x.split("\t")[0] for x in zfs_list]
    return zfs_list


_cached_ffs_prefix = None


def find_ffs_prefix(storage_prefix_or_msg):  # also takes msg
    if isinstance(storage_prefix_or_msg, dict):
        storage_prefix = storage_prefix_or_msg["storage_prefix"]
    else:
        storage_prefix = storage_prefix_or_msg
    result = storage_prefix[1:]
    if not result.endswith("/"):
        result += "/"
    return result


def get_clone_dir(msg):
    return find_ffs_prefix(msg) + ".ffs_sync_clones"


def list_ffs(storage_prefix, strip_prefix=False, include_testing=False):
    res = [
        x
        for x in list_zfs()
        if x.startswith(find_ffs_prefix(storage_prefix))
        and (not x.startswith(find_ffs_prefix(storage_prefix) + "."))
    ]
    if strip_prefix:
        ffs_prefix = find_ffs_prefix(storage_prefix)
        res = [x[len(ffs_prefix) :] for x in res]
    return res


def list_snapshots():
    return [
        x.split("\t")[0]
        for x in zfs_output(
            ["sudo", "zfs", "list", "-t", "snapshot", "-H", "-s", "creation"]
        )
        .strip()
        .split("\n")
    ]

def list_snapshots_for_ffs_unordered(zfs):
    # nice idea, but does not guarantee order
    return os.listdir("/" + zfs + "/.zfs/snapshot")

def list_snapshots_for_ffs(zfs):
    """Using zfs list gurantees olderst->newest order"""
    full= [
        x.split("\t")[0]
        for x in zfs_output(
            ["sudo", "zfs", "list", "-t", "snapshot", "-H", "-s", "creation", 
                zfs]
        )
        .strip()
        .split("\n")
    ]
    # now cut to just snapshot names
    return [x[x.find("@")+1:] for x in full]




def get_snapshots(ffs, storage_prefix):
    all_snapshots = list_snapshots()
    prefix = find_ffs_prefix(storage_prefix) + ffs + "@"
    matching = [x[x.find("@") + 1 :] for x in all_snapshots if x.startswith(prefix)]
    return matching


def msg_list_ffs(msg):
    result = {"msg": "ffs_list", "ffs": {}}
    ffs_prefix = find_ffs_prefix(msg)
    ffs_list = list_ffs(msg["storage_prefix"])
    ffs_info = {
        x[len(ffs_prefix) :]: {"snapshots": [], "properties": _get_zfs_properties(x)}
        for x in ffs_list
    }
    snapshots = list_snapshots()
    for x in snapshots:
        if x.startswith(ffs_prefix) and not x.startswith(ffs_prefix + "."):
            ffs_name = x[len(ffs_prefix) : x.find("@")]
            snapshot_name = x[x.find("@") + 1 :]
            ffs_info[ffs_name]["snapshots"].append(snapshot_name)
    result["ffs"] = ffs_info
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


def ensure_zfs_mounted(zfs_name):
    if not os.path.ismount("/" + zfs_name):
        zfs_parent = os.path.split(zfs_name)[0]
        restore_ro = False
        if get_zfs_property(zfs_parent, "readonly") == "on":
            restore_ro = True
            check_call(["sudo", "zfs", "set", "readonly=off", zfs_parent])
        try:
            check_call(["sudo", "zfs", "mount", zfs_name])
        finally:
            if restore_ro:
                check_call(["sudo", "zfs", "set", "readonly=on", zfs_parent])


def ensure_zfs_unmounted(zfs_name):
    if os.path.ismount("/" + zfs_name):
        zfs_parent = os.path.split(zfs_name)[0]
        restore_ro = False
        if get_zfs_property(zfs_parent, "readonly") == "on":
            restore_ro = True
            check_call(["sudo", "zfs", "set", "readonly=off", zfs_parent])
        try:
            check_call(["sudo", "zfs", "unmount", zfs_name])
        except subprocess.CalledProcessError:
            check_call(["sudo", "umount", "-lf", "/" + zfs_name])
            try:
                check_call(["rmdir", "/" + zfs_name])
            except subprocess.CalledProcessError:
                pass
        finally:
            if restore_ro:
                check_call(["sudo", "zfs", "set", "readonly=on", zfs_parent])


def msg_set_properties(msg):
    ffs = msg["ffs"]
    full_ffs_path = find_ffs_prefix(msg) + ffs
    if full_ffs_path not in list_ffs(msg["storage_prefix"], False, True):
        raise ValueError("invalid ffs: '%s'" % full_ffs_path)
    for prop, value in msg["properties"].items():
        check_property_name_and_value(prop, value)

    for prop, value in msg["properties"].items():
        check_call(["sudo", "zfs", "set", "%s=%s" % (prop, value), full_ffs_path])
    if msg.get("do_mount", False):
        ensure_zfs_mounted(full_ffs_path)
    return {
        "msg": "set_properties_done",
        "ffs": ffs,
        "properties": _get_zfs_properties(full_ffs_path),
    }


def msg_new(msg):
    ffs = msg["ffs"]
    full_ffs_path = find_ffs_prefix(msg) + ffs
    for prop, value in msg["properties"].items():
        check_property_name_and_value(prop, value)
    if not re.match("^0[0-7]{3}$", msg["rights"]) and not re.match(
        "^([ugoa]+[+=-][rwxXst]*,?)+$", msg["rights"]
    ):
        raise ValueError("invalid rights - needs to look like 0777")
    rights = msg["rights"]
    owner = msg["owner"]

    parent_zfs = full_ffs_path[: full_ffs_path.rfind("/")]
    parent_readonly = get_zfs_property(parent_zfs, "readonly") == "on"
    if parent_readonly:
        check_call(["sudo", "zfs", "set", "readonly=off", parent_zfs])
    check_call(["sudo", "zfs", "create", full_ffs_path])
    check_call(["sudo", "chown", owner, "/" + full_ffs_path])
    check_call(["sudo", "chmod", rights, "/" + full_ffs_path])
    # less we set readonly before actually changing the rights ^^
    for prop, value in msg["properties"].items():
        check_call(["sudo", "zfs", "set", "%s=%s" % (prop, value), full_ffs_path])

    if parent_readonly:
        check_call(["sudo", "zfs", "set", "readonly=on", parent_zfs])
    return {
        "msg": "new_done",
        "ffs": ffs,
        "properties": _get_zfs_properties(full_ffs_path),
    }


def msg_capture(msg):
    ffs = msg["ffs"]
    snapshot_name = msg["snapshot"]
    full_ffs_path = find_ffs_prefix(msg) + ffs
    if full_ffs_path not in list_ffs(msg["storage_prefix"], False, True):
        raise ValueError("invalid ffs")
    combined = "%s@%s" % (full_ffs_path, snapshot_name)
    if snapshot_name in list_snapshots_for_ffs_unordered(full_ffs_path):
        raise ValueError("Snapshot already exists")
    if "chown_and_chmod" in msg and msg["chown_and_chmod"]:
        msg["sub_path"] = "/"
        msg_chown_and_chmod(msg)  # fields do match

    check_call(["sudo", "zfs", "snapshot", combined])
    return {"msg": "capture_done", "ffs": ffs, "snapshot": snapshot_name}


def msg_capture_if_changed(msg):
    ffs = msg["ffs"]
    snapshot_name = msg["snapshot"]
    full_ffs_path = find_ffs_prefix(msg) + ffs
    if full_ffs_path not in list_ffs(msg["storage_prefix"], False, True):
        raise ValueError("invalid ffs")
    combined = "%s@%s" % (full_ffs_path, snapshot_name)
    sn_list = list_snapshots_for_ffs_unordered(full_ffs_path)
    if snapshot_name in sn_list:
        raise ValueError("Snapshot already exists")
    if "chown_and_chmod" in msg and msg["chown_and_chmod"]:
        msg["sub_path"] = "/"
        msg_chown_and_chmod(msg)  # fields do match

    ffs_snapshots = [x for x in sn_list if x.startswith("ffs-")]
    if ffs_snapshots:
        last_snapshot = ffs_snapshots[-1]
        cmd = [
            "sudo",
            "zfs",
            "diff",
            full_ffs_path + "@" + last_snapshot,
            full_ffs_path,
        ]
        try:
            try:
                ctx = check_output(cmd, 120).strip()
            except subprocess.TimeoutExpired:  # if it takes this long, just assume it's the real mccoy
                ctx = True
            if ctx:
                changed = True
            else:
                changed = False
        except subprocess.CalledProcessError:  # if it fails, snapshot to be safe
            changed = True
    else:
        changed = True  # no snapshot - changed
    if changed:
        check_call(["sudo", "zfs", "snapshot", combined])
    return {
        "msg": msg["msg"] + "_done",
        "ffs": ffs,
        "snapshot": snapshot_name,
        "changed": changed,
    }


def msg_remove(msg):
    ffs = msg["ffs"]
    full_ffs_path = find_ffs_prefix(msg) + ffs
    if full_ffs_path not in list_ffs(msg["storage_prefix"], False, True):
        return {"msg": "remove_failed", "reason": "target_does_not_exists", "ffs": ffs}
    check_call(["sudo", "zfs", "set", "ffs:remove_asap=on", full_ffs_path])
    p = subprocess.Popen(
        ["sudo", "zfs", "destroy", full_ffs_path, "-r"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = p.communicate()
    if p.returncode == 0:
        return {"msg": "remove_done", "ffs": ffs}
    else:
        if b"target is busy" in stderr or b"dataset is busy" in stderr:
            return {"msg": "remove_failed", "reason": "target_is_busy", "ffs": ffs}
        else:
            return {
                "error": "zfs_error_return",
                "content": "zfs destroy %s failed. stdout:%s \nstderr: %s"
                % (ffs, stdout, stderr),
            }


def msg_remove_snapshot(msg):
    ffs = msg["ffs"]
    full_ffs_path = find_ffs_prefix(msg) + ffs
    if full_ffs_path not in list_ffs(msg["storage_prefix"], False, True):
        raise ValueError("invalid ffs")
    snapshot_name = msg["snapshot"]
    combined = "%s@%s" % (full_ffs_path, snapshot_name)
    if combined not in list_snapshots():
        raise ValueError("invalid snapshot %s" % (combined,))
    try:
        check_call(["sudo", "zfs", "destroy", combined])
    except subprocess.CalledProcessError as e:
        if "snapshot has dependent clones" in e.output:
            return {
                "msg": "remove_snapshot_failed",
                "ffs": ffs,
                "snapshot": snapshot_name,
                "error_msg": "Snapshot had dependent clones.",
            }

    return {
        "msg": "remove_snapshot_done",
        "ffs": ffs,
        "snapshots": get_snapshots(ffs, msg["storage_prefix"]),
        "snapshot": snapshot_name,
    }


def msg_zpool_status(msg):
    status = check_output(["sudo", "zpool", "status"]).decode("utf-8")
    try:
        disks = zpool_disk_info()
    except subprocess.CalledProcessError:
        disks = {'failed_to_retrieve_disk_list_check_sudoers': 0}
    return {"msg": "zpool_status", "status": status, 'disks': disks}


def list_all_users():
    import pwd

    return [x.pw_name for x in pwd.getpwall()]


def msg_chown_and_chmod(msg):
    ffs = msg["ffs"]
    sub_path = msg["sub_path"]
    if "/.." in sub_path or "../" in sub_path:
        raise ValueError("sub path must not contain ../")
    if not sub_path.startswith("/"):
        raise ValueError("sub path must start with /")
    full_ffs_path = find_ffs_prefix(msg) + ffs
    if full_ffs_path not in list_ffs(msg["storage_prefix"], False, True):
        raise ValueError("invalid ffs")
    if "user" not in msg:
        raise ValueError("no user set")
    user = msg["user"]
    if user not in list_all_users():
        raise ValueError("invalid user")
    if "rights" not in msg:
        raise ValueError("no rights set")
    if not re.match("^0[0-7]{3}$", msg["rights"]) and not re.match(
        "^([ugoa]+[+=-][rwxXst]*,?)+$", msg["rights"]
    ):
        raise ValueError("invalid rights - needs to look like 0777")
    check_call_and_ignore_read_only_fs(
        ["sudo", "chown", user, "/" + full_ffs_path + sub_path, "-R"]
    )

    # can't use the find | xargs variant - xargs will stop on first error
    # and we have to at least ignore the read-only-filesystem errors
    check_call_and_ignore_read_only_fs(
        ["sudo", "chmod", msg["rights"], "/" + full_ffs_path + sub_path, "-R"]
    )
    return {"msg": "chown_and_chmod_done", "ffs": ffs}


def check_call_and_ignore_read_only_fs(cmd, *args, **kwargs):
    p = subprocess.Popen(cmd, stderr=subprocess.PIPE, *args, **kwargs)
    stdout, stderr = p.communicate()
    if p.returncode == 1:
        lines = stderr.strip().split(b"\n")
        lines = [l for l in lines if not b"Read-only file system" in l]
        if lines:
            raise subprocess.CalledProcessError(p.returncode, cmd, stdout, stderr)
        else:
            return True
    elif p.returncode == 0:
        return True
    else:
        raise subprocess.CalledProcessError(p.returncode, cmd, stdout, stderr)


def clean_up_clones(storage_prefix):
    clone_dir = get_clone_dir(storage_prefix)
    try:
        for fn in os.listdir("/" + clone_dir):
            cmd = [
                "sudo",
                "zfs",
                "destroy",
                clone_dir + "/" + fn,
                "-r",
            ]  # don't care if some auuto snapshot tried to snapshot these clones...
            p = subprocess.Popen(cmd).communicate()
    except OSError:
        pass


def msg_send_snapshot(msg):
    ffs_from = msg["ffs"]
    full_ffs_path = find_ffs_prefix(msg) + ffs_from
    if full_ffs_path not in list_ffs(msg["storage_prefix"], False, True):
        raise ValueError("invalid ffs")
    target_node = msg["target_node"]
    target_host = msg["target_host"]
    target_user = msg["target_user"]
    target_ssh_cmd = msg["target_ssh_cmd"]
    if not (target_ssh_cmd[0] == "ssh" or target_ssh_cmd[0].endswith('/ssh')):
        raise ValueError("Invalid ssh command - first value must be 'ssh'")
    target_ffs = msg["target_ffs"]
    target_storage_prefix = msg["target_storage_prefix"]
    if not target_storage_prefix.startswith("/") or target_storage_prefix.endswith("/"):
        raise ValueError("Malformated target_storage_prefix")
    target_path = target_storage_prefix + "/" + target_ffs
    excluded_subdirs = msg.get("excluded_subdirs", [])
    for x in excluded_subdirs:
        if "/" in x:
            raise ValueError("excluded_subdirs can only exclude *direct* subdirs")
    snapshot = msg["snapshot"]
    my_hash = hashlib.md5()
    my_hash.update(ffs_from.encode("utf-8"))
    my_hash.update(target_path.encode("utf-8"))
    my_hash.update(target_node.encode("utf-8"))
    my_hash = my_hash.hexdigest()
    clone_name = "%f_%s" % (time.time(), my_hash)
    clone_dir = get_clone_dir(msg["storage_prefix"])
    # step -1 - make sure we have an .ffs_sync_clones directory.
    subprocess.Popen(
        ["sudo", "zfs", "create", clone_dir], stderr=subprocess.PIPE
    ).communicate()  # ignore the error on this one.
    # don't auto snapshot this one.
    subprocess.Popen(['sudo','zfs','set','com.sun:auto-snapshot=false', clone_dir]).communicate()

    # step 0 - prepare a clone to rsync from
    if msg.get(
        "source_is_readonly", False
    ):  # read from snapshot directly - for readonly pools
        source_path = "/" + full_ffs_path + "/.zfs/snapshot/" + snapshot
    else:
        source_path = "/" + clone_dir + "/" + clone_name
        p = subprocess.Popen(
            [
                "sudo",
                "zfs",
                "clone",
                full_ffs_path + "@" + snapshot,
                clone_dir + "/" + clone_name,
            ],
            stderr=subprocess.PIPE,
        )
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            if b"dataset does not exist" in stderr:
                raise ValueError("invalid snapshot")
            else:
                raise ValueError("Could not clone. Error:%s" % stderr)

    # step1 - set readonly=false on receiver
    cmd = target_ssh_cmd + ["%s@%s" % (target_user, target_host), "-T"]

    p = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE
    )
    #
    stdout, stderr = p.communicate(
        json.dumps(
            {
                "msg": "set_properties",
                "ffs": target_ffs,
                "properties": {"readonly": "off"},
                "storage_prefix": target_storage_prefix,
                "do_mount": True,
            }
        ).encode("utf-8")
    )
    if p.returncode != 0:
        return {
            "error": "set_properties_read_only_off",
            "content": "stdout:\n%s\n\nstderr:\n%s" % (stdout, stderr),
        }
    try:
        r = json.loads(stdout.decode("utf-8"))
        if not "msg" in r or r["msg"] != "set_properties_done":
            return {
                "error": "set_properties_read_only_off_no_json",
                "content": "stdout:\n%s\n\nstderr:\n%s" % (stdout, stderr),
            }

    except ValueError:
        return {
            "error": "set_properties_read_only_off_no_json",
            "content": "stdout:\n%s\n\nstderr:\n%s" % (stdout, stderr),
        }

    # step2: rsync
    rsync_cmd = {
        "source_path": source_path,
        "target_path": target_path,
        "target_host": target_host,
        "target_user": target_user,
        "target_ssh_cmd": target_ssh_cmd,
        "cores": 4,  # limit to a 'sane' value - you will run into ssh-concurrent connection limits otherwise
        "excluded_subdirs": excluded_subdirs,
    }
    p = subprocess.Popen(
        ["python3", "/home/ffs/robust_parallel_rsync.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE,
    )
    rsync_stdout, rsync_stderr = p.communicate(json.dumps(rsync_cmd).encode("utf-8"))
    rc = p.returncode
    if rc != 0:
        return {
            "error": "rsync_failure",
            "content": "stdout:\n%s\n\nstderr:\n%s" % (rsync_stdout, rsync_stderr),
        }
    p = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE
    )
    # step4: restore readonly
    stdout, stderr = p.communicate(
        json.dumps(
            {
                "msg": "set_properties",
                "ffs": target_ffs,
                "properties": {"readonly": "on"},
                "storage_prefix": target_storage_prefix,
            }
        ).encode("utf-8")
    )
    if p.returncode != 0:
        return {
            "error": "set_properties_read_only_on",
            "content": "stdout:\n%s\n\nstderr:\n%s" % (stdout, stderr),
        }
    # step5: make a snapshot on receiver
    p = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE
    )
    #
    stdout, stderr = p.communicate(
        json.dumps(
            {
                "msg": "capture",
                "ffs": target_ffs,
                "snapshot": snapshot,
                "storage_prefix": target_storage_prefix,
            }
        ).encode("utf-8")
    )
    if p.returncode != 0:
        return {
            "error": "snapshot_after_rsync",
            "content": "stdout:\n%s\n\nstderr:\n%s" % (stdout, stderr),
        }
    # step 6: clean up *our* clone dir (close in time)
    if not msg.get(
        "source_is_readonly", False
    ):  # read from snapshot directly - for readonly pools
        p = subprocess.Popen(
            ["sudo", "zfs", "destroy", clone_dir + "/" + clone_name, "-r"],
            stderr=subprocess.PIPE,
        )
        stdout, stderr = p.communicate()
        # output of step6 is ignored
    res = {
        "msg": "send_snapshot_done",
        "target_node": target_node,
        "ffs": ffs_from,
        "snapshot": snapshot,
    }
    if not msg.get(
        "source_is_readonly", False
    ):  # read from snapshot directly - for readonly pools
        res["clone_name"] = clone_name
    return res


def msg_deploy(msg):
    import base64
    import zipfile

    check_call(["sudo", "chmod", "u+rwX", "/home/ffs/", "-R"])
    check_call(["sudo", "chmod", "u+rwX", "/home/ffs/.ssh", "-R"])
    org_dir = os.getcwd()
    try:
        os.chdir("/home/ffs")
        with open("/home/ffs/node.zip", "wb") as op:
            op.write(base64.decodebytes(msg["node.zip"].encode("utf-8")))
            op.flush()
        time.sleep(1)
        with zipfile.ZipFile("/home/ffs/node.zip") as zf:
            zf.extractall()
        clean_up_clones(msg["storage_prefix"])
        return {"msg": "deploy_done"}
    finally:
        os.chdir(org_dir)


def msg_rename(msg):
    ffs = msg["ffs"]
    full_ffs_path = find_ffs_prefix(msg) + ffs
    lf = list_ffs(msg["storage_prefix"], False, True)
    if full_ffs_path not in lf:
        raise ValueError("invalid ffs")
    if not "new_name" in msg:
        raise ValueError("no new_name set")
    new_name = msg["new_name"]
    full_new_path = find_ffs_prefix(msg) + new_name
    if full_new_path in lf:
        raise ValueError("new_name already exists")

    ensure_zfs_unmounted(full_ffs_path)
    check_call(["sudo", "zfs", "rename", full_ffs_path, full_new_path])
    check_call(["sudo", "zfs", "set", "ffs:renamed_from=%s" % (ffs,), full_new_path])
    try:
        ensure_zfs_mounted(full_new_path)
    except subprocess.CalledProcessError:
        pass
    return {"msg": "rename_done", "ffs": ffs, "new_name": new_name}


def msg_rollback(msg):
    ffs = msg["ffs"]
    full_ffs_path = find_ffs_prefix(msg) + ffs
    lf = list_ffs(msg["storage_prefix"], False, True)
    if full_ffs_path not in lf:
        raise ValueError("invalid ffs")
    if not "snapshot" in msg:
        raise ValueError("no snapshot set")
    snapshot = msg["snapshot"]
    if not snapshot in list_snapshots_for_ffs(full_ffs_path):
        raise ValueError("Snapshot not found %s" % (list_snapshots_for_ffs(full_ffs_path)))
    check_call(["sudo", "zfs", "rollback", "%s@%s" % (full_ffs_path, snapshot), "-r"])
    return {"msg": "rollback_done", "ffs": ffs, "snapshots": list_snapshots_for_ffs(full_ffs_path)}


def iterate_parent_paths(path):
    parts = path.split("/")
    for i in reversed(range(2, len(parts) + 1)):
        yield "/".join(parts[:i])


def is_inside_ffs_root(path):
    zfs = list_zfs()
    if not path.startswith("/"):
        return False
    for pp in iterate_parent_paths(path):
        if pp[1:] in zfs:
            try:
                if get_zfs_property(pp[1:], "ffs:root") == "on":
                    return True
                else:
                    pass
            except KeyError:
                continue
    return False


def shell_cmd_rprsync(cmd_line):
    """The receiving end of an rsync sync"""

    def path_ok(target_path):
        if "@" in target_path:
            raise ValueError("invalid path")
        if target_path.startswith("/tmp/RPsTests"):
            return True
        if target_path.startswith("/mf/scb"):
            return True
        if target_path.startswith("/mf/secrets"):
            return True

        if is_inside_ffs_root(target_path):
            return True
        raise ValueError("Path rejected: '%s" % target_path)

    target_path = cmd_line[cmd_line.find("/") :]
    chmod_after = False
    todo = []
    do_sudo = True
    if "@@@" in cmd_line:
        parts = cmd_line.split("@@@")
        target_path = parts[0][parts[0].find("/") :]
        for p in parts[1:]:
            if p.startswith("chmod="):
                rights = p[p.find("=") + 1 :]
                # todo sanity check rights
                todo.append(["sudo", "chmod", rights, target_path, "-R"])
            elif p.startswith("chown="):
                user_group = p[p.find("=") + 1 :]
                # todo: very user and group exists ar at least is valid...
                todo.append(["sudo", "chown", user_group, target_path, "-R"])
            # elif p.startswith('chmod_before'):
            # chmod_before = True
            elif p.startswith("chmod_after"):
                chmod_after = True
            elif p.startswith("no_sudo"):
                do_sudo = False
            else:
                raise ValueError("Invalid @ command")

    path_ok(target_path)
    reset_rights = False
    if target_path.endswith("/."):
        try:
            org_rights = os.stat(target_path)[stat.ST_MODE]
        except PermissionError:  # target directory is without +X
            check_call(["sudo", "chmod", "oug+rwX", target_path])
            reset_rights = True
    if do_sudo:
        real_cmd = cmd_line.replace("rprsync", "sudo rsync")
    else:
        real_cmd = cmd_line.replace("rprsync", "rsync")
    if "@@@" in real_cmd:
        real_cmd = real_cmd[: real_cmd.find("@@@")]
    p = subprocess.Popen(real_cmd, shell=True)
    p.communicate()
    if todo and chmod_after:
        for cmd in todo:
            subprocess.check_call(cmd)

    # if reset_rights:
    # subprocess.check_call(['sudo','chmod', '%.3o'  % (org_rights & 0o777), target_path])


def check_storage_prefix(msg):
    if "storage_prefix" not in msg:
        raise ValueError("No storage_prefix in msg")
    zfs_name = msg["storage_prefix"][1:]
    try:
        is_root = get_zfs_property(zfs_name, "ffs:root") == "on"
        if not is_root:
            raise ValueError(
                "ffs:root not set to 'on' on storage_prefix %s"
                % msg["storage_prefix"][1:]
            )
    except KeyError:
        raise ValueError(
            "ffs:root not set on storage_prefix %s" % msg["storage_prefix"][1:]
        )
    except subprocess.CalledProcessError as e:
        raise ValueError("storage prefix not a ZFS / called process error: ", e)


def dispatch(msg):
    try:
        check_storage_prefix(msg)
        if msg["msg"] == "list_ffs":
            result = msg_list_ffs(msg)
        elif msg["msg"] == "set_properties":
            result = msg_set_properties(msg)
        elif msg["msg"] == "new":
            result = msg_new(msg)
        elif msg["msg"] == "capture":
            result = msg_capture(msg)
        elif msg["msg"] == "capture_if_changed":
            result = msg_capture_if_changed(msg)
        elif msg["msg"] == "remove":
            result = msg_remove(msg)
        elif msg["msg"] == "remove_snapshot":
            result = msg_remove_snapshot(msg)
        elif msg["msg"] == "zpool_status":
            result = msg_zpool_status(msg)
        elif msg["msg"] == "chown_and_chmod":
            result = msg_chown_and_chmod(msg)
        elif msg["msg"] == "send_snapshot":
            result = msg_send_snapshot(msg)
        elif msg["msg"] == "deploy":
            result = msg_deploy(msg)
        elif msg["msg"] == "rename":
            result = msg_rename(msg)
        elif msg["msg"] == "rollback":
            result = msg_rollback(msg)

        else:
            result = {"error": "message_not_understood"}
    except subprocess.CalledProcessError as e:
        import traceback

        tb = traceback.format_exc()
        result = {
            "error": "exception",
            "content": str(e),
            "traceback": tb,
            "output": e.output,
        }

    except Exception as e:
        import traceback

        tb = traceback.format_exc()
        result = {"error": "exception", "content": str(e), "traceback": tb}
    if not isinstance(result, dict):
        result = {"error": "non_dict_result"}
    return result
