import base64
import collections
import time
import pprint
import shutil
import os
import re
from collections import OrderedDict
from . import ssh_message_que
from . import default_config
from .exceptions import (StartupNotDone, EngineFaulted, SSHConnectFailed, ManualInterventionNeeded, CodingError, InconsistencyError,
                         InProgress, MoveInProgress, NewInProgress, RemoveInProgress, RenameInProgress, InvalidTarget, RestartError,)


class needs_startup:
    def __init__(self, fault_ok=False):
        if not isinstance(fault_ok, bool):
            raise ValueError('@needs_startup should read @needs_startup() ')
        self.fault_ok = fault_ok
    
    def __call__(self, func):
        def wrapper(inner_self, *args, **kwargs):
            if inner_self.startup_done:
                if inner_self.faulted and not self.fault_ok:
                    return EngineFaulted(inner_self.faulted)
                else:
                    return func(inner_self, *args, **kwargs)
            else:
                raise StartupNotDone()
        return wrapper


class Engine:

    def __init__(self, config, sender=None, dry_run=False):
        """Config is a dictionary node_name -> node info
        send_function handles sending  messages to nodes
        and get's the node_info and a message passed"""
        self.logger = config.get_logging()
        self.config = config
        if not isinstance(config, default_config.CheckedConfig):
            raise ValueError("Config must be a CheckedConfig instance")
        self.node_config = self.config.get_nodes()
        if sender is None:
            if dry_run:
                sender = ssh_message_que.OutgoingMessagesDryRun(
                    self.logger, self, self.config.get_ssh_cmd())
            else:
                sender = ssh_message_que.OutgoingMessages(
                    self.logger, self, self.config.get_ssh_cmd())
        self.sender = sender
        self.node_ffs_infos = {}
        self.model = {}
        self.startup_done = False
        self.faulted = False
        self.trigger_message = None
        self.zpool_stati = {}
        self.error_callback = lambda x: False
        self.write_authorized_keys()
        self.deployment_zip_filename = os.path.join('node', 'node.zip')
        self.build_deployment_zip()
        self.deployment_count = 0

    def write_authorized_keys(self):
        if not os.path.exists(os.path.join('node', 'home', '.ssh')):
            os.makedirs(os.path.join('node', 'home', '.ssh'))
        fn = os.path.join('node', 'home', '.ssh', 'authorized_keys')
        if os.path.exists(fn):
            os.unlink(fn)
        with open(fn, 'wb') as op:
            for node in sorted(self.node_config):
                pub_key = self.node_config[node]['public_key']
                op.write(b'command="/home/ffs/ssh.py",no-port-forwarding,no-X11-forwarding,no-agent-forwarding %s\n' %
                         pub_key)

    def build_deployment_zip(self):
        shutil.make_archive(self.deployment_zip_filename[
                            :-4], 'zip', os.path.join('node', 'home'))

    def send(self, node_name, message):
        """allow sending by name"""
        message['storage_prefix'] = self.node_config[
            node_name]['storage_prefix']
        self.sender.send_message(
            node_name, self.node_config[node_name], message)

    def fault(self, message, trigger=None, exception=ManualInterventionNeeded):
        self.logger.error("Faulted engine with message: %s", message)
        self.config.complain("Faulted engine with message: %s" % message)
        if trigger:
            self.logger.error("Fault triggered by incoming message", trigger)
        self.faulted = message
        self.trigger_message = trigger
        self.sender.kill_unsent_messages()
        raise exception(message)

    def incoming_node(self, msg):
        if 'msg' not in msg:
            if 'content' in msg and 'error' in msg:
                if 'Connection refused' in msg['content']:
                    raise SSHConnectFailed()
            self.fault("No message in msg", msg)
        if 'from' not in msg:
            self.fault("No from in message - should not happen", msg)
        if msg['from'] not in self.node_config:
            self.fault("Invalid sender", msg)
        elif msg['msg'] == 'deploy_done':
            self.node_deploy_done(msg['from'])
        elif msg['msg'] == 'ffs_list':
            self.node_ffs_list(msg['ffs'], msg['from'])
        elif msg['msg'] == 'set_properties_done':
            self.node_set_properties_done(msg)
        elif msg['msg'] == 'new_done':
            self.node_new_done(msg)
        elif msg['msg'] == 'capture_done':
            self.node_capture_done(msg)
        elif msg['msg'] == 'send_snapshot_done':
            self.node_send_snapshot_done(msg)
        elif msg['msg'] == 'remove_snapshot_done':
            self.node_remove_snapshot_done(msg)
        elif msg['msg'] == 'remove_snapshot_failed':
            self.node_remove_snapshot_failed(msg)
        elif msg['msg'] == 'remove_done':
            self.node_remove_done(msg)
        elif msg['msg'] == 'remove_failed':
            self.node_remove_failed(msg)
        elif msg['msg'] == 'zpool_status':
            self.node_zpool_status(msg)
        elif msg['msg'] == 'rename_done':
            self.node_rename_done(msg)
        elif msg['msg'] == 'chown_and_chmod_done':
            self.node_chown_and_chmod_done(msg)
        else:
            self.fault("Invalid msg from node: %s" % msg)

    def incoming_client(self, msg):
        command = msg['msg']
        if command == 'startup':
            return self.client_startup()
        elif command == 'new':
            return self.client_new(msg)
        elif command == 'remove_target':
            return self.client_remove_target(msg)
        elif command == 'add_targets':
            return self.client_add_targets(msg)
        elif command == 'capture':
            return self.client_capture(msg)
        elif command == 'chown_and_chmod':
            return self.client_chown_and_chmod(msg)
        elif command == 'move_main':
            return self.client_move_main(msg)
        elif command == 'rename':
            return self.client_rename(msg)
        elif command == 'deploy':
            return self.client_deploy()
        elif command == 'service_que':
            return self.client_service_que()
        elif command == 'service_is_started':
            return self.client_service_is_started()
        elif command == 'service_restart':
            return self.client_service_restart()
        elif command == 'list_ffs':
            return self.client_list_ffs(msg)
        elif command == 'list_targets':
            return self.client_list_targets()
        elif command == 'set_snapshot_interval':
            return self.client_set_snapshot_interval(msg)
        elif command == 'set_priority':
            return self.client_set_priority(msg)

        else:
            raise ValueError("invalid message from client, ignoring")

    def client_service_restart(self):
        self.logger.info("Client requested restart")
        raise RestartError()

    @needs_startup(True)
    def client_list_ffs(self, msg):
        full = bool(msg.get('full', False))
        result = {}
        if not full:
            for ffs, ffs_info in self.model.items():
                result[ffs] = [ffs_info['_main']] + [x for x in ffs_info if x !=
                                                    ffs_info['_main'] and not x.startswith('_')]
        else:
            for ffs, ffs_info in self.model.items():
                result[ffs] = {'targets': [ffs_info['_main']] + [x for x in ffs_info if x !=
                                                    ffs_info['_main'] and not x.startswith('_')],
                }
                result[ffs]['properties'] = {node: self.model[ffs][node]['properties'] for node in result[ffs]['targets']}
        return result

    def client_list_targets(self):
        return list(self.config.get_nodes().keys())

    def client_service_is_started(self):
        return {'started': self.startup_done and not self.faulted}

    def client_service_que(self):
        if self.faulted:
            return {"error": "Engine is in faulted state.", 'Fault message': self.faulted}
        res = {}
        for node in self.sender.outgoing:
            res[node] = [{'status': x.status, 'msg': x.msg, 'runtime': x.get_runtime()} for x in
                         self.sender.prioritize(self.sender.outgoing[node])]
        return res

    def client_deploy(self):
        with open(self.deployment_zip_filename, 'rb') as op:
            d = op.read()
            return {'node.zip': base64.b64encode(d).decode('utf-8')}

    def client_startup(self):
        """Request a list of ffs from each and every of our nodes"""
        self.faulted = False
        self.logger.info("Client_startup(): id(self)=%s pid=%s" %
                         (id(self), os.getpid()))
        if not self.faulted:
            self.config.inform("Engine startup")
        import traceback

        if self.config.do_deploy():
            msg = {'msg': 'deploy'}
            with open(self.deployment_zip_filename, 'rb') as op:
                d = op.read()
                msg['node.zip'] = base64.b64encode(d).decode('utf-8')
            for node_name, node_info in sorted(self.node_config.items()):
                self.send(node_name, msg)
        else:
            msg = {'msg': 'list_ffs'}
            for node_name, node_info in sorted(self.node_config.items()):
                self.send(node_name, msg)

    def check_ffs_name(self, path):
        if not re.match("^[a-zA-Z0-9][A-Za-z0-9/_-]*$", path):
            raise ValueError("Invalid path: %s" % repr(path))
        
    def check_targets_have_parent(self, ffs, targets):
        if '/' in ffs:
            parent = os.path.split(ffs)[0]
            if parent not in self.model:
                raise ValueError("Parent ffs does not exist")
            targets_without_parent = set(targets).difference([x for x in self.model[parent] if not x.startswith('_')])
            if targets_without_parent:
                raise ValueError("Parent ffs not present on %s" % (targets_without_parent, ))


    @needs_startup()
    def client_new(self, msg):
        if 'ffs' not in msg:
            raise CodingError("No ffs specified")
        ffs = msg['ffs']
        self.check_ffs_name(ffs)
        if not self.config.accepted_ffs_name(ffs):
            raise ValueError("Config rejects this ffs name")
        if 'targets' not in msg:
            raise CodingError("No targets specified")
        if not isinstance(msg['targets'], list):
            raise CodingError("targets must be alist")
        if not msg['targets']:
            msg['targets'] = self.config.decide_targets(ffs)
            if not isinstance(msg['targets'], list):
                self.fault("config.decide_targets returned non-list")
        targets = [self.config.find_node(x) for x in msg['targets']]
        for x in targets:
            if x not in self.node_config:
                raise ValueError("Not a valid target: %s" % x)
        if ffs in self.model:
            raise ValueError("Already present, can't create as new")
        self.check_targets_have_parent(ffs, targets)
        main = targets[0]
        any_found = False
        for node in sorted(self.node_config):
            if node in targets:
                props = self.config.get_default_properties().copy()
                props.update(self.config.get_enforced_properties())
                if main == node:
                    props.update({'ffs:main': 'on',
                                  'readonly': 'off'
                                  })
                else:
                    props.update({'ffs:main': 'off',
                                  'readonly': 'on'
                                  })
                self.send(node,
                          {'msg': 'new',
                           'ffs': msg['ffs'],
                           'properties': props
                           }
                          )
                any_found = True
                if ffs not in self.model:
                    self.model[ffs] = {'_main': main}
                self.model[ffs][node] = {'_new': True}
                self.model[ffs][
                    '_snapshots_in_transit'] = collections.Counter()
        if any_found:
            return {'ok': True}
        else:
            return {'error': 'no_targets'}

    @needs_startup()
    def client_remove_target(self, msg):
        if 'ffs' not in msg:
            raise CodingError("No ffs specified")
        if 'target' not in msg:
            raise CodingError("target not specified")
        ffs = msg['ffs']
        target = self.config.find_node(msg['target'])
        if ffs not in self.model:
            raise ValueError("FFs unknown")
        if target not in self.model[ffs]:
            raise ValueError("Target not in list of targets")
        if self.model[ffs][target].get('_new', False):
            raise NewInProgress(
                "Target is still new - can not remove. Try again later.")
        if self.is_ffs_moving(ffs):
            raise MoveInProgress(
                "FFS is moving, can't remove targets during move. Try again later.")
        if self.is_ffs_renaming(ffs):
            raise RenameInProgress()
        if target == self.model[ffs]['_main']:
            raise ValueError("Remove failed, target is main - not removing")

        # little harm in sending it again if we're already removing
        self.send(target,
                  {'msg': 'remove',
                   'ffs': ffs}
                  )
        self.model[ffs][target] = {'removing': True}
        return {'ok': True}

    @needs_startup()
    def client_add_targets(self, msg):
        if 'ffs' not in msg:
            raise CodingError("No ffs specified")
        if 'targets' not in msg:
            raise CodingError("target nots specified")
        ffs = msg['ffs']
        if ffs not in self.model:
            raise ValueError("FFs unknown")
        if not isinstance(msg['targets'], list):
            raise CodingError("targets must be alist")

        targets = [self.config.find_node(x) for x in msg['targets']]
        self.check_targets_have_parent(ffs, targets)
        targets = sorted(set(targets))
        if not targets:
            raise ValueError("Empty target list")
        for target in targets:
            if target in self.model[ffs]:
                if self.model[ffs][target].get('removing', False):
                    raise RemoveInProgress(
                        "Remove in progress - can't add again before remove is completed")
                else:
                    raise ValueError("Add failed, target already in list")
        for target in targets:
            props = self.config.get_default_properties().copy()
            props.update(self.config.get_enforced_properties())
            props.update({'ffs:main': 'off', 'readonly': 'on'})
            self.send(target,
                      {'msg': 'new',
                       'ffs': ffs,
                       'properties': props,
                       }
                      )
            self.model[ffs][target] = {'_new': True}
        return {'ok': True}

    def any_new(self, ffs):
        for node, node_info in self.model[ffs].items():
            if not node.startswith('_') and node_info.get('_new', False):
                return True
        return False

    @needs_startup()
    def client_capture(self, msg):
        if 'ffs' not in msg:
            raise ValueError("No ffs specified")
        ffs = msg['ffs']
        if ffs not in self.model:
            raise ValueError("Nonexistant ffs specified")
        if self.any_new(ffs):
            raise NewInProgress(
                "New targets are currently being added to this ffs. Please try again later.")
        if self.is_ffs_moving(ffs):
            raise MoveInProgress(
                "This ffs is moving to a different main - you should not have been able to change the files anyhow")
        if self.is_ffs_renaming(ffs):
            raise RenameInProgress()
        postfix = msg.get('postfix', '')
        snapshot = self.do_capture(ffs, msg.get(
            'chown_and_chmod', False), postfix)
        return {'ok': True, 'snapshot': snapshot}

    def do_capture(self, ffs, chown_and_chmod, postfix=''):
        snapshot = self._name_snapshot(ffs, postfix)
        main = self.model[ffs]['_main']
        if not snapshot in self.config.decide_snapshots_to_send(ffs, self.model[ffs][main]['snapshots'] + [snapshot]):
            self.fault("config.decide_on_snapshots_to_send did not include newly captured snapshot %s - check your configuration code" % snapshot,
                       )

        out_msg = {
            'msg': 'capture',
            'ffs': ffs,
            'snapshot': snapshot,
        }
        if chown_and_chmod:
            out_msg['chown_and_chmod'] = True
            out_msg['user'] = self.config.get_chown_user(ffs)
            out_msg['rights'] = self.config.get_chmod_rights(ffs)
        self.send(
            self.model[ffs]['_main'],
            out_msg
        )
        # so we don't reuse the name. ever
        node_info = self.model[ffs][self.model[ffs]['_main']]
        if snapshot in node_info['upcoming_snapshots']:
            self.fault(
                "Adding a snapshot to upcoming snapshot that was already present", exception=CodingError)
        node_info['upcoming_snapshots'].append(snapshot)
        return snapshot

    @needs_startup()
    def client_chown_and_chmod(self, msg):
        if 'ffs' not in msg:
            raise CodingError("No ffs specified")
        if 'sub_path' not in msg:
            raise CodingError("No sub_path specified")

        ffs = msg['ffs']
        sub_path = msg['sub_path']
        if '..' in sub_path:
            raise ValueError("../ paths not allowed")
        if not sub_path.startswith('/'):
            raise ValueError("sub_path must start with /")
        if ffs not in self.model:
            raise ValueError("Nonexistant ffs specified")
        if self.is_ffs_moving(ffs):
            raise MoveInProgress(
                "This ffs is moving to a different main - you should not have been able to change the files anyhow")
        if self.is_ffs_renaming(ffs):
            raise RenameInProgress()
        self.send(
            self.model[ffs]['_main'],
            {
                'msg': 'chown_and_chmod',
                'ffs': ffs,
                'sub_path': sub_path,
                'user': self.config.get_chown_user(ffs),
                'rights': self.config.get_chmod_rights(ffs)
            }
        )
        return {"ok": True}

    @needs_startup()
    def client_move_main(self, msg):
        # flow is as follows
        # 1 - set ffs:moving=target on old main, set read only.
        # 2 - capture on old main
        # 3 - replicate
        # 4 - set ffs:main = False on old main
        # 5 - set main and remove ro on new main
        # 6 - remove ffs:moving on old_main

        if 'ffs' not in msg:
            raise CodingError("no ffs specified'")
        ffs = msg['ffs']
        if ffs not in self.model:
            raise ValueError("Nonexistant ffs specified")
        if 'target' not in msg:
            raise ValueError("Missing target (=new_main) in msg")
        try:
            target = self.config.find_node(msg['target'])
        except InvalidTarget:
            raise InvalidTarget("Move failed, invalid target.")
        if target.startswith("_"):
            raise InvalidTarget("Move failed, invalid target.")
        if target not in self.model[ffs]:
            raise InvalidTarget("Move failed, target does not have this ffs.")
        current_main = self.model[ffs]['_main']
        if target == current_main:
            raise ValueError("Move failed, target is already main")
        if self.is_ffs_removing_any(ffs):
            raise RemoveInProgress()
        if self.is_ffs_renaming(ffs):
            raise RenameInProgress()
        if self.is_ffs_new_any(ffs):
            raise NewInProgress()
        self.model[ffs]['_moving'] = target
        # self.model[ffs][current_main]['properties']['readonly'] = 'on'
        self.config.inform("Starting move for: %s" % ffs)
        self.send(current_main, {
            'msg': 'set_properties',
            'ffs': ffs,
            'properties': {
                'readonly': 'on',
                'ffs:moving_to': target,
            }
        })
        return {"ok": True}

    @needs_startup()
    def client_rename(self, msg):
        if 'ffs' not in msg:
            raise CodingError("no ffs specified'")
        ffs = msg['ffs']
        if not isinstance(ffs, str):
            raise ValueError("ffs parameter must be a string")
        if ffs not in self.model:
            raise ValueError("Nonexistant ffs specified")
        if 'new_name' not in msg:
            raise CodingError("no ffs specified'")
        new_name = msg['new_name']
        if not isinstance(new_name, str):
            raise ValueError("new_name parameter must be a string")
        if new_name in self.model:
            raise ValueError("An ffs with the new name already exists.")
        if self.is_ffs_moving(ffs):
            raise MoveInProgress()
        if self.is_ffs_removing_any(ffs):
            raise RemoveInProgress()
        if '_renaming' in self.model[ffs]:
            raise RenameInProgress()
        if any([node_info.get('_new', False) for node, node_info in self.model[ffs].items() if not node.startswith('_')]):
            raise NewInProgress()

        self.model[ffs]['_renaming'] = ('to', new_name)
        self.model[new_name] = {'_renaming': ('from', ffs)}

        for node in sorted(self.model[ffs]):
            if not node.startswith('_'):
                self.send(node, {
                    'msg': 'rename',
                    'ffs': ffs,
                    'new_name': new_name
                })
        return {'ok': True}

    @needs_startup()
    def client_set_snapshot_interval(self, msg):
        if 'ffs' not in msg:
            raise CodingError("no ffs specified'")
        ffs = msg['ffs']
        if not isinstance(ffs, str):
            raise ValueError("ffs parameter must be a string")
        if ffs not in self.model:
            raise ValueError("Nonexistant ffs specified")
        if 'interval' not in msg:
            raise CodingError("no interval specified'")
        interval = msg['interval']
        if not isinstance(interval, int):
            raise ValueError("interval parameter must be an integer")
        if interval < 0:
            raise ValueError("Interval must be positive")
        if interval == 0:
            interval = '-'
        else:
            interval = str(interval)
        # store on evyr node in order to remain stored on move
        for node in sorted(self.model[ffs]):
            if not node.startswith('_'):
                self.send(node, {
                    'msg': 'set_properties',
                    'ffs': ffs,
                    'properties': {'ffs:snapshot_interval': interval}
                })
        return {'ok': True}

    @needs_startup()
    def client_set_priority(self, msg):
        if 'ffs' not in msg:
            raise CodingError("no ffs specified'")
        ffs = msg['ffs']
        if not isinstance(ffs, str):
            raise ValueError("ffs parameter must be a string")
        if ffs not in self.model:
            raise ValueError("Nonexistant ffs specified")
        if 'priority' not in msg:
            raise CodingError("no priority specified'")
        priority = msg['priority']
        if not isinstance(priority, int) and priority != '-':
            raise ValueError("priority parameter must be an integer")
        priority = str(priority)
        # store on evyr node in order to remain stored on move
        for node in sorted(self.model[ffs]):
            if not node.startswith('_'):
                self.send(node, {
                    'msg': 'set_properties',
                    'ffs': ffs,
                    'properties': {'ffs:priority': priority}
                })
        return {'ok': True}

    def is_ffs_moving(self, ffs):
        if '_moving' in self.model[ffs]:
            return True
        if self.is_ffs_renaming(ffs):  # can be only one..
            return False
        main = self.model[ffs]['_main']
        moving_to = self.model[ffs][main][
            'properties'].get('ffs:moving_to', '-')
        if moving_to != '-':
            self.model[ffs]['_moving'] = moving_to
            return True
        return False

    def is_ffs_removing_any(self, ffs):
        return any([node_info.get('removing', False) for node, node_info in self.model[ffs].items() if not node.startswith('_')])

    def is_ffs_new_any(self, ffs):
        return any([node_info.get('_new', False) for node, node_info in self.model[ffs].items() if not node.startswith('_')])

    def is_ffs_renaming(self, ffs):
        return '_renaming' in self.model[ffs]

    def _name_snapshot(self, ffs, postfix=''):
        t = time.gmtime(time.time())
        t = ["%.4i" % t.tm_year, "%.2i" % t.tm_mon, "%.2i" % t.tm_mday,
             "%.2i" % t.tm_hour, "%.2i" % t.tm_min, "%.2i" % t.tm_sec]
        res = 'ffs-' + '-'.join(t)
        if postfix:
            res += '-' + postfix
        no = 'a'
        while (
            (res in self.model[ffs][self.model[ffs]['_main']]['snapshots']) or
            (res in self.model[ffs][self.model[ffs][
             '_main']].get('upcoming_snapshots', []))
        ):
            res = 'ffs-' + '-'.join(t)
            res += '-' + no
            if postfix:
                res += '-' + postfix
            no = chr(ord(no) + 1)
        return res

    def parse_time_from_snapshot(self, snapshot):
        if not snapshot.startswith('ffs-'):
            raise ValueError("Not an ffs- snapshot")
        import calendar
        parts = snapshot.split('-')
        year = int(parts[1])
        month = int(parts[2])
        day = int(parts[3])
        hour = int(parts[4])
        minute = int(parts[5])
        second = int(parts[6])
        return calendar.timegm((year, month, day, hour, minute, second, 0, 0, 0))

    def node_deploy_done(self, sender):
        msg = {'msg': 'list_ffs'}
        self.send(sender, msg)
        self.deployment_count += 1

    def node_ffs_list(self, ffs_list, sender):
        # remove those starting with .ffs_testing - only '.' filesystem that
        # the node's will report
        for x in list(ffs_list.keys()):
            # all other . something filesystems have been filtered by node.py
            # already
            if x.startswith('.ffs_testing'):
                del ffs_list[x]
        self.node_ffs_infos[sender] = ffs_list
        if len(self.node_ffs_infos) == len(self.node_config):
            self.build_model()
            self.startup_done = True
            self.logger.info("Startup complete")
            os = self.count_outgoing_snapshots()
            self.config.inform(
                "Startup completed, outstanding snapshot transfers: %i" % os)

    def build_model(self):
        self.config.inform("All nodes reported back, building model")
        self.logger.info("All list_ffs returned")
        self.model = {}
        for node, ffs_list in self.node_ffs_infos.items():
            for ffs, ffs_info in ffs_list.items():
                if ffs not in self.model:
                    self.model[ffs] = {}
                    self.model[ffs][
                        '_snapshots_in_transit'] = collections.Counter()
                self.model[ffs][node] = ffs_info
                self.model[ffs][node]['upcoming_snapshots'] = [] # these are snapshots that are being captured, - not in_transit!
        #print("stage 2")
        self._check_invalid_properties()
        #print("stage 3")
        self._parse_main_and_readonly()
        self._check_main_and_target_consistency()
        # do removal after assigning a main, so we can trigger on ffs:main=on
        # and ffs:remove_asap=on!
        #print("stage 4")
        self._handle_remove_asap()
        #print("stage 5")
        self._enforce_properties()
        #print("stage 6")
        self._prune_snapshots()
        #print("stage 7")
        self._send_missing_snapshots()
        #print("stage 8")
        self._capture_replicated_without_any_snapshots()
        #print("stage 9")

    def _check_invalid_properties(self):
        for ffs in self.model:
            for node, node_info in self.model[ffs].items():
                if not node.startswith('_'):
                    if node_info['properties'].get('ffs:root', '-') != '-':
                        self.fault("ffs:root set on sub ffs - nesting is not suported: %s" % ffs,
                                   exception=ManualInterventionNeeded)
                    for must_be_numeric, must_be_positive in [
                        ('snapshot_interval', True),
                        ('priority', False)]:
                        value = node_info['properties'].get(
                            'ffs:' + must_be_numeric, '-')
                        if value != '-':
                            try:
                                if must_be_positive and int(value) < 0:
                                    self.fault("ffs:%s was less than 0: %s on %s" % (must_be_numeric, ffs, node),
                                               exception=ManualInterventionNeeded)
                                int(value)
                            except ValueError:
                                self.fault("ffs:%s was not numeric: %s on %s" % (must_be_numeric, ffs, node),
                                           exception=ManualInterventionNeeded)

    def _check_main_and_target_consistency(self):
            for ffs in self.model:
                if not self.is_ffs_moving(ffs) and not self.is_ffs_renaming(ffs):
                    main = self.model[ffs]['_main']
                    main_info = self.model[ffs][main]
                    for node, node_info in self.model[ffs].items():
                        if node != main and not node.startswith('_'):
                            for prop in [
                                    'ffs:snapshot_interval',
                                    'ffs:priority']:
                                node_prop = node_info['properties'].get(prop, '-') 
                                if node_prop != '-': 
                                    main_prop = main_info['properties'].get(prop, '-') 
                                    if main_prop == '-':
                                        self.fault("%s set on %s on %s, but not on main (%s)."% (prop, ffs, node, main))
                                    elif main_prop != node_prop:
                                        self.send(node, {'msg': 'set_properties', 'ffs': ffs,
                                            'properties': {prop: main_prop}})

    def _handle_remove_asap(self):
        for ffs in self.model:
            main = self.model[ffs]['_main']
            for node in self.model[ffs]:
                if not node.startswith('_'):
                    if self.model[ffs][node]['properties'].get('ffs:remove_asap', '-') == 'on':
                        if node == main:
                            self.fault("ffs:main and ffs:remove_asap set at the same time. Manual fix necessory. FFS: %s, node%s" % (
                                ffs, node), exception=ManualInterventionNeeded)
                        else:
                            self.model[ffs][node]['removing'] = True
                            self.logger.info(
                                "Handling remove_asap for %s on %s", ffs, node)
                            self.send(node, {
                                'msg': 'remove',
                                'ffs': ffs,
                            })
                    else:
                        self.model[ffs][node]['removing'] = False

    def _parse_main_and_readonly(self):
        """Go through the ffs:main and readonly properties.
        Make sure there is exactly one main, it is non-readonly,
        and everything else is ffs:main=off and readonly
        """
        renames = {}  # from -> to

        for ffs, node_ffs_info in self.model.items():
            for node, node_info in node_ffs_info.items():
                if not node.startswith('_'):
                    props = node_info['properties']
                    ffs_rename_from = props.get('ffs:renamed_from', '-')
                    if ffs_rename_from != '-':
                        if ffs_rename_from in renames:
                            if renames[ffs_rename_from] != ffs:
                                self.fault("Multiple renames to different targets: %s: %s %s" %
                                           (ffs, renames[ffs_rename_from], ffs_rename_from), exception=InconsistencyError)
                        else:
                            renames[ffs_rename_from] = ffs
        if len(renames) != len(set(renames.values())):
            self.fault("Multiple renames to the same target",
                       exception=InconsistencyError)

        for ffs, node_ffs_info in self.model.items():
            main = None
            non_ro_count = 0
            ro_count = 0
            last_non_ro = None
            any_moving_to = None
            any_moving_from = None
            for node, node_info in node_ffs_info.items():
                if not node.startswith('_'):
                    props = node_info['properties']
                    if props.get('readonly', 'off') == 'on':
                        ro_count += 1
                    else:
                        non_ro_count += 1
                        last_non_ro = node
                    if props.get('ffs:main', 'off') == 'on':
                        if main is None:
                            main = node
                            # no break, - need to check for multiple
                        else:
                            self.fault("Multiple mains for %s" % ffs)
                    if props.get('ffs:moving_to', '-') != '-':
                        if any_moving_to is not None:
                            self.fault("Multiple moving_to for %s" % ffs)
                        any_moving_to = props['ffs:moving_to']
                        any_moving_from = node
            if main is None:
                if non_ro_count == 1:
                    # treat the only non-ro as the man
                    main = last_non_ro
                else:
                    if any_moving_to:
                        if ffs in renames.keys() or ffs in renames.values():
                            ffs_involved = [x for x in renames.items() if x[0] == ffs or x[
                                1] == ffs][0]
                            self.fault("FFS moving and renaming at the same time - data model inconsintent. ffs involved: %s" % ffs_involved,
                                       exception=InconsistencyError)
                        main = None  # stays None.
                    elif ffs in renames.keys() or ffs in renames.values():
                        main = None
                    else:
                        if non_ro_count > 1:
                            self.fault(
                                "No main, muliple non-readonly for '%s' on %s" % (ffs, [x for x in node_ffs_info.keys() if not x.startswith('_')]))
                        else:
                            self.fault(
                                "No main, none non-readonly for '%s' on %s" % (ffs, [x for x in node_ffs_info.keys() if not x.startswith('_')]))

            self.model[ffs]['_main'] = main
            if not any_moving_to:
                # make sure the right readonly/main properties are set.
                prop_adjust_messages = []
                for node in sorted(self.node_config):  # always in the same order
                    if node in node_ffs_info:
                        node_info = node_ffs_info[node]
                        props = node_info['properties']
                        if node == main:
                            if props.get('readonly', False) != 'off':
                                prop_adjust_messages.append((node, {'msg': 'set_properties', 'ffs': ffs,
                                                                    'properties': {'readonly': 'off'}}))

                            if props.get('ffs:main', 'off') != 'on':
                                prop_adjust_messages.append((
                                    node, {'msg': 'set_properties', 'ffs': ffs,
                                           'properties': {'ffs:main': 'on'}}))
                        else:
                            if props.get('readonly', 'off') != 'on':
                                prop_adjust_messages.append((node, {'msg': 'set_properties', 'ffs': ffs,
                                                                    'properties': {'readonly': 'on'}}))
                            if 'ffs:main' not in props:
                                prop_adjust_messages.append((
                                    node, {'msg': 'set_properties', 'ffs': ffs,
                                           'properties': {'ffs:main': 'off'}}))
                if prop_adjust_messages:
                    if ffs in renames.keys() or ffs in renames.values():
                        ffs_involved = [x for x in renames.items() if x[0] == ffs or x[
                            1] == ffs][0]
                        self.fault(
                            "Main/readonly inconsistencies during rename. Not supported. FFs involved: ", exception=InconsistencyError)
                    else:
                        for receiver, msg in prop_adjust_messages:
                            self.send(receiver, msg)
            else:  # caught in a move.
                # step 0 -
                move_target = any_moving_to
                if ((ffs in renames.keys() or ffs in renames.values()) or (
                    move_target in renames.keys() or move_target in renames.values())
                    ):
                    ffs_involved = [x for x in renames.items() if x[0] == ffs or x[1] == ffs or x[
                        0] == move_target or x[1] == move_target]
                    self.fault("Rename and move mixed. Unsupported: %s %s %s" % (
                        ffs, move_target, ffs_involved), exception=InconsistencyError)
                self.model[ffs]['_moving'] = move_target
                if main is not None:
                    if main != any_moving_to:
                        # we were before step 3, remove main, so we restart with a capture
                        # and ignore if we had already captured and replicated.
                        self.do_capture(ffs, False)
                    else:  # main had already been moved
                        # all that remains is to remove the moving marker
                        self.send(any_moving_from, {
                            'msg': 'set_properties',
                            'ffs': ffs,
                            'properties': {'ffs:moving_to': '-'}
                        })

                else:  # we had successfully captured and replicated and the main has been removed
                    # so we continue by setting main=on and readonly=off on the
                    # moving target...
                    self.send(any_moving_to, {
                        'msg': 'set_properties',
                        'ffs': ffs,
                        'properties': {'ffs:main': 'on', 'readonly': 'off'}
                    })
                    main = any_moving_to
                # we can deal with main being None until the ffs:moving_to = -
                # job  is done..
                node_ffs_info['_main'] = main

        for rename_from, rename_to in renames.items():
            any_missing = False
            for node, node_ffs_info in self.model[rename_to].items():
                if not node.startswith('_'):
                    ffs_renamed_from_missing = node_ffs_info[
                        'properties'].get('ffs:renamed_from', '-') == '-'
                    if ffs_renamed_from_missing:
                        any_missing = True
            if rename_from in self.model:  # at least one still needs to be renamed...
                if any_missing:  # this should only be removed once all were renamed. but there are open renames
                    self.fault("In move, but some targets did not have ffs:renamed_from. FFS involved: %s %s " % (rename_from, rename_to),
                               exception=InconsistencyError)

                self.model[rename_from]['_renaming'] = ('to', rename_to)
                self.model[rename_to]['_renaming'] = ('from', rename_from)
                for node in sorted(self.model[rename_from]):
                    if not node.startswith('_'):
                        self.send(node, {
                            'msg': 'rename',
                            'ffs': rename_from,
                            'new_name': rename_to,
                        })
            else:  # ok, properties need to be removed
                for node, node_info in sorted(self.model[rename_to].items()):
                    if not node.startswith('_'):
                        if node_info['properties'].get('ffs:renamed_from', '-') != '-':
                            self.send(node, {
                                'msg': 'set_properties',
                                'ffs': rename_to,
                                'properties': {'ffs:renamed_from': '-'}
                            })

        for ffs, node_ffs_info in self.model.items():
            if node_ffs_info['_main'] is None and not self.is_ffs_renaming(ffs):
                raise CodingError("Main remained None")

    def _enforce_properties(self):
        for ffs in self.model:
            for node, ffs_node_info in sorted(self.model[ffs].items()):
                if node.startswith('_'):
                    continue
                to_set = {}
                for k, v in self.config.get_enforced_properties().items():
                    v = str(v)
                    if ffs_node_info['properties'].get(k, False) != v:
                        to_set[k] = v
                if to_set:
                    self.send(
                        node, {'msg': 'set_properties',
                               'ffs': ffs, 'properties': to_set}
                    )

    def _prune_snapshots(self):
        for ffs in self.model.keys():
            if not self.is_ffs_renaming(ffs):
                self._prune_snapshots_for_ffs(ffs)

    def _prune_snapshots_for_ffs(self, ffs, restrict_to_node=None):
        node_fss_info = self.model[ffs]
        main_node = node_fss_info['_main']
        main_snapshots = node_fss_info[main_node]['snapshots']
        if not main_snapshots:
            return
        keep_snapshots = self.config.decide_snapshots_to_keep(
            ffs, main_snapshots)
        # always! keep the latest snapshot
        keep_snapshots.add(main_snapshots[-1])
        # also if a snapshot is yet to be send / is currently sending, we keep
        # it
        keep_snapshots.update(node_fss_info.get(
            '_snapshots_in_transit', {}).keys())
        self.logger.info("keeping for %s %s" % (ffs, keep_snapshots))
        if restrict_to_node is None or restrict_to_node == main_node:
            remove_from_main = [
                x for x in main_snapshots if x not in keep_snapshots]
            for snapshot in remove_from_main:
                self.send(
                    main_node, {'msg': 'remove_snapshot', 'ffs': ffs, 'snapshot': snapshot})
                # and forget they existed for now.
                node_fss_info[main_node]['snapshots'].remove(snapshot)
        for node in sorted(node_fss_info):
            if node != main_node and not node.startswith('_'):
                if restrict_to_node is None or restrict_to_node == node:
                    target_snapshots = node_fss_info[node]['snapshots']
                    too_many = [
                        x for x in target_snapshots if x not in keep_snapshots]
                    # never delete the last snapshot from a target
                    if len(too_many) == len(target_snapshots):
                        too_many = too_many[:-1]
                    for snapshot in too_many:
                        self.send(node, {'msg': 'remove_snapshot',
                                         'ffs': ffs, 'snapshot': snapshot})
                        # and forget they existed for now.
                        node_fss_info[node]['snapshots'].remove(snapshot)

    def _send_missing_snapshots(self):
        """Once we have prased the ffs_lists into our model (see _parse_main_and_readonly),
        and pruned the snapshots that we could,
        we send out replication requestes for the missing snapshots.

        We only send prev snapshots if there's an unbroken line to the current one.
        otherwise we'd have to discuss how and whether to roll back the state first
        and rollback to the current snapshot later, and rolling back
        eats later snapshots
        """
        def get_prio(ffs_node_info_tup):
            ffs, node_fss_info = ffs_node_info_tup 
            main = node_fss_info['_main']
            prio = int(node_fss_info[main]['properties'].get('ffs:priority', 1000))
            return prio

        ffs_to_consider = [(ffs, node_ffs_info) for (ffs, node_ffs_info) in self.model.items() if
                not self.is_ffs_moving(ffs) and not self.is_ffs_renaming(ffs)]
        for ffs, node_fss_info in sorted(ffs_to_consider, key=get_prio):
            main = node_fss_info['_main']
            main_snapshots = node_fss_info[main]['snapshots']
            if len([x for x in node_fss_info if x != main and not x.startswith('_')]) == 0:
                self.logger.info("No replicates for %s on %s", ffs, main)
            snapshots_to_send = set(
                self.config.decide_snapshots_to_send(ffs, main_snapshots))
            ordered_to_send = [
                x for x in main_snapshots if x in snapshots_to_send]
            self.logger.info(
                "Snapshots to consider sending for  %s - %s (main=%s)", ffs,  ordered_to_send, main)
            if ordered_to_send:
                for node, node_info in node_fss_info.items():
                    if node.startswith('_'):
                        continue
                    if node_info['removing']:
                        continue
                    missing = []
                    for sn in reversed(ordered_to_send):
                        if sn not in node_info['snapshots']:
                            missing.append(sn)
                        else:
                            break
                    missing = list(reversed(missing))
                    self.logger.info(
                        "Missing on %s for %s - %s", ffs, node, missing)
                    for sn in missing:
                        self._send_snapshot(main, node, ffs, sn)

    def _capture_replicated_without_any_snapshots(self):
        for ffs, node_fss_info in self.model.items():
            if self.is_ffs_moving(ffs) or self.is_ffs_renaming(ffs):
                continue
            main = node_fss_info['_main']
            main_snapshots = node_fss_info[main]['snapshots']
            has_replicates = [x for x in node_fss_info if x !=
                              main and not x.startswith('_')]
            if has_replicates:
                sendable_snapshots = self.config.decide_snapshots_to_send(
                    ffs, main_snapshots)
                if not sendable_snapshots:
                    self.logger.info(
                        "Replicated, but never snapshoted - capturing %s" % ffs)
                    self.do_capture(ffs, False)

    def _send_snapshot(self, sending_node, receiving_node, ffs, snapshot_name):
        excluded_sub_ffs = []
        for another_ffs in self.model:
            if another_ffs.startswith(ffs + '/'):
                remainder = another_ffs[len(ffs) + 1:]
                if not '/' in remainder: # don't nest
                    excluded_sub_ffs.append(remainder)
        msg = {
            'msg': 'send_snapshot',
            'ffs': ffs,
            'snapshot': snapshot_name,
            'target_host': self.node_config[receiving_node]['hostname'],
            # to be able to find it again later.
            'target_node': receiving_node,
            'target_user': 'ffs',
            'target_ssh_cmd': self.config.get_ssh_cmd(),
            'target_ffs': ffs,
            'target_storage_prefix': self.node_config[receiving_node]['storage_prefix'],
            'excluded_subdirs': excluded_sub_ffs,
        }
        prio = self.model[ffs][sending_node][
            'properties'].get('ffs:priority', None)
        if prio is not None:
            msg['priority'] = int(self.model[ffs][sending_node][
                                  'properties']['ffs:priority'])
        self.send(sending_node, msg)
        self.model[ffs]['_snapshots_in_transit'][snapshot_name] += 1

    def node_set_properties_done(self, msg):
        node = msg['from']
        if msg['ffs'] not in self.model:
            self.fault("set_properties_done from ffs not in model.",
                       msg, exception=InconsistencyError)
        ffs = msg['ffs']
        if node not in self.model[ffs]:
            self.fault("set_properties_done from ffs not on that node",
                       msg, exception=InconsistencyError)
        if 'properties' not in msg:
            self.fault("No properties in set_properties_done msg",
                       msg, CodingError)
        props = msg['properties']
        self.model[ffs][node]['properties'].update(props)
        if 'ffs:moving_to' in props:  # first step in moving to a new main
            if not self.is_ffs_moving(ffs):
                self.fault(
                    "Received unexpected set_propertes_done containing ffs:moving_to on an unmoving ffs", msg, InconsistencyError)
            if (props.get('ffs:main', False) == 'off' and
                    node == self.model[ffs]['_main']):
                # move step 4 done, happens after successful capture & replication & main=off on old main.
                # set main=on on new main.
                self.config.inform("Move step 4 done: %s" % ffs)
                self.send(self.model[ffs]['_moving'], {
                    'msg': 'set_properties',
                    'ffs': ffs,
                    'properties': {'readonly': 'off', 'ffs:main': 'on'},
                })
            else:
                moving_to = props['ffs:moving_to']
                if moving_to != '-':  # move step 1 done, proceed with step 2
                    self.config.inform("Move step 1 done %s" % ffs)
                    if '_move_snapshot' in self.model[ffs]:
                        self.fault(
                            'Repeated capture during move. A test case that does not correctly send ffs:moving_to?', msg, CodingError)
                    self.model[ffs][
                        '_move_snapshot'] = self.do_capture(ffs, False)
                else:  # move step 7 done, remove our _moving flag
                    self.config.inform(
                        "Move step 7 (final step) done: %s" % ffs)
                    del self.model[ffs]['_moving']
                    del self.model[ffs]['_move_snapshot']
                    if self.is_ffs_moving(ffs):
                        self.fault(
                            "Still moving after set_properties moving_to = -, Something is fishy ", msg, CodingError)
                    self._prune_snapshots_for_ffs(ffs)
        elif (  # happens after ffs:main=off on the old main.
                self.is_ffs_moving(ffs) and
                props.get('ffs:main', False) == 'on'):
            self.send(self.model[ffs]['_main'], {
                'msg': 'set_properties',
                'ffs': ffs,
                'properties': {'ffs:moving_to': '-'}
            })
            self.model[ffs]['_main'] = self.model[ffs]['_moving']

    def node_new_done(self, msg):
        node = msg['from']
        if msg['ffs'] not in self.model:
            self.fault("node_new_done from ffs not in model.",
                       msg, InconsistencyError)
        ffs = msg['ffs']
        if node not in self.model[ffs]:
            self.fault("node_new_done from ffs not on that node",
                       msg, CodingError)
        if self.model[ffs][node] != {'_new': True}:
            self.fault(
                "node_new_done from an node/ffs where we already have data", msg, CodingError)
        if msg['properties']['ffs:main'] == 'on' and not self.model[ffs]['_main'] == node:
            self.fault("ffs:main=on from non-main node", msg, CodingError)
        self.model[ffs][node] = {
            'snapshots': [],
            'upcoming_snapshots': [],
            'properties': msg['properties']
        }
        main = self.model[ffs]['_main']

        # This happens if we were actually a add_new_target
        if node != main:
            # case 1: adding a new ffs + replication targets
            # and we've returned before the main is done
            if self.model[ffs][main].get('_new', False):
                pass  # new ffs -> no snapshots to send
            else:  # either were in add_new_target, or the main was done before the rep targets
                # should only have snapshots to send in the add_new_target case
                to_send = self.config.decide_snapshots_to_send(
                    ffs, self.model[ffs][main]['snapshots'])
                if to_send:  # we have snapshots to send
                    for sn in self.model[ffs][main]['snapshots']:
                        if sn in to_send:  # to send is a set!
                            self._send_snapshot(main, node, ffs, sn)
                else:  # this ffs was never captured, but we want to sync the status quo.
                    self.do_capture(ffs, False)

    def node_capture_done(self, msg):
        node = msg['from']
        if 'ffs' not in msg:
            self.fault("missing ffs parameter", CodingError)
        ffs = msg['ffs']
        if ffs not in self.model:
            self.fault("capture_done from ffs not in model.",
                       msg, InconsistencyError)
        main = self.model[ffs]['_main']
        if main != node:
            self.fault("Capture message received from non main node",
                       msg, InconsistencyError)
        if 'snapshot' not in msg:
            self.fault("No snapshot in msg", msg, CodingError)
        snapshot = msg['snapshot']
        if snapshot in self.model[ffs][node]['snapshots']:
            self.fault("Snapshot was already in model", msg, CodingError)
        self.model[ffs][node]['snapshots'].append(snapshot)

        if snapshot in self.model[ffs][node].get('upcoming_snapshots', []):
            self.model[ffs][node]['upcoming_snapshots'].remove(snapshot)

        main = self.model[ffs]['_main']
        for node in sorted(self.node_config):
            if node != main and node in self.model[ffs] and not self.model[ffs][node].get('removing', False):
                postfix = self.model[ffs][node][
                    'properties'].get('ffs:postfix_only', True)
                if postfix is True or snapshot.endswith('-' + postfix):
                    self._send_snapshot(main, node, ffs, snapshot)
        if not self.is_ffs_moving(ffs):
            self._prune_snapshots_for_ffs(ffs, main)
        else:
            self.config.inform("Move step 2 done: %s" % ffs)

    def node_send_snapshot_done(self, msg):
        main = msg['from']
        if 'ffs' not in msg:
            self.fault("missing ffs parameter", msg, CodingError)
        ffs = msg['ffs']
        if ffs not in self.model:
            self.fault("send_snapshot_done from ffs not in model.",
                       msg, InconsistencyError)
        node = msg['target_node']
        if main == node:
            self.fault("Send done from main to main?!",
                       msg, InconsistencyError)
        if 'snapshot' not in msg:
            self.fault("No snapshot in msg", msg, CodingError)
        snapshot = msg['snapshot']
        if snapshot in self.model[ffs][node]['snapshots']:
            self.fault("Snapshot was already in model", msg, CodingError)
        
        self.model[ffs][node]['snapshots'].append(snapshot)
        self.model[ffs]['_snapshots_in_transit'][snapshot] -= 1
        if self.model[ffs]['_snapshots_in_transit'][snapshot] == 0:
            del self.model[ffs]['_snapshots_in_transit'][snapshot]
        os = self.count_outgoing_snapshots()
        self.config.inform("Send of %s@%s to %s done, outstanding snapshot transfers: %i" % (
            ffs, snapshot, node, os))
        if (self.is_ffs_moving(ffs) and
                node == self.model[ffs]['_moving'] and
                msg['snapshot'] == self.model[ffs]['_move_snapshot']
                ):
            self.config.inform(("Move step 3 done: %s" % ffs))
            self.send(self.model[ffs]['_main'], {
                'msg': 'set_properties',
                'ffs': ffs,
                'properties': {'ffs:main': 'off'}
            })
        else:
            if not self.is_ffs_moving(ffs):
                self._prune_snapshots_for_ffs(ffs, main)
                self._prune_snapshots_for_ffs(ffs, node)

    def node_remove_done(self, msg):
        node = msg['from']
        if 'ffs' not in msg:
            self.fault("missing ffs parameter", msg, CodingError)
        ffs = msg['ffs']
        if ffs not in self.model:
            self.fault("remove_done from ffs not in model.",
                       msg, InconsistencyError)
        if node not in self.model[ffs]:
            raise InconsistencyError(
                "remove_done from node not in ffs for this model")
        if self.model[ffs]['_main'] == node:
            self.fault("remove_done from main!", msg, InconsistencyError)
        del self.model[ffs][node]

    def node_remove_failed(self, msg):
        if msg['reason'] == 'target_is_busy':
            # just keep it in 'removing' status (or already removed).
            # the node will have set ffs:remove_asap=on and that will retrigger
            # removal upon startup
            pass
        elif msg['reason'] == 'target_does_not_exist':
            # most likely a repeated request from the user
            # ignore
            pass
        else:
            self.fault(
                "remove_failed with something other than target_is_busy: %s" % msg)

    def node_remove_snapshot_done(self, msg):
        node = msg['from']
        if 'ffs' not in msg:
            self.fault("missing ffs parameter", msg, CodingError)
        ffs = msg['ffs']
        if ffs not in self.model:
            self.fault("remove_done from ffs not in model.",
                       msg, InconsistencyError)
        if node not in self.model[ffs]:
            raise InconsistencyError(
                "remove_done from node not in ffs for this model")
        # we ignore the message if the snapshot had already been removed in our
        # database.
        if msg['snapshot'] in self.model[ffs][node]['snapshots']:
            self.model[ffs][node]['snapshots'].remove(msg['snapshot'])

    def node_remove_snapshot_failed(self, msg):
        self.config.complain("Non-fatal: Removal of snapshot %s@%s on %s failed with message: %s" %
                             (msg['ffs'], msg['snapshot'], msg['from'], msg['error_msg']))
        self.logger.error("Non-fatal: Removal of snapshot %s@%s on %s failed with message: %s" %
                          (msg['ffs'], msg['snapshot'], msg['from'], msg['error_msg']))

    def node_zpool_status(self, msg):
        # don't start checking before the newest code is downstream
        if self.deployment_count < len(self.node_config):
            return
        node = msg['from']
        status = {}
        for k in ['ONLINE', 'DEGRADED', 'UNAVAIL']:
            status[k] = msg['status'].count(k)
        if node in self.zpool_stati:
            old_status = self.zpool_stati[node]
            if old_status != status:
                self.error_callback(
                    "Zpool status changed: %s - %s" % (node, status))
        else:
            if status['DEGRADED'] or status['UNAVAIL']:
                self.error_callback("Zpool status: %s - %s" % (node, status))
        self.zpool_stati[node] = status

    def do_zpool_status_check(self):
        do_not_send_to = set()
        for node in self.node_config:
            do_send = True
            for msg in self.sender.get_messages_for_node(node):
                if msg['msg'] == 'zpool_status':
                    do_send = False
                    break
            if do_send:
                self.send(node, {'msg': 'zpool_status'})

    def count_outgoing_snapshots(self):
        count = 0
        for node in self.node_config:
            for msg in self.sender.get_messages_for_node(node):
                if msg['msg'] == 'send_snapshot':
                    count += 1
        return count

    def get_snapshot_interval(self, ffs):
        info = self.model[ffs]
        main = info['_main']
        interval = info[main]['properties'].get('ffs:snapshot_interval', '-')
        if interval == '-':
            return False
        else:
            return int(interval)

    def one_minute_passed(self):
        if not self.faulted:
            now = time.time()
            for ffs, ffs_info in self.model.items():
                iv = self.get_snapshot_interval(ffs)
                if iv and iv > 0:
                    self.logger.info("Checking snapshot interval for %s, interval=%ss", ffs, iv)
                    main = ffs_info['_main']
                    do_snapshot = False
                    if ffs_info[main]['upcoming_snapshots']:
                        # never auto snapshot while we're lagging behind.
                        self.logger.info("No auto snapshot, lagging behind: %s: %s", ffs, ffs_info[main]['upcoming_snapshots'])
                        pass
                    else:
                        if len(ffs_info[main]['snapshots']) == 0:
                            self.logger.info("No snapshot so far, %s", ffs)
                            do_snapshot = True
                        else:
                            try:
                                snapshot_time = self.parse_time_from_snapshot(
                                    ffs_info[main]['snapshots'][-1]
                                )
                                self.logger.info("Last snapshot time: %s, now: %s, make snapshot=%s", snapshot_time, now, snapshot_time + (iv) < now)
                                if snapshot_time + (iv) < now:
                                    self.logger.info("Auto-snapshot: %s" % ffs)
                                    do_snapshot = True
                            except ValueError:  # could not parse time, assume we need to redo it
                                do_snapshot = True
                                pass
                        if do_snapshot:
                            self.do_capture(ffs, False, 'auto')
            return True
        return False

    def node_rename_done(self, msg):
        node = msg['from']
        ffs = msg['ffs']
        new_name = msg['new_name']
        if '_renaming' not in self.model[ffs]:
            self.fault("rename_done from non-renaming ffs?!",
                       msg, InconsistencyError)
        if node not in self.model[ffs]:
            self.fault("rename_done from node not in model for this ffs",
                       msg, InconsistencyError)
        if self.model[ffs]['_renaming'][0] != 'to':
            self.fault("rename_done for ffs that is not renaming-from?",
                       msg, InconsistencyError)
        if '_renaming' not in self.model[new_name]:
            self.fault(
                'rename_done for new_name that was not renaming target?', msg, InconsistencyError)
        if self.model[new_name]['_renaming'][0] != 'from':
            self.fault(
                'rename_done for new_name that was not renaming target - case 2?', msg, InconsistencyError)
        if self.model[new_name]['_renaming'][1] != ffs:
            self.fault('rename_done for wrong source ffs?',
                       msg, InconsistencyError)
        rename_target = self.model[ffs]['_renaming'][1]
        if new_name != rename_target:
            self.fault(
                "rename_done new_name disagrees with _renaming (to, new_name)", msg, InconsistencyError)
        if node in self.model[rename_target]:
            self.fault(
                "rename_done for node that is already in the new target?", msg, InconsistencyError)
        self.model[rename_target][node] = self.model[ffs][node]
        del self.model[ffs][node]
        if '_main' in self.model[ffs] and node == self.model[ffs]['_main']:
            del self.model[ffs]['_main']
            self.model[rename_target]['_main'] = node
        if len([x for x in self.model[ffs] if not x.startswith('_')]) == 0:
            del self.model[ffs]
            del self.model[rename_target]['_renaming']
            if not '_main' in self.model[rename_target]:
                self.fault("No _main after rename?!", msg, InconsistencyError)
            for node in sorted(self.model[rename_target]):
                if not node.startswith('_'):
                    self.send(node, {
                        'msg': 'set_properties',
                        'ffs': rename_target,
                        'properties': {'ffs:renamed_from': '-'}
                    })

    def node_chown_and_chmod_done(self, msg):
        pass

    def shutdown(self):
        self.sender.shutdown()
