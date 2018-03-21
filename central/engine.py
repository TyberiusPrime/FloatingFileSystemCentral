import base64
import pprint
import shutil
import os
import re
from collections import OrderedDict
from . import ssh_message_que


class StartupNotDone(Exception):
    """To client, when the engine is still booting up"""
    pass


class EngineFaulted(Exception):
    """to client, when the engine is in ManualInterventionNeeded state"""
    pass


class ManualInterventionNeeded(ValueError):
    pass


class CodingError(ManualInterventionNeeded):
    pass


class InconsistencyError(ManualInterventionNeeded):
    pass


class MoveInProgress(ValueError):
    pass


def needs_startup(func):
    def wrapper(self, *args, **kwargs):
        if self.startup_done:
            if self.faulted:
                return EngineFaulted(self.faulted)
            else:
                return func(self, *args, **kwargs)
        else:
            raise StartupNotDone()
    return wrapper


class Engine:

    def __init__(self, config, sender=None, logger=None,
                 non_node_config=None):
        """Config is a dictionary node_name -> node info
        send_function handles sending  messages to nodes
        and get's the node_info and a message passed"""
        if logger is None:
            import logging
            logger = logging.Logger(name='Dummy')
            logger.addHandler(logging.NullHandler())
        self.logger = logger
        for node in config:
            if node.startswith('_'):
                raise ValueError("Node can not start with _: %s" % node)
        self.node_config = config
        if non_node_config is None:
            non_node_config = {}
        self.non_node_config = non_node_config
        allowed_options = {'chown_user', 'chmod_rights', 'ssh_cmd', 'inform',
                           'complain', 'enforced_properties', 'decide_snapshots_to_keep',
                           'decide_snapshots_to_send'}
        too_many_options = set(non_node_config).difference(allowed_options)
        if too_many_options:
            raise ValueError("Invalid option set: %s" % (too_many_options, ))
        if 'inform' not in non_node_config:
            non_node_config['inform'] = lambda x: None
        if 'complain' not in non_node_config:
            non_node_config['complain'] = lambda x: None
        if 'enforced_properties' not in non_node_config:
            non_node_config['enforced_properties'] = {}
        if 'decide_snapshots_to_keep' not in non_node_config:
            non_node_config[
                'decide_snapshots_to_keep'] = lambda dummy_ffs_name, snapshots: snapshots
        if 'decide_snapshots_to_send' not in non_node_config:
            non_node_config[
                'decide_snapshots_to_send'] = lambda dummy_ffs_name, snapshots: snapshots
        if 'ssh_cmd' not in non_node_config:
            non_node_config['ssh_cmd'] = sh_cmd = ['ssh', '-o', 'StrictHostKeyChecking=no', '-i',
                                                   '/home/ffs/.ssh/id_rsa']  # default ssh command, #-i is necessary for 'sudo rsync'
        if sender is None:
            sender = ssh_message_que.OutgoingMessages(
                logger, self, non_node_config['ssh_cmd'])
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

    def write_authorized_keys(self):
        if not os.path.exists(os.path.join('node', 'home', '.ssh')):
            os.makedirs(os.path.join('node', 'home', '.ssh'))
        fn = os.path.join('node', 'home', '.ssh', 'authorized_keys')
        if os.path.exists(fn):
            os.unlink(fn)
        with open(fn, 'wb') as op:
            for node in self.node_config:
                pub_key = self.node_config[node]['public_key']
                op.write(b'command="/home/ffs/ssh.py",no-port-forwarding,no-X11-forwarding,no-agent-forwarding %s\n' %
                         pub_key)

    def build_deployment_zip(self):
        shutil.make_archive(self.deployment_zip_filename[
                            :-4], 'zip', os.path.join('node', 'home'))

    def send(self, node_name, message):
        """allow sending by name"""
        self.sender.send_message(
            node_name, self.node_config[node_name], message)

    def fault(self, message, trigger=None, exception=ManualInterventionNeeded):
        self.logger.error("Faulted engine with message: %s", message)
        if trigger:
            self.logger.error("Fault triggeredb by incoming message", trigger)
        self.faulted = message
        self.trigger_message = trigger
        self.sender.kill_unsent_messages()
        raise exception(message)

    def incoming_node(self, msg):
        if 'msg' not in msg:
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
            self.send_snapshot_done(msg)
        elif msg['msg'] == 'remove_snapshot_done':
            self.node_remove_snapshot_done(msg)
        elif msg['msg'] == 'remove_done':
            self.node_remove_done(msg)
        elif msg['msg'] == 'zpool_status':
            self.node_zpool_status(msg)
        else:
            self.fault("Invalid msg from node", msg)

    def incoming_client(self, msg):
        command = msg['msg']
        if command == 'startup':
            return self.client_startup()
        elif command == 'new':
            return self.client_new(msg)
        elif command == 'remove_target':
            return self.client_remove_target(msg)
        elif command == 'add_target':
            return self.client_add_target(msg)
        elif command == 'capture':
            return self.client_capture(msg)
        elif command == 'chown_and_chmod':
            return self.client_chown_and_chmod(msg)
        elif command == 'move_main':
            return self.client_move_main(msg)
        elif command == 'deploy':
            return self.client_deploy()
        else:
            raise ValueError("invalid message from client, ignoring")

    def client_deploy(self):
        with open(self.deployment_zip_filename, 'rb') as op:
            d = op.read()
            return {'node.zip': base64.b64encode(d).decode('utf-8')}

    def client_startup(self):
        """Request a list of ffs from each and every of our nodes"""
        msg = {'msg': 'deploy'}
        with open(self.deployment_zip_filename, 'rb') as op:
            d = op.read()
            msg['node.zip'] = base64.b64encode(d).decode('utf-8')
        for node_name, node_info in sorted(self.node_config.items()):
            self.sender.send_message(node_name, node_info, msg)

    def check_ffs_name(self, path):
        if not re.match("^[a-z0-9][A-Za-z0-9/_-]*$", path):
            raise ValueError("Invalid path: %s" % repr(path))

    @needs_startup
    def client_new(self, msg):
        if 'ffs' not in msg:
            raise CodingError("No ffs specified")
        ffs = msg['ffs']
        self.check_ffs_name(ffs)
        if 'targets' not in msg:
            raise CodingError("No targets specified")
        if not msg['targets']:
            raise CodingError("Targets empty")
        for x in msg['targets']:
            if x not in self.node_config:
                raise ValueError("Not a valid target: %s" % x)
        if ffs in self.model:
            raise ValueError("Already present, can't create as new")
        main = msg['targets'][0]
        for node in self.node_config:
            if node in msg['targets']:
                if main == node:
                    props = {'ffs:main': 'on',
                             'readonly': 'off'
                             }
                else:
                    props = {'ffs:main': 'off',
                             'readonly': 'on'
                             }
                self.send(node,
                          {'msg': 'new',
                           'ffs': msg['ffs'],
                           'properties': props
                           }
                          )
                if ffs not in self.model:
                    self.model[ffs] = {'_new': True, '_main': main}
                self.model[ffs][node] = {}

    @needs_startup
    def client_remove_target(self, msg):
        if 'ffs' not in msg:
            raise CodingError("No ffs specified")
        if 'target' not in msg:
            raise CodingError("target not specified")
        ffs = msg['ffs']
        target = msg['target']
        if ffs not in self.model:
            raise ValueError("FFs unknown")
        if target not in self.model[ffs]:
            raise ValueError("Target not in list of targets")
        if target == self.model[ffs]['_main']:
            raise ValueError("Target is main - not removing")
        self.send(target,
                  {'msg': 'remove',
                   'ffs': ffs}
                  )
        self.model[ffs][target]['removing'] = True

    @needs_startup
    def client_add_target(self, msg):
        if 'ffs' not in msg:
            raise CodingError("No ffs specified")
        if 'target' not in msg:
            raise CodingError("target not specified")
        ffs = msg['ffs']
        target = msg['target']
        if ffs not in self.model:
            raise ValueError("FFs unknown")
        if target in self.model[ffs]:
            raise ValueError("Target already in list")
        self.send(target,
                  {'msg': 'new',
                   'ffs': ffs,
                   'properties': {'ffs:main': 'off', 'readonly': 'on'}
                   }
                  )
        self.model[ffs][target] = {}

    @needs_startup
    def client_capture(self, msg):
        if 'ffs' not in msg:
            raise ValueError("No ffs specified")
        ffs = msg['ffs']
        if ffs not in self.model:
            raise ValueError("Nonexistant ffs specified")
        if self.is_ffs_moving(ffs):
            raise MoveInProgress(
                "This ffs is moving to a different main - you should not have been able to change the files anyhow")
        postfix = msg.get('postfix', '')
        self.do_capture(ffs, msg.get('chown_and_chmod', False), postfix)

    def do_capture(self, ffs, chown_and_chmod, postfix=''):
        snapshot = self._name_snapshot(ffs, postfix)
        out_msg = {
            'msg': 'capture',
            'ffs': ffs,
            'snapshot': snapshot,
        }
        if chown_and_chmod:
            out_msg['chown_and_chmod'] = True
            out_msg['user'] = self.non_node_config['chown_user']
            out_msg['rights'] = self.non_node_config['chmod_rights']
        self.send(
            self.model[ffs]['_main'],
            out_msg
        )
        # so we don't reuse the name. ever
        node_info = self.model[ffs][self.model[ffs]['_main']]
        if not 'upcoming_snapshots' in node_info:
            node_info['upcoming_snapshots'] = []
        if snapshot in node_info['upcoming_snapshots']:
            self.fault(
                "Adding a snapshot to upcoming snapshot that was already present", exception=CodingError)
        node_info['upcoming_snapshots'].append(snapshot)
        return snapshot

    @needs_startup
    def client_chown_and_chmod(self, msg):
        if 'ffs' not in msg:
            raise ValueError("No ffs specified")
        ffs = msg['ffs']
        if ffs not in self.model:
            raise ValueError("Nonexistant ffs specified")
        if self.is_ffs_moving(ffs):
            raise MoveInProgress(
                "This ffs is moving to a different main - you should not have been able to change the files anyhow")
        self.send(
            self.model[ffs]['_main'],
            {
                'msg': 'chown_and_chmod',
                'ffs': ffs,
                'user': self.non_node_config['chown_user'],
                'rights': self.non_node_config['chmod_rights']
            }


        )

    @needs_startup
    def client_move_main(self, msg):
        if not 'ffs' in msg:
            raise ValueError("no ffs specified'")
        ffs = msg['ffs']
        if ffs not in self.model:
            raise ValueError("Nonexistant ffs specified")
        if not 'target' in msg:
            raise ValueError("Missing target (=new_main) in msg")
        target = msg['target']
        if target.startswith("_"):
            raise ValueError("Invalid target.")
        if target not in self.model[ffs]:
            raise ValueError("Target does not have this ffs")
        current_main = self.model[ffs]['_main']
        if target == current_main:
            raise ValueError("Target is already main")
        self.model[ffs]['_moving'] = target
        # self.model[ffs][current_main]['properties']['readonly'] = 'on'
        self.send(current_main, {
            'msg': 'set_properties',
            'ffs': ffs,
            'properties': {
                'readonly': 'on',
                'ffs:moving_to': target,
            }
        })

    def is_ffs_moving(self, ffs):
        if '_moving' in self.model[ffs]:
            return True
        main = self.model[ffs]['_main']
        moving_to = self.model[ffs][main][
            'properties'].get('ffs:moving_to', '-')
        if moving_to != '-':
            self.model[ffs]['_moving'] = moving_to
            return True
        return False

    def _name_snapshot(self, ffs, postfix=''):
        import time
        t = time.localtime()
        t = [t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec]
        t = [str(x) for x in t]
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

    def node_deploy_done(self, sender):
        msg = {'msg': 'list_ffs'}
        self.sender.send_message(sender, self.node_config[sender], msg)

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
            self.non_node_config['inform']("Startup completed, sending syncs.")

    def build_model(self):
        self.logger.info("All list_ffs returned")
        self.model = {}
        for node, ffs_list in self.node_ffs_infos.items():
            for ffs, ffs_info in ffs_list.items():
                if ffs not in self.model:
                    self.model[ffs] = {}
                ffs_info['removing'] = False
                self.model[ffs][node] = ffs_info
        self._parse_main_and_readonly()
        self._enforce_properties()
        self._prune_snapshots()
        self._send_missing_snapshots()

    def _parse_main_and_readonly(self):
        """Go through the ffs:main and readonly properties.
        Make sure there is exactly one main, it is non-readonly,
        and everything else is ffs:main=off and readonly
        """
        for ffs, node_ffs_info in self.model.items():
            main = None
            non_ro_count = 0
            ro_count = 0
            last_non_ro = None
            any_moving_to = None
            any_moving_from = None
            for node, node_info in node_ffs_info.items():
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
                        main = None  # stays None.
                    else:
                        self.fault(
                            "No main, muliple non-readonly for %s" % ffs)
            self.model[ffs]['_main'] = main
            if not any_moving_to:
                # make sure the right readonly/main properties are set.
                for node in self.node_config:  # always in the same order
                    if node in node_ffs_info:
                        node_info = node_ffs_info[node]
                        props = node_info['properties']
                        if node == main:
                            if props.get('readonly', False) != 'off':
                                self.send(node, {'msg': 'set_properties', 'ffs': ffs,
                                                 'properties': {'readonly': 'off'}})

                            if props.get('ffs:main', 'off') != 'on':
                                self.send(
                                    node, {'msg': 'set_properties', 'ffs': ffs,
                                           'properties': {'ffs:main': 'on'}})
                        else:
                            if props.get('readonly', 'off') != 'on':
                                self.send(node, {'msg': 'set_properties', 'ffs': ffs,
                                                 'properties': {'readonly': 'on'}})
                            if 'ffs:main' not in props:
                                self.send(
                                    node, {'msg': 'set_properties', 'ffs': ffs,
                                           'properties': {'ffs:main': 'off'}})
            else:  # caught in a move.
                # step 0 -
                move_target = any_moving_to
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
        for ffs, node_ffs_info in self.model.items():
            if node_ffs_info['_main'] is None:
                raise CodingError("Main remained None")

    def _enforce_properties(self):
        for ffs in self.model:
            for node, ffs_node_info in sorted(self.model[ffs].items()):
                if node.startswith('_'):
                    continue
                to_set = {}
                for k, v in self.non_node_config['enforced_properties'].items():
                    v = str(v)
                    if ffs_node_info['properties'].get(k, False) != v:
                        to_set[k] = v
                if to_set:
                    self.send(
                        node, {'msg': 'set_properties',
                               'ffs': ffs, 'properties': to_set}
                    )

    def _prune_snapshots(self):
        for ffs, node_fss_info in self.model.items():
            main_node = node_fss_info['_main']
            main_snapshots = node_fss_info[main_node]['snapshots']
            keep_snapshots = self.non_node_config[
                'decide_snapshots_to_keep'](ffs, main_snapshots)
            self.logger.info("keeping for %s %s" % (ffs, keep_snapshots))
            remove_from_main = [
                x for x in main_snapshots if x not in keep_snapshots]
            for snapshot in remove_from_main:
                self.send(
                    main_node, {'msg': 'remove_snapshot', 'ffs': ffs, 'snapshot': snapshot})
                # and forget they existed for now.
                node_fss_info[main_node]['snapshots'].remove(snapshot)
            for node in sorted(node_fss_info):
                if node != main_node and not node.startswith('_'):
                    target_snapshots = node_fss_info[node]['snapshots']
                    too_many = [
                        x for x in target_snapshots if x not in keep_snapshots]
                    for snapshot in too_many:
                        self.send(node, {'msg': 'remove_snapshot',
                                         'ffs': ffs, 'snapshot': snapshot})
                        # and forget they existed for now.
                        node_fss_info[node]['snapshots'].remove(snapshot)

    def _send_missing_snapshots(self):
        """Once we have prased the ffs_lists into our model (see _parse_main_and_readonly),
        we send out pull requests for the missing snapshots
        and prunes for those that are too many"""
        for ffs, node_fss_info in self.model.items():
            if self.is_ffs_moving(ffs):
                continue
            main = node_fss_info['_main']
            main_snapshots = node_fss_info[main]['snapshots']
            snapshots_to_send = set(self.non_node_config[
                'decide_snapshots_to_send'](ffs, main_snapshots))
            ordered_to_send = [x for x in main_snapshots if x in snapshots_to_send]
            if ordered_to_send:
                for node, node_info in node_fss_info.items():
                    if node.startswith('_'):
                        continue
                    missing = []
                    for sn in reversed(ordered_to_send):
                        if sn not in node_info['snapshots']:
                            missing.append(sn)
                        else:
                             break
                    missing = reversed(missing)
                    for sn in missing:
                        self._send_snapshot(main, node, ffs, sn)

    def _send_snapshot(self, sending_node, receiving_node, ffs, snapshot_name):
        self.send(sending_node, {
            'msg': 'send_snapshot',
            'ffs': ffs,
            'snapshot': snapshot_name,
            'target_host': receiving_node,
            'target_user': 'ffs',
            'target_ssh_cmd': self.non_node_config['ssh_cmd'],
            'target_path': '/%%ffs%%/' + ffs,
        }
        )

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
            moving_to = props['ffs:moving_to']
            if moving_to != '-':
                if not self.is_ffs_moving(ffs):
                    self.fault(
                        "Received unexpected set_propertes_done for ffs:moving_to", msg, InconsistencyError)
                self.model[ffs]['_move_snapshot'] = self.do_capture(ffs, False)

            else:
                del self.model[ffs]['_moving']
                if self.is_ffs_moving(ffs):
                    self.fault(
                        "Still moving after set_properties moving_to = -, Something is fishy ", msg, CodingError)
        elif (  # happens after successful capture & replication.
                self.is_ffs_moving(ffs) and
                props.get('ffs:main', False) == 'off'):
            if node != self.model[ffs]['_main']:
                self.fault(
                    "Received unexpected set_propertes_done for ffs:main=off for non main", msg, InconsistencyError)
                raise InconsistencyError(self.fault)
            self.send(self.model[ffs]['_moving'], {
                'msg': 'set_properties',
                'ffs': ffs,
                'properties': {'readonly': 'off', 'ffs:main': 'on'},
            })
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
        if self.model[ffs][node] != {}:
            self.fault(
                "node_new_done from an node/ffs where we already have data", msg, CodingError)
        self.model[ffs][node] = {
            'snapshots': [],
            'properties': msg['properties']
        }
        main = self.model[ffs]['_main']
        # This happens if we were actually a add_new_target
        if node != main and self.model[ffs][main]['snapshots']:
            for sn in self.model[ffs][main]['snapshots']:
                self._send_snapshot(main, node, ffs, sn)
            

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
        if not 'snapshot' in msg:
            self.fault("No snapshot in msg", msg, CodingError)
        snapshot = msg['snapshot']
        if snapshot in self.model[ffs][node]['snapshots']:
            self.fault("Snapshot was already in model", msg, CodingError)
        if snapshot in self.model[ffs][node].get('upcoming_snapshots', []):
            self.model[ffs][node]['upcoming_snapshots'].remove(snapshot)

        main = self.model[ffs]['_main']
        for node in self.node_config:
            if node != main and node in self.model[ffs]:
                postfix = self.model[ffs][node][
                    'properties'].get('ffs:postfix_only', True)
                if postfix is True or snapshot.endswith('-' + postfix):
                    self._send_snapshot(main, node, ffs, snapshot)
                    

    def send_snapshot_done(self, msg):
        main = msg['from']
        if 'ffs' not in msg:
            self.fault("missing ffs parameter", msg, CodingError)
        ffs = msg['ffs']
        if ffs not in self.model:
            self.fault("send_snapshot_done from ffs not in model.",
                       msg, InconsistencyError)
        node = msg['target_host']
        if main == node:
            self.fault("Send done from main to main?!",
                       msg, InconsistencyError)
        if 'snapshot' not in msg:
            self.fault("No snapshot in msg", msg, CodingError)
        snapshot = msg['snapshot']
        if snapshot in self.model[ffs][node]['snapshots']:
            self.fault("Snapshot was already in model", msg, CodingError)
        self.model[ffs][node]['snapshots'].append(snapshot)

        if (self.is_ffs_moving(ffs) and
            node == self.model[ffs]['_moving'] and
            msg['snapshot'] == self.model[ffs]['_move_snapshot']
            ):
            self.send(self.model[ffs]['_main'], {
                'msg': 'set_properties',
                'ffs': ffs,
                'properties': {'ffs:main': 'off'}
            })

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

    def node_zpool_status(self, msg):
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
        for node in self.node_config:
            self.send(node, {'msg': 'zpool_status'})

    def shutdown(self):
        self.sender.shutdown()
