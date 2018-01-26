import pprint
import re


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

    def __init__(self, config, send_function):
        """Config is a dictionary node_name -> node info
        send_function handles sending  messages to nodes
        and get's the node_info and a message passed"""
        self.send_function = send_function
        for node in config:
            if node.startswith('_'):
                raise ValueError("Node can not start with _")
        self.config = config
        self.node_ffs_infos = {}
        self.model = {}
        self.startup_done = False
        self.faulted = False
        self.trigger_message = None
        self.zpool_stati = {}
        self.error_callback = lambda x: False

    def send(self, node_name, message):
        """allow sending by name"""
        self.send_function(self.config[node_name], message)

    def incoming_node(self, msg):
        if 'msg' not in msg:
            raise ValueError("No message in msg")
        if 'from' not in msg:
            self.trigger_message = msg
            self.faulted = "No from in message - should not happen"
            raise ManualInterventionNeeded(self.faulted)
        if msg['from'] not in self.config:
            self.faulted = "Invalid sender"
            self.trigger_message = msg
            raise ManualInterventionNeeded(self.faulted)
        elif msg['msg'] == 'ffs_list':
            self.node_ffs_list(msg['ffs'], msg['from'])
        elif msg['msg'] == 'set_properties_done':
            self.node_set_properties_done(msg)
        elif msg['msg'] == 'new_done':
            self.node_new_done(msg)
        elif msg['msg'] == 'capture_done':
            self.node_capture_done(msg)
        elif msg['msg'] == 'pull_done':
            self.node_pull_done(msg)
        elif msg['msg'] == 'remove_done':
            self.node_remove_done(msg)
        elif msg['msg'] == 'zpool_status':
            self.node_zpool_status(msg)
        else:
            self.faulted = "Invalid msg from node"
            self.trigger_message = msg
            raise CodingError(self.faulted)

    def incoming_client(self, msg):
        command = msg['msg']
        if command == 'startup':
            self.client_startup()
        elif command == 'new':
            self.client_new(msg)
        elif command == 'remove_target':
            self.client_remove_target(msg)
        elif command == 'add_target':
            self.client_add_target(msg)
        elif command == 'capture':
            self.client_capture(msg)
        elif command == 'chown_and_chmod':
            self.client_chown_and_chmod(msg)
        elif command == 'move_main':
            self.client_move_main(msg)
        else:
            raise ValueError("invalid message from client, ignoring")

    def client_startup(self):
        """Request a list of ffs from each and every of our nodes"""
        for node_info in self.config.values():
            msg = {'msg': 'list_ffs'}
            self.send_function(node_info, msg)

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
            if not x in self.config:
                raise ValueError("Not a valid target: %s" % x)
        if ffs in self.model:
            raise ValueError("Already present, can't create as new")
        main = msg['targets'][0]
        for node in self.config:
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
        self.do_capture(ffs, msg.get('chown_and_chmod', False))

    def do_capture(self, ffs, chown_and_chmod, postfix = ''):
        snapshot = self._name_snapshot(ffs, postfix)
        out_msg = {
            'msg': 'capture',
            'ffs': ffs,
            'snapshot': snapshot,
        }
        if chown_and_chmod:
            out_msg['chown_and_chmod'] = True
        self.send(
            self.model[ffs]['_main'],
            out_msg
        )
        # so we don't reuse the name. ever
        node_info = self.model[ffs][self.model[ffs]['_main']]
        if not 'upcoming_snapshots' in node_info:
            node_info['upcoming_snapshots'] = []
        if snapshot in node_info['upcoming_snapshots']:
            self.faulted = "Adding a snapshot to upcoming snapshot that was already present"
            raise CodingError(self.faulted)
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
            (res in self.model[ffs][self.model[ffs]['_main']].get('upcoming_snapshots', []))
        ):
            res = 'ffs-' + '-'.join(t)
            res += '-' + no
            if postfix:
                res += '-' + postfix
            no = chr(ord(no) + 1)
 
        return res


    def node_ffs_list(self, ffs_list, sender):
        self.node_ffs_infos[sender] = ffs_list
        if len(self.node_ffs_infos) == len(self.config):
            self.build_model()
            self.startup_done = True

    def build_model(self):
        self.model = {}
        for node, ffs_list in self.node_ffs_infos.items():
            for ffs, ffs_info in ffs_list.items():
                if ffs not in self.model:
                    self.model[ffs] = {}
                ffs_info['removing'] = False
                self.model[ffs][node] = ffs_info
        self._parse_main_and_readonly()

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
                        self.faulted = "Multiple mains for %s" % ffs
                        raise ManualInterventionNeeded(self.faulted)
                if props.get('ffs:moving_to', '-') != '-':
                    if any_moving_to is not None:
                        self.faulted = "Multiple moving_to for %s" % ffs
                        raise ManualInterventionNeeded(self.faulted)
                    any_moving_to = props['ffs:moving_to']
                    any_moving_from = node
            if main is None:
                if non_ro_count == 1:
                    # treat the only non-ro as the man
                    main = last_non_ro
                else:
                    if any_moving_to:
                        main = None # stays None.
                    else:
                        self.faulted = "No main, muliple non-readonly for %s" % ffs
                        raise ManualInterventionNeeded(self.faulted)
            self.model[ffs]['_main'] = main
            if not any_moving_to:
                #make sure the right readonly/main properties are set.
                for node in self.config:  # always in the same order
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
            else: #caught in a move.
                #step 0 - 
                move_target = any_moving_to
                self.model[ffs]['_moving'] = move_target
                if main is not None: 
                    if main != any_moving_to:
                        # we were before step 3, remove main, so we restart with a capture
                        # and ignore if we had already captured and replicated.
                        self.do_capture(ffs, False)
                    else: #main had already been moved
                        #all that remains is to remove the moving marker
                        self.send(any_moving_from, {
                            'msg': 'set_properties',
                            'ffs': ffs,
                            'properties': {'ffs:moving_to': '-'}
                        })

                else: # we had successfully captured and replicated and the main has been removed
                    # so we continue by setting main=on and readonly=off on the moving target...
                    self.send(any_moving_to, {
                        'msg': 'set_properties',
                        'ffs': ffs,
                        'properties': {'ffs:main': 'on', 'readonly': 'off'}
                    })
                node_ffs_info['_main'] = main # we can deal with main being None until the ffs:moving_to = - job  is done..
        self._send_pulls_and_removals()

    def _send_pulls_and_removals(self):
        """Once we have prased the ffs_lists into our model (see _parse_main_and_readonly),
        we send out pull requests for the missing snapshots
        and prunes for those that are too many"""
        for ffs, node_fss_info in self.model.items():
            if self.is_ffs_moving(ffs):
                continue
            main = node_fss_info['_main']
            main_snapshots = node_fss_info[main]['snapshots']
            if main_snapshots:
                latest_snapshot = main_snapshots[-1]
                for node, node_info in node_fss_info.items():
                    if not node.startswith('_') and node != main:
                        if latest_snapshot not in node_info['snapshots']:
                            self.send(node, {
                                'msg': 'pull_snapshot',
                                'pull_from': main,
                                'ffs': ffs,
                                'snapshot': latest_snapshot
                            }
                            )
                        for snapshot in reversed(node_info['snapshots']):
                            if not snapshot in main_snapshots:
                                self.send(node, {
                                    'msg': 'remove_snapshot',
                                    'ffs': ffs,
                                    'snapshot': snapshot,
                                })

    def node_set_properties_done(self, msg):
        node = msg['from']
        if msg['ffs'] not in self.model:
            self.faulted = ("set_properties_done from ffs not in model.")
            self.trigger_message = msg
            raise InconsistencyError(self.faulted)
        ffs = msg['ffs']
        if node not in self.model[ffs]:
            self.faulted = ("set_properties_done from ffs not on that node")
            self.trigger_message = msg
            raise InconsistencyError(self.faulted)
        if 'properties' not in msg:
            self.faulted = "No properties in set_properties_done msg"
            self.trigger_message = msg
            raise CodingError(self.faulted)
        props = msg['properties']
        self.model[ffs][node]['properties'].update(props)
        if 'ffs:moving_to' in props:  # first step in moving to a new main
            moving_to = props['ffs:moving_to']
            if moving_to != '-':
                if not self.is_ffs_moving(ffs):
                    self.faulted = "Received unexpected set_propertes_done for ffs:moving_to"
                    self.trigger_message = msg
                    raise InconsistencyError(self.fault)
                self.model[ffs]['_move_snapshot'] = self.do_capture(ffs, False)

            else:
                del self.model[ffs]['_moving']
                if self.is_ffs_moving(ffs):
                    self.faulted = "Still moving after set_properties moving_to = -, Something is fishy "
                    self.trigger_message = msg
                    raise CodingError(self.fault)
        elif (  # happens after successful capture & replication.
                self.is_ffs_moving(ffs) and
                props.get('ffs:main', False) == 'off'):
            if node != self.model[ffs]['_main']:
                self.faulted = "Received unexpected set_propertes_done for ffs:main=off for non mainjjj"
                self.trigger_message = msg
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
            self.faulted = ("node_new_done from ffs not in model.")
            self.trigger_message = msg
            raise InconsistencyError(self.faulted)
        ffs = msg['ffs']
        if node not in self.model[ffs]:
            self.faulted = ("node_new_done from ffs not on that node")
            pprint.pprint(msg)
            pprint.pprint(self.model)
            self.trigger_message = msg
            raise CodingError(self.faulted)
        if self.model[ffs][node] != {}:
            self.faulted = (
                "node_new_done from an node/ffs where we already have data")
            self.trigger_message = msg
            raise CodingError(self.faulted)
        self.model[ffs][node] = {
            'snapshots': [],
            'properties': msg['properties']
        }
        main = self.model[ffs]['_main']
        if node != main and self.model[ffs][main]['snapshots']:
            self.send(node,
                      {'msg': 'pull_snapshot',
                       'pull_from': main,
                       'ffs': ffs,
                       'snapshot': self.model[ffs][main]['snapshots'][-1]
                       }
                      )

    def node_capture_done(self, msg):
        node = msg['from']
        if 'ffs' not in msg:
            self.faulted = "missing ffs parameter"
            self.trigger_message = msg
            raise CodingError()
        ffs = msg['ffs']
        if ffs not in self.model:
            self.faulted = "capture_done from ffs not in model."
            self.trigger_message = msg
            raise InconsistencyError(self.faulted)
        main = self.model[ffs]['_main']
        if main != node:
            self.trigger_message = msg
            self.faulted = "Capture message received from non main node"
            raise InconsistencyError(self.faulted)
        if not 'snapshot' in msg:
            self.faulted = "No snapshot in msg"
            self.trigger_message = msg
            raise CodingError(self.faulted)
        snapshot = msg['snapshot']
        if snapshot in self.model[ffs][node]['snapshots']:
            self.faulted = "Snapshot was already in model"
            self.trigger_message = msg
            raise CodingError(self.faulted)
        if snapshot in self.model[ffs][node].get('upcoming_snapshots', []):
            self.model[ffs][node]['upcoming_snapshots'].remove(snapshot)

        main = self.model[ffs]['_main']
        for node in self.config:
            if node != main and node in self.model[ffs]:
                self.send(node,
                          {
                              'msg': 'pull_snapshot',
                              'ffs': ffs,
                              'snapshot': snapshot,
                              'pull_from': main,
                          })

    def node_pull_done(self, msg):
        node = msg['from']
        if 'ffs' not in msg:
            self.faulted = "missing ffs parameter"
            self.trigger_message = msg
            raise CodingError()
        ffs = msg['ffs']
        if ffs not in self.model:
            self.faulted = "capture_done from ffs not in model."
            self.trigger_message = msg
            raise InconsistencyError(self.faulted)
        main = self.model[ffs]['_main']
        if main == node:
            self.trigger_message = msg
            self.faulted = "Pull received from main node?!"
            raise InconsistencyError(self.faulted)
        if 'snapshot' not in msg:
            self.faulted = "No snapshot in msg"
            self.trigger_message = msg
            raise CodingError(self.faulted)
        snapshot = msg['snapshot']
        if snapshot in self.model[ffs][node]['snapshots']:
            self.faulted = "Snapshot was already in model"
            self.trigger_message = msg
            raise CodingError(self.faulted)
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
            self.faulted = "missing ffs parameter"
            self.trigger_message = msg
            raise CodingError()
        ffs = msg['ffs']
        if ffs not in self.model:
            self.faulted = "remove_done from ffs not in model."
            self.trigger_message = msg
            raise InconsistencyError(self.faulted)
        if node not in self.model[ffs]:
            raise InconsistencyError(
                "remove_done from node not in ffs for this model")
        if self.model[ffs]['_main'] == node:
            self.faulted = "remove_done from main!"
            self.trigger_message = msg
            raise InconsistencyError(self.faulted)
        del self.model[ffs][node]
    
    def node_zpool_status(self, msg):
        node = msg['from']
        status = {}
        for k in ['ONLINE', 'DEGRADED', 'UNAVAIL']:
            status[k] = msg['status'].count(k)
        if node in self.zpool_stati:
            old_status = self.zpool_stati[node]
            if old_status != status:
                self.error_callback("Zpool status changed: %s - %s" % (node, status))
        else:
            if status['DEGRADED'] or status['UNAVAIL']:
                self.error_callback("Zpool status: %s - %s" % (node, status))
        self.zpool_stati[node] = status 


    @needs_startup
    def prune_snapshots(self):
        """Pruning means to remove snapshots on target
        that do not / no longer exist on main"""
        for ffs in self.model:
            main = self.model[ffs]['_main']
            keep = set(self.model[ffs][main]['snapshots'])
            for target in self.model[ffs]:
                if target != main and not target.startswith('_'):
                    to_remove = set(self.model[ffs][target][
                                    'snapshots']).difference(keep)
                    for s in to_remove:
                        self.send(target, {
                            'msg': 'remove_snapshot',
                            'ffs': ffs,
                            'snapshot': s
                        })

    def  do_zpool_status_check(self):
        for node in self.config:
            self.send(node, {'msg': 'zpool_status'})
        
   