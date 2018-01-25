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
                raise ValueError("Nod can not start with _")
        self.config = config
        self.node_ffs_infos = {}
        self.model = {}
        self.startup_done = False
        self.faulted = False
        self.trigger_message = None

    def send(self, node_name, message):
        """allow sending by name"""
        self.send_function(self.config[node_name], message)

    def incoming_node(self, msg):
        if not 'msg' in msg:
            raise ValueError("No message in msg")
        if not 'from' in msg:
            self.trigger_message = msg
            self.faulted = "No from in message - should not happen"
            raise ManualInterventionNeeded(self.faulted)
        if msg['from'] not in self.config:
            self.faulted = "Invalid sender"
            self.trigger_message = msg
            raise ManualInterventionNeeded(self.faulted)
        elif msg['msg'] == 'ffs_list':
            self.node_ffs_list(msg['ffs'], msg['from'])
        elif msg['msg'] == 'set_property_done':
            self.node_set_property(msg)
        elif msg['msg'] == 'new_done':
            self.node_new_done(msg)
        elif msg['msg'] == 'capture_done':
            self.node_capture_done(msg)
        elif msg['msg'] == 'pull_done':
            self.node_pull_done(msg)
        elif msg['msg'] == 'remove_done':
            self.node_remove_done(msg)
        else:
            self.faulted = "Invalid msg from node"
            self.trigger_message = msg
            raise CodingError(self.faulted)

    def incoming_client(self, msg):
        if msg['msg'] == 'startup':
            self.client_startup()
        elif msg['msg'] == 'new':
            self.client_new(msg)
        elif msg['msg'] == 'remove_target':
            self.client_remove_target(msg)
        elif msg['msg'] == 'add_target':
            self.client_add_target(msg)
        elif msg['msg'] == 'capture':
            self.client_capture(msg)
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
        if not msg['ffs'] in self.model:
            raise ValueError("Nonexistant ffs specified")
        self.send(
            self.model[msg['ffs']]['_main'],
            {
                'msg': 'capture',
                'ffs': msg['ffs']
            }
        )

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
            if main is None:
                if non_ro_count == 1:
                    # treat the only non-ro as the man
                    main = last_non_ro
                else:
                    self.faulted = "No main, muliple non-readonly for %s" % ffs
                    raise ManualInterventionNeeded(self.faulted)
            for node in self.config:  # always in the same order
                if node in node_ffs_info:
                    node_info = node_ffs_info[node]
                    props = node_info['properties']
                    if node == main:
                        if props.get('readonly', False) != 'off':
                            self.send(node, {'msg': 'set_property', 'ffs': ffs,
                                             'property': 'readonly', 'value': 'off'})

                        if props.get('ffs:main', 'off') != 'on':
                            self.send(
                                node, {'msg': 'set_property', 'ffs': ffs, 'property': 'ffs:main', 'value': 'on'})
                    else:
                        if props.get('readonly', 'off') != 'on':
                            self.send(node, {'msg': 'set_property', 'ffs': ffs,
                                             'property': 'readonly', 'value': 'on'})
                        if 'ffs:main' not in props:
                            self.send(
                                node, {'msg': 'set_property', 'ffs': ffs, 'property': 'ffs:main', 'value': 'off'})

                node_ffs_info['_main'] = main

    def node_set_property(self, msg):
        node = msg['from']
        if not msg['ffs'] in self.model:
            self.faulted = ("set_property_done from ffs not in model.")
            self.trigger_message = msg
            raise InconsistencyError(self.faulted)
        if not node in self.model[msg['ffs']]:
            self.faulted = ("set_property_done from ffs not on that node")
            self.trigger_message = msg
            raise InconsistencyError(self.faulted)
        self.model[msg['ffs']][node]['properties'][
            msg['property']] = msg['value']

    def node_new_done(self, msg):
        node = msg['from']
        if not msg['ffs'] in self.model:
            self.faulted = ("node_new_done from ffs not in model.")
            self.trigger_message = msg
            raise InconsistencyError(self.faulted)
        ffs = msg['ffs']
        if not node in self.model[ffs]:
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
