#!/usr/bin/python3
import logging
import time

class DefaultConfig:

    def get_nodes(self):
        if hasattr(self, '_nodes'):
            return self._nodes
        raise NotImplementedError("Overwrite get_nodes in your config")

    def decide_targets(self, dummy_ffs):
        return ['mm']

    def find_node(self, incoming_name):
        for node, node_info in self.get_nodes().items():
            if node == incoming_name:
                return node
            if node_info['hostname'] == incoming_name:
                return node

    def complain(self, message):
        """This gets called on (catastrophic) failures. Contact your admin stuff basically"""
        pass

    def inform(self, message):
        """Keep your users informed."""
        pass

    def get_ssh_cmd(self):
       return ['ssh', '-p', '223', '-o', 'StrictHostKeyChecking=no', '-i', '/home/ffs/.ssh/id_rsa']  # default ssh command, #-i is necessary for 'sudo rsync'
    
    def get_ssh_concurrent_connection_limit(self):
        return 5 

    def get_zpool_frequency_check(self):
        #in seconds
        return  60 * 15  # in seconds

    def get_zmq_port(self):
        return 47777

    def get_chown_user(self, dummy_ffs):
        return 'finkernagel'
    
    def get_chmod_rights(self, dummy_ffs):
        return 'uog+rwX'
    
    def accepted_ffs_name(self, ffs):
        """False will lead to an execption when calling client_new / ffs.py new.
        Names are not filtered otherwise!
        """
        return True

    def decide_snapshots_to_send(self, dummy_ffs_name, snapshots):
        """What snapshots for this ffs should be transmitted?"""
        return set([x for x in snapshots])

    def decide_snapshots_to_keep(self, dummy_ffs_name, snapshots):
        """Decide which snapshots to keep.

        """
        return snapshots

    def get_enforced_properties(self):
        # properties that are always set on our ffs
        return {  # properties that *every* ffs get's assigned!
            'com.sun:auto-snapshot': 'false',
            'atime': 'off'
        }

    def get_default_properties(self):
        return  {
            'compression': 'on',
            #'com.sun:auto-snapshot': 'false',
            #'atime': 'off'
        }
    
    def get_logging(self):
        import logging
        logger = logging.Logger(name='Dummy')
        logger.addHandler(logging.NullHandler())
        return logger
        
def must_return_type(typ):
    def deco(func):
        def wrapper(*args, **kwargs):
            res = func(*args, **kwargs)
            if not isinstance(res, typ):
                raise ValueError("%s must return %s was %s" % (func.__name__, typ, type(res)))
            return res
        return wrapper
    return deco

class CheckedConfig:

    def __init__(self, config):
        self.config = config
        if not hasattr(config, 'get_nodes'):
            raise ValueError("Config object had no get_nodes - not a config object.")
        mine = set(dir(self))
        for k in dir(config):
            if not k.startswith('_') and not k in mine:
                raise ValueError("Missing config wrapper: %s" % k)

    def get_nodes(self):
        nodes = self.config.get_nodes()
        if not isinstance(nodes, dict):
            raise ValueError(
                "Config.nodes must be a dictionary node -> node_def")
        for node, node_info in nodes.items():
            if not 'public_key' in node_info:
                raise ValueError("no public key for node" % node)
            if node.startswith('_'):
                raise ValueError("Node can not start with _: %s" % node)
 

        # stuff that ascertains that the config is as expected - no need to edit
        for n in nodes:
            if nodes[n].get('hostname', None) is None:
                nodes[n]['hostname'] = n
        return nodes.copy()

    def complain(self, message):
        self.config.complain(message)

    def inform(self, message):
        self.config.inform(message)

    def decide_targets(self, ffs_name):
        return [self.config.find_node(x) for x in self.config.decide_targets(ffs_name)]

    @must_return_type(dict)
    def get_enforced_properties(self):
        res = self.config.get_enforced_properties()
        if not isinstance(res, dict):
            raise ValueError("get_default_properties must return a dict")
        for k in res:
           res[k] = str(res[k])
        return res

    @must_return_type(dict)
    def get_default_properties(self):
        res = self.config.get_default_properties()
        if not isinstance(res, dict):
            raise ValueError("get_default_properties must return a dict")
        enforced = self.get_enforced_properties()
        for k in res:
            if k in enforced:
                raise ValueError(
                    "Duplicate property in default and enforced: %s" % k)
            res[k] = str(res[k])
        return res

    @must_return_type(str)
    def get_chown_user(self, ffs):
        return self.config.get_chown_user(ffs)

    @must_return_type(str)
    def get_chmod_rights(self, ffs):
        return self.config.get_chmod_rights(ffs)

    @must_return_type(list)
    def get_ssh_cmd(self):
        return self.config.get_ssh_cmd()
    
    @must_return_type(int)
    def get_ssh_concurrent_connection_limit(self):
        return self.config.get_ssh_concurrent_connection_limit()

    @must_return_type(int)
    def get_zpool_frequency_check(self):
        return self.config.get_zpool_frequency_check()

    @must_return_type(int)
    def get_zmq_port(self):
        return self.config.get_zmq_port()
    
    @must_return_type(logging.Logger)
    def get_logging(self):
        if not hasattr(self, '_logger'):
            self._logger = self.config.get_logging()
        return self._logger
        
    def decide_snapshots_to_keep(self, ffs_name, snapshots):
        return set(self.config.decide_snapshots_to_keep(ffs_name, snapshots))

    def decide_snapshots_to_send(self, ffs_name, snapshots):
        return set(self.config.decide_snapshots_to_send(ffs_name, snapshots))

    @must_return_type(str)
    def find_node(self, incoming_name):
        found = self.config.find_node(incoming_name)
        if not found in self.get_nodes():
            from .engine import InvalidTarget
            raise InvalidTarget("invalid target: %s" % incoming_name)
        return found

    @must_return_type(bool)
    def accepted_ffs_name(self, ffs):
        return self.config.accepted_ffs_name(ffs)

