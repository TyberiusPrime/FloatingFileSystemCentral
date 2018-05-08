#!/usr/bin/python3
import re
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

    def do_deploy(self):
        return True

    def complain(self, message):
        """This gets called on (catastrophic) failures. Contact your admin stuff basically"""
        pass

    def inform(self, message):
        """Keep your users informed."""
        pass

    def get_ssh_cmd(self):
       return ['ssh', '-p', '223', '-o', 'StrictHostKeyChecking=no', '-i', '/home/ffs/.ssh/id_rsa']  # default ssh command, #-i is necessary for 'sudo rsync'
    
    def get_ssh_concurrent_connection_limit(self):
        return 6 

    def get_ssh_rate_limit(self):
        """Time (in decimal seconds) to wait between ssh requests"""
        return 0.

    def get_concurrent_rsync_limit(self):
        """How many rsync send_snapshots may run per sending system at a time?
        """
        return 2

    def get_zpool_frequency_check(self):
        #in seconds
        return  0 # 0 = disabled, seconds otherwise

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
    
    def do_timebased_actions(self):
        return True

    def restart_on_code_changes(self):
        return True

    def exclude_subdirs_callback(self, ffs, source_node, target_node):
        """Execlude some subdirs from being synced to target node
        (via rsync --exclude= option).
        Useful for example to exclude cache dirs from being synced.

        This only works for top-level sub dirs!
        Returning anything with a '/' in it will lead to an exception.
        """
        return []

        

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
                raise ValueError("Missing config wrapper / invalid configuration function (typo?): %s" % k)

    def get_nodes(self):
        nodes = self.config.get_nodes()
        if not isinstance(nodes, dict):
            raise ValueError(
                "Config.nodes must be a dictionary node -> node_def")
        for node, node_info in nodes.items():
            if 'public_key' not in node_info:
                raise ValueError("no public key for node" % node)
            if 'storage_prefix' not in node_info:
                raise ValueError("No storage_prefix (eg. pool/ffs) specified for node: %s" % node)
            storage_prefix = node_info['storage_prefix']
            if not storage_prefix.startswith('/'):
                raise ValueError("Storage prefix must be an absolute path")
            if storage_prefix.endswith('/'):
                raise ValueError("Storage prefix must not end in /")
            if node.startswith('_'):
                raise ValueError("Node can not start with _: %s" % node)
            if isinstance(node_info['public_key'], str):
                node_info['public_key'] = node_info['public_key'].encode('ascii')
            if node_info.get('readonly_node', False):
                ignore_callback = self.config.get_nodes()[node].get('ignore_callback', lambda dummy_ffs, dummy_ffs_props: False)
                def ic(ffs, properties):
                    if ignore_callback(ffs, properties):
                        return True
                    elif properties.get('ffs:main', 'off') == 'off':
                        return True
                    return False
                node_info['ignore_callback'] = ic

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
        rights = self.config.get_chmod_rights(ffs)
        if not re.match("^0[0-7]{3}$", rights) and not re.match("^([ugoa]+[+=-][rwxXst]*,?)+$", rights):
            raise ValueError("Rights were not a valid right string")
        return rights


    @must_return_type(list)
    def get_ssh_cmd(self):
        return self.config.get_ssh_cmd()
    
    @must_return_type(int)
    def get_ssh_concurrent_connection_limit(self):
        res = self.config.get_ssh_concurrent_connection_limit()
        if res < 1:
            raise ValueError("get_ssh_concurrent_connection_limit must be >= 1")
        return res

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
            raise InvalidTarget("invalid target: %s - %s" % (incoming_name, found))
        return found

    @must_return_type(bool)
    def accepted_ffs_name(self, ffs):
        return self.config.accepted_ffs_name(ffs)


    @must_return_type(bool)
    def do_deploy(self):
        return self.config.do_deploy()

    @must_return_type(bool)
    def do_timebased_actions(self):
        return self.config.do_timebased_actions()

    @must_return_type(float)
    def get_ssh_rate_limit(self):
        return self.config.get_ssh_rate_limit()

    @must_return_type(int)
    def get_concurrent_rsync_limit(self):
        res = self.config.get_concurrent_rsync_limit()
        if res < 1:
            raise ValueError('get_concurrent_rsync_limit must be at least 1')
        if res >= self.get_ssh_concurrent_connection_limit():
            raise ValueError('get_concurrent_rsync_limit must be less than get_ssh_concurrent_connection_limit - otherwise sends will block everything else')
        return res

    @must_return_type(bool)
    def restart_on_code_changes(self):
        return self.config.restart_on_code_changes()

    @must_return_type(list)
    def exclude_subdirs_callback(self, ffs, source_node, target_node):
        res = self.config.exclude_subdirs_callback(ffs, source_node, target_node)
        if res is None:
            res = []
        for x in res:
            if '/' in x:
                raise ValueError("exclude_subdirs_callback only works on top level sub-dirs - no / allowed")
        return res

def keep_snapshots_time_policy(snapshots, quarters=4, hours=12, days=7, weeks=10, months=6, years=5, allow_one_snapshot_to_fill_multiple_intervals=False, now=None):
    """Keep one snapshot (starting with ffs) for each of the following intervals
    <quarters>  15 minutes,
    <hours>  hours,
    <days>  days,
    <weeks>  weeks,
    <months>  months, 
    <years>  years
    each.

    if allow_one_snapshot_to_fill_multiple_intervals is set,
        you get the smallest possible number of snapshots 
        to fullfill the intervals
        otherwise each interval get's a unique snapshot (if availabel)

    For testing, now can be set to a unix timestamp.
     
    This is a heler for your own decide_snapshots_to_keep method

    """
    snapshots = [x for x in snapshots if x.startswith('ffs')]
    keep = set()
    import time, calendar
    if now is None:
        now = time.time()
    def parse_snapshot(x):
            parts = x.split("-")
            ts = "-".join(parts[1:7])
            #ts = x[x.find('-') + 1:x.rfind('-')]
            return calendar.timegm(time.strptime(ts, "%Y-%m-%d-%H-%M-%S"))

    snapshot_times = sorted([(parse_snapshot(x), x)
                        for x in snapshots])  # oldest first
    snapshots = set(snapshots)

    def find_snapshot_between(start, stop):
        return [sn for (ts, sn) in snapshot_times if start <= ts < stop]
        for ts, sn in snapshot_times:
            if start <= ts < stop:
                return sn
        return None

    intervals_to_check = []
    for count, seconds, name in [
        (quarters, 15 * 60, 'quarter'),  # last quarters
        (hours, 3600, 'hour'),  # last 24 h,
        (days, 3600 * 24, 'day'),  # last 7 days
        (weeks, 3600 * 24 * 7, 'week'),  # last 5 weeks
        (months, 3600 * 24 * 30, 'month'),  # last 12 months
        (years, 3600 * 24 * 365, 'year'),  # last 10 years
    ]:
        # keep one from each of the last hours
        for interval in range(1, count + 1):
            start = now - interval * seconds
            stop = now - (interval - 1) * seconds
            intervals_to_check.append(
                (start, stop, "%s_%i" % (name, interval)))
    def format_time(t):
        ft = ["%.4i" % t.tm_year, "%.2i" % t.tm_mon, "%.2i" % t.tm_mday,
                "%.2i" % t.tm_hour, "%.2i" % t.tm_min, "%.2i" % t.tm_sec]
        return 'ffs-' + '-'.join(ft)
    for start, stop, name in intervals_to_check:
        found = find_snapshot_between(start, stop)
        if found:
            found = found[0]
            if not allow_one_snapshot_to_fill_multiple_intervals:
                snapshots.remove(found)
                snapshot_times.remove((parse_snapshot(found), found))
            keep.add(found)
    return keep



