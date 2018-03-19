#!/usr/bin/python3
import logging
import sys
import logging.handlers
import os
import re
if not os.path.exists('/var/log/ffs'):
    raise ValueError("Please create /var/log/ffs")
try:
    open('/var/log/ffs/debug.log', 'a')
except PermissionError:
    raise ValueError("Please check that ffs can write to /var/log/ffs")
# nodes configuration
nodes = {
    'pcmt391': {
        'hostname': 'amy',
        'storage_prefix': 'amy/ffs',
        'public_key': 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCqZJ8e3c5VQFysAKrkbZJ7RD1SPs1LOHfdYtCH5LubJBEp/SC9EIhfq4YWSLaq/QQiGB/YKtLfAfpth1bz4Iw3LHqoi2jrC1bNSaZnsoJ40qTehVxJCJoFlyHB+UoNoPXvnVR/o189Xiitz0iRDFVZ1QrvAfL0ddorskCkPV2adhV1TQDS9qkHa/uUhDjmIOzLXZYVcryjNzLBk0hp5nn7N6ghJifsAFjVvQYGwuQu3ldLlVTn7SY5Qy80D0sF6ch0kA9DfACzcP/1m3+cPlD3XvKOLoDCXzMEEc3AYFR/vlSzdMgVK4VWpdh9BOKgJRoFBCQbyPAbn7mho03Yo40H ffs@pcmt391'
    },
    'pcmt321': {  # must be the name the machine identifies by
        'hostname': 'rose',  # on what name to call the machine
        'storage_prefix': 'rose/ffs',
        'public_key': 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDRjLKaU8k5/b4eFax2Xd+wm2HqkLuMDby4ZkeMX8D3N6yaCJasi1+lmWSdDtwUKXF7Ox6w0JOmkHxvSnM/e5W0EBnSzc0Hg7OtC+MqqNJ8CMO7BaGisk6wQ5ejPHeP/fUZ5M8JPdDzP2vpmG9KUvdecteHbqDN8+9V6uRb8FMFch4NxPvJkhAYvOfcRGtdQ67Bcu4LSboDA6scHbPqElaB/z7mlLUXKoxsYKO/Mynbl00zZXBz/bOYn+n6cS3EKVpO8OKE+uDfLC2lw7XuL77txS4N4jvqbi9wZifWDLvbI+TFTtsmegYr892mJ2UZiVNmYHkdNgL5KnTR+00KoWbp ffs@pcmt321'
    },
    'pcmt335': {
        'hostname': 'donna',
        'storage_prefix': 'donna/ffs',
        'public_key': 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDVBWJOP1VCBDLbgp6uChMD/PSKAg2VoqUAP/ztT5CuvuEXfnRJbrghVsZ6r08ttOYD3JrtVmclclUPqs3ValVORpmuCydxU9zSGcmtca4JtooDi2aBYBLy4KlOPM30EQqEvGFcl+lRLJW6rHBO8KC28nbpDHEgZauCbDKA0PvLDT71XvDZAaJd7VKz00nsI+7kc6Ez2wkXnENNCWLEtC0Sw7elOn17Td2JoHkpi8TSk7W8HiRPJSkJOA1jkkgrxYDfC0TPTe85WML2ah9I3nz/iLBxPSooGY+g4CacKnaS0i6p7IYMqTocchjjmlHhSIMrwgbfxhkXM5wIQKx4D77R ffs@pcmt335',
    },
    'pcmt380': {
        'hostname': 'mf',
        'storage_prefix': 'mf/ffs',
        'public_key': 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDUu0S+IjR10GvikbawoTVUiMpy8vHT93j2zMdRDTwq6/LgoYoqd74CvVcYGzUZ4SMivoV0dL2RYbo9tmKHvt6N65oEL5cqwval8cQZ8m3PZ4zQYdbqbhpVW432BKJOGYswV5tqH7sJV7BxrFIxmqIUvUI5BM/qHZEVg27zVdSN1VE5OtPVp/jckj3XnsBLdXr122kabuVNGRT9DS2ddIxLyK9DGgqdeDCPbWCEYR/zE3eWKTHvtm2Qxse48k8GrGq9+/Jpa9o4OKCAtY2mCwlGW+Zx6XpDmcTbS9KBNNw7NpssBuIte+dKDcaTlLvy5hZBPElvOiaa08wi9eW4wml9 ffs@pcmt380\n',
        },
    'pcmt322': {
        'hostname': 'martha',
        'storage_prefix': 'martha/ffs',
        'public_key': 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDyjtBBoSslSuXmgidRocloyFbEwJc36SmW+vnAIIzT5OBX+yKv529CMsIttsUScu/BARMVeEkDX+0pI5b8vMygpjtQM9783/+/3OheEw8MgOm4HS90rc7g8xOqWjydIagotI4YhlUK2bCvrjYxavovUA0wsOTIBoCBHwqF5SrarzqN4nvZgcY8McHLwfjjYElcpbPutLXHneY+nPevrlKqkbnPONpXJ+Zf/BhgEiVbtvbU7QKewhf4TDJQU8y7Eto9pMwTqz2hcR9t+p0HFFYcd9JzEvyBQIvO6VL529rxhI0s2Qz1heV2NRVx6ZrReoDBjuNfoHjW7VdRUYsBI633 ffs@pcmt322\n'
    },
    'pcmt383': {
        'hostname': 'nostromo',
        'storage_prefix': 'nostromo/ffs',
        'public_key': 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDsHKLAos/0PnCUS4F/vXUmjDpPEUUgMSNPIFgluBKlFjFh0z4gHgZcAr4oRWO1wRv66Fu3hK+jM0doEL+bCAa4hDZT2vSAlUOGGgnphtcjhzUrNwDUorT7ZtY2/0PWvldPFnKcBYkSpweiLKmiiJbhK2qhENsQee2UfFqB2W1yqc43eCCksxnQtrCJAA+I7ilHGde++8t6z5A7fW4M857LegPQfrAcXAlIIT6A//HEwYvS2XadX0jlBNC/oo9Gy4bGYiAwjsePAjC0ItRUjNLVN2bbgqC6CYpotmCTaX9788Cra9/B1AJ2UoxBwmDMzT5b5jrYpjVI1BEL9sFH2wPv ffs@pcmt383\n',
    },
}

nodes = {
    'pcmt283': {
        'hostname': 'mm',
        'storage_prefix': 'mm/ffs',
        'public_key': b'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDIjVSaY586Lq03HgF47MjEh+Kt7dZsxkrnzaQ+pQq3wAJMV2by4M1aSUb3ETmHkHdgD2Eda3uMFM4wNR3BnBMKSkcwJqcGLPAASOBXhEVgEOIZ7lNy34UZAdUMsm7HmnulTj75dsw5e/WwDVZfDY6+kSnL7ZuyNJtkR/j0YlN6TivYMoPw7OJIJWozFeUStIoG98kzwRH/Psv2NMQoQ51fOlkfJ+sIGxMGjDE2AlyGCX0+cbERAnmYakzuPt9NNa19p9I9aGz2qltW6xXk/yJ4iaWsyECc4tFw8uL4QlMVzLH5CY+FKKlxSLZTEOdLZ8Xu/5CNWgRCdwU/RbHLfgnN ffs@pcmt283',
    }

}

def decide_nodes(ffs_name):
    """Decide who is master and what targets are going to be used"""
    import random
    if ffs_name.startswith('datasets'):
        return ['ensembl']
    if random.random() < 0.5:
        return ['pcmt391', 'pcmt321']
    else:
        return ['pcmt321', 'pcmt391']

# output channel


def complain(message):
    """This gets called on (catastrophic) failures. Contact your admin stuff basically"""
    with open("/var/log/ffs/complain.log", 'a') as op:
        print('', file=op)
        print("-----------Complain----------", file=op)
        print(message, file=op)
        print("-----------EndComplain----------", file=op)
        print('', file=op)

def inform(message):
    """Keep your users informed."""
    with open("/var/log/ffs/inform.log", 'a') as op:
        print('', file=op)
        print("-----------INFORM----------")
        print(message, file=op)
        print("-----------EndInform----------", file=op)
        print('', file=op)


def decide_snapshots_to_keep(dummy_ffs_name, snapshots):
    """Decide which snapshots to keep.
    Snapshots relevant are labled like 'ffs-2017-04-24-16-43-32-604
    """
    import time

    def parse_snapshot(x):
        parts = x.split("-")
        ts = "-".join(parts[1:7])
        #ts = x[x.find('-') + 1:x.rfind('-')]
        return time.mktime(time.strptime(ts, "%Y-%m-%d-%H-%M-%S"))
    snapshots = list(reversed(sorted(snapshots)))  # newest first
    keep = set([x for x in snapshots if not x.startswith('ffs-')
                and not x.startswith('zfs-auto-snap_')
                ])  # keep any non-ffs snapshots. They don't get synced though!
    snapshots = [x for x in snapshots if x.startswith('ffs-')]
    keep_by_default = 10
    keep.update(snapshots[:keep_by_default])  # always keep the last 10

    # scb special casing - postfix set
    scb_snapshots = [x for x in snapshots if x.endswith('-scb')]
    if scb_snapshots:
        keep.add(scb_snapshots[0]) # always keep the newest scb snapshot. Otherwise, they get sorted in with the regular snapshots

    snapshots = snapshots[keep_by_default:]
    snapshot_times = [(parse_snapshot(x), x)
                      for x in snapshots][::-1]  # oldest first
    snapshots = set(snapshots)

    def find_snapshot_between(start, stop):
        for ts, sn in snapshot_times:
            if start < ts < stop:
                return sn
        return None

    intervals_to_check = []
    for count, seconds, name in [
        (24, 3600, 'hour'),  # last 24 h,
        (7, 3600 * 24, 'day'),  # last 7 days
        (5, 3600 * 24 * 8, 'week'),  # last 5 weeks
        (12, 3600 * 24 * 30, 'month'),  # last 12 months
        (10, 3600 * 24 * 365, 'year'),  # last 10 years
    ]:
        for interval in range(1, count + 1):  # keep one from each of the last hours
            start = time.time() - interval * seconds
            stop = time.time() - (interval - 1) * seconds
            intervals_to_check.append((start, stop, "%s_%i" % (name, interval)))
    for start, stop, name in intervals_to_check:
        found = find_snapshot_between(start, stop)
        if found:
            logger.debug("Decided to keep: %s because of %s", found, name)
            snapshots.remove(found)
            snapshot_times.remove((parse_snapshot(found), found))
            keep.add(found)
    return keep


chown_user = 'finkernagel'
chmod_rights = 'uog+rwX'

ssh_cmd = ['ssh', '-p', '223', '-o', 'StrictHostKeyChecking=no', ]  # default ssh command
ssh_concurrent_connection_limit = 5
zpool_check_frequency = 60 * 15  # in seconds
zmq_port = 47777
zmq_server = 'pcmt283.imt.uni-marburg.de'  # martha
# default zfs properties
default_properties = {'compression': 'on',
                      'com.sun:auto-snapshot': 'false',
                      'atime': 'off'}
# logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

debug_logger = logging.handlers.RotatingFileHandler(
    "/var/log/ffs/debug.log", mode='a', maxBytes=10 * 1024 * 1024, backupCount=1, encoding=None, delay=0)
debug_logger.setLevel(logging.DEBUG)
error_logger = logging.handlers.RotatingFileHandler(
    "/var/log/ffs/error.log", mode='a', maxBytes=10 * 1024 * 1024, backupCount=1, encoding=None, delay=0)
error_logger.setLevel(logging.ERROR)
console_logger = logging.StreamHandler()
console_logger.setLevel(logging.ERROR)
formatter = logging.Formatter(
    '%(asctime)s  %(levelname)-8s  %(module)s:%(lineno)d %(message)s')
debug_logger.setFormatter(formatter)
console_logger.setFormatter(formatter)
logger.addHandler(debug_logger)
logger.addHandler(console_logger)
logger.addHandler(error_logger)

# stuff that ascertains that the config is as expected - no need to edit
for n in nodes:
    if nodes[n].get('hostname', None) is None:
        nodes[n]['hostname'] = n
    if not nodes[n]['storage_prefix'].endswith('/'):
        nodes[n]['storage_prefix'] += '/'

#sanity checking