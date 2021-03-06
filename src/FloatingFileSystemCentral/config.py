#!/usr/bin/python3
import logging
import subprocess
import sys
import logging.handlers
import os
import re
from .default_config import DefaultConfig, keep_snapshots_time_policy


class Config(DefaultConfig):
    def get_nodes(self):
        result = {
            "amy": {
                "hostname": "amy",  # so we use the 10gb interface
                "storage_prefix": "/amy/ffs",
                "public_key": b"ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCi2cog0uWovQ+/iXwPDdx9/umOVPz1n2GbR/kNkZNKKYVncuI47ROXYv232l9dLcWGfKIBYio4039ulc+xvkwSwA5MOEqXTKad9gpS74eE//ydZUcbrlmF3rQoKNj+oeV/+2JKj7LpTlS23YgvA4xShhLiigOT9b8soIeFL1UlEqh0lgczQhgPaSbjxyRan8KKaJ2rQkC6bKxppsV/i6ENlb17hepzZBrRtvSQQ3dUBxrkQ/76Xe2Fr7bKX9BQiHCAw9KTmuofr+cVRG7jV4r1g7wmqQkHvJoncXP9Us72XBW7hmNg1mhjWI0+4n48ZRnIrFolAksvpIIuKoXg4Xtz ffs@pcmt391",
            },
            "rose": {  # what to call the machine
                "hostname": "rose",  #
                "storage_prefix": "/rose/ffs",
                "public_key": b"ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDRjLKaU8k5/b4eFax2Xd+wm2HqkLuMDby4ZkeMX8D3N6yaCJasi1+lmWSdDtwUKXF7Ox6w0JOmkHxvSnM/e5W0EBnSzc0Hg7OtC+MqqNJ8CMO7BaGisk6wQ5ejPHeP/fUZ5M8JPdDzP2vpmG9KUvdecteHbqDN8+9V6uRb8FMFch4NxPvJkhAYvOfcRGtdQ67Bcu4LSboDA6scHbPqElaB/z7mlLUXKoxsYKO/Mynbl00zZXBz/bOYn+n6cS3EKVpO8OKE+uDfLC2lw7XuL77txS4N4jvqbi9wZifWDLvbI+TFTtsmegYr892mJ2UZiVNmYHkdNgL5KnTR+00KoWbp ffs@pcmt321",
            },
            "donna": {
                "hostname": "pcmt335",
                "storage_prefix": "/donna/ffs",
                "public_key": b"ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDVBWJOP1VCBDLbgp6uChMD/PSKAg2VoqUAP/ztT5CuvuEXfnRJbrghVsZ6r08ttOYD3JrtVmclclUPqs3ValVORpmuCydxU9zSGcmtca4JtooDi2aBYBLy4KlOPM30EQqEvGFcl+lRLJW6rHBO8KC28nbpDHEgZauCbDKA0PvLDT71XvDZAaJd7VKz00nsI+7kc6Ez2wkXnENNCWLEtC0Sw7elOn17Td2JoHkpi8TSk7W8HiRPJSkJOA1jkkgrxYDfC0TPTe85WML2ah9I3nz/iLBxPSooGY+g4CacKnaS0i6p7IYMqTocchjjmlHhSIMrwgbfxhkXM5wIQKx4D77R ffs@pcmt335",
            },
            "mf": {
                "hostname": "pcmt380",
                "storage_prefix": "/mf/ffs",
                "public_key": b"ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDcN2Wr/+Ezo119pgjDDOahK8XH8qJTXqoRR1XOMhoPPsJTfnPvSDB8LNWcRkdIADSolwNU9I2PyODlVJonCgDVNDDgzr1oTSQUovkc37cl3aU1PKHkuVVyWveBGOsE1fIfWQHXUepZFyxqVG+nLKOOOxV3EH0t2mjOJYLgDw9uCOnnMcI9G2XkOm1jSg6kZCmHL63rAs617SX2D5rvKQEc/HxxLLqd0ofA1YzBc3O/dq92J1bRnUOJcWu8vCdWxhRIZtV0fYnGjfD6JPHX0hRyXyJB25sYrqGvEa4YQN9r2XETsdcjAylhAxS22SMZVeQ1AEc2JQKatvl3ximTf2b9 ffs@pcmt380",
            },
            "martha": {
                "hostname": "martha",
                "storage_prefix": "/martha/ffs",
                "public_key": b"ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDyjtBBoSslSuXmgidRocloyFbEwJc36SmW+vnAIIzT5OBX+yKv529CMsIttsUScu/BARMVeEkDX+0pI5b8vMygpjtQM9783/+/3OheEw8MgOm4HS90rc7g8xOqWjydIagotI4YhlUK2bCvrjYxavovUA0wsOTIBoCBHwqF5SrarzqN4nvZgcY8McHLwfjjYElcpbPutLXHneY+nPevrlKqkbnPONpXJ+Zf/BhgEiVbtvbU7QKewhf4TDJQU8y7Eto9pMwTqz2hcR9t+p0HFFYcd9JzEvyBQIvO6VL529rxhI0s2Qz1heV2NRVx6ZrReoDBjuNfoHjW7VdRUYsBI633 ffs@pcmt322",
            },
            "nostromo": {
                "hostname": "nostromo",
                "storage_prefix": "/nostromo/ffs",
                "public_key": b"ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDsHKLAos/0PnCUS4F/vXUmjDpPEUUgMSNPIFgluBKlFjFh0z4gHgZcAr4oRWO1wRv66Fu3hK+jM0doEL+bCAa4hDZT2vSAlUOGGgnphtcjhzUrNwDUorT7ZtY2/0PWvldPFnKcBYkSpweiLKmiiJbhK2qhENsQee2UfFqB2W1yqc43eCCksxnQtrCJAA+I7ilHGde++8t6z5A7fW4M857LegPQfrAcXAlIIT6A//HEwYvS2XadX0jlBNC/oo9Gy4bGYiAwjsePAjC0ItRUjNLVN2bbgqC6CYpotmCTaX9788Cra9/B1AJ2UoxBwmDMzT5b5jrYpjVI1BEL9sFH2wPv ffs@pcmt383",
            },
            "mm": {
                "hostname": "pcmt283",
                "storage_prefix": "/mm/ffs",
                "public_key": b"ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDIjVSaY586Lq03HgF47MjEh+Kt7dZsxkrnzaQ+pQq3wAJMV2by4M1aSUb3ETmHkHdgD2Eda3uMFM4wNR3BnBMKSkcwJqcGLPAASOBXhEVgEOIZ7lNy34UZAdUMsm7HmnulTj75dsw5e/WwDVZfDY6+kSnL7ZuyNJtkR/j0YlN6TivYMoPw7OJIJWozFeUStIoG98kzwRH/Psv2NMQoQ51fOlkfJ+sIGxMGjDE2AlyGCX0+cbERAnmYakzuPt9NNa19p9I9aGz2qltW6xXk/yJ4iaWsyECc4tFw8uL4QlMVzLH5CY+FKKlxSLZTEOdLZ8Xu/5CNWgRCdwU/RbHLfgnN ffs@pcmt283",
            },
            "clara": {
                "hostname": "pcmt289",
                "storage_prefix": "/clara/ffs",
                'public_key': b'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDHnQYh3xT9OSJMDFZvQgBcAPM3/rakCRt6bHhzXa2JHDvhfLvM+Q5yEOUMUAD6ELiAqdqvfBIf7bqtHqa69KMaLr91cIvRrRAZUAelGPhk+HNOXcC8Q9pEbWuQZ6OADzDNkgd/FmqyDpVqKl6hF0B6i2WAzJdrmu7rOPS4wL0Sy9qpPSqyAbifi4gMnWZemiv0tNbmTBT1E6QkBAks9hQKIGCGXZUphOU2ryS+p9sP2IIRSKOCIPR19TUsO5WFOY5xZrUpoWXsEQenLDvExi62ZZINxGrEZtXV5zh2/+r/h1QufPSl1MkVZDIuGCgrEzR4vXtRktdIi5k9Ldcr+TX/ ffs@pcmt289',
            },
            'fr': {
                'hostname': 'pcmt230.imt.uni-marburg.de',
                'storage_prefix': '/fr/ffs',
                'public_key': b'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDLZr6eGWB7j+mzBzmIY6eqa2fv0J+CfrzVPbp8QpnVV+/VF/WTY7SsyB7nIEni8ofj4HSx/HZsnMVLC3Kg2dDkamWjo5V5YIA+qAUmr2H6vCK/Xxq+80juZatjUQw9Zkr69o838yyhvM82eV0mR0wKhcVbljWi+p9EMZ+W7t0utwWdRa5FRaRVHS+91d2DX73eblnOmeH8J+kOeOuEuPhFkIs+yaCuon89/V4eGkUGgKsCPbYeilv4Lf9sc8C+9RXJKzwoudxSXZYZfGBMl846NCg+GIdNPmi5QboW0rTWtnXeZjNumUuBc4HjQes0uQu1fv6sDN/uHsdHBDkRksfH ffs@pcmt230.imt.uni-marburg.de',
            },
        }
        if True:
            result.update(
                {
                    "wilfred": {
                        "hostname": "wilfred",
                        "storage_prefix": "/wilfred/ffs",
                        "public_key": b"ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCym9t69yMEUNeULQPpplD9oIYlUgWhoPOo8XZkYD3gU5ErsJuKQ5wwWKlvITbuTo3M+sboVgBd7gXJormo9WT1lCHXDZ5E3IL6gyIckdWV4fuptN6f18gbKcQPviOhN8YehlM7fijGigwGSvUmR01p9ckoTuJCRfp5GyQIRMNL93mkSH7ir89EP0z14LFzfTeeWZZmh9+ZGomhc/Xll9TsrOosy7jhPOvrYeUXJ6a/vMSovu4gbXJ5lzm0hW1zhNp21X3uridKX2XYyl+vOT19Bg+ZBVxPR9i37L+S4RRjb/NvuoQHiuTAzQ6GT87HVo7kb4fIgRxtT1vGi85D4wkn ffs@pcmt410",
                    }
                }
            )
        return result

    def get_logging(self):
        if not os.path.exists("/var/log/ffs"):
            raise ValueError("Please create /var/log/ffs")
        try:
            open("/var/log/ffs/debug.log", "a")
        except PermissionError:
            raise ValueError("Please check that ffs can write to /var/log/ffs")
        # zmq_auth = logging.getLogger('zmq.auth')
        # zmq_auth.addHandler(logging.NullHandler())

        logger = logging.getLogger("FFS")
        logger.setLevel(logging.DEBUG)

        debug_logger = logging.handlers.RotatingFileHandler(
            "/var/log/ffs/debug.log",
            mode="a",
            maxBytes=200 * 1024,
            backupCount=2,
            encoding=None,
            delay=0,
        )
        debug_logger.setLevel(logging.DEBUG)
        error_logger = logging.handlers.RotatingFileHandler(
            "/var/log/ffs/error.log",
            mode="a",
            maxBytes=10 * 1024,
            backupCount=1,
            encoding=None,
            delay=0,
        )
        error_logger.setLevel(logging.ERROR)
        console_logger = logging.StreamHandler()
        console_logger.setLevel(logging.ERROR)
        formatter = logging.Formatter(
            "%(asctime)s  %(levelname)-8s  %(module)s:%(lineno)d %(message)s"
        )
        debug_logger.setFormatter(formatter)
        console_logger.setFormatter(formatter)
        error_logger.setFormatter(formatter)
        logger.addHandler(debug_logger)
        logger.addHandler(console_logger)
        logger.addHandler(error_logger)
        zmq_auth = logging.getLogger("zmq.auth")
        zmq_auth.addHandler(debug_logger)

        return logger

    def complain(self, message):
        with open("/var/log/ffs/complain.log", "a") as op:
            print("", file=op)
            print("-----------Complain----------", file=op)
            print(message, file=op)
            print("-----------EndComplain----------", file=op)
            print("", file=op)
        print("complain:", message)
        message = "FFS Complaining about: " + message
        p = subprocess.Popen(
            ["/machine/opt/infrastructure/client/call_mattermost.py"],
            stdin=subprocess.PIPE,
        )
        p.communicate(message.encode("utf-8"))

    def inform(self, message):
        """Keep your users informed."""
        with open("/var/log/ffs/inform.log", "a") as op:
            print("", file=op)
            print("-----------INFORM----------", file=op)
            print(message, file=op)
            print("-----------EndInform----------", file=op)
            print("", file=op)
        print("inform:", message)
        message = "FFS Informing about: " + message
        p = subprocess.Popen(
            ["/machine/opt/infrastructure/client/call_mattermost.py"],
            stdin=subprocess.PIPE,
        )
        #p.communicate(message.encode("utf-8"))
        p.stdin.close()

    def decide_targets(self, ffs):
        if ffs.startswith("20"):
            raise ValueError(
                "Your ffs must not be '2018...', you probably meant e/2018..."
            )
        choices = [
            ["martha", "rose"],
            ["rose", "martha"],
            ["amy", "martha"],
            ["amy", "rose"],
            ["martha", "amy"],
            ["rose", "amy"],
        ]
        import random

        return random.choice(choices)

    def decide_snapshots_to_keep(self, dummy_ffs_name, snapshots):
        """  
        capture-snapshots relevant are labled like 'ffs-2017-04-24-16-43-32-604

        Default policy is: 
            -delete all zfs-auto-snap
            -keep anything non-ffs 
            -for ffs-* keep based on date
        """
        import time

        snapshots = list(reversed(sorted(snapshots)))  # newest first
        keep = set(
            [
                x
                for x in snapshots
                if not x.startswith("ffs-")
                and not x.startswith("zfs-auto-snap_")
                or x.endswith("-keep")
            ]
        )  # keep any non-ffs snapshots. They don't get synced though!
        snapshots = [x for x in snapshots if x.startswith("ffs-")]
        keep_by_default = 10
        keep.update(snapshots[:keep_by_default])  # always keep the last 10
        # now apply time based policy
        keep.update(
            keep_snapshots_time_policy(
                snapshots, quarters=4, hours=24, days=10, weeks=5, months=12, years=10
            )
        )
        return keep

    def decide_snapshots_to_send(self, dummy_ffs_name, snapshots):
        """What snapshots for this ffs should be transmitted?"""
        if snapshots:
            return set([snapshots[-1]])
        else:
            return set()

    def get_zpool_frequency_check(self):
        # in seconds
        return 15 * 60  # 0 = disabled, seconds otherwise

    def get_ssh_rate_limit(self):
        return 0.5

    def restart_on_code_changes(self):
        return True

    def get_concurrent_rsync_limit(self):
        return 5

    def exclude_subdirs_callback(self, ffs, source_node, target_node):
        """You can prevent some sub directories of your FFS to be synced to specific targets by 
        returning a list of sub directories to be excluded here"""
        if target_node == "wilfred":
            if "e/" in ffs:
                return ["cache", "results"]
            elif "www/imtwww_lims" in ffs:
                return ["genomes"]
        return []

    def get_chown_user(self, dummy_ffs):
        return "finkernagel"

    def get_ssh_cmd(self):
        return [
            "ssh",
            "-p",
            "223",
            "-o",
            "StrictHostKeyChecking=no",
            "-i",
            "/home/ffs/.ssh/id_rsa",
            #'-v',
        ]  # default ssh command, #-i is necessary for 'sudo rsync'

    


config = Config()
all = [config]
