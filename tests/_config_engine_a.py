from central.default_config import DefaultConfig
import logging, logging.handlers
import os
import subprocess

_pool = None
def get_pool():
    global _pool
    if _pool is None:
        lines = subprocess.check_output(['sudo','zpool','status']).split(b"\n")  # use status, it's in the sudoers
        for l in lines:
            l = l.strip()
            if l.startswith(b'pool:'):
                pool = l[l.find(b':') + 2:].strip().decode('utf-8')
                _pool = pool
                break
        else:
            raise ValueError("Could not find a zpool to create .ffs_testing zfs on")
    return _pool

class Config(DefaultConfig):

    def __init__(self):
        self._get_logging_count = 0

    def get_nodes(self):
        return {
            'A': {
                'hostname': 'localhost',
                'storage_prefix': '/' + get_pool() + '/.ffs_testing_client_from',
                'public_key': 'not_in_use'
            },
            'B': {
                'hostname': 'localhost',
                'storage_prefix': '/' + get_pool() + '/.ffs_testing_client_to',
                'public_key': 'not_in_use'
            },

        }

    def do_timebased_actions(self):
        return False

    def get_logging(self):
        self._get_logging_count += 1 
        if self._get_logging_count > 1:
            raise ValueError("Repeated calls to get_logging - are you accessing outside of CheckedConfig?")
        zmq_auth = logging.getLogger('zmq.auth')
        zmq_auth.addHandler(logging.NullHandler())
        logger = logging.getLogger('FFS')
        logger.setLevel(logging.DEBUG)

        debug_logger = logging.handlers.RotatingFileHandler(
            os.path.join(os.path.dirname(__file__), "client_test_debug.log"), mode='w', maxBytes=10 * 1024 * 1024, backupCount=1, encoding=None, delay=0)
        debug_logger.setLevel(logging.DEBUG)
        error_logger = logging.handlers.RotatingFileHandler(
            os.path.join(os.path.dirname(__file__), "client_test_error.log"), mode='w', maxBytes=10 * 1024 * 1024, backupCount=1, encoding=None, delay=0)
        error_logger.setLevel(logging.ERROR)
        console_logger = logging.StreamHandler()
        console_logger.setLevel(logging.ERROR)
        formatter = logging.Formatter(
            '%(asctime)s  %(levelname)-8s  %(module)s:%(lineno)d %(message)s')
        debug_logger.setFormatter(formatter)
        console_logger.setFormatter(formatter)
        logger.addHandler(debug_logger)
        # logger.addHandler(console_logger)
        logger.addHandler(error_logger)
        return logger

    def get_zmq_port(self):
        return 47776

    def do_deploy(self):
        return False

    def get_chmod_rights(self, dummy_ffs):
        return '0751'
    
config = Config()
          
