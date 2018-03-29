#!/usr/bin/python3
import pprint
import sys
if sys.version_info[0] < 3:
    raise ValueError("FFS needs python3")
import hashlib
import os
from twisted.internet import reactor, task, error
from txzmq import ZmqEndpoint, ZmqFactory, ZmqREPConnection
import hashlib
import logging
from central import default_config
if len(sys.argv) == 2:
    config_file = sys.argv[1]
    if not os.path.exists(config_file):
        raise ValueError("Could not import config, config file does not exist")
    if not config_file.endswith('.py'):
        raise ValueError("Could not import config, config file is not python")
    config_file = os.path.abspath(config_file)
    import importlib.util
    spec = importlib.util.spec_from_file_location("config", config_file)
    config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config)
else:
    from central import config
cfg = default_config.CheckedConfig(config.config)
logger = cfg.get_logging()
from central import engine
from central import ssh_message_que
from twisted.python import log
import pwd
import atexit
import json
log.startLogging(sys.stdout)


restart = False
file_changed_hash = None


def check_if_changed():
    global restart
    excluded = ['cmd.py']
    included = [__file__, 'node/home/node.py', 'node/home/ssh.py']
    included.extend([os.path.join('central', x)
                     for x in os.listdir("central") if x.endswith('.py')])
    global file_changed_hash
    h = hashlib.md5()
    for fn in os.listdir('.') + included:
        if fn.endswith('.py') and fn not in excluded:
            with open(fn, 'rb') as op:
                d = op.read()
                h.update(d)
    if file_changed_hash is not None and file_changed_hash != h.hexdigest():
        logger.info('FloatingFileSystem code changed - restarting')
        cfg.inform('FloatingFileSystem code changed - restarting')
        restart = True
        reactor.stop()
    file_changed_hash = h.hexdigest()


our_engine = None


def on_shutdown():
    logger.info("Shutdown")
    if our_engine:
        logger.info("Shutdown ->engine")
        our_engine.shutdown()
    else:
        logger.info("Shutdown engine was falsy")


class EncryptedZmqREPConnection(ZmqREPConnection):

    def __init__(self, factory, endpoint=None, identity=None):
        ZmqREPConnection.__init__(self, factory, None, identity)
        self.keys_dir = 'certificates'
        self.key_filename = os.path.join(self.keys_dir, "server.key_secret")
        self.make_keys()
        public_key, secret_key = self.load_keys()
        logger.debug("server public key %s" % public_key)
        self.socket.curve_publickey = public_key
        self.socket.curve_secretkey = secret_key
        self.socket.curve_server = True
        self.addEndpoints([endpoint])

    def make_keys(self):
        import zmq.auth
        if not os.path.exists(self.keys_dir):
            os.mkdir(self.keys_dir)
        if not os.path.exists(self.key_filename):
            zmq.auth.create_certificates(self.keys_dir, "server")

    def load_keys(self):
        import zmq.auth
        return zmq.auth.load_certificate(self.key_filename)


def main():
    dry_run = '--dry-run' in sys.argv
    global our_engine
    logger.debug("")
    logger.debug("")
    logger.debug("")
    logger.debug("")
    logger.debug("Start")
    if dry_run:
        logger.debug("Dry run!")
    user = pwd.getpwuid(os.getuid())[0]
    if user != 'ffs':
        print(os.getuid())
        raise ValueError(
            "Must be started as user ffs - use ffs_central.sh - was %s" % user)

    zf = ZmqFactory()

    from zmq.auth.thread import ThreadAuthenticator
    auth = ThreadAuthenticator(zf.context, log=logging.getLogger("zmq.auth"))
    auth.start()
    import zmq.auth
    auth.configure_curve(domain='*', location=zmq.auth.CURVE_ALLOW_ANY)
    try:

        e = ZmqEndpoint('bind', 'tcp://*:%s' % cfg.get_zmq_port())
        s = EncryptedZmqREPConnection(zf, e)
        our_engine = engine.Engine(cfg, sender=None, dry_run=dry_run)
        check_if_changed()  # capture hashes
        l = task.LoopingCall(check_if_changed)
        l.start(1.0)  # call every second
        if cfg.do_timebased_actions():
            l2 = task.LoopingCall(lambda: our_engine.one_minute_passed())
            l2.start(60)
        if cfg.get_zpool_frequency_check()>  0 and configcfgdo_timebased_actions():
            l3 = task.LoopingCall(lambda: our_engine.do_zpool_status_check())
            l3.start(cfg.get_zpool_frequency_check())
        

        reactor.addSystemEventTrigger('before', 'shutdown', on_shutdown)

        def handle_message(msgId, *args):
            global restart
            try:
                str_payload = args[0].decode('utf-8')
                j = json.loads(str_payload)
                logger.info("Incoming client message: %s", pprint.pformat(j))
                reply = our_engine.incoming_client(j)
                if reply is None:
                    reply = {'error': 'no_return_value_set'}
            except engine.RestartError:
                restart = True
                reply = {"ok": True}
            except Exception as e:
                import traceback
                logger.error(traceback.format_exc())
                logger.error("Exception occured: %s", repr(e))
                cfg.complain("Exception occured handling %s: %s" %
                                (j, repr(e)))
                reply = {"error": "exception", 'content': repr(e)}
            logger.info("Reply to client: (%i) %s",
                        len(repr(reply)), repr(reply)[:80])
            reply = json.dumps(reply)
            reply = reply.encode("utf-8")
            s.reply(msgId, reply)
            if restart:
                reactor.stop()

        s.gotMessage = handle_message
        logger.debug("Entering reactor")
        our_engine.incoming_client({"msg": 'startup'})
        try:
            reactor.run()
        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt")
            raise
        logger.debug("Left reactor")
    finally:
        auth.stop()
    if restart:
        os.execv(sys.executable, ['python3'] + sys.argv)

if __name__ == '__main__':

    main()
