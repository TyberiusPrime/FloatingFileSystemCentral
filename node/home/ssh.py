#!/usr/bin/python3
import sys
import os
try:
    import node
except SyntaxError as e:
    # if we can not import node,
    # we still need to run in order to accept
    # scp transfers for node.py and ssh.py
    # for the 'manual_redeployment'
    # in case you accidentially
    # broke the code while editing
    # and it has been distributed to the clients
    # and you're calling manual_deployment.py
    node = False
    pass
import json
import traceback
import signal
import time
import atexit

cmd_line = os.environ.get('SSH_ORIGINAL_COMMAND', '')
if cmd_line.startswith('scp'):
    # needed for manual deploy of node.py when node.py has been broken.
    # this is also why this is at the very beginning of this file
    if cmd_line not in ('scp -t /home/ffs/node.py', 'scp -t /home/ffs/ssh.py'):
        raise ValueError("invalid scp target")
    os.system(cmd_line)
    sys.exit()


def fail(message=''):
    print(json.dumps({
        'error': 'unkown_error',
        'content': message
    }))
    sys.exit(55)


def onexit():
    try:
        #group_to_kill = os.getpgid(os.getpid())
        # os.killpg(, signal.SIGTERM)
        if node is not False:
            for p in node.child_processes:
                try:
                    p.terminate()
                except:
                    pass
        pass
    except Exception:
        pass


def on_sighup(dummy_signum, dummy_frame):  # SSH connection was terminated.
    sys.exit(2)


def on_sig_term(dummy_signum, dummy_frame):  # SSH connection was terminated.
    sys.exit(0)

atexit.register(onexit)
signal.signal(signal.SIGHUP, on_sighup)
#signal.signal(signal.SIGTERM, on_sig_term)

if True:
    if cmd_line.startswith('rprsync'):  # robust parallel rsync
        node.shell_cmd_rprsync(cmd_line)
    else:
        json_input = ''
        j = sys.stdin.read()
        while j:
            json_input += j
            j = sys.stdin.read()
        try:
            j = json.loads(json_input)
            result = node.dispatch(j)
            sys.stdout.buffer.write(json.dumps(reply).encode('utf-8'))
            sys.exit(0)
        except Exception as e:  # pylint: disable=W0703
            import traceback
            tb = traceback.format_exc()
            reply = {"error": 'exception', 'content': str(e), 'traceback': tb}
            sys.stdout.buffer.write(json.dumps(reply).encode('utf-8'))
            sys.exit(1)
