import json
import pprint
from twisted.internet import reactor, protocol, error
import time


class MessageInProgress:

    def __init__(self, node_name, node_info, msg):
        self.node_name = node_name
        self.node_info = node_info
        self.msg = msg.copy()
        self.status = 'unsent'


def format_msg(msg):
    x = msg.copy()
    if 'node.zip' in x:
        x['node.zip'] = '...base64 encoded data...'
    return pprint.pformat(x)


class OutgoingMessages:

    def __init__(self, logger, engine, ssh_cmd):
        self.max_per_host = engine.config.get_ssh_concurrent_connection_limit()
        self.logger = logger
        self.job_id = 0
        self.outgoing = {}
        self.running_processes = []
        self.engine = engine
        self.ssh_cmd = ssh_cmd
        self._shutdown = False

    def kill_unsent_messages(self):
        self.logger.warn("Killing all unsent messages!")
        for node in self.outgoing:
            self.outgoing[node] = [x for x in self.outgoing[node] if x.status != 'unsent']

    def send_message(self, node_name, node_info, msg):
        if msg['msg'] not in ('deploy', 'list_ffs', 'set_properties', 'remove_snapshot', 'send_snapshot',
            'new'):
            self.logger.info(
                "Msgfiltered to %s: %s", node_name, format_msg(msg))
            return
        self.logger.info("Outgoing to %s: %s",
                         node_name, format_msg(msg))
        if node_name not in self.outgoing:
            self.outgoing[node_name] = []
        self.outgoing[node_name].append(
            MessageInProgress(node_name, node_info, msg))
        self.send_if_possible()

    def send_if_possible(self):
        for dummy_node_name, outbox in self.outgoing.items():
            unsent = [x for x in outbox if x.status == 'unsent']
            in_progress = [x for x in outbox if x.status == 'in_progress']
            transfers_in_progress = [
                x for x in in_progress if x.msg['msg'] == 'send_snapshot']
            if unsent:
                for x in unsent:
                    if len(in_progress) < self.max_per_host:
                        if (
                            (x.msg['msg'] == 'send_snapshot' and not transfers_in_progress) or
                            (x.msg['msg'] != 'send_snapshot')
                        ):
                            x.job_id = self.job_id
                            self.job_id += 1
                            self.do_send(x)
                            in_progress.append(x)
                            x.status = 'in_progress'
                            if x.msg['msg'] == 'send_snapshot':
                                transfers_in_progress.append(x)
                    else:
                        break

    def do_send(self, msg):
        self.logger.info("Sending to %s: %s",
                         msg.node_name,  format_msg(msg.msg))
        ssh_cmd = self.ssh_cmd + \
            [msg.node_info['hostname'], '/home/ffs/ssh.py']
        m = msg.msg.copy()
        m['to'] = msg.node_name
        p = LoggingProcessProtocol(m, msg.job_id, self.job_returned,
                                   self.logger, self.running_processes)
        self.running_processes.append(p)
        reactor.spawnProcess(p, ssh_cmd[0], ssh_cmd, {})

    def job_returned(self, job_id, result):
        found = None
        for msgs in self.outgoing.values():
            for m in msgs:
                if m.job_id == job_id:
                    found = m
                    break
            if found:
                break
        if not found:
            self.logger.error(
                "Job_id %s return, but no such job found", job_id)
            return
        if found.status != 'in_progress':
            self.logger.error(
                "Job_id %s return, but not in progress! - was %s", job_id, m.status)
            return
        try:
            result['from'] = m.node_name
            self.engine.incoming_node(result)
        except Exception as e:
            import traceback
            self.logger.error(
                "Node processing error in job_id return %s %s %s, outgoing was: %s", job_id, result, e, format_msg(m.msg))
            self.logger.error(traceback.format_exc())
        self.outgoing[m.node_name].remove(m)
        self.send_if_possible()

    def shutdown(self):
        if self._shutdown:
            return
        self._shutdown = True
        self.kill_unsent_messages()
        for p in self.running_processes:
            p.terminated = True
            self.logger.info("Terminating running child: %s", p)
            try:
                p.transport.signalProcess('INT')
                p.transport.signalProcess('HUP')
            except error.ProcessExitedAlready:
                pass
        time.sleep(1)
        for p in self.running_processes:
            self.logger.info("Killing running child: %s", p)
            try:
                p.transport.signalProcess('KILL')
            except error.ProcessExitedAlready:
                pass
        #os.killpg(os.getpgid(os.getpid()), signal.SIGTERM)

class OutgoingMessagesDryRun(OutgoingMessages):
    """Filter all messages that change the downstream.
    Useful to check whether the system will do 
    madness on first startup"""

    def send_message(self, node_name, node_info, msg):
        if msg['msg'] in ('deploy', 'list_ffs'):
            OutgoingMessages.send_message(self, node_name, node_info, msg)
        else:
            self.logger.info(
                "Msgfiltered to %s: %s", node_name, format_msg(msg))
            return
   

class LoggingProcessProtocol(protocol.ProcessProtocol):

    def __init__(self, cmd, job_id, job_done_callback, logger, running_processes):
        self.cmd = cmd
        self.job_id = job_id
        self.job_done_calleback = job_done_callback
        self.stdout = b''
        self.stderr = b''
        self.logger = logger
        self.running_processes = running_processes
        self.terminated = False

    def __str__(self):
        return "LoggingProcessProtocol: %s" % self.cmd

    def connectionMade(self):
        #self.logger.debug(
            #"Process started: %s, job_id=%s", format_msg(self.cmd), self.job_id)
        self.transport.write(json.dumps(self.cmd).encode('utf-8'))
        self.transport.closeStdin()  # tell them we're done

    def outReceived(self, data):
        self.stdout += data

    def errReceived(self, data):
        self.stderr += data

    def processEnded(self, reason):
        # self.logger.debug(
            # "Process ended,  %s, job_id=%s", format_msg(self.cmd), self.job_id)
        try:
            self.running_processes.remove(self)
        except ValueError as e:
            self.logger.error(
                "ValueError when removing running proccess: %s", e)
        if self.terminated:
            self.logger.info("Terminated job_id=%i, no result back to engine", self.job_id)
            return

        exit_code = reason.value.exitCode
        # logger.debug("Result: %s" % repr(self.stdout)[:30])
        try:
            result = json.loads(self.stdout.decode('utf-8'))
        except json.decoder.JSONDecodeError:
            self.logger.warning(
                "Non json result: %s - cmd was :%s", repr(self.stdout), format_msg(self.cmd))
            result = {'error': 'non_json',
                      'stdout': self.stdout, 'stderr': self.stderr}
        result['ssh_process_return_code'] = exit_code
        self.logger.debug(
            "Process ended, return code 0, %s, job_id=%s, result: %s", format_msg(self.cmd), self.job_id, format_msg(result))
        self.job_done_calleback(self.job_id, result)


