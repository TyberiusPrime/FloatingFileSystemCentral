import json
from twisted.internet import reactor, protocol, error
import time


class MessageInProgress:

    def __init__(self, node_name, node_info, msg):
        self.node_name = node_name
        self.node_info = node_info
        self.msg = msg
        self.status = 'unsent'


class OutgoingMessages():

    def __init__(self, logger, engine, ssh_cmd):
        self.max_per_host = 5
        self.logger = logger
        self.job_id = 0
        self.outgoing = {}
        self.running_processes = []
        self.engine = engine
        self.ssh_cmd = ssh_cmd

    def send_message(self, node_name, node_info, msg):
        if msg['msg'] not in ('deploy', 'list_ffs'):
            self.logger.info(
                "Msgfiltered to %s (%s): %s", node_name, node_info, msg)
            return
        self.logger.info("Outgoing to %s (%s): %s",
                               node_name, node_info, msg)
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
                x for x in in_progress if x.msg == 'send_snapshot']
            if unsent:
                for x in unsent:
                    if len(in_progress) < self.max_per_host:
                        if (
                            (x.msg == 'send_snapshot' and not transfers_in_progress) or
                            (x.msg != 'send_snapshot')
                        ):
                            self.do_send(x)
                            in_progress.append(x)
                    else:
                        break

    def do_send(self, msg):
        self.logger.info("Sending to %s(%s): %s",
                               msg.node_name, msg.node_info, msg.msg)
        ssh_cmd = self.ssh_cmd + \
            [msg.node_info['hostname'], '/home/ffs/ssh.py']
        msg.job_id = self.job_id
        self.job_id += 1
        p = LoggingProcessProtocol(msg.msg, msg.job_id, self.job_returned,
                                   self.logger, self.running_processes)
        msg.status = 'in_progress'
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
                "Node processing error in job_id return %s %s %s", job_id, result, e)
            self.logger.error(traceback.format_exc())

    def shutdown(self):
        for p in self.running_processes:
            self.logger.info("Terminating running child: %s", p)
            try:
                p.transport.signalProcess('INT')
                p.transport.signalProcess('HUP')
            except error.ProcessExitedAlready:
                pass
        time.sleep(1)
        for p in self.running_processes:
            self.info("Killing running child: %s", p)
            try:
                p.transport.signalProcess('KILL')
            except error.ProcessExitedAlready:
                pass
        #os.killpg(os.getpgid(os.getpid()), signal.SIGTERM)


class LoggingProcessProtocol(protocol.ProcessProtocol):

    def __init__(self, cmd, job_id, job_done_callback, logger, running_processes):
        self.cmd = cmd
        self.job_id = job_id
        self.job_done_calleback = job_done_callback
        self.stdout = b''
        self.stderr = b''
        self.logger = logger
        self.running_processes = running_processes

    def __str__(self):
        return "LoggingProcessProtocol: %s" % self.cmd

    def connectionMade(self):
        self.logger.debug(
            "Process started: %s, job_id=%s", self.cmd, self.job_id)
        self.transport.write(json.dumps(self.cmd).encode('utf-8'))
        self.transport.closeStdin()  # tell them we're done

    def outReceived(self, data):
        self.stdout += data

    def errReceived(self, data):
        self.stderr += data

    def processEnded(self, reason):
        self.logger.debug(
            "Process ended,  %s, job_id=%s", self.cmd, self.job_id)
        try:
            self.running_processes.remove(self)
        except ValueError as e:
            self.logger.error("ValueError when removing running proccess: %s", e)

        exit_code = reason.value.exitCode
        # logger.debug("Result: %s" % repr(self.stdout)[:30])
        try:
            result = json.loads(self.stdout.decode('utf-8'))
        except json.decoder.JSONDecodeError:
            self.logger.warning(
                "Non json result: %s - cmd was :%s", repr(self.stdout), self.cmd)
            result = {'error': 'non_json',
                      'stdout': self.stdout, 'stderr': self.stderr}
        result['ssh_process_return_code'] = exit_code
        self.logger.debug(
            "Process ended, return code 0, %s, job_id=%s, result: %s", self.cmd, self.job_id, result)
        self.job_done_calleback(self.job_id, result)
