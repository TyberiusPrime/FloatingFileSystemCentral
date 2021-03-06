import json
import pprint
from twisted.internet import reactor, protocol, error
import time
import os
from .exceptions import SSHConnectFailed, ManualInterventionNeeded


class MessageInProgress:
    def __init__(self, node_name, node_info, msg):
        self.node_name = node_name
        self.node_info = node_info
        self.msg = msg.copy()
        self.status = "unsent"
        self.send_time = 0

    def get_runtime(self):
        if self.status == "in_progress":
            return time.time() - self.send_time
        else:
            return -1

    def __repr__(self):
        return "Message to %s: %s - %s" % (self.node_name, self.msg, self.status)


def format_msg(msg):
    x = msg.copy()
    if "node.zip" in x:
        x["node.zip"] = "...base64 encoded data..."
    return pprint.pformat(x)


class OutgoingMessages:
    def __init__(self, logger, engine, ssh_cmd):
        self.max_per_host = engine.config.get_ssh_concurrent_connection_limit()
        self.max_rsync_per_host = engine.config.get_concurrent_rsync_limit()
        self.wait_time_between_requests = engine.config.get_ssh_rate_limit()
        self.last_message_times = {}
        self.logger = logger
        self.job_id = 0
        self.outgoing = {}
        self.running_processes = []
        self.engine = engine
        self.ssh_cmd = ssh_cmd
        self._shutdown = False

    def get_messages_for_node(self, node):
        try:
            return [x.msg for x in self.outgoing[node]]
        except KeyError:
            return []

    def kill_unsent_messages(self):
        self.logger.warn("Killing all unsent messages!")
        for node in self.outgoing:
            self.outgoing[node] = [
                x for x in self.outgoing[node] if x.status != "unsent"
            ]

    def send_message(self, node_name, node_info, msg):
        self.logger.info("Outgoing to %s: %s", node_name, format_msg(msg))
        if node_name not in self.outgoing:
            self.outgoing[node_name] = []
        x = MessageInProgress(node_name, node_info, msg)
        x.job_id = self.job_id
        self.job_id += 1

        self.outgoing[node_name].append(x)
        self.send_if_possible()

    def prioritize(self, messages):
        "new,  capture, send, remove_snapshot. Within, order by priority."

        def key(msg):
            order = 100
            if (
                msg.msg["msg"] == "set_properties"
            ):  # do these first, they might be user set_interval/set_priority requests
                order = 4
            elif (
                msg.msg["msg"] == "chown_and_chmod"
            ):  # these are also user/interactive requests
                order = 5
            elif msg.msg["msg"] == "new":
                order = 6
            elif msg.msg["msg"] == "capture":
                order = 7
            elif msg.msg["msg"] == "send_snapshot":
                order = 8
            elif msg.msg["msg"] == "remove_snapshot":
                order = 9
            prio = int(msg.msg.get("priority", 1000))
            return (order, prio)

        return sorted(messages, key=key)

    def send_if_possible(self):
        def any_parent_being_sent(ffs, new_in_progress):
            suffix, _ = os.path.split(ffs)
            while suffix:
                if suffix in new_in_progress:
                    return True
                suffix, _ = os.path.split(suffix)
            return False

        def any_sibling_being_being_sent_before(ffs, new_in_progress):
            parent, _ = os.path.split(ffs)
            for x in new_in_progress:
                if x == ffs:  # nothing *before this
                    break
                if x.startswith(parent + "/"):
                    return True
            return False

        for dummy_node_name, outbox in self.outgoing.items():
            unsent = [x for x in outbox if x.status == "unsent"]
            in_progress = [x for x in outbox if x.status == "in_progress"]
            transfers_in_progress = set(
                [x.msg["ffs"] for x in in_progress if x.msg["msg"] == "send_snapshot"]
            )
            new_in_progress = [
                x.msg["ffs"]
                for x in outbox
                if x.status in ("unsent", "in_progress") and x.msg["msg"] == "new"
            ]
            if unsent:
                if (
                    len(in_progress) < self.max_per_host
                ):  # no need to check anything if we're already at max send capacity
                    for x in self.prioritize(unsent):
                        if len(in_progress) < self.max_per_host:
                            if x.msg["msg"] == "send_snapshot":
                                if (
                                    len(transfers_in_progress)
                                    >= self.max_rsync_per_host
                                ):  # no more concurrent sends than this
                                    continue
                                if (
                                    x.msg["ffs"] in transfers_in_progress
                                ):  # only one send per receiving ffs!
                                    continue
                            elif x.msg["msg"] == "new" and (
                                any_parent_being_sent(x.msg["ffs"], new_in_progress)
                                or any_sibling_being_being_sent_before(
                                    x.msg["ffs"], new_in_progress
                                )
                            ):  # one new at a time per parent / don't send if parents are not done
                                # otherwise the readonly=off&back-on-again on non-main parents will cause issues
                                continue
                            while (
                                self.last_message_times.get(x.node_name, 0)
                                > time.time() - self.wait_time_between_requests
                            ):
                                self.logger.info("Delaying sending to %s", x.node_name)
                                time.sleep(self.wait_time_between_requests)
                            self.last_message_times[x.node_name] = time.time()
                            self.do_send(x)
                            in_progress.append(x)
                            x.status = "in_progress"
                            x.send_time = time.time()
                            if x.msg["msg"] == "send_snapshot":
                                transfers_in_progress.add(x.msg["ffs"])
                            if x.msg["msg"] == "new":
                                new_in_progress.append(x.msg["ffs"])
                        else:
                            break

    def do_send(self, msg):
        self.logger.info("Sending to %s: %s", msg.node_name, format_msg(msg.msg))
        ssh_cmd = self.ssh_cmd + [msg.node_info["hostname"], "/home/ffs/ssh.py"]
        m = msg.msg.copy()
        m["to"] = msg.node_name
        p = LoggingProcessProtocol(
            m, msg.job_id, self.job_returned, self.logger, self.running_processes
        )
        self.running_processes.append(p)
        reactor.spawnProcess(p, ssh_cmd[0], ssh_cmd, {})

    def job_returned(self, job_id, result):
        found = None
        for msgs in self.outgoing.values():
            for m in msgs:
                # non sent messages don't have a job_id
                if hasattr(m, "job_id") and m.job_id == job_id:
                    found = m
                    break
            if found:
                break
        if not found:
            self.logger.error("Job_id %s return, but no such job found", job_id)
            return
        if found.status != "in_progress":
            self.logger.error(
                "Job_id %s return, but not in progress! - was %s", job_id, m.status
            )
            return
        try:
            result["from"] = m.node_name
            self.engine.incoming_node(result)
        except SSHConnectFailed:
            self.engine.fault(
                "SSH connect to %s failed." % m.receiver,
                exception=ManualInterventionNeeded,
            )
        except Exception as e:
            import traceback

            self.logger.error(
                "Node processing error in job_id return %s %s %s, outgoing was: %s",
                job_id,
                result,
                e,
                format_msg(m.msg),
            )
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
                p.transport.signalProcess("INT")
                p.transport.signalProcess("HUP")
            except error.ProcessExitedAlready:
                pass
        time.sleep(1)
        for p in self.running_processes:
            self.logger.info("Killing running child: %s", p)
            try:
                p.transport.signalProcess("KILL")
            except error.ProcessExitedAlready:
                pass
        # os.killpg(os.getpgid(os.getpid()), signal.SIGTERM)


class OutgoingMessagesDryRun(OutgoingMessages):
    """Filter all messages that change the downstream.
    Useful to check whether the system will do 
    madness on first startup"""

    def send_message(self, node_name, node_info, msg):
        if msg["msg"] in ("deploy", "list_ffs"):
            OutgoingMessages.send_message(self, node_name, node_info, msg)
        else:
            self.logger.info("Msgfiltered to %s: %s", node_name, format_msg(msg))
            return


class LoggingProcessProtocol(protocol.ProcessProtocol):
    def __init__(self, cmd, job_id, job_done_callback, logger, running_processes):
        self.cmd = cmd
        self.job_id = job_id
        self.job_done_calleback = job_done_callback
        self.stdout = b""
        self.stderr = b""
        self.logger = logger
        self.running_processes = running_processes
        self.terminated = False

    def __str__(self):
        return "LoggingProcessProtocol: %s" % self.cmd

    def connectionMade(self):
        # self.logger.debug(
        # "Process started: %s, job_id=%s", format_msg(self.cmd), self.job_id)
        self.start_time = time.time()
        self.transport.write(json.dumps(self.cmd).encode("utf-8"))
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
            self.logger.error("ValueError when removing running proccess: %s", e)
        if self.terminated:
            self.logger.info(
                "Terminated job_id=%i, no result back to engine", self.job_id
            )
            return

        exit_code = reason.value.exitCode
        # logger.debug("Result: %s" % repr(self.stdout)[:30])
        try:
            result = json.loads(self.stdout.decode("utf-8"))
        except json.decoder.JSONDecodeError:
            self.logger.warning(
                "Non json result: %s - cmd was :%s",
                repr(self.stdout),
                format_msg(self.cmd),
            )
            result = {"error": "non_json", "stdout": self.stdout, "stderr": self.stderr}
        result["ssh_process_return_code"] = exit_code
        self.logger.debug(
            "Process ended, return code 0, time=%.2fs, %s, job_id=%s, result: %s",
            time.time() - self.start_time,
            format_msg(self.cmd),
            self.job_id,
            format_msg(result),
        )
        self.job_done_calleback(self.job_id, result)
