import glob
import io
import json
import os
import pdb
import readline
import signal
import socket
import sys
import threading
from dataclasses import dataclass
from types import FrameType
from typing import Optional

import a0


def node(name: Optional[str] = None) -> str:
    if name is not None and not hasattr(node, "_name"):
        node._name = name
    elif name is not None and hasattr(node, "_name"):
        raise RuntimeError("Node has already been initialized.")
    elif name is None and not hasattr(node, "_name"):
        raise RuntimeError("Node has not been initialized.")
    return node._name


@dataclass(frozen=True)
class Target:
    node: str
    thread: str

    def __str__(self) -> str:
        if self.thread == "MainThread":
            return self.node
        return f"{self.node}[{self.thread}]"

    @staticmethod
    def here():
        return Target(node(), threading.current_thread().name)

    @property
    def sock_path(self):
        return f"/dev/shm/dpdb/{self.node}/{self.thread}.sock"

    @property
    def pid_path(self):
        return f"/dev/shm/dpdb/{self.node}.pid"

    @property
    def deadman_topic(self):
        return f"dpdb/{self.node}/{self.thread}"


class PdbIO:
    def __init__(self):
        self.remote_sock_io: io.IOBase = None

    def write(self, *args):
        if self.remote_sock_io:
            self.remote_sock_io.write(*args)

    def flush(self):
        if self.remote_sock_io:
            self.remote_sock_io.flush()

    def readline(self):
        if self.remote_sock_io:
            return self.remote_sock_io.readline()
        return "c"


class DisconnectablePdb(pdb.Pdb):
    def __init__(self):
        self._io = PdbIO()
        pdb.Pdb.__init__(self, stdin=self._io, stdout=self._io, nosigint=True)

    def do_disconnect(self, arg):
        self._io.remote_sock_io = None
        self.set_continue()


@dataclass
class ThreadContext:
    pdb: pdb.Pdb
    sock: Optional[socket.socket]
    deadman: a0.Deadman


def thread_ctx() -> ThreadContext:
    if not hasattr(thread_ctx, "_thread_local"):
        thread_ctx._thread_local = threading.local()

    if not hasattr(thread_ctx._thread_local, "ctx"):
        thread_ctx._thread_local.ctx = ThreadContext(
            pdb=DisconnectablePdb(),
            sock=None,
            deadman=a0.Deadman(Target.here().deadman_topic),
        )

    return thread_ctx._thread_local.ctx


def pdb_connect() -> pdb.Pdb:
    ctx = thread_ctx()

    if ctx.pdb._io.remote_sock_io:
        return ctx.pdb

    server, _ = ctx.sock.accept()
    ctx.pdb._io.remote_sock_io = server.makefile("rw", buffering=1)

    return ctx.pdb


def declare_thread():
    ctx = thread_ctx()
    if not ctx.deadman.try_take():
        return

    sock_path = Target.here().sock_path
    os.makedirs(os.path.dirname(sock_path), exist_ok=True)

    ctx.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    ctx.sock.bind(sock_path)
    os.chmod(sock_path, 0o666)
    ctx.sock.listen(1)


def set_trace() -> None:
    """
    Initiates a debugger session.

    This function pauses execution and waits for a dpdb terminal to connect.
    This can be called from any thread.

    Example:
        def some_function():
            # ... some code ...
            dpdb.set_trace()  # This will initiate a debugger session right at this line.
            # ... more code ...
    """
    declare_thread()
    pdb_connect().set_trace(sys._getframe().f_back)


def onsignal(signum: int, frame: FrameType) -> None:
    pdb_connect().set_trace(sys._getframe().f_back)


def InitNode(name: str) -> None:
    """
    Initializes the process for debugging with the dpdb (distributed-pdb) terminal.

    Call this function once, ideally at the start of your script or main function, before any debugging sessions.

    dpdb can connect to the main thread at any time. However, other threads need to call dpdb.set_trace() explicitly.

    :param name: A unique name for the debugging node. This helps tell apart different processes.

    Example:
        if __name__ == '__main__':
            dpdb.InitNode('my_node_name')
            # ... rest of the code ...
    """
    node(name)

    for file in glob.glob(Target(name, "*").sock_path):
        os.remove(file)

    signal.signal(signal.SIGUSR1, onsignal)
    declare_thread()

    with open(Target.here().pid_path, "w") as pid_file:
        pid_file.write(str(os.getpid()))


__all__ = ["InitNode", "set_trace"]


class Connection:
    def __init__(self, target: Target):
        self.target = target
        self.pdb_client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.pdb_client.connect(target.sock_path)
        self.pdb_io = self.pdb_client.makefile("rw", buffering=1)
        self.read()  # Clear initial output.

    def __del__(self):
        self.pdb_io.write("disconnect")

    def read(self):
        prompt = "(Pdb) "

        buffer = ""
        while True:
            char_ = self.pdb_io.read(1)
            if not char_:
                raise ConnectionAbortedError("Pdb socket closed by server")
            buffer += char_
            if buffer.endswith(prompt):
                return buffer[: -len(prompt)]

    def write(self, msg):
        self.pdb_io.write(msg + "\n")
        self.pdb_io.flush()


@dataclass(frozen=True)
class State:
    known_targets = set()
    connections = {}
    active_target = None
    active_conn = None


def set_active(target: Target):
    State.active_target = target
    State.active_conn = None
    if target and target in State.connections:
        State.active_conn = State.connections[target]


def scan_targets():
    for path in glob.glob(Target("*", "*").sock_path):
        parts = path.split("/")
        target = Target(parts[-2], parts[-1][: -len(".sock")])
        if a0.Deadman(target.deadman_topic).state().is_taken:
            yield target


def signal_connect(target: Target) -> Connection:
    pid = int(open(target.pid_path).read())
    os.kill(pid, signal.SIGUSR1)
    return Connection(target)


def parse_target(s: Optional[str]) -> Target:
    if not s:
        raise ValueError("missing target")

    if "[" not in s:
        return Target(s, "MainThread")

    if s[-1] != "]":
        raise ValueError("missing target")

    parts = s.split("[")
    return Target(parts[0], parts[1][:-1])


class Commands:
    @staticmethod
    def dpdb_list(args):
        list_elem = {}
        for target in State.known_targets:
            if target == State.active_target:
                list_elem[target] = "active "
            elif target in State.connections:
                list_elem[target] = "paused "
            else:
                list_elem[target] = "running"

        for target, state in sorted(list_elem.items()):
            print(f"[{state=}] {str(target)}")

    @staticmethod
    def dpdb_help(args):
        print(
            """DPDB (Distributed-PDB):
* dpdb help: This.
* dpdb list: Lists all available pdb.
* dpdb pause node[thread]: Pauses the given pdb.
* dpdb resume node[thread]: Resumes the given pdb.
* dpdb attach node[thread]: Pauses and connects to the given pdb.
* dpdb detach: Resumes and disconnects from the current pdb.
All other commands are forwarded to the connected pdb.
"""
        )

    @staticmethod
    def dpdb_pause(args) -> Optional[Target]:
        try:
            target = parse_target(args[0])
        except ValueError as err:
            print(err)
            return None

        if target not in State.known_targets:
            print("unknown target")
            return None

        if target not in State.connections:
            if target.thread == "MainThread":
                State.connections[target] = signal_connect(target)
            else:
                State.connections[target] = Connection(target)

        return target

    @staticmethod
    def dpdb_resume(args):
        try:
            target = parse_target(args[0])
        except ValueError as err:
            print(err)
            return

        if target not in State.known_targets:
            print("unknown target")
            return

        if target not in State.connections:
            return

        print(f"resuming {target}")
        del State.connections[target]

        if target == State.active_target:
            set_active(None)

    @staticmethod
    def dpdb_detach(args):
        if not State.active_target:
            return
        Commands.dpdb_resume([str(State.active_target)])

    @staticmethod
    def dpdb_attach(args):
        set_active(Commands.dpdb_pause(args))


def main():
    def onsignal(signum, frame):
        targets = list(State.connections.keys())
        for target in targets:
            Commands.dpdb_resume([str(target)])
        sys.exit(0)

    signal.signal(signal.SIGINT, onsignal)

    State.known_targets = set(scan_targets())

    print()
    Commands.dpdb_help(None)
    if State.known_targets:
        print("Detected Nodes")
        print("--------------")
        Commands.dpdb_list(None)
    print()

    while True:
        active_repr = f" {str(State.active_target)}" if State.active_target else ""
        try:
            command = input(f"dpdb PDB{active_repr}\n$ ")
        except EOFError:
            break

        command = command.strip()
        parts = command.split()
        if not parts:
            continue
        if parts[0] == "dpdb":
            if len(parts) < 2:
                continue
            State.known_targets = set(scan_targets())

            subcmd = parts[1]
            if subcmd == "help":
                Commands.dpdb_help(parts[2:])
            elif subcmd == "list":
                Commands.dpdb_list(parts[2:])
            elif subcmd == "pause":
                Commands.dpdb_pause(parts[2:])
            elif subcmd == "resume":
                Commands.dpdb_resume(parts[2:])
            elif subcmd == "attach":
                Commands.dpdb_attach(parts[2:])
            elif subcmd == "detach":
                Commands.dpdb_detach(parts[2:])
            else:
                print("unknown command")
            print()
        else:
            if not State.active_target:
                print("not attached")
                continue
            State.active_conn.write(command)
            print(State.active_conn.read())


if __name__ == "__main__":
    main()
