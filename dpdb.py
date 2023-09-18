import glob
import io
import inspect
import os
import pdb
import readline
import signal
import socket
import sys
import time
import threading
from dataclasses import dataclass
from types import FrameType
from typing import Optional


@dataclass(frozen=True)
class Target:
    node: str
    thread: str

    def __str__(self) -> str:
        if self.thread == "MainThread":
            return self.node
        return f"{self.node}[{self.thread}]"

    def __lt__(self, rhs: "Target") -> bool:
        return str(self) < str(rhs)

    @staticmethod
    def here():
        return Target(Target.here.node, threading.current_thread().name)

    @property
    def sock_path(self):
        return f"/dev/shm/dpdb/{self.node}/{self.thread}.sock"

    @property
    def pid_path(self):
        return f"/dev/shm/dpdb/{self.node}.pid"


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
        os.remove(Target.here().sock_path)
        if Target.here().thread == "MainThread":
            declare_thread()
        self.set_continue()


@dataclass
class ThreadContext:
    pdb: pdb.Pdb
    sock: Optional[socket.socket]


def thread_ctx() -> ThreadContext:
    if not hasattr(thread_ctx, "_thread_local"):
        thread_ctx._thread_local = threading.local()

    if not hasattr(thread_ctx._thread_local, "ctx"):
        thread_ctx._thread_local.ctx = ThreadContext(
            pdb=DisconnectablePdb(),
            sock=None,
        )

    return thread_ctx._thread_local.ctx


def pdb_connect() -> pdb.Pdb:
    ctx = thread_ctx()

    if not ctx.pdb._io.remote_sock_io:
        server, _ = ctx.sock.accept()
        ctx.pdb._io.remote_sock_io = server.makefile("rw", buffering=1)

    return ctx.pdb


def declare_thread():
    ctx = thread_ctx()

    sock_path = Target.here().sock_path
    os.makedirs(os.path.dirname(sock_path), exist_ok=True)

    if not os.path.exists(sock_path):
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
    print(">> onsignal")
    declare_thread()
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
    Target.here.node = name

    for file in glob.glob(Target(name, "*").sock_path):
        os.remove(file)

    signal.signal(signal.SIGUSR1, onsignal)

    pid_path = Target.here().pid_path
    os.makedirs(os.path.dirname(pid_path), exist_ok=True)
    with open(pid_path, "w") as pid_file:
        pid_file.write(str(os.getpid()))


__all__ = ["InitNode", "set_trace"]


class Connection:
    def __init__(self, target: Target):
        self.target = target
        self.pdb_client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.pdb_io = self.pdb_client.makefile("rw", buffering=1)
        self.pdb_client.connect(target.sock_path)
        print(">> connected")
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
    connections = {}
    active_target = None
    active_conn = None


def set_active(target: Optional[Target] = None):
    State.active_target = target
    State.active_conn = None
    if target and target in State.connections:
        State.active_conn = State.connections[target]


def scan_targets():
    for path in glob.glob(Target("*", "*").pid_path):
        parts = path.split("/")
        target = Target(parts[-1][: -len(".pid")], "MainThread")
        try:
            os.kill(int(open(path).read()), 0)
            yield target
        except ProcessLookupError:
            os.remove(path)

        for path in glob.glob(Target(target.node, "*").sock_path):
            parts = path.split("/")
            thread_target = Target(parts[-2], parts[-1][: -len(".sock")])
            if thread_target != target:
                yield thread_target


def signal_connect(target: Target) -> Connection:
    pid = int(open(target.pid_path).read())
    os.kill(pid, signal.SIGUSR1)
    # TODO: Instead of sleeping, watch for the sock file.
    time.sleep(0.1)
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
    def dpdb_list():
        list_elem = {}
        for target in scan_targets():
            if target == State.active_target:
                list_elem[target] = "active "
            elif target in State.connections:
                list_elem[target] = "paused "
            else:
                list_elem[target] = "running"

        for target, state in sorted(list_elem.items()):
            print(f"[{state=}] {str(target)}")

    @staticmethod
    def dpdb_help():
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
    def dpdb_pause(target) -> Optional[Target]:
        if target in State.connections:
            return target

        try:
            State.connections[target] = Connection(target)
            return target
        except FileNotFoundError:
            pass

        if target.thread == "MainThread":
            try:
                State.connections[target] = signal_connect(target)
                return target
            except ProcessLookupError:
                print("stale pid. removing")
                os.remove(target.pid_path)
                os.remove(target.sock_path)
                return None

        print(f"unable to pause {target=}")
        return None

    @staticmethod
    def dpdb_resume(target):
        if target not in State.connections:
            return

        print(f"resuming {target}")
        del State.connections[target]

        if target == State.active_target:
            set_active(None)

    @staticmethod
    def dpdb_detach():
        if not State.active_target:
            return
        Commands.dpdb_resume(State.active_target)

    @staticmethod
    def dpdb_attach(target):
        set_active(Commands.dpdb_pause(target))

    @staticmethod
    def dispatch(cmd, args):
        if not hasattr(Commands, f"dpdb_{cmd}"):
            print("unknown command")
        fn = getattr(Commands, f"dpdb_{cmd}")
        kwargs = {}
        if "target" in inspect.signature(fn).parameters:
            try:
                kwargs["target"] = parse_target(args[0])
            except ValueError as err:
                print(err)
                return
        fn(**kwargs)


def completer(text, state):
    try:
        commands = [
            method[len("dpdb_") :]
            for method in dir(Commands)
            if method.startswith("dpdb_")
        ]

        line = readline.get_line_buffer().split()
        if not line or (len(line) == 1 and "dpbd".startswith(line[0])):
            # If we're on the first word, complete top-level commands
            return ["dpdb"][state]

        if len(line) == 2 and line[0] == "dpdb":
            if line[1] not in commands:
                # If we're on the second word and the first word is "dpdb", complete with sub-commands
                return [
                    cmd
                    for cmd in commands
                    if cmd.startswith(line[1]) and cmd != line[1]
                ][state]

        if len(line) == 3 and line[0] == "dpdb" and line[1] in commands:
            active_subcommand = getattr(Commands, f"dpdb_{line[1]}")
            if "target" in inspect.signature(active_subcommand).parameters:
                return sorted(
                    str(target)
                    for target in scan_targets()
                    if str(target).startswith(line[2])
                )[state]

    except IndexError:
        pass
    return None


def main():
    readline.parse_and_bind("tab: complete")
    readline.set_completer(completer)

    def onsignal(signum, frame):
        targets = list(State.connections.keys())
        for target in targets:
            Commands.dpdb_resume(target)
        sys.exit(0)

    signal.signal(signal.SIGINT, onsignal)

    print()
    Commands.dpdb_help()
    print("Detected Nodes")
    print("--------------")
    Commands.dpdb_list()
    print()

    while True:
        active_repr = (
            f" attached to {str(State.active_target)}"
            if State.active_target
            else " dettached"
        )
        try:
            command = input(f"DPDB{active_repr}\n$ ")
        except EOFError:
            break

        command = command.strip()
        parts = command.split()
        if not parts:
            continue
        if parts[0] == "dpdb":
            if len(parts) < 2:
                continue
            Commands.dispatch(parts[1], parts[2:])
            print()
        else:
            if not State.active_target:
                print("not attached")
                continue
            try:
                State.active_conn.write(command)
                print(State.active_conn.read())
            except ConnectionAbortedError:
                del State.connections[State.active_target]
                set_active(None)


if __name__ == "__main__":
    main()
