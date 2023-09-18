import multiprocessing
import time
import pytest
import dpdb
import os


@pytest.fixture(scope="function")
def reset_active():
    dpdb.set_active()
    dpdb.State.connections = {}
    yield None
    dpdb.set_active()
    dpdb.State.connections = {}


@pytest.fixture(scope="function")
def run_nodes(request):
    names = [name for _, name in request.param]
    print(f"run_nodes({names})")
    procs = [
        multiprocessing.Process(target=fn, args=(name,)) for fn, name in request.param
    ]
    for proc in procs:
        proc.start()
    time.sleep(1)  # Give some time for the process to set things up
    yield names
    for proc in procs:
        proc.terminate()


def _init_node(node_name):
    dpdb.InitNode(node_name)
    while True:
        time.sleep(0.1)  # Keeps the process alive


def _init_trace_node(node_name):
    dpdb.InitNode(node_name)
    while True:
        dpdb.set_trace()
        time.sleep(0.1)  # Keeps the process alive


@pytest.mark.parametrize(
    "run_nodes",
    [
        [
            (_init_node, "_init_node"),
            (_init_trace_node, "_init_trace_node"),
        ],
    ],
    indirect=True,
)
def test_list(reset_active, run_nodes):
    print(f"test_list({run_nodes})")
    # dpdb.Commands.dpdb_list()

#     captured = capsys.readouterr()
#     assert captured.out == """[state='running'] _init_trace_node
# [state='running'] _init_node
# """

    dpdb.Commands.dpdb_attach(dpdb.Target("_init_trace_node", "MainThread"))

#     captured = capsys.readouterr()
#     assert captured.out == """[state='running'] _init_trace_node
# [state='running'] _init_node
# """


@pytest.mark.parametrize(
    "run_nodes",
    [
        [(_init_node, "_init_node")],
        [(_init_trace_node, "_init_trace_node")],
    ],
    indirect=True,
)
def test_attach(reset_active, run_nodes):
    print(f"test_attach({run_nodes})")
    node_name = run_nodes[0]

    target = dpdb.Target(node_name, "MainThread")
    dpdb.Commands.dpdb_attach(target)

    assert dpdb.State.active_target == target
    assert dpdb.State.active_conn == dpdb.State.connections[target]

    dpdb.State.active_conn.write("p node_name")
    assert dpdb.State.active_conn.read() == f"'{node_name}'\n"
