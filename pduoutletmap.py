_worker_to_outlet = {
    1: [15, 3],
    2: [16, 4],
    3: [17, 5],
    4: [18, 19],
    5: [6, 7],
    6: [11, 14],
}

_worker_to_slaves_vm6cores = {
    1: ["slave401"],
    2: ["slave402"],
    3: ["slave403"],
    4: ["slave404"],
    5: ["slave405"],
    6: ["slave406"],
}

_worker_to_slaves_vm3cores = {
    1: ["slave201", "slave207"],
    2: ["slave202", "slave208"],
    3: ["slave203", "slave209"],
    4: ["slave204", "slave210"],
    5: ["slave205", "slave211"],
    6: ["slave206", "slave212"],
}

_num_slaves_to_workers_vm3cores = {
    12: 6,
    10: 5,
    8: 4,
    6: 3,
}

_num_slaves_to_workers_vm6cores = {
    6: 6,
    5: 5,
    4: 4,
    3: 3,
}

_workers = [1, 2, 3, 4, 5, 6]


def get_all_workers():
    return _workers


def get_worker_outlets(worker):
    return _worker_to_outlet[worker]


# def get_workers_from_slaves(slaves):
#    ws1 = [w for w in _worker_to_slaves_vm3cores if len(set(_worker_to_slaves_vm3cores[w]) & set(slaves)) > 0]
#    ws2 = [w for w in _worker_to_slaves_vm6cores if len(set(_worker_to_slaves_vm6cores[w]) & set(slaves)) > 0]
#    return sorted(list(set(ws1 + ws2)))

def get_workers_from_hosts(slaves):
    slaves = list(set(slaves))
    vm_config = None
    for s in slaves:
        if s.startswith("slave2"):
            vm_config = 3
        else:
            vm_config = 6

    num_slaves = len(slaves)
    if vm_config == 3:
        return sorted(list(range(1, _num_slaves_to_workers_vm3cores[num_slaves] + 1)))
    elif vm_config == 6:
        return sorted(list(range(1, _num_slaves_to_workers_vm6cores[num_slaves] + 1)))


def get_outlets_from_hosts(slaves):
    workers = get_workers_from_hosts(slaves)
    return [o for w in workers for o in _worker_to_outlet[w]]


def get_worker_from_host(host):
    ws1 = set([w for w in _worker_to_slaves_vm3cores if host in _worker_to_slaves_vm3cores[w]])
    ws2 = set([w for w in _worker_to_slaves_vm6cores if host in _worker_to_slaves_vm6cores[w]])
    ws = list(ws1 | ws2)
    assert len(ws) == 1
    return ws[0]
