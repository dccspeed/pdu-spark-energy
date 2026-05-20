"""
PDU Outlet Mapping Module

Maps physical cluster workers to PDU outlets and manages worker-host relationships.

This module contains the hardware configuration that defines:
- Which PDU outlets are connected to which physical workers
- How virtual machines (VMs) are distributed across workers
- Mapping between Spark slave hostnames and physical workers

Hardware Configuration:
- 6 physical workers, each connected to 2 PDU outlets
- Two VM configurations: 3-core VMs and 6-core VMs
- Different slave naming conventions for each VM type (slave2XX for 3-core, slave4XX for 6-core)
"""

# Mapping: worker ID -> list of PDU outlet numbers
_worker_to_outlet = {
    1: [15, 3],
    2: [16, 4],
    3: [17, 5],
    4: [18, 19],
    5: [6, 7],
    6: [11, 14],
}

# Mapping for 6-core VM configuration: worker ID -> list of slave hostnames
_worker_to_slaves_vm6cores = {
    1: ["slave401"],
    2: ["slave402"],
    3: ["slave403"],
    4: ["slave404"],
    5: ["slave405"],
    6: ["slave406"],
}

# Mapping for 3-core VM configuration: worker ID -> list of slave hostnames
# Each worker hosts 2 VMs in this configuration
_worker_to_slaves_vm3cores = {
    1: ["slave201", "slave207"],
    2: ["slave202", "slave208"],
    3: ["slave203", "slave209"],
    4: ["slave204", "slave210"],
    5: ["slave205", "slave211"],
    6: ["slave206", "slave212"],
}

# Mapping: number of slaves -> number of active workers for 3-core VMs
_num_slaves_to_workers_vm3cores = {
    12: 6,
    10: 5,
    8: 4,
    6: 3,
}

# Mapping: number of slaves -> number of active workers for 6-core VMs
_num_slaves_to_workers_vm6cores = {
    6: 6,
    5: 5,
    4: 4,
    3: 3,
}

_workers = [1, 2, 3, 4, 5, 6]


def get_all_workers():
    """
    Get list of all available worker IDs.
    
    Returns:
        list: Worker IDs [1, 2, 3, 4, 5, 6]
    """
    return _workers


def get_worker_outlets(worker):
    """
    Get PDU outlet numbers for a specific worker.
    
    Args:
        worker (int): Worker ID (1-6)
        
    Returns:
        list: List of PDU outlet numbers connected to this worker
    """
    return _worker_to_outlet[worker]


def get_workers_from_hosts(slaves):
    """
    Determine which workers are active based on the list of slave hostnames.
    
    This function infers:
    1. The VM configuration (3-core or 6-core) from hostname patterns
    2. The number of active workers based on the number of slaves
    
    Args:
        slaves (list): List of slave hostnames (e.g., ['slave201', 'slave202'])
        
    Returns:
        list: Sorted list of active worker IDs
    """
    slaves = list(set(slaves))
    vm_config = None
    
    # Determine VM configuration from hostname pattern
    for s in slaves:
        if s.startswith("slave2"):
            vm_config = 3  # 3-core VMs
        else:
            vm_config = 6  # 6-core VMs

    num_slaves = len(slaves)
    
    # Return active workers based on VM config and number of slaves
    if vm_config == 3:
        return sorted(list(range(1, _num_slaves_to_workers_vm3cores[num_slaves] + 1)))
    elif vm_config == 6:
        return sorted(list(range(1, _num_slaves_to_workers_vm6cores[num_slaves] + 1)))


def get_outlets_from_hosts(slaves):
    """
    Get all PDU outlets for the workers hosting the given slaves.
    
    Args:
        slaves (list): List of slave hostnames
        
    Returns:
        list: Flattened list of all PDU outlet numbers for these slaves
    """
    workers = get_workers_from_hosts(slaves)
    return [o for w in workers for o in _worker_to_outlet[w]]


def get_worker_from_host(host):
    """
    Get the worker ID for a specific slave hostname.
    
    Args:
        host (str): Slave hostname (e.g., 'slave401' or 'slave201')
        
    Returns:
        int: Worker ID (1-6)
        
    Raises:
        AssertionError: If host is found in multiple workers or not found
    """
    # Search in both VM configurations
    ws1 = set([w for w in _worker_to_slaves_vm3cores if host in _worker_to_slaves_vm3cores[w]])
    ws2 = set([w for w in _worker_to_slaves_vm6cores if host in _worker_to_slaves_vm6cores[w]])
    ws = list(ws1 | ws2)
    
    # Each host should belong to exactly one worker
    assert len(ws) == 1
    return ws[0]
