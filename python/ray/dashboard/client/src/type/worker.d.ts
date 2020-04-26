export interface CoreWorkerStats {
  "currentTaskFuncDesc": string,
  "ipAddress": string,
  "port": string,
  "actorId": string,
  "usedResources": { [key: string]: number },
  "numExecutedTasks": number,
  "workerId": string
}

export interface Worker {
  "create_time": number,
  "cpu_percent": number,
  "cmdline": string[],
  "memory_info": {
    "rss": number, // aka “Resident Set Size”, this is the non-swapped physical memory a process has used. On UNIX it matches “top“‘s RES column). On Windows this is an alias for wset field and it matches “Mem Usage” column of taskmgr.exe.
    "vms": number, // aka “Virtual Memory Size”, this is the total amount of virtual memory used by the process. On UNIX it matches “top“‘s VIRT column. On Windows this is an alias for pagefile field and it matches “Mem Usage” “VM Size” column of taskmgr.exe.
    "pfaults": number, // number of page faults.
    "pageins": number, // number of actual pageins.
    [key: string]: number,
  },
  "cpu_times": {
    "user": number,
    "system": number,
    "children_user": number,
    "children_system": number,
    iowait?: number,
  },
  "pid": number,
  "coreWorkerStats": CoreWorkerStats[]
}